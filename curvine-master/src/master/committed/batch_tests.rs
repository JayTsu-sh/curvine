use super::*;
use crate::master::{committed::authority::*, journal::JournalSystem, Master};
use curvine_core_error::{CommonResult, ErrorExt};
use curvine_fs_api::RpcCode;
use curvine_model::{hsm::native::Namespace, CreateFileOpts, OpenFlags, ProtoUtils};
use curvine_runtime::common::LocalTime;

fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem)> {
    Master::init_test_metrics();
    let root = tempfile::tempdir()?;
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
    conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
    let fs = JournalSystem::fs_only_for_test(&conf)?;
    {
        let mut dir = fs.fs_dir.write();
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(
            b"\x03hsm:namespace:v1",
            &serde_json::to_vec(&Namespace {
                ordinary_format: 2,
                root_inode: 1,
                storage: "source".into(),
                incarnation: "incarnation".into(),
                epoch: 1,
            })?,
        )?;
        batch.commit()?;
        dir.hsm_enabled = true;
    }
    Ok((root, fs))
}
fn create(path: &str, overwrite: bool) -> CreateFileRequest {
    CreateFileRequest {
        path: path.into(),
        opts: ProtoUtils::create_opts_to_pb(CreateFileOpts::with_create(true), "batch-writer"),
        flags: OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(overwrite)
            .value(),
    }
}
fn complete(path: &str) -> CompleteFileRequest {
    CompleteFileRequest {
        path: path.into(),
        len: 0,
        client_name: "batch-writer".into(),
        commit_blocks: vec![],
        only_flush: false,
        inode_id: None,
        set_attr_opts: None,
        return_file_blocks: Some(false),
    }
}
fn command(
    fs: &MasterFilesystem,
    view: &mut super::super::view::BatchView<'_>,
    code: RpcCode,
    header: impl Message,
) -> CommonResult<NativeCommand> {
    let now = LocalTime::mills() as i64;
    let header = header.encode_to_vec();
    let request = NativeMutationRequest {
        version: 1,
        code: code as u32,
        header: header.clone(),
        ..Default::default()
    };
    let prepared = prepare(
        NativeInput::parse(&request)?,
        fs,
        &MountManager::new(fs.clone()),
        view,
        now,
    )?;
    let mut hash = blake3::Hasher::new();
    hash.update(&(code as u32).to_le_bytes());
    hash.update(&header);
    Ok(NativeCommand {
        identity: RequestIdentity {
            incarnation: "incarnation".into(),
            session: format!("{now:016x}{}", curvine_model::hsm::native::token()),
            sequence: 1,
            acknowledged: 0,
        },
        fingerprint: *hash.finalize().as_bytes(),
        time: now,
        edits: prepared.edits,
        reply: NativeReply {
            code: code as i8,
            success: prepared.failure.is_none(),
            header: if prepared.failure.is_none() {
                prepared.response
            } else {
                vec![]
            },
            body: prepared
                .failure
                .map(|error| error.encode().to_vec())
                .unwrap_or_default(),
        },
    })
}

#[test]
fn batch_create_coalesces_shared_parents_and_commits_only_the_successful_prefix() -> CommonResult<()>
{
    let (_root, fs) = fixture()?;
    let operation = {
        let dir = fs.fs_dir.read();
        let mut view = super::super::view::BatchView::new(&dir, 2 * 1024 * 1024);
        command(
            &fs,
            &mut view,
            RpcCode::CreateFilesBatch,
            CreateFilesBatchRequest {
                requests: vec![
                    create("/batch/first", false),
                    create("/batch/first", false),
                    create("/batch/never", false),
                ],
            },
        )?
    };
    assert!(!operation.reply.success);
    assert!(!fs.exists("/batch")?);
    let first = apply_creation(&mut fs.fs_dir.write(), &operation)?;
    assert!(matches!(first.kind, ResultKind::Applied));
    assert!(!first.reply.success);
    assert!(fs.exists("/batch/first")?);
    assert!(!fs.exists("/batch/never")?);
    assert_eq!(fs.list_status("/batch")?.len(), 1);
    fs.delete("/batch/first", false)?;
    let retry = apply_creation(&mut fs.fs_dir.write(), &operation)?;
    assert_eq!(retry.reply.body, first.reply.body);
    assert!(
        !fs.exists("/batch/first")?,
        "retry reexecuted a known successful prefix"
    );
    Ok(())
}

#[test]
fn batch_create_publishes_many_siblings_in_one_owned_command() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let operation = {
        let dir = fs.fs_dir.read();
        let mut view = super::super::view::BatchView::new(&dir, 2 * 1024 * 1024);
        command(
            &fs,
            &mut view,
            RpcCode::CreateFilesBatch,
            CreateFilesBatchRequest {
                requests: (0..64)
                    .map(|i| create(&format!("/batch/file-{i}"), false))
                    .collect(),
            },
        )?
    };
    assert!(operation.reply.success);
    assert!(!fs.exists("/batch")?);
    let result = apply_creation(&mut fs.fs_dir.write(), &operation)?;
    assert!(result.reply.success, "{:?}", result.reply.body);
    assert_eq!(fs.list_status("/batch")?.len(), 64);
    let response = CreateFilesBatchResponse::decode(result.reply.header.as_slice())?;
    assert_eq!(response.file_statuses.len(), 64);
    assert_eq!(
        apply_creation(&mut fs.fs_dir.write(), &operation)?
            .reply
            .header,
        result.reply.header
    );
    Ok(())
}

#[test]
fn batch_close_preserves_item_results_and_rebases_repeated_record_versions() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let file = fs.create("/data", false)?;
    let (operation, later) = {
        let dir = fs.fs_dir.read();
        let mut view = super::super::view::BatchView::new(&dir, 2 * 1024 * 1024);
        let operation = command(
            &fs,
            &mut view,
            RpcCode::CompleteFilesBatch,
            CompleteFilesBatchRequest {
                requests: vec![complete("/data"), complete("/missing"), complete("/data")],
            },
        )?;
        let later = super::super::commands::set_attr(
            &mut view,
            "/data",
            curvine_model::SetAttrOpts {
                mode: Some(0o640),
                ..Default::default()
            },
            LocalTime::mills() as i64,
        )?;
        (operation, later)
    };
    assert!(fs.fs_dir.read().hsm_record(file.id)?.is_none());
    let result = apply_creation(&mut fs.fs_dir.write(), &operation)?;
    assert!(result.reply.success);
    assert_eq!(
        CompleteFilesBatchResponse::decode(result.reply.header.as_slice())?.results,
        vec![true, false, true]
    );
    assert_eq!(fs.fs_dir.read().hsm_record(file.id)?.unwrap().version, 1);
    let next = NativeCommand {
        identity: RequestIdentity {
            sequence: 2,
            incarnation: operation.identity.incarnation.clone(),
            session: operation.identity.session.clone(),
            acknowledged: 0,
        },
        fingerprint: [42; 32],
        time: operation.time,
        edits: later.edits,
        reply: NativeReply {
            code: RpcCode::SetAttr as i8,
            success: true,
            header: later.response,
            body: vec![],
        },
    };
    assert!(
        apply_creation(&mut fs.fs_dir.write(), &next)?.reply.success,
        "following coordinator command observed an unpublished intermediate version"
    );
    assert_eq!(fs.file_status("/data")?.mode, 0o640);
    Ok(())
}

#[test]
fn batch_prefix_is_not_published_when_a_read_changes_before_commit() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let file = fs.create("/existing", false)?;
    fs.complete_file("/existing", Some(file.id), 0, vec![], "", false, None)?;
    let operation = {
        let dir = fs.fs_dir.read();
        let mut view = super::super::view::BatchView::new(&dir, 2 * 1024 * 1024);
        command(
            &fs,
            &mut view,
            RpcCode::CreateFilesBatch,
            CreateFilesBatchRequest {
                requests: vec![create("/new", false), create("/existing", true)],
            },
        )?
    };
    assert!(operation.reply.success);
    {
        let dir = fs.fs_dir.read();
        let mut record = dir.hsm_record(file.id)?.unwrap();
        record.version += 1;
        record.readers.insert("concurrent-reader".into(), i64::MAX);
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(
            format!("\x03hsm:record:{}", file.id).as_bytes(),
            &serde_json::to_vec(&record)?,
        )?;
        batch.commit()?;
    }
    let result = apply_creation(&mut fs.fs_dir.write(), &operation)?;
    assert!(matches!(result.kind, ResultKind::Conflict));
    assert!(!fs.exists("/new")?);
    assert!(fs
        .fs_dir
        .read()
        .hsm_record(file.id)?
        .unwrap()
        .readers
        .contains_key("concurrent-reader"));
    Ok(())
}
