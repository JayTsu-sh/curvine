use super::*;
use crate::master::{journal::JournalSystem, Master};
use curvine_core_error::CommonResult;
use curvine_model::{hsm::native::Namespace, ProtoUtils, RenameFlags, SetAttrOpts};
use curvine_proto::{DeleteRequest, DeleteResponse, RenameRequest, SetAttrRequest};
use prost::Message;

fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem)> {
    Master::init_test_metrics();
    let root = tempfile::tempdir()?;
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
    conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
    let fs = JournalSystem::fs_only_for_test(&conf)?;
    let namespace = Namespace {
        ordinary_format: 2,
        root_inode: 1,
        storage: "source".into(),
        incarnation: "incarnation".into(),
        epoch: 1,
    };
    {
        let mut dir = fs.fs_dir.write();
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(b"\x03hsm:namespace:v1", &serde_json::to_vec(&namespace)?)?;
        batch.commit()?;
        dir.hsm_enabled = true;
    }
    Ok((root, fs))
}
fn file(fs: &MasterFilesystem, path: &str) -> CommonResult<i64> {
    let file = fs.create(path, true)?;
    fs.complete_file(path, Some(file.id), 0, vec![], "", false, None)?;
    Ok(file.id)
}
fn command(
    fs: &MasterFilesystem,
    code: curvine_fs_api::RpcCode,
    header: impl Message,
) -> CommonResult<NamespaceCommand> {
    let time = LocalTime::mills() as i64;
    let header = header.encode_to_vec();
    Ok(NamespaceCommand {
        version: VERSION,
        identity: RequestIdentity {
            incarnation: "incarnation".into(),
            session: format!("{:016x}{}", time, curvine_model::hsm::native::token()),
            sequence: 1,
            acknowledged: 0,
        },
        fingerprint: fingerprint(code as u32, &header),
        time,
        protection: fs.fs_dir.read().hsm_read_time()?,
        code: code as u32,
        header,
        admission_error: None,
        ttl: None,
    })
}
fn replay(fs: &MasterFilesystem, bytes: &[u8]) -> CommonResult<NativeMutationResponse> {
    apply(&mut fs.fs_dir.write(), SerdeUtils::deserialize(bytes)?)
}
fn rename(
    fs: &MasterFilesystem,
    src: &str,
    dst: &str,
    flags: RenameFlags,
) -> CommonResult<Vec<u8>> {
    SerdeUtils::serialize(&command(
        fs,
        curvine_fs_api::RpcCode::Rename,
        RenameRequest {
            src: src.into(),
            dst: dst.into(),
            flags: flags.value(),
        },
    )?)
}

#[test]
fn compact_rename_moves_full_subtrees_exchanges_and_keeps_original_results() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let id = file(&fs, "/left/sub/data")?;
    let other = file(&fs, "/right/other")?;
    let operation = rename(&fs, "/left", "/moved", RenameFlags::new(0))?;
    assert!(operation.len() < 4096);
    let response = replay(&fs, &operation)?;
    assert!(response.success, "{:?}", response);
    assert!(!fs.exists("/left")?);
    assert_eq!(fs.file_status("/moved/sub/data")?.id, id);
    assert_eq!(
        fs.fs_dir.read().hsm_record(id)?.unwrap().path,
        "/moved/sub/data"
    );
    assert_eq!(fs.list_status("/")?.len(), 2);
    assert_eq!(fs.list_status("/moved/sub")?.len(), 1);
    // Reusing the old path must not make an old retry move it again.
    let replacement = file(&fs, "/left/new")?;
    assert_eq!(replay(&fs, &operation)?, response);
    assert_eq!(fs.file_status("/left/new")?.id, replacement);
    let exchange = rename(&fs, "/moved", "/right", RenameFlags::EXCHANGE)?;
    assert!(replay(&fs, &exchange)?.success);
    assert_eq!(fs.file_status("/right/sub/data")?.id, id);
    assert_eq!(fs.file_status("/moved/other")?.id, other);
    assert_eq!(
        fs.fs_dir.read().hsm_record(id)?.unwrap().path,
        "/right/sub/data"
    );
    assert_eq!(
        fs.fs_dir.read().hsm_record(other)?.unwrap().path,
        "/moved/other"
    );
    assert!(
        !replay(
            &fs,
            &rename(&fs, "/right", "/right/sub/cycle", RenameFlags::new(0))?
        )?
        .success
    );
    assert_eq!(fs.file_status("/right/sub/data")?.id, id);
    Ok(())
}

#[test]
fn recursive_attributes_use_compact_input_and_preserve_child_option_rules() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let mut ids = vec![];
    for i in 0..2048 {
        ids.push(file(&fs, &format!("/wide/data-{i}"))?);
    }
    let before = fs.fs_dir.read().hsm_record(ids[0])?.unwrap();
    let opts = SetAttrOpts {
        recursive: true,
        owner: Some("policy-owner".into()),
        mode: Some(0o640),
        mtime: Some(12345),
        ..Default::default()
    };
    let operation = SerdeUtils::serialize(&command(
        &fs,
        curvine_fs_api::RpcCode::SetAttr,
        SetAttrRequest {
            path: "/wide".into(),
            opts: ProtoUtils::set_attr_opts_to_pb(opts),
        },
    )?)?;
    assert!(
        operation.len() < 4096,
        "subtree was expanded in the Raft input"
    );
    let result = replay(&fs, &operation)?;
    assert!(result.success, "{:?}", result);
    assert_eq!(fs.file_status("/wide")?.mtime, 12345);
    for id in ids {
        let record = fs.fs_dir.read().hsm_record(id)?.unwrap();
        assert_eq!(record.owner, "policy-owner");
        assert_ne!(
            record.mtime_ms, 12345,
            "recursive parent mtime leaked to child"
        );
        assert_eq!(record.version, before.version + 1);
    }
    assert_eq!(replay(&fs, &operation)?, result);
    Ok(())
}

#[test]
fn recursive_unlink_counts_all_aliases_without_deleting_outside_links() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    // Untracked multiply-linked files are supported by ordinary namespace
    // operations. Seed before HSM enrollment to avoid managed-link restrictions.
    fs.fs_dir.write().hsm_enabled = false;
    let survivor = file(&fs, "/outside")?;
    fs.mkdir("/tree/sub", true)?;
    fs.link("/outside", "/tree/a")?;
    fs.link("/outside", "/tree/sub/b")?;
    let retired = file(&fs, "/tree/last")?;
    fs.link("/tree/last", "/tree/sub/last-alias")?;
    fs.fs_dir.write().hsm_enabled = true;
    let operation = SerdeUtils::serialize(&command(
        &fs,
        curvine_fs_api::RpcCode::Delete,
        DeleteRequest {
            path: "/tree".into(),
            recursive: true,
        },
    )?)?;
    let result = replay(&fs, &operation)?;
    assert!(result.success, "{:?}", result);
    assert_eq!(
        DeleteResponse::decode(result.header.as_slice())?
            .res
            .unwrap()
            .inodes,
        1
    );
    assert!(!fs.exists("/tree")?);
    assert_eq!(fs.file_status("/outside")?.id, survivor);
    let dir = fs.fs_dir.read();
    assert_eq!(
        dir.store
            .get_inode(survivor, None)?
            .unwrap()
            .as_file_ref()?
            .nlink(),
        1
    );
    assert!(dir.store.get_inode(retired, None)?.is_none());
    drop(dir);
    file(&fs, "/tree/recreated")?;
    assert_eq!(replay(&fs, &operation)?, result);
    assert!(fs.exists("/tree/recreated")?);
    Ok(())
}

#[test]
fn captured_read_clock_guards_the_entire_command_without_partial_publication() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let id = file(&fs, "/protected/data")?;
    // Capture the compact command BEFORE a concurrent protected read commits.
    // Shared admission must still observe that reader at committed expansion.
    let bytes = rename(&fs, "/protected", "/moved", RenameFlags::new(0))?;
    let mut blocked: NamespaceCommand = SerdeUtils::deserialize(&bytes)?;
    blocked.protection =
        serde_json::from_value(serde_json::json!({"now":1000,"recovering":false}))?;
    let bytes = SerdeUtils::serialize(&blocked)?;
    {
        let dir = fs.fs_dir.read();
        let mut record = dir.hsm_record(id)?.unwrap();
        record.readers.insert("old-reader".into(), 2000);
        record.version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(
            format!("\x03hsm:record:{id}").as_bytes(),
            &serde_json::to_vec(&record)?,
        )?;
        batch.commit()?;
    }
    let result = replay(&fs, &bytes)?;
    assert!(!result.success);
    assert!(fs.exists("/protected/data")? && !fs.exists("/moved")?);
    assert_eq!(
        fs.fs_dir.read().hsm_record(id)?.unwrap().path,
        "/protected/data"
    );
    assert_eq!(replay(&fs, &bytes)?, result);
    let mut allowed = command(
        &fs,
        curvine_fs_api::RpcCode::Rename,
        RenameRequest {
            src: "/protected".into(),
            dst: "/moved".into(),
            flags: 0,
        },
    )?;
    allowed.protection =
        serde_json::from_value(serde_json::json!({"now":3000,"recovering":false}))?;
    assert!(replay(&fs, &SerdeUtils::serialize(&allowed)?)?.success);
    assert_eq!(fs.file_status("/moved/data")?.id, id);
    Ok(())
}

#[test]
fn ttl_candidates_recheck_metadata_identity_ownership_and_actual_children() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let status = fs.mkdir("/ttl", false)?;
    fs.set_attr(
        "/ttl",
        SetAttrOpts {
            mtime: Some(1),
            ttl_ms: Some(1),
            ttl_action: Some(curvine_model::TtlAction::Delete),
            ..Default::default()
        },
    )?;
    let observed = fs.fs_dir.read().store.get_inode(status.id, None)?.unwrap();
    let make_ttl = || -> CommonResult<Vec<u8>> {
        let mut input = command(
            &fs,
            curvine_fs_api::RpcCode::Delete,
            DeleteRequest {
                path: "/ttl".into(),
                recursive: true,
            },
        )?;
        input.ttl = Some(TtlCondition {
            inode: status.id,
            metadata: super::super::view::inode_fingerprint(&observed)?,
        });
        input.fingerprint = ttl_fingerprint(input.fingerprint, input.ttl.as_ref());
        SerdeUtils::serialize(&input)
    };
    let obsolete = make_ttl()?;
    file(&fs, "/ttl/child")?;
    {
        // Restore exactly the old parent metadata while keeping the new edge.
        // Child fingerprints intentionally do not hash entire directories.
        let dir = fs.fs_dir.write();
        let path =
            crate::master::meta::inode::InodePath::resolve(dir.root_ptr(), "/ttl", &dir.store)?;
        let pointer = path.get_last_inode().unwrap();
        let old = std::mem::replace(pointer.as_mut(), observed.clone_metadata());
        pointer.as_mut().adopt_directory_children(old);
        let mut batch = dir.get_rocks_store().new_batch();
        batch.write_inode(&observed)?;
        batch.commit()?;
        assert_eq!(
            super::super::view::inode_fingerprint(pointer.as_ref())?,
            super::super::view::inode_fingerprint(&observed)?
        );
    }
    assert!(replay(&fs, &obsolete)?.success);
    assert!(
        fs.exists("/ttl/child")?,
        "TTL recursively deleted a directory populated after screening"
    );
    fs.delete("/ttl/child", false)?;
    let valid = make_ttl()?;
    // Child removal updates metadata; restore the observation for a valid
    // empty-dir operation, then verify the committed delete can finish.
    {
        let dir = fs.fs_dir.write();
        let path =
            crate::master::meta::inode::InodePath::resolve(dir.root_ptr(), "/ttl", &dir.store)?;
        let pointer = path.get_last_inode().unwrap();
        let old = std::mem::replace(pointer.as_mut(), observed.clone_metadata());
        pointer.as_mut().adopt_directory_children(old);
        let mut batch = dir.get_rocks_store().new_batch();
        batch.write_inode(&observed)?;
        batch.commit()?;
    }
    assert!(replay(&fs, &valid)?.success);
    assert!(!fs.exists("/ttl")?);
    fs.mkdir("/ttl", false)?;
    assert!(replay(&fs, &make_ttl()?)?.success);
    assert!(
        fs.exists("/ttl")?,
        "old inode candidate deleted a replacement directory"
    );
    Ok(())
}

#[test]
fn recursive_delete_removes_wide_nested_tree_without_reusing_detached_pointers() -> CommonResult<()>
{
    let (_root, fs) = fixture()?;
    let mut ids = vec![];
    for directory in 0..128 {
        for file_no in 0..4 {
            ids.push(file(
                &fs,
                &format!("/wide-delete/d-{directory}/f-{file_no}"),
            )?);
        }
    }
    let operation = SerdeUtils::serialize(&command(
        &fs,
        curvine_fs_api::RpcCode::Delete,
        DeleteRequest {
            path: "/wide-delete".into(),
            recursive: true,
        },
    )?)?;
    let result = replay(&fs, &operation)?;
    assert!(result.success, "{:?}", result);
    assert_eq!(
        DeleteResponse::decode(result.header.as_slice())?
            .res
            .unwrap()
            .inodes,
        512
    );
    assert!(!fs.exists("/wide-delete")?);
    for id in ids {
        assert!(fs.fs_dir.read().hsm_record(id)?.is_none());
    }
    file(&fs, "/wide-delete/recreated")?;
    assert_eq!(replay(&fs, &operation)?, result);
    assert!(fs.exists("/wide-delete/recreated")?);
    Ok(())
}

#[test]
fn namespace_attribute_original_results_are_identical_across_fresh_map_encodings(
) -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    file(&fs, "/canonical-attributes")?;
    let time = LocalTime::mills() as i64 + 10_000;
    let mut first = None;
    for _ in 0..16 {
        let opts = SetAttrOpts {
            recursive: true,
            mtime: Some(1234),
            atime: Some(1234),
            add_x_attr: (0..16)
                .map(|i| (format!("user.key-{i}"), vec![i]))
                .collect(),
            ..Default::default()
        };
        let mut input = command(
            &fs,
            curvine_fs_api::RpcCode::SetAttr,
            SetAttrRequest {
                path: "/canonical-attributes".into(),
                opts: ProtoUtils::set_attr_opts_to_pb(opts),
            },
        )?;
        input.time = time;
        let response = replay(&fs, &SerdeUtils::serialize(&input)?)?;
        assert!(response.success);
        let status = curvine_proto::SetAttrResponse::decode(response.header.as_slice())?.status;
        assert_eq!(status.x_attr.len(), 16);
        if let Some(original) = &first {
            assert_eq!(
                &response.header, original,
                "replicated attribute response depends on a process-local HashMap order"
            );
        } else {
            first = Some(response.header);
        }
    }
    Ok(())
}
