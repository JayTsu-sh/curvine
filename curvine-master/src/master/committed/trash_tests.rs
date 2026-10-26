use super::*;
use crate::master::{
    journal::JournalSystem,
    meta::{BlockMeta, InodeId},
    Master,
};
use curvine_fs_api::RpcCode;
use curvine_model::{hsm::native::Namespace, BlockLocation, SetAttrOpts};
use curvine_proto::{DeleteRequest, EmptyTrashRequest, RestoreTrashRequest};

fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem)> {
    Master::init_test_metrics();
    let root = tempfile::tempdir()?;
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
    conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
    let mut fs = JournalSystem::fs_only_for_test(&conf)?;
    std::sync::Arc::make_mut(&mut fs.conf).trash_enabled = true;
    let ns = Namespace {
        ordinary_format: 2,
        root_inode: 9999,
        storage: "s".into(),
        incarnation: "n".into(),
        epoch: 1,
    };
    {
        let mut dir = fs.fs_dir.write();
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(b"\x03hsm:namespace:v1", &serde_json::to_vec(&ns)?)?;
        batch.commit()?;
        dir.hsm_enabled = true;
    }
    Ok((root, fs))
}
fn file(fs: &MasterFilesystem, path: &str) -> CommonResult<i64> {
    let id = fs.create(path, true)?.id;
    fs.complete_file(path, Some(id), 0, vec![], "", false, None)?;
    Ok(id)
}
fn command(code: RpcCode, header: impl Message, operation: Operation) -> TrashCommand {
    let time = LocalTime::mills() as i64;
    let header = header.encode_to_vec();
    TrashCommand {
        version: VERSION,
        identity: RequestIdentity {
            incarnation: "n".into(),
            session: format!("{time:016x}{}", curvine_model::hsm::native::token()),
            sequence: 1,
            acknowledged: 0,
        },
        fingerprint: fingerprint(code as u32, &header, &operation),
        time,
        protection: serde_json::from_value(serde_json::json!({"now":1000,"recovering":false}))
            .unwrap(),
        code: code as u32,
        header,
        operation,
        trash_inode: None,
        admission_error: None,
    }
}
fn replay(fs: &MasterFilesystem, bytes: &[u8]) -> CommonResult<NativeMutationResponse> {
    apply(&mut fs.fs_dir.write(), SerdeUtils::deserialize(bytes)?)
}
fn soft(
    fs: &MasterFilesystem,
    path: &str,
) -> CommonResult<(String, Vec<u8>, NativeMutationResponse)> {
    let mut operation = command(
        RpcCode::Delete,
        DeleteRequest {
            path: path.into(),
            recursive: true,
            skip_trash: Some(false),
        },
        Operation::Delete { soft: true },
    );
    operation.trash_inode = Some(fs.fs_dir.read().inode_id.next()?);
    let bytes = SerdeUtils::serialize(&operation)?;
    let response = replay(fs, &bytes)?;
    assert!(response.success, "{response:?}");
    assert!(DeleteResponse::decode(response.header.as_slice())?.trashed());
    let entry = fs
        .list_trash()?
        .into_iter()
        .find(|entry| entry.origin == path)
        .unwrap();
    Ok((entry.path, bytes, response))
}
fn block(fs: &MasterFilesystem, id: i64) -> CommonResult<i64> {
    let dir = fs.fs_dir.read();
    let mut inode = dir.store.get_inode(id, None)?.unwrap();
    let block = InodeId::create_block_id(id, 0)?;
    let file = inode.as_file_mut()?;
    file.len = 4096;
    file.blocks = vec![BlockMeta::new(block, 4096)];
    let mut batch = dir.get_rocks_store().new_batch();
    batch.write_inode(&inode)?;
    batch.add_location(block, &BlockLocation::with_id(7))?;
    batch.commit()?;
    dir.hsm_track_file(&dir.get_inode_path(id)?, inode.as_file_ref()?)?;
    Ok(block)
}

#[test]
fn soft_delete_and_restore_are_atomic_replayable_and_refresh_descendants() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let id = file(&fs, "/tree/child")?;
    let old = fs.fs_dir.read().hsm_record(id)?.unwrap();
    let (path, bytes, original) = soft(&fs, "/tree")?;
    assert!(!fs.exists("/tree")?);
    let record = fs.fs_dir.read().hsm_record(id)?.unwrap();
    assert_eq!(record.path, format!("{path}/child"));
    assert_eq!(record.state.data, old.state.data);
    assert_eq!(record.version, old.version + 1);
    file(&fs, "/tree/new")?;
    assert_eq!(replay(&fs, &bytes)?, original);
    assert!(fs.exists("/tree/new")?);
    let restore = SerdeUtils::serialize(&command(
        RpcCode::RestoreTrash,
        RestoreTrashRequest {
            path,
            dst: Some("/restored".into()),
            overwrite: Some(false),
        },
        Operation::Restore,
    ))?;
    let restored = replay(&fs, &restore)?;
    assert!(restored.success, "{restored:?}");
    assert_eq!(fs.file_status("/restored/child")?.id, id);
    assert_eq!(
        fs.fs_dir.read().hsm_record(id)?.unwrap().path,
        "/restored/child"
    );
    let parent = fs.file_status("/restored")?.id;
    assert!(trash::decode_origin(
        fs.fs_dir
            .read()
            .store
            .get_inode(parent, None)?
            .unwrap()
            .x_attr()?
    )
    .is_none());
    let checkpoint = fs.fs_dir.read().create_checkpoint(901)?;
    fs.fs_dir.write().restore(&checkpoint, 0)?;
    assert_eq!(replay(&fs, &restore)?, restored);
    assert_eq!(replay(&fs, &bytes)?, original);
    assert!(fs.exists("/tree/new")? && fs.exists("/restored/child")?);
    Ok(())
}

#[test]
fn protected_empty_fails_atomically_and_successful_retry_does_not_purge_new_entries(
) -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let first = file(&fs, "/first")?;
    let protected = file(&fs, "/protected")?;
    soft(&fs, "/first")?;
    soft(&fs, "/protected")?;
    {
        let dir = fs.fs_dir.read();
        let mut record = dir.hsm_record(protected)?.unwrap();
        record.readers.insert("reader".into(), 2000);
        record.version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put_record(protected, &record)?;
        batch.commit()?;
    }
    let empty = SerdeUtils::serialize(&command(
        RpcCode::EmptyTrash,
        EmptyTrashRequest {},
        Operation::Empty,
    ))?;
    let rejected = replay(&fs, &empty)?;
    assert!(!rejected.success);
    assert_eq!(fs.list_trash()?.len(), 2);
    assert!(fs.fs_dir.read().store.get_inode(first, None)?.is_some());
    {
        let dir = fs.fs_dir.read();
        let mut record = dir.hsm_record(protected)?.unwrap();
        record.readers.clear();
        record.version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put_record(protected, &record)?;
        batch.commit()?;
    }
    assert_eq!(replay(&fs, &empty)?, rejected);
    let empty = SerdeUtils::serialize(&command(
        RpcCode::EmptyTrash,
        EmptyTrashRequest {},
        Operation::Empty,
    ))?;
    let applied = replay(&fs, &empty)?;
    assert!(applied.success, "{applied:?}");
    assert_eq!(
        curvine_proto::EmptyTrashResponse::decode(applied.header.as_slice())?
            .res
            .inodes,
        2
    );
    file(&fs, "/new")?;
    soft(&fs, "/new")?;
    assert_eq!(replay(&fs, &empty)?, applied);
    assert_eq!(fs.list_trash()?.len(), 1);
    Ok(())
}

#[test]
fn overwrite_restore_and_expiry_persist_native_gc_without_retiring_soft_deleted_blocks(
) -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let source = file(&fs, "/source")?;
    let target = file(&fs, "/target")?;
    let source_block = block(&fs, source)?;
    let target_block = block(&fs, target)?;
    let (path, _, _) = soft(&fs, "/source")?;
    assert!(!super::super::garbage::has_retirement(
        &fs.fs_dir.read(),
        source_block
    )?);
    let restore = SerdeUtils::serialize(&command(
        RpcCode::RestoreTrash,
        RestoreTrashRequest {
            path,
            dst: Some("/target".into()),
            overwrite: Some(true),
        },
        Operation::Restore,
    ))?;
    let restored = replay(&fs, &restore)?;
    assert!(restored.success, "{restored:?}");
    assert!(super::super::garbage::has_retirement(
        &fs.fs_dir.read(),
        target_block
    )?);
    assert!(!super::super::garbage::has_retirement(
        &fs.fs_dir.read(),
        source_block
    )?);
    assert_eq!(fs.file_status("/target")?.id, source);
    assert_eq!(replay(&fs, &restore)?, restored);
    let (path, _, _) = soft(&fs, "/target")?;
    let entry = fs
        .list_trash()?
        .into_iter()
        .find(|entry| entry.path == path)
        .unwrap();
    let purge = SerdeUtils::serialize(&command(
        RpcCode::Delete,
        DeleteRequest {
            path: path.clone(),
            recursive: true,
            skip_trash: Some(true),
        },
        Operation::Expired {
            inode: source,
            delete_ms: entry.delete_ms,
            cutoff_ms: entry.delete_ms,
        },
    ))?;
    let purged = replay(&fs, &purge)?;
    assert!(purged.success, "{purged:?}");
    assert!(!fs.exists(&path)?);
    assert!(super::super::garbage::has_retirement(
        &fs.fs_dir.read(),
        source_block
    )?);
    assert_eq!(replay(&fs, &purge)?, purged);
    Ok(())
}

#[test]
fn native_admission_rejects_reserved_paths_internal_metadata_flags_and_stale_by_id_paths(
) -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let id = file(&fs, "/file")?;
    soft(&fs, "/file")?;
    let dir = fs.fs_dir.read();
    assert!(NativeInput::Mkdir(curvine_proto::MkdirRequest {
        path: "/.Trash/forged".into(),
        ..Default::default()
    })
    .validate_client(&dir)
    .is_err());
    for flags in [RenameFlags::RESTORE, RenameFlags::TRASH] {
        assert!(NativeInput::Rename(curvine_proto::RenameRequest {
            src: "/a".into(),
            dst: "/b".into(),
            flags: flags.value()
        })
        .validate_client(&dir)
        .is_err());
    }
    let mut opts = SetAttrOpts::default();
    opts.add_x_attr
        .insert(trash::TRASH_XATTR_ORIGIN.into(), b"/forged".to_vec());
    assert!(NativeInput::SetAttr(curvine_proto::SetAttrRequest {
        path: "/public".into(),
        opts: ProtoUtils::set_attr_opts_to_pb(opts.clone())
    })
    .validate_client(&dir)
    .is_err());
    assert!(NativeInput::Complete(curvine_proto::CompleteFileRequest {
        path: "/public".into(),
        set_attr_opts: Some(ProtoUtils::set_attr_opts_to_pb(opts)),
        ..Default::default()
    })
    .validate_client(&dir)
    .is_err());
    assert!(NativeInput::Resize(curvine_proto::FileResizeRequest {
        path: "/old-public-name".into(),
        inode_id: Some(id),
        ..Default::default()
    })
    .validate_client(&dir)
    .is_err());
    Ok(())
}

#[test]
fn frozen_namespace_v5_delete_remains_permanent_with_trash_enabled() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    file(&fs, "/old-version/file")?;
    let new = command(
        RpcCode::Delete,
        DeleteRequest {
            path: "/old-version".into(),
            recursive: true,
            skip_trash: None,
        },
        Operation::Delete { soft: true },
    );
    let old = super::super::namespace::NamespaceCommand {
        version: super::super::namespace::VERSION,
        identity: new.identity,
        fingerprint: new.fingerprint,
        time: new.time,
        protection: new.protection,
        code: new.code,
        header: new.header,
        admission_error: None,
        ttl: None,
    };
    assert!(super::super::namespace::apply(&mut fs.fs_dir.write(), old)?.success);
    assert!(!fs.exists("/old-version")?);
    assert!(fs.list_trash()?.is_empty());
    Ok(())
}

#[test]
fn trash_root_uses_captured_inode_even_when_local_reservation_counter_has_advanced(
) -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    file(&fs, "/file")?;
    let mut input = command(
        RpcCode::Delete,
        DeleteRequest {
            path: "/file".into(),
            recursive: false,
            skip_trash: Some(false),
        },
        Operation::Delete { soft: true },
    );
    let captured = fs.fs_dir.read().inode_id.next()?;
    input.trash_inode = Some(captured);
    for _ in 0..20 {
        fs.fs_dir.read().inode_id.next()?;
    }
    let result = replay(&fs, &SerdeUtils::serialize(&input)?)?;
    assert!(result.success, "{result:?}");
    assert_eq!(fs.file_status(trash::TRASH_ROOT)?.id, captured);
    Ok(())
}

#[test]
fn expiry_receipt_distinguishes_empty_directory_purge_from_stale_noop() -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let id = fs.mkdir("/empty", false)?.id;
    let (path, _, _) = soft(&fs, "/empty")?;
    let entry = fs
        .list_trash()?
        .into_iter()
        .find(|entry| entry.path == path)
        .unwrap();
    let make = || {
        command(
            RpcCode::Delete,
            DeleteRequest {
                path: path.clone(),
                recursive: true,
                skip_trash: Some(true),
            },
            Operation::Expired {
                inode: id,
                delete_ms: entry.delete_ms,
                cutoff_ms: entry.delete_ms,
            },
        )
    };
    let bytes = SerdeUtils::serialize(&make())?;
    let purged = replay(&fs, &bytes)?;
    assert!(purged.success, "{purged:?}");
    let result = DeleteResponse::decode(purged.header.as_slice())?
        .res
        .unwrap();
    assert_eq!(result.inodes, 0);
    assert!(!fs.exists(&path)?);
    assert_eq!(replay(&fs, &bytes)?, purged);
    let stale = replay(&fs, &SerdeUtils::serialize(&make())?)?;
    assert!(stale.success, "{stale:?}");
    assert!(DeleteResponse::decode(stale.header.as_slice())?
        .res
        .is_none());
    Ok(())
}

#[test]
fn cached_success_and_failure_remain_read_only_during_recovery_but_new_trash_is_refused(
) -> CommonResult<()> {
    let (_root, fs) = fixture()?;
    let id = file(&fs, "/file")?;
    let (_, success_bytes, success) = soft(&fs, "/file")?;
    {
        let dir = fs.fs_dir.read();
        let mut record = dir.hsm_record(id)?.unwrap();
        record.readers.insert("protected".into(), 2000);
        record.version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put_record(id, &record)?;
        batch.commit()?;
    }
    let failure_input = command(RpcCode::EmptyTrash, EmptyTrashRequest {}, Operation::Empty);
    let failure_bytes = SerdeUtils::serialize(&failure_input)?;
    let failure = replay(&fs, &failure_bytes)?;
    assert!(!failure.success);
    fs.fs_dir.write().restore_from_rocksdb()?;
    assert!(fs.fs_dir.read().hsm_read_time()?.recovering());
    let make_request = |input: &TrashCommand| NativeMutationRequest {
        version: 1,
        incarnation: input.identity.incarnation.clone(),
        session: input.identity.session.clone(),
        sequence: input.identity.sequence,
        acknowledged: input.identity.acknowledged,
        code: input.code,
        header: input.header.clone(),
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        // The live coordinator qualifies ReadIndex before entering this
        // helper. Here isolate its cached/new admission decision under the
        // same exclusive owner, with an actual reset recovery clock.
        let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
        let _owner = gate.write().await;
        for (bytes, expected) in [(&success_bytes, &success), (&failure_bytes, &failure)] {
            let input: TrashCommand = SerdeUtils::deserialize(bytes)?;
            let request = make_request(&input);
            let result = fs
                .trash_mutate(&request, NativeInput::parse(&request)?, None, 0)
                .await?;
            assert_eq!(&result, expected);
        }
        let new_input = command(RpcCode::EmptyTrash, EmptyTrashRequest {}, Operation::Empty);
        let request = make_request(&new_input);
        let error = fs
            .trash_mutate(&request, NativeInput::parse(&request)?, None, 0)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("recovery protects prior read grants"),
            "{error}"
        );
        assert_eq!(fs.list_trash()?.len(), 1);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })?;
    Ok(())
}

#[test]
fn restore_response_keeps_user_attribute_encoding_identical_across_fresh_maps() -> CommonResult<()>
{
    let mut original = None;
    for rotation in 0..32 {
        let mut status = curvine_proto::FileStatusProto {
            id: 42,
            path: "/restored".into(),
            name: "restored".into(),
            ..Default::default()
        };
        for offset in 0..16 {
            let key = (offset + rotation) % 16;
            status.x_attr.insert(
                format!("user.key-{key:02}"),
                format!("value-{key}").into_bytes(),
            );
        }
        let expected = status.clone();
        let encoded = restore_reply("/restored".into(), status);
        let decoded = curvine_proto::RestoreTrashResponse::decode(encoded.as_slice())?;
        assert_eq!(decoded.dst.as_deref(), Some("/restored"));
        assert_eq!(decoded.status.unwrap(), expected);
        if let Some(original) = &original {
            assert_eq!(
                &encoded, original,
                "restore receipt changed with HashMap ordering"
            );
        } else {
            original = Some(encoded);
        }
    }
    Ok(())
}
