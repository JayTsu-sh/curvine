// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Creation publication and durable original results. This is deliberately
//! narrower than native activation: other mutation families must gain owned
//! preparation, atomic writes and deferred effects before transport switches.
use super::session::{Decision, Session};
use super::view::{inode_fingerprint, PreparedEdits};
use crate::master::meta::{
    inode::{InodePath, InodePtr, InodeView, ROOT_INODE_ID},
    store::RocksInodeStore,
    FsDir,
};
use curvine_core_error::{err_box, CommonResult, ErrorExt};
use curvine_error::FsError;
use curvine_model::hsm::native::Namespace;
use curvine_rocksdb::RocksUtils;
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
pub(super) struct RequestIdentity {
    pub(super) incarnation: String,
    pub(super) session: String,
    pub(super) sequence: u64,
    pub(super) acknowledged: u64,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct NativeReply {
    pub(super) code: i8,
    pub(super) success: bool,
    pub(super) header: Vec<u8>,
    pub(super) body: Vec<u8>,
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum ResultKind {
    Applied,
    Conflict,
    IdentityMismatch,
    Fenced,
    Uncertain,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct NativeResult {
    pub(super) kind: ResultKind,
    pub(super) reply: NativeReply,
}
#[derive(Serialize, Deserialize)]
pub(super) struct NativeCommand {
    pub(super) identity: RequestIdentity,
    pub(super) fingerprint: [u8; 32],
    pub(super) time: i64,
    pub(super) edits: PreparedEdits,
    pub(super) reply: NativeReply,
}
#[derive(Serialize, Deserialize)]
struct StoredOutcome {
    format: u32,
    fingerprint: [u8; 32],
    result: NativeResult,
}
fn rejected(command: &NativeCommand, kind: ResultKind, message: &str) -> NativeResult {
    NativeResult {
        kind,
        reply: NativeReply {
            code: command.reply.code,
            success: false,
            header: vec![],
            body: FsError::common(message).encode().to_vec(),
        },
    }
}
fn edge(fs: &FsDir, parent: i64, name: &str) -> CommonResult<Option<i64>> {
    fs.get_rocks_store()
        .db
        .get_cf(
            RocksInodeStore::CF_EDGES,
            RocksUtils::i64_str_to_bytes(parent, name),
        )?
        .map(|value| RocksUtils::i64_from_bytes(&value))
        .transpose()
}
fn current_inode(fs: &FsDir, id: i64) -> CommonResult<Option<InodeView>> {
    if id == ROOT_INODE_ID {
        return Ok(Some(fs.root_dir.clone_metadata()));
    }
    fs.store.get_inode(id, None)
}

pub(super) fn apply_creation(
    fs: &mut FsDir,
    command: &NativeCommand,
) -> CommonResult<NativeResult> {
    let namespace: Namespace = serde_json::from_slice(
        &fs.get_rocks_store()
            .hsm_get(b"\x03hsm:namespace:v1")?
            .ok_or("native commit has no namespace identity")?,
    )?;
    if namespace.incarnation != command.identity.incarnation {
        return Ok(rejected(
            command,
            ResultKind::Fenced,
            "native request belongs to another namespace incarnation",
        ));
    }
    let mut session = Session::prepare(
        fs.get_rocks_store(),
        &command.identity.session,
        command.identity.sequence,
        command.identity.acknowledged,
        command.time,
    )?;
    let early = match &session.decision {
        Decision::Cached(value) => {
            let old: StoredOutcome = SerdeUtils::deserialize(&value)?;
            if old.format != 1 {
                return err_box!("unsupported native result format");
            }
            Some(if old.fingerprint == command.fingerprint {
                old.result
            } else {
                rejected(
                    command,
                    ResultKind::IdentityMismatch,
                    "native request identity rebound",
                )
            })
        }
        Decision::Fenced => Some(rejected(
            command,
            ResultKind::Fenced,
            "native sequence/session retired",
        )),
        Decision::Full => Some(rejected(
            command,
            ResultKind::Uncertain,
            "native retry window full; retain original identity",
        )),
        Decision::Admit => None,
    };
    if let Some(result) = early {
        let mut batch = fs.get_rocks_store().new_batch();
        session.write(&mut batch)?;
        batch.commit()?;
        return Ok(result);
    }
    let edits = &command.edits;
    if edits.last_inode < 0
        || edits.last_inode > crate::master::meta::InodeId::ID_MASK
        || edits.inodes.keys().any(|id| {
            *id < ROOT_INODE_ID || *id > edits.last_inode || !edits.inode_checks.contains_key(id)
        })
        || edits.edges.keys().any(|(parent, _)| {
            !edits.inodes.contains_key(parent) || !edits.inode_checks.contains_key(parent)
        })
    {
        return err_box!("native creation has incomplete checks or allocation history");
    }
    // Until deletion/move/block/HSM preparation is connected, this publisher
    // accepts new directories/files and existing parent header updates only.
    if edits.inodes.iter().any(|(id, inode)| {
        inode.as_ref().is_none_or(|inode| {
            inode.id() != *id
                || inode.child_len() != 0
                || inode.is_file_entry()
                || !inode.is_dir()
                    && (!inode.is_file() || edits.inode_checks.get(id) != Some(&None))
        })
    }) || edits
        .edges
        .iter()
        .any(|(key, value)| value.is_none() || edits.edge_checks.get(key) != Some(&None))
    {
        return err_box!("unsupported native creation effect");
    }
    let mut valid = true;
    for (id, expected) in &edits.inode_checks {
        let current = current_inode(fs, *id)?;
        if current.as_ref().map(inode_fingerprint).transpose()? != *expected {
            valid = false;
        }
    }
    for ((parent, name), expected) in &edits.edge_checks {
        if edge(fs, *parent, name)? != *expected {
            valid = false;
        }
    }
    let result = if valid {
        NativeResult {
            kind: ResultKind::Applied,
            reply: command.reply.clone(),
        }
    } else {
        rejected(
            command,
            ResultKind::Conflict,
            "native metadata observation changed before commit",
        )
    };
    // Resolve live directory pointers before persisting new parent IDs/names.
    // They remain inside this apply call, under FsDir's exclusive lock.
    let mut directories: BTreeMap<i64, InodePtr> = BTreeMap::new();
    if valid {
        for (id, expected) in &edits.inode_checks {
            if expected.is_some() && current_inode(fs, *id)?.is_some_and(|inode| inode.is_dir()) {
                let path = fs.get_inode_path(*id)?;
                let input = InodePath::resolve(fs.root_ptr(), path, &fs.store)?;
                directories.insert(
                    *id,
                    input
                        .get_last_inode()
                        .ok_or("live native directory is missing")?,
                );
            }
        }
        for ((parent, name), child) in &edits.edges {
            let child = child.unwrap();
            let inode = edits
                .inodes
                .get(&child)
                .and_then(Option::as_ref)
                .ok_or("native creation has no child metadata")?;
            if edits.inode_checks.get(&child) != Some(&None)
                || inode.name() != name
                || (if inode.is_dir() {
                    inode.as_dir_ref()?.parent_id
                } else {
                    inode.as_file_ref()?.parent_id
                }) != *parent
            {
                return err_box!("invalid native creation edge/identity");
            }
        }
    }
    let floor = fs
        .get_rocks_store()
        .native_inode_floor()?
        .unwrap_or(ROOT_INODE_ID)
        .max(edits.last_inode);
    let stored = StoredOutcome {
        format: 1,
        fingerprint: command.fingerprint,
        result: result.clone(),
    };
    if !session.record(SerdeUtils::serialize(&stored)?)? {
        let mut batch = fs.get_rocks_store().new_batch();
        session.write(&mut batch)?;
        batch.commit()?;
        return Ok(rejected(
            command,
            ResultKind::Uncertain,
            "native result byte budget full; retain original identity",
        ));
    }
    let mut batch = fs.get_rocks_store().new_batch();
    if valid {
        for inode in edits.inodes.values().flatten() {
            batch.write_inode(inode)?;
        }
        for ((parent, name), child) in &edits.edges {
            batch.add_child(*parent, name, child.unwrap())?;
        }
    }
    session.write(&mut batch)?;
    batch.hsm_put(
        RocksInodeStore::NATIVE_INODE_FLOOR,
        &SerdeUtils::serialize(&floor)?,
    )?;
    batch.commit()?;
    fs.update_last_inode_id(floor)?;
    if valid {
        for (id, inode) in &edits.inodes {
            if let Some(pointer) = directories.get(id) {
                let old =
                    std::mem::replace(pointer.as_mut(), inode.as_ref().unwrap().clone_metadata());
                pointer.as_mut().adopt_directory_children(old);
            }
        }
        // New IDs are allocated parent first. Do not infer that all future
        // operations can use this creation-specific publication order.
        for ((parent, _name), child) in &edits.edges {
            let parent = directories
                .get(parent)
                .ok_or("native creation parent is not published")?;
            let inode = edits
                .inodes
                .get(&child.unwrap())
                .and_then(Option::as_ref)
                .unwrap()
                .clone_metadata();
            let pointer = parent.as_mut().add_child(inode)?;
            directories.insert(child.unwrap(), pointer);
        }
        for (id, expected) in &edits.inode_checks {
            if expected.is_none() && edits.inodes.contains_key(id) {
                if edits
                    .inodes
                    .get(id)
                    .and_then(Option::as_ref)
                    .unwrap()
                    .is_dir()
                {
                    fs.store.fs_stats.increment_dir_count();
                } else {
                    fs.store.fs_stats.increment_file_count();
                }
                fs.get_ttl_bucket_list().add(
                    directories
                        .get(id)
                        .ok_or("new directory publication missing")?
                        .as_ref(),
                );
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::super::{commands, view::BatchView};
    use super::*;
    use crate::master::{fs::MasterFilesystem, journal::JournalSystem};
    use curvine_config::ClusterConf;
    use curvine_fs_api::RpcCode;
    use curvine_model::{MkdirOpts, RenameFlags};

    fn configuration(root: &std::path::Path) -> ClusterConf {
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.join("journal").to_string_lossy().into_owned();
        conf
    }
    fn seed_namespace(fs: &MasterFilesystem) -> CommonResult<()> {
        let namespace = Namespace {
            ordinary_format: 2,
            root_inode: ROOT_INODE_ID,
            storage: "source".into(),
            incarnation: "incarnation".into(),
            epoch: 1,
        };
        let dir = fs.fs_dir.read();
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(b"\x03hsm:namespace:v1", &serde_json::to_vec(&namespace)?)?;
        batch.commit()
    }
    fn prepare(fs: &MasterFilesystem, path: &str) -> CommonResult<NativeCommand> {
        let dir = fs.fs_dir.read();
        let mut view = BatchView::new(&dir, 1024 * 1024);
        let prepared = commands::mkdir(&mut view, path, MkdirOpts::with_create(true), 1000)?;
        Ok(NativeCommand {
            identity: RequestIdentity {
                incarnation: "incarnation".into(),
                session: format!("{:016x}{}", 1000, "a".repeat(32)),
                sequence: 1,
                acknowledged: 0,
            },
            fingerprint: [1; 32],
            time: 1000,
            edits: prepared.edits,
            reply: NativeReply {
                code: RpcCode::Mkdir.into(),
                success: true,
                header: prepared.response,
                body: vec![],
            },
        })
    }

    #[test]
    fn new_file_creation_publishes_identically_and_acknowledgement_prevents_reexecution(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let a = tempfile::tempdir()?;
        let b = tempfile::tempdir()?;
        let source = JournalSystem::fs_only_for_test(&configuration(a.path()))?;
        let follower = JournalSystem::fs_only_for_test(&configuration(b.path()))?;
        seed_namespace(&source)?;
        seed_namespace(&follower)?;
        let command = {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::create(
                &mut view,
                "/files/nested/payload",
                curvine_model::CreateFileOpts::with_create(true),
                7777,
            )?;
            assert!(dir.root_dir.get_child("files").is_none());
            NativeCommand {
                identity: RequestIdentity {
                    incarnation: "incarnation".into(),
                    session: format!("{:016x}{}", 1000, "c".repeat(32)),
                    sequence: 1,
                    acknowledged: 0,
                },
                fingerprint: [9; 32],
                time: 7777,
                edits: prepared.edits,
                reply: NativeReply {
                    code: RpcCode::CreateFile.into(),
                    success: true,
                    header: prepared.response,
                    body: vec![],
                },
            }
        };
        for replica in [&source, &follower] {
            let result = apply_creation(&mut replica.fs_dir.write(), &command)?;
            assert!(result.kind == ResultKind::Applied && result.reply == command.reply);
            let status = replica.file_status("/files/nested/payload")?;
            assert!(!status.is_dir && !status.is_complete);
            assert_eq!(status.mtime, 7777);
        }
        source.rename("/files", "/renamed", RenameFlags::new(0))?;
        assert!(apply_creation(&mut source.fs_dir.write(), &command)?.reply == command.reply);
        let mut next = prepare(&source, "/next")?;
        next.identity.session = command.identity.session.clone();
        next.identity.sequence = 2;
        next.identity.acknowledged = 1;
        assert!(apply_creation(&mut source.fs_dir.write(), &next)?.kind == ResultKind::Applied);
        assert!(apply_creation(&mut source.fs_dir.write(), &command)?.kind == ResultKind::Fenced);
        assert!(!source.exists("/files")?);
        Ok(())
    }

    #[test]
    fn creation_applies_on_both_roles_and_retries_return_original_bytes_after_rename_and_reopen(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let a = tempfile::tempdir()?;
        let b = tempfile::tempdir()?;
        let mut conf = configuration(a.path());
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let follower = JournalSystem::fs_only_for_test(&configuration(b.path()))?;
        seed_namespace(&fs)?;
        seed_namespace(&follower)?;
        let mut command = prepare(&fs, "/a/b")?;
        for replica in [&fs, &follower] {
            let before = replica.fs_dir.read().get_file_counts().0;
            let result = apply_creation(&mut replica.fs_dir.write(), &command)?;
            assert!(result.kind == ResultKind::Applied && result.reply == command.reply);
            assert_eq!(replica.file_status("/a/b")?.mtime, 1000);
            assert_eq!(replica.fs_dir.read().get_file_counts().0, before + 2);
        }
        fs.rename("/a", "/later", RenameFlags::new(0))?;
        let result = apply_creation(&mut fs.fs_dir.write(), &command)?;
        assert!(result.kind == ResultKind::Applied && result.reply == command.reply);
        assert!(!fs.exists("/a")?);
        assert!(fs.exists("/later/b")?);
        command.fingerprint = [2; 32];
        assert!(
            apply_creation(&mut fs.fs_dir.write(), &command)?.kind == ResultKind::IdentityMismatch
        );
        command.fingerprint = [1; 32];
        fs.fs_dir.read().get_rocks_store().db.flush(true)?;
        drop(fs);
        conf.format_master = false;
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        fs.fs_dir.write().restore_from_rocksdb()?;
        assert!(fs.exists("/later/b")?);
        let result = apply_creation(&mut fs.fs_dir.write(), &command)?;
        assert!(result.kind == ResultKind::Applied && result.reply == command.reply);
        command.identity.incarnation = "another-history".into();
        assert!(apply_creation(&mut fs.fs_dir.write(), &command)?.kind == ResultKind::Fenced);
        Ok(())
    }

    #[test]
    fn replay_discards_untrusted_native_tail_and_allocation_floor_survives_deleted_high_ids(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let command = prepare(&fs, "/a/b")?;
        assert!(apply_creation(&mut fs.fs_dir.write(), &command)?.kind == ResultKind::Applied);
        let floor = fs.fs_dir.read().last_inode_id();
        // Simulate a cold bootstrap: native rows from the last process are
        // discarded before the namespace and this committed entry replay.
        {
            let mut dir = fs.fs_dir.write();
            dir.get_rocks_store().reset_committed_projection()?;
            dir.root_dir = FsDir::create_root();
            dir.inode_id = crate::master::meta::InodeId::new();
        }
        seed_namespace(&fs)?;
        assert!(apply_creation(&mut fs.fs_dir.write(), &command)?.kind == ResultKind::Applied);
        assert!(fs.exists("/a/b")?);
        fs.delete("/a", true)?;
        fs.fs_dir.write().restore_from_rocksdb()?;
        assert!(!fs.exists("/a")?);
        assert!(fs.fs_dir.read().last_inode_id() >= floor);
        Ok(())
    }
}
