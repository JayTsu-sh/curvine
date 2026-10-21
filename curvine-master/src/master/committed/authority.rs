// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Owned native publication and durable original results. This is deliberately
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

pub(super) enum Prior {
    Cached(NativeResult),
    Rejected,
    New,
}
pub(super) fn prior(
    fs: &FsDir,
    identity: &RequestIdentity,
    fingerprint: [u8; 32],
    code: i8,
    time: i64,
) -> CommonResult<Prior> {
    let session = Session::prepare(
        fs.get_rocks_store(),
        &identity.session,
        identity.sequence,
        identity.acknowledged,
        time,
    )?;
    Ok(match session.decision {
        Decision::Cached(bytes) => {
            let old: StoredOutcome = SerdeUtils::deserialize(&bytes)?;
            if old.format != 1 {
                return err_box!("unsupported native result format");
            }
            Prior::Cached(if old.fingerprint == fingerprint {
                old.result
            } else {
                NativeResult {
                    kind: ResultKind::IdentityMismatch,
                    reply: NativeReply {
                        code,
                        success: false,
                        header: vec![],
                        body: FsError::common("native request identity rebound")
                            .encode()
                            .to_vec(),
                    },
                }
            })
        }
        Decision::Fenced | Decision::Full => Prior::Rejected,
        Decision::Admit => Prior::New,
    })
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
    // Deletion/moves are not activated. Existing file updates carry an HSM
    // observation, so a committed protection or generation change fences them.
    if edits.inodes.iter().any(|(id, inode)| {
        inode.as_ref().is_none_or(|inode| {
            inode.id() != *id
                || inode.child_len() != 0
                || inode.is_file_entry()
                || !inode.is_dir() && !inode.is_file()
                || inode.is_file()
                    && edits.inode_checks.get(id).is_some_and(Option::is_some)
                    && !edits.hsm_checks.contains_key(id)
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
    let mut retired = BTreeMap::new();
    for control in &edits.controls {
        let value = control
            .value
            .as_ref()
            .ok_or("native requests cannot acknowledge GC")?;
        let garbage = super::garbage::validate(&control.key, value, &namespace.incarnation)?;
        let owner = crate::master::meta::InodeId::get_id(garbage.block);
        if edits.inode_checks.get(&owner).is_none_or(Option::is_none)
            || !edits.inodes.contains_key(&owner)
            || retired.insert(garbage.block, garbage.locations).is_some()
            || control
                .expected
                .as_ref()
                .is_some_and(|expected| expected != value)
        {
            return err_box!("native garbage has no observed owner or repeats a block");
        }
        if fs.get_rocks_store().hsm_get(&control.key)? != control.expected {
            valid = false;
        }
    }
    if valid {
        for block in retired.keys() {
            let owner = crate::master::meta::InodeId::get_id(*block);
            let previous = current_inode(fs, owner)?.ok_or("retired block owner missing")?;
            let next = edits
                .inodes
                .get(&owner)
                .and_then(Option::as_ref)
                .ok_or("retired block replacement missing")?;
            if !previous
                .as_file_ref()?
                .blocks
                .iter()
                .any(|meta| meta.id == *block)
                || next
                    .as_file_ref()?
                    .blocks
                    .iter()
                    .any(|meta| meta.id == *block)
            {
                return err_box!("native GC attempted to retire a live/unobserved block");
            }
        }
    }
    let mut hsm_records = BTreeMap::new();
    for (id, expected) in &edits.hsm_checks {
        if !edits.inode_checks.contains_key(id) {
            return err_box!("native HSM observation has no inode check");
        }
        if fs.hsm_record(*id)?.as_ref().map(|record| record.version) != *expected {
            valid = false;
        }
    }
    for (id, json) in &edits.hsm_records {
        let record: curvine_model::hsm::native::Record = serde_json::from_slice(json)?;
        let expected = edits
            .hsm_checks
            .get(id)
            .ok_or("native HSM update has no observation")?;
        let file = edits
            .inodes
            .get(id)
            .and_then(Option::as_ref)
            .ok_or("native HSM update has no inode")?
            .as_file_ref()?;
        if record.state.data.entity.entity != id.to_string()
            || record.state.data.entity.namespace != namespace.incarnation
            || record.state.data.entity.storage != namespace.storage
            || record.version
                != expected
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or("HSM version exhausted")?
            || record.state.length != file.len.max(0) as u64
        {
            return err_box!("invalid native HSM update identity/version");
        }
        hsm_records.insert(*id, record);
    }
    for ((block, worker), location) in &edits.locations {
        if *worker != location.worker_id
            || !edits.inodes.values().flatten().any(|inode| {
                inode
                    .as_file_ref()
                    .is_ok_and(|file| file.blocks.iter().any(|meta| meta.id == *block))
            }) && !retired.contains_key(block)
        {
            return err_box!("native block location has no matching file");
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
        for ((block, _), location) in &edits.locations {
            batch.add_location(*block, location)?;
        }
        for control in &edits.controls {
            batch.hsm_put(&control.key, control.value.as_ref().unwrap())?;
        }
        for (id, record) in &hsm_records {
            crate::master::hsm::write_native_record(
                &mut batch,
                fs.hsm_record(*id)?.as_ref(),
                record,
            )?;
            batch.hsm_delete(format!("\u{3}hsm:enroll:{id}").as_bytes())?;
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
            if expected.is_some() {
                if let Some(Some(inode)) = edits.inodes.get(id) {
                    if inode.is_file() {
                        fs.get_ttl_bucket_list().add(inode);
                        fs.evictor.on_access(*id);
                    }
                }
            }
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
    fn overwrite_commits_retirement_before_worker_effects_and_never_reuses_block_ids(
    ) -> CommonResult<()> {
        use super::super::garbage;
        use curvine_model::{ClientAddress, CreateFileOpts, WorkerAddress, WorkerInfo};
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = configuration(root.path());
        conf.master.worker_policy = "robin".into();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        seed_namespace(&fs)?;
        fs.worker_manager.write().add_test_worker(WorkerInfo {
            address: WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            ..Default::default()
        });
        let status = fs.create("/file", false)?;
        let block = fs
            .add_block(
                "/file",
                Some(status.id),
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )?
            .block;
        let old = {
            let dir = fs.fs_dir.read();
            let file = dir.store.get_inode(status.id, None)?.unwrap();
            let record = crate::master::hsm::native_record(
                &dir.hsm_namespace()?.unwrap(),
                "/file",
                file.as_file_ref()?,
                None,
            )?
            .unwrap();
            let mut batch = dir.get_rocks_store().new_batch();
            crate::master::hsm::write_native_record(&mut batch, None, &record)?;
            batch.commit()?;
            assert!(
                dir.get_block_locations(block.id)?.is_empty(),
                "pre-assigned replica has no report yet"
            );
            assert!(record.writing);
            record
        };
        let mut command = prepare(&fs, "/unused")?;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared =
                commands::overwrite(&mut view, "/file", CreateFileOpts::with_create(false), 2000)?;
            command.edits = prepared.edits;
            command.reply.code = RpcCode::CreateFile.into();
            command.reply.header = prepared.response;
            assert_eq!(
                dir.store
                    .get_inode(status.id, None)?
                    .unwrap()
                    .as_file_ref()?
                    .blocks
                    .len(),
                1
            );
            assert!(dir
                .get_rocks_store()
                .hsm_scan(garbage::PREFIX, None, 10)?
                .is_empty());
            assert!(dir.get_block_locations(block.id)?.is_empty());
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &command)?.kind == ResultKind::Applied);
        let bytes = {
            let dir = fs.fs_dir.read();
            let current = dir.hsm_record(status.id)?.unwrap();
            assert_eq!(
                current.state.data.generation,
                old.state.data.generation + 1,
                "overwrite must invalidate even an already-writing generation"
            );
            assert_eq!(current.version, old.version + 1);
            let file = dir.store.get_inode(status.id, None)?.unwrap();
            assert!(file.as_file_ref()?.blocks.is_empty());
            assert_eq!(file.as_file_ref()?.next_seq, 1);
            assert_eq!(
                dir.get_block_locations(block.id)?.len(),
                1,
                "unreported preallocation still needs an explicit delete acknowledgement"
            );
            dir.get_rocks_store()
                .hsm_get(&garbage::key(block.id))?
                .unwrap()
        };
        assert!(apply_creation(&mut fs.fs_dir.write(), &command)?.reply == command.reply);
        assert_eq!(
            fs.fs_dir
                .read()
                .hsm_record(status.id)?
                .unwrap()
                .state
                .data
                .generation,
            old.state.data.generation + 1
        );
        // The new layout cannot reuse a retired block, even with the same inode.
        let next = fs
            .add_block(
                "/file",
                Some(status.id),
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )?
            .block;
        assert_ne!(next.id, block.id);
        assert!(fs
            .fs_dir
            .read()
            .get_rocks_store()
            .hsm_get(&garbage::key(block.id))?
            .is_some());
        // Simulate the committed acknowledgement observed by a new leader.
        // Follower soft placement may still contain the old rows; apply removes
        // those rows with the matching fact, without touching the new layout.
        let mut ack = garbage::GarbageAck {
            incarnation: "other-volume".into(),
            epoch: 1,
            items: vec![(garbage::key(block.id), bytes)],
            cursor_expected: None,
            cursor_next: None,
        };
        garbage::apply_ack(&mut fs.fs_dir.write(), &ack)?;
        assert!(fs
            .fs_dir
            .read()
            .get_rocks_store()
            .hsm_get(&garbage::key(block.id))?
            .is_some());
        ack.incarnation = "incarnation".into();
        garbage::apply_ack(&mut fs.fs_dir.write(), &ack)?;
        garbage::apply_ack(&mut fs.fs_dir.write(), &ack)?;
        let dir = fs.fs_dir.read();
        assert!(dir
            .get_rocks_store()
            .hsm_get(&garbage::key(block.id))?
            .is_none());
        assert!(dir.get_block_locations(block.id)?.is_empty());
        assert_eq!(
            dir.store
                .get_inode(status.id, None)?
                .unwrap()
                .as_file_ref()?
                .blocks[0]
                .id,
            next.id
        );
        Ok(())
    }

    #[test]
    fn frozen_v2_completion_journal_preserves_hsm_fields() -> CommonResult<()> {
        use super::super::{legacy::*, transport::decode_batch};
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let file = fs.create("/file", false)?;
        let mut c = prepare(&fs, "/unused")?;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::complete(
                &mut view,
                &fs,
                curvine_proto::CompleteFileRequest {
                    path: "/file".into(),
                    ..Default::default()
                },
                1000,
            )?;
            c.edits = prepared.edits;
            c.reply.header = prepared.response;
            c.reply.code = RpcCode::CompleteFile.into();
        }
        let expected = c.reply.clone();
        let e = c.edits;
        let bytes = SerdeUtils::serialize(&NativeBatchV2 {
            version: 2,
            commands: vec![NativeCommandV2 {
                identity: c.identity,
                fingerprint: c.fingerprint,
                time: c.time,
                reply: c.reply,
                edits: PreparedEditsV2 {
                    inodes: e.inodes,
                    edges: e.edges,
                    inode_checks: e.inode_checks,
                    edge_checks: e.edge_checks,
                    child_deltas: e.child_deltas,
                    last_inode: e.last_inode,
                    hsm_checks: e.hsm_checks,
                    hsm_records: e.hsm_records,
                    locations: e.locations,
                },
            }],
        })?;
        let batch = decode_batch(&bytes)?;
        assert!(batch.garbage_ack.is_none());
        assert!(batch.commands[0].edits.controls.is_empty());
        assert!(apply_creation(&mut fs.fs_dir.write(), &batch.commands[0])?.reply == expected);
        assert!(!fs.fs_dir.read().hsm_record(file.id)?.unwrap().writing);
        Ok(())
    }

    #[test]
    fn native_blocks_share_pending_allocation_and_failed_preparation_releases_pressure(
    ) -> CommonResult<()> {
        use curvine_model::{ProtoUtils, WorkerAddress, WorkerInfo};
        use prost::Message;
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = configuration(root.path());
        conf.master.worker_policy = "robin".into();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        seed_namespace(&fs)?;
        fs.worker_manager.write().add_test_worker(WorkerInfo {
            address: WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            available: 1 << 30,
            capacity: 1 << 30,
            ..Default::default()
        });
        let status = fs.create("/payload", false)?;
        let request = || curvine_proto::AddBlockRequest {
            path: "/payload".into(),
            inode_id: Some(status.id),
            ..Default::default()
        };
        {
            let dir = fs.fs_dir.read();
            let mut budget = BatchView::new(&dir, 128);
            assert!(commands::add_block(&mut budget, &fs, request(), 1000).is_err());
        }
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            0
        );
        let (first, duplicate) = {
            let dir = fs.fs_dir.read();
            let mut batch = BatchView::new(&dir, 1024 * 1024);
            let first = commands::add_block(&mut batch, &fs, request(), 1000)?;
            let duplicate = commands::add_block(&mut batch, &fs, request(), 1001)?;
            assert_eq!(first.response, duplicate.response);
            assert!(first.allocation.is_some() && duplicate.allocation.is_none());
            assert!(dir
                .store
                .get_inode(status.id, None)?
                .unwrap()
                .as_file_ref()?
                .blocks
                .is_empty());
            (first, duplicate)
        };
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            status.block_size
        );
        let original = curvine_proto::LocatedBlockProto::decode(first.response.as_slice())?;
        let block = ProtoUtils::located_block_from_pb(original).block;
        let to_command = |prepared: commands::PreparedCommand, sequence| {
            let command = NativeCommand {
                identity: RequestIdentity {
                    incarnation: "incarnation".into(),
                    session: format!("{:016x}{}", 1000, "b".repeat(32)),
                    sequence,
                    acknowledged: 0,
                },
                fingerprint: [sequence as u8; 32],
                time: 1000,
                edits: prepared.edits,
                reply: NativeReply {
                    code: RpcCode::AddBlock.into(),
                    success: true,
                    header: prepared.response,
                    body: vec![],
                },
            };
            (command, prepared.allocation)
        };
        let (first, allocation) = to_command(first, 1);
        assert!(apply_creation(&mut fs.fs_dir.write(), &first)?.kind == ResultKind::Applied);
        allocation.unwrap().retain();
        let (duplicate, _) = to_command(duplicate, 2);
        assert!(apply_creation(&mut fs.fs_dir.write(), &duplicate)?.reply == first.reply);
        assert!(apply_creation(&mut fs.fs_dir.write(), &first)?.reply == first.reply);
        let dir = fs.fs_dir.read();
        let inode = dir.store.get_inode(status.id, None)?.unwrap();
        assert_eq!(inode.as_file_ref()?.blocks.len(), 1);
        assert_eq!(inode.as_file_ref()?.blocks[0].id, block.id);
        assert!(
            dir.hsm_record(status.id)?.is_none(),
            "block scheduling does not rewrite stable HSM state"
        );
        Ok(())
    }

    #[test]
    fn reopen_invalidates_generation_once_and_multiwriter_fingerprints_are_canonical(
    ) -> CommonResult<()> {
        use curvine_model::{CreateFileOpts, OpenFlags, ProtoUtils};
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let file = fs.create("/payload", false)?;
        let mut close = prepare(&fs, "/unused")?;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::complete(
                &mut view,
                &fs,
                curvine_proto::CompleteFileRequest {
                    path: "/payload".into(),
                    inode_id: Some(file.id),
                    ..Default::default()
                },
                1000,
            )?;
            close.edits = prepared.edits;
            close.reply.code = RpcCode::CompleteFile.into();
            close.reply.header = prepared.response;
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &close)?.kind == ResultKind::Applied);
        let old = fs.fs_dir.read().hsm_record(file.id)?.unwrap();
        let mut commands = vec![];
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            for (index, name) in ["writer-z", "writer-a", "writer-q"].into_iter().enumerate() {
                let prepared = commands::reopen(
                    &mut view,
                    &fs,
                    curvine_proto::OpenFileRequest {
                        path: "/payload".into(),
                        opts: ProtoUtils::create_opts_to_pb(
                            CreateFileOpts::with_create(false),
                            name,
                        ),
                        flags: OpenFlags::new_write_only().value(),
                    },
                )?;
                commands.push(NativeCommand {
                    identity: RequestIdentity {
                        incarnation: "incarnation".into(),
                        session: format!("{:016x}{}", 1000, "d".repeat(32)),
                        sequence: index as u64 + 1,
                        acknowledged: 0,
                    },
                    fingerprint: [index as u8; 32],
                    time: 1000,
                    edits: prepared.edits,
                    reply: NativeReply {
                        code: RpcCode::OpenFile.into(),
                        success: true,
                        header: prepared.response,
                        body: vec![],
                    },
                });
            }
        }
        for command in &commands {
            assert!(apply_creation(&mut fs.fs_dir.write(), command)?.kind == ResultKind::Applied);
        }
        let dir = fs.fs_dir.read();
        let record = dir.hsm_record(file.id)?.unwrap();
        assert!(record.writing);
        assert_eq!(record.state.data.generation, old.state.data.generation + 1);
        let inode = dir.store.get_inode(file.id, None)?.unwrap();
        let fingerprint = inode_fingerprint(&inode)?;
        for _ in 0..50 {
            let serialized = SerdeUtils::serialize(&inode)?;
            let fresh = SerdeUtils::deserialize::<InodeView>(&serialized)?;
            assert_eq!(inode_fingerprint(&fresh)?, fingerprint);
        }
        drop(dir);
        for command in &commands {
            assert!(apply_creation(&mut fs.fs_dir.write(), command)?.reply == command.reply);
        }
        assert_eq!(
            fs.fs_dir
                .read()
                .hsm_record(file.id)?
                .unwrap()
                .state
                .data
                .generation,
            record.state.data.generation
        );
        Ok(())
    }

    #[test]
    fn complete_keeps_hsm_and_native_atomic_across_replicas_retries_and_conflicts(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let a = tempfile::tempdir()?;
        let b = tempfile::tempdir()?;
        let source = JournalSystem::fs_only_for_test(&configuration(a.path()))?;
        let follower = JournalSystem::fs_only_for_test(&configuration(b.path()))?;
        for fs in [&source, &follower] {
            seed_namespace(fs)?;
        }
        let mut create = prepare(&source, "/unused")?;
        {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::create(
                &mut view,
                "/data/payload",
                curvine_model::CreateFileOpts::with_create(true),
                1000,
            )?;
            create.edits = prepared.edits;
            create.reply.code = RpcCode::CreateFile.into();
            create.reply.header = prepared.response;
        }
        for fs in [&source, &follower] {
            assert!(apply_creation(&mut fs.fs_dir.write(), &create)?.kind == ResultKind::Applied);
        }
        let id = source.file_status("/data/payload")?.id;
        let make_request = |flush| curvine_proto::CompleteFileRequest {
            path: "/old-name-after-open".into(),
            inode_id: Some(id),
            len: 0,
            client_name: "".into(),
            only_flush: flush,
            return_file_blocks: Some(false),
            ..Default::default()
        };
        let make_command = |prepared: commands::PreparedCommand, sequence| NativeCommand {
            identity: RequestIdentity {
                incarnation: "incarnation".into(),
                session: create.identity.session.clone(),
                sequence,
                acknowledged: 0,
            },
            fingerprint: [sequence as u8; 32],
            time: 2000,
            edits: prepared.edits,
            reply: NativeReply {
                code: RpcCode::CompleteFile.into(),
                success: true,
                header: prepared.response,
                body: vec![],
            },
        };
        let (flush, close) = {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let flush = make_command(
                commands::complete(&mut view, &source, make_request(true), 2000)?,
                2,
            );
            let close = make_command(
                commands::complete(&mut view, &source, make_request(false), 3000)?,
                3,
            );
            assert!(dir.hsm_record(id)?.is_none());
            assert_eq!(
                dir.store.get_inode(id, None)?.unwrap().as_file_ref()?.mtime,
                1000
            );
            (flush, close)
        };
        for fs in [&source, &follower] {
            assert!(apply_creation(&mut fs.fs_dir.write(), &flush)?.kind == ResultKind::Applied);
            let record = fs.fs_dir.read().hsm_record(id)?.unwrap();
            assert_eq!(record.version, 1);
            assert_eq!(record.path, "/data/payload");
            assert!(apply_creation(&mut fs.fs_dir.write(), &close)?.kind == ResultKind::Applied);
            let record = fs.fs_dir.read().hsm_record(id)?.unwrap();
            assert!(!record.writing);
            assert_eq!(record.version, 2);
            assert_eq!(record.mtime_ms, fs.file_status("/data/payload")?.mtime);
            assert_eq!(record.mtime_ms, 3000);
        }
        source.rename("/data", "/renamed", RenameFlags::new(0))?;
        assert!(apply_creation(&mut source.fs_dir.write(), &close)?.reply == close.reply);
        assert_eq!(source.fs_dir.read().hsm_record(id)?.unwrap().version, 2);
        let late = {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            make_command(
                commands::complete(&mut view, &source, make_request(false), 4000)?,
                4,
            )
        };
        // An access-state change can invalidate a close without changing native
        // metadata. It must neither publish the inode nor overwrite the record.
        {
            let dir = source.fs_dir.read();
            let old = dir.hsm_record(id)?.unwrap();
            let mut record = old.clone();
            record.version += 1;
            let mut batch = dir.get_rocks_store().new_batch();
            crate::master::hsm::write_native_record(&mut batch, Some(&old), &record)?;
            batch.commit()?;
        }
        assert!(apply_creation(&mut source.fs_dir.write(), &late)?.kind == ResultKind::Conflict);
        assert_eq!(source.file_status("/renamed/payload")?.mtime, 3000);
        assert_eq!(source.fs_dir.read().hsm_record(id)?.unwrap().version, 3);
        Ok(())
    }

    #[test]
    fn frozen_v1_journal_replays_with_empty_new_fields_and_original_result() -> CommonResult<()> {
        use super::super::{legacy::*, transport::decode_batch};
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let c = prepare(&fs, "/old-format")?;
        let expected = c.reply.clone();
        let e = c.edits;
        let bytes = SerdeUtils::serialize(&NativeBatchV1 {
            version: 1,
            commands: vec![NativeCommandV1 {
                identity: c.identity,
                fingerprint: c.fingerprint,
                time: c.time,
                reply: c.reply,
                edits: PreparedEditsV1 {
                    inodes: e.inodes,
                    edges: e.edges,
                    inode_checks: e.inode_checks,
                    edge_checks: e.edge_checks,
                    child_deltas: e.child_deltas,
                    last_inode: e.last_inode,
                },
            }],
        })?;
        let mut batch = decode_batch(&bytes)?;
        let command = batch.commands.pop().unwrap();
        assert!(command.edits.hsm_records.is_empty() && command.edits.locations.is_empty());
        assert!(apply_creation(&mut fs.fs_dir.write(), &command)?.reply == expected);
        assert!(fs.exists("/old-format")?);
        assert!(decode_batch(&SerdeUtils::serialize(&99u32)?).is_err());
        assert!(decode_batch(&[1, 2]).is_err());
        Ok(())
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
            let result = apply_creation(&mut replica.fs_dir.write(), &command)?;
            assert!(result.kind == ResultKind::Applied && result.reply == command.reply);
            assert_eq!(replica.file_status("/a/b")?.mtime, 1000);
            // MasterMetrics is process-global, including other concurrently
            // running test stores. Inspect this replica's actual namespace.
            assert_eq!(replica.fs_dir.read().root_dir.child_len(), 1);
            assert_eq!(replica.file_status("/a")?.children_num, 1);
            assert_eq!(replica.file_status("/a/b")?.children_num, 0);
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
