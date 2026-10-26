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

#[derive(Clone, Serialize, Deserialize)]
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
            let old: StoredOutcome = SerdeUtils::deserialize(value)?;
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
    // Updates/deletions/moves carry observations from one committed prefix.
    if edits.inodes.iter().any(|(id, inode)| {
        if inode.is_none() {
            return *id == ROOT_INODE_ID
                || edits.inode_checks.get(id).is_none_or(Option::is_none)
                || !edits.hsm_checks.contains_key(id);
        }
        inode.as_ref().is_some_and(|inode| {
            inode.id() != *id
                || inode.child_len() != 0
                || inode.is_file_entry()
                || !inode.is_dir() && !inode.is_file()
                || inode.is_file()
                    && edits.inode_checks.get(id).is_some_and(Option::is_some)
                    && !edits.hsm_checks.contains_key(id)
        })
    }) || edits.edges.iter().any(|(key, value)| match value {
        Some(_) => !edits.edge_checks.contains_key(key),
        None => edits
            .edge_checks
            .get(key)
            .and_then(|id| *id)
            .is_none_or(|id| {
                !edits.inodes.contains_key(&id)
                    || edits.inode_checks.get(&id).is_none_or(Option::is_none)
            }),
    }) {
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
                .ok_or("retired block owner effect missing")?;
            if !previous
                .as_file_ref()?
                .blocks
                .iter()
                .any(|meta| meta.id == *block)
                || next.as_ref().is_some_and(|inode| {
                    inode
                        .as_file_ref()
                        .is_ok_and(|file| file.blocks.iter().any(|meta| meta.id == *block))
                })
            {
                return err_box!("native GC attempted to retire a live/unobserved block");
            }
        }
    }
    let mut deleted = BTreeMap::new();
    let mut removed_edges = BTreeMap::new();
    let deletion_only = edits.edges.values().all(Option::is_none);
    if valid {
        for (id, inode) in &edits.inodes {
            if inode.is_none() {
                let previous = current_inode(fs, *id)?.ok_or("deleted inode missing")?;
                if let Ok(file) = previous.as_file_ref() {
                    if file
                        .blocks
                        .iter()
                        .any(|block| !retired.contains_key(&block.id))
                    {
                        return err_box!("deleted file has no durable block retirement");
                    }
                } else {
                    // Metadata fingerprints omit directory children. Empty
                    // deletion therefore needs an explicit apply-time check.
                    let path = fs.get_inode_path(*id)?;
                    let input = InodePath::resolve(fs.root_ptr(), path, &fs.store)?;
                    let inode = input.get_last_inode().ok_or("deleted directory missing")?;
                    // A recursive command removes every observed child in the
                    // same WriteBatch. A newly inserted/unobserved child fences
                    // the whole operation, including same-millisecond inserts.
                    for child in inode.as_dir_ref()?.children_iter() {
                        let key = (*id, child.name().to_owned());
                        if edits.edge_checks.get(&key) != Some(&Some(child.id()))
                            || edits.edges.get(&key) != Some(&None)
                        {
                            valid = false;
                        }
                    }
                    if edits
                        .edges
                        .range((*id, String::new())..)
                        .take_while(|((parent, _), _)| *parent == *id)
                        .any(|(_, child)| child.is_some())
                    {
                        return err_box!("deleted directory receives a new child");
                    }
                }
                deleted.insert(*id, previous);
            }
        }
        for (edge, child) in &edits.edges {
            if edits.edge_checks[edge].is_some() && edits.edge_checks[edge] != *child {
                let id = edits.edge_checks[edge].ok_or("removed edge was not observed")?;
                let directory = match deleted.get(&id) {
                    Some(previous) => previous.is_dir(),
                    None => current_inode(fs, id)?
                        .ok_or("removed edge inode missing")?
                        .is_dir(),
                };
                let depth = if deletion_only {
                    0
                } else {
                    fs.get_inode_path(edge.0)?
                        .split('/')
                        .filter(|part| !part.is_empty())
                        .count()
                };
                removed_edges.insert(edge.clone(), (id, directory, depth));
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
            if expected.is_some()
                && !(deletion_only && edits.inodes.get(id).is_some_and(Option::is_none))
                && current_inode(fs, *id)?.is_some_and(|inode| inode.is_dir())
            {
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
            let Some(child) = *child else {
                continue;
            };
            let inode = edits
                .inodes
                .get(&child)
                .and_then(Option::as_ref)
                .ok_or("native creation has no child metadata")?;
            let existing_move = edits.inode_checks.get(&child).is_some_and(Option::is_some)
                && edits.edges.iter().any(|(old_edge, value)| {
                    edits.edge_checks.get(old_edge) == Some(&Some(child)) && *value != Some(child)
                });
            if !(edits.inode_checks.get(&child) == Some(&None) || existing_move)
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
        for (id, inode) in &edits.inodes {
            if let Some(inode) = inode {
                batch.write_inode(inode)?;
            } else {
                batch.delete_inode(*id)?;
                if let Some(record) = fs.hsm_record(*id)? {
                    crate::master::hsm::forget_native_record(&mut batch, &record)?;
                }
                batch.hsm_delete(format!("\u{3}hsm:enroll:{id}").as_bytes())?;
            }
        }
        for ((parent, name), child) in &edits.edges {
            if let Some(child) = child {
                batch.add_child(*parent, name, *child)?;
            } else {
                batch.delete_child(*parent, name)?;
            }
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
            if let (Some(pointer), Some(inode)) = (directories.get(id), inode) {
                let old = std::mem::replace(pointer.as_mut(), inode.clone_metadata());
                pointer.as_mut().adopt_directory_children(old);
            }
        }
        let mut detached = BTreeMap::new();
        if deletion_only {
            // All persistent edges are removed in the WriteBatch. In memory,
            // detach only deletion boundaries: dropping the owned subtree
            // removes descendants without N parent-path lookups or sorting N
            // individual pointer removals. No pointer inside it is reused.
            let mut files = 0;
            let mut dirs = 0;
            for ((parent_id, name), (id, directory, _)) in removed_edges {
                if directory {
                    dirs += 1;
                } else {
                    files += 1;
                }
                if edits.inodes.get(&parent_id).is_some_and(Option::is_none) {
                    continue;
                }
                directories
                    .get(&parent_id)
                    .ok_or("removed parent missing")?
                    .as_mut()
                    .delete_child(id, &name)?;
            }
            fs.store.fs_stats.add_file_count(-files);
            fs.store.fs_stats.add_dir_count(-dirs);
        } else {
            // Moves retain the old subtree until it is attached at its new
            // edge. Remove deeper edges first while borrowed pointers live.
            let mut removals = removed_edges
                .into_iter()
                .map(|(edge, (id, directory, depth))| (depth, edge, (id, directory)))
                .collect::<Vec<_>>();
            removals.sort_by_key(|removal| std::cmp::Reverse(removal.0));
            for (_, (parent_id, name), (id, directory)) in removals {
                let parent = directories
                    .get(&parent_id)
                    .ok_or("removed parent missing")?;
                let inode = parent.as_mut().delete_child(id, &name)?;
                if edits.inodes.get(&id).is_some_and(Option::is_some) && directory {
                    detached.insert(id, inode);
                }
                if directory {
                    fs.store.fs_stats.add_dir_count(-1);
                } else {
                    fs.store.fs_stats.add_file_count(-1);
                }
            }
        }
        // Creation allocates parent first. Moves cannot target their own
        // subtree and compact namespace commands never allocate new IDs.
        for ((parent_id, name), child) in &edits.edges {
            let Some(child) = child else {
                continue;
            };
            if edits.edge_checks.get(&(*parent_id, name.clone())) == Some(&Some(*child)) {
                continue;
            }
            let parent = directories
                .get(parent_id)
                .ok_or("native parent is not published")?;
            let mut inode = edits
                .inodes
                .get(child)
                .and_then(Option::as_ref)
                .ok_or("attached child missing")?
                .clone_metadata();
            if let Some(original) = detached.remove(child) {
                inode.adopt_directory_children(original);
            }
            let directory = inode.is_dir();
            let pointer = parent.as_mut().add_child(inode)?;
            directories.insert(*child, pointer);
            if directory {
                fs.store.fs_stats.increment_dir_count();
            } else {
                fs.store.fs_stats.increment_file_count();
            }
        }
        for (id, inode) in &deleted {
            fs.get_ttl_bucket_list().remove(inode);
            fs.evictor.remove_victims(&[*id]);
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
    fn nonrecursive_directory_attributes_do_not_refresh_or_guard_descendants() -> CommonResult<()> {
        use curvine_model::SetAttrOpts;
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        fs.fs_dir.write().hsm_enabled = true;
        fs.mkdir("/tree", false)
            .map_err(|e| format!("mkdir setup: {e}"))?;
        let file = fs
            .create("/tree/file", false)
            .map_err(|e| format!("create setup: {e}"))?;
        fs.complete_file("/tree/file", Some(file.id), 0, vec![], "", false, None)
            .map_err(|e| format!("close setup: {e}"))?;
        let original = fs.fs_dir.read().hsm_record(file.id)?.unwrap();
        fs.set_attr(
            "/tree",
            SetAttrOpts {
                mode: Some(0o700),
                ..Default::default()
            },
        )?;
        assert_eq!(
            fs.fs_dir.read().hsm_record(file.id)?.unwrap().version,
            original.version,
            "nonrecursive parent attributes rewrote a descendant HSM record"
        );
        // A protected child is unaffected by a nonrecursive parent chmod.
        {
            let dir = fs.fs_dir.read();
            let mut protected = original.clone();
            protected.version += 1;
            protected.readers.insert("reader".into(), i64::MAX);
            let mut batch = dir.get_rocks_store().new_batch();
            crate::master::hsm::write_native_record(&mut batch, Some(&original), &protected)?;
            batch.commit()?;
        }
        fs.set_attr(
            "/tree",
            SetAttrOpts {
                mode: Some(0o750),
                ..Default::default()
            },
        )?;
        // Owned preparation also observes only the target, even while the
        // descendant is protected. Its work is independent of subtree size.
        let prepared = {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            commands::set_attr(
                &mut view,
                "/tree",
                SetAttrOpts {
                    mode: Some(0o700),
                    ..Default::default()
                },
                2000,
            )?
        };
        assert_eq!(prepared.edits.inodes.len(), 1);
        assert!(!prepared.edits.hsm_checks.contains_key(&file.id));
        assert!(
            fs.set_attr(
                "/tree",
                SetAttrOpts {
                    recursive: true,
                    mode: Some(0o755),
                    ..Default::default()
                }
            )
            .is_err(),
            "recursive changes must still protect the child"
        );
        Ok(())
    }

    #[test]
    fn attributes_commit_native_and_hsm_together_and_replay_original_results() -> CommonResult<()> {
        use curvine_model::{CreateFileOpts, SetAttrOpts, INTERNAL_CTIME_XATTR};
        crate::master::Master::init_test_metrics();
        let a = tempfile::tempdir()?;
        let b = tempfile::tempdir()?;
        let source = JournalSystem::fs_only_for_test(&configuration(a.path()))?;
        let follower = JournalSystem::fs_only_for_test(&configuration(b.path()))?;
        for fs in [&source, &follower] {
            seed_namespace(fs)?;
        }
        let seed = prepare(&source, "/unused")?;
        let make =
            |prepared: commands::PreparedCommand, sequence: u64, code: RpcCode| NativeCommand {
                identity: RequestIdentity {
                    incarnation: "incarnation".into(),
                    session: seed.identity.session.clone(),
                    sequence,
                    acknowledged: 0,
                },
                fingerprint: [sequence as u8; 32],
                time: 1000 + sequence as i64,
                edits: prepared.edits,
                reply: NativeReply {
                    code: code.into(),
                    success: true,
                    header: prepared.response,
                    body: vec![],
                },
            };
        let (create, close) = {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let create = make(
                commands::create(
                    &mut view,
                    "/data/file",
                    CreateFileOpts::with_create(true),
                    1000,
                )?,
                1,
                RpcCode::CreateFile,
            );
            let close = make(
                commands::complete(
                    &mut view,
                    &source,
                    curvine_proto::CompleteFileRequest {
                        path: "/data/file".into(),
                        len: 0,
                        client_name: "".into(),
                        only_flush: false,
                        ..Default::default()
                    },
                    2000,
                )?,
                2,
                RpcCode::CompleteFile,
            );
            (create, close)
        };
        for fs in [&source, &follower] {
            assert!(apply_creation(&mut fs.fs_dir.write(), &create)?.kind == ResultKind::Applied);
            assert!(apply_creation(&mut fs.fs_dir.write(), &close)?.kind == ResultKind::Applied);
        }
        let id = source.file_status("/data/file")?.id;
        let original = source.fs_dir.read().hsm_record(id)?.unwrap();
        assert_ne!(source.file_status("/data/file")?.owner, "policy-owner");
        let (owner, modified) = {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let owner = make(
                commands::set_attr(
                    &mut view,
                    "/data/file",
                    SetAttrOpts {
                        owner: Some("policy-owner".into()),
                        add_x_attr: [(INTERNAL_CTIME_XATTR.into(), 999i64.to_le_bytes().to_vec())]
                            .into(),
                        remove_x_attr: vec![INTERNAL_CTIME_XATTR.into()],
                        ..Default::default()
                    },
                    3000,
                )?,
                3,
                RpcCode::SetAttr,
            );
            let modified = make(
                commands::set_attr(
                    &mut view,
                    "/data/file",
                    SetAttrOpts {
                        mtime: Some(4000),
                        ..Default::default()
                    },
                    5000,
                )?,
                4,
                RpcCode::SetAttr,
            );
            assert_eq!(dir.hsm_record(id)?.unwrap().version, original.version);
            (owner, modified)
        };
        for fs in [&source, &follower] {
            assert!(apply_creation(&mut fs.fs_dir.write(), &owner)?.kind == ResultKind::Applied);
            let record = fs.fs_dir.read().hsm_record(id)?.unwrap();
            assert_eq!(record.owner, "policy-owner");
            assert_eq!(record.version, original.version + 1);
            assert_eq!(record.state.data.generation, original.state.data.generation);
            assert_eq!(fs.file_status("/data/file")?.ctime(), 3000);
            assert!(apply_creation(&mut fs.fs_dir.write(), &modified)?.kind == ResultKind::Applied);
            let record = fs.fs_dir.read().hsm_record(id)?.unwrap();
            assert_eq!(record.version, original.version + 2);
            assert_eq!(record.mtime_ms, 4000);
            assert_eq!(
                record.state.data.generation,
                original.state.data.generation + 1
            );
            assert!(apply_creation(&mut fs.fs_dir.write(), &owner)?.reply == owner.reply);
            assert_eq!(fs.file_status("/data/file")?.ctime(), 5000);
        }
        let late = {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            make(
                commands::set_attr(
                    &mut view,
                    "/data/file",
                    SetAttrOpts {
                        mode: Some(0o600),
                        ..Default::default()
                    },
                    6000,
                )?,
                5,
                RpcCode::SetAttr,
            )
        };
        let before = source.file_status("/data/file")?;
        {
            let dir = source.fs_dir.read();
            let old = dir.hsm_record(id)?.unwrap();
            let mut accessed = old.clone();
            accessed.version += 1;
            let mut batch = dir.get_rocks_store().new_batch();
            crate::master::hsm::write_native_record(&mut batch, Some(&old), &accessed)?;
            batch.commit()?;
        }
        assert!(apply_creation(&mut source.fs_dir.write(), &late)?.kind == ResultKind::Conflict);
        assert_eq!(source.file_status("/data/file")?.ctime(), before.ctime());
        assert_eq!(source.file_status("/data/file")?.mode, before.mode);
        Ok(())
    }

    #[test]
    fn delete_retires_blocks_independently_of_inode_and_retries_do_not_delete_replacement(
    ) -> CommonResult<()> {
        use super::super::garbage;
        use curvine_model::{ClientAddress, WorkerAddress, WorkerInfo};
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
        let original = fs.create("/file", false)?;
        let block = fs
            .add_block(
                "/file",
                Some(original.id),
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )?
            .block;
        fs.fs_dir.write().hsm_enabled = true;
        fs.complete_file("/file", Some(original.id), 0, vec![], "", false, None)?;
        assert!(fs.fs_dir.read().hsm_record(original.id)?.is_some());
        let mut deletion = prepare(&fs, "/unused")?;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::delete(&mut view, "/file", false, 2000)?;
            deletion.edits = prepared.edits;
            deletion.reply.header = prepared.response;
            deletion.reply.code = RpcCode::Delete.into();
            assert!(dir.store.get_inode(original.id, None)?.is_some());
            assert!(dir
                .get_rocks_store()
                .hsm_scan(garbage::PREFIX, None, 10)?
                .is_empty());
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &deletion)?.kind == ResultKind::Applied);
        let fact = {
            let dir = fs.fs_dir.read();
            assert!(dir.store.get_inode(original.id, None)?.is_none());
            assert!(dir.hsm_record(original.id)?.is_none());
            assert!(matches!(
                dir.block_inode_state(block.id)?,
                crate::master::fs::BlockInodeState::Retired
            ));
            assert_eq!(dir.get_block_locations(block.id)?.len(), 1);
            dir.get_rocks_store()
                .hsm_get(&garbage::key(block.id))?
                .unwrap()
        };
        assert!(!fs.exists("/file")?);
        let replacement = fs.create("/file", false)?;
        assert!(replacement.id > original.id);
        assert!(apply_creation(&mut fs.fs_dir.write(), &deletion)?.reply == deletion.reply);
        assert_eq!(fs.file_status("/file")?.id, replacement.id);
        {
            let mut dir = fs.fs_dir.write();
            let mut ack = dir.get_rocks_store().new_batch();
            ack.delete_location(block.id, 1)?;
            ack.commit()?;
            garbage::apply_ack(
                &mut dir,
                &garbage::GarbageAck {
                    incarnation: "incarnation".into(),
                    epoch: 1,
                    items: vec![(garbage::key(block.id), fact)],
                    cursor_expected: None,
                    cursor_next: None,
                },
            )?;
            assert!(dir
                .get_rocks_store()
                .hsm_get(&garbage::key(block.id))?
                .is_none());
            assert!(matches!(
                dir.block_inode_state(block.id)?,
                crate::master::fs::BlockInodeState::Missing
            ));
        }
        Ok(())
    }

    #[test]
    fn empty_directory_delete_checks_children_even_when_metadata_hash_did_not_change(
    ) -> CommonResult<()> {
        use curvine_model::CreateFileOpts;
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let mkdir = prepare(&fs, "/empty")?;
        assert!(apply_creation(&mut fs.fs_dir.write(), &mkdir)?.kind == ResultKind::Applied);
        let mut deletion = prepare(&fs, "/unused")?;
        deletion.identity.sequence = 2;
        deletion.fingerprint = [2; 32];
        let id = fs.file_status("/empty")?.id;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::delete(&mut view, "/empty", false, 1000)?;
            deletion.edits = prepared.edits;
            deletion.reply.header = prepared.response;
            deletion.reply.code = RpcCode::Delete.into();
        }
        let mut child = prepare(&fs, "/unused")?;
        child.identity.sequence = 3;
        child.fingerprint = [3; 32];
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::create(
                &mut view,
                "/empty/child",
                CreateFileOpts::with_create(false),
                1000,
            )?;
            child.edits = prepared.edits;
            child.reply.header = prepared.response;
            child.reply.code = RpcCode::CreateFile.into();
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &child)?.kind == ResultKind::Applied);
        assert_eq!(
            inode_fingerprint(&fs.fs_dir.read().store.get_inode(id, None)?.unwrap())?,
            deletion.edits.inode_checks[&id].unwrap()
        );
        assert!(apply_creation(&mut fs.fs_dir.write(), &deletion)?.kind == ResultKind::Conflict);
        assert!(fs.exists("/empty/child")?);
        Ok(())
    }

    #[test]
    fn empty_directory_deletion_replays_on_both_roles_and_retries_after_recreation(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let a = tempfile::tempdir()?;
        let b = tempfile::tempdir()?;
        let source = JournalSystem::fs_only_for_test(&configuration(a.path()))?;
        let follower = JournalSystem::fs_only_for_test(&configuration(b.path()))?;
        for fs in [&source, &follower] {
            seed_namespace(fs)?;
        }
        let mkdir = prepare(&source, "/empty")?;
        for fs in [&source, &follower] {
            assert!(apply_creation(&mut fs.fs_dir.write(), &mkdir)?.kind == ResultKind::Applied);
        }
        let mut deletion = prepare(&source, "/unused")?;
        deletion.identity.sequence = 2;
        deletion.fingerprint = [2; 32];
        {
            let dir = source.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::delete(&mut view, "/empty", false, 2000)?;
            deletion.edits = prepared.edits;
            deletion.reply.header = prepared.response;
            deletion.reply.code = RpcCode::Delete.into();
        }
        for fs in [&source, &follower] {
            assert!(apply_creation(&mut fs.fs_dir.write(), &deletion)?.kind == ResultKind::Applied);
            assert!(!fs.exists("/empty")?);
            fs.mkdir("/empty", false)?;
            assert!(apply_creation(&mut fs.fs_dir.write(), &deletion)?.reply == deletion.reply);
            assert!(fs.exists("/empty")?);
        }
        Ok(())
    }

    #[test]
    fn native_unlink_preserves_other_links_until_the_last_edge() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let file = fs.create("/original", false)?;
        fs.complete_file("/original", Some(file.id), 0, vec![], "", false, None)?;
        fs.link("/original", "/alias")?;
        for (index, path) in ["/original", "/alias"].into_iter().enumerate() {
            let mut deletion = prepare(&fs, "/unused")?;
            deletion.identity.sequence = index as u64 + 1;
            deletion.fingerprint = [index as u8; 32];
            {
                let dir = fs.fs_dir.read();
                let mut view = BatchView::new(&dir, 1024 * 1024);
                let prepared = commands::delete(&mut view, path, false, 2000)?;
                deletion.edits = prepared.edits;
                deletion.reply.header = prepared.response;
                deletion.reply.code = RpcCode::Delete.into();
            }
            assert!(apply_creation(&mut fs.fs_dir.write(), &deletion)?.kind == ResultKind::Applied);
            assert!(!fs.exists(path)?);
            if index == 0 {
                assert_eq!(fs.file_status("/alias")?.id, file.id);
                assert_eq!(fs.file_status("/alias")?.nlink, 1);
            }
        }
        assert!(fs.fs_dir.read().store.get_inode(file.id, None)?.is_none());
        Ok(())
    }

    #[test]
    fn pre_delete_v3_journal_remains_replayable() -> CommonResult<()> {
        use super::super::transport::{decode_batch, NativeBatch};
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let fs = JournalSystem::fs_only_for_test(&configuration(root.path()))?;
        seed_namespace(&fs)?;
        let bytes = SerdeUtils::serialize(&NativeBatch {
            version: 3,
            commands: vec![prepare(&fs, "/old-v3")?],
            garbage_ack: None,
        })?;
        let batch = decode_batch(&bytes)?;
        assert_eq!(batch.version, 3);
        assert!(
            apply_creation(&mut fs.fs_dir.write(), &batch.commands[0])?.kind == ResultKind::Applied
        );
        assert!(fs.exists("/old-v3")?);
        Ok(())
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
    #[test]
    fn resize_and_assignment_publish_owned_layouts_and_retire_truncated_blocks() -> CommonResult<()>
    {
        use curvine_model::{FileAllocOpts, ProtoUtils, WorkerAddress, WorkerInfo};
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
            ..Default::default()
        });
        let status = fs.create("/sparse", false)?;
        fs.complete_file("/sparse", Some(status.id), 0, vec![], "", false, None)?;
        {
            let mut dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(status.id, None)?.unwrap();
            inode.as_file_mut()?.block_size = 4096;
            let mut batch = dir.get_rocks_store().new_batch();
            batch.write_inode(&inode)?;
            batch.commit()?;
            dir.hsm_enabled = true;
            dir.hsm_track_file("/sparse", inode.as_file_ref()?)?;
        }
        let resize = |len| curvine_proto::FileResizeRequest {
            path: "/sparse".into(),
            inode_id: Some(status.id),
            opts: ProtoUtils::file_alloc_opts_to_pb(FileAllocOpts::with_truncate(len)),
        };
        let mut growth = prepare(&fs, "/unused")?;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::resize(&mut view, &fs, resize(8193), 2000)?;
            growth.edits = prepared.edits;
            growth.reply.header = prepared.response;
            growth.reply.code = RpcCode::ResizeFile.into();
            assert_eq!(
                dir.store
                    .get_inode(status.id, None)?
                    .unwrap()
                    .as_file_ref()?
                    .len,
                0
            );
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &growth)?.kind == ResultKind::Applied);
        let grown = ProtoUtils::file_blocks_from_pb(
            curvine_proto::FileResizeResponse::decode(growth.reply.header.as_slice())?.file_blocks,
        );
        assert_eq!(grown.status.len, 8193);
        assert_eq!(grown.block_locs.len(), 3);
        assert!(grown
            .block_locs
            .iter()
            .all(|block| block.locs.is_empty() && block.block.alloc_opts.is_some()));
        let request = || curvine_proto::AssignWorkerRequest {
            path: "/sparse".into(),
            inode_id: Some(status.id),
            block: ProtoUtils::extend_block_to_pb(grown.block_locs[0].block.clone()),
            exclude_workers: vec![],
            client_address: Default::default(),
        };
        let mut assignment = prepare(&fs, "/unused")?;
        assignment.identity.sequence = 2;
        assignment.fingerprint = [2; 32];
        let allocation;
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::assign_worker(&mut view, &fs, request())?;
            allocation = prepared.allocation;
            assignment.edits = prepared.edits;
            assignment.reply.header = prepared.response;
            assignment.reply.code = RpcCode::AssignWorker.into();
            assert!(dir
                .store
                .get_inode(status.id, None)?
                .unwrap()
                .as_file_ref()?
                .blocks[0]
                .locs
                .is_none());
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &assignment)?.kind == ResultKind::Applied);
        allocation.unwrap().retain();
        let assigned =
            curvine_proto::AssignWorkerResponse::decode(assignment.reply.header.as_slice())?;
        assert_eq!(assigned.block.locs.len(), 1);
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let retry = commands::assign_worker(&mut view, &fs, request())?;
            assert!(
                retry.allocation.is_none(),
                "already assigned block reserved another Worker"
            );
            assert_eq!(retry.response, assignment.reply.header);
        }
        let mut shrink = prepare(&fs, "/unused")?;
        shrink.identity.sequence = 3;
        shrink.fingerprint = [3; 32];
        {
            let dir = fs.fs_dir.read();
            let mut view = BatchView::new(&dir, 1024 * 1024);
            let prepared = commands::resize(&mut view, &fs, resize(0), 3000)?;
            shrink.edits = prepared.edits;
            shrink.reply.header = prepared.response;
            shrink.reply.code = RpcCode::ResizeFile.into();
            assert_eq!(
                dir.store
                    .get_inode(status.id, None)?
                    .unwrap()
                    .as_file_ref()?
                    .blocks
                    .len(),
                3
            );
            assert!(dir
                .get_rocks_store()
                .hsm_scan(super::super::garbage::PREFIX, None, 10)?
                .is_empty());
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &shrink)?.kind == ResultKind::Applied);
        {
            let dir = fs.fs_dir.read();
            assert_eq!(dir.hsm_record(status.id)?.unwrap().state.length, 0);
            assert!(dir
                .store
                .get_inode(status.id, None)?
                .unwrap()
                .as_file_ref()?
                .blocks
                .is_empty());
            assert_eq!(
                dir.get_rocks_store()
                    .hsm_scan(super::super::garbage::PREFIX, None, 10)?
                    .len(),
                3
            );
            assert_eq!(
                dir.get_block_locations(grown.block_locs[0].block.id)?.len(),
                1
            );
        }
        assert!(apply_creation(&mut fs.fs_dir.write(), &assignment)?.reply == assignment.reply);
        assert_eq!(fs.file_status("/sparse")?.len, 0);
        let dir = fs.fs_dir.read();
        let mut view = BatchView::new(&dir, 1024 * 1024);
        let next = commands::resize(&mut view, &fs, resize(1), 4000)?;
        let next = curvine_proto::FileResizeResponse::decode(next.response.as_slice())?.file_blocks;
        assert!(grown
            .block_locs
            .iter()
            .all(|old| old.block.id != next.block_locs[0].block.id));
        Ok(())
    }
}
