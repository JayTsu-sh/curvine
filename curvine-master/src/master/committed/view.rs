// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Owned touched-inode preparation over one committed namespace. References to
//! live directories are read-only and bounded by the caller's FsDir read lock;
//! they never appear in PreparedEdits or survive into a Raft wait.
use crate::master::meta::{
    inode::{InodeView, ROOT_INODE_ID},
    FsDir,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::hsm::native::{ControlChange, Record};
use curvine_model::{BlockLocation, CommitBlock, FileStatus};
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

type Edge = (i64, String);

/// Map iteration order is process-local; CAS fingerprints must describe the
/// logical metadata rather than whichever order bincode emitted its xattrs.
pub(super) fn inode_fingerprint(inode: &InodeView) -> CommonResult<[u8; 32]> {
    let mut value = serde_json::to_value(inode.clone_metadata())?;
    if let Some(clients) = value
        .pointer_mut("/File/file/features/file_write/clients")
        .and_then(serde_json::Value::as_array_mut)
    {
        clients.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
    value.sort_all_objects();
    Ok(*blake3::hash(&serde_json::to_vec(&value)?).as_bytes())
}

#[derive(Default, Serialize, Deserialize)]
pub(super) struct PreparedEdits {
    pub(super) inodes: BTreeMap<i64, Option<InodeView>>,
    pub(super) edges: BTreeMap<Edge, Option<i64>>,
    pub(super) inode_checks: BTreeMap<i64, Option<[u8; 32]>>,
    pub(super) edge_checks: BTreeMap<Edge, Option<i64>>,
    pub(super) child_deltas: BTreeMap<i64, i64>,
    pub(super) last_inode: i64,
    pub(super) hsm_checks: BTreeMap<i64, Option<u64>>,
    pub(super) hsm_records: BTreeMap<i64, Vec<u8>>,
    pub(super) locations: BTreeMap<(i64, u32), BlockLocation>,
    pub(super) controls: Vec<ControlChange>,
}
impl PreparedEdits {
    pub(super) fn encoded_bytes(&self) -> CommonResult<usize> {
        Ok(SerdeUtils::serialize(self)?.len())
    }
}

#[derive(Clone)]
pub(super) struct BatchView<'a> {
    authority: &'a FsDir,
    base_dirs: HashMap<i64, &'a InodeView>,
    inodes: BTreeMap<i64, Option<InodeView>>,
    edges: BTreeMap<Edge, Option<i64>>,
    child_deltas: BTreeMap<i64, i64>,
    hsm_records: BTreeMap<i64, Option<Record>>,
    locations: BTreeMap<(i64, u32), BlockLocation>,
    controls: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    last_inode: i64,
    bytes: usize,
    budget: usize,
    protection_time: Option<crate::master::hsm::read_clock::ReadTime>,
    trusted_trash: bool,
}
impl<'a> BatchView<'a> {
    pub(super) fn new(authority: &'a FsDir, budget: usize) -> Self {
        Self {
            authority,
            base_dirs: [(ROOT_INODE_ID, &authority.root_dir)].into(),
            inodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            child_deltas: BTreeMap::new(),
            hsm_records: BTreeMap::new(),
            locations: BTreeMap::new(),
            controls: BTreeMap::new(),
            last_inode: authority.last_inode_id(),
            bytes: 0,
            budget,
            protection_time: None,
            trusted_trash: false,
        }
    }
    /// Compact namespace inputs are expanded only inside committed apply.
    /// The captured lease clock makes guards identical on every replica.
    pub(super) fn for_namespace(
        authority: &'a FsDir,
        time: crate::master::hsm::read_clock::ReadTime,
    ) -> Self {
        let mut view = Self::new(authority, usize::MAX);
        view.protection_time = Some(time);
        view
    }
    pub(super) fn guard_path(&self, path: &str) -> curvine_error::FsResult<()> {
        self.authority.hsm_guard_path(path)
    }
    pub(super) fn for_trash(
        authority: &'a FsDir,
        time: crate::master::hsm::read_clock::ReadTime,
    ) -> Self {
        let mut view = Self::for_namespace(authority, time);
        view.trusted_trash = true;
        view
    }
    pub(super) fn trusted_trash(&self) -> bool {
        self.trusted_trash
    }
    pub(super) fn authority(&self) -> &FsDir {
        self.authority
    }
    pub(super) fn command<'b>(&'b mut self) -> CommandView<'b, 'a> {
        let last_inode = self.last_inode;
        CommandView {
            batch: self,
            edits: PreparedEdits {
                last_inode,
                ..Default::default()
            },
            estimated: 0,
            poisoned: false,
        }
    }
    fn inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        if let Some(inode) = self.inodes.get(&id) {
            return Ok(inode.as_ref().map(InodeView::clone_metadata));
        }
        if let Some(inode) = self.base_dirs.get(&id) {
            return Ok(Some(inode.clone_metadata()));
        }
        self.authority.store.get_inode(id, None)
    }
    fn base_dir(&mut self, id: i64) -> CommonResult<Option<&'a InodeView>> {
        if let Some(dir) = self.base_dirs.get(&id) {
            return Ok(Some(*dir));
        }
        if self.inodes.contains_key(&id) && self.authority.store.get_inode(id, None)?.is_none() {
            return Ok(None);
        }
        let path = self.authority.get_inode_path(id)?;
        let mut inode: &'a InodeView = &self.authority.root_dir;
        for component in path.split('/').filter(|c| !c.is_empty()) {
            inode = inode
                .get_child(component)
                .ok_or("committed directory edge is missing")?;
        }
        if inode.id() != id || !inode.is_dir() {
            return err_box!("parent inode is not a committed directory");
        }
        self.base_dirs.insert(id, inode);
        Ok(Some(inode))
    }
    fn edge(&mut self, edge: &Edge) -> CommonResult<Option<i64>> {
        if let Some(id) = self.edges.get(edge) {
            return Ok(*id);
        }
        let child = self
            .base_dir(edge.0)?
            .and_then(|dir| dir.get_child(&edge.1));
        if let Some(child) = child.filter(|child| child.is_dir()) {
            self.base_dirs.insert(child.id(), child);
        }
        Ok(child.map(InodeView::id))
    }
    fn child_count(&mut self, id: i64) -> CommonResult<i64> {
        let original = self.base_dir(id)?.map_or(0, |dir| dir.child_len() as i64);
        original
            .checked_add(*self.child_deltas.get(&id).unwrap_or(&0))
            .filter(|n| *n >= 0)
            .ok_or_else(|| "invalid prepared directory child count".into())
    }
    pub(super) fn rebase_records(&mut self, edits: &PreparedEdits) -> CommonResult<()> {
        for (id, json) in &edits.hsm_records {
            self.hsm_records
                .insert(*id, Some(serde_json::from_slice(json)?));
        }
        Ok(())
    }
    pub(super) fn install(&mut self, edits: &PreparedEdits) -> CommonResult<()> {
        let records = edits
            .hsm_records
            .iter()
            .map(|(id, json)| {
                serde_json::from_slice::<Record>(json).map(|record| (*id, Some(record)))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        for (id, inode) in &edits.inodes {
            self.inodes
                .insert(*id, inode.as_ref().map(InodeView::clone_metadata));
        }
        self.edges.extend(edits.edges.clone());
        for (id, delta) in &edits.child_deltas {
            *self.child_deltas.entry(*id).or_default() += delta;
        }
        self.last_inode = self.last_inode.max(edits.last_inode);
        self.hsm_records.extend(records);
        for (id, inode) in &edits.inodes {
            if inode.is_none() {
                self.hsm_records.insert(*id, None);
            }
        }
        self.locations.extend(edits.locations.clone());
        for control in &edits.controls {
            self.controls
                .insert(control.key.clone(), control.value.clone());
        }
        Ok(())
    }
}

pub(super) struct CommandView<'b, 'a> {
    batch: &'b mut BatchView<'a>,
    edits: PreparedEdits,
    estimated: usize,
    poisoned: bool,
}
impl CommandView<'_, '_> {
    pub(super) fn file(&mut self, path: &str, inode_id: Option<i64>) -> CommonResult<InodeView> {
        self.batch.guard_path(path)?;
        let id = match inode_id.filter(|id| *id > 0) {
            Some(id) => id,
            None => {
                let ids = self.resolve(path)?;
                if ids.len() != InodeView::path_components(path)?.len() {
                    return Err(Box::new(curvine_error::FsError::file_not_found(path)));
                }
                *ids.last().unwrap()
            }
        };
        let file = self
            .inode(id)?
            .ok_or_else(|| curvine_error::FsError::file_not_found(path))?;
        file.as_file_ref()?;
        Ok(file)
    }

    pub(super) fn path(&mut self, id: i64) -> CommonResult<String> {
        let mut seen = std::collections::HashSet::new();
        let mut names = vec![];
        let mut id = id;
        while id != ROOT_INODE_ID {
            if !seen.insert(id) {
                return err_box!("cycle in prepared inode parent chain");
            }
            let inode = self.inode(id)?.ok_or("prepared path inode is missing")?;
            names.push(inode.name().to_owned());
            id = match &inode {
                InodeView::Dir(dir) => dir.parent_id,
                InodeView::File(file) => file.parent_id,
                _ => return err_box!("unresolved entry in prepared parent chain"),
            };
        }
        names.reverse();
        Ok(format!("/{}", names.join("/")))
    }

    fn record(&mut self, id: i64) -> CommonResult<Option<Record>> {
        if let Some(json) = self.edits.hsm_records.get(&id) {
            return Ok(Some(serde_json::from_slice(json)?));
        }
        let record = match self.batch.hsm_records.get(&id) {
            Some(record) => record.clone(),
            None => self.batch.authority.hsm_record(id)?,
        };
        if !self.edits.hsm_checks.contains_key(&id) {
            self.charge(32)?;
            self.edits
                .hsm_checks
                .insert(id, record.as_ref().map(|record| record.version));
        }
        Ok(record)
    }

    pub(super) fn guard_write(&mut self, id: i64) -> CommonResult<()> {
        let record = self.record(id)?;
        if let Some(time) = self.batch.protection_time {
            if let Some(record) = record {
                if !record.ordinary
                    || !record.native_blocks
                    || record.seal_token.is_some()
                    || !record.views.is_empty()
                    || record.pending.is_some()
                    || record.readers.values().any(|expiry| time.active(*expiry))
                {
                    return err_box!("HSM operation or active read protects this generation");
                }
            }
            if self
                .batch
                .authority
                .hsm_namespace()?
                .is_some_and(|ns| ns.root_inode == id)
            {
                return err_box!("managed HSM inode requires the native HSM Interface");
            }
        } else {
            self.batch.authority.hsm_guard_id(id)?;
        }
        Ok(())
    }

    pub(super) fn enroll(&mut self, inode: &InodeView) -> CommonResult<()> {
        self.enroll0(inode, false)
    }
    pub(super) fn enroll_overwrite(&mut self, inode: &InodeView) -> CommonResult<()> {
        self.enroll0(inode, true)
    }
    fn enroll0(&mut self, inode: &InodeView, overwrite: bool) -> CommonResult<()> {
        let id = inode.id();
        let previous = self.record(id)?;
        let namespace = self
            .batch
            .authority
            .hsm_namespace()?
            .ok_or("native command has no namespace")?;
        let path = self.path(id)?;
        let build = if overwrite {
            crate::master::hsm::native_overwrite_record
        } else {
            crate::master::hsm::native_record
        };
        if let Some(record) = build(&namespace, &path, inode.as_file_ref()?, previous.as_ref())? {
            let json = serde_json::to_vec(&record)?;
            self.charge(json.len().saturating_add(32))?;
            self.edits.hsm_records.insert(id, json);
        }
        Ok(())
    }

    pub(super) fn guard_delete(&mut self, id: i64) -> CommonResult<()> {
        self.guard_write(id)?;
        if let Some(record) = self.record(id)? {
            if !record.garbage.is_empty() || !record.native_garbage.is_empty() {
                return err_box!("HSM physical cleanup must complete before deletion");
            }
        }
        Ok(())
    }

    pub(super) fn guard_overwrite(&mut self, id: i64) -> CommonResult<()> {
        self.batch.authority.hsm_guard_overwrite(id)?;
        self.record(id)?;
        Ok(())
    }
    fn control(&self, key: &[u8]) -> CommonResult<Option<Vec<u8>>> {
        if let Some(change) = self.edits.controls.iter().find(|change| change.key == key) {
            return Ok(change.value.clone());
        }
        match self.batch.controls.get(key) {
            Some(value) => Ok(value.clone()),
            None => self.batch.authority.get_rocks_store().hsm_get(key),
        }
    }
    /// Physical cleanup survives generation changes and even inode deletion.
    /// Retain replica rows until a Worker deletion acknowledgement removes them.
    pub(super) fn retire_blocks(
        &mut self,
        file: &crate::master::meta::inode::InodeFile,
    ) -> CommonResult<()> {
        self.retire_metas(&file.blocks)
    }
    pub(super) fn retire_metas(
        &mut self,
        blocks: &[crate::master::meta::BlockMeta],
    ) -> CommonResult<()> {
        let namespace = self
            .batch
            .authority
            .hsm_namespace()?
            .ok_or("GC has no namespace")?;
        for meta in blocks {
            let mut locations: BTreeMap<_, _> = meta
                .locs
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|location| (location.worker_id, location))
                .collect();
            locations.extend(
                self.locations(meta.id)?
                    .into_iter()
                    .map(|location| (location.worker_id, location)),
            );
            let locations: Vec<_> = locations.into_values().collect();
            let garbage = super::garbage::Garbage {
                incarnation: namespace.incarnation.clone(),
                block: meta.id,
                locations: locations.clone(),
            };
            let key = super::garbage::key(meta.id);
            let value = SerdeUtils::serialize(&garbage)?;
            let expected = self.control(&key)?;
            if expected.as_ref().is_some_and(|old| old != &value) {
                return err_box!("block retirement identity rebound");
            }
            self.charge(key.len().saturating_add(value.len()).saturating_add(32))?;
            self.edits.controls.push(ControlChange {
                key,
                expected,
                value: Some(value),
            });
            for location in locations {
                self.charge(32)?;
                self.edits
                    .locations
                    .insert((meta.id, location.worker_id), location);
            }
        }
        Ok(())
    }

    pub(super) fn commit_locations(&mut self, commits: &[CommitBlock]) -> CommonResult<()> {
        for commit in commits {
            for location in &commit.locations {
                self.charge(32)?;
                self.edits
                    .locations
                    .insert((commit.block_id, location.worker_id), location.clone());
            }
        }
        Ok(())
    }

    pub(super) fn locations(&self, block: i64) -> CommonResult<Vec<BlockLocation>> {
        let mut locations: BTreeMap<_, _> = self
            .batch
            .authority
            .store
            .get_locations(block)?
            .into_iter()
            .map(|location| (location.worker_id, location))
            .collect();
        for map in [&self.batch.locations, &self.edits.locations] {
            for ((_, worker), location) in map.range((block, 0)..=(block, u32::MAX)) {
                locations.insert(*worker, location.clone());
            }
        }
        Ok(locations.into_values().collect())
    }
    fn charge(&mut self, bytes: usize) -> CommonResult<()> {
        let next = self
            .estimated
            .checked_add(bytes)
            .ok_or("prepared command size overflow")?;
        if self.poisoned || self.batch.bytes.saturating_add(next) > self.batch.budget {
            self.poisoned = true;
            return err_box!("prepared command exceeds byte budget");
        }
        self.estimated = next;
        Ok(())
    }
    pub(super) fn inode(&mut self, id: i64) -> CommonResult<Option<InodeView>> {
        if let Some(inode) = self.edits.inodes.get(&id) {
            return Ok(inode.as_ref().map(InodeView::clone_metadata));
        }
        let inode = self.batch.inode(id)?;
        if !self.edits.inode_checks.contains_key(&id) {
            self.charge(64)?;
            self.edits
                .inode_checks
                .insert(id, inode.as_ref().map(inode_fingerprint).transpose()?);
        }
        Ok(inode)
    }
    pub(super) fn edge(&mut self, parent: i64, name: &str) -> CommonResult<Option<i64>> {
        let edge = (parent, name.to_string());
        if let Some(value) = self.edits.edges.get(&edge) {
            return Ok(*value);
        }
        let inode = self.inode(parent)?.ok_or("prepared parent is missing")?;
        if !inode.is_dir() {
            return err_box!("prepared parent is not a directory");
        }
        let value = if self
            .edits
            .inode_checks
            .get(&parent)
            .is_some_and(Option::is_none)
        {
            None
        } else {
            self.batch.edge(&edge)?
        };
        if !self.edits.edge_checks.contains_key(&edge) {
            self.charge(edge.1.len().saturating_add(48))?;
            self.edits.edge_checks.insert(edge, value);
        }
        Ok(value)
    }
    /// Returns resolved identities only. No owning or borrowed InodePtr leaks
    /// through the preparation boundary.
    pub(super) fn resolve(&mut self, path: &str) -> CommonResult<Vec<i64>> {
        let components = InodeView::path_components(path)?;
        if components.last().is_none_or(String::is_empty)
            || components
                .iter()
                .skip(1)
                .any(|name| name.is_empty() || name == "." || name == ".." || name.contains('\0'))
        {
            return err_box!("invalid empty path component");
        }
        let mut resolved = vec![ROOT_INODE_ID];
        for name in components.iter().skip(1) {
            let parent = *resolved.last().unwrap();
            let Some(id) = self.edge(parent, name)? else {
                break;
            };
            resolved.push(id);
        }
        Ok(resolved)
    }
    pub(super) fn allocate_inode(&mut self) -> CommonResult<i64> {
        // Native preparation and private HSM import share one reservation
        // allocator. Failed/abandoned commands may leave gaps, never aliases.
        let id = self.batch.authority.inode_id.next()?;
        self.edits.last_inode = self.edits.last_inode.max(id);
        Ok(id)
    }
    pub(super) fn reserve_inode(&mut self, id: i64) -> CommonResult<()> {
        if id <= ROOT_INODE_ID
            || id > crate::master::meta::InodeId::ID_MASK
            || self.inode(id)?.is_some()
        {
            return err_box!("captured native inode is invalid or already allocated");
        }
        self.edits.last_inode = self.edits.last_inode.max(id);
        Ok(())
    }
    pub(super) fn put_inode(&mut self, inode: InodeView) -> CommonResult<()> {
        let id = inode.id();
        self.inode(id)?;
        self.charge(
            SerdeUtils::serialize(&inode.clone_metadata())?
                .len()
                .saturating_add(32),
        )?;
        self.edits.inodes.insert(id, Some(inode.clone_metadata()));
        Ok(())
    }
    pub(super) fn delete_inode(&mut self, id: i64) -> CommonResult<()> {
        self.inode(id)?;
        self.charge(32)?;
        self.edits.inodes.insert(id, None);
        Ok(())
    }
    pub(super) fn set_edge(
        &mut self,
        parent: i64,
        name: &str,
        value: Option<i64>,
    ) -> CommonResult<()> {
        let old = self.edge(parent, name)?;
        self.charge(name.len().saturating_add(64))?;
        *self.edits.child_deltas.entry(parent).or_default() +=
            i64::from(value.is_some()) - i64::from(old.is_some());
        self.edits.edges.insert((parent, name.into()), value);
        Ok(())
    }
    pub(super) fn children(&mut self, id: i64) -> CommonResult<Vec<(String, i64)>> {
        let mut children: BTreeMap<String, i64> = self
            .batch
            .base_dir(id)?
            .map(|dir| {
                dir.as_dir_ref().map(|dir| {
                    dir.children_iter()
                        .map(|child| (child.name().to_owned(), child.id()))
                        .collect()
                })
            })
            .transpose()?
            .unwrap_or_default();
        for edges in [&self.batch.edges, &self.edits.edges] {
            for ((_, name), child) in edges
                .range((id, String::new())..)
                .take_while(|((parent, _), _)| *parent == id)
            {
                if let Some(child) = child {
                    children.insert(name.clone(), *child);
                } else {
                    children.remove(name);
                }
            }
        }
        Ok(children.into_iter().collect())
    }
    pub(super) fn subtree(&mut self, root: i64) -> CommonResult<Vec<i64>> {
        let mut stack = vec![root];
        let mut ids = vec![];
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            let inode = self.inode(id)?.ok_or("subtree inode is missing")?;
            if inode.is_dir() {
                stack.extend(self.children(id)?.into_iter().map(|(_, id)| id));
            }
            ids.push(id);
        }
        Ok(ids)
    }
    pub(super) fn status(&mut self, path: &str, id: i64) -> CommonResult<FileStatus> {
        let mut inode = self.inode(id)?.ok_or("prepared status inode is missing")?;
        if id != ROOT_INODE_ID {
            inode.change_name(path.rsplit('/').next().ok_or("invalid status path")?.into());
        }
        let mut status = inode.to_file_status(path)?;
        if inode.is_dir() {
            let original = if self
                .edits
                .inode_checks
                .get(&id)
                .is_some_and(Option::is_none)
            {
                0
            } else {
                self.batch.child_count(id)?
            };
            let count = original
                .checked_add(*self.edits.child_deltas.get(&id).unwrap_or(&0))
                .filter(|n| *n >= 0)
                .ok_or("invalid prepared child count")?;
            status.children_num =
                i32::try_from(count).map_err(|_| "directory child count overflow")?;
            status.len = count;
        }
        Ok(status)
    }
    /// Budget rejection and dropping a failed command leave both the batch's
    /// pending view and the authority unchanged. Only owned edits are returned.
    pub(super) fn finish(self) -> CommonResult<PreparedEdits> {
        if self.poisoned {
            return err_box!("prepared command was rejected by its byte budget");
        }
        if self.batch.protection_time.is_some() {
            // Apply-only namespace expansion has no following speculative
            // command. Avoid serializing/cloning a whole subtree just to
            // enforce the transport budget of an already compact input.
            return Ok(self.edits);
        }
        let size = self.edits.encoded_bytes()?;
        let bytes = self
            .batch
            .bytes
            .checked_add(size)
            .ok_or("prepared batch size overflow")?;
        if bytes > self.batch.budget {
            return err_box!("prepared batch exceeds byte budget");
        }
        self.batch.install(&self.edits)?;
        self.batch.bytes = bytes;
        Ok(self.edits)
    }
}
