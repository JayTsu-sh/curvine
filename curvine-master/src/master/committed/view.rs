// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Owned touched-inode preparation over one committed namespace. References to
//! live directories are read-only and bounded by the caller's FsDir read lock;
//! they never appear in PreparedEdits or survive into a Raft wait.
use crate::master::meta::{
    inode::{InodeView, ROOT_INODE_ID},
    FsDir,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::FileStatus;
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

type Edge = (i64, String);

/// Map iteration order is process-local; CAS fingerprints must describe the
/// logical metadata rather than whichever order bincode emitted its xattrs.
pub(super) fn inode_fingerprint(inode: &InodeView) -> CommonResult<[u8; 32]> {
    let mut value = serde_json::to_value(inode.clone_metadata())?;
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
}
impl PreparedEdits {
    pub(super) fn encoded_bytes(&self) -> CommonResult<usize> {
        Ok(SerdeUtils::serialize(self)?.len())
    }
}

pub(super) struct BatchView<'a> {
    authority: &'a FsDir,
    base_dirs: HashMap<i64, &'a InodeView>,
    inodes: BTreeMap<i64, Option<InodeView>>,
    edges: BTreeMap<Edge, Option<i64>>,
    child_deltas: BTreeMap<i64, i64>,
    last_inode: i64,
    bytes: usize,
    budget: usize,
}
impl<'a> BatchView<'a> {
    pub(super) fn new(authority: &'a FsDir, budget: usize) -> Self {
        Self {
            authority,
            base_dirs: [(ROOT_INODE_ID, &authority.root_dir)].into(),
            inodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            child_deltas: BTreeMap::new(),
            last_inode: authority.last_inode_id(),
            bytes: 0,
            budget,
        }
    }
    pub(super) fn guard_path(&self, path: &str) -> curvine_error::FsResult<()> {
        self.authority.hsm_guard_path(path)
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
    fn install(&mut self, edits: &PreparedEdits) {
        for (id, inode) in &edits.inodes {
            self.inodes
                .insert(*id, inode.as_ref().map(InodeView::clone_metadata));
        }
        self.edges.extend(edits.edges.clone());
        for (id, delta) in &edits.child_deltas {
            *self.child_deltas.entry(*id).or_default() += delta;
        }
        self.last_inode = self.last_inode.max(edits.last_inode);
    }
}

pub(super) struct CommandView<'b, 'a> {
    batch: &'b mut BatchView<'a>,
    edits: PreparedEdits,
    estimated: usize,
    poisoned: bool,
}
impl CommandView<'_, '_> {
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
        self.edits.last_inode = self
            .edits
            .last_inode
            .checked_add(1)
            .filter(|id| *id <= crate::master::meta::InodeId::ID_MASK)
            .ok_or("inode allocation exhausted")?;
        Ok(self.edits.last_inode)
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
        let size = self.edits.encoded_bytes()?;
        let bytes = self
            .batch
            .bytes
            .checked_add(size)
            .ok_or("prepared batch size overflow")?;
        if bytes > self.batch.budget {
            return err_box!("prepared batch exceeds byte budget");
        }
        self.batch.install(&self.edits);
        self.batch.bytes = bytes;
        Ok(self.edits)
    }
}
