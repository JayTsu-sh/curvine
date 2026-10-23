// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::full_block_report::{FullReportProgress, FullReportTracker};
use crate::master::fs::policy::ChooseContext;
use crate::master::fs::worker_manager::WorkerLossToken;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::{InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR};
use crate::master::meta::{CacheInvalidationResult, FsDir};

use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::*;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::ArcRwLock;
use log::{error, info, warn};
use parking_lot::Mutex;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct CvMetadataSnapshotEntry {
    pub status: FileStatus,
    pub blocks: Option<FileBlocks>,
}

pub struct CvMetadataSnapshotPage {
    pub entries: Vec<CvMetadataSnapshotEntry>,
    pub next_page_token: Option<String>,
    pub epoch: u64,
}

struct CompleteFileOptions {
    only_flush: bool,
    return_file_blocks: bool,
    set_attr_opts: Option<SetAttrOpts>,
}

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    full_report_tracker: FullReportTracker,
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
}

pub struct BlockReportResult {
    pub delete_blocks: Vec<i64>,
}

#[derive(Default)]
pub struct LostWorkerLocationCleanup {
    pub removed_block_ids: Vec<i64>,
    pub replication_block_ids: Vec<i64>,
}

pub(crate) enum BlockInodeState {
    File,
    // Durable retirement still needs an actual Worker deletion acknowledgement.
    Retired,
    Missing,
    NotFile,
}

fn child_snapshot_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn snapshot_token_in_subtree(token: &str, subtree_path: &str) -> bool {
    token == subtree_path
        || (subtree_path != "/"
            && token
                .strip_prefix(subtree_path)
                .map(|rest| rest.starts_with(PATH_SEPARATOR))
                .unwrap_or(false))
}

struct FullBlockReconcileState {
    running: bool,
    generation: u64,
    pending: Option<FullBlockReconcileJob>,
}

struct FullBlockReconcileJob {
    generation: u64,
    reported_blocks: HashSet<i64>,
}

const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;

impl MasterFilesystem {
    /// Transitional ordering for TTL/quota/heartbeat scheduled OS threads.
    /// This is not the RPC entry: RPCs already join the async coordinator.
    pub(crate) fn committed_background<T>(
        &self,
        action: impl FnOnce() -> FsResult<T>,
    ) -> FsResult<T> {
        self.committed_background_in_term(None, action, |_| true, |_| {})
    }

    pub(super) fn committed_background_in_term<T>(
        &self,
        originating_term: Option<u64>,
        action: impl FnOnce() -> FsResult<T>,
        needs_confirmation: impl FnOnce(&T) -> bool,
        after_commit: impl FnOnce(&T),
    ) -> FsResult<T> {
        let (enabled, gate, writer) = {
            let dir = self.fs_dir.read();
            (
                dir.hsm_enabled,
                dir.hsm_mutation_gate.clone(),
                dir.journal_writer.clone(),
            )
        };
        if !enabled {
            if let Some(term) = originating_term {
                if !self.master_monitor.is_active()
                    || self
                        .master_monitor
                        .journal_term
                        .load(std::sync::atomic::Ordering::Acquire)
                        != term
                {
                    return Err(FsError::not_leader("lost Worker cleanup leader changed"));
                }
            }
            let result = action()?;
            after_commit(&result);
            return Ok(result);
        }
        let _serial = gate.blocking_write();
        let term = self
            .master_monitor
            .journal_term
            .load(std::sync::atomic::Ordering::Acquire);
        if !self.master_monitor.is_active()
            || term == 0
            || originating_term.is_some_and(|expected| term != expected)
        {
            return Err(FsError::not_leader(
                "background mutation has no active leader",
            ));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.in_origin_term_at(term, action)
        }))
        .unwrap_or_else(|_| {
            log::error!("background mutation panicked after admission; aborting master");
            std::process::abort();
        });
        // Pure derived location retirement does not emit a journal prefix.
        // Errors can follow partial legacy effects, so they always confirm.
        if result.as_ref().map(needs_confirmation).unwrap_or(true) {
            if let Err(error) = writer.commit_pending_blocking() {
                log::error!("background mutation commit uncertain: {error}; aborting master");
                std::process::abort();
            }
        }
        if let Ok(value) = &result {
            // Keep legacy block identities protected until deletion commands
            // have been scheduled. This is still not a durable outbox.
            after_commit(value);
        }
        result
    }
    // Max block-report location updates applied under a single fs_dir write lock.
    const BLOCK_REPORT_WRITE_CHUNK: usize = 4096;
    // Re-enter the background authority queue between pages, not after the
    // complete inventory of a potentially very large Worker.
    const LOST_WORKER_LOCATION_PAGE: usize = 1024;

    pub(crate) fn validate_alloc_capacity(
        current_len: i64,
        replicas: u8,
        opts: &FileAllocOpts,
        available: i64,
    ) -> FsResult<()> {
        if opts.truncate || opts.len <= current_len {
            return Ok(());
        }

        let logical_growth = opts.len - current_len;
        let required = logical_growth.saturating_mul(i64::from(replicas));
        if required > available {
            return err_ext!(FsError::disk_out_of_space(format!(
                "fallocate requires {} bytes for {} replicas, but only {} bytes are available",
                logical_growth, replicas, available
            )));
        }

        Ok(())
    }

    pub fn new(
        conf: &ClusterConf,
        fs_dir: SyncFsDir,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> Self {
        Self {
            fs_dir,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
        }
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> Self {
        Self {
            fs_dir: js.fs().fs_dir.clone(),
            worker_manager: js.worker_manager(),
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
            full_report_tracker: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
        }
    }

    pub fn check_parent(path: &InodePath) -> FsResult<()> {
        // The root directory must exist.All /a does not require verification
        if path.len() > 2 {
            if let Some(v) = path.get_inode(-2) {
                if !v.is_dir() {
                    err_box!(
                        "Parent path is not a directory:: {}",
                        path.get_parent_path()
                    )
                } else {
                    Ok(())
                }
            } else {
                err_box!("Parent directory doesn't exist: {}", path.get_parent_path())
            }
        } else {
            Ok(())
        }
    }

    pub fn print_tree(&self) {
        let fs_dir = self.fs_dir.read();
        fs_dir.print_tree();
    }

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        // Creation of root directory is not allowed
        if inp.is_root() {
            return err_box!("Not allowed to create existing root path: {}", inp.path());
        }

        if inp.is_full() {
            if opts.create_parent {
                if let Some(last_inode) = inp.get_last_inode() {
                    if last_inode.is_dir() {
                        let status = last_inode.to_file_status(inp.path())?;
                        return Ok(status);
                    }
                }
            }
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Check whether the directory can be created recursively.
        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = fs_dir.mkdir(inp, opts)?;
        let last = try_option!(
            inp.get_last_inode(),
            "Path {} has no inode after mkdir",
            inp.path()
        );
        let status = last.to_file_status(inp.path())?;
        Ok(status)
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let mut delete_res = fs_dir.delete(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut delete_res.blocks),
        });

        Ok(delete_res)
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let mut free_res = fs_dir.free(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut free_res.blocks),
        });

        Ok(free_res)
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        let mut fs_dir = self.fs_dir.write();
        let src_inp = Self::resolve_path(&fs_dir, src)?;
        let dst_inp = Self::resolve_path(&fs_dir, dst)?;

        if src_inp.is_root() {
            return err_box!("Cannot rename root path");
        }

        if src == dst {
            return Ok(false);
        }

        // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed (POSIX EINVAL).
        if let Some(rest) = dst.strip_prefix(src) {
            if rest.starts_with(PATH_SEPARATOR) {
                return err_ext!(FsError::invalid_argument(format!(
                    "cannot rename {} to {}: destination is under source",
                    src, dst
                )));
            }
        }

        // EXCHANGE also rejects src under dst (/a/b <-> /a would make /a its own descendant).
        if flags.exchange_mode() {
            if let Some(rest) = src.strip_prefix(dst) {
                if rest.starts_with(PATH_SEPARATOR) {
                    return err_ext!(FsError::invalid_argument(format!(
                        "cannot exchange {} with {}: source is under destination",
                        src, dst
                    )));
                }
            }
        }

        if let Some(del_res) = fs_dir.rename(&src_inp, &dst_inp, flags)? {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&del_res);
        }

        Ok(true)
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx, OpenFlags::new_create().set_overwrite(true))
    }

    fn truncate(&self, fs_dir: &mut FsDir, inp: &InodePath, opts: CreateFileOpts) -> FsResult<()> {
        let clean_result = fs_dir.overwrite_file(inp, opts)?;
        if !clean_result.blocks.is_empty() {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&clean_result);
        }
        Ok(())
    }

    pub(crate) fn validate_create_opts(
        &self,
        path: &str,
        opts: &CreateFileOpts,
        flags: &OpenFlags,
    ) -> FsResult<()> {
        if !flags.create() {
            return err_box!("create_with_opts requires O_CREAT flag");
        }
        // Check the path length
        self.check_path_length(path)?;

        if opts.replicas < self.conf.min_replication || opts.replicas >= self.conf.max_replication {
            return err_box!(
                "The replica number {} needs to be between {} and {}",
                opts.replicas,
                self.conf.min_replication,
                self.conf.max_replication
            );
        }

        if opts.block_size < self.conf.min_block_size || opts.block_size >= self.conf.max_block_size
        {
            return err_box!(
                "Block size needs to be between {} and {}",
                self.conf.min_block_size,
                self.conf.max_block_size
            );
        }

        Ok(())
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        let path = path.as_ref();
        self.validate_create_opts(path, &opts, &flags)?;

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let last_inode = inp.get_last_inode();
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }

            if flags.exclusive() {
                return err_ext!(FsError::file_exists(inp.path()));
            }
        }

        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = if last_inode.is_some() {
            if flags.overwrite() {
                self.truncate(&mut fs_dir, &inp, opts)?;
            } else {
                return err_ext!(FsError::file_exists(inp.path()));
            }
            inp
        } else {
            fs_dir.create_file(inp, opts)?
        };

        let status = fs_dir.file_status(&inp)?;

        Ok(status)
    }

    pub fn open_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();

        if flags.read_only() {
            if flags.truncate() {
                return err_box!("cannot combine O_RDONLY with O_TRUNC");
            }
            if flags.create() {
                return err_box!("cannot combine O_RDONLY with O_CREAT");
            }
            return self.get_block_locations(path);
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            None => {
                return if flags.create() {
                    drop(fs_dir);
                    let status = self.create_with_opts(path, opts, flags)?;
                    Ok(FileBlocks::new(status, vec![]))
                } else {
                    err_ext!(FsError::file_not_found(inp.path()))
                }
            }

            Some(inode) => {
                if inode.is_dir() {
                    return err_box!("{} is a directory", inp.path());
                }
                inode
            }
        };

        if flags.truncate() {
            self.truncate(&mut fs_dir, &inp, opts)?;
            let status = fs_dir.file_status(&inp)?;
            return Ok(FileBlocks::new(status, vec![]));
        }

        let status = fs_dir.reopen_file(&inp, opts.client_name)?;
        let file = inode.as_file_ref()?;
        let blocks = if !file.blocks.is_empty() {
            self.get_block_locs(path, &fs_dir, file)?
        } else {
            vec![]
        };
        Ok(FileBlocks::new(status, blocks))
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        self.file_status_by_id(path, None)
    }

    pub fn file_status_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
    ) -> FsResult<FileStatus> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        Ok(inode.to_file_status(path)?)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Ok(inp.get_last_inode().is_some())
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path.as_ref());
        if is_glob_pattern {
            let paths = Self::resolve_path_by_glob_pattern(&fs_dir, path.as_ref())?;
            let mut all_statuses = Vec::new();
            for path in &paths {
                let statuses = fs_dir.list_status(path)?;
                all_statuses.extend(statuses);
            }
            Ok(all_statuses)
        } else {
            let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
            fs_dir.list_status(&inp)
        }
    }

    pub fn list_options<T: AsRef<str>>(
        &self,
        path: T,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            err_box!("list_options does not support glob pattern, path {}", path)
        } else {
            let inp = Self::resolve_path(&fs_dir, path)?;
            fs_dir.list_options(&inp, &opts)
        }
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn resolve_path_by_glob_pattern(fs_dir: &FsDir, path: &str) -> CommonResult<Vec<InodePath>> {
        InodePath::resolve_for_glob_pattern(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    pub fn check_path_length(&self, path: &str) -> CommonResult<()> {
        if path.len() > self.conf.max_path_len {
            return err_box!(
                "create: Path too long, limit {} characters",
                self.conf.max_path_len
            );
        }

        let depth = path.split(PATH_SEPARATOR).count();
        if depth > self.conf.max_path_depth {
            return err_box!(
                "create: Path too long, limit {} levels",
                self.conf.max_path_depth
            );
        }

        Ok(())
    }

    pub fn validate_add_block(
        file: &InodeFile,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        if let Some(v) = previous {
            if v.block_len != file.block_size as i64 {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas as u16,
            block_size: file.block_size as i64,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn choose_worker(
        &self,
        inp: &InodePath,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;
        self.choose_worker_for_file(file, client_addr, exclude_workers)
    }

    pub fn choose_worker_for_file(
        &self,
        file: &InodeFile,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut wm = self.worker_manager.write();
        let validate_block = Self::validate_add_block(file, &client_addr, None)?;
        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);
        Ok(wm.choose_worker(choose_ctx)?)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        self.worker_manager
            .read()
            .create_locate_block(path, block, locs)
    }

    pub fn resolve_file_inode(
        fs_dir: &FsDir,
        path: &str,
        inode_id: Option<i64>,
    ) -> FsResult<InodePtr> {
        match inode_id {
            Some(v) if v > 0 => match fs_dir.store.get_inode(v, None)? {
                Some(view) => Ok(InodePtr::from_owned(view)),
                None => err_ext!(FsError::file_not_found(path).ctx(format!("inode_id={}", v))),
            },

            _ => {
                let inp = Self::resolve_path(fs_dir, path)?;
                match inp.task_last() {
                    Some(ptr) => Ok(ptr),
                    None => err_ext!(FsError::file_not_found(path)),
                }
            }
        }
    }

    /// Document application to allocate a new block.
    #[allow(clippy::too_many_arguments)]
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        let file = inode.as_file_ref()?;

        // File allows concurrent writes, 'previous' is the previous block,
        // need to check if the next block has already been allocated。
        // If it has been allocated, return that block
        if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
            let locs = fs_dir.get_block_locations(next.id)?;
            let extend_block = ExtendedBlock {
                id: next.id,
                len: next.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: next.alloc_opts.clone(),
            };

            return self.create_locate_block(path, extend_block, &locs);
        }

        let block_size = file.block_size as i64;
        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block =
            match fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len) {
                Ok(block) => block,
                Err(e) => {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                    return Err(e);
                }
            };
        let located = LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        };

        Ok(located)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush,
                return_file_blocks: true,
                set_attr_opts,
            },
        )
    }

    /// Flushes file metadata without building the full block-location snapshot.
    pub fn flush_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush: true,
                return_file_blocks: false,
                set_attr_opts: None,
            },
        )
        .map(|_| ())
    }

    fn complete_file0<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        fs_dir.complete_file(
            path,
            &mut inode,
            len,
            commit_blocks,
            client_name,
            options.only_flush,
            options.set_attr_opts,
        )?;

        if options.only_flush && options.return_file_blocks {
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            let status = inode.to_file_status(path)?;
            return Ok(Some(FileBlocks::new(status, locs)));
        }

        Ok(None)
    }

    pub fn get_file_blocks(
        &self,
        path: &str,
        fs_dir: &FsDir,
        inp: &InodePath,
    ) -> FsResult<FileBlocks> {
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        let file = inode.as_file_ref()?;
        let blocks = self.get_block_locs(path, fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let file_locs = fs_dir.get_file_locations(file)?;
        // Location reads only need the caller's namespace guard. Acquire worker
        // state afterwards so RocksDB lookups do not delay heartbeats.
        let wm = self.worker_manager.read();
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let lc = try_option!(
                file_locs.get(&meta.id),
                "File {}, block {} Lost (no worker can read)",
                path,
                meta.id
            );
            let lb = wm.create_locate_block(path, extend_block, lc)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        let fs_dir = self.fs_dir.read();
        let path = path.as_ref();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(path)),
        };
        let file = inode.as_file_ref()?;
        let block_locs = self.get_block_locs(path, &fs_dir, file)?;
        let locate_blocks = FileBlocks {
            status: inode.to_file_status(path)?,
            block_locs,
        };

        Ok(locate_blocks)
    }

    pub fn get_file_block_details<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlockDetails> {
        let path = path.as_ref();

        let (status, block_snapshots, worker_snapshots) = {
            let fs_dir = self.fs_dir.read();
            let inp = Self::resolve_path(&fs_dir, path)?;
            let inode = match inp.get_last_inode() {
                Some(inode) => inode,
                None => return err_ext!(FsError::file_not_found(path)),
            };
            let file = inode.as_file_ref()?;
            let file_locs = fs_dir.get_file_locations(file)?;
            let status = inode.to_file_status(path)?;
            let mut offset = 0;
            let mut block_snapshots = Vec::with_capacity(file.blocks.len());
            let mut worker_ids = HashSet::new();

            for meta in &file.blocks {
                let len = meta.len();
                let replicas = file_locs.get(&meta.id).cloned().unwrap_or_default();
                for replica in &replicas {
                    worker_ids.insert(replica.worker_id);
                }
                block_snapshots.push((meta.id, len, offset, replicas));
                offset += len;
            }

            // Snapshot worker state under the existing fs_dir -> worker_manager lock order.
            let worker_snapshots = {
                let worker_manager = self.worker_manager.read();
                worker_ids
                    .into_iter()
                    .map(|worker_id| {
                        let (address, state) = if let Some(worker) =
                            worker_manager.get_worker(worker_id)
                        {
                            (Some(worker.address.clone()), worker.status)
                        } else if let Some(worker) = worker_manager.get_known_worker(worker_id) {
                            (Some(worker.address.clone()), WorkerStatus::Lost)
                        } else {
                            (None, WorkerStatus::Unknown)
                        };
                        (worker_id, (address, state))
                    })
                    .collect::<HashMap<_, _>>()
            };

            (status, block_snapshots, worker_snapshots)
        };

        let blocks = block_snapshots
            .into_iter()
            .map(|(block_id, len, offset, locations)| {
                let mut replicas = locations
                    .into_iter()
                    .map(|location| BlockReplicaDetail {
                        worker_id: location.worker_id,
                        storage_type: location.storage_type,
                        address: worker_snapshots
                            .get(&location.worker_id)
                            .and_then(|(address, _)| address.clone()),
                        state: worker_snapshots
                            .get(&location.worker_id)
                            .map(|(_, state)| *state)
                            .unwrap_or(WorkerStatus::Unknown),
                    })
                    .collect::<Vec<_>>();
                replicas.sort_by_key(|replica| replica.worker_id);

                FileBlockDetail {
                    block_id,
                    len,
                    offset,
                    replicas,
                }
            })
            .collect();

        Ok(FileBlockDetails { status, blocks })
    }

    pub fn cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataSnapshotPage> {
        if page_size == 0 {
            return err_box!("cv metadata snapshot page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let epoch = fs_dir.op_id.get();
        let start_after = page_token.filter(|token| !token.is_empty());
        let mut entries = Vec::with_capacity(page_size.saturating_add(1));
        self.collect_cv_metadata_snapshot_page(
            &fs_dir,
            fs_dir.root_dir(),
            "/",
            start_after.as_deref(),
            page_size.saturating_add(1),
            &mut entries,
        )?;

        let next_page_token = if entries.len() > page_size {
            let token = entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.status.path.clone());
            entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataSnapshotPage {
            entries,
            next_page_token,
            epoch,
        })
    }

    fn collect_cv_metadata_snapshot_page(
        &self,
        fs_dir: &FsDir,
        inode: &InodeView,
        path: &str,
        start_after: Option<&str>,
        limit: usize,
        entries: &mut Vec<CvMetadataSnapshotEntry>,
    ) -> FsResult<()> {
        if entries.len() >= limit {
            return Ok(());
        }
        if let Some(token) = start_after {
            if path != "/" && token > path && !snapshot_token_in_subtree(token, path) {
                return Ok(());
            }
        }

        let resolved = self.resolve_snapshot_inode(fs_dir, inode)?;
        if start_after.map(|token| path > token).unwrap_or(true) {
            let status = resolved.to_file_status(path)?;
            let blocks = if let Ok(file) = resolved.as_file_ref() {
                Some(FileBlocks::new(
                    status.clone(),
                    self.get_block_locs(path, fs_dir, file)?,
                ))
            } else {
                None
            };
            entries.push(CvMetadataSnapshotEntry { status, blocks });
            if entries.len() >= limit {
                return Ok(());
            }
        }

        if let InodeView::Dir(dir) = resolved.as_ref() {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_snapshot_page(
                    fs_dir,
                    child,
                    &child_path,
                    start_after,
                    limit,
                    entries,
                )?;
                if entries.len() >= limit {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Directory children live only in the in-memory tree. Borrow that node.
    /// `FileEntry` stores a name and id, so load the file inode for its blocks
    /// and hard-link name. The caller holds the namespace read lock for the walk.
    fn resolve_snapshot_inode<'a>(
        &self,
        fs_dir: &FsDir,
        inode: &'a InodeView,
    ) -> FsResult<Cow<'a, InodeView>> {
        if let InodeView::FileEntry(entry) = inode {
            let loaded = fs_dir
                .store
                .get_inode(entry.id, Some(&entry.name))?
                .ok_or_else(|| FsError::file_not_found(entry.name.clone()))?;
            return Ok(Cow::Owned(loaded));
        }
        Ok(Cow::Borrowed(inode))
    }

    pub fn filesystem_info(&self) -> FsResult<FilesystemInfo> {
        let metrics = Master::get_metrics()?;
        let mut info = FilesystemInfo {
            inode_dir_num: metrics.inode_dir_num.get(),
            inode_file_num: metrics.inode_file_num.get(),
            ..Default::default()
        };

        let wm = self.worker_manager.read();

        // Requests can only reach active master
        info.active_master = wm.conf.master_addr().to_string();
        for peer in &wm.conf.journal.journal_addrs {
            info.journal_nodes.push(peer.to_string())
        }

        for (_, worker) in wm.worker_map.workers() {
            info.capacity += worker.capacity;
            info.available += worker.available;
            info.fs_used += worker.fs_used;
            info.non_fs_used += worker.non_fs_used;
            info.reserved_bytes += worker.reserved_bytes;
            info.block_num += worker.block_num;

            match worker.status {
                WorkerStatus::Live => {
                    info.live_workers.push(worker.clone());
                    // Only Live workers are eligible for new allocations, so the
                    // allocatable view mirrors the allocation policy. Failed
                    // storage dirs are already excluded from worker.capacity.
                    info.allocatable_capacity += worker.capacity;
                    info.allocatable_available += worker.allocatable_available().max(0);
                }
                WorkerStatus::Blacklist => info.blacklist_workers.push(worker.clone()),
                WorkerStatus::Decommission => info.decommission_workers.push(worker.clone()),
                _ => (),
            }
        }

        for (_, worker) in wm.worker_map.lost_workers() {
            info.lost_workers.push(worker.clone());
        }

        Ok(info)
    }

    pub fn fs_dir(&self) -> ArcRwLock<FsDir> {
        self.fs_dir.clone()
    }

    // Add a test worker and unit tests will use it.
    pub fn add_test_worker(&self, worker: WorkerInfo) {
        let mut wm = self.worker_manager.write();
        wm.add_test_worker(worker);
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let fs_dir = self.fs_dir.read();
        fs_dir.sum_hash()
    }

    pub fn last_inode_id(&self) -> i64 {
        let fs_dir = self.fs_dir.read();
        fs_dir.last_inode_id()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        let fs_dir = self.fs_dir.read();
        fs_dir.get_file_counts()
    }

    // Create a directory number based on rocksdb data for testing.
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        let fs_dir = self.fs_dir.read();
        fs_dir.create_tree()
    }

    // Restore in-memory tree from RocksDB (for testing without Raft).
    // In production, Raft automatically restores via apply_snapshot().
    pub fn restore_from_rocksdb(&self) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore_from_rocksdb()
    }

    fn block_inode_state(&self, id: i64) -> FsResult<BlockInodeState> {
        let fs_dir = self.fs_dir.read();
        fs_dir.block_inode_state(id)
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_report_tracker.reset(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    /// Report id of the last fully processed full report of the worker.
    #[doc(hidden)]
    pub fn full_report_completed_round(&self, worker_id: u32) -> Option<u64> {
        self.full_report_tracker.completed_round(worker_id)
    }

    /// Number of workers with a partially accumulated full report.
    #[doc(hidden)]
    pub fn full_report_open_sessions(&self) -> usize {
        self.full_report_tracker.open_sessions()
    }

    fn invalidate_full_block_report_session(&self, worker_id: u32) {
        self.full_report_tracker
            .invalidate_session(worker_id, LocalTime::mills());
    }

    fn invalidate_full_block_state(&self, worker_id: u32) {
        self.invalidate_full_block_report_session(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    fn invalidate_full_block_reconcile(&self, worker_id: u32) {
        let mut reconciles = self.full_block_reconciles.lock();
        if let Some(state) = reconciles.get_mut(&worker_id) {
            state.generation = state.generation.saturating_add(1);
            state.pending = None;
            if !state.running {
                reconciles.remove(&worker_id);
            }
        }
    }

    /// Process block reports
    pub fn block_report(
        &self,
        list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        // @todo check cluster.
        let invalidate_full_reconcile = !list.full_report
            && list.blocks.iter().any(|block| {
                matches!(
                    block.status,
                    BlockReportStatus::Finalized | BlockReportStatus::Writing
                )
            });
        if invalidate_full_reconcile {
            self.invalidate_full_block_state(list.worker_id);
        }

        let full_reported_blocks = match self.full_report_tracker.collect(&list, LocalTime::mills())
        {
            FullReportProgress::Duplicate => {
                // The response to this batch may have been lost, so its delete
                // commands and location updates must still be delivered. Both are
                // idempotent; only the session and the reconcile are skipped.
                info!(
                    "resent batch of completed full block report {} from worker {}: not reconciling again",
                    list.report_id, list.worker_id
                );
                None
            }
            FullReportProgress::InFlight => {
                // The first attempt may still fail, so do not acknowledge the resend.
                // The worker retries IO errors after a short backoff.
                return Err(FsError::io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "full block report {} from worker {} is still being processed",
                        list.report_id, list.worker_id
                    ),
                )));
            }
            FullReportProgress::Complete(blocks) => Some(blocks),
            FullReportProgress::Partial => None,
        };
        if list.blocks.is_empty() && full_reported_blocks.is_none() {
            return Ok(BlockReportResult {
                delete_blocks: Vec::new(),
            });
        }

        //(Whether to increase, block id, block location)
        let mut checked = Vec::with_capacity(list.blocks.len());
        let mut delete_blocks = Vec::new();
        let mut missing_blocks = 0usize;
        let mut not_file_blocks = 0usize;
        for item in list.blocks {
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let defer_writing_delete =
                        item.status == BlockReportStatus::Writing && !list.full_report;
                    let state = match self.block_inode_state(item.id) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("block_report {item:?}: {e}");
                            continue;
                        }
                    };
                    match state {
                        BlockInodeState::File => checked.push((item, Some(BlockInodeState::File))),
                        BlockInodeState::Retired => {
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::Retired)));
                        }
                        BlockInodeState::Missing if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is missing",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::NotFile if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is not a file",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::Missing => {
                            missing_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::Missing)));
                        }
                        BlockInodeState::NotFile => {
                            not_file_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::NotFile)));
                        }
                    }
                }
                BlockReportStatus::Deleted => checked.push((item, None)),
            }
        }
        if missing_blocks > 0 || not_file_blocks > 0 {
            warn!(
                "block_report found {} missing-inode and {} non-file-inode blocks for worker {}; scheduling worker deletion",
                missing_blocks, not_file_blocks, list.worker_id
            );
        }

        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for (item, exists) in checked {
            let loc = BlockLocation::new(list.worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let state = match exists {
                        Some(v) => v,
                        None => {
                            warn!(
                                "block_report invariant violated: missing inode state for block {}",
                                item.id
                            );
                            continue;
                        }
                    };

                    match state {
                        BlockInodeState::File => batch.push((true, item.id, loc)),
                        BlockInodeState::Retired => {
                            // Sending deletion does not acknowledge it. Preserve
                            // this replica until a Deleted report or a complete
                            // report reconciliation proves it absent.
                            batch.push((true, item.id, loc));
                            wm.remove_block(list.worker_id, item.id);
                        }
                        BlockInodeState::Missing | BlockInodeState::NotFile => {
                            batch.push((false, item.id, loc));
                            wm.remove_block(list.worker_id, item.id);
                        }
                    }
                }
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(list.worker_id, item.id);
                }
            }
        }
        drop(wm);

        // Keep the block ids of a completing batch so a failure can be retried.
        let retry_blocks = full_reported_blocks.clone();
        let applied = (|| -> FsResult<()> {
            if let Some(reported_blocks) = full_reported_blocks {
                self.submit_full_block_reconcile(
                    list.worker_id,
                    reported_blocks,
                    replication_handler,
                )?;
            }
            self.apply_block_report_batch(batch)
        })();

        if let Some(blocks) = retry_blocks {
            let now = LocalTime::mills();
            match &applied {
                Ok(()) => self
                    .full_report_tracker
                    .finish(list.worker_id, list.report_id, now),
                Err(_) => self.full_report_tracker.abort(
                    list.worker_id,
                    list.report_id,
                    list.total_len,
                    blocks,
                    now,
                ),
            }
        }
        applied?;

        Ok(BlockReportResult { delete_blocks })
    }

    fn submit_full_block_reconcile(
        &self,
        worker_id: u32,
        reported_blocks: HashSet<i64>,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<()> {
        let should_spawn = {
            let mut reconciles = self.full_block_reconciles.lock();
            let state = reconciles
                .entry(worker_id)
                .or_insert_with(|| FullBlockReconcileState {
                    running: false,
                    generation: 0,
                    pending: None,
                });
            state.generation = state.generation.saturating_add(1);
            let generation = state.generation;
            state.pending = Some(FullBlockReconcileJob {
                generation,
                reported_blocks,
            });
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if !should_spawn {
            return Ok(());
        }

        let fs = self.clone();
        let res = self
            .full_block_reconcile_executor
            .fixed_spawn(worker_id as i64, move || {
                fs.run_full_block_reconcile(worker_id, replication_handler);
            });
        if let Err(e) = &res {
            self.full_block_reconciles.lock().remove(&worker_id);
            error!("submit full block report reconcile for worker {worker_id} failed: {e}");
        }
        res?;
        Ok(())
    }

    fn run_full_block_reconcile(
        &self,
        worker_id: u32,
        replication_handler: Option<MasterReplicationHandler>,
    ) {
        loop {
            let job = {
                let mut reconciles = self.full_block_reconciles.lock();
                match reconciles.get_mut(&worker_id) {
                    Some(state) => match state.pending.take() {
                        Some(v) => v,
                        None => {
                            reconciles.remove(&worker_id);
                            return;
                        }
                    },
                    None => return,
                }
            };

            if !self.is_full_block_reconcile_current(worker_id, job.generation) {
                info!(
                    "skip stale full block report reconcile for worker {}, generation {}",
                    worker_id, job.generation
                );
                continue;
            }

            match self.reconcile_full_block_report(worker_id, job.generation, job.reported_blocks) {
                Ok(stale_block_ids) => {
                    let stale_block_count = stale_block_ids.len();
                    if stale_block_count > 0 {
                        info!(
                            "full block report reconciled {} stale block locations for worker {}",
                            stale_block_count, worker_id
                        );
                        if let Some(replication_handler) = &replication_handler {
                            if let Err(e) = replication_handler
                                .report_under_replicated_blocks(worker_id, stale_block_ids)
                            {
                                error!(
                                    "Errors on reporting under-replicated {} blocks from full block report reconciliation. err: {:?}",
                                    stale_block_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "full block report reconcile for worker {} failed: {}",
                        worker_id, e
                    );
                }
            }
        }
    }

    fn reconcile_full_block_report(
        &self,
        worker_id: u32,
        generation: u64,
        reported_blocks: HashSet<i64>,
    ) -> FsResult<Vec<i64>> {
        let existing_blocks = {
            let fs_dir = self.fs_dir.read();
            fs_dir.get_worker_block_ids(worker_id)?
        };

        let mut stale_block_ids = Vec::new();
        let mut batch = Vec::new();
        for block_id in existing_blocks {
            if !reported_blocks.contains(&block_id) {
                batch.push((false, block_id, BlockLocation::with_id(worker_id)));
                stale_block_ids.push(block_id);
            }
        }

        if !batch.is_empty() {
            let reconciles = self.full_block_reconciles.lock();
            if !reconciles
                .get(&worker_id)
                .map(|state| state.generation == generation)
                .unwrap_or(false)
            {
                info!(
                    "skip stale full block report reconcile apply for worker {}, generation {}",
                    worker_id, generation
                );
                return Ok(Vec::new());
            }
            self.apply_block_report_batch(batch)?;
        }

        Ok(stale_block_ids)
    }

    /// Applies block-report location updates in bounded chunks so the global
    /// fs_dir write lock is held only briefly per chunk. Each entry is an
    /// independent add/remove for one block location, so chunk boundaries do
    /// not break cross-entry invariants.
    fn apply_block_report_batch(&self, batch: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }
            let mut fs_dir = self.fs_dir.write();
            // A successful new location report supersedes the old loss even
            // if its Running heartbeat has not been processed yet. Serialize
            // cancellation with the location publication under fs_dir.
            {
                let mut wm = self.worker_manager.write();
                let mut previous_worker = None;
                for (add, _, location) in &chunk {
                    if *add && previous_worker != Some(location.worker_id) {
                        wm.cancel_worker_loss(location.worker_id);
                        previous_worker = Some(location.worker_id);
                    }
                }
            }
            fs_dir.block_report(chunk)?;
        }
        Ok(())
    }

    fn is_full_block_reconcile_current(&self, worker_id: u32, generation: u64) -> bool {
        self.full_block_reconciles
            .lock()
            .get(&worker_id)
            .map(|state| state.generation == generation)
            .unwrap_or(false)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let term = self.fs_dir.read().hsm_enabled.then(|| {
            self.master_monitor
                .journal_term
                .load(std::sync::atomic::Ordering::Acquire)
        });
        let mut result = LostWorkerLocationCleanup::default();
        self.delete_location_pages(worker_id, None, term, |page| {
            result.removed_block_ids.extend(page.removed_block_ids);
            result
                .replication_block_ids
                .extend(page.replication_block_ids);
        })?;
        Ok(result)
    }

    pub(crate) fn delete_lost_worker_locations(
        &self,
        worker_id: u32,
        token: &WorkerLossToken,
        originating_term: u64,
        on_page: impl FnMut(LostWorkerLocationCleanup),
    ) -> FsResult<usize> {
        self.delete_location_pages(worker_id, Some(token), Some(originating_term), on_page)
    }

    /// Fault-test entry into the real loss-token/term-checked cleanup pipeline.
    #[cfg(feature = "hsm-scale-test")]
    pub fn hsm_test_delete_lost_worker_locations(
        &self,
        worker_id: u32,
        mut on_page: impl FnMut(usize),
    ) -> FsResult<usize> {
        let token = {
            let mut wm = self.worker_manager.write();
            wm.remove_expired_worker(worker_id)
                .ok_or_else(|| FsError::common("test Worker is not registered"))?;
            wm.lost_worker_token(worker_id).unwrap()
        };
        let term = self
            .master_monitor
            .journal_term
            .load(std::sync::atomic::Ordering::Acquire);
        self.delete_lost_worker_locations(worker_id, &token, term, |page| {
            on_page(page.removed_block_ids.len());
        })
    }

    fn delete_location_pages(
        &self,
        worker_id: u32,
        loss: Option<&WorkerLossToken>,
        originating_term: Option<u64>,
        mut on_page: impl FnMut(LostWorkerLocationCleanup),
    ) -> FsResult<usize> {
        let mut after = None;
        let mut removed = 0;
        loop {
            let page = self.committed_background_in_term(originating_term, || {
                let mut fs_dir = self.fs_dir.write();
                // Keep the normal fs_dir -> WorkerManager lock order. Heartbeat
                // registration cannot invalidate the loss between this check
                // and deletion. The guard never spans journal confirmation.
                let wm = self.worker_manager.read();
                if loss.is_some_and(|token| !wm.worker_loss_is_current(worker_id, token)) {
                    return Ok((LostWorkerLocationCleanup::default(), CacheInvalidationResult::default()));
                }
                let removed_block_ids = fs_dir.get_rocks_store().delete_locations_page(
                    worker_id, after, Self::LOST_WORKER_LOCATION_PAGE,
                )?;
                let invalidated = if removed_block_ids.is_empty() {
                    CacheInvalidationResult::default()
                } else {
                    match fs_dir.invalidate_lost_cache_files(worker_id, &removed_block_ids) {
                        Ok(result) => result,
                        Err(error) => {
                            warn!("failed to invalidate lost cache files for worker {worker_id}; continuing with replica recovery: {error}");
                            CacheInvalidationResult { journal_dirty: true, ..Default::default() }
                        }
                    }
                };
                let replication_block_ids = removed_block_ids.iter().copied()
                    .filter(|id| !invalidated.invalidated_block_ids.contains(id)).collect();
                Ok((LostWorkerLocationCleanup { removed_block_ids, replication_block_ids }, invalidated))
            }, |(_, invalidated)| invalidated.journal_dirty, |(_, invalidated)| {
                if !invalidated.delete_result.blocks.is_empty() {
                    self.worker_manager.write().remove_blocks(&invalidated.delete_result);
                }
            })?;
            let (page, _) = page;
            let Some(last) = page.removed_block_ids.last().copied() else {
                return Ok(removed);
            };
            // Replica-manager handoff is outside the authority turn. It does
            // its own current-inode/location checks before scheduling work.
            after = Some(last);
            removed += page.removed_block_ids.len();
            on_page(page);
        }
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        fs_dir.set_attr(inp, opts)
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, mode, None, None)
    }

    pub fn symlink_with_owner_group<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let target = target.as_ref().to_string();
        let link = Self::resolve_path(&fs_dir, link.as_ref())?;
        fs_dir.symlink(target, link, force, mode, owner, group)
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let src_path = Self::resolve_path(&fs_dir, src_path.as_ref())?;
        let dst_path = Self::resolve_path(&fs_dir, dst_path.as_ref())?;
        fs_dir.link(src_path, dst_path)
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        self.resize_by_id(path, None, opts)
    }

    pub fn resize_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        opts: FileAllocOpts,
    ) -> FsResult<FileBlocks> {
        opts.validate()?;

        let path = path.as_ref();
        // This snapshot only rejects individually impossible requests; it is not a
        // reservation, so concurrent fallocates may observe the same capacity.
        // Worker-side block allocation remains the hard enforcement point.
        let available = if opts.truncate {
            i64::MAX
        } else {
            self.worker_manager.read().available_bytes()
        };

        let (del_res, inode_id) = {
            let mut fs_dir = self.fs_dir.write();
            let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
            let file = inode.as_file_ref()?;
            Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
            let inode_id = inode.id();
            let del_res = fs_dir.resize_inode(path, &mut inode, opts)?;
            (del_res, inode_id)
        };

        if !del_res.blocks.is_empty() {
            self.worker_manager.write().remove_blocks(&del_res);
        }

        let blocks = {
            let fs_dir = self.fs_dir.read();
            let inode = Self::resolve_file_inode(&fs_dir, path, Some(inode_id))?;
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            FileBlocks::new(inode.to_file_status(path)?, locs)
        };

        Ok(blocks)
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        self.assign_worker_by_id(path, None, block, client_addr, exclude_workers)
    }

    pub fn assign_worker_by_id<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;

        let file = inode.as_file_ref()?;
        let block_size = file.block_size as i64;
        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block = match fs_dir.assign_worker_inode(path, &mut inode, block.id, &choose_workers) {
            Ok((block, assigned)) => {
                if !assigned {
                    self.worker_manager
                        .write()
                        .unschedule_chosen_workers(&choose_workers, block_size);
                }
                block
            }
            Err(e) => {
                self.worker_manager
                    .write()
                    .unschedule_chosen_workers(&choose_workers, block_size);
                return Err(e);
            }
        };

        Ok(LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        })
    }

    pub fn get_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path)?;
        let expire_ms = self.conf.lock_expire_time_ms();

        fs_dir.get_lock(inp, &lock, expire_ms)
    }

    pub fn set_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        fs_dir.set_lock(inp, lock, self.conf.lock_expire_time_ms())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::ErrorExt;
    use curvine_error::ErrorKind;
    use curvine_runtime::common::Utils;

    fn test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn lost_worker_fixture(name: &str, blocks: usize) -> (MasterFilesystem, i64, Vec<i64>) {
        use crate::master::meta::{BlockMeta, InodeId};
        let fs = test_fs(name);
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        fs.master_monitor
            .journal_term
            .store(7, std::sync::atomic::Ordering::Release);
        let status = fs.create("/payload", false).unwrap();
        fs.add_test_worker(worker_with_status(7, WorkerStatus::Live, 10000, 10000));
        let ids: Vec<_> = (0..blocks)
            .map(|index| InodeId::create_block_id(status.id, index as i64).unwrap())
            .collect();
        {
            let dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(status.id, None).unwrap().unwrap();
            inode.as_file_mut().unwrap().blocks =
                ids.iter().map(|id| BlockMeta::new(*id, 0)).collect();
            let mut batch = dir.get_rocks_store().new_batch();
            batch.write_inode(&inode).unwrap();
            for id in &ids {
                batch.add_location(*id, &BlockLocation::with_id(7)).unwrap();
                batch.add_location(*id, &BlockLocation::with_id(8)).unwrap();
            }
            batch.commit().unwrap();
        }
        (fs, status.id, ids)
    }

    fn lose_worker(fs: &MasterFilesystem) -> WorkerLossToken {
        let mut wm = fs.worker_manager.write();
        wm.remove_expired_worker(7).unwrap();
        wm.lost_worker_token(7).unwrap()
    }

    #[test]
    fn lost_worker_cleanup_streams_pages_without_holding_namespace_locks() -> FsResult<()> {
        let (fs, _, ids) = lost_worker_fixture("lost-worker-streaming", 2050);
        let token = lose_worker(&fs);
        let mut pages = Vec::new();
        let removed = fs.delete_lost_worker_locations(7, &token, 7, |page| {
            pages.push(page.removed_block_ids.len());
            assert_eq!(page.replication_block_ids, page.removed_block_ids);
            // This used to happen only after the whole Worker inventory. Each
            // callback runs with neither namespace nor WorkerManager locked.
            fs.create(format!("/during-cleanup-{}", pages.len()), false)
                .unwrap();
            assert_eq!(
                fs.fs_dir
                    .read()
                    .get_block_locations(ids[2049])
                    .unwrap()
                    .len(),
                if pages.len() < 3 { 2 } else { 1 }
            );
        })?;
        assert_eq!(removed, 2050);
        assert_eq!(pages, [1024, 1024, 2]);
        assert!(fs.fs_dir.read().get_worker_block_ids(7)?.is_empty());
        assert_eq!(fs.fs_dir.read().get_worker_block_ids(8)?.len(), 2050);
        Ok(())
    }

    #[test]
    fn lost_worker_pure_locations_do_not_require_a_journal_receipt() -> FsResult<()> {
        let (fs, _, _) = lost_worker_fixture("lost-worker-no-journal", 1025);
        let token = lose_worker(&fs);
        // Exercise the HSM gate with a test-only writer that cannot produce
        // committed receipts. A namespace mutation would still require one;
        // retiring derived positions has no journal to confirm.
        fs.fs_dir.write().hsm_enabled = true;
        let mut pages = Vec::new();
        assert_eq!(
            fs.delete_lost_worker_locations(7, &token, 7, |page| {
                pages.push(page.removed_block_ids.len());
            })?,
            1025
        );
        assert_eq!(pages, [1024, 1]);
        assert!(fs.fs_dir.read().get_worker_block_ids(7)?.is_empty());
        Ok(())
    }

    #[test]
    fn lost_worker_cleanup_cancels_after_new_report_before_running_heartbeat() -> FsResult<()> {
        let (fs, _, ids) = lost_worker_fixture("lost-worker-new-report", 1025);
        let token = lose_worker(&fs);
        let removed = fs.delete_lost_worker_locations(7, &token, 7, |_| {
            fs.block_report(
                BlockReportList {
                    cluster_id: "test".into(),
                    worker_id: 7,
                    full_report: false,
                    total_len: 2,
                    report_id: 0,
                    blocks: vec![
                        BlockReportInfo::new(
                            ids[0],
                            BlockReportStatus::Finalized,
                            StorageType::Disk,
                            0,
                        ),
                        BlockReportInfo::new(
                            ids[1024],
                            BlockReportStatus::Finalized,
                            StorageType::Disk,
                            0,
                        ),
                    ],
                },
                None,
            )
            .unwrap();
        })?;
        assert_eq!(removed, 1024);
        assert!(!fs.worker_manager.read().worker_loss_is_current(7, &token));
        assert_eq!(fs.fs_dir.read().get_worker_block_ids(7)?.len(), 2);
        Ok(())
    }

    #[test]
    fn lost_worker_cleanup_does_not_continue_in_a_new_leader_term() -> FsResult<()> {
        let (fs, _, ids) = lost_worker_fixture("lost-worker-new-term", 1025);
        let token = lose_worker(&fs);
        let result = fs.delete_lost_worker_locations(7, &token, 7, |_| {
            fs.master_monitor
                .journal_term
                .store(8, std::sync::atomic::Ordering::Release);
        });
        assert!(result.is_err());
        assert_eq!(fs.fs_dir.read().get_worker_block_ids(7)?, vec![ids[1024]]);
        Ok(())
    }

    #[test]
    fn lost_worker_cache_invalidation_ignores_locations_in_later_pages() -> FsResult<()> {
        let (fs, inode_id, ids) = lost_worker_fixture("lost-worker-cache-pages", 1025);
        {
            let dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(inode_id, None)?.unwrap();
            let file = inode.as_file_mut()?;
            file.storage_policy.state = StorageState::Both;
            file.storage_policy.ttl_action = TtlAction::Delete;
            let mut batch = dir.get_rocks_store().new_batch();
            batch.write_inode(&inode)?;
            // The first page has surviving replicas; only the last page has
            // an unreadable block. Invalidate on the first visit nonetheless.
            batch.delete_location(ids[1024], 8)?;
            batch.commit()?;
        }
        let token = lose_worker(&fs);
        let mut pages = 0;
        assert_eq!(
            fs.delete_lost_worker_locations(7, &token, 7, |page| {
                pages += 1;
                assert!(page.replication_block_ids.is_empty());
                assert!(fs
                    .file_status("/payload")
                    .unwrap()
                    .storage_policy
                    .ufs_only());
            })?,
            1025
        );
        assert_eq!(pages, 2);
        Ok(())
    }

    #[test]
    fn hsm_block_reports_reject_old_layouts_and_keep_retirement_until_deleted() -> FsResult<()> {
        use crate::master::meta::{BlockMeta, InodeId};
        use curvine_model::hsm::native::Namespace;
        let fs = test_fs("hsm-retired-report");
        fs.worker_manager.write().add_test_worker(WorkerInfo {
            address: WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            ..Default::default()
        });
        let status = fs.create("/payload", false)?;
        let old = InodeId::create_block_id(status.id, 0)?;
        let live = InodeId::create_block_id(status.id, 1)?;
        let mut record = {
            let mut dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(status.id, None)?.unwrap();
            inode.as_file_mut()?.blocks.push(BlockMeta::new(live, 0));
            let mut batch = dir.get_rocks_store().new_batch();
            batch.write_inode(&inode)?;
            batch.commit()?;
            // Exercise an enabled authority after fixture construction without
            // starting a real cluster in this synchronous block-report test.
            dir.hsm_enabled = true;
            crate::master::hsm::native_record(
                &Namespace {
                    ordinary_format: 2,
                    root_inode: 1,
                    storage: "storage".into(),
                    incarnation: "namespace".into(),
                    epoch: 1,
                },
                "/payload",
                inode.as_file_ref()?,
                None,
            )?
            .unwrap()
        };
        let report = |id, state| BlockReportList {
            cluster_id: "test".into(),
            worker_id: 1,
            full_report: false,
            total_len: 1,
            report_id: 0,
            blocks: vec![BlockReportInfo::new(id, state, StorageType::Disk, 0)],
        };
        let stale = fs.block_report(report(old, BlockReportStatus::Finalized), None)?;
        assert!(
            stale.delete_blocks.contains(&old),
            "an inode's existence must not revive a retired layout"
        );
        assert!(fs.fs_dir.read().get_block_locations(old)?.is_empty());
        // Release's Record-based outbox also needs durable replica sentinels.
        record
            .native_garbage
            .insert(old, vec![BlockLocation::with_id(1)]);
        {
            let dir = fs.fs_dir.read();
            let mut batch = dir.get_rocks_store().new_batch();
            crate::master::hsm::write_native_record(&mut batch, None, &record)?;
            batch.commit()?;
        }
        assert_eq!(fs.fs_dir.read().get_block_locations(old)?.len(), 1);
        let stale = fs.block_report(report(old, BlockReportStatus::Finalized), None)?;
        assert!(stale.delete_blocks.contains(&old));
        assert_eq!(
            fs.fs_dir.read().get_block_locations(old)?.len(),
            1,
            "a delete command is not a deletion acknowledgement"
        );
        fs.block_report(report(old, BlockReportStatus::Deleted), None)?;
        assert!(fs.fs_dir.read().get_block_locations(old)?.is_empty());
        {
            let dir = fs.fs_dir.read();
            let mut next = record.clone();
            next.native_garbage.clear();
            next.version += 1;
            let mut batch = dir.get_rocks_store().new_batch();
            crate::master::hsm::write_native_record(&mut batch, Some(&record), &next)?;
            batch.commit()?;
        }
        let stale = fs.block_report(report(old, BlockReportStatus::Finalized), None)?;
        assert!(stale.delete_blocks.contains(&old));
        assert!(fs.fs_dir.read().get_block_locations(old)?.is_empty());
        assert!(fs
            .block_report(report(live, BlockReportStatus::Finalized), None)?
            .delete_blocks
            .is_empty());
        assert_eq!(fs.fs_dir.read().get_block_locations(live)?.len(), 1);
        Ok(())
    }

    fn assert_file_not_found_roundtrip(err: &FsError) {
        assert!(
            matches!(err.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound, got {:?}",
            err.kind()
        );
        let decoded = FsError::decode(err.encode());
        assert!(
            matches!(decoded.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound after encode/decode, got {:?}",
            decoded.kind()
        );
        assert!(
            matches!(decoded, FsError::FileNotFound(_)),
            "decoded error collapsed away from FileNotFound: {}",
            decoded
        );
    }

    #[test]
    fn fallocate_rejects_growth_larger_than_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        let err = MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 359).unwrap_err();
        assert!(matches!(err, FsError::DiskOutOfSpace(_)));
    }

    #[test]
    fn fallocate_accepts_exact_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 360).is_ok());
    }

    #[test]
    fn truncate_growth_does_not_require_physical_capacity() {
        let opts = FileAllocOpts::with_truncate(200);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 0).is_ok());
    }

    fn replication_file(fs: &MasterFilesystem, path: &str) -> i64 {
        for worker_id in [100, 200, 300] {
            let mut worker = WorkerInfo::default();
            worker.address.worker_id = worker_id;
            fs.add_test_worker(worker);
        }
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create())
            .unwrap();
        let block = fs
            .add_block(
                path,
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .unwrap();
        let block_id = block.block.id;
        fs.fs_dir
            .write()
            .add_block_location(block_id, BlockLocation::with_id(100))
            .unwrap();
        block_id
    }

    #[test]
    fn multiblock_open_preserves_order_and_filters_current_worker_state() {
        for count in [0, 1, 16, 256] {
            let fs = test_fs(&format!("multiblock-open-{count}"));
            let worker = |id, status| {
                let mut worker = WorkerInfo::default();
                worker.address.worker_id = id;
                worker.status = status;
                worker
            };
            for id in [100, 200] {
                fs.add_test_worker(worker(id, WorkerStatus::Live));
            }
            let path = "/multiblock";
            fs.create_with_opts(
                path,
                CreateFileOptsBuilder::new()
                    .block_size(1_048_576)
                    .replicas(2)
                    .build(),
                OpenFlags::new_create(),
            )
            .unwrap();
            let mut commits = Vec::new();
            let mut previous = None;
            for index in 0..count {
                let located = fs
                    .add_block(
                        path,
                        None,
                        ClientAddress::default(),
                        commits.clone(),
                        vec![],
                        index * 1_048_576,
                        previous,
                    )
                    .unwrap();
                previous = Some(located.block.clone());
                commits.push(CommitBlock {
                    block_id: located.block.id,
                    block_len: 1_048_576,
                    locations: vec![BlockLocation::with_id(100), BlockLocation::with_id(200)],
                });
            }
            fs.complete_file(
                path,
                None,
                count * 1_048_576,
                commits.clone(),
                "",
                false,
                None,
            )
            .unwrap();
            let expected_ids = commits
                .iter()
                .map(|block| block.block_id)
                .collect::<Vec<_>>();
            let check = |blocks: FileBlocks, workers: &[u32]| {
                assert_eq!(blocks.status.len, count * 1_048_576);
                assert_eq!(
                    blocks
                        .block_locs
                        .iter()
                        .map(|block| block.block.id)
                        .collect::<Vec<_>>(),
                    expected_ids
                );
                for block in blocks.block_locs {
                    assert_eq!(block.block.len, 1_048_576);
                    assert_eq!(
                        block
                            .locs
                            .iter()
                            .map(|loc| loc.worker_id)
                            .collect::<Vec<_>>(),
                        workers
                    );
                }
            };
            check(fs.get_block_locations(path).unwrap(), &[100, 200]);
            check(
                fs.open_file(path, CreateFileOpts::with_create(false), OpenFlags::new(0))
                    .unwrap(),
                &[100, 200],
            );
            let details = fs.get_file_block_details(path).unwrap();
            assert_eq!(
                details
                    .blocks
                    .iter()
                    .map(|block| block.block_id)
                    .collect::<Vec<_>>(),
                expected_ids
            );

            fs.add_test_worker(worker(100, WorkerStatus::Blacklist));
            check(fs.get_block_locations(path).unwrap(), &[200]);
            fs.add_test_worker(worker(200, WorkerStatus::Decommission));
            if count == 0 {
                check(fs.get_block_locations(path).unwrap(), &[]);
            } else {
                assert!(fs
                    .get_block_locations(path)
                    .unwrap_err()
                    .to_string()
                    .contains("all replicas has been lost"));
            }
        }
    }

    #[test]
    fn replication_state_rejects_deleted_block() {
        let fs = test_fs("replication-state-deleted");
        let path = "/replication-state-deleted.log";
        let block_id = replication_file(&fs, path);
        assert!(fs
            .fs_dir
            .read()
            .replication_block_state(block_id)
            .unwrap()
            .is_some());

        fs.delete(path, false).unwrap();
        let mut fs_dir = fs.fs_dir.write();
        assert!(fs_dir.replication_block_state(block_id).unwrap().is_none());
        assert!(!fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
            .unwrap());
    }

    #[test]
    fn replication_state_rejects_overwritten_block() {
        let fs = test_fs("replication-state-overwritten");
        let path = "/replication-state-overwritten.log";
        let old_block_id = replication_file(&fs, path);

        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();

        let mut fs_dir = fs.fs_dir.write();
        assert!(fs_dir
            .replication_block_state(old_block_id)
            .unwrap()
            .is_none());
        assert!(!fs_dir
            .add_replication_location_if_needed(old_block_id, BlockLocation::with_id(200))
            .unwrap());
    }

    #[test]
    fn replication_location_update_is_idempotent_and_respects_replica_count() {
        let fs = test_fs("replication-location-guard");
        let block_id = replication_file(&fs, "/replication-location-guard.log");
        let mut fs_dir = fs.fs_dir.write();

        assert!(fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
            .unwrap());
        assert!(
            !fs_dir
                .add_replication_location_if_needed(
                    block_id,
                    BlockLocation::new(200, StorageType::Ssd),
                )
                .unwrap()
        );
        assert!(!fs_dir
            .add_replication_location_if_needed(block_id, BlockLocation::with_id(300))
            .unwrap());

        let state = fs_dir.replication_block_state(block_id).unwrap().unwrap();
        assert_eq!(state.replicas, 2);
        assert_eq!(state.locations.len(), 2);
        assert_eq!(
            state
                .locations
                .iter()
                .map(|location| location.worker_id)
                .collect::<Vec<_>>(),
            vec![100, 200]
        );
    }

    #[test]
    fn resolve_file_inode_missing_inode_id_returns_file_not_found() {
        let fs = test_fs("missing-inode-id");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let missing_id = 9_999_999_i64;
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, "/missing", Some(missing_id))
            .unwrap_err();

        assert_file_not_found_roundtrip(&err);
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            msg
        );
    }

    #[test]
    fn resolve_file_inode_unresolved_path_returns_file_not_found() {
        let fs = test_fs("unresolved-path");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let path = "/does/not/exist";
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, path, None).unwrap_err();

        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string().contains(path),
            "expected path in error, got: {}",
            err
        );
    }

    #[test]
    fn get_inode_path_follows_parent_links() {
        let fs = test_fs("get-inode-path");
        let status = fs.create("/dir/file.log", true).unwrap();
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/file.log");
    }

    #[test]
    fn get_inode_path_after_rename_returns_new_path() {
        let fs = test_fs("get-inode-path-rename");
        let status = fs.create("/dir/old.log", true).unwrap();
        fs.rename("/dir/old.log", "/dir/new.log", RenameFlags::empty())
            .unwrap();

        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        assert_eq!(fs_dir.get_inode_path(status.id).unwrap(), "/dir/new.log");
    }

    #[test]
    fn cv_metadata_snapshot_page_walks_live_children_and_loads_file_entries() {
        let fs = test_fs("snapshot-page-borrow");
        let source = fs.create("/a/b.txt", true).unwrap();
        fs.create("/a/c/d.txt", true).unwrap();
        fs.create("/z.txt", true).unwrap();
        fs.link("/a/b.txt", "/a/b-link.txt").unwrap();

        let mut token = None;
        let mut paths = Vec::new();
        let mut pages = 0;
        loop {
            let page = fs.cv_metadata_snapshot_page(token, 2).unwrap();
            assert!(pages < 8, "snapshot pagination did not finish");
            pages += 1;
            assert!(page.entries.len() <= 2);
            paths.extend(page.entries.iter().map(|entry| entry.status.path.clone()));
            match page.next_page_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }

        assert_eq!(
            paths,
            vec![
                "/".to_string(),
                "/a".to_string(),
                "/a/b-link.txt".to_string(),
                "/a/b.txt".to_string(),
                "/a/c".to_string(),
                "/a/c/d.txt".to_string(),
                "/z.txt".to_string(),
            ]
        );

        let full = fs.cv_metadata_snapshot_page(None, 100).unwrap();
        assert!(full.next_page_token.is_none());
        let dir = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a")
            .unwrap();
        assert!(dir.status.is_dir);
        assert!(dir.blocks.is_none());
        assert_eq!(dir.status.children_num, 3);

        let link = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a/b-link.txt")
            .unwrap();
        let file = full
            .entries
            .iter()
            .find(|entry| entry.status.path == "/a/b.txt")
            .unwrap();
        assert_eq!(link.status.name, "b-link.txt");
        assert_eq!(link.status.id, source.id);
        assert_eq!(file.status.id, source.id);
        assert!(link.blocks.is_some());
        assert!(file.blocks.is_some());
    }

    #[test]
    fn file_status_by_id_after_rename_resolves_inode_keeps_request_path() {
        // By-id status resolves the inode (id/name) but keeps the request path.
        let fs = test_fs("file-status-by-id-stale-path");
        let created = fs.create("/a/old.log", true).unwrap();
        fs.rename("/a/old.log", "/a/new.log", RenameFlags::empty())
            .unwrap();

        let status = fs
            .file_status_by_id("/a/old.log", Some(created.id))
            .unwrap();
        assert_eq!(status.id, created.id);
        assert_eq!(status.path, "/a/old.log");
        assert_eq!(status.name, "new.log");

        // Path-based status still requires the live path.
        let by_path = fs.file_status("/a/new.log").unwrap();
        assert_eq!(by_path.id, created.id);
        assert_eq!(by_path.path, "/a/new.log");

        // Write path still journals/returns the request path (no hot-path rebuild).
        let blocks = fs
            .resize_by_id(
                "/a/old.log",
                Some(created.id),
                FileAllocOpts::with_truncate(128),
            )
            .unwrap();
        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/a/old.log");
        assert_eq!(blocks.status.name, "new.log");
        assert_eq!(blocks.status.len, 128);
    }

    #[test]
    fn file_status_by_id_missing_inode_returns_file_not_found() {
        let fs = test_fs("file-status-missing-inode");
        let missing_id = 9_999_999_i64;
        let err = fs
            .file_status_by_id("/missing.log", Some(missing_id))
            .unwrap_err();
        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string()
                .contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            err
        );
    }

    #[test]
    fn complete_file_by_id_keeps_request_path() {
        let fs = test_fs("complete-by-id-stale-path");
        let created = fs.create("/b/old.log", true).unwrap();
        fs.rename("/b/old.log", "/b/new.log", RenameFlags::empty())
            .unwrap();

        let blocks = fs
            .complete_file(
                "/b/old.log",
                Some(created.id),
                64,
                vec![],
                "test-client",
                true,
                None,
            )
            .unwrap()
            .expect("only_flush should return FileBlocks");

        assert_eq!(blocks.status.id, created.id);
        assert_eq!(blocks.status.path, "/b/old.log");
        assert_eq!(blocks.status.name, "new.log");
    }

    fn worker_with_status(
        id: u32,
        status: WorkerStatus,
        capacity: i64,
        available: i64,
    ) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            1,
        );
        worker.status = status;
        worker.capacity = capacity;
        worker.available = available;
        worker
    }

    #[test]
    fn filesystem_info_allocatable_only_counts_live_workers() {
        // See issue #1460: statfs must report capacity eligible for new writes,
        // i.e. only Live workers. Blacklist/Decommission workers must not
        // contribute to the allocatable view (they still count toward the total
        // physical capacity).
        let fs = test_fs("allocatable-live-only");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.add_test_worker(worker_with_status(2, WorkerStatus::Blacklist, 2000, 1500));
        fs.add_test_worker(worker_with_status(
            3,
            WorkerStatus::Decommission,
            3000,
            2500,
        ));

        let info = fs.filesystem_info().unwrap();

        // Total physical capacity across all non-lost worker states.
        assert_eq!(info.capacity, 6000, "total capacity should sum all workers");
        assert_eq!(
            info.available, 4800,
            "total available should sum all workers"
        );
        assert_eq!(info.live_workers.len(), 1);
        assert_eq!(info.blacklist_workers.len(), 1);
        assert_eq!(info.decommission_workers.len(), 1);

        // Allocatable view mirrors allocation eligibility: Live workers only.
        assert_eq!(
            info.allocatable_capacity, 1000,
            "allocatable capacity must be Live workers only"
        );
        assert_eq!(
            info.allocatable_available, 800,
            "allocatable available must be Live workers only"
        );
    }

    #[test]
    fn filesystem_info_allocatable_excludes_failed_storage_dirs() {
        // A Live worker with a healthy dir plus a failed dir: add_storage already
        // skips failed dirs, so neither total nor allocatable capacity should
        // include the failed dir's bytes.
        let fs = test_fs("allocatable-excludes-failed");
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 10,
                ..Default::default()
            },
            1,
        );
        worker.status = WorkerStatus::Live;
        worker.add_storage(StorageInfo {
            dir_id: 1,
            storage_id: "healthy".into(),
            failed: false,
            capacity: 1000,
            available: 800,
            ..Default::default()
        });
        worker.add_storage(StorageInfo {
            dir_id: 2,
            storage_id: "failed".into(),
            failed: true,
            capacity: 500,
            available: 400,
            ..Default::default()
        });
        fs.add_test_worker(worker);

        let info = fs.filesystem_info().unwrap();

        assert_eq!(
            info.capacity, 1000,
            "failed dir must not count toward total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(info.allocatable_capacity, 1000);
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn filesystem_info_allocatable_excludes_lost_workers() {
        // Lost workers live in a separate map and must not contribute to either
        // the total or the allocatable view. Insert a lost worker directly into
        // the lost map (add_test_worker only touches the live map) and assert its
        // capacity never leaks into aggregation.
        let fs = test_fs("allocatable-excludes-lost");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.worker_manager
            .write()
            .worker_map
            .lost_workers
            .insert(99, worker_with_status(99, WorkerStatus::Lost, 9999, 9999));

        let info = fs.filesystem_info().unwrap();

        // The lost worker is reported but its bytes stay out of both views.
        assert_eq!(info.lost_workers.len(), 1);
        assert_eq!(
            info.capacity, 1000,
            "lost worker capacity must not leak into total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(
            info.allocatable_capacity, 1000,
            "lost worker capacity must not leak into allocatable"
        );
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn worker_info_rest_json_omits_scheduled_fields() {
        let mut worker = worker_with_status(1, WorkerStatus::Live, 1000, 800);
        worker.scheduled_bytes = 128;
        worker.scheduled_since_ms = 99;
        let json = serde_json::to_string(&worker).unwrap();
        assert!(!json.contains("scheduled_bytes"));
        assert!(!json.contains("scheduled_since_ms"));
    }

    #[test]
    fn assign_worker_retry_does_not_leak_scheduled_bytes() {
        let fs = test_fs("assign-worker-retry-unschedule");
        let capacity = 1 << 30;
        fs.add_test_worker(worker_with_status(
            1,
            WorkerStatus::Live,
            capacity,
            capacity,
        ));

        let created = fs.create("/retry.log", true).unwrap();
        let blocks = fs
            .resize(
                "/retry.log",
                FileAllocOpts::with_alloc(created.block_size, FileAllocMode::DEFAULT),
            )
            .unwrap();
        let located = &blocks.block_locs[0];
        assert!(located.block.alloc_opts.is_some());
        assert!(located.locs.is_empty());
        let block = located.block.clone();
        let client = ClientAddress::default();

        fs.assign_worker("/retry.log", block.clone(), client.clone(), vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            created.block_size
        );

        // Idempotent retry must not stack a second reservation on top of the
        // first successful assignment.
        fs.assign_worker("/retry.log", block.clone(), client.clone(), vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            created.block_size
        );

        // After heartbeat reports the write, leftover reservation is gone.
        // Another retry still must not leak scheduled_bytes.
        {
            let mut wm = fs.worker_manager.write();
            let worker = wm.worker_map.workers.get_mut(&1).unwrap();
            let prev = worker.available;
            worker.available = prev - created.block_size;
            worker.reclaim_scheduled_from_heartbeat(prev);
            assert_eq!(worker.scheduled_bytes, 0);
        }

        fs.assign_worker("/retry.log", block, client, vec![])
            .unwrap();
        assert_eq!(
            fs.worker_manager
                .read()
                .get_worker(1)
                .unwrap()
                .scheduled_bytes,
            0
        );
    }
}
