// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Deterministic native preparation: time is captured by admission; publication
//! consumes the returned owned effects and exact encoded response.
use super::view::{BatchView, CommandView, PreparedEdits};
use crate::master::meta::inode::InodeFile;
use crate::master::meta::inode::{InodeDir, InodeView};
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_model::CreateFileOpts;
use curvine_model::{MkdirOpts, ProtoUtils};
use curvine_proto::CreateFileResponse;
use curvine_proto::MkdirResponse;
use prost::Message;

pub(super) struct PreparedCommand {
    pub(super) edits: PreparedEdits,
    pub(super) response: Vec<u8>,
    pub(super) allocation: Option<Allocation>,
}

fn directory(
    view: &mut CommandView<'_, '_>,
    parent_id: i64,
    name: &str,
    mut opts: MkdirOpts,
    time: i64,
) -> FsResult<i64> {
    let mut parent = view
        .inode(parent_id)?
        .ok_or_else(|| FsError::common("prepared parent is missing"))?;
    if !parent.is_dir() {
        return err_box!("Parent path is not a directory");
    }
    let acl = parent.acl()?;
    if acl.mode & 0o2000 != 0 {
        opts.group = acl.group.clone();
        opts.mode |= 0o2000;
    }
    let id = view.allocate_inode()?;
    let mut inode = InodeDir::with_opts(id, time, opts);
    inode.parent_id = parent_id;
    parent.update_mtime(time);
    parent.incr_nlink(time);
    view.put_inode(parent)?;
    view.put_inode(InodeView::new_dir(name.into(), inode))?;
    view.set_edge(parent_id, name, Some(id))?;
    Ok(id)
}

pub(super) fn mkdir(
    batch: &mut BatchView<'_>,
    path: &str,
    opts: MkdirOpts,
    time: i64,
) -> FsResult<PreparedCommand> {
    batch.guard_path(path)?;
    let parts = InodeView::path_components(path)?;
    if parts.len() == 1 {
        return err_box!("Not allowed to create existing root path: {}", path);
    }
    let mut view = batch.command();
    let mut resolved = view.resolve(path)?;
    if resolved.len() == parts.len() {
        let inode = view
            .inode(*resolved.last().unwrap())?
            .ok_or_else(|| FsError::file_not_found(path))?;
        if !opts.create_parent || !inode.is_dir() {
            return Err(FsError::file_exists(path));
        }
    } else {
        if !opts.create_parent && resolved.len() + 1 != parts.len() {
            return err_box!("Parent directory doesn't exist: {}", path);
        }
        for pos in resolved.len()..parts.len() {
            let options = if pos + 1 == parts.len() {
                opts.clone()
            } else {
                opts.parent_opts()
            };
            let id = directory(
                &mut view,
                *resolved.last().unwrap(),
                &parts[pos],
                options,
                time,
            )?;
            resolved.push(id);
        }
    }
    let status = view.status(path, *resolved.last().unwrap())?;
    let response = MkdirResponse {
        status: ProtoUtils::file_status_to_pb(status),
        ..Default::default()
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}

/// New file creation only. Overwrite requires a generation check and durable
/// Worker cleanup; it must not be silently translated to this operation.
pub(super) fn create(
    batch: &mut BatchView<'_>,
    path: &str,
    mut opts: CreateFileOpts,
    time: i64,
) -> FsResult<PreparedCommand> {
    batch.guard_path(path)?;
    let parts = InodeView::path_components(path)?;
    let mut view = batch.command();
    let mut resolved = view.resolve(path)?;
    if resolved.len() == parts.len() {
        return Err(FsError::file_exists(path));
    }
    if !opts.create_parent && resolved.len() + 1 != parts.len() {
        return err_box!("Parent directory doesn't exist: {}", path);
    }
    for name in parts.iter().take(parts.len() - 1).skip(resolved.len()) {
        let id = directory(
            &mut view,
            *resolved.last().unwrap(),
            name,
            opts.dir_opts().parent_opts(),
            time,
        )?;
        resolved.push(id);
    }
    let parent_id = *resolved.last().unwrap();
    let mut parent = view
        .inode(parent_id)?
        .ok_or_else(|| FsError::common("prepared file parent missing"))?;
    if !parent.is_dir() {
        return err_box!("Parent path is not a directory: {}", path);
    }
    let acl = parent.acl()?;
    if acl.mode & 0o2000 != 0 {
        opts.group = acl.group.clone();
    }
    let id = view.allocate_inode()?;
    let mut file = InodeFile::with_opts(id, time, opts);
    file.parent_id = parent_id;
    parent.update_mtime(time);
    view.put_inode(parent)?;
    view.put_inode(InodeView::new_file(parts.last().unwrap().clone(), file))?;
    view.set_edge(parent_id, parts.last().unwrap(), Some(id))?;
    let response = CreateFileResponse {
        file_status: ProtoUtils::file_status_to_pb(view.status(path, id)?),
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}

/// Complete native data and HSM enrollment in one owned command. No namespace
/// or candidate index is changed until the corresponding journal is applied.
pub(super) fn complete(
    batch: &mut BatchView<'_>,
    fs: &crate::master::fs::MasterFilesystem,
    request: curvine_proto::CompleteFileRequest,
    time: i64,
) -> FsResult<PreparedCommand> {
    let mut view = batch.command();
    let mut inode = view.file(&request.path, request.inode_id)?;
    view.guard_write(inode.id())?;
    let commits: Vec<_> = request
        .commit_blocks
        .into_iter()
        .map(ProtoUtils::commit_block_from_pb)
        .collect();
    inode.as_file_mut()?.complete_at(
        request.len,
        &commits,
        request.client_name,
        request.only_flush,
        time,
    )?;
    if !request.only_flush {
        if let Some(opts) = request.set_attr_opts {
            inode.set_attr(ProtoUtils::set_attr_opts_from_pb(opts))?;
        }
    }
    view.commit_locations(&commits)?;
    view.put_inode(inode.clone_metadata())?;
    view.enroll(&inode)?;
    let file_blocks = if request.only_flush && request.return_file_blocks.unwrap_or(true) {
        let file = inode.as_file_ref()?;
        let mut locations = Vec::with_capacity(file.blocks.len());
        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!("block status abnormal, block id {}", meta.id);
            }
            locations.push(block_response(&view, fs, &request.path, file, meta)?);
        }
        Some(ProtoUtils::file_blocks_to_pb(
            curvine_model::FileBlocks::new(inode.to_file_status(&request.path)?, locations),
        ))
    } else {
        None
    };
    let response = curvine_proto::CompleteFileResponse {
        result: true,
        file_blocks,
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}

/// Scheduling pressure is local and soft. Undo failed preparation or a known
/// rejected commit; on an unknown outcome keep the conservative reservation
/// until the worker's ordinary scheduling timeout/report reconciles it.
pub(super) struct Allocation {
    manager: crate::master::SyncWorkerManager,
    workers: Vec<curvine_model::WorkerAddress>,
    block_size: i64,
    release: bool,
}
impl Allocation {
    pub(super) fn retain(mut self) {
        self.release = false;
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        if self.release {
            self.manager
                .write()
                .unschedule_chosen_workers(&self.workers, self.block_size);
        }
    }
}

fn block_response(
    view: &CommandView<'_, '_>,
    fs: &crate::master::fs::MasterFilesystem,
    path: &str,
    file: &InodeFile,
    meta: &crate::master::meta::BlockMeta,
) -> FsResult<curvine_model::LocatedBlock> {
    let replicas = match &meta.locs {
        Some(locs) => locs.clone(),
        None => view.locations(meta.id)?,
    };
    if replicas.is_empty() {
        return err_box!("File {}, block {} Lost (no worker can read)", path, meta.id);
    }
    fs.create_locate_block(
        path,
        curvine_model::ExtendedBlock {
            id: meta.id,
            len: meta.len(),
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
            alloc_opts: meta.alloc_opts.clone(),
        },
        &replicas,
    )
}

pub(super) fn add_block(
    batch: &mut BatchView<'_>,
    fs: &crate::master::fs::MasterFilesystem,
    request: curvine_proto::AddBlockRequest,
    time: i64,
) -> FsResult<PreparedCommand> {
    let mut view = batch.command();
    let mut inode = view.file(&request.path, request.inode_id)?;
    view.guard_write(inode.id())?;
    let file = inode.as_file_ref()?;
    if let Some(next) = file.search_next_block(request.last_block.as_ref().map(|b| b.id)) {
        let response =
            ProtoUtils::located_block_to_pb(block_response(&view, fs, &request.path, file, next)?)
                .encode_to_vec();
        return Ok(PreparedCommand {
            edits: view.finish()?,
            response,
            allocation: None,
        });
    }
    let commits: Vec<_> = request
        .commit_blocks
        .into_iter()
        .map(ProtoUtils::commit_block_from_pb)
        .collect();
    let file = inode.as_file_mut()?;
    let block_id = file.next_block_id()?;
    file.complete_at(request.file_len, &commits, "", true, time)?;
    let workers = fs.choose_worker_for_file(
        file,
        ProtoUtils::client_address_from_pb(request.client_address),
        request.exclude_workers,
    )?;
    let allocation = Allocation {
        manager: fs.worker_manager.clone(),
        block_size: file.block_size as i64,
        workers: workers.clone(),
        release: true,
    };
    let has_spdk = fs.worker_manager.read().workers_have_spdk(&workers);
    file.add_block(crate::master::meta::BlockMeta::with_pre(block_id, &workers));
    let block = curvine_model::LocatedBlock {
        block: curvine_model::ExtendedBlock {
            id: block_id,
            len: 0,
            storage_type: file.storage_policy.storage_type,
            file_type: file.file_type,
            alloc_opts: None,
        },
        locs: workers,
        has_spdk,
    };
    view.commit_locations(&commits)?;
    view.put_inode(inode.clone_metadata())?;
    Ok(PreparedCommand {
        edits: view.finish()?,
        response: ProtoUtils::located_block_to_pb(block).encode_to_vec(),
        allocation: Some(allocation),
    })
}

pub(super) fn reopen(
    batch: &mut BatchView<'_>,
    fs: &crate::master::fs::MasterFilesystem,
    request: curvine_proto::OpenFileRequest,
) -> FsResult<PreparedCommand> {
    let flags = curvine_model::OpenFlags::new(request.flags);
    if flags.read_only() || flags.truncate() {
        return err_box!("native reopen requires writable non-truncating flags");
    }
    let mut view = batch.command();
    let mut inode = view.file(&request.path, None)?;
    view.guard_write(inode.id())?;
    inode.as_file_mut()?.reopen(request.opts.client_name);
    view.put_inode(inode.clone_metadata())?;
    view.enroll(&inode)?;
    let file = inode.as_file_ref()?;
    let mut blocks = Vec::with_capacity(file.blocks.len());
    for (index, meta) in file.blocks.iter().enumerate() {
        if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
            return err_box!("block status abnormal, block id {}", meta.id);
        }
        blocks.push(block_response(&view, fs, &request.path, file, meta)?);
    }
    let response = curvine_proto::OpenFileResponse {
        file_blocks: ProtoUtils::file_blocks_to_pb(curvine_model::FileBlocks::new(
            inode.to_file_status(&request.path)?,
            blocks,
        )),
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}

/// Replace the inode's current layout only after its physical cleanup facts
/// and new generation are durably committed in the same authority batch.
pub(super) fn overwrite(
    batch: &mut BatchView<'_>,
    path: &str,
    opts: CreateFileOpts,
    time: i64,
) -> FsResult<PreparedCommand> {
    let mut view = batch.command();
    let mut inode = view.file(path, None)?;
    view.guard_overwrite(inode.id())?;
    view.retire_blocks(inode.as_file_ref()?)?;
    inode.as_file_mut()?.overwrite(opts, time);
    view.put_inode(inode.clone_metadata())?;
    view.enroll_overwrite(&inode)?;
    let response = CreateFileResponse {
        file_status: ProtoUtils::file_status_to_pb(inode.to_file_status(path)?),
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}

/// Nonrecursive metadata changes touch only the target, regardless of the
/// number of descendants. The HSM record/candidate and original response are
/// part of the same conditional publication as the native attributes.
pub(super) fn set_attr(
    batch: &mut BatchView<'_>,
    path: &str,
    mut opts: curvine_model::SetAttrOpts,
    time: i64,
) -> FsResult<PreparedCommand> {
    batch.guard_path(path)?;
    if opts.recursive {
        return err_box!("recursive attributes require a bounded subtree command");
    }
    let mut view = batch.command();
    let resolved = view.resolve(path)?;
    if resolved.len() != InodeView::path_components(path)?.len() {
        return Err(FsError::file_not_found(path));
    }
    let id = *resolved.last().unwrap();
    let mut inode = view
        .inode(id)?
        .ok_or_else(|| FsError::file_not_found(path))?;
    view.guard_write(id)?;
    opts.stamp_ctime(time);
    inode.set_attr(opts)?;
    view.put_inode(inode.clone_metadata())?;
    if inode.is_file() {
        view.enroll(&inode)?;
    }
    let response = curvine_proto::SetAttrResponse {
        status: ProtoUtils::file_status_to_pb(view.status(path, id)?),
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}

/// Remove one file/link or empty directory. Physical deletion is driven by
/// committed independent retirement facts, never the lifetime of this inode.
pub(super) fn delete(
    batch: &mut BatchView<'_>,
    path: &str,
    recursive: bool,
    time: i64,
) -> FsResult<PreparedCommand> {
    batch.guard_path(path)?;
    let parts = InodeView::path_components(path)?;
    if parts.len() == 1 {
        return err_box!("The root is not allowed to be deleted");
    }
    let mut view = batch.command();
    let resolved = view.resolve(path)?;
    if resolved.len() != parts.len() {
        return Err(FsError::file_not_found(path));
    }
    let id = *resolved.last().unwrap();
    let mut inode = view
        .inode(id)?
        .ok_or_else(|| FsError::file_not_found(path))?;
    view.guard_delete(id)?;
    let parent_id = resolved[resolved.len() - 2];
    let mut parent = view
        .inode(parent_id)?
        .ok_or_else(|| FsError::common("delete parent missing"))?;
    match parent.expiration_ms() {
        Ok(Some(expiry)) if time > expiry => (),
        _ => parent.update_mtime(time),
    }
    let mut result = curvine_model::DeleteResult::default();
    if inode.is_dir() {
        if view.status(path, id)?.children_num != 0 {
            if recursive {
                return err_box!("recursive delete requires a bounded subtree command");
            }
            return Err(FsError::dir_not_empty(path));
        }
        parent.dec_nlink(time);
        view.delete_inode(id)?;
    } else {
        let file = inode.as_file_mut()?;
        if file.nlink() == 0 {
            return err_box!("cannot unlink a zero-link file");
        }
        if file.decrement_nlink(time) == 0 {
            for block in &file.blocks {
                let locations = match &block.locs {
                    Some(locations) => locations.clone(),
                    None => view.locations(block.id)?,
                };
                if !locations.is_empty() {
                    result.blocks.insert(block.id, locations);
                }
            }
            result.inodes = 1;
            result.bytes = u64::try_from(file.get_locs_bytes(&result.blocks)).unwrap_or(0);
            view.retire_blocks(file)?;
            view.delete_inode(id)?;
        } else {
            view.put_inode(inode)?;
        }
    }
    view.put_inode(parent)?;
    view.set_edge(parent_id, parts.last().unwrap(), None)?;
    let response = curvine_proto::DeleteResponse {
        res: Some(ProtoUtils::delete_res_to_pb(result)),
        ..Default::default()
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
    })
}
