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
    pub(super) failure: Option<FsError>,
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
        failure: None,
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
        failure: None,
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
        failure: None,
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
    additional: Vec<Allocation>,
}
impl Allocation {
    pub(super) fn combine(mut items: Vec<Self>) -> Option<Self> {
        let mut first = items.pop()?;
        first.additional.extend(items);
        Some(first)
    }
    pub(super) fn retain(mut self) {
        self.release = false;
        for item in self.additional.drain(..) {
            item.retain();
        }
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
    if replicas.is_empty() && meta.alloc_opts.is_none() {
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
            failure: None,
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
        additional: vec![],
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
        failure: None,
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
        failure: None,
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
        failure: None,
    })
}

/// Protobuf maps have no canonical iteration order. Namespace commands build
/// this response independently on each replica, so persist a sorted encoding.
/// The small wrappers use the existing wire tags/types; the public generated
/// DTO and already-cached original response bytes remain unchanged.
fn attribute_response(mut status: curvine_proto::FileStatusProto) -> Vec<u8> {
    #[derive(Clone, PartialEq, Message)]
    struct OrderedAttributes {
        #[prost(btree_map = "string, bytes", tag = "13")]
        values: std::collections::BTreeMap<String, Vec<u8>>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Response {
        // A nested message and bytes share the length-delimited wire type.
        #[prost(bytes = "vec", required, tag = "1")]
        status: Vec<u8>,
    }
    let attributes = OrderedAttributes {
        values: std::mem::take(&mut status.x_attr).into_iter().collect(),
    };
    let mut encoded = status.encode_to_vec();
    encoded.extend(attributes.encode_to_vec());
    Response { status: encoded }.encode_to_vec()
}

/// Recursive inputs are expanded only inside committed apply. Child options
/// retain Curvine's ACL/replica inheritance without propagating timestamps/TTL.
pub(super) fn set_attr(
    batch: &mut BatchView<'_>,
    path: &str,
    mut opts: curvine_model::SetAttrOpts,
    time: i64,
) -> FsResult<PreparedCommand> {
    batch.guard_path(path)?;
    if opts.recursive && path == "/" {
        return err_box!("recursive mutation crosses the reserved HSM namespace");
    }
    let mut view = batch.command();
    let resolved = view.resolve(path)?;
    if resolved.len() != InodeView::path_components(path)?.len() {
        return Err(FsError::file_not_found(path));
    }
    let root = *resolved.last().unwrap();
    let ids = if opts.recursive {
        view.subtree(root)?
    } else {
        vec![root]
    };
    opts.stamp_ctime(time);
    let child_opts = opts.child_opts();
    for id in ids {
        let mut inode = view
            .inode(id)?
            .ok_or_else(|| FsError::file_not_found(path))?;
        view.guard_write(id)?;
        inode.set_attr(if id == root {
            opts.clone()
        } else {
            child_opts.clone()
        })?;
        view.put_inode(inode.clone_metadata())?;
        if inode.is_file() {
            view.enroll(&inode)?;
        }
    }
    let response = attribute_response(ProtoUtils::file_status_to_pb(view.status(path, root)?));
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
        failure: None,
    })
}

fn unlink(
    view: &mut CommandView<'_, '_>,
    id: i64,
    parent_id: i64,
    name: &str,
    time: i64,
    result: &mut curvine_model::DeleteResult,
) -> FsResult<()> {
    let mut inode = view
        .inode(id)?
        .ok_or_else(|| FsError::common("unlink inode missing"))?;
    view.guard_delete(id)?;
    let mut parent = view
        .inode(parent_id)?
        .ok_or_else(|| FsError::common("delete parent missing"))?;
    match parent.expiration_ms() {
        Ok(Some(expiry)) if time > expiry => (),
        _ => parent.update_mtime(time),
    }
    if inode.is_dir() {
        if !view.children(id)?.is_empty() {
            return err_box!("directory still has children");
        }
        parent.dec_nlink(time);
        view.delete_inode(id)?;
    } else {
        let file = inode.as_file_mut()?;
        if file.nlink() == 0 {
            return err_box!("cannot unlink a zero-link file");
        }
        if file.decrement_nlink(time) == 0 {
            // Only counts cross the RPC boundary. Retirement facts keep the
            // physical replica lists; do not also accumulate them for a tree.
            let mut locations = std::collections::HashMap::new();
            for block in &file.blocks {
                locations.insert(
                    block.id,
                    match &block.locs {
                        Some(locations) => locations.clone(),
                        None => view.locations(block.id)?,
                    },
                );
            }
            result.inodes += 1;
            result.bytes += u64::try_from(file.get_locs_bytes(&locations)).unwrap_or(0);
            view.retire_blocks(file)?;
            view.delete_inode(id)?;
        } else {
            view.put_inode(inode)?;
        }
    }
    view.put_inode(parent)?;
    view.set_edge(parent_id, name, None)?;
    Ok(())
}

/// Last-link cleanup is independent of inode lifetime, including recursion.
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
    let root = *resolved.last().unwrap();
    let mut stack = vec![(
        root,
        resolved[resolved.len() - 2],
        parts.last().unwrap().clone(),
        false,
    )];
    let mut result = curvine_model::DeleteResult::default();
    while let Some((id, parent, name, visited)) = stack.pop() {
        let inode = view
            .inode(id)?
            .ok_or_else(|| FsError::common("delete subtree inode missing"))?;
        if inode.is_dir() && !visited {
            let children = view.children(id)?;
            if !recursive && !children.is_empty() {
                return Err(FsError::dir_not_empty(path));
            }
            stack.push((id, parent, name, true));
            for (name, child) in children {
                stack.push((child, id, name, false));
            }
        } else {
            unlink(&mut view, id, parent, &name, time, &mut result)?;
        }
    }
    let response = curvine_proto::DeleteResponse {
        res: Some(ProtoUtils::delete_res_to_pb(result)),
    }
    .encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
        failure: None,
    })
}

pub(super) fn rename(
    batch: &mut BatchView<'_>,
    request: curvine_proto::RenameRequest,
    time: i64,
) -> FsResult<PreparedCommand> {
    use curvine_model::RenameFlags;
    let flags = RenameFlags::new(request.flags);
    if ![
        0,
        RenameFlags::NO_REPLACE.value(),
        RenameFlags::EXCHANGE.value(),
        RenameFlags::RESTORE.value(),
    ]
    .contains(&request.flags)
    {
        return Err(FsError::invalid_argument("unsupported rename flags"));
    }
    batch.guard_path(&request.src)?;
    batch.guard_path(&request.dst)?;
    let src_parts = InodeView::path_components(&request.src)?;
    let dst_parts = InodeView::path_components(&request.dst)?;
    if src_parts.len() == 1 || dst_parts.len() == 1 {
        return err_box!("Cannot rename root path");
    }
    let mut view = batch.command();
    let src = view.resolve(&request.src)?;
    let dst = view.resolve(&request.dst)?;
    if src.len() != src_parts.len() {
        return Err(FsError::file_not_found(&request.src));
    }
    if dst.len() + 1 < dst_parts.len() {
        return Err(FsError::file_not_found(&request.dst));
    }
    let source = *src.last().unwrap();
    let destination = (dst.len() == dst_parts.len()).then(|| *dst.last().unwrap());
    let src_parent = src[src.len() - 2];
    let dst_parent = dst[dst_parts.len() - 2];
    if request.src == request.dst {
        let response = curvine_proto::RenameResponse { result: false }.encode_to_vec();
        return Ok(PreparedCommand {
            edits: view.finish()?,
            response,
            allocation: None,
            failure: None,
        });
    }
    if dst[..dst_parts.len() - 1].contains(&source)
        || flags.exchange_mode() && destination.is_some_and(|id| src[..src.len() - 1].contains(&id))
    {
        return Err(FsError::invalid_argument(
            "rename would create a directory cycle",
        ));
    }
    if destination.is_some() && flags.no_replace() {
        return Err(FsError::file_exists(&request.dst));
    }
    if flags.exchange_mode() && destination.is_none() {
        return Err(FsError::file_not_found(&request.dst));
    }
    let mut moving = view
        .inode(source)?
        .ok_or_else(|| FsError::common("rename source missing"))?;
    // POSIX rename over another hardlink to the same inode is a no-op.
    if destination == Some(source) {
        let response = curvine_proto::RenameResponse { result: true }.encode_to_vec();
        return Ok(PreparedCommand {
            edits: view.finish()?,
            response,
            allocation: None,
            failure: None,
        });
    }
    let mut refresh = view.subtree(source)?;
    if let Some(target) = destination {
        let other = view
            .inode(target)?
            .ok_or_else(|| FsError::common("rename target missing"))?;
        if !flags.exchange_mode() {
            if moving.is_file() && other.is_dir() {
                return Err(FsError::is_a_directory(&request.dst));
            }
            if moving.is_dir() && other.is_file() {
                return Err(FsError::not_a_directory(&request.dst));
            }
            if other.is_dir() && !view.children(target)?.is_empty() {
                return Err(FsError::dir_not_empty(&request.dst));
            }
            unlink(
                &mut view,
                target,
                dst_parent,
                dst_parts.last().unwrap(),
                time,
                &mut Default::default(),
            )?;
        } else {
            refresh.extend(view.subtree(target)?);
        }
    }
    for id in &refresh {
        view.guard_write(*id)?;
    }
    let mut parents = std::collections::BTreeMap::new();
    for id in [src_parent, dst_parent] {
        parents.insert(
            id,
            view.inode(id)?
                .ok_or_else(|| FsError::common("rename parent missing"))?,
        );
    }
    if flags.exchange_mode() {
        let target = destination.unwrap();
        let mut other = view
            .inode(target)?
            .ok_or_else(|| FsError::common("exchange target missing"))?;
        if src_parent != dst_parent && moving.is_dir() != other.is_dir() {
            if moving.is_dir() {
                parents.get_mut(&src_parent).unwrap().dec_nlink(time);
                parents.get_mut(&dst_parent).unwrap().incr_nlink(time);
            } else {
                parents.get_mut(&src_parent).unwrap().incr_nlink(time);
                parents.get_mut(&dst_parent).unwrap().dec_nlink(time);
            }
        }
        other.change_name(src_parts.last().unwrap().clone());
        other.set_parent_id(src_parent);
        other.update_ctime(time);
        view.put_inode(other)?;
        view.set_edge(src_parent, src_parts.last().unwrap(), Some(target))?;
    } else {
        if moving.is_dir() && src_parent != dst_parent {
            parents.get_mut(&src_parent).unwrap().dec_nlink(time);
            parents.get_mut(&dst_parent).unwrap().incr_nlink(time);
        }
        view.set_edge(src_parent, src_parts.last().unwrap(), None)?;
    }
    moving.change_name(dst_parts.last().unwrap().clone());
    moving.set_parent_id(dst_parent);
    moving.update_ctime(time);
    view.put_inode(moving)?;
    view.set_edge(dst_parent, dst_parts.last().unwrap(), Some(source))?;
    for (_, mut parent) in parents {
        parent.update_mtime(time);
        view.put_inode(parent)?;
    }
    refresh.sort_unstable();
    refresh.dedup();
    for id in refresh {
        let inode = view
            .inode(id)?
            .ok_or_else(|| FsError::common("moved inode missing"))?;
        if inode.is_file() {
            view.put_inode(inode.clone_metadata())?;
            view.enroll(&inode)?;
        }
    }
    let response = curvine_proto::RenameResponse { result: true }.encode_to_vec();
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: None,
        failure: None,
    })
}

/// Sparse resize carries no Worker choice. Retired whole blocks and the next
/// logical layout are published atomically; the writer later assigns/commits
/// unplaced blocks through the same stable native request session.
pub(super) fn resize(
    batch: &mut BatchView<'_>,
    fs: &crate::master::fs::MasterFilesystem,
    request: curvine_proto::FileResizeRequest,
    time: i64,
) -> FsResult<PreparedCommand> {
    let opts = ProtoUtils::file_alloc_opts_from_pb(request.opts);
    opts.validate()?;
    let mut view = batch.command();
    let mut inode = view.file(&request.path, request.inode_id)?;
    view.guard_write(inode.id())?;
    let file = inode.as_file_mut()?;
    let available = if opts.truncate {
        i64::MAX
    } else {
        fs.worker_manager.read().available_bytes()
    };
    crate::master::fs::MasterFilesystem::validate_alloc_capacity(
        file.len,
        file.replicas,
        &opts,
        available,
    )?;
    if file.len != opts.len {
        let removed = file.resize(opts)?;
        view.retire_metas(&removed)?;
        file.complete_at(file.len, &[], "", true, time)?;
        view.put_inode(inode.clone_metadata())?;
        view.enroll(&inode)?;
    }
    let file = inode.as_file_ref()?;
    let mut blocks = Vec::with_capacity(file.blocks.len());
    for (index, meta) in file.blocks.iter().enumerate() {
        if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
            return err_box!("block status abnormal, block id {}", meta.id);
        }
        blocks.push(block_response(&view, fs, &request.path, file, meta)?);
    }
    let response = curvine_proto::FileResizeResponse {
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
        failure: None,
    })
}

pub(super) fn assign_worker(
    batch: &mut BatchView<'_>,
    fs: &crate::master::fs::MasterFilesystem,
    request: curvine_proto::AssignWorkerRequest,
) -> FsResult<PreparedCommand> {
    let mut view = batch.command();
    let mut inode = view.file(&request.path, request.inode_id)?;
    view.guard_write(inode.id())?;
    let file = inode.as_file_mut()?;
    let meta = file.search_block_mut_check(request.block.id)?;
    if meta.alloc_opts.is_none() || meta.locs.is_some() {
        let file = inode.as_file_ref()?;
        let meta = file
            .blocks
            .iter()
            .find(|meta| meta.id == request.block.id)
            .ok_or_else(|| FsError::common("assignment block missing"))?;
        let response = curvine_proto::AssignWorkerResponse {
            block: ProtoUtils::located_block_to_pb(block_response(
                &view,
                fs,
                &request.path,
                file,
                meta,
            )?),
        }
        .encode_to_vec();
        return Ok(PreparedCommand {
            edits: view.finish()?,
            response,
            allocation: None,
            failure: None,
        });
    }
    let workers = fs.choose_worker_for_file(
        file,
        ProtoUtils::client_address_from_pb(request.client_address),
        request.exclude_workers,
    )?;
    let allocation = Allocation {
        manager: fs.worker_manager.clone(),
        workers: workers.clone(),
        block_size: file.block_size as i64,
        release: true,
        additional: vec![],
    };
    let has_spdk = fs.worker_manager.read().workers_have_spdk(&workers);
    let meta = file.search_block_mut_check(request.block.id)?;
    if !meta.assign_worker(&workers) {
        return err_box!("private assignment did not install selected workers");
    }
    let response = curvine_proto::AssignWorkerResponse {
        block: ProtoUtils::located_block_to_pb(curvine_model::LocatedBlock {
            block: curvine_model::ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                alloc_opts: meta.alloc_opts.clone(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
            },
            locs: workers,
            has_spdk,
        }),
    }
    .encode_to_vec();
    view.put_inode(inode)?;
    Ok(PreparedCommand {
        edits: view.finish()?,
        response,
        allocation: Some(allocation),
        failure: None,
    })
}
