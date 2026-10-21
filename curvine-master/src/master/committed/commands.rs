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
    })
}
