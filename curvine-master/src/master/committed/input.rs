// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Decode once at admission; preserve native protobuf response bytes.
use super::{
    commands::{self, PreparedCommand},
    view::BatchView,
};
use crate::master::{fs::MasterFilesystem, MountManager};
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, RpcCode};
use curvine_model::{OpenFlags, ProtoUtils};
use curvine_proto::{
    CompleteFileRequest, CompleteFileResponse, CreateFileRequest, CreateFileResponse, MkdirRequest,
    MkdirResponse, NativeMutationRequest,
};
use prost::Message;

pub(crate) enum NativeInput {
    Mkdir(MkdirRequest),
    CreateBatch(curvine_proto::CreateFilesBatchRequest),
    AddBatch(curvine_proto::AddBlocksBatchRequest),
    CompleteBatch(curvine_proto::CompleteFilesBatchRequest),
    Create(CreateFileRequest),
    Complete(CompleteFileRequest),
    AddBlock(curvine_proto::AddBlockRequest),
    Reopen(curvine_proto::OpenFileRequest),
    SetAttr(curvine_proto::SetAttrRequest),
    Delete(curvine_proto::DeleteRequest),
    // Captured at admission, never interpreted from configuration at apply.
    // Ordinary permanent file deletes retain their native microbatch path.
    PermanentDelete(curvine_proto::DeleteRequest),
    Resize(curvine_proto::FileResizeRequest),
    AssignWorker(curvine_proto::AssignWorkerRequest),
    Rename(curvine_proto::RenameRequest),
    RestoreTrash(curvine_proto::RestoreTrashRequest),
    EmptyTrash,
    TrashPurge {
        path: String,
        inode: i64,
        delete_ms: i64,
        cutoff_ms: i64,
    },
    TtlDelete {
        path: String,
        inode: i64,
        metadata: [u8; 32],
    },
}
impl NativeInput {
    pub(crate) fn parse(request: &NativeMutationRequest) -> FsResult<Self> {
        if request.version != 1 || request.header.len() > 2 * 1024 * 1024 {
            return err_box!("unsupported native mutation format or size");
        }
        match request.code {
            value if value == RpcCode::RestoreTrash as u32 => {
                curvine_proto::RestoreTrashRequest::decode(request.header.as_slice())
                    .map(Self::RestoreTrash)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::EmptyTrash as u32 => {
                curvine_proto::EmptyTrashRequest::decode(request.header.as_slice())
                    .map(|_| Self::EmptyTrash)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::CreateFilesBatch as u32 => {
                curvine_proto::CreateFilesBatchRequest::decode(request.header.as_slice())
                    .map(Self::CreateBatch)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::AddBlocksBatch as u32 => {
                curvine_proto::AddBlocksBatchRequest::decode(request.header.as_slice())
                    .map(Self::AddBatch)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::CompleteFilesBatch as u32 => {
                curvine_proto::CompleteFilesBatchRequest::decode(request.header.as_slice())
                    .map(Self::CompleteBatch)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::Mkdir as u32 => {
                MkdirRequest::decode(request.header.as_slice())
                    .map(Self::Mkdir)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::CreateFile as u32 => {
                CreateFileRequest::decode(request.header.as_slice())
                    .map(Self::Create)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::CompleteFile as u32 => {
                CompleteFileRequest::decode(request.header.as_slice())
                    .map(Self::Complete)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::AddBlock as u32 => {
                curvine_proto::AddBlockRequest::decode(request.header.as_slice())
                    .map(Self::AddBlock)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::OpenFile as u32 => {
                curvine_proto::OpenFileRequest::decode(request.header.as_slice())
                    .map(Self::Reopen)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::SetAttr as u32 => {
                curvine_proto::SetAttrRequest::decode(request.header.as_slice())
                    .map(Self::SetAttr)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::Delete as u32 => {
                curvine_proto::DeleteRequest::decode(request.header.as_slice())
                    .map(Self::Delete)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::Rename as u32 => {
                curvine_proto::RenameRequest::decode(request.header.as_slice())
                    .map(Self::Rename)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::ResizeFile as u32 => {
                curvine_proto::FileResizeRequest::decode(request.header.as_slice())
                    .map(Self::Resize)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            value if value == RpcCode::AssignWorker as u32 => {
                curvine_proto::AssignWorkerRequest::decode(request.header.as_slice())
                    .map(Self::AssignWorker)
                    .map_err(|error| FsError::common(error.to_string()))
            }
            _ => err_box!("unsupported native mutation operation"),
        }
    }
    pub(crate) fn path(&self) -> &str {
        match self {
            Self::CreateBatch(request) => request
                .requests
                .first()
                .map_or("/", |item| item.path.as_str()),
            Self::AddBatch(request) => request
                .requests
                .first()
                .map_or("/", |item| item.path.as_str()),
            Self::CompleteBatch(request) => request
                .requests
                .first()
                .map_or("/", |item| item.path.as_str()),
            Self::Mkdir(request) => &request.path,
            Self::Create(request) => &request.path,
            Self::Complete(request) => &request.path,
            Self::AddBlock(request) => &request.path,
            Self::Reopen(request) => &request.path,
            Self::SetAttr(request) => &request.path,
            Self::Delete(request) | Self::PermanentDelete(request) => &request.path,
            Self::Rename(request) => &request.src,
            Self::RestoreTrash(request) => &request.path,
            Self::EmptyTrash => crate::master::meta::trash::TRASH_ROOT,
            Self::TrashPurge { path, .. } => path,
            Self::Resize(request) => &request.path,
            Self::AssignWorker(request) => &request.path,
            Self::TtlDelete { path, .. } => path,
        }
    }
    pub(super) fn background(&self) -> bool {
        matches!(self, Self::TtlDelete { .. } | Self::TrashPurge { .. })
    }
    pub(super) fn capture_delete_mode(self, trash_enabled: bool) -> Self {
        match self {
            Self::Delete(request)
                if !request.recursive
                    && (!trash_enabled || request.skip_trash())
                    && Path::from_str(&request.path).is_ok_and(|path| {
                        !crate::master::meta::trash::is_trash_path(path.path())
                    }) =>
            {
                Self::PermanentDelete(request)
            }
            input => input,
        }
    }
    pub(super) fn namespace_input(&self) -> bool {
        match self {
            Self::Rename(_)
            | Self::TtlDelete { .. }
            | Self::Delete(_)
            | Self::RestoreTrash(_)
            | Self::EmptyTrash
            | Self::TrashPurge { .. } => true,
            Self::SetAttr(request) => request.opts.recursive,
            _ => false,
        }
    }
    pub(super) fn trash_input(&self) -> bool {
        matches!(
            self,
            Self::Delete(_) | Self::RestoreTrash(_) | Self::EmptyTrash | Self::TrashPurge { .. }
        )
    }
    /// Admission-only checks. Historical namespace DTOs deliberately decode
    /// without these new client restrictions during journal replay.
    pub(crate) fn validate_client(&self, dir: &crate::master::meta::FsDir) -> FsResult<()> {
        let public_path = |path: &str| -> FsResult<()> {
            let path = Path::from_str(path)?;
            if crate::master::meta::trash::is_reserved_path(path.path()) {
                return Err(FsError::invalid_argument(
                    "path is reserved for the recycle bin",
                ));
            }
            dir.hsm_guard_path(path.path())
        };
        match self {
            Self::CreateBatch(request) => {
                for item in &request.requests {
                    Self::Create(item.clone()).validate_client(dir)?;
                }
                return Ok(());
            }
            Self::AddBatch(request) => {
                for item in &request.requests {
                    Self::AddBlock(item.clone()).validate_client(dir)?;
                }
                return Ok(());
            }
            Self::CompleteBatch(request) => {
                for item in &request.requests {
                    Self::Complete(item.clone()).validate_client(dir)?;
                }
                return Ok(());
            }
            Self::SetAttr(request) => crate::master::meta::FsDir::check_not_internal_x_attr(
                &ProtoUtils::set_attr_opts_from_pb(request.opts.clone()),
            )?,
            Self::Complete(request) => {
                if let Some(opts) = &request.set_attr_opts {
                    crate::master::meta::FsDir::check_not_internal_x_attr(
                        &ProtoUtils::set_attr_opts_from_pb(opts.clone()),
                    )?;
                }
            }
            Self::Rename(request) => {
                if curvine_model::RenameFlags::new(request.flags)
                    .intersects(curvine_model::RenameFlags::internal_mask())
                {
                    return Err(FsError::invalid_argument(
                        "rename flags contain a master-internal bit",
                    ));
                }
                public_path(&request.dst)?;
            }
            Self::Delete(request) | Self::PermanentDelete(request) => {
                let path = Path::from_str(&request.path)?;
                if path.path() == crate::master::meta::trash::TRASH_ROOT {
                    return Err(FsError::invalid_argument(
                        "the recycle bin root cannot be deleted",
                    ));
                }
                return dir.hsm_guard_path(path.path());
            }
            Self::RestoreTrash(request) => {
                if !crate::master::meta::trash::is_trash_entry_path(&request.path) {
                    return Err(FsError::invalid_argument(
                        "restore requires a direct trash entry",
                    ));
                }
                if let Some(dst) = request.dst.as_deref().filter(|dst| !dst.is_empty()) {
                    public_path(dst)?;
                }
                return Ok(());
            }
            Self::EmptyTrash | Self::TrashPurge { .. } | Self::TtlDelete { .. } => return Ok(()),
            _ => (),
        }
        public_path(self.path())?;
        let inode = match self {
            Self::Complete(request) => request.inode_id,
            Self::AddBlock(request) => request.inode_id,
            Self::Resize(request) => request.inode_id,
            Self::AssignWorker(request) => request.inode_id,
            _ => None,
        };
        if let Some(id) = inode.filter(|id| *id > 0) {
            public_path(&dir.get_inode_path(id)?)?;
        }
        Ok(())
    }
    pub(crate) fn legacy(self, fs: &MasterFilesystem) -> FsResult<Vec<u8>> {
        if !matches!(
            self,
            Self::CreateBatch(_) | Self::AddBatch(_) | Self::CompleteBatch(_)
        ) {
            self.validate_client(&fs.fs_dir.read())?;
        }
        match self {
            Self::RestoreTrash(request) => {
                let restored =
                    fs.restore_trash(&request.path, request.dst.as_deref(), request.overwrite())?;
                Ok(curvine_proto::RestoreTrashResponse {
                    dst: Some(restored.dst),
                    status: Some(ProtoUtils::file_status_to_pb(restored.status)),
                }
                .encode_to_vec())
            }
            Self::EmptyTrash => Ok(curvine_proto::EmptyTrashResponse {
                res: ProtoUtils::delete_res_to_pb(fs.empty_trash()?),
            }
            .encode_to_vec()),
            Self::TrashPurge { .. } => err_box!("trash purge requires committed native authority"),
            Self::CreateBatch(request) => {
                let mut file_statuses = vec![];
                for item in request.requests {
                    let bytes = Self::Create(item).legacy(fs)?;
                    file_statuses.push(
                        CreateFileResponse::decode(bytes.as_slice())
                            .map_err(|error| FsError::common(error.to_string()))?
                            .file_status,
                    );
                }
                Ok(curvine_proto::CreateFilesBatchResponse { file_statuses }.encode_to_vec())
            }
            Self::AddBatch(request) => {
                let mut blocks = vec![];
                for item in request.requests {
                    let bytes = Self::AddBlock(item).legacy(fs)?;
                    blocks.push(
                        curvine_proto::LocatedBlockProto::decode(bytes.as_slice())
                            .map_err(|error| FsError::common(error.to_string()))?,
                    );
                }
                Ok(curvine_proto::AddBlocksBatchResponse { blocks }.encode_to_vec())
            }
            Self::CompleteBatch(request) => {
                let results = request
                    .requests
                    .into_iter()
                    .map(|mut item| {
                        item.return_file_blocks = Some(false);
                        Self::Complete(item).legacy(fs).is_ok()
                    })
                    .collect();
                Ok(curvine_proto::CompleteFilesBatchResponse { results }.encode_to_vec())
            }
            Self::Resize(request) => Ok(curvine_proto::FileResizeResponse {
                file_blocks: ProtoUtils::file_blocks_to_pb(fs.resize_by_id(
                    request.path,
                    request.inode_id,
                    ProtoUtils::file_alloc_opts_from_pb(request.opts),
                )?),
            }
            .encode_to_vec()),
            Self::AssignWorker(request) => Ok(curvine_proto::AssignWorkerResponse {
                block: ProtoUtils::located_block_to_pb(fs.assign_worker_by_id(
                    request.path,
                    request.inode_id,
                    ProtoUtils::extend_block_from_pb(request.block),
                    ProtoUtils::client_address_from_pb(request.client_address),
                    request.exclude_workers,
                )?),
            }
            .encode_to_vec()),
            Self::TtlDelete { .. } => err_box!("TTL input requires committed native authority"),
            Self::Rename(request) => Ok(curvine_proto::RenameResponse {
                result: fs.rename(
                    request.src,
                    request.dst,
                    curvine_model::RenameFlags::new(request.flags),
                )?,
            }
            .encode_to_vec()),
            Self::Delete(request) | Self::PermanentDelete(request) => {
                let skip_trash = request.skip_trash();
                let result = fs.delete_with_opts(request.path, request.recursive, skip_trash)?;
                let trashed = result.trashed;
                Ok(curvine_proto::DeleteResponse {
                    res: Some(ProtoUtils::delete_res_to_pb(result)),
                    trashed: Some(trashed),
                }
                .encode_to_vec())
            }
            Self::SetAttr(request) => {
                let status = fs.set_attr(
                    request.path,
                    ProtoUtils::set_attr_opts_from_pb(request.opts),
                )?;
                Ok(curvine_proto::SetAttrResponse {
                    status: ProtoUtils::file_status_to_pb(status),
                }
                .encode_to_vec())
            }
            Self::AddBlock(request) => {
                let block = fs.add_block(
                    request.path,
                    request.inode_id,
                    ProtoUtils::client_address_from_pb(request.client_address),
                    request
                        .commit_blocks
                        .into_iter()
                        .map(ProtoUtils::commit_block_from_pb)
                        .collect(),
                    request.exclude_workers,
                    request.file_len,
                    request.last_block.map(ProtoUtils::extend_block_from_pb),
                )?;
                Ok(ProtoUtils::located_block_to_pb(block).encode_to_vec())
            }
            Self::Reopen(request) => {
                let blocks = fs.open_file(
                    request.path,
                    ProtoUtils::create_opts_from_pb(request.opts),
                    OpenFlags::new(request.flags),
                )?;
                Ok(curvine_proto::OpenFileResponse {
                    file_blocks: ProtoUtils::file_blocks_to_pb(blocks),
                }
                .encode_to_vec())
            }
            Self::Complete(request) => {
                let commits = request
                    .commit_blocks
                    .into_iter()
                    .map(ProtoUtils::commit_block_from_pb)
                    .collect();
                let blocks = if request.only_flush && !request.return_file_blocks.unwrap_or(true) {
                    fs.flush_file(
                        request.path,
                        request.inode_id,
                        request.len,
                        commits,
                        request.client_name,
                    )?;
                    None
                } else {
                    fs.complete_file(
                        request.path,
                        request.inode_id,
                        request.len,
                        commits,
                        request.client_name,
                        request.only_flush,
                        request.set_attr_opts.map(ProtoUtils::set_attr_opts_from_pb),
                    )?
                };
                Ok(CompleteFileResponse {
                    result: true,
                    file_blocks: blocks.map(ProtoUtils::file_blocks_to_pb),
                }
                .encode_to_vec())
            }
            Self::Mkdir(request) => {
                let status = fs
                    .mkdir_with_opts(&request.path, ProtoUtils::mkdir_opts_from_pb(request.opts))?;
                Ok(MkdirResponse {
                    status: ProtoUtils::file_status_to_pb(status),
                    ..Default::default()
                }
                .encode_to_vec())
            }
            Self::Create(request) => {
                let status = fs.create_with_opts(
                    &request.path,
                    ProtoUtils::create_opts_from_pb(request.opts),
                    OpenFlags::new(request.flags),
                )?;
                Ok(CreateFileResponse {
                    file_status: ProtoUtils::file_status_to_pb(status),
                }
                .encode_to_vec())
            }
        }
    }
    pub(super) fn prepare(
        self,
        fs: &MasterFilesystem,
        mounts: &MountManager,
        view: &mut BatchView<'_>,
        time: i64,
    ) -> FsResult<PreparedCommand> {
        if matches!(
            self,
            Self::CreateBatch(_) | Self::AddBatch(_) | Self::CompleteBatch(_)
        ) {
            return super::batch::prepare(self, fs, mounts, view, time);
        }
        self.validate_client(view.authority())?;
        if mounts
            .get_mount_info(&Path::from_str(self.path())?)?
            .is_some()
        {
            return err_box!("native HSM metadata requires a path outside UFS mounts");
        }
        match self {
            Self::CreateBatch(_) | Self::AddBatch(_) | Self::CompleteBatch(_) => unreachable!(),
            Self::Resize(request) => commands::resize(view, fs, request, time),
            Self::AssignWorker(request) => commands::assign_worker(view, fs, request),
            Self::TtlDelete { .. }
            | Self::TrashPurge { .. }
            | Self::RestoreTrash(_)
            | Self::EmptyTrash => err_box!("namespace input requires compact committed expansion"),
            Self::Rename(request) => commands::rename(view, request, time),
            Self::Delete(request) | Self::PermanentDelete(request) => {
                commands::delete(view, &request.path, request.recursive, time)
            }
            Self::SetAttr(request) => commands::set_attr(
                view,
                &request.path,
                ProtoUtils::set_attr_opts_from_pb(request.opts),
                time,
            ),
            Self::Complete(request) => commands::complete(view, fs, request, time),
            Self::AddBlock(request) => commands::add_block(view, fs, request, time),
            Self::Reopen(request) => {
                let flags = OpenFlags::new(request.flags);
                if flags.read_only() {
                    return err_box!("native reopen requires writable non-truncating flags");
                }
                // Inspect the private batch view: a preceding command may have
                // created this path without publishing it yet.
                let exists = {
                    let mut command = view.command();
                    command.resolve(&request.path)?.len()
                        == crate::master::meta::inode::InodeView::path_components(&request.path)?
                            .len()
                };
                if exists && !flags.truncate() {
                    commands::reopen(view, fs, request)
                } else if exists || flags.create() {
                    let opts = ProtoUtils::create_opts_from_pb(request.opts);
                    if !exists {
                        fs.validate_create_opts(&request.path, &opts, &flags)?;
                    }
                    let mut prepared = if exists {
                        commands::overwrite(view, &request.path, opts, time)?
                    } else {
                        commands::create(view, &request.path, opts, time)?
                    };
                    let status = CreateFileResponse::decode(prepared.response.as_slice())
                        .map_err(|e| FsError::common(e.to_string()))?
                        .file_status;
                    prepared.response = curvine_proto::OpenFileResponse {
                        file_blocks: curvine_proto::FileBlocksProto {
                            status,
                            block_locs: vec![],
                        },
                    }
                    .encode_to_vec();
                    Ok(prepared)
                } else {
                    Err(FsError::file_not_found(&request.path))
                }
            }
            Self::Mkdir(request) => {
                fs.check_path_length(&request.path)?;
                commands::mkdir(
                    view,
                    &request.path,
                    ProtoUtils::mkdir_opts_from_pb(request.opts),
                    time,
                )
            }
            Self::Create(request) => {
                let flags = OpenFlags::new(request.flags);
                let opts = ProtoUtils::create_opts_from_pb(request.opts);
                fs.validate_create_opts(&request.path, &opts, &flags)?;
                let exists = {
                    let mut command = view.command();
                    command.resolve(&request.path)?.len()
                        == crate::master::meta::inode::InodeView::path_components(&request.path)?
                            .len()
                };
                if exists && flags.overwrite() && !flags.exclusive() {
                    commands::overwrite(view, &request.path, opts, time)
                } else {
                    commands::create(view, &request.path, opts, time)
                }
            }
        }
    }
}
