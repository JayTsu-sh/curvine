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
    CreateFileRequest, CreateFileResponse, MkdirRequest, MkdirResponse, NativeMutationRequest,
};
use prost::Message;

pub(crate) enum NativeInput {
    Mkdir(MkdirRequest),
    Create(CreateFileRequest),
}
impl NativeInput {
    pub(crate) fn parse(request: &NativeMutationRequest) -> FsResult<Self> {
        if request.version != 1 || request.header.len() > 2 * 1024 * 1024 {
            return err_box!("unsupported native mutation format or size");
        }
        match request.code {
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
            _ => err_box!("unsupported native mutation operation"),
        }
    }
    pub(crate) fn path(&self) -> &str {
        match self {
            Self::Mkdir(request) => &request.path,
            Self::Create(request) => &request.path,
        }
    }
    pub(crate) fn legacy(self, fs: &MasterFilesystem) -> FsResult<Vec<u8>> {
        match self {
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
        if mounts
            .get_mount_info(&Path::from_str(self.path())?)?
            .is_some()
        {
            return err_box!("native HSM metadata creation requires a path outside UFS mounts");
        }
        match self {
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
                if flags.overwrite() {
                    return err_box!(
                        "native overwrite is not activated; use its original gated operation"
                    );
                }
                commands::create(view, &request.path, opts, time)
            }
        }
    }
}
