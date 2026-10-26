// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Versioned, apply-only recycle-bin commands. The soft-delete decision is
//! captured by the leader; historical namespace v5 remains a permanent delete.
use super::{
    authority::*,
    commands, transport,
    view::{BatchView, PreparedEdits},
    NativeInput,
};
use crate::master::{
    fs::MasterFilesystem,
    hsm::read_clock::ReadTime,
    meta::{trash, FsDir},
    MountManager,
};
use curvine_core_error::{err_box, CommonResult, ErrorExt};
use curvine_error::{FsError, FsResult};
use curvine_fs_api::Path;
use curvine_model::{DeleteResult, MkdirOpts, ProtoUtils, RenameFlags};
use curvine_proto::{DeleteResponse, NativeMutationRequest, NativeMutationResponse};
use curvine_runtime::common::{LocalTime, SerdeUtils};
use prost::Message;
use serde::{Deserialize, Serialize};

pub(super) const VERSION: u32 = 6;
#[derive(Clone, Serialize, Deserialize)]
pub(super) enum Operation {
    Delete {
        soft: bool,
    },
    Restore,
    Empty,
    Expired {
        inode: i64,
        delete_ms: i64,
        cutoff_ms: i64,
    },
}
#[derive(Serialize, Deserialize)]
pub(super) struct TrashCommand {
    pub(super) version: u32,
    pub(super) identity: RequestIdentity,
    pub(super) fingerprint: [u8; 32],
    pub(super) time: i64,
    pub(super) protection: ReadTime,
    pub(super) code: u32,
    pub(super) header: Vec<u8>,
    pub(super) operation: Operation,
    pub(super) trash_inode: Option<i64>,
    pub(super) admission_error: Option<Vec<u8>>,
}
fn fingerprint(code: u32, header: &[u8], operation: &Operation) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(&code.to_le_bytes());
    hash.update(header);
    // Configuration-derived soft mode is intentionally not an identity input:
    // a retry after a configuration change still returns its original receipt.
    if let Operation::Expired {
        inode,
        delete_ms,
        cutoff_ms,
    } = operation
    {
        hash.update(b"hsm-trash-expired-v1");
        hash.update(&inode.to_le_bytes());
        hash.update(&delete_ms.to_le_bytes());
        hash.update(&cutoff_ms.to_le_bytes());
    }
    *hash.finalize().as_bytes()
}
fn install(view: &mut BatchView<'_>, all: &mut PreparedEdits, next: PreparedEdits) -> FsResult<()> {
    view.install(&next)?;
    all.append(next);
    Ok(())
}
fn resolve(
    view: &mut BatchView<'_>,
    path: &str,
) -> FsResult<(i64, crate::master::meta::inode::InodeView)> {
    let mut command = view.command();
    let ids = command.resolve(path)?;
    if ids.len() != crate::master::meta::inode::InodeView::path_components(path)?.len() {
        return Err(FsError::file_not_found(path));
    }
    let id = *ids.last().unwrap();
    Ok((
        id,
        command
            .inode(id)?
            .ok_or_else(|| FsError::file_not_found(path))?,
    ))
}
fn prepared(edits: PreparedEdits, response: Vec<u8>) -> commands::PreparedCommand {
    commands::PreparedCommand {
        edits,
        response,
        allocation: None,
        failure: None,
    }
}
fn delete_reply(result: DeleteResult) -> Vec<u8> {
    let trashed = result.trashed;
    DeleteResponse {
        res: Some(ProtoUtils::delete_res_to_pb(result)),
        trashed: Some(trashed),
    }
    .encode_to_vec()
}
fn restore_reply(dst: String, status: curvine_proto::FileStatusProto) -> Vec<u8> {
    #[derive(Clone, PartialEq, Message)]
    struct Reply {
        #[prost(string, optional, tag = "1")]
        dst: Option<String>,
        // Match the public nested message's length-delimited wire type.
        #[prost(bytes = "vec", optional, tag = "2")]
        status: Option<Vec<u8>>,
    }
    Reply {
        dst: Some(dst),
        status: Some(commands::canonical_file_status(status)),
    }
    .encode_to_vec()
}
fn prepare(
    view: &mut BatchView<'_>,
    command: &TrashCommand,
) -> FsResult<commands::PreparedCommand> {
    let request = NativeMutationRequest {
        version: 1,
        code: command.code,
        header: command.header.clone(),
        ..Default::default()
    };
    let input = NativeInput::parse(&request)?;
    let mut edits = PreparedEdits::default();
    let response = match (&command.operation, input) {
        (Operation::Delete { soft }, NativeInput::Delete(request)) => {
            if request.path == trash::TRASH_ROOT {
                return Err(FsError::invalid_argument(
                    "the recycle bin root cannot be deleted",
                ));
            }
            let (id, inode) = resolve(view, &request.path)?;
            if !*soft
                || trash::is_trash_path(&request.path)
                || inode.as_file_ref().is_ok_and(|file| file.nlink() > 1)
            {
                return commands::delete(view, &request.path, request.recursive, command.time);
            }
            if inode.is_dir() && !request.recursive && !view.command().children(id)?.is_empty() {
                return Err(FsError::dir_not_empty(&request.path));
            }
            let root = view.command().resolve(trash::TRASH_ROOT)?;
            if root.len() < 2 {
                let id = command
                    .trash_inode
                    .ok_or_else(|| FsError::common("missing captured trash root inode"))?;
                let mut stage = view.command();
                commands::directory(
                    &mut stage,
                    crate::master::meta::inode::ROOT_INODE_ID,
                    trash::TRASH_ROOT_NAME,
                    MkdirOpts::with_create(false),
                    command.time,
                    Some(id),
                )?;
                let next = stage.finish()?;
                install(view, &mut edits, next)?;
            } else if !resolve(view, trash::TRASH_ROOT)?.1.is_dir() {
                return Err(FsError::invalid_argument(
                    "recycle bin root is not a directory",
                ));
            }
            let mut destination = None;
            for sequence in 0..1000 {
                let base = trash::trash_entry_name(command.time, id);
                let name = if sequence == 0 {
                    base
                } else {
                    format!("{base}-{sequence}")
                };
                let path = trash::trash_entry_path(&name);
                if view.command().resolve(&path)?.len() < 3 {
                    destination = Some(path);
                    break;
                }
            }
            let destination =
                destination.ok_or_else(|| FsError::common("trash entry name exhausted"))?;
            let next = commands::rename(
                view,
                curvine_proto::RenameRequest {
                    src: request.path,
                    dst: destination,
                    flags: (RenameFlags::NO_REPLACE | RenameFlags::TRASH).value(),
                },
                command.time,
            )?;
            install(view, &mut edits, next.edits)?;
            delete_reply(DeleteResult {
                trashed: true,
                ..Default::default()
            })
        }
        (Operation::Restore, NativeInput::RestoreTrash(request)) => {
            if !trash::is_trash_entry_path(&request.path) {
                return Err(FsError::invalid_argument(
                    "restore requires a direct trash entry",
                ));
            }
            let (_, inode) = resolve(view, &request.path)?;
            let origin = trash::decode_origin(inode.x_attr()?)
                .ok_or_else(|| FsError::invalid_argument("source is not a trash entry"))?;
            let destination = request
                .dst
                .as_deref()
                .filter(|dst| !dst.is_empty())
                .unwrap_or(&origin)
                .to_owned();
            let destination = Path::from_str(&destination)?.path().to_owned();
            if trash::is_reserved_path(&destination) {
                return Err(FsError::invalid_argument("restore destination is reserved"));
            }
            let flags = if request.overwrite() {
                RenameFlags::RESTORE
            } else {
                RenameFlags::RESTORE | RenameFlags::NO_REPLACE
            };
            let next = commands::rename(
                view,
                curvine_proto::RenameRequest {
                    src: request.path,
                    dst: destination.clone(),
                    flags: flags.value(),
                },
                command.time,
            )?;
            install(view, &mut edits, next.edits)?;
            let (id, _) = resolve(view, &destination)?;
            restore_reply(
                destination.clone(),
                ProtoUtils::file_status_to_pb(view.command().status(&destination, id)?),
            )
        }
        (Operation::Empty, NativeInput::EmptyTrash) => {
            let ids = view.command().resolve(trash::TRASH_ROOT)?;
            let mut total = DeleteResult::default();
            if ids.len() == 2 {
                let children = view.command().children(*ids.last().unwrap())?;
                for (name, _) in children {
                    if trash::parse_entry_delete_ms(&name).is_none() {
                        continue;
                    }
                    let path = trash::trash_entry_path(&name);
                    let next = commands::delete(view, &path, true, command.time)?;
                    let result = DeleteResponse::decode(next.response.as_slice())
                        .map_err(|error| FsError::common(error.to_string()))?;
                    let result = ProtoUtils::delete_res_from_pb(result.res.unwrap_or_default());
                    total.inodes += result.inodes;
                    total.bytes += result.bytes;
                    install(view, &mut edits, next.edits)?;
                }
            }
            curvine_proto::EmptyTrashResponse {
                res: ProtoUtils::delete_res_to_pb(total),
            }
            .encode_to_vec()
        }
        (
            Operation::Expired {
                inode,
                delete_ms,
                cutoff_ms,
            },
            NativeInput::Delete(request),
        ) => {
            if !trash::is_trash_entry_path(&request.path) {
                return Err(FsError::invalid_argument(
                    "expiry purge requires a direct trash entry",
                ));
            }
            let current = resolve(view, &request.path);
            match current {
                // An absent result means a stale expiry observation. A real
                // empty-directory purge still returns Some with zero files.
                Err(FsError::FileNotFound(_)) => DeleteResponse {
                    res: None,
                    trashed: None,
                }
                .encode_to_vec(),
                Err(error) => return Err(error),
                Ok((id, current))
                    if id == *inode
                        && *delete_ms <= *cutoff_ms
                        && trash::decode_delete_ms(current.x_attr()?) == Some(*delete_ms) =>
                {
                    return commands::delete(view, &request.path, true, command.time);
                }
                Ok(_) => DeleteResponse {
                    res: None,
                    trashed: None,
                }
                .encode_to_vec(),
            }
        }
        _ => {
            return Err(FsError::invalid_argument(
                "trash command operation/header mismatch",
            ))
        }
    };
    edits.coalesce_versions()?;
    Ok(prepared(edits, response))
}

pub(super) fn apply(fs: &mut FsDir, input: TrashCommand) -> CommonResult<NativeMutationResponse> {
    if input.version != VERSION
        || input.fingerprint != fingerprint(input.code, &input.header, &input.operation)
    {
        return err_box!("invalid committed trash command");
    }
    let mut command = NativeCommand {
        identity: input.identity.clone(),
        fingerprint: input.fingerprint,
        time: input.time,
        edits: Default::default(),
        reply: NativeReply {
            code: input.code as i8,
            success: false,
            header: vec![],
            body: vec![],
        },
    };
    if matches!(
        prior(
            fs,
            &command.identity,
            command.fingerprint,
            input.code as i8,
            input.time
        )?,
        Prior::New
    ) && fs
        .hsm_namespace()?
        .is_some_and(|ns| ns.incarnation == command.identity.incarnation)
    {
        if let Some(error) = &input.admission_error {
            command.reply.body = error.clone();
        } else {
            let mut view = BatchView::for_trash(fs, input.protection);
            match prepare(&mut view, &input) {
                Ok(prepared) => {
                    command.edits = prepared.edits;
                    command.reply.success = true;
                    command.reply.header = prepared.response;
                }
                Err(error) => command.reply.body = error.encode().to_vec(),
            }
        }
    }
    Ok(transport::response(
        &command.identity,
        apply_creation(fs, &command)?,
    ))
}

impl MasterFilesystem {
    pub(super) async fn trash_mutate(
        &self,
        request: &NativeMutationRequest,
        input: NativeInput,
        mounts: Option<&MountManager>,
        term: u64,
    ) -> FsResult<NativeMutationResponse> {
        let operation = match &input {
            NativeInput::Delete(delete) => Operation::Delete {
                soft: self.conf.trash_enabled
                    && !delete.skip_trash()
                    && !trash::is_trash_path(&delete.path),
            },
            NativeInput::RestoreTrash(_) => Operation::Restore,
            NativeInput::EmptyTrash => Operation::Empty,
            NativeInput::TrashPurge {
                inode,
                delete_ms,
                cutoff_ms,
                ..
            } => Operation::Expired {
                inode: *inode,
                delete_ms: *delete_ms,
                cutoff_ms: *cutoff_ms,
            },
            _ => return Err(FsError::invalid_argument("not a trash namespace input")),
        };
        let time = LocalTime::mills() as i64;
        let identity = transport::identity(request);
        let hash = fingerprint(request.code, &request.header, &operation);
        {
            let dir = self.fs_dir.read();
            if let Prior::Cached(result) = prior(&dir, &identity, hash, request.code as i8, time)? {
                return Ok(transport::response(&identity, result));
            }
        }
        // An exact prior receipt does not mutate the namespace. New commands
        // still require the complete recovery and observation-drain boundary.
        self.hsm_before_stable_underheld()
            .await
            .map_err(|error| FsError::common(error.to_string()))?;
        let (protection, trash_inode) = {
            let dir = self.fs_dir.read();
            let trash_inode = if matches!(operation, Operation::Delete { soft: true })
                && crate::master::meta::inode::InodePath::resolve(
                    dir.root_ptr(),
                    trash::TRASH_ROOT,
                    &dir.store,
                )?
                .get_last_inode()
                .is_none()
            {
                Some(dir.inode_id.next()?)
            } else {
                None
            };
            (dir.hsm_read_time()?.protection(), trash_inode)
        };
        let admission = (|| -> FsResult<()> {
            let dir = self.fs_dir.read();
            input.validate_client(&dir)?;
            self.check_path_length(input.path())?;
            if let NativeInput::RestoreTrash(restore) = &input {
                if let Some(dst) = restore.dst.as_deref() {
                    self.check_path_length(dst)?;
                }
            }
            let whole_namespace =
                !matches!(&input, NativeInput::Delete(delete) if !delete.recursive);
            if whole_namespace && !dir.get_mount_table()?.is_empty() {
                return Err(FsError::unsupported(
                    "committed trash requires an unmounted native volume",
                ));
            }
            if let Some(mounts) = mounts {
                if mounts
                    .get_mount_info(&Path::from_str(input.path())?)?
                    .is_some()
                    || matches!(operation, Operation::Delete { soft: true })
                        && mounts
                            .get_mount_info(&Path::from_str(trash::TRASH_ROOT)?)?
                            .is_some()
                {
                    return Err(FsError::unsupported(
                        "committed trash cannot cross a UFS mount",
                    ));
                }
            }
            Ok(())
        })();
        let payload = SerdeUtils::serialize(&TrashCommand {
            version: VERSION,
            identity,
            fingerprint: hash,
            time,
            protection,
            code: request.code,
            header: request.header.clone(),
            operation,
            trash_inode,
            admission_error: admission.err().map(|error| error.encode().to_vec()),
        })?;
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_batch_size
                .with_label_values(&["namespace_committed"])
                .observe(1.0);
        }
        let result = self.submit_native_payload(payload, 1, term).await;
        if result.is_err() && self.hsm_read_barrier().await.is_err() {
            log::error!("uncertain committed trash command cannot be fenced; aborting master");
            std::process::abort();
        }
        result.map(|mut results| results.remove(0))
    }
}

#[cfg(test)]
#[path = "trash_tests.rs"]
mod tests;
