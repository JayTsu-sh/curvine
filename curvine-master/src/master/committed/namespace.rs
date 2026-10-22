// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Compact deterministic namespace inputs. Expansion happens once per replica
//! at committed apply, never speculatively and never in the Raft payload.
use super::{authority::*, commands, transport, view::BatchView, NativeInput};
use crate::master::{fs::MasterFilesystem, hsm::read_clock::ReadTime, meta::FsDir, MountManager};
use curvine_core_error::{err_box, CommonResult, ErrorExt};
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_proto::{NativeMutationRequest, NativeMutationResponse};
use curvine_runtime::common::{LocalTime, SerdeUtils};
use serde::{Deserialize, Serialize};

pub(super) const VERSION: u32 = 5;
#[derive(Serialize, Deserialize)]
pub(super) struct TtlCondition {
    pub(super) inode: i64,
    pub(super) metadata: [u8; 32],
}
#[derive(Serialize, Deserialize)]
pub(super) struct NamespaceCommand {
    pub(super) version: u32,
    pub(super) identity: RequestIdentity,
    pub(super) fingerprint: [u8; 32],
    pub(super) time: i64,
    pub(super) protection: ReadTime,
    pub(super) code: u32,
    pub(super) header: Vec<u8>,
    pub(super) admission_error: Option<Vec<u8>>,
    pub(super) ttl: Option<TtlCondition>,
}
fn fingerprint(code: u32, header: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(&code.to_le_bytes());
    hash.update(header);
    *hash.finalize().as_bytes()
}
fn ttl_fingerprint(base: [u8; 32], ttl: Option<&TtlCondition>) -> [u8; 32] {
    match ttl {
        None => base,
        Some(ttl) => {
            let mut hash = blake3::Hasher::new();
            hash.update(b"hsm-ttl-delete-v1");
            hash.update(&base);
            hash.update(&ttl.inode.to_le_bytes());
            hash.update(&ttl.metadata);
            *hash.finalize().as_bytes()
        }
    }
}
fn prepare(
    input: NativeInput,
    view: &mut BatchView<'_>,
    time: i64,
) -> FsResult<commands::PreparedCommand> {
    match input {
        NativeInput::Rename(request) => commands::rename(view, request, time),
        NativeInput::Delete(request) if request.recursive => {
            commands::delete(view, &request.path, true, time)
        }
        NativeInput::SetAttr(request) if request.opts.recursive => commands::set_attr(
            view,
            &request.path,
            curvine_model::ProtoUtils::set_attr_opts_from_pb(request.opts),
            time,
        ),
        _ => err_box!("unsupported compact namespace input"),
    }
}
pub(super) fn apply(
    fs: &mut FsDir,
    input: NamespaceCommand,
) -> CommonResult<NativeMutationResponse> {
    if input.version != VERSION
        || ttl_fingerprint(fingerprint(input.code, &input.header), input.ttl.as_ref())
            != input.fingerprint
    {
        return err_box!("invalid compact namespace command");
    }
    let mut command = NativeCommand {
        identity: input.identity,
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
    // Cached/retired identities must not even expand a subtree. apply_creation
    // persists session cleanup and returns the exact original outcome.
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
        if let Some(error) = input.admission_error {
            command.reply.body = error;
        } else {
            let request = NativeMutationRequest {
                version: transport::VERSION,
                code: input.code,
                header: input.header,
                ..Default::default()
            };
            let result = NativeInput::parse(&request).and_then(|operation| {
                let mut view = BatchView::for_namespace(fs, input.protection);
                if let Some(expected) = &input.ttl {
                    let mut observed = view.command();
                    let path = operation.path();
                    let ids = observed.resolve(path)?;
                    let current = observed.inode(expected.inode)?;
                    let unchanged = ids.len()
                        == crate::master::meta::inode::InodeView::path_components(path)?.len()
                        && ids.last() == Some(&expected.inode)
                        && current
                            .as_ref()
                            .map(super::view::inode_fingerprint)
                            .transpose()?
                            == Some(expected.metadata)
                        && fs.hsm_record(expected.inode)?.is_none()
                        && current
                            .as_ref()
                            .and_then(|inode| inode.expiration_ms().ok().flatten())
                            .is_some_and(|expiry| input.time > expiry)
                        && current.as_ref().is_some_and(|inode| {
                            !inode.is_dir()
                                || observed
                                    .children(expected.inode)
                                    .is_ok_and(|children| children.is_empty())
                        });
                    if !unchanged {
                        use prost::Message;
                        return Ok(commands::PreparedCommand {
                            edits: Default::default(),
                            response: curvine_proto::DeleteResponse {
                                res: Some(Default::default()),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            allocation: None,
                        });
                    }
                }
                prepare(operation, &mut view, input.time)
            });
            match result {
                Ok(prepared) => {
                    command.edits = prepared.edits;
                    command.reply.success = true;
                    command.reply.header = prepared.response;
                }
                Err(error) => command.reply.body = error.encode().to_vec(),
            }
        }
    }
    let result = apply_creation(fs, &command)?;
    Ok(transport::response(&command.identity, result))
}
impl MasterFilesystem {
    /// Called by the owned coordinator with its exclusive projection turn.
    /// Cancellation of the connection cannot release this turn before fencing.
    pub(super) async fn namespace_mutate(
        &self,
        request: &NativeMutationRequest,
        input: NativeInput,
        mounts: Option<&MountManager>,
        term: u64,
    ) -> FsResult<NativeMutationResponse> {
        let identity = transport::identity(request);
        let ttl = match &input {
            NativeInput::TtlDelete {
                inode, metadata, ..
            } => Some(TtlCondition {
                inode: *inode,
                metadata: *metadata,
            }),
            _ => None,
        };
        let hash = ttl_fingerprint(fingerprint(request.code, &request.header), ttl.as_ref());
        let time = LocalTime::mills() as i64;
        let protection = {
            let dir = self.fs_dir.read();
            if let Prior::Cached(result) = prior(&dir, &identity, hash, request.code as i8, time)? {
                return Ok(transport::response(&identity, result));
            }
            dir.hsm_read_time()?
        };
        let admission = (|| -> FsResult<()> {
            let mut paths = vec![input.path()];
            if let NativeInput::Rename(rename) = &input {
                paths.push(&rename.dst);
            }
            for path in paths {
                self.check_path_length(path)?;
                if mounts
                    .map(|mounts| mounts.get_mount_info(&Path::from_str(path)?))
                    .transpose()?
                    .flatten()
                    .is_some()
                {
                    return err_box!("native HSM metadata requires paths outside UFS mounts");
                }
            }
            // Recursive inputs must not cross a nested mount either.
            if !self.fs_dir.read().get_mount_table()?.is_empty() {
                return err_box!(
                    "compact HSM namespace operations require an unmounted native volume"
                );
            }
            Ok(())
        })();
        let payload = SerdeUtils::serialize(&NamespaceCommand {
            version: VERSION,
            identity,
            fingerprint: hash,
            time,
            protection,
            code: request.code,
            header: request.header.clone(),
            admission_error: admission.err().map(|error| error.encode().to_vec()),
            ttl,
        })?;
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_batch_size
                .with_label_values(&["namespace_committed"])
                .observe(1.0);
        }
        let result = self.submit_native_payload(payload, 1, term).await;
        if result.is_err() {
            if let Err(error) = self.hsm_read_barrier().await {
                log::error!(
                    "uncertain namespace command could not be fenced: {error}; aborting master"
                );
                std::process::abort();
            }
        }
        result.map(|mut results| results.remove(0))
    }
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
