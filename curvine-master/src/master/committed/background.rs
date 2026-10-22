// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! OS schedulers submit owned commands to the same bounded native coordinator.
//! No scheduler mutates the namespace or sends Worker deletes speculatively.
use super::{transport, view::inode_fingerprint, NativeInput};
use crate::master::{fs::MasterFilesystem, meta::inode::InodeView};
use curvine_core_error::ErrorExt;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_proto::{DeleteRequest, NativeMutationRequest, NativeMutationStatusProto as Status};
use curvine_runtime::common::LocalTime;
use prost::Message;
use std::time::Duration;

impl MasterFilesystem {
    pub(crate) fn hsm_ttl_delete(&self, observed: InodeView) -> FsResult<(bool, InodeView)> {
        let (writer, coordinator, incarnation, path, metadata) = {
            let dir = self.fs_dir.read();
            let namespace = dir
                .hsm_namespace()?
                .filter(|namespace| namespace.ordinary_format >= 2)
                .ok_or_else(|| {
                    FsError::not_leader("TTL namespace not initialized; retry candidate")
                })?;
            (
                dir.journal_writer.clone(),
                dir.native_coordinator.clone(),
                namespace.incarnation,
                dir.get_inode_path(observed.id())?,
                inode_fingerprint(&observed)?,
            )
        };
        let request = NativeMutationRequest {
            version: transport::VERSION,
            incarnation,
            session: format!(
                "{:016x}{}",
                LocalTime::mills(),
                curvine_model::hsm::native::token()
            ),
            sequence: 1,
            acknowledged: 0,
            code: RpcCode::Delete as u32,
            header: DeleteRequest {
                path: path.clone(),
                recursive: true,
            }
            .encode_to_vec(),
        };
        let input = NativeInput::TtlDelete {
            path,
            inode: observed.id(),
            metadata,
        };
        let fs = self.clone();
        let (send, receive) = std::sync::mpsc::channel();
        writer.spawn_background_authority(async move {
            let result = async {
                let response = coordinator
                    .execute(fs.clone(), None, request, input)
                    .await?;
                if response.status != Status::Committed as i32 {
                    return Err(FsError::not_leader(
                        "TTL publication unqualified; retry candidate",
                    ));
                }
                if !response.success {
                    return Err(FsError::decode(bytes::BytesMut::from(
                        response.body.as_slice(),
                    )));
                }
                let current = fs.fs_dir.read().store.get_inode(observed.id(), None)?;
                Ok(match current {
                    Some(inode) => (false, inode),
                    None => (true, observed),
                })
            }
            .await;
            let _ = send.send(result);
        });
        // Timeout does not cancel the authority owner. A later attempt checks
        // identity, metadata, expiry, HSM ownership and empty-dir status again.
        receive
            .recv_timeout(Duration::from_secs(60))
            .map_err(|error| FsError::not_leader(format!("TTL owner still pending: {error}")))?
    }
}
