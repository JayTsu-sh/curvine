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
use std::future::Future;
use std::time::Duration;

/// Retry only an unconfirmed admission/outcome, never a committed business
/// rejection. Every attempt keeps the same identity and re-enters authority.
async fn retry_trash_purge<F, Fut>(
    request: &NativeMutationRequest,
    monitor: &crate::master::MasterMonitor,
    mut submit: F,
) -> FsResult<Option<curvine_model::DeleteResult>>
where
    F: FnMut(NativeMutationRequest) -> Fut,
    Fut: Future<Output = FsResult<curvine_proto::NativeMutationResponse>>,
{
    const ATTEMPTS: usize = 3;
    for attempt in 0..ATTEMPTS {
        if !monitor.is_active() {
            return Ok(None);
        }
        let error = match submit(request.clone()).await {
            Err(error) => error,
            Ok(response) => {
                if response.version != request.version
                    || response.incarnation != request.incarnation
                    || response.session != request.session
                    || response.sequence != request.sequence
                    || response.code != request.code
                {
                    return Err(FsError::common("trash purge reply identity mismatch"));
                }
                let status = Status::from_i32(response.status)
                    .ok_or_else(|| FsError::common("invalid trash purge reply status"))?;
                match status {
                    Status::Committed => {
                        if !response.success {
                            return Err(if response.body.is_empty() {
                                FsError::common("committed trash failure has no error receipt")
                            } else {
                                FsError::decode(bytes::BytesMut::from(response.body.as_slice()))
                            });
                        }
                        let reply =
                            curvine_proto::DeleteResponse::decode(response.header.as_slice())
                                .map_err(|error| FsError::common(error.to_string()))?;
                        return Ok(reply.res.map(curvine_model::ProtoUtils::delete_res_from_pb));
                    }
                    Status::Uncertain => {
                        FsError::not_leader("trash purge outcome remains uncertain")
                    }
                    Status::Fenced | Status::NeedsNamespace | Status::LegacyResult => {
                        return Err(if response.body.is_empty() {
                            FsError::not_leader("trash purge identity/namespace is fenced")
                        } else {
                            FsError::decode(bytes::BytesMut::from(response.body.as_slice()))
                        });
                    }
                }
            }
        };
        if !monitor.is_active() {
            return Ok(None);
        }
        if attempt + 1 == ATTEMPTS {
            return Err(error);
        }
        tokio::time::sleep(Duration::from_millis(25 * (attempt as u64 + 1))).await;
    }
    unreachable!("bounded purge attempts return their final result")
}

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
                skip_trash: Some(true),
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

impl MasterFilesystem {
    /// Schedulers wait for an owned, idempotent committed purge. Timing out
    /// this OS wait never cancels the accepted authority operation.
    pub(crate) fn hsm_expunge_trash(
        &self,
        path: &str,
        cutoff_ms: i64,
    ) -> FsResult<Option<curvine_model::DeleteResult>> {
        if !self.master_monitor.is_active() {
            return Ok(None);
        }
        let (writer, coordinator, incarnation, inode, delete_ms) = {
            let dir = self.fs_dir.read();
            let input =
                crate::master::meta::inode::InodePath::resolve(dir.root_ptr(), path, &dir.store)?;
            let Some(inode) = input.get_last_inode() else {
                return Ok(None);
            };
            let Some(delete_ms) = crate::master::meta::trash::decode_delete_ms(inode.x_attr()?)
            else {
                return Ok(None);
            };
            let namespace = dir
                .hsm_namespace()?
                .ok_or_else(|| FsError::not_leader("trash namespace requires initialization"))?;
            (
                dir.journal_writer.clone(),
                dir.native_coordinator.clone(),
                namespace.incarnation,
                inode.id(),
                delete_ms,
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
                path: path.into(),
                recursive: true,
                skip_trash: Some(true),
            }
            .encode_to_vec(),
        };
        let path = path.to_owned();
        let fs = self.clone();
        let (send, receive) = std::sync::mpsc::channel();
        writer.spawn_background_authority(async move {
            let monitor = fs.master_monitor.clone();
            let result = retry_trash_purge(&request, &monitor, |request| {
                let fs = fs.clone();
                let coordinator = coordinator.clone();
                let input = NativeInput::TrashPurge {
                    path: path.clone(),
                    inode,
                    delete_ms,
                    cutoff_ms,
                };
                async move { coordinator.execute(fs, None, request, input).await }
            })
            .await;
            let _ = send.send(result);
        });
        receive
            .recv_timeout(Duration::from_secs(60))
            .map_err(|error| {
                FsError::not_leader(format!("trash purge owner still pending: {error}"))
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::{DeleteResponse, NativeMutationResponse};
    use std::cell::{Cell, RefCell};
    use std::future::ready;

    fn monitor() -> crate::master::MasterMonitor {
        let monitor = crate::master::MasterMonitor::new(
            curvine_runtime::sync::StateCtl::new(0),
            curvine_runtime::sync::StateCtl::new(0),
        );
        monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        monitor
    }
    fn request() -> NativeMutationRequest {
        NativeMutationRequest {
            version: transport::VERSION,
            incarnation: "namespace".into(),
            session: format!(
                "{:016x}{}",
                LocalTime::mills(),
                curvine_model::hsm::native::token()
            ),
            sequence: 1,
            acknowledged: 0,
            code: RpcCode::Delete as u32,
            header: DeleteRequest {
                path: "/.Trash/123-42".into(),
                recursive: true,
                skip_trash: Some(true),
            }
            .encode_to_vec(),
        }
    }
    fn response(
        request: &NativeMutationRequest,
        status: Status,
        success: bool,
    ) -> NativeMutationResponse {
        NativeMutationResponse {
            version: request.version,
            incarnation: request.incarnation.clone(),
            session: request.session.clone(),
            sequence: request.sequence,
            code: request.code,
            status: status as i32,
            success,
            header: DeleteResponse {
                res: Some(Default::default()),
                trashed: None,
            }
            .encode_to_vec(),
            body: if success {
                vec![]
            } else {
                FsError::common("durable refusal").encode().to_vec()
            },
        }
    }

    #[test]
    fn purge_retries_transport_and_uncertainty_with_identical_request() -> FsResult<()> {
        let request = request();
        let monitor = monitor();
        let seen = RefCell::new(Vec::new());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = rt.block_on(retry_trash_purge(&request, &monitor, |attempt| {
            seen.borrow_mut().push(attempt.clone());
            ready(match seen.borrow().len() {
                1 => Err(FsError::from(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "idle connection closed",
                ))),
                2 => Ok(response(&attempt, Status::Uncertain, false)),
                _ => Ok(response(&attempt, Status::Committed, true)),
            })
        }))?;
        assert_eq!(
            result.unwrap().inodes,
            0,
            "empty-directory purge lost its confirmed receipt"
        );
        assert_eq!(seen.borrow().len(), 3);
        assert!(seen.borrow().iter().all(|attempt| attempt == &request));
        Ok(())
    }

    #[test]
    fn purge_does_not_retry_committed_rejection_fencing_or_reply_mismatch() -> FsResult<()> {
        let request = request();
        let monitor = monitor();
        let mut mismatch = response(&request, Status::Committed, true);
        mismatch.session.push('0');
        let mut invalid = response(&request, Status::Committed, true);
        invalid.status = 999;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        for terminal in [
            response(&request, Status::Committed, false),
            response(&request, Status::Fenced, false),
            response(&request, Status::NeedsNamespace, false),
            response(&request, Status::LegacyResult, true),
            mismatch,
            invalid,
        ] {
            let calls = Cell::new(0);
            let result = rt.block_on(retry_trash_purge(&request, &monitor, |_| {
                calls.set(calls.get() + 1);
                ready(Ok(terminal.clone()))
            }));
            assert!(result.is_err());
            assert_eq!(calls.get(), 1, "terminal receipt was submitted again");
        }
        Ok(())
    }

    #[test]
    fn purge_unconfirmed_retries_have_a_fixed_bound() -> FsResult<()> {
        let request = request();
        let monitor = monitor();
        let calls = Cell::new(0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = rt.block_on(retry_trash_purge(&request, &monitor, |attempt| {
            assert_eq!(attempt, request);
            calls.set(calls.get() + 1);
            ready(Err(FsError::common("qualification unavailable")))
        }));
        assert!(result.is_err());
        assert_eq!(calls.get(), 3);
        Ok(())
    }

    #[test]
    fn purge_stops_unconfirmed_retries_when_leadership_is_lost() -> FsResult<()> {
        let request = request();
        let monitor = monitor();
        let calls = Cell::new(0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = rt.block_on(retry_trash_purge(&request, &monitor, |_| {
            calls.set(calls.get() + 1);
            monitor
                .journal_ctl
                .set_state(curvine_raft::raft::RoleState::Follower);
            ready(Err(FsError::common("qualification interrupted")))
        }))?;
        assert!(result.is_none());
        assert_eq!(calls.get(), 1);
        let result = rt.block_on(retry_trash_purge(&request, &monitor, |_| {
            calls.set(calls.get() + 1);
            ready(Err(FsError::common("unexpected admission")))
        }))?;
        assert!(result.is_none());
        assert_eq!(calls.get(), 1);
        Ok(())
    }
}
