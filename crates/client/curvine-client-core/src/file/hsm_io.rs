// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
use curvine_fs_api::RpcCode;
use curvine_model::{hsm::native::*, WorkerAddress};
use curvine_proto::{HsmControlRequest, HsmControlResponse};
use curvine_rpc::{client::ClientConf, message::Builder};
#[path = "hsm_pool.rs"]
mod pool;
/// One bounded native HSM data RPC. Network failures preserve uncertainty.
pub async fn hsm_worker_rpc(
    conf: &ClientConf,
    worker: &WorkerAddress,
    request: &WorkerRequest,
) -> HsmResult<WorkerReply> {
    let call = async {
        let mut connection = pool::WorkerConnection::acquire(conf, worker.inet_addr()).await?;
        let (payload, body) = curvine_model::hsm::wire::encode_request(request)?;
        let msg = Builder::new_rpc(RpcCode::HsmData)
            .proto_header(HsmControlRequest {
                protocol_version: curvine_model::hsm::wire::DATA_PROTOCOL,
                payload,
            })
            .data(curvine_io::DataSlice::Bytes(bytes::Bytes::copy_from_slice(
                body,
            )))
            .build();
        let response = connection
            .client()
            .rpc(msg)
            .await
            .map_err(|e| HsmError::Unknown(e.to_string()))?;
        if !response.is_success() {
            return Err(HsmError::Unknown(response.to_error_msg()));
        }
        let body: HsmControlResponse = response
            .parse_header()
            .map_err(|e| HsmError::Unknown(e.to_string()))?;
        if body.protocol_version != curvine_model::hsm::wire::DATA_PROTOCOL {
            return Err(HsmError::Unknown("invalid HSM worker response".into()));
        }
        let result = curvine_model::hsm::wire::decode_reply(
            &body.payload,
            response.data_bytes().unwrap_or_default(),
        )?;
        connection.reusable();
        result.into_result()
    };
    tokio::time::timeout(std::time::Duration::from_millis(conf.rpc_timeout_ms), call)
        .await
        .map_err(|_| HsmError::Unknown("HSM worker RPC timeout".into()))?
}

/// The same open RPC returns native block locations on an unmanaged volume,
/// or a committed read grant on a managed volume. No negative capability cache
/// can survive a reconnect and accidentally bypass protection on a new leader.
pub enum OpenedFile {
    Native(curvine_model::FileBlocks),
    Managed(ReadHandle, std::time::Instant),
    ManagedFast(ReadHandle, LeaseDeadline),
    Legacy,
}

#[doc(hidden)]
pub struct LeaseDeadline {
    pub(crate) instant: std::time::Instant,
    pub(crate) boot_ms: Option<u64>,
}

pub async fn open_managed(
    fs: &super::CurvineFileSystem,
    path: &curvine_fs_api::Path,
) -> curvine_error::FsResult<OpenedFile> {
    let mut reader = token();
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() > std::time::Duration::from_secs(300) {
            return curvine_core_error::err_box!(
                "automatic HSM open timed out; queued work remains durable"
            );
        }
        let request_id = format!(
            "{:016x}{}",
            curvine_runtime::common::LocalTime::mills(),
            token()
        );
        let request_started = std::time::Instant::now();
        let request_boot = curvine_sys::lease_time_millis().ok();
        let issuer = fs.fs_context.fast_read_issuer();
        let mut request = serde_json::json!({"op":"open_path","path":path.path(),"reader":reader,"request_id":request_id,"read_mode":if request_boot.is_some() { "fast_v1" } else { "legacy" },"read_encoding":"protobuf_v1"});
        if request_boot.is_some() {
            if let Some(issuer) = &issuer {
                request["issuer"] = serde_json::Value::String(issuer.clone());
            }
        }
        let response = fs
            .hsm_control(HsmControlRequest {
                protocol_version: 1,
                payload: serde_json::to_vec(&request)
                    .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
            })
            .await?;
        let wire_handle = curvine_model::hsm::native::read_wire::decode(&response.payload)
            .map_err(|error| curvine_error::FsError::common(error.to_string()))?;
        let value: serde_json::Value = if wire_handle.is_some() {
            serde_json::json!({"kind":"read_fast"})
        } else {
            serde_json::from_slice(&response.payload)
                .map_err(|e| curvine_error::FsError::common(e.to_string()))?
        };
        match value["kind"].as_str() {
            Some("read_fast_bind") => {
                let issuer = value["data"]
                    .as_str()
                    .filter(|issuer| {
                        issuer.len() == 32 && issuer.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
                    .ok_or_else(|| {
                        curvine_error::FsError::common("invalid fast read issuer binding")
                    })?;
                if request_boot.is_none() {
                    return curvine_core_error::err_box!(
                        "fast HSM read requires a suspend-aware clock"
                    );
                }
                fs.fs_context.set_fast_read_issuer(issuer.into());
                // Binding grants no access. Retry with a fresh request id and
                // the same unpublished caller session identity.
                continue;
            }
            Some("normal") => return Ok(OpenedFile::Legacy),
            Some("normal_blocks") => {
                return Ok(OpenedFile::Native(
                    serde_json::from_value(value["data"].clone())
                        .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
                ))
            }
            Some("read" | "read_fast") => {
                let handle: ReadHandle = match wire_handle {
                    Some(handle) => handle,
                    None => serde_json::from_value(value["data"].clone())
                        .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
                };
                let Some(deadline) =
                    lease_deadline(request_started, handle.granted_ms, handle.expires_ms)?
                else {
                    reader = token();
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    continue;
                };
                return Ok(if value["kind"] == "read_fast" {
                    let issuer = issuer.as_ref().ok_or_else(|| {
                        curvine_error::FsError::common("missing fast read issuer binding")
                    })?;
                    if handle.reader != format!("{issuer}:{reader}") {
                        return curvine_core_error::err_box!(
                            "fast read grant issuer or session mismatch"
                        );
                    }
                    let started = request_boot.ok_or_else(|| {
                        curvine_error::FsError::common(
                            "fast HSM read requires a suspend-aware clock",
                        )
                    })?;
                    let Some(boot_ms) =
                        boot_deadline(started, handle.granted_ms, handle.expires_ms)?
                    else {
                        reader = token();
                        continue;
                    };
                    OpenedFile::ManagedFast(
                        handle,
                        LeaseDeadline {
                            instant: deadline,
                            boot_ms: Some(boot_ms),
                        },
                    )
                } else {
                    OpenedFile::Managed(handle, deadline)
                });
            }
            Some("pending") => {
                if started.elapsed() > std::time::Duration::from_secs(300) {
                    return curvine_core_error::err_box!(
                        "automatic HSM recall timed out; queued work remains durable"
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Some("error") if value["data"]["code"] == "unsupported" => {
                return Ok(OpenedFile::Legacy)
            }
            Some("error") if value["data"]["code"] == "fenced" => {
                // No Reader or data I/O has been published. A new caller nonce
                // starts a fresh open after the previous attempt is fenced.
                reader = token();
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Some("error") if value["data"]["code"] == "conflict" => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await
            }
            _ => return curvine_core_error::err_box!("HSM read: {}", value["data"]),
        }
    }
}

pub(crate) async fn reader_control(
    context: std::sync::Arc<super::FsContext>,
    handle: &ReadHandle,
    op: &str,
) -> curvine_error::FsResult<Option<std::time::Instant>> {
    Ok(reader_control_mode(context, handle, op, false)
        .await?
        .map(|deadline| deadline.instant))
}

pub(crate) async fn reader_control_mode(
    context: std::sync::Arc<super::FsContext>,
    handle: &ReadHandle,
    op: &str,
    fast: bool,
) -> curvine_error::FsResult<Option<LeaseDeadline>> {
    for attempt in 0..16 {
        let request_id = format!(
            "{:016x}{}",
            curvine_runtime::common::LocalTime::mills(),
            token()
        );
        let started = std::time::Instant::now();
        let request_boot = if fast {
            Some(
                curvine_sys::lease_time_millis()
                    .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
            )
        } else {
            None
        };
        let response = super::FsClient::new(context.clone()).hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: serde_json::to_vec(&serde_json::json!({"op":op,"entity":handle.data.entity,"data":handle.data,"reader":handle.reader,"request_id":request_id,"read_mode":if fast { "fast_v1" } else { "legacy" }}))
                .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
        }).await?;
        let value: serde_json::Value = serde_json::from_slice(&response.payload)
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        if value["kind"] == "done" {
            if op == "read_close" {
                return Ok(None);
            }
            let granted = value["data"]["granted_ms"]
                .as_i64()
                .ok_or_else(|| curvine_error::FsError::common("missing read grant time"))?;
            let expires = value["data"]["expires_ms"]
                .as_i64()
                .ok_or_else(|| curvine_error::FsError::common("missing read expiry"))?;
            if let Some(deadline) = lease_deadline(started, granted, expires)? {
                let boot_ms = if let Some(started) = request_boot {
                    let Some(deadline) = boot_deadline(started, granted, expires)? else {
                        continue;
                    };
                    Some(deadline)
                } else {
                    None
                };
                return Ok(Some(LeaseDeadline {
                    instant: deadline,
                    boot_ms,
                }));
            }
            if attempt < 15 {
                continue;
            }
            return curvine_core_error::err_box!(
                "read renewal reply arrived after its grant expired"
            );
        }
        if value["data"]["code"] == "conflict" && attempt < 15 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            continue;
        }
        return curvine_core_error::err_box!("HSM read lease: {}", value["data"]);
    }
    curvine_core_error::err_box!("read renewal replies arrived after their grants expired")
}

/// Begin the local deadline at RPC send, not response receipt. This subtracts
/// network/commit delay conservatively and avoids comparing clocks on nodes.
fn lease_deadline(
    started: std::time::Instant,
    granted: i64,
    expires: i64,
) -> curvine_error::FsResult<Option<std::time::Instant>> {
    let ttl = expires
        .checked_sub(granted)
        .filter(|ttl| (1..=120_000).contains(ttl))
        .ok_or_else(|| curvine_error::FsError::common("invalid bounded read lease"))?;
    let deadline = started + std::time::Duration::from_millis(ttl as u64);
    Ok((std::time::Instant::now() < deadline).then_some(deadline))
}

fn boot_deadline(started: u64, granted: i64, expires: i64) -> curvine_error::FsResult<Option<u64>> {
    let now = curvine_sys::lease_time_millis()
        .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
    boot_deadline_at(started, granted, expires, now)
}

fn boot_deadline_at(
    started: u64,
    granted: i64,
    expires: i64,
    now: u64,
) -> curvine_error::FsResult<Option<u64>> {
    let ttl = expires
        .checked_sub(granted)
        .filter(|ttl| (1..=120_000).contains(ttl))
        .ok_or_else(|| curvine_error::FsError::common("invalid bounded read lease"))?;
    let deadline = started
        .checked_add(ttl as u64)
        .ok_or_else(|| curvine_error::FsError::common("read lease clock overflow"))?;
    Ok((now < deadline).then_some(deadline))
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn suspend_time_expires_fast_grants_even_without_instant_progress() {
        // The server's clock origin is unrelated to this client's boot clock.
        let granted = 9_000_000;
        let expires = granted + 119_000;
        assert_eq!(
            boot_deadline_at(100, granted, expires, 101).unwrap(),
            Some(119_100)
        );
        assert_eq!(
            boot_deadline_at(100, granted, expires, 119_100).unwrap(),
            None
        );
        assert_eq!(
            boot_deadline_at(100, granted, expires, 200_100).unwrap(),
            None
        );
        assert!(boot_deadline_at(100, granted, granted + 120_001, 101).is_err());
        assert!(boot_deadline_at(u64::MAX, granted, expires, u64::MAX).is_err());
    }

    #[test]
    fn grant_deadline_starts_at_request_and_ignores_remote_clock_offset() {
        let started = Instant::now() - Duration::from_secs(10);
        let granted = 9_000_000;
        let expected = started + Duration::from_secs(120);
        assert_eq!(
            lease_deadline(started, granted, granted + 120_000).unwrap(),
            Some(expected)
        );
        assert!(lease_deadline(
            started - Duration::from_secs(120),
            granted,
            granted + 120_000
        )
        .unwrap()
        .is_none());
        assert!(lease_deadline(started, granted, granted - 1).is_err());
        assert!(lease_deadline(started, granted, granted + 120_001).is_err());
    }
}
