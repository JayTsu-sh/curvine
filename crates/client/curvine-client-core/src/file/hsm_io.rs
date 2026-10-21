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
    Legacy,
}

pub async fn open_managed(
    fs: &super::CurvineFileSystem,
    path: &curvine_fs_api::Path,
) -> curvine_error::FsResult<OpenedFile> {
    let reader = token();
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
        let response = fs
            .hsm_control(HsmControlRequest {
                protocol_version: 1,
                payload: serde_json::to_vec(
                    &serde_json::json!({"op":"open_path","path":path.path(),"reader":reader,"request_id":request_id}),
                )
                .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
            })
            .await?;
        let value: serde_json::Value = serde_json::from_slice(&response.payload)
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        match value["kind"].as_str() {
            Some("normal") => return Ok(OpenedFile::Legacy),
            Some("normal_blocks") => {
                return Ok(OpenedFile::Native(
                    serde_json::from_value(value["data"].clone())
                        .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
                ))
            }
            Some("read") => {
                let handle: ReadHandle = serde_json::from_value(value["data"].clone())
                    .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
                let Some(deadline) =
                    lease_deadline(request_started, handle.granted_ms, handle.expires_ms)?
                else {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    continue;
                };
                return Ok(OpenedFile::Managed(handle, deadline));
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
    for attempt in 0..16 {
        let request_id = format!(
            "{:016x}{}",
            curvine_runtime::common::LocalTime::mills(),
            token()
        );
        let started = std::time::Instant::now();
        let response = super::FsClient::new(context.clone()).hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: serde_json::to_vec(&serde_json::json!({"op":op,"entity":handle.data.entity,"data":handle.data,"reader":handle.reader,"request_id":request_id}))
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
                return Ok(Some(deadline));
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
    unreachable!("bounded lease retry loop always returns")
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

#[cfg(test)]
mod lease_tests {
    use super::*;
    use std::time::{Duration, Instant};

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
