// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
use curvine_fs_api::RpcCode;
use curvine_model::{hsm::native::*, WorkerAddress};
use curvine_proto::{HsmControlRequest, HsmControlResponse};
use curvine_rpc::{
    client::{ClientConf, RpcClient},
    message::Builder,
};
/// One bounded native HSM data RPC. Network failures preserve uncertainty.
pub async fn hsm_worker_rpc(
    conf: &ClientConf,
    worker: &WorkerAddress,
    request: &WorkerRequest,
) -> HsmResult<WorkerReply> {
    let call = async {
        let client = RpcClient::with_raw(&worker.inet_addr(), conf)
            .await
            .map_err(|e| HsmError::Unknown(e.to_string()))?;
        let payload = serde_json::to_vec(request).map_err(|e| HsmError::Invalid(e.to_string()))?;
        let msg = Builder::new_rpc(RpcCode::HsmData)
            .proto_header(HsmControlRequest {
                protocol_version: 1,
                payload,
            })
            .build();
        let response = client
            .rpc(msg)
            .await
            .map_err(|e| HsmError::Unknown(e.to_string()))?;
        if !response.is_success() {
            return Err(HsmError::Unknown(response.to_error_msg()));
        }
        let body: HsmControlResponse = response
            .parse_header()
            .map_err(|e| HsmError::Unknown(e.to_string()))?;
        if body.protocol_version != 1 || body.payload.len() > 512 * 1024 {
            return Err(HsmError::Unknown("invalid HSM worker response".into()));
        }
        let result: WorkerReply =
            serde_json::from_slice(&body.payload).map_err(|e| HsmError::Unknown(e.to_string()))?;
        result.into_result()
    };
    tokio::time::timeout(std::time::Duration::from_millis(conf.rpc_timeout_ms), call)
        .await
        .map_err(|_| HsmError::Unknown("HSM worker RPC timeout".into()))?
}

/// Ordinary readers ask the authority to pin online data or enqueue one shared Recall.
pub async fn open_managed(
    fs: &super::CurvineFileSystem,
    path: &curvine_fs_api::Path,
) -> curvine_error::FsResult<Option<ReadHandle>> {
    let reader = token();
    let started = std::time::Instant::now();
    loop {
        let response = fs
            .hsm_control(HsmControlRequest {
                protocol_version: 1,
                payload: serde_json::to_vec(
                    &serde_json::json!({"op":"read_path","path":path.path(),"reader":reader}),
                )
                .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
            })
            .await?;
        let value: serde_json::Value = serde_json::from_slice(&response.payload)
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        match value["kind"].as_str() {
            Some("normal") => return Ok(None),
            Some("read") => {
                return Ok(Some(
                    serde_json::from_value(value["data"].clone())
                        .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
                ))
            }
            Some("pending") => {
                if started.elapsed() > std::time::Duration::from_secs(300) {
                    return curvine_core_error::err_box!(
                        "automatic HSM recall timed out; queued work remains durable"
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Some("error") if value["data"]["code"] == "unsupported" => return Ok(None),
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
) -> curvine_error::FsResult<()> {
    for attempt in 0..16 {
        let response = super::FsClient::new(context.clone()).hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: serde_json::to_vec(&serde_json::json!({"op":op,"entity":handle.data.entity,"reader":handle.reader}))
                .map_err(|e| curvine_error::FsError::common(e.to_string()))?,
        }).await?;
        let value: serde_json::Value = serde_json::from_slice(&response.payload)
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        if value["kind"] == "done" {
            return Ok(());
        }
        if value["data"]["code"] == "conflict" && attempt < 15 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            continue;
        }
        return curvine_core_error::err_box!("HSM read lease: {}", value["data"]);
    }
    unreachable!("bounded lease retry loop always returns")
}
