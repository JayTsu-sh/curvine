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
