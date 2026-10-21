// Copyright 2025 OPPO. Licensed under the Apache License, Version 2.0.
//! Versioned HSM control capability contract. Native effects fail closed until
//! committed records, immutable views and conditional layouts are implemented.

use curvine_proto::{HsmControlRequest, HsmControlResponse};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const HSM_PROTOCOL_VERSION: u32 = 1;
pub const HSM_MAX_CONTROL_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HsmCapabilities {
    pub native_records: bool,
    pub stable_views: bool,
    pub conditional_release: bool,
    pub private_recall: bool,
    pub distributed_authority: bool,
    pub durable_success_results: bool,
    pub capacity_reservation: bool,
    pub transparent_recall: bool,
}

/// This baseline supports capability discovery, not destructive HSM effects.
/// Do not enable fields from configuration alone: each requires real guarantees.
pub fn control_response(request: HsmControlRequest) -> HsmControlResponse {
    let failure =
        |code, message| json!({"kind": "error", "data": {"code": code, "message": message}});
    let result = if request.protocol_version != HSM_PROTOCOL_VERSION {
        failure("unsupported", "unsupported HSM protocol version")
    } else if request.payload.len() > HSM_MAX_CONTROL_BYTES {
        failure("invalid", "HSM control request exceeds limit")
    } else {
        match serde_json::from_slice::<serde_json::Value>(&request.payload) {
            Ok(value) if value == json!({"op": "capabilities"}) => {
                json!({"kind": "capabilities", "data": HsmCapabilities::default()})
            }
            Ok(value) if value.get("op").and_then(|v| v.as_str()).is_some() => failure(
                "unsupported",
                "native HSM authority, view and layout effects are not enabled",
            ),
            _ => failure("invalid", "invalid HSM control envelope"),
        }
    };
    HsmControlResponse {
        protocol_version: HSM_PROTOCOL_VERSION,
        // serde_json::Value serialization is infallible for the values above.
        payload: result.to_string().into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(version: u32, payload: Vec<u8>) -> serde_json::Value {
        let response = control_response(HsmControlRequest {
            protocol_version: version,
            payload,
        });
        serde_json::from_slice(&response.payload).unwrap()
    }

    #[test]
    fn advertises_no_unimplemented_guarantees() {
        let value = reply(1, br#"{"op":"capabilities"}"#.to_vec());
        assert_eq!(value["kind"], "capabilities");
        let capabilities: HsmCapabilities = serde_json::from_value(value["data"].clone()).unwrap();
        assert_eq!(capabilities, HsmCapabilities::default());
    }

    #[test]
    fn release_and_recall_are_explicitly_unsupported() {
        for op in [
            "release",
            "prepare_archive",
            "commit_archive",
            "prepare_recall",
            "commit_recall",
        ] {
            let value = reply(1, json!({"op": op}).to_string().into_bytes());
            assert_eq!(value["data"]["code"], "unsupported");
        }
    }

    #[test]
    fn rejects_incompatible_malformed_and_oversized_requests() {
        assert_eq!(
            reply(2, br#"{"op":"capabilities"}"#.to_vec())["data"]["code"],
            "unsupported"
        );
        assert_eq!(reply(1, b"{".to_vec())["data"]["code"], "invalid");
        assert_eq!(
            reply(1, vec![b' '; HSM_MAX_CONTROL_BYTES + 1])["data"]["code"],
            "invalid"
        );
    }
}

pub mod native;
pub mod wire;
