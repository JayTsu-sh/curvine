// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Native managed-file control and protected Worker allocations.
//! Opaque credentials are never included in Debug output.
use crate::WorkerAddress;
pub use hsm_core::{
    ArchiveKey, ArchiveReceipt, ContentDigest, DataEndpoint, DataKey, DataView, EntityKey,
    EntityState, HsmError, HsmResult, IntentClaim, IntentCompletion, IntentStatus, LifecycleIntent,
    LifecyclePage, LifecyclePolicy, ManagedAction, OperationContext, PolicyAction, RecallTarget,
    Residency, ScanBatch, TransferEvidence,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const CHUNK_BYTES: usize = 64 * 1024;
pub const HSM_ROOT: &str = "/.hsm";
pub const MANAGED_ATTR: &str = "curvine.internal.hsm.v1";
pub fn token() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}
pub fn failure(code: &str, message: impl ToString) -> Value {
    serde_json::json!({"kind":"error","data":{"code":code,"message":message.to_string()}})
}
pub fn reply(kind: &str, data: impl Serialize) -> Value {
    serde_json::json!({"kind":kind,"data":data})
}
pub fn error_value(e: HsmError) -> Value {
    let code = match &e {
        HsmError::Invalid(_) => "invalid",
        HsmError::Conflict(_) => "conflict",
        HsmError::Busy(_) => "busy",
        HsmError::NotFound(_) => "not_found",
        HsmError::Fenced => "fenced",
        HsmError::Integrity(_) => "integrity",
        HsmError::Unsupported(_) => "unsupported",
        _ => "unknown",
    };
    failure(code, e)
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Namespace {
    /// Versioned ordinary-file hooks; old journal replay keeps its original rules.
    #[serde(default)]
    pub ordinary_format: u32,
    pub root_inode: i64,
    pub storage: String,
    pub incarnation: String,
    pub epoch: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Layout {
    pub id: String,
    pub worker: WorkerAddress,
    pub admin: String,
    pub write: String,
    pub read: String,
    pub length: u64,
    pub digest: ContentDigest,
}
impl Layout {
    pub fn new(worker: WorkerAddress, length: u64, digest: ContentDigest) -> Self {
        Self {
            id: token(),
            worker,
            admin: token(),
            write: token(),
            read: token(),
            length,
            digest,
        }
    }
    pub fn endpoint(&self, writable: bool) -> DataEndpoint {
        DataEndpoint {
            adapter: if writable {
                "curvine-recall-v1"
            } else {
                "curvine-view-v1"
            }
            .into(),
            capability: serde_json::to_vec(&Endpoint {
                allocation: self.id.clone(),
                worker: self.worker.clone(),
                token: if writable {
                    self.write.clone()
                } else {
                    self.read.clone()
                },
            })
            .expect("serializable endpoint"),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub allocation: String,
    pub worker: WorkerAddress,
    pub token: String,
}
fn sealed_default() -> bool {
    true
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    /// Ordinary namespace file; keeps its inode and path across tier changes.
    #[serde(default)]
    pub ordinary: bool,
    /// Online content currently lives in the regular replicated block layout.
    #[serde(default)]
    pub native_blocks: bool,
    #[serde(default = "sealed_default")]
    pub sealed: bool,
    #[serde(default)]
    pub seal_token: Option<String>,
    #[serde(default)]
    pub atime_ms: i64,
    #[serde(default)]
    pub mtime_ms: i64,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub writing: bool,
    #[serde(default)]
    pub readers: BTreeMap<String, i64>,
    #[serde(default)]
    pub native_garbage: BTreeMap<i64, Vec<crate::BlockLocation>>,
    #[serde(default)]
    pub state_since_ms: i64,
    pub version: u64,
    pub state: EntityState,
    pub path: String,
    pub published: Option<Layout>,
    pub pending: Option<(OperationContext, RecallTarget, Layout)>,
    pub views: BTreeMap<String, DataView>,
    pub garbage: Vec<Layout>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub fingerprint: String,
    pub response: Value,
    pub data: Option<DataKey>,
    pub action: Option<ManagedAction>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowBinding {
    pub operation_id: String,
    pub kind: String,
    pub data: Option<DataKey>,
}
/// A committed compare-and-swap, not a speculative inode update.
#[derive(Clone, Serialize, Deserialize)]
pub struct Transaction {
    #[serde(default)]
    pub controls: Vec<ControlChange>,
    #[serde(default)]
    pub record_checks: Vec<(i64, u64)>,
    pub workflow: Option<WorkflowBinding>,
    pub outcome: String,
    pub fingerprint: String,
    pub response: Value,
    pub inode: Option<i64>,
    pub expected_version: Option<u64>,
    pub record: Option<Record>,
    pub namespace: Option<Namespace>,
    pub expected_epoch: Option<u64>,
    pub create: bool,
    pub time: i64,
    pub data: Option<DataKey>,
    pub action: Option<ManagedAction>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerRequest {
    Allocate {
        layout: Layout,
    },
    Write {
        endpoint: Endpoint,
        offset: u64,
        bytes: Vec<u8>,
    },
    Read {
        endpoint: Endpoint,
        offset: u64,
        length: usize,
    },
    Verify {
        endpoint: Endpoint,
    },
    Seal {
        allocation: String,
        admin: String,
    },
    Delete {
        allocation: String,
        admin: String,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum WorkerReply {
    Done,
    Bytes(Vec<u8>),
    Sealed(TransferEvidence),
    Verified(TransferEvidence),
    Error { code: String, message: String },
}
impl WorkerReply {
    pub fn into_result(self) -> HsmResult<Self> {
        match self {
            Self::Error { code, message } => Err(match code.as_str() {
                "invalid" => HsmError::Invalid(message),
                "fenced" => HsmError::Fenced,
                "conflict" => HsmError::Conflict(message),
                "not_found" => HsmError::NotFound(message),
                "integrity" => HsmError::Integrity(message),
                _ => HsmError::Unknown(message),
            }),
            value => Ok(value),
        }
    }
}

/// Internal control-key CAS, applied in the native authority batch.
#[derive(Clone, Serialize, Deserialize)]
pub struct ControlChange {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// Protected ordinary read, transported only by the native control RPC.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReadHandle {
    pub blocks: crate::FileBlocks,
    pub data: DataKey,
    pub reader: String,
    pub endpoint: Option<Endpoint>,
    pub expires_ms: i64,
}
