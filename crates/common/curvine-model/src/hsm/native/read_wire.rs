// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Negotiated fast-read replies reuse the ordinary protobuf block layout.
//! Field tags and the version prefix are wire contracts. JSON control callers
//! retain their existing replies; this format never enters the Raft journal.
use super::{DataKey, Endpoint, EntityKey, ReadHandle};
use crate::ProtoUtils;
use curvine_core_error::{err_box, CommonResult};
use curvine_proto::{FileBlocksProto, WorkerAddressProto};
use prost::Message;

const MAGIC: &[u8] = b"curvine-fast-read-protobuf-v1\n";
#[derive(Clone, PartialEq, Message)]
struct Grant {
    #[prost(message, optional, tag = "1")]
    blocks: Option<FileBlocksProto>,
    #[prost(string, tag = "2")]
    storage: String,
    #[prost(string, tag = "3")]
    namespace: String,
    #[prost(string, tag = "4")]
    entity: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(string, tag = "6")]
    reader: String,
    #[prost(message, optional, tag = "7")]
    endpoint: Option<PrivateEndpoint>,
    #[prost(int64, tag = "8")]
    granted_ms: i64,
    #[prost(int64, tag = "9")]
    expires_ms: i64,
}
#[derive(Clone, PartialEq, Message)]
struct PrivateEndpoint {
    #[prost(string, tag = "1")]
    allocation: String,
    #[prost(message, optional, tag = "2")]
    worker: Option<WorkerAddressProto>,
    #[prost(string, tag = "3")]
    token: String,
}

pub fn encode(handle: ReadHandle) -> Vec<u8> {
    let grant = Grant {
        blocks: Some(ProtoUtils::file_blocks_to_pb(handle.blocks)),
        storage: handle.data.entity.storage,
        namespace: handle.data.entity.namespace,
        entity: handle.data.entity.entity,
        generation: handle.data.generation,
        reader: handle.reader,
        endpoint: handle.endpoint.map(|endpoint| PrivateEndpoint {
            allocation: endpoint.allocation,
            worker: Some(ProtoUtils::worker_address_to_pb(&endpoint.worker)),
            token: endpoint.token,
        }),
        granted_ms: handle.granted_ms,
        expires_ms: handle.expires_ms,
    };
    let mut payload = Vec::with_capacity(MAGIC.len() + grant.encoded_len());
    payload.extend_from_slice(MAGIC);
    grant.encode(&mut payload).expect("Vec write cannot fail");
    payload
}

pub fn decode(payload: &[u8]) -> CommonResult<Option<ReadHandle>> {
    let Some(payload) = payload.strip_prefix(MAGIC) else {
        return Ok(None);
    };
    let grant = Grant::decode(payload)?;
    let Some(blocks) = grant.blocks.filter(|blocks| blocks.status.id > 0) else {
        return err_box!("fast read reply omitted its file layout");
    };
    if grant.storage.is_empty()
        || grant.namespace.is_empty()
        || grant.entity.is_empty()
        || grant.reader.is_empty()
        || grant.entity.parse::<i64>().ok() != Some(blocks.status.id)
        || grant.expires_ms <= grant.granted_ms
    {
        return err_box!("fast read reply omitted its grant identity");
    }
    let endpoint = grant
        .endpoint
        .map(|endpoint| {
            let worker = endpoint
                .worker
                .ok_or("fast read reply omitted its private Worker")?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Endpoint {
                allocation: endpoint.allocation,
                worker: ProtoUtils::worker_address_from_pb(&worker),
                token: endpoint.token,
            })
        })
        .transpose()?;
    Ok(Some(ReadHandle {
        blocks: ProtoUtils::file_blocks_from_pb(blocks),
        data: DataKey {
            entity: EntityKey {
                storage: grant.storage,
                namespace: grant.namespace,
                entity: grant.entity,
            },
            generation: grant.generation,
        },
        reader: grant.reader,
        endpoint,
        granted_ms: grant.granted_ms,
        expires_ms: grant.expires_ms,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileBlocks, FileStatus, WorkerAddress};
    #[test]
    fn negotiated_reply_preserves_layout_identity_credentials_and_deadline() -> CommonResult<()> {
        let handle = ReadHandle {
            blocks: FileBlocks::new(
                FileStatus {
                    id: 42,
                    path: "/file".into(),
                    atime: 100,
                    ..Default::default()
                },
                vec![],
            ),
            data: DataKey {
                entity: EntityKey {
                    storage: "s".into(),
                    namespace: "n".into(),
                    entity: "42".into(),
                },
                generation: 9,
            },
            reader: "issuer:nonce".into(),
            endpoint: Some(Endpoint {
                allocation: "allocation".into(),
                worker: WorkerAddress::default(),
                token: "secret".into(),
            }),
            granted_ms: 100,
            expires_ms: 200,
        };
        let decoded = decode(&encode(handle.clone()))?.unwrap();
        assert_eq!(decoded.data, handle.data);
        assert_eq!(decoded.reader, handle.reader);
        assert_eq!(decoded.blocks.status.id, 42);
        assert_eq!(decoded.blocks.status.atime, 100);
        assert_eq!(decoded.endpoint.unwrap().token, "secret");
        assert_eq!(decoded.granted_ms, 100);
        assert_eq!(decoded.expires_ms, 200);
        assert!(decode(br#"{"kind":"read"}"#)?.is_none());
        assert!(decode(MAGIC).is_err());
        Ok(())
    }
}
