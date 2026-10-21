// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Small JSON control headers and bounded binary RPC bodies. The data plane
//! keeps its allocation capabilities and evidence checks; payload bytes never
//! become JSON number arrays in protocol version 2.
use super::native::{HsmError, HsmResult};
use super::native::{WorkerReply, WorkerRequest, CHUNK_BYTES};

pub const DATA_PROTOCOL: u32 = 2;
pub const MAX_HEADER_BYTES: usize = 32 * 1024;

fn invalid(message: impl ToString) -> HsmError {
    HsmError::Invalid(message.to_string())
}

fn bounds(header: &[u8], body: &[u8]) -> HsmResult<()> {
    if header.len() > MAX_HEADER_BYTES || body.len() > CHUNK_BYTES {
        return Err(invalid("HSM data frame exceeds its bounded header/body"));
    }
    Ok(())
}

pub fn encode_request(request: &WorkerRequest) -> HsmResult<(Vec<u8>, &[u8])> {
    let (metadata, body) = match request {
        WorkerRequest::Write {
            endpoint,
            offset,
            bytes,
        } => (
            WorkerRequest::Write {
                endpoint: endpoint.clone(),
                offset: *offset,
                bytes: vec![],
            },
            bytes.as_slice(),
        ),
        _ => (request.clone(), &[][..]),
    };
    let header = serde_json::to_vec(&metadata).map_err(invalid)?;
    bounds(&header, body)?;
    Ok((header, body))
}

pub fn decode_request(header: &[u8], body: &[u8]) -> HsmResult<WorkerRequest> {
    bounds(header, body)?;
    let mut request: WorkerRequest = serde_json::from_slice(header).map_err(invalid)?;
    match &mut request {
        WorkerRequest::Write { bytes, .. } if bytes.is_empty() => *bytes = body.to_vec(),
        WorkerRequest::Write { .. } => {
            return Err(invalid("v2 write bytes must be in the binary body"))
        }
        WorkerRequest::Read { length, .. } if *length > CHUNK_BYTES => {
            return Err(invalid("read exceeds HSM chunk bound"))
        }
        _ if !body.is_empty() => return Err(invalid("unexpected HSM request body")),
        _ => (),
    }
    Ok(request)
}

pub fn encode_reply(reply: WorkerReply) -> HsmResult<(Vec<u8>, Vec<u8>)> {
    let (metadata, body) = match reply {
        WorkerReply::Bytes(bytes) => (WorkerReply::Bytes(vec![]), bytes),
        other => (other, vec![]),
    };
    let header = serde_json::to_vec(&metadata).map_err(invalid)?;
    bounds(&header, &body)?;
    Ok((header, body))
}

pub fn decode_reply(header: &[u8], body: &[u8]) -> HsmResult<WorkerReply> {
    bounds(header, body)?;
    let reply: WorkerReply = serde_json::from_slice(header).map_err(invalid)?;
    match reply {
        WorkerReply::Bytes(bytes) if bytes.is_empty() => Ok(WorkerReply::Bytes(body.to_vec())),
        WorkerReply::Bytes(_) => Err(invalid("v2 read bytes must be in the binary body")),
        _ if !body.is_empty() => Err(invalid("unexpected HSM response body")),
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{hsm::native::Endpoint, WorkerAddress};

    #[test]
    fn megabyte_roundtrip_uses_small_header_and_exact_binary_body() {
        let bytes: Vec<u8> = (0..CHUNK_BYTES).map(|n| (n % 251) as u8).collect();
        let request = WorkerRequest::Write {
            endpoint: Endpoint {
                allocation: "allocation".into(),
                worker: WorkerAddress::default(),
                token: "capability".into(),
            },
            offset: 123,
            bytes: bytes.clone(),
        };
        let (header, body) = encode_request(&request).unwrap();
        assert!(header.len() < 1024);
        assert_eq!(body, bytes);
        let WorkerRequest::Write {
            bytes: decoded,
            offset,
            endpoint,
        } = decode_request(&header, body).unwrap()
        else {
            panic!("wrong write request")
        };
        assert_eq!(decoded, bytes);
        assert_eq!(offset, 123);
        assert_eq!(endpoint.token, "capability");
        let (header, body) = encode_reply(WorkerReply::Bytes(bytes.clone())).unwrap();
        assert!(header.len() < 100);
        let WorkerReply::Bytes(decoded) = decode_reply(&header, &body).unwrap() else {
            panic!("wrong read reply")
        };
        assert_eq!(decoded, bytes);
    }

    #[test]
    fn rejects_oversize_inline_and_unexpected_bodies() {
        assert!(decode_reply(br#"{"kind":"done"}"#, &[1]).is_err());
        assert!(decode_reply(br#"{"kind":"bytes","data":[1]}"#, &[]).is_err());
        assert!(decode_reply(br#"{"kind":"bytes","data":[]}"#, &vec![0; CHUNK_BYTES + 1]).is_err());
        assert!(decode_request(&vec![b' '; MAX_HEADER_BYTES + 1], &[]).is_err());
        assert!(decode_request(br#"{"op":"delete","allocation":"x","admin":"a"}"#, &[1]).is_err());
    }
}
