// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
use super::authority::{apply_creation, NativeCommand, NativeResult, RequestIdentity, ResultKind};
use crate::master::{fs::MasterFilesystem, journal::NativeEntry, meta::FsDir};
use curvine_core_error::{err_box, CommonResult, ErrorExt};
use curvine_error::{FsError, FsResult};
use curvine_proto::{
    NativeMutationRequest, NativeMutationResponse, NativeMutationStatusProto as Status,
};
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub(super) const VERSION: u32 = 1;
pub(super) const JOURNAL_VERSION: u32 = 4;
pub(super) const MAX_ENTRY: usize = 8 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub(super) struct NativeBatch {
    pub(super) version: u32,
    pub(super) commands: Vec<NativeCommand>,
    pub(super) garbage_ack: Option<super::garbage::GarbageAck>,
}
pub(super) fn identity(request: &NativeMutationRequest) -> RequestIdentity {
    RequestIdentity {
        incarnation: request.incarnation.clone(),
        session: request.session.clone(),
        sequence: request.sequence,
        acknowledged: request.acknowledged,
    }
}
pub(super) fn response(identity: &RequestIdentity, result: NativeResult) -> NativeMutationResponse {
    let status = match result.kind {
        ResultKind::Fenced => Status::Fenced,
        ResultKind::Uncertain => Status::Uncertain,
        _ => Status::Committed,
    };
    NativeMutationResponse {
        version: VERSION,
        incarnation: identity.incarnation.clone(),
        session: identity.session.clone(),
        sequence: identity.sequence,
        code: result.reply.code as u32,
        status: status as i32,
        success: result.reply.success,
        header: result.reply.header,
        body: result.reply.body,
    }
}
pub(crate) fn basic(
    request: &NativeMutationRequest,
    status: Status,
    message: &str,
) -> NativeMutationResponse {
    NativeMutationResponse {
        version: VERSION,
        incarnation: request.incarnation.clone(),
        session: request.session.clone(),
        sequence: request.sequence,
        code: request.code,
        status: status as i32,
        success: false,
        header: vec![],
        body: FsError::common(message).encode().to_vec(),
    }
}

pub(super) fn decode_batch(bytes: &[u8]) -> CommonResult<NativeBatch> {
    let version: u32 = SerdeUtils::deserialize(bytes.get(..4).ok_or("truncated native version")?)?;
    match version {
        1 => Ok(SerdeUtils::deserialize::<super::legacy::NativeBatchV1>(bytes)?.into()),
        2 => Ok(SerdeUtils::deserialize::<super::legacy::NativeBatchV2>(bytes)?.into()),
        3 | JOURNAL_VERSION => SerdeUtils::deserialize(bytes),
        _ => err_box!("unsupported native journal version"),
    }
}

impl FsDir {
    pub(crate) fn apply_native_entry(&mut self, entry: &NativeEntry) -> CommonResult<()> {
        if entry.payload.len() > MAX_ENTRY {
            return err_box!("native entry exceeds byte limit");
        }
        let batch = decode_batch(&entry.payload)?;
        if batch.commands.len() > 32 || (batch.commands.is_empty() == batch.garbage_ack.is_none()) {
            return err_box!("unsupported native batch version or size");
        }
        if let Some(ack) = batch.garbage_ack {
            super::garbage::apply_ack(self, &ack)?;
        }
        let mut results = Vec::with_capacity(batch.commands.len());
        for command in batch.commands {
            let result = apply_creation(self, &command)?;
            results.push(response(&command.identity, result));
        }
        if let Some((_, waiter)) = self.native_local_apply.remove(&entry.ticket) {
            let _ = waiter.send(results);
        }
        Ok(())
    }
}

struct ApplyWait {
    ticket: String,
    replies:
        Arc<dashmap::DashMap<String, tokio::sync::oneshot::Sender<Vec<NativeMutationResponse>>>>,
}
impl Drop for ApplyWait {
    fn drop(&mut self) {
        self.replies.remove(&self.ticket);
    }
}

impl MasterFilesystem {
    pub(crate) async fn native_mutate(
        &self,
        request: NativeMutationRequest,
        mount_manager: Arc<crate::master::MountManager>,
        input: super::NativeInput,
    ) -> FsResult<NativeMutationResponse> {
        if request.incarnation.is_empty() {
            let gate = self.fs_dir.read().hsm_mutation_gate.clone();
            let _serial = gate.lock().await;
            self.hsm_read_barrier()
                .await
                .map_err(|error| FsError::common(error.to_string()))?;
            let ns = self
                .hsm_namespace_init()
                .await
                .map_err(|error| FsError::common(error.to_string()))?;
            let mut response = basic(
                &request,
                Status::NeedsNamespace,
                "bind native namespace before mutation",
            );
            response.incarnation = ns.incarnation;
            return Ok(response);
        }
        let coordinator = self.fs_dir.read().native_coordinator.clone();
        coordinator
            .execute(self.clone(), mount_manager, request, input)
            .await
    }

    pub(super) async fn submit_native(
        &self,
        commands: Vec<NativeCommand>,
        term: u64,
    ) -> FsResult<Vec<NativeMutationResponse>> {
        let expected = commands.len();
        let payload = SerdeUtils::serialize(&NativeBatch {
            version: JOURNAL_VERSION,
            commands,
            garbage_ack: None,
        })?;
        self.submit_native_payload(payload, expected, term).await
    }

    pub(super) async fn submit_native_payload(
        &self,
        payload: Vec<u8>,
        expected: usize,
        term: u64,
    ) -> FsResult<Vec<NativeMutationResponse>> {
        if payload.len() > MAX_ENTRY {
            return err_box!("prepared native bundle exceeds byte limit");
        }
        let (writer, op_id, replies) = {
            let fs = self.fs_dir.read();
            (
                fs.journal_writer.clone(),
                fs.next_op_id(),
                fs.native_local_apply.clone(),
            )
        };
        let ticket = curvine_model::hsm::native::token();
        let (applied, result) = tokio::sync::oneshot::channel();
        replies.insert(ticket.clone(), applied);
        let _wait = ApplyWait {
            ticket: ticket.clone(),
            replies,
        };
        let entry = NativeEntry {
            op_id,
            ticket,
            payload,
        };
        let commit = writer.in_origin_term_at(term, || writer.submit_native(entry))?;
        let results = tokio::time::timeout(Duration::from_secs(60), async {
            commit
                .await
                .map_err(|error| FsError::common(error.to_string()))?
                .map_err(FsError::common)?;
            result
                .await
                .map_err(|error| FsError::common(error.to_string()))
        })
        .await
        .map_err(|error| FsError::common(format!("native outcome unknown: {error}")))??;
        if results.len() != expected {
            return err_box!("native apply result count mismatch");
        }
        Ok(results)
    }
}
