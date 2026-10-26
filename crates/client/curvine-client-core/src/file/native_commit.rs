// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Private native metadata identity/retry handling. Independent new calls may
//! rotate sessions; an in-flight or uncertain call never changes its identity.
use super::FsClient;
use curvine_core_error::ErrorExt;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_proto::{
    NativeMutationRequest, NativeMutationResponse, NativeMutationStatusProto as Status,
};
use curvine_runtime::common::LocalTime;
use prost::Message;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

const ROTATE_MS: u64 = 15 * 60 * 1000;
const MAX_WINDOW: u64 = 512;

struct Session {
    id: String,
    issued: u64,
    next: u64,
    acknowledged: u64,
    completed: BTreeSet<u64>,
    rotate: bool,
}
#[derive(Default)]
pub(super) struct ClientState {
    incarnation: Mutex<Option<String>>,
    active: Mutex<Option<Arc<Mutex<Session>>>>,
}
struct Ticket {
    session: Arc<Mutex<Session>>,
    id: String,
    sequence: u64,
    acknowledged: u64,
}
impl Ticket {
    fn complete(&self) {
        let mut session = self.session.lock().unwrap();
        session.completed.insert(self.sequence);
        loop {
            let next = session.acknowledged + 1;
            if !session.completed.remove(&next) {
                break;
            }
            session.acknowledged = next;
        }
    }
    fn uncertain(&self) {
        self.session.lock().unwrap().rotate = true;
    }
    fn known(&self) -> bool {
        let state = self.session.lock().unwrap();
        self.sequence <= state.acknowledged || state.completed.contains(&self.sequence)
    }
}
impl ClientState {
    fn ticket(&self, now: u64) -> Ticket {
        let mut active = self.active.lock().unwrap();
        let rotate = active.as_ref().is_none_or(|session| {
            let session = session.lock().unwrap();
            session.rotate
                || now.saturating_sub(session.issued) >= ROTATE_MS
                || session.next.saturating_sub(session.acknowledged) > MAX_WINDOW
                || session.next == u64::MAX
        });
        if rotate {
            *active = Some(Arc::new(Mutex::new(Session {
                id: format!("{now:016x}{}", curvine_model::hsm::native::token()),
                issued: now,
                next: 1,
                acknowledged: 0,
                completed: BTreeSet::new(),
                rotate: false,
            })));
        }
        let session = active.as_ref().unwrap().clone();
        drop(active);
        let (id, sequence, acknowledged) = {
            let mut state = session.lock().unwrap();
            let sequence = state.next;
            state.next += 1;
            (state.id.clone(), sequence, state.acknowledged)
        };
        Ticket {
            session,
            id,
            sequence,
            acknowledged,
        }
    }
    fn bind(&self, incarnation: &str) -> FsResult<()> {
        if incarnation.is_empty() {
            return Err(FsError::common("empty native namespace binding"));
        }
        let mut current = self.incarnation.lock().unwrap();
        if current.as_ref().is_some_and(|value| value != incarnation) {
            return Err(FsError::common(
                "native namespace incarnation changed; result unknown",
            ));
        }
        *current = Some(incarnation.into());
        Ok(())
    }
}

impl FsClient {
    pub(super) async fn native_rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: Message + Default,
        R: Message + Default,
    {
        self.native_rpc_encoded(code, header.encode_to_vec()).await
    }

    pub(super) async fn native_rpc_encoded<R>(&self, code: RpcCode, header: Vec<u8>) -> FsResult<R>
    where
        R: Message + Default,
    {
        self.context.flush_hsm_read_closes().await?;
        let ticket = self.native_commits.ticket(LocalTime::mills());
        let mut request = NativeMutationRequest {
            version: 1,
            incarnation: self
                .native_commits
                .incarnation
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default(),
            session: ticket.id.clone(),
            sequence: ticket.sequence,
            acknowledged: ticket.acknowledged,
            code: code as u32,
            header,
        };
        let result = async {
            for _ in 0..5 {
                let reply: NativeMutationResponse = self
                    .raw_rpc(RpcCode::NativeMutation, request.clone())
                    .await?;
                let status = Status::from_i32(reply.status).ok_or_else(|| {
                    FsError::common("unknown native reply status; result unknown")
                })?;
                if reply.version != 1
                    || reply.session != ticket.id
                    || reply.sequence != ticket.sequence
                    || reply.code != code as u32
                    || status != Status::NeedsNamespace && reply.incarnation != request.incarnation
                {
                    return Err(FsError::common(
                        "native reply identity mismatch; result unknown",
                    ));
                }
                match status {
                    Status::NeedsNamespace => {
                        if !request.incarnation.is_empty() {
                            return Err(FsError::common("native reply attempted namespace rebind"));
                        }
                        self.native_commits.bind(&reply.incarnation)?;
                        request.incarnation = reply.incarnation;
                    }
                    Status::Uncertain => {
                        // Ask again with this exact body and identity; a timed
                        // out copy may already have committed its original result.
                        tokio::task::yield_now().await;
                    }
                    Status::Committed | Status::Fenced | Status::LegacyResult => {
                        if status == Status::LegacyResult && !request.incarnation.is_empty() {
                            return Err(FsError::common(
                                "namespace-bound request returned a legacy result",
                            ));
                        }
                        if !reply.success && reply.body.is_empty() {
                            return Err(FsError::common("native failure has no encoded error"));
                        }
                        ticket.complete();
                        return if reply.success {
                            R::decode(reply.header.as_slice())
                                .map_err(|error| FsError::common(error.to_string()))
                        } else {
                            Err(FsError::decode(bytes::BytesMut::from(
                                reply.body.as_slice(),
                            )))
                        };
                    }
                }
            }
            Err(FsError::common(
                "native commit outcome unknown after retries",
            ))
        }
        .await;
        if result.is_err() && !ticket.known() {
            // Rotation affects only new independent operations. The request
            // above is never retried with a fresh session or sequence.
            ticket.uncertain();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acknowledgements_wait_for_holes_and_rotation_keeps_old_tickets_bound() {
        let state = ClientState::default();
        let first = state.ticket(1000);
        let second = state.ticket(1000);
        second.complete();
        let third = state.ticket(1000);
        assert_eq!(third.acknowledged, 0);
        first.complete();
        let fourth = state.ticket(1000);
        assert_eq!(fourth.acknowledged, 2);
        third.uncertain();
        let next = state.ticket(1000);
        assert_ne!(next.id, third.id);
        assert_eq!(third.sequence, 3);
        fourth.complete();
        third.complete();
        assert_eq!(next.session.lock().unwrap().acknowledged, 0);
        assert_eq!(third.session.lock().unwrap().acknowledged, 4);
    }
    #[test]
    fn namespace_binding_is_positive_and_cannot_be_replaced() {
        let state = ClientState::default();
        assert!(state.bind("").is_err());
        state.bind("one").unwrap();
        state.bind("one").unwrap();
        assert!(state.bind("two").is_err());
        assert_eq!(state.incarnation.lock().unwrap().as_deref(), Some("one"));
    }
}
