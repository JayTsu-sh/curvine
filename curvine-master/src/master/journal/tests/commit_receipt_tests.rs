// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Transport-level tests: inspect the actual serialized proposal and its ACK.
use super::*;
use crate::master::journal::DeleteEntry;
use curvine_net::net::{ConnState, NetUtils};
use curvine_raft::proto::raft::{ProposeRequest, ProposeResponse};
use curvine_raft::raft::{RaftCode, RaftError};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct ProposalPeer {
    proposals: Arc<Mutex<Vec<ProposeRequest>>>,
    applied_index: Option<u64>,
    ack_delay_ms: Arc<AtomicU64>,
}
impl HandlerService for ProposalPeer {
    type Item = Self;
    fn get_message_handler(&self, _: Option<ConnState>) -> Self {
        self.clone()
    }
}
impl MessageHandler for ProposalPeer {
    type Error = RaftError;
    fn handle(&self, msg: &Message) -> Result<Message, Self::Error> {
        assert_eq!(msg.code(), RaftCode::Propose as i8);
        self.proposals.lock().unwrap().push(msg.parse_header()?);
        std::thread::sleep(Duration::from_millis(
            self.ack_delay_ms.load(Ordering::Acquire),
        ));
        Ok(Builder::success(msg)
            .proto_header(ProposeResponse {
                applied_index: self.applied_index,
            })
            .build())
    }
}
fn sender(applied_index: Option<u64>) -> (SenderTask, ProposalPeer) {
    Master::init_test_metrics();
    let mut conf = JournalConf::with_test();
    conf.rpc_port = NetUtils::hold_available_port();
    conf.journal_addrs = vec![curvine_config::RaftPeer::from_addr(
        &conf.hostname,
        conf.rpc_port,
    )];
    conf.writer_flush_batch_ms = 60_000;
    conf.writer_flush_batch_size = 1000;
    conf.io_timeout_ms = 500;
    let rt = conf.create_runtime();
    let peer = ProposalPeer {
        proposals: Default::default(),
        applied_index,
        ack_delay_ms: Default::default(),
    };
    let server = RpcServer::with_rt(
        rt.clone(),
        ServerConf::with_hostname(&conf.hostname, conf.rpc_port),
        peer.clone(),
    );
    RpcServer::run_server(server);
    let client = RaftClient::from_conf(rt, &conf);
    (SenderTask::new(client, &conf, 0).unwrap(), peer)
}
fn native(op_id: u64) -> JournalMessage {
    JournalMessage::Entry {
        entry: JournalEntry::Delete(DeleteEntry {
            op_id,
            rpc_id: 0,
            path: format!("/{op_id}"),
            mtime: 1,
        }),
        term: Some(7),
    }
}

#[test]
fn receipt_covers_all_native_entries_without_persisting_a_barrier() {
    let (mut sender, peer) = sender(Some(42));
    sender.handle(Some(native(1))).unwrap();
    sender.handle(Some(native(2))).unwrap();
    assert!(peer.proposals.lock().unwrap().is_empty());
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    assert!(rx.try_recv().is_err());
    sender.handle(Some(JournalMessage::Commit(tx))).unwrap();
    assert_eq!(rx.try_recv().unwrap(), Ok(()));
    let proposals = peer.proposals.lock().unwrap();
    assert_eq!(proposals.len(), 1);
    assert_eq!(proposals[0].expected_term, Some(7));
    let batch: JournalBatch = SerdeUtils::deserialize(&proposals[0].data).unwrap();
    assert_eq!(batch.batch.len(), 2);
    assert!(batch
        .batch
        .iter()
        .all(|entry| matches!(entry, JournalEntry::Delete(_))));
    assert!(sender.batch.is_empty());
}

#[test]
fn missing_committed_index_rejects_receipt_and_keeps_pending_batch() {
    for applied_index in [None, Some(0)] {
        let (mut sender, peer) = sender(applied_index);
        sender.handle(Some(native(1))).unwrap();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        assert!(sender.handle(Some(JournalMessage::Commit(tx))).is_err());
        assert!(rx
            .try_recv()
            .unwrap()
            .unwrap_err()
            .contains("no committed apply index"));
        assert_eq!(sender.batch.len(), 1);
        assert_eq!(peer.proposals.lock().unwrap().len(), 1);
    }
}

#[test]
fn canceled_receipt_does_not_cancel_commit_or_duplicate_the_next_flush() {
    let (mut sender, peer) = sender(Some(42));
    sender.handle(Some(native(1))).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    drop(rx);
    sender.handle(Some(JournalMessage::Commit(tx))).unwrap();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    sender.handle(Some(JournalMessage::Commit(tx))).unwrap();
    assert_eq!(rx.try_recv().unwrap(), Ok(()));
    assert_eq!(peer.proposals.lock().unwrap().len(), 1);
    // A later request needs its own proposal; the old ACK cannot cover it.
    sender.handle(Some(native(2))).unwrap();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    sender.handle(Some(JournalMessage::Commit(tx))).unwrap();
    assert_eq!(rx.try_recv().unwrap(), Ok(()));
    assert_eq!(peer.proposals.lock().unwrap().len(), 2);
}

#[test]
fn lost_ack_returns_unknown_without_reproposing_speculative_journal() {
    let (mut sender, peer) = sender(Some(42));
    peer.ack_delay_ms.store(1500, Ordering::Release);
    sender.handle(Some(native(1))).unwrap();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    assert!(sender.handle(Some(JournalMessage::Commit(tx))).is_err());
    assert!(rx
        .try_recv()
        .unwrap()
        .unwrap_err()
        .contains("outcome unknown"));
    assert_eq!(peer.proposals.lock().unwrap().len(), 1);
    assert_eq!(sender.batch.len(), 1);
}
