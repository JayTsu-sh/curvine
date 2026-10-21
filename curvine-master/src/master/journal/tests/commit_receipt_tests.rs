// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Transport-level tests: inspect the actual serialized proposal and its ACK.
use super::*;
use crate::master::journal::{DeleteEntry, NativeEntry};
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
fn scheduled_thread_receipt_confirms_original_prefix_without_a_runtime() {
    let (mut sender, peer) = sender(Some(42));
    sender.handle(Some(native(1))).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    sender
        .handle(Some(JournalMessage::CommitBlocking(tx)))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), Ok(()));
    let proposals = peer.proposals.lock().unwrap();
    assert_eq!(proposals.len(), 1);
    let batch: JournalBatch = SerdeUtils::deserialize(&proposals[0].data).unwrap();
    assert_eq!(batch.batch.len(), 1);
    assert!(matches!(batch.batch[0], JournalEntry::Delete(_)));
}

#[test]
fn native_bundle_receives_its_own_applied_ack_after_earlier_legacy_prefix() {
    let (mut sender, peer) = sender(Some(42));
    sender.handle(Some(native(1))).unwrap();
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    sender.hsm_waiters.insert("native-bundle".into(), tx);
    sender
        .handle(Some(JournalMessage::Entry {
            term: Some(7),
            entry: JournalEntry::Native(NativeEntry {
                op_id: 2,
                ticket: "native-bundle".into(),
                payload: vec![1, 2, 3],
            }),
        }))
        .unwrap();
    assert_eq!(rx.try_recv().unwrap(), Ok(()));
    assert!(sender.hsm_waiters.is_empty());
    let proposals = peer.proposals.lock().unwrap();
    assert_eq!(proposals.len(), 2);
    assert!(proposals
        .iter()
        .all(|proposal| proposal.expected_term == Some(7)));
    let batch: JournalBatch = SerdeUtils::deserialize(&proposals[1].data).unwrap();
    assert!(matches!(&batch.batch[0], JournalEntry::Native(entry) if entry.payload == [1, 2, 3]));
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

fn authority(op_id: u64, ticket: &str, term: u64, native_entry: bool) -> JournalMessage {
    let entry = if native_entry {
        JournalEntry::Native(NativeEntry {
            op_id,
            ticket: ticket.into(),
            payload: vec![1, 2, 3],
        })
    } else {
        JournalEntry::Hsm(crate::master::journal::HsmEntry {
            op_id,
            ticket: ticket.into(),
            payload: vec![4, 5, 6],
        })
    };
    JournalMessage::Entry {
        entry,
        term: Some(term),
    }
}
fn ready_receiver(messages: Vec<JournalMessage>) -> BlockingReceiver<JournalMessage> {
    let (send, receiver) = curvine_runtime::sync::channel::BlockingChannel::new(0).split();
    for message in messages {
        send.send(message).unwrap();
    }
    receiver
}
#[test]
fn ready_native_and_hsm_entries_share_one_proposal_and_each_original_receipt() {
    let (mut sender, peer) = sender(Some(42));
    let (a, mut ar) = tokio::sync::oneshot::channel();
    let (b, mut br) = tokio::sync::oneshot::channel();
    sender.hsm_waiters.insert("a".into(), a);
    sender.hsm_waiters.insert("b".into(), b);
    // Idle time must not force the first authority entry out before draining
    // its already-ready neighbour.
    sender.last_flush_ms = 0;
    let queue = ready_receiver(vec![authority(2, "b", 7, false)]);
    sender
        .handle_ready(Some(authority(1, "a", 7, true)), &queue)
        .unwrap();
    assert_eq!(ar.try_recv().unwrap(), Ok(()));
    assert_eq!(br.try_recv().unwrap(), Ok(()));
    let proposals = peer.proposals.lock().unwrap();
    assert_eq!(proposals.len(), 1);
    let batch: JournalBatch = SerdeUtils::deserialize(&proposals[0].data).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(proposals[0].expected_term, Some(7));
    assert!(matches!(&batch.batch[0],JournalEntry::Native(entry) if entry.ticket=="a"));
    assert!(matches!(&batch.batch[1],JournalEntry::Hsm(entry) if entry.ticket=="b"));
}
#[test]
fn ready_authority_burst_preserves_legacy_and_term_boundaries() {
    let (mut sender, peer) = sender(Some(42));
    sender.handle(Some(native(1))).unwrap();
    let queue = ready_receiver(vec![authority(3, "b", 8, false), native(4)]);
    sender
        .handle_ready(Some(authority(2, "a", 7, true)), &queue)
        .unwrap();
    assert_eq!(sender.batch.len(), 1); // following legacy entry keeps normal batching
    sender.handle(None).unwrap();
    let proposals = peer.proposals.lock().unwrap();
    assert_eq!(proposals.len(), 3);
    assert_eq!(proposals[0].expected_term, Some(7));
    assert_eq!(proposals[1].expected_term, Some(7));
    assert_eq!(proposals[2].expected_term, Some(8));
    for proposal in proposals.iter() {
        let batch: JournalBatch = SerdeUtils::deserialize(&proposal.data).unwrap();
        assert_eq!(batch.len(), 1);
    }
}
#[test]
fn ready_authority_commit_fence_and_cancelled_receipt_keep_fifo_commit() {
    let (mut sender, peer) = sender(Some(42));
    let (a, ar) = tokio::sync::oneshot::channel();
    drop(ar);
    let (b, mut br) = tokio::sync::oneshot::channel();
    let (fence, mut done) = tokio::sync::oneshot::channel();
    sender.hsm_waiters.insert("a".into(), a);
    sender.hsm_waiters.insert("b".into(), b);
    let queue = ready_receiver(vec![
        authority(2, "b", 7, false),
        JournalMessage::Commit(fence),
    ]);
    sender
        .handle_ready(Some(authority(1, "a", 7, true)), &queue)
        .unwrap();
    assert_eq!(br.try_recv().unwrap(), Ok(()));
    assert_eq!(done.try_recv().unwrap(), Ok(()));
    assert!(sender.hsm_waiters.is_empty());
    assert!(sender.batch.is_empty());
    assert_eq!(peer.proposals.lock().unwrap().len(), 1);
}
#[test]
fn ready_authority_missing_commit_proof_rejects_all_receipts_without_reproposal() {
    let (mut sender, peer) = sender(None);
    let (a, mut ar) = tokio::sync::oneshot::channel();
    let (b, mut br) = tokio::sync::oneshot::channel();
    sender.hsm_waiters.insert("a".into(), a);
    sender.hsm_waiters.insert("b".into(), b);
    let queue = ready_receiver(vec![authority(2, "b", 7, false)]);
    assert!(sender
        .handle_ready(Some(authority(1, "a", 7, true)), &queue)
        .is_err());
    assert!(ar
        .try_recv()
        .unwrap()
        .unwrap_err()
        .contains("no committed apply index"));
    assert!(br
        .try_recv()
        .unwrap()
        .unwrap_err()
        .contains("no committed apply index"));
    assert_eq!(sender.batch.len(), 2);
    assert_eq!(peer.proposals.lock().unwrap().len(), 1);
}
#[test]
fn ready_authority_burst_leaves_requests_beyond_its_count_bound_in_the_queue() {
    let (mut sender, peer) = sender(Some(42));
    let queue = ready_receiver(
        (2..=33)
            .map(|id| authority(id, &id.to_string(), 7, id % 2 == 0))
            .collect(),
    );
    sender
        .handle_ready(Some(authority(1, "first", 7, true)), &queue)
        .unwrap();
    assert_eq!(peer.proposals.lock().unwrap().len(), 1);
    let next = queue.try_recv().unwrap();
    sender.handle_ready(Some(next), &queue).unwrap();
    let proposals = peer.proposals.lock().unwrap();
    assert_eq!(proposals.len(), 2);
    let first: JournalBatch = SerdeUtils::deserialize(&proposals[0].data).unwrap();
    let second: JournalBatch = SerdeUtils::deserialize(&proposals[1].data).unwrap();
    assert_eq!(first.len(), 32);
    assert_eq!(second.len(), 1);
}
