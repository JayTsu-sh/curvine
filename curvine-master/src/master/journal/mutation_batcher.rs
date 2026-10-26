// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.

//! Transitional HSM mutation batching. The coordinator owns the existing
//! global gate through commit, while each connection executes its own normal
//! synchronous handler in FIFO order. HSM controls cannot see speculative
//! native state, and cancellation of a reply cannot cancel the commit.

use super::JournalWriter;
use crate::master::MasterMonitor;
use curvine_error::FsResult;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

const MAX_PENDING: usize = 128;
const MAX_BATCH: usize = 32;
const MAX_BATCH_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct BatchState {
    pending: VecDeque<PendingMutation>,
    running: bool,
}

struct PendingMutation {
    bytes: usize,
    start: oneshot::Sender<u64>,
    started: oneshot::Receiver<()>,
    finished: oneshot::Receiver<()>,
    committed: oneshot::Sender<()>,
    _slot: OwnedSemaphorePermit,
}

/// A caller only needs a term, then reports completion of its synchronous
/// handler. The coordinator handles all scheduling and committed publication.
pub(crate) struct MutationTicket {
    start: oneshot::Receiver<u64>,
    started: Option<oneshot::Sender<()>>,
    finished: Option<oneshot::Sender<()>>,
    committed: oneshot::Receiver<()>,
}

impl MutationTicket {
    pub(crate) async fn admitted(&mut self) -> FsResult<u64> {
        let term = (&mut self.start).await.map_err(|_| {
            curvine_error::FsError::common("HSM mutation was not admitted by the active leader")
        })?;
        // Receiving a grant is not the same as starting execution: a caller
        // can be canceled between send and poll. This acknowledgement is sent
        // in the same poll as the subsequent synchronous business handler.
        let _ = self.started.take().unwrap().send(());
        Ok(term)
    }

    pub(crate) fn finish(&mut self) {
        // After admission, the caller runs its handler synchronously without
        // an await. A panic closes this channel and makes the master fail-stop.
        if let Some(finished) = self.finished.take() {
            let _ = finished.send(());
        }
    }

    pub(crate) async fn committed(self) -> FsResult<()> {
        self.committed
            .await
            .map_err(|_| curvine_error::FsError::common("HSM mutation commit outcome is unknown"))
    }
}

pub(crate) struct HsmMutationBatcher {
    state: Mutex<BatchState>,
    slots: Arc<Semaphore>,
}

impl Default for HsmMutationBatcher {
    fn default() -> Self {
        Self {
            state: Mutex::new(BatchState::default()),
            slots: Arc::new(Semaphore::new(MAX_PENDING)),
        }
    }
}

impl HsmMutationBatcher {
    pub(crate) async fn enqueue(
        self: &Arc<Self>,
        bytes: usize,
        gate: Arc<crate::master::authority_gate::AuthorityGate>,
        writer: Arc<JournalWriter>,
        monitor: MasterMonitor,
        fs: Option<crate::master::fs::MasterFilesystem>,
    ) -> FsResult<MutationTicket> {
        // Backpressure occurs before any speculative namespace mutation.
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| curvine_error::FsError::common("HSM mutation coordinator stopped"))?;
        let (start, admitted) = oneshot::channel();
        let (started, executing) = oneshot::channel();
        let (finished, done) = oneshot::channel();
        let (committed, published) = oneshot::channel();
        let spawn = {
            let mut state = self.state.lock().unwrap();
            state.pending.push_back(PendingMutation {
                bytes,
                start,
                started: executing,
                finished: done,
                committed,
                _slot: slot,
            });
            let spawn = !state.running;
            state.running = true;
            spawn
        };
        if spawn {
            let coordinator = self.clone();
            crate::master::spawn_authority("legacy native", async move {
                coordinator.run(gate, writer, monitor, fs).await
            });
        }
        Ok(MutationTicket {
            start: admitted,
            started: Some(started),
            finished: Some(finished),
            committed: published,
        })
    }

    fn take_batch(&self) -> Option<Vec<PendingMutation>> {
        let mut state = self.state.lock().unwrap();
        if state.pending.is_empty() {
            state.running = false;
            return None;
        }
        let mut batch = Vec::with_capacity(MAX_BATCH);
        let mut bytes = 0usize;
        while batch.len() < MAX_BATCH {
            let Some(next) = state.pending.front() else {
                break;
            };
            if !batch.is_empty() && bytes.saturating_add(next.bytes) > MAX_BATCH_BYTES {
                break;
            }
            let next = state.pending.pop_front().unwrap();
            bytes = bytes.saturating_add(next.bytes);
            batch.push(next);
        }
        Some(batch)
    }

    async fn run(
        self: Arc<Self>,
        gate: Arc<crate::master::authority_gate::AuthorityGate>,
        writer: Arc<JournalWriter>,
        monitor: MasterMonitor,
        fs: Option<crate::master::fs::MasterFilesystem>,
    ) {
        loop {
            // Yield once so already-ready callers can join without adding a
            // fixed timer penalty when a client has only one RPC connection.
            tokio::task::yield_now().await;
            let wait = std::time::Instant::now();
            let serial = gate.write().await;
            writer
                .metrics
                .hsm_stage_duration
                .with_label_values(&["native", "gate"])
                .observe(wait.elapsed().as_micros() as f64);
            let Some(batch) = self.take_batch() else {
                return;
            };
            let term = monitor.journal_term.load(Ordering::Acquire);
            if let Some(fs) = &fs {
                if let Err(error) = async {
                    fs.hsm_read_barrier().await?;
                    fs.hsm_before_stable_underheld().await
                }
                .await
                {
                    log::debug!("legacy mutation admission unavailable: {error}");
                    continue;
                }
            }
            let mut completed = Vec::with_capacity(batch.len());
            for request in batch {
                if !monitor.is_active()
                    || term == 0
                    || monitor.journal_term.load(Ordering::Acquire) != term
                {
                    // Dropping start tells an unstarted caller to retry at the
                    // current leader. Already executed requests still commit.
                    continue;
                }
                let execute_at = std::time::Instant::now();
                if request.start.send(term).is_err() {
                    // Canceled before admission: no mutation occurred.
                    continue;
                }
                if request.started.await.is_err() {
                    // Canceled after the grant but before the handler ran.
                    continue;
                }
                if request.finished.await.is_err() {
                    // A handler panicked or disappeared after admission. Its
                    // speculative state cannot be exposed by releasing gate.
                    log::error!("admitted HSM mutation vanished before commit; aborting master");
                    std::process::abort();
                }
                writer
                    .metrics
                    .hsm_stage_duration
                    .with_label_values(&["native", "execute"])
                    .observe(execute_at.elapsed().as_micros() as f64);
                completed.push(request.committed);
            }
            if !completed.is_empty() {
                writer
                    .metrics
                    .hsm_batch_size
                    .with_label_values(&["native"])
                    .observe(completed.len() as f64);
                let commit_at = std::time::Instant::now();
                if let Err(error) = writer.commit_pending().await {
                    // An unknown result cannot safely release the gate over
                    // speculative native state. Recovery rebuilds from Raft.
                    log::error!("HSM mutation batch commit uncertain: {error}; aborting master");
                    std::process::abort();
                }
                writer
                    .metrics
                    .hsm_stage_duration
                    .with_label_values(&["native", "commit"])
                    .observe(commit_at.elapsed().as_micros() as f64);
                for waiter in completed {
                    let _ = waiter.send(());
                }
            }
            drop(serial);
        }
    }
}
