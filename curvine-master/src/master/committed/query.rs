// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Qualified metadata reads. One bounded cohort shares a ReadIndex while the
//! legacy projection gate excludes speculative mutations. Callers execute only
//! synchronous, read-only handlers; cancellation has no authority effects.
use crate::master::{fs::MasterFilesystem, Master};
use curvine_error::{FsError, FsResult};
use std::{
    collections::VecDeque,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Instant,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

const MAX_PENDING: usize = 128;
const MAX_BATCH: usize = 64;
const MAX_BYTES: usize = 512 * 1024;
struct Pending {
    bytes: usize,
    queued: Instant,
    grant: Option<oneshot::Sender<FsResult<()>>>,
    done: oneshot::Receiver<()>,
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    running: bool,
}

pub(crate) struct ReadTicket {
    grant: oneshot::Receiver<FsResult<()>>,
    done: Option<oneshot::Sender<()>>,
}
impl ReadTicket {
    pub(crate) async fn admitted(&mut self) -> FsResult<()> {
        (&mut self.grant)
            .await
            .map_err(|_| FsError::not_leader("metadata read qualification stopped"))?
    }
    pub(crate) fn finish(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
pub(crate) struct MetadataReads {
    queue: Mutex<Queue>,
    slots: Arc<Semaphore>,
}
impl Default for MetadataReads {
    fn default() -> Self {
        Self {
            queue: Mutex::default(),
            slots: Arc::new(Semaphore::new(MAX_PENDING)),
        }
    }
}
impl MetadataReads {
    pub(crate) async fn enqueue(
        self: &Arc<Self>,
        fs: MasterFilesystem,
        bytes: usize,
    ) -> FsResult<ReadTicket> {
        if bytes > MAX_BYTES {
            return Err(FsError::common("metadata read request exceeds byte limit"));
        }
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| FsError::not_leader("metadata read coordinator stopped"))?;
        let (grant, admitted) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        let spawn = {
            let mut queue = self.queue.lock().unwrap();
            queue.pending.push_back(Pending {
                bytes,
                queued: Instant::now(),
                grant: Some(grant),
                done: finished,
                _slot: slot,
            });
            let spawn = !queue.running;
            queue.running = true;
            spawn
        };
        if spawn {
            let this = self.clone();
            crate::master::spawn_authority("metadata reads", async move { this.run(fs).await });
        }
        Ok(ReadTicket {
            grant: admitted,
            done: Some(done),
        })
    }
    fn take(&self) -> Option<Vec<Pending>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.pending.is_empty() {
            queue.running = false;
            return None;
        }
        let mut batch = vec![];
        let mut bytes = 0usize;
        while batch.len() < MAX_BATCH {
            let Some(next) = queue.pending.front() else {
                break;
            };
            if !batch.is_empty() && bytes.saturating_add(next.bytes) > MAX_BYTES {
                break;
            }
            let next = queue.pending.pop_front().unwrap();
            bytes += next.bytes;
            batch.push(next);
        }
        Some(batch)
    }
    async fn run(self: Arc<Self>, fs: MasterFilesystem) {
        loop {
            tokio::task::yield_now().await;
            let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
            let waiting = Instant::now();
            let _serial = gate.lock().await;
            let Some(mut batch) = self.take() else {
                return;
            };
            batch.retain(|request| !request.grant.as_ref().unwrap().is_closed());
            if batch.is_empty() {
                continue;
            }
            let metrics = Master::get_metrics().expect("Master metrics initialized");
            metrics
                .hsm_stage_duration
                .with_label_values(&["metadata_read", "gate"])
                .observe(waiting.elapsed().as_micros() as f64);
            let term = fs.master_monitor.journal_term.load(Ordering::Acquire);
            let ready_at = Instant::now();
            let qualified = async {
                if term == 0 || !fs.master_monitor.is_active() {
                    return Err(FsError::not_leader("metadata read has no active leader"));
                }
                fs.hsm_read_barrier()
                    .await
                    .map_err(|error| FsError::not_leader(error.to_string()))?;
                if term != fs.master_monitor.journal_term.load(Ordering::Acquire)
                    || !fs.master_monitor.is_active()
                {
                    return Err(FsError::not_leader("metadata read leader changed"));
                }
                Ok(())
            }
            .await;
            metrics
                .hsm_stage_duration
                .with_label_values(&["metadata_read", "readiness"])
                .observe(ready_at.elapsed().as_micros() as f64);
            let error = qualified.err().map(|error| error.to_string());
            let mut executing = vec![];
            for mut request in batch {
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["metadata_read", "queue"])
                    .observe(request.queued.elapsed().as_micros() as f64);
                let result = error
                    .as_ref()
                    .map_or(Ok(()), |error| Err(FsError::not_leader(error.clone())));
                if request.grant.take().unwrap().send(result).is_ok() && error.is_none() {
                    executing.push(request);
                }
            }
            if !executing.is_empty() {
                metrics
                    .hsm_batch_size
                    .with_label_values(&["metadata_read"])
                    .observe(executing.len() as f64);
            }
            let execute_at = Instant::now();
            for request in executing {
                // Dropping a read ticket cancels or finishes only a pure
                // query. It cannot abandon an unconfirmed metadata mutation.
                let _ = request.done.await;
            }
            metrics
                .hsm_stage_duration
                .with_label_values(&["metadata_read", "execute"])
                .observe(execute_at.elapsed().as_micros() as f64);
        }
    }
}
