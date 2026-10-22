// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! One FIFO qualification queue for all authority families. A proof covers only
//! requests detached before it starts; arrivals during I/O need a new proof.
use super::JournalWriter;
use curvine_error::{FsError, FsResult};
use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
const MAX_PENDING: usize = 128;
const MAX_COHORT: usize = 64;
type Proof = Result<u64, String>;
struct Pending {
    term: u64,
    queued: Instant,
    response: oneshot::Sender<Proof>,
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    running: bool,
}
pub(super) struct ReadCohorts {
    queue: Mutex<Queue>,
    slots: Arc<Semaphore>,
}
impl Default for ReadCohorts {
    fn default() -> Self {
        Self {
            queue: Mutex::default(),
            slots: Arc::new(Semaphore::new(MAX_PENDING)),
        }
    }
}
impl ReadCohorts {
    async fn register(&self, term: u64) -> FsResult<(oneshot::Receiver<Proof>, bool)> {
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| FsError::common("read qualification queue stopped"))?;
        let (response, result) = oneshot::channel();
        let mut queue = self.queue.lock().unwrap();
        queue.pending.push_back(Pending {
            term,
            queued: Instant::now(),
            response,
            _slot: slot,
        });
        let spawn = !queue.running;
        queue.running = true;
        Ok((result, spawn))
    }
    pub(super) async fn qualify(self: &Arc<Self>, writer: Arc<JournalWriter>) -> FsResult<()> {
        if !writer.hsm_ready || writer.current_term() == 0 {
            return Err(FsError::common(
                "read qualification has no committed HSM journal/term",
            ));
        }
        let (result, spawn) = self.register(writer.current_term()).await?;
        if spawn {
            let this = self.clone();
            let metrics = writer.metrics;
            crate::master::spawn_authority("read qualification", async move {
                this.run(
                    || {
                        let writer = writer.clone();
                        async move {
                            let term = writer.current_term();
                            writer
                                .read_committed_once()
                                .await
                                .map(|_| term)
                                .map_err(|error| error.to_string())
                        }
                    },
                    Some(metrics),
                )
                .await
            });
        }
        result
            .await
            .map_err(|_| FsError::common("read qualification outcome unavailable"))?
            .map(|_| ())
            .map_err(FsError::common)
    }
    fn take(&self) -> Option<Vec<Pending>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.pending.is_empty() {
            queue.running = false;
            return None;
        }
        let count = queue.pending.len().min(MAX_COHORT);
        Some(queue.pending.drain(..count).collect())
    }
    async fn run<F, Fut>(
        self: Arc<Self>,
        proof: F,
        metrics: Option<&'static crate::master::MasterMetrics>,
    ) where
        F: Fn() -> Fut,
        Fut: Future<Output = Proof>,
    {
        loop {
            tokio::task::yield_now().await;
            let Some(mut cohort) = self.take() else {
                return;
            };
            cohort.retain(|request| !request.response.is_closed());
            if cohort.is_empty() {
                continue;
            }
            if let Some(metrics) = metrics {
                metrics
                    .hsm_batch_size
                    .with_label_values(&["read_qualification"])
                    .observe(cohort.len() as f64);
            }
            let result = proof().await;
            for request in cohort {
                if let Some(metrics) = metrics {
                    metrics
                        .hsm_stage_duration
                        .with_label_values(&["read_qualification", "queue_proof"])
                        .observe(request.queued.elapsed().as_micros() as f64);
                }
                let answer = match &result {
                    Ok(term) if *term == request.term && *term != 0 => Ok(*term),
                    Ok(_) => Err("read qualification changed originating term".into()),
                    Err(error) => Err(error.clone()),
                };
                let _ = request.response.send(answer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn ready_families_share_a_proof_but_arrivals_during_io_need_another() -> FsResult<()> {
        let queue = Arc::new(ReadCohorts::default());
        let mut first = vec![];
        for _ in 0..3 {
            first.push(queue.register(7).await?.0);
        }
        let entered = Arc::new(Semaphore::new(0));
        let resume = Arc::new(Semaphore::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = {
            let queue = queue.clone();
            let entered = entered.clone();
            let resume = resume.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                queue
                    .run(
                        || {
                            let entered = entered.clone();
                            let resume = resume.clone();
                            let calls = calls.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                entered.add_permits(1);
                                resume.acquire().await.unwrap().forget();
                                Ok(7)
                            }
                        },
                        None,
                    )
                    .await
            })
        };
        entered.acquire().await.unwrap().forget();
        let mut late = queue.register(7).await?.0;
        resume.add_permits(1);
        for result in first {
            assert_eq!(result.await.unwrap().unwrap(), 7);
        }
        entered.acquire().await.unwrap().forget();
        assert!(matches!(
            late.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        resume.add_permits(1);
        assert_eq!(late.await.unwrap().unwrap(), 7);
        runner.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(queue.slots.available_permits(), MAX_PENDING);
        Ok(())
    }
    #[tokio::test]
    async fn term_changes_and_proof_failures_never_grant_old_waiters() -> FsResult<()> {
        let queue = Arc::new(ReadCohorts::default());
        let (old, _) = queue.register(7).await?;
        let (current, _) = queue.register(8).await?;
        queue.clone().run(|| async { Ok(8) }, None).await;
        assert!(old.await.unwrap().is_err());
        assert_eq!(current.await.unwrap().unwrap(), 8);
        let (failed, _) = queue.register(8).await?;
        queue
            .clone()
            .run(|| async { Err("quorum unavailable".into()) }, None)
            .await;
        assert_eq!(failed.await.unwrap().unwrap_err(), "quorum unavailable");
        assert_eq!(queue.slots.available_permits(), MAX_PENDING);
        Ok(())
    }
    #[tokio::test]
    async fn cancellation_and_cohort_limits_preserve_bounded_fifo_progress() -> FsResult<()> {
        let queue = Arc::new(ReadCohorts::default());
        let mut replies = vec![];
        for _ in 0..MAX_PENDING {
            replies.push(queue.register(1).await?.0);
        }
        let extra = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.register(1).await })
        };
        tokio::task::yield_now().await;
        assert!(!extra.is_finished());
        // Canceled replies still consume their queue slot until detached;
        // no detached proof task can be canceled by a caller.
        drop(replies.remove(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = {
            let queue = queue.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                queue
                    .run(
                        || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            async { Ok(1) }
                        },
                        None,
                    )
                    .await
            })
        };
        for reply in replies {
            assert_eq!(reply.await.unwrap().unwrap(), 1);
        }
        let (last, spawn) = extra.await.unwrap()?;
        // The tail can arrive before or after the last drain. Start its owner
        // if it observed the idle transition, exactly as production does.
        if spawn {
            queue.clone().run(|| async { Ok(1) }, None).await;
        }
        assert_eq!(last.await.unwrap().unwrap(), 1);
        runner.await.unwrap();
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(queue.slots.available_permits(), MAX_PENDING);
        Ok(())
    }
}
