// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Priority scheduling for projection authority. Shared owned work may overlap;
//! speculative writers remain exclusive. Scheduling never grants authority to
//! mutate, bypasses ReadIndex, or preempts an operation already in progress.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    Recall = 0,
    Foreground = 1,
    Background = 2,
}
impl Class {
    pub(crate) fn control(op: &str) -> Self {
        match op {
            "prepare_recall" | "commit_recall" | "abort_recall" | "pending_recall" => Self::Recall,
            "prepare_archive" | "commit_archive" | "release" | "gc_batch" | "collect_garbage" => {
                Self::Background
            }
            op if op.starts_with("policy_") || op.starts_with("intent_") => Self::Background,
            _ => Self::Foreground,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Recall => "recall",
            Self::Foreground => "foreground",
            Self::Background => "background",
        }
    }
}
// A grant is one bounded coordinator turn, often containing many logical RPCs.
const CYCLE: [Class; 13] = [
    Class::Recall,
    Class::Recall,
    Class::Recall,
    Class::Recall,
    Class::Foreground,
    Class::Foreground,
    Class::Foreground,
    Class::Foreground,
    Class::Foreground,
    Class::Foreground,
    Class::Foreground,
    Class::Foreground,
    Class::Background,
];
const MAX_SHARED: usize = 32;
const MAX_CLASS_PENDING: usize = 128;
struct Pending {
    id: u64,
    shared: bool,
    send: oneshot::Sender<AuthorityGuard>,
    slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct State {
    queues: [VecDeque<Pending>; 3],
    readers: usize,
    writer: bool,
    cursor: usize,
    selected: Option<Class>,
    next_id: u64,
}
/// Owns unit-valued projection access. Its queues/guards are local, never part
/// of a checkpoint; persistent business ordering and fencing remain in Raft.
pub struct AuthorityGate {
    state: Mutex<State>,
    slots: [Arc<Semaphore>; 3],
}
impl Default for AuthorityGate {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            slots: std::array::from_fn(|_| Arc::new(Semaphore::new(MAX_CLASS_PENDING))),
        }
    }
}
pub struct AuthorityGuard {
    gate: Arc<AuthorityGate>,
    shared: bool,
    _slot: OwnedSemaphorePermit,
}
impl Drop for AuthorityGuard {
    fn drop(&mut self) {
        {
            let mut state = self.gate.state.lock().unwrap();
            if self.shared {
                state.readers -= 1;
            } else {
                assert!(state.writer);
                state.writer = false;
            }
        }
        self.gate.pump();
    }
}
struct Waiting {
    gate: Arc<AuthorityGate>,
    id: u64,
    armed: bool,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        if self.armed {
            {
                let mut state = self.gate.state.lock().unwrap();
                for queue in &mut state.queues {
                    queue.retain(|item| item.id != self.id);
                }
            }
            self.gate.pump();
        }
    }
}
impl AuthorityGate {
    pub async fn read(self: &Arc<Self>) -> AuthorityGuard {
        self.enter(Class::Foreground, true).await
    }
    pub async fn write(self: &Arc<Self>) -> AuthorityGuard {
        self.enter(Class::Foreground, false).await
    }
    pub fn blocking_write(self: &Arc<Self>) -> AuthorityGuard {
        // Only the existing OS scheduler/spawn_blocking paths use this API.
        futures::executor::block_on(self.enter(Class::Background, false))
    }
    pub(crate) async fn enter(self: &Arc<Self>, class: Class, shared: bool) -> AuthorityGuard {
        let started = std::time::Instant::now();
        let slot = self.slots[class as usize]
            .clone()
            .acquire_owned()
            .await
            .expect("authority class semaphore is never closed");
        let (send, receive) = oneshot::channel();
        let id = {
            let mut state = self.state.lock().unwrap();
            state.next_id = state.next_id.wrapping_add(1);
            let id = state.next_id;
            state.queues[class as usize].push_back(Pending {
                id,
                shared,
                send,
                slot,
            });
            id
        };
        let mut waiting = Waiting {
            gate: self.clone(),
            id,
            armed: true,
        };
        self.pump();
        let guard = receive
            .await
            .expect("authority gate owns every queued grant");
        waiting.armed = false;
        if let Ok(metrics) = super::Master::get_metrics() {
            metrics
                .hsm_stage_duration
                .with_label_values(&["authority_queue", class.name()])
                .observe(started.elapsed().as_micros() as f64);
        }
        guard
    }
    fn pump(self: &Arc<Self>) {
        let mut grants = vec![];
        {
            let mut state = self.state.lock().unwrap();
            loop {
                if state.writer || state.readers == MAX_SHARED {
                    break;
                }
                if state
                    .selected
                    .is_some_and(|class| state.queues[class as usize].is_empty())
                {
                    state.selected = None;
                }
                if state.selected.is_none() {
                    for _ in 0..CYCLE.len() {
                        let class = CYCLE[state.cursor];
                        state.cursor = (state.cursor + 1) % CYCLE.len();
                        if !state.queues[class as usize].is_empty() {
                            state.selected = Some(class);
                            break;
                        }
                    }
                }
                let Some(class) = state.selected else {
                    break;
                };
                let queue = &mut state.queues[class as usize];
                if queue.front().unwrap().send.is_closed() {
                    queue.pop_front();
                    state.selected = None;
                    continue;
                }
                // Once chosen, an exclusive waiter retains its turn until
                // existing readers leave. Newly arriving readers cannot
                // repeatedly skip it and starve a writer or background task.
                if !queue.front().unwrap().shared && state.readers != 0 {
                    break;
                }
                let pending = state.queues[class as usize].pop_front().unwrap();
                state.selected = None;
                if pending.shared {
                    state.readers += 1;
                } else {
                    state.writer = true;
                }
                grants.push((
                    pending.send,
                    AuthorityGuard {
                        gate: self.clone(),
                        shared: pending.shared,
                        _slot: pending.slot,
                    },
                ));
            }
        }
        // Sending/dropping an abandoned grant can release it and pump again.
        // Never run a guard destructor while holding the state mutex.
        for (send, guard) in grants {
            let _ = send.send(guard);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn pending(gate: &AuthorityGate, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if gate
                    .state
                    .lock()
                    .unwrap()
                    .queues
                    .iter()
                    .map(VecDeque::len)
                    .sum::<usize>()
                    == count
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waiters did not reach expected queue size");
    }
    #[tokio::test]
    async fn weighted_turns_prioritize_recalls_and_guarantee_background_progress() {
        let gate = Arc::new(AuthorityGate::default());
        let hold = gate.write().await;
        let order = Arc::new(Mutex::new(vec![]));
        let mut tasks = vec![];
        for class in [Class::Background, Class::Foreground, Class::Recall] {
            for _ in 0..26 {
                let gate = gate.clone();
                let order = order.clone();
                tasks.push(tokio::spawn(async move {
                    let _guard = gate.enter(class, false).await;
                    order.lock().unwrap().push(class);
                }));
            }
        }
        pending(&gate, 78).await;
        drop(hold);
        for task in tasks {
            task.await.unwrap();
        }
        let order = order.lock().unwrap();
        // All three queues remain populated through these two full cycles.
        for window in order[..26].chunks(13) {
            assert_eq!(
                window
                    .iter()
                    .filter(|class| **class == Class::Recall)
                    .count(),
                4
            );
            assert_eq!(
                window
                    .iter()
                    .filter(|class| **class == Class::Foreground)
                    .count(),
                8
            );
            assert_eq!(
                window
                    .iter()
                    .filter(|class| **class == Class::Background)
                    .count(),
                1
            );
        }
    }
    #[tokio::test]
    async fn cancelling_a_selected_writer_unblocks_shared_work_without_releasing_other_readers() {
        let gate = Arc::new(AuthorityGate::default());
        let hold = gate.read().await;
        let writer = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.write().await })
        };
        pending(&gate, 1).await;
        let reader = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.read().await })
        };
        pending(&gate, 2).await;
        writer.abort();
        assert!(matches!(writer.await, Err(error) if error.is_cancelled()));
        let reader = tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(gate.state.lock().unwrap().readers, 2);
        drop(reader);
        drop(hold);
        pending(&gate, 0).await;
    }
    #[tokio::test]
    async fn full_background_admission_does_not_consume_recall_capacity() {
        let gate = Arc::new(AuthorityGate::default());
        let hold = gate.write().await;
        let mut tasks = vec![];
        for _ in 0..MAX_CLASS_PENDING {
            let gate = gate.clone();
            tasks.push(tokio::spawn(async move {
                gate.enter(Class::Background, false).await
            }));
        }
        pending(&gate, MAX_CLASS_PENDING).await;
        let recall = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.enter(Class::Recall, false).await })
        };
        pending(&gate, MAX_CLASS_PENDING + 1).await;
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        }
        drop(hold);
        drop(
            tokio::time::timeout(Duration::from_secs(1), recall)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            gate.slots[Class::Background as usize].available_permits(),
            MAX_CLASS_PENDING
        );
    }
}
