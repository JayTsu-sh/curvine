// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Owned preparation and committed publication; the transitional gate remains
//! until every legacy native/background writer joins this ordered path.
use super::{
    authority::*,
    transport,
    view::{BatchView, PreparedEdits},
    NativeInput,
};
use crate::master::{fs::MasterFilesystem, Master};
use curvine_core_error::ErrorExt;
use curvine_error::{FsError, FsResult};
use curvine_proto::{
    NativeMutationRequest, NativeMutationResponse, NativeMutationStatusProto as Status,
};
use curvine_runtime::common::LocalTime;
use std::{
    collections::{HashMap, VecDeque},
    sync::{atomic::Ordering, Arc, Mutex},
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

struct Pending {
    request: NativeMutationRequest,
    input: Option<NativeInput>,
    response: oneshot::Sender<FsResult<NativeMutationResponse>>,
    queued: std::time::Instant,
    mounts: Option<Arc<crate::master::MountManager>>,
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    running: bool,
    foreground_turns: usize,
}
pub(crate) struct NativeCoordinator {
    queue: Mutex<Queue>,
    slots: [Arc<Semaphore>; 2],
}
impl Default for NativeCoordinator {
    fn default() -> Self {
        Self {
            queue: Mutex::default(),
            slots: std::array::from_fn(|_| Arc::new(Semaphore::new(128))),
        }
    }
}
impl NativeCoordinator {
    pub(super) async fn execute(
        self: &Arc<Self>,
        fs: MasterFilesystem,
        mount_manager: Option<Arc<crate::master::MountManager>>,
        request: NativeMutationRequest,
        input: NativeInput,
    ) -> FsResult<NativeMutationResponse> {
        let queued = std::time::Instant::now();
        let slot = self.slots[input.background() as usize]
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| FsError::common(error.to_string()))?;
        let (response, result) = oneshot::channel();
        let spawn = {
            let mut queue = self.queue.lock().unwrap();
            queue.pending.push_back(Pending {
                request,
                input: Some(input),
                response,
                queued,
                mounts: mount_manager,
                _slot: slot,
            });
            let spawn = !queue.running;
            queue.running = true;
            spawn
        };
        if spawn {
            let this = self.clone();
            crate::master::spawn_authority("native", async move { this.run(fs).await });
        }
        result
            .await
            .map_err(|error| FsError::common(format!("native outcome unknown: {error}")))?
    }
    fn take(&self) -> Option<Vec<Pending>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.pending.is_empty() {
            queue.running = false;
            return None;
        }
        let foreground = queue
            .pending
            .iter()
            .position(|item| !item.input.as_ref().unwrap().background());
        let background = queue
            .pending
            .iter()
            .position(|item| item.input.as_ref().unwrap().background());
        if let Some(index) =
            background.filter(|_| foreground.is_none() || queue.foreground_turns >= 8)
        {
            queue.foreground_turns = 0;
            return Some(vec![queue.pending.remove(index).unwrap()]);
        }
        queue.foreground_turns = queue.foreground_turns.saturating_add(1).min(8);
        let mut batch: Vec<Pending> = vec![];
        let mut bytes = 0usize;
        while batch.len() < 32 {
            let Some(index) = queue
                .pending
                .iter()
                .position(|item| !item.input.as_ref().unwrap().background())
            else {
                break;
            };
            let next = &queue.pending[index];
            if !batch.is_empty()
                && (bytes.saturating_add(next.request.header.len()) > 512 * 1024
                    || next.input.as_ref().unwrap().namespace_input()
                    || batch[0].input.as_ref().unwrap().namespace_input())
            {
                break;
            }
            let next = queue.pending.remove(index).unwrap();
            bytes = bytes.saturating_add(next.request.header.len());
            batch.push(next);
        }
        Some(batch)
    }
    async fn run(self: Arc<Self>, fs: MasterFilesystem) {
        loop {
            tokio::task::yield_now().await;
            let Some(mut pending) = self.take() else {
                return;
            };
            let namespace = pending[0].input.as_ref().unwrap().namespace_input();
            let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
            let gate_at = std::time::Instant::now();
            // Both prepared edits and compact namespace inputs are owned.
            // Namespace expansion reads the current committed state at apply,
            // including readers admitted after its captured protection clock.
            // Keep speculative legacy writers excluded until commit/fencing,
            // without excluding other owned requests while Raft is waiting.
            let class = if pending[0].input.as_ref().unwrap().background() {
                crate::master::authority_gate::Class::Background
            } else {
                crate::master::authority_gate::Class::Foreground
            };
            let _shared = gate.enter(class, true).await;
            let metrics = Master::get_metrics().expect("Master metrics initialized");
            metrics
                .hsm_stage_duration
                .with_label_values(&["native_committed", "gate"])
                .observe(gate_at.elapsed().as_micros() as f64);
            for item in &pending {
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["native_committed", "queue"])
                    .observe(item.queued.elapsed().as_micros() as f64);
            }
            let term = fs.master_monitor.journal_term.load(Ordering::Acquire);
            let ready_at = std::time::Instant::now();
            let readiness = async {
                fs.hsm_read_barrier()
                    .await
                    .map_err(|error| FsError::common(error.to_string()))?;
                // Bound calls cannot bootstrap namespace state while sharing
                // the projection boundary with other owned command families.
                let ns = fs
                    .fs_dir
                    .read()
                    .hsm_namespace()?
                    .filter(|ns| ns.ordinary_format >= 2)
                    .ok_or_else(|| {
                        FsError::common("native namespace requires binding or upgrade")
                    })?;
                if term == 0 || term != fs.master_monitor.journal_term.load(Ordering::Acquire) {
                    return Err(FsError::common("native preparation leader changed"));
                }
                Ok(ns)
            }
            .await;
            metrics
                .hsm_stage_duration
                .with_label_values(&["native_committed", "readiness"])
                .observe(ready_at.elapsed().as_micros() as f64);
            let ns = match readiness {
                Ok(ns) => ns,
                Err(error) => {
                    for item in pending {
                        let _ = item.response.send(Err(FsError::common(format!(
                            "native outcome unknown: {error}"
                        ))));
                    }
                    continue;
                }
            };
            if namespace {
                let mut item = pending.remove(0);
                let result = if item.request.incarnation != ns.incarnation {
                    Ok(transport::basic(
                        &item.request,
                        Status::Fenced,
                        "native namespace incarnation changed",
                    ))
                } else {
                    fs.namespace_mutate(
                        &item.request,
                        item.input.take().unwrap(),
                        item.mounts.as_deref(),
                        term,
                    )
                    .await
                };
                let _ = item.response.send(result);
                continue;
            }
            let prepare_at = std::time::Instant::now();
            let mut commands = vec![];
            let mut allocations = vec![];
            let mut replies = vec![];
            {
                let dir = fs.fs_dir.read();
                let mut view = BatchView::new(&dir, 2 * 1024 * 1024);
                let mut submitted: HashMap<(String, u64), ([u8; 32], usize)> = HashMap::new();
                for mut item in pending {
                    if item.response.is_closed() {
                        continue;
                    }
                    let request = &item.request;
                    if request.incarnation != ns.incarnation {
                        let value = transport::basic(
                            request,
                            Status::Fenced,
                            "native namespace incarnation changed",
                        );
                        replies.push((item, Ok(value)));
                        continue;
                    }
                    let identity = transport::identity(request);
                    let mut hash = blake3::Hasher::new();
                    hash.update(&request.code.to_le_bytes());
                    hash.update(&request.header);
                    let fingerprint = *hash.finalize().as_bytes();
                    let key = (identity.session.clone(), identity.sequence);
                    if let Some((original, index)) = submitted.get(&key) {
                        let answer = if *original == fingerprint {
                            Err(*index)
                        } else {
                            Ok(transport::basic(
                                request,
                                Status::Committed,
                                "native identity rebound within batch",
                            ))
                        };
                        replies.push((item, answer));
                        continue;
                    }
                    let time = LocalTime::mills() as i64;
                    let prior = match prior(&dir, &identity, fingerprint, request.code as i8, time)
                    {
                        Ok(prior) => prior,
                        Err(error) => {
                            let value =
                                transport::basic(request, Status::Uncertain, &error.to_string());
                            replies.push((item, Ok(value)));
                            continue;
                        }
                    };
                    if let Prior::Cached(result) = prior {
                        replies.push((item, Ok(transport::response(&identity, result))));
                        continue;
                    }
                    let prepared = if matches!(prior, Prior::Rejected) {
                        Err(FsError::common(
                            "native session needs committed retirement/admission",
                        ))
                    } else {
                        item.input.take().unwrap().prepare(
                            &fs,
                            item.mounts
                                .as_deref()
                                .expect("ordinary native request mount context"),
                            &mut view,
                            time,
                        )
                    };
                    let (edits, reply) = match prepared {
                        Ok(prepared) => {
                            let body = prepared
                                .failure
                                .as_ref()
                                .map(|error| error.encode().to_vec())
                                .unwrap_or_default();
                            if let Some(allocation) = prepared.allocation {
                                allocations.push((commands.len(), allocation, body.clone()));
                            }
                            (
                                prepared.edits,
                                NativeReply {
                                    code: request.code as i8,
                                    success: prepared.failure.is_none(),
                                    header: if prepared.failure.is_none() {
                                        prepared.response
                                    } else {
                                        vec![]
                                    },
                                    body,
                                },
                            )
                        }
                        Err(error) => (
                            PreparedEdits::default(),
                            NativeReply {
                                code: request.code as i8,
                                success: false,
                                header: vec![],
                                body: error.encode().to_vec(),
                            },
                        ),
                    };
                    let index = commands.len();
                    commands.push(NativeCommand {
                        identity,
                        fingerprint,
                        time,
                        edits,
                        reply,
                    });
                    submitted.insert(key, (fingerprint, index));
                    replies.push((item, Err(index)));
                }
            }
            metrics
                .hsm_stage_duration
                .with_label_values(&["native_committed", "prepare"])
                .observe(prepare_at.elapsed().as_micros() as f64);
            let committed = if commands.is_empty() {
                Ok(vec![])
            } else {
                metrics
                    .hsm_batch_size
                    .with_label_values(&["native_committed"])
                    .observe(commands.len() as f64);
                let at = std::time::Instant::now();
                let result = fs.submit_native(commands, term).await;
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["native_committed", "commit"])
                    .observe(at.elapsed().as_micros() as f64);
                if result.is_err() {
                    // Legacy native writers still publish speculatively. Do
                    // not release their gate over an uncertain native bundle.
                    if let Err(error) = fs.hsm_read_barrier().await {
                        log::error!(
                            "uncertain native bundle could not be fenced: {error}; aborting master"
                        );
                        std::process::abort();
                    }
                }
                result
            };
            for (index, allocation, prefix_error) in allocations {
                let retain = match &committed {
                    Ok(results) => {
                        results[index].success
                            || results[index].status == Status::Uncertain as i32
                            || !prefix_error.is_empty()
                                && results[index].status == Status::Committed as i32
                                && results[index].body == prefix_error
                    }
                    Err(_) => true,
                };
                if retain {
                    allocation.retain();
                }
            }
            for (item, reply) in replies {
                let result = match reply {
                    Ok(value) => Ok(value),
                    Err(index) => match &committed {
                        Ok(results) => Ok(results[index].clone()),
                        Err(error) => Ok(transport::basic(
                            &item.request,
                            Status::Uncertain,
                            &error.to_string(),
                        )),
                    },
                };
                let _ = item.response.send(result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn foreground_batches_overtake_ttl_but_background_gets_a_bounded_turn() {
        let coordinator = NativeCoordinator::default();
        let push = |sequence, background| {
            let input = if background {
                NativeInput::TtlDelete {
                    path: "/ttl".into(),
                    inode: 2,
                    metadata: [0; 32],
                }
            } else {
                // Namespace inputs take one turn each; no 32-item ordinary
                // coalescing obscures the foreground turn bound in this test.
                NativeInput::Rename(curvine_proto::RenameRequest {
                    src: "/a".into(),
                    dst: "/b".into(),
                    flags: 0,
                })
            };
            let (response, _) = oneshot::channel();
            coordinator
                .queue
                .lock()
                .unwrap()
                .pending
                .push_back(Pending {
                    request: NativeMutationRequest {
                        sequence,
                        ..Default::default()
                    },
                    input: Some(input),
                    response,
                    queued: std::time::Instant::now(),
                    mounts: None,
                    _slot: coordinator.slots[background as usize]
                        .clone()
                        .try_acquire_owned()
                        .unwrap(),
                });
        };
        push(100, true);
        for sequence in 1..=12 {
            push(sequence, false);
        }
        for sequence in 1..=8 {
            assert_eq!(coordinator.take().unwrap()[0].request.sequence, sequence);
        }
        assert_eq!(coordinator.take().unwrap()[0].request.sequence, 100);
        for sequence in 9..=12 {
            assert_eq!(coordinator.take().unwrap()[0].request.sequence, sequence);
        }
        assert!(coordinator.take().is_none());
        assert_eq!(coordinator.slots[0].available_permits(), 128);
        assert_eq!(coordinator.slots[1].available_permits(), 128);
    }
}
