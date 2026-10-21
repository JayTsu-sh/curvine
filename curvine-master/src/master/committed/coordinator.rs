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
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    running: bool,
}
pub(crate) struct NativeCoordinator {
    queue: Mutex<Queue>,
    slots: Arc<Semaphore>,
}
impl Default for NativeCoordinator {
    fn default() -> Self {
        Self {
            queue: Mutex::default(),
            slots: Arc::new(Semaphore::new(128)),
        }
    }
}
impl NativeCoordinator {
    pub(super) async fn execute(
        self: &Arc<Self>,
        fs: MasterFilesystem,
        mount_manager: Arc<crate::master::MountManager>,
        request: NativeMutationRequest,
        input: NativeInput,
    ) -> FsResult<NativeMutationResponse> {
        let queued = std::time::Instant::now();
        let slot = self
            .slots
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
                _slot: slot,
            });
            let spawn = !queue.running;
            queue.running = true;
            spawn
        };
        if spawn {
            let this = self.clone();
            crate::master::spawn_authority(
                "native",
                async move { this.run(fs, mount_manager).await },
            );
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
        let mut batch = vec![];
        let mut bytes = 0usize;
        while batch.len() < 32 {
            let Some(next) = queue.pending.front() else {
                break;
            };
            if !batch.is_empty() && bytes.saturating_add(next.request.header.len()) > 512 * 1024 {
                break;
            }
            let next = queue.pending.pop_front().unwrap();
            bytes = bytes.saturating_add(next.request.header.len());
            batch.push(next);
        }
        Some(batch)
    }
    async fn run(
        self: Arc<Self>,
        fs: MasterFilesystem,
        mount_manager: Arc<crate::master::MountManager>,
    ) {
        loop {
            tokio::task::yield_now().await;
            let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
            let gate_at = std::time::Instant::now();
            let _serial = gate.read().await;
            let Some(pending) = self.take() else {
                return;
            };
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
                        item.input
                            .take()
                            .unwrap()
                            .prepare(&fs, &mount_manager, &mut view, time)
                    };
                    let (edits, reply) = match prepared {
                        Ok(prepared) => {
                            if let Some(allocation) = prepared.allocation {
                                allocations.push((commands.len(), allocation));
                            }
                            (
                                prepared.edits,
                                NativeReply {
                                    code: request.code as i8,
                                    success: true,
                                    header: prepared.response,
                                    body: vec![],
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
            for (index, allocation) in allocations {
                let retain = match &committed {
                    Ok(results) => {
                        results[index].success || results[index].status == Status::Uncertain as i32
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
