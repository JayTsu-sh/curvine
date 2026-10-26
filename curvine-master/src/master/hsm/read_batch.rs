// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Ordered read protection batches. Preparation builds a private, bounded
//! per-batch record projection; only committed apply changes authority. Hot
//! inode requests see earlier projected readers, so their version checks are
//! evaluated in the same order when the bundle commits.
use super::{ordinary::OrdinaryPlan, *};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

const MAX_PENDING: usize = 128;
const MAX_BATCH: usize = 32;
const MAX_BYTES: usize = 512 * 1024;

struct Pending {
    request: Value,
    queued: std::time::Instant,
    response: oneshot::Sender<Value>,
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
struct State {
    pending: VecDeque<Pending>,
    running: bool,
}
pub(crate) struct ReadBatcher {
    state: Mutex<State>,
    slots: Arc<Semaphore>,
}
impl Default for ReadBatcher {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            slots: Arc::new(Semaphore::new(MAX_PENDING)),
        }
    }
}
impl ReadBatcher {
    pub(super) async fn execute(
        self: &Arc<Self>,
        fs: MasterFilesystem,
        request: Value,
    ) -> HsmResult<Value> {
        let queued = std::time::Instant::now();
        let slot = self.slots.clone().acquire_owned().await.map_err(io)?;
        let (response, result) = oneshot::channel();
        let spawn = {
            let mut state = self.state.lock().unwrap();
            state.pending.push_back(Pending {
                request,
                queued,
                response,
                _slot: slot,
            });
            let spawn = !state.running;
            state.running = true;
            spawn
        };
        if spawn {
            let batcher = self.clone();
            crate::master::spawn_authority("read", async move { batcher.run(fs).await });
        }
        result.await.map_err(io)
    }

    fn take(&self) -> Option<Vec<Pending>> {
        let mut state = self.state.lock().unwrap();
        let n = state.pending.len().min(MAX_BATCH);
        if n == 0 {
            state.running = false;
            return None;
        }
        Some(state.pending.drain(..n).collect())
    }

    async fn run(self: Arc<Self>, fs: MasterFilesystem) {
        loop {
            tokio::task::yield_now().await;
            if let Err(error) = fs.hsm_ensure_read_namespace().await {
                let Some(requests) = self.take() else {
                    return;
                };
                let response = error_value(error);
                for request in requests {
                    let _ = request.response.send(response.clone());
                }
                continue;
            }
            let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
            let wait = std::time::Instant::now();
            let _serial = gate.write().await;
            let metrics = crate::master::Master::get_metrics()
                .expect("Master metrics initialized before read coordinator");
            metrics
                .hsm_stage_duration
                .with_label_values(&["read", "gate"])
                .observe(wait.elapsed().as_micros() as f64);
            let Some(requests) = self.take() else {
                return;
            };
            for request in &requests {
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["read", "queue"])
                    .observe(request.queued.elapsed().as_micros() as f64);
            }
            let term = fs.master_monitor.journal_term.load(Ordering::Acquire);
            let result = self.prepare_and_commit(&fs, term, requests).await;
            if matches!(&result, Err(HsmError::Unknown(_))) {
                // Cancellation cannot release the native projection gate over
                // an uncertain command. Confirm the prefix or recover cold.
                if let Err(error) = fs.hsm_read_barrier().await {
                    log::error!(
                        "uncertain read batch could not be fenced: {error}; aborting master"
                    );
                    std::process::abort();
                }
            }
            // The batch delivers each result itself. Failure before preparation
            // delivers the same typed error to all pending requests.
        }
    }

    async fn prepare_and_commit(
        &self,
        fs: &MasterFilesystem,
        term: u64,
        requests: Vec<Pending>,
    ) -> HsmResult<()> {
        if term == 0 || !fs.master_monitor.is_active() {
            for pending in requests {
                let _ = pending
                    .response
                    .send(failure("unknown", "read batch has no active leader"));
            }
            return Ok(());
        }
        let metrics = crate::master::Master::get_metrics().map_err(io)?;
        let ready_at = std::time::Instant::now();
        let readiness = async {
            fs.hsm_read_barrier().await?;
            fs.hsm_before_stable_underheld().await?;
            if term != fs.master_monitor.journal_term.load(Ordering::Acquire) {
                return Err(HsmError::Unknown("read batch leader term changed".into()));
            }
            let namespace = fs
                .fs_dir
                .read()
                .hsm_namespace()
                .map_err(io)?
                .filter(|namespace| namespace.ordinary_format >= 2)
                .ok_or_else(|| {
                    HsmError::Unknown("read namespace requires initialization".into())
                })?;
            if term != fs.master_monitor.journal_term.load(Ordering::Acquire) {
                return Err(HsmError::Unknown(
                    "read batch leader term changed during namespace initialization".into(),
                ));
            }
            Ok(namespace)
        }
        .await;
        metrics
            .hsm_stage_duration
            .with_label_values(&["read", "readiness"])
            .observe(ready_at.elapsed().as_micros() as f64);
        let ns = match readiness {
            Ok(ns) => ns,
            Err(error) => {
                let response = error_value(error);
                for pending in requests {
                    let _ = pending.response.send(response.clone());
                }
                // Nothing was prepared or submitted. The caller has its
                // retryable error; there is no speculative/unknown mutation
                // to fence. A demoted Leader must remain available to rejoin.
                return Ok(());
            }
        };
        let prepare_at = std::time::Instant::now();
        let lease_now = fs.fs_dir.read().hsm_read_time().map_err(io)?.now;
        let mut projected: HashMap<i64, Record> = HashMap::new();
        let mut pending_recalls: HashMap<i64, Value> = HashMap::new();
        let mut submitted: HashMap<String, (String, usize)> = HashMap::new();
        let mut transactions = vec![];
        let mut public_responses = vec![];
        let mut replies = vec![];
        let mut bytes = 0usize;
        let mut remaining = requests.into_iter();
        while let Some(mut pending) = remaining.next() {
            if pending.response.is_closed() {
                continue;
            }
            let unified = pending.request["op"] == "open_path";
            if unified {
                pending.request["op"] = Value::String("read_path".into());
            }
            let id = if pending.request["op"] == "read_path" {
                field::<String>(&pending.request, "path").and_then(|path| {
                    let dir = fs.fs_dir.read();
                    let inp = InodePath::resolve(dir.root_ptr(), &path, &dir.store).map_err(io)?;
                    Ok(inp.get_last_inode().map(|inode| inode.id()))
                })
            } else {
                field::<EntityKey>(&pending.request, "entity")
                    .and_then(|entity| entity_id(&entity, &ns).map(Some))
            };
            let prepared = id.and_then(|id| {
                if pending.request["op"] == "read_path" {
                    if let Some(response) = id.and_then(|id| pending_recalls.get(&id)) {
                        return Ok(OrdinaryPlan::Immediate(response.clone()));
                    }
                }
                fs.hsm_prepare_ordinary(
                    &ns,
                    &pending.request,
                    id.and_then(|id| projected.get(&id).cloned()),
                )
            });
            let result = match prepared {
                Ok(OrdinaryPlan::Immediate(value)) => {
                    let value = if unified && value["kind"] == "normal" {
                        fs.hsm_native_open(&pending.request)
                            .unwrap_or_else(error_value)
                    } else {
                        value
                    };
                    Ok(value)
                }
                Ok(OrdinaryPlan::Transaction(tx, projection, public)) => {
                    if let Some((fingerprint, index)) = submitted.get(&tx.outcome) {
                        let needs_live_reader = read_grant::is_grant(&tx.response)
                            || pending.request["op"] == "read_renew";
                        let protected = !needs_live_reader
                            || tx
                                .inode
                                .and_then(|id| projected.get(&id))
                                .and_then(|record| {
                                    pending.request["reader"]
                                        .as_str()
                                        .and_then(|reader| record.readers.get(reader))
                                })
                                .is_some_and(|deadline| *deadline > lease_now);
                        let result = if fingerprint != &tx.fingerprint {
                            Ok(failure(
                                "conflict",
                                "read request identity rebound within batch",
                            ))
                        } else if !protected {
                            Ok(failure(
                                "fenced",
                                "original read authorization is closed or expired",
                            ))
                        } else {
                            Err(*index)
                        };
                        replies.push((pending, result));
                        continue;
                    }
                    let size = pack(&tx).map_err(io)?.len();
                    if !transactions.is_empty() && bytes.saturating_add(size) > MAX_BYTES {
                        let mut tail = vec![pending];
                        tail.extend(remaining);
                        let mut state = self.state.lock().unwrap();
                        for request in tail.into_iter().rev() {
                            state.pending.push_front(request);
                        }
                        break;
                    }
                    bytes = bytes.saturating_add(size);
                    if let (Some(id), Some(record)) = (tx.inode, &projection) {
                        projected.insert(id, record.clone());
                        if tx.response["kind"] == "pending" {
                            pending_recalls.insert(id, tx.response.clone());
                        }
                    }
                    let index = transactions.len();
                    submitted.insert(tx.outcome.clone(), (tx.fingerprint.clone(), index));
                    public_responses.push(public);
                    transactions.push(tx);
                    Err(index)
                }
                Err(error) => Ok(error_value(error)),
            };
            replies.push((pending, result));
        }
        metrics
            .hsm_stage_duration
            .with_label_values(&["read", "prepare"])
            .observe(prepare_at.elapsed().as_micros() as f64);
        if !transactions.is_empty() {
            metrics
                .hsm_batch_size
                .with_label_values(&["read"])
                .observe(transactions.len() as f64);
        }
        let commit_at = std::time::Instant::now();
        let committed = fs.hsm_submit_many(&transactions, Some(term)).await;
        metrics
            .hsm_stage_duration
            .with_label_values(&["read", "commit"])
            .observe(commit_at.elapsed().as_micros() as f64);
        let error = committed
            .as_ref()
            .err()
            .map(|error| failure("unknown", error));
        for (pending, result) in replies {
            let value = if let Some(error) = &error {
                error.clone()
            } else {
                match result {
                    Ok(value) => value,
                    Err(index) => read_grant::publish(
                        &transactions[index].response,
                        committed.as_ref().unwrap()[index].clone(),
                        public_responses[index].as_ref(),
                    ),
                }
            };
            let _ = pending.response.send(value);
        }
        committed.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_readiness_is_delivered_without_reporting_an_uncertain_submission() -> CommonResult<()>
    {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let mut fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        fs.master_monitor = crate::master::MasterMonitor::new(
            curvine_runtime::sync::StateCtl::new(1),
            curvine_runtime::sync::StateCtl::new(0),
        );
        fs.master_monitor.journal_term.store(7, Ordering::Release);
        assert!(fs.master_monitor.is_active());
        let batcher = ReadBatcher::default();
        let (response, result) = oneshot::channel();
        let request = Pending {
            request: serde_json::json!({"op":"read_path","path":"/unused"}),
            queued: std::time::Instant::now(),
            response,
            _slot: batcher.slots.clone().try_acquire_owned()?,
        };
        let before = fs
            .fs_dir
            .read()
            .get_rocks_store()
            .db
            .get_db()
            .latest_sequence_number();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            // The fixture has no committed HSM journal. Its read proof fails
            // while the monitor still reports the old leader term.
            assert!(fs.hsm_read_barrier().await.is_err());
            assert!(
                batcher
                    .prepare_and_commit(&fs, 7, vec![request])
                    .await
                    .is_ok(),
                "preflight failure incorrectly requests fail-stop fencing"
            );
            let reply = result.await?;
            assert_eq!(reply["kind"], "error");
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        assert_eq!(
            fs.fs_dir
                .read()
                .get_rocks_store()
                .db
                .get_db()
                .latest_sequence_number(),
            before
        );
        assert_eq!(batcher.slots.available_permits(), MAX_PENDING);
        Ok(())
    }
}
