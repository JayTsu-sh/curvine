// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Replicated cleanup checkpoints and repair demand. These are advisory work
//! records: only a newly qualified local loss token permits location deletion.
use super::{worker_manager::WorkerLossToken, MasterFilesystem};
use crate::master::{
    authority_gate::Class,
    meta::{CacheInvalidationResult, FsDir, InodeId},
};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::{FsError, FsResult};
use curvine_model::{
    hsm::native::{token, HsmError},
    WorkerInfo,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub(crate) const MAGIC: &[u8] = b"hsm-worker-cleanup-v1\n";
const JOBS: &[u8] = b"\x03hsm:worker-loss:v1:";
const REPAIRS: &[u8] = b"\x03hsm:worker-repair:v1:";
const NAMESPACE: &[u8] = b"\x03hsm:namespace:v1";
const PAGE: usize = 1024;
const MAX_ROWS: usize = PAGE + 1;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_RUN_TURNS: usize = 32;

fn job_key(worker: u32) -> Vec<u8> {
    format!("\u{3}hsm:worker-loss:v1:{worker:08x}").into_bytes()
}
fn repair_key(block: i64) -> Vec<u8> {
    format!("\u{3}hsm:worker-repair:v1:{:016x}", block as u64).into_bytes()
}
fn encode<T: Serialize>(value: &T) -> CommonResult<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> CommonResult<T> {
    Ok(serde_json::from_slice(bytes)?)
}
fn error(error: impl std::fmt::Display) -> FsError {
    FsError::common(error.to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Job {
    worker: u32,
    loss: String,
    term: u64,
    cursor: Option<i64>,
    pending: Vec<i64>,
    rewind: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Repair {
    block: i64,
    observation: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct Row {
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
struct Mutation {
    namespace: Option<Vec<u8>>,
    rows: Vec<Row>,
    // Health acknowledgements also guard current file identity/replica policy.
    inodes: Vec<(i64, Option<Vec<u8>>)>,
}

fn validate_value(key: &[u8], value: &[u8]) -> CommonResult<()> {
    if key.starts_with(JOBS) {
        let job: Job = decode(value)?;
        if key != job_key(job.worker)
            || job.term == 0
            || job.loss.len() != 32
            || !job.loss.bytes().all(|b| b.is_ascii_hexdigit())
            || job.pending.len() > PAGE
            || !job
                .pending
                .windows(2)
                .all(|ids| (ids[0] as u64) < ids[1] as u64)
        {
            return err_box!("invalid durable Worker cleanup job");
        }
    } else if key.starts_with(REPAIRS) {
        let repair: Repair = decode(value)?;
        if key != repair_key(repair.block)
            || repair.observation.len() != 32
            || !repair.observation.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return err_box!("invalid durable Worker repair hint");
        }
    } else {
        return err_box!("invalid Worker cleanup key");
    }
    Ok(())
}

fn validate(command: &Mutation) -> CommonResult<()> {
    if command.rows.is_empty() || command.rows.len() > MAX_ROWS || command.inodes.len() > 64 {
        return err_box!("invalid Worker cleanup mutation size");
    }
    let mut keys = BTreeSet::new();
    for row in &command.rows {
        if !keys.insert(&row.key)
            || !(row.key.len() == JOBS.len() + 8 && row.key.starts_with(JOBS)
                || row.key.len() == REPAIRS.len() + 16 && row.key.starts_with(REPAIRS))
        {
            return err_box!("invalid or duplicate Worker cleanup row");
        }
        let prefix = if row.key.starts_with(JOBS) {
            JOBS
        } else {
            REPAIRS
        };
        if !row.key[prefix.len()..].iter().all(u8::is_ascii_hexdigit) {
            return err_box!("invalid Worker cleanup key suffix");
        }
        for value in [&row.expected, &row.value].into_iter().flatten() {
            validate_value(&row.key, value)?;
        }
    }
    if command
        .inodes
        .iter()
        .map(|(id, _)| id)
        .collect::<BTreeSet<_>>()
        .len()
        != command.inodes.len()
    {
        return err_box!("duplicate Worker cleanup inode check");
    }
    Ok(())
}

/// All Masters apply only captured CAS inputs, never their local Worker liveness.
/// A stale command writes nothing and does not create a workflow outcome.
pub(crate) fn apply(fs: &mut FsDir, bytes: &[u8]) -> CommonResult<Value> {
    if bytes.len() > MAX_BYTES {
        return err_box!("Worker cleanup payload too large");
    }
    let command: Mutation = decode(bytes)?;
    validate(&command)?;
    let store = fs.get_rocks_store();
    if store.hsm_get(NAMESPACE)? != command.namespace {
        return Ok(json!({"applied":false}));
    }
    for row in &command.rows {
        if store.hsm_get(&row.key)? != row.expected {
            return Ok(json!({"applied":false}));
        }
    }
    for (id, expected) in &command.inodes {
        if store.inode_bytes(*id)? != *expected {
            return Ok(json!({"applied":false}));
        }
    }
    let mut batch = store.new_batch();
    for row in &command.rows {
        match &row.value {
            Some(value) => batch.hsm_put(&row.key, value)?,
            None => batch.hsm_delete(&row.key)?,
        }
    }
    batch.commit()?;
    Ok(json!({"applied":true}))
}

impl MasterFilesystem {
    #[cfg(feature = "hsm-scale-test")]
    pub fn hsm_test_seed_cleanup_locations(&self, worker: u32, ids: &[i64]) -> FsResult<()> {
        if ids.len() > 4096 {
            return Err(error("test cleanup inventory exceeds 4096 rows"));
        }
        let dir = self.fs_dir.write();
        let mut batch = dir.get_rocks_store().new_batch();
        for id in ids {
            batch.add_location(*id, &curvine_model::BlockLocation::with_id(worker))?;
        }
        batch.commit()?;
        let mut info = WorkerInfo::new(
            curvine_model::WorkerAddress {
                worker_id: worker,
                ..Default::default()
            },
            1,
        );
        info.status = curvine_model::WorkerStatus::Live;
        self.worker_manager.write().add_test_worker(info);
        Ok(())
    }

    #[cfg(feature = "hsm-scale-test")]
    pub fn hsm_test_run_cleanup(
        &self,
        worker: u32,
        on_phase: impl FnMut(&str) -> FsResult<()>,
    ) -> FsResult<usize> {
        let loss = {
            let mut wm = self.worker_manager.write();
            wm.remove_expired_worker(worker)
                .ok_or_else(|| error("test Worker is not registered"))?;
            wm.lost_worker_token(worker).unwrap()
        };
        let _running = loss
            .try_run()
            .ok_or_else(|| error("test cleanup already running"))?;
        let term = self.master_monitor.journal_term.load(Ordering::Acquire);
        self.run_cleanup_job(worker, &loss, term, on_phase)
    }

    #[cfg(feature = "hsm-scale-test")]
    pub fn hsm_test_cleanup_snapshot(&self, worker: u32) -> FsResult<Value> {
        let dir = self.fs_dir.read();
        let job = dir
            .get_rocks_store()
            .hsm_get(&job_key(worker))?
            .map(|bytes| decode::<Job>(&bytes))
            .transpose()?;
        Ok(
            json!({ "job": job, "repairs": dir.get_rocks_store().hsm_scan(REPAIRS, None, 4096)?.len(),
            "locations": dir.get_worker_block_ids(worker)?.len(),
            "term": self.master_monitor.journal_term.load(Ordering::Acquire) }),
        )
    }

    async fn submit_cleanup_mutation(
        &self,
        rows: Vec<Row>,
        inodes: Vec<(i64, Option<Vec<u8>>)>,
        term: u64,
    ) -> FsResult<bool> {
        let gate = self.fs_dir.read().hsm_mutation_gate.clone();
        let _shared = gate.enter(Class::Background, true).await;
        // No effects exist before this qualification, so a failed preflight
        // returns normally and must not abort a former Leader.
        self.hsm_read_barrier().await.map_err(error)?;
        if !self.master_monitor.is_active()
            || self.master_monitor.journal_term.load(Ordering::Acquire) != term
        {
            return Err(FsError::not_leader(
                "Worker cleanup originating term changed",
            ));
        }
        let namespace = self.fs_dir.read().get_rocks_store().hsm_get(NAMESPACE)?;
        let command = Mutation {
            namespace,
            rows,
            inodes,
        };
        validate(&command)?;
        let bytes = encode(&command)?;
        if bytes.len() > MAX_BYTES {
            return Err(error("Worker cleanup payload too large"));
        }
        let result = self
            .hsm_submit_payload([MAGIC, bytes.as_slice()].concat(), 1, Some(term))
            .await;
        if matches!(&result, Err(HsmError::Unknown(_))) {
            if let Err(fence) = self.hsm_read_barrier().await {
                log::error!(
                    "uncertain Worker cleanup prefix could not be fenced: {fence}; aborting master"
                );
                std::process::abort();
            }
        }
        let values = result.map_err(error)?;
        Ok(values[0]["applied"] == true)
    }

    fn save_cleanup_rows(
        &self,
        rows: Vec<Row>,
        inodes: Vec<(i64, Option<Vec<u8>>)>,
        term: u64,
    ) -> FsResult<bool> {
        let writer = self.fs_dir.read().journal_writer.clone();
        let fs = self.clone();
        let (send, receive) = std::sync::mpsc::channel();
        writer.spawn_background_authority(async move {
            let result = fs.submit_cleanup_mutation(rows, inodes, term).await;
            let _ = send.send(result);
        });
        // Timeout does not cancel the owner. Its only late effect is CAS of an
        // advisory checkpoint; destructive work always rechecks the local token.
        receive
            .recv_timeout(Duration::from_secs(60))
            .map_err(|error| {
                FsError::not_leader(format!("Worker cleanup owner pending: {error}"))
            })?
    }

    fn cleanup_job_row(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        job: Option<&Job>,
        term: u64,
    ) -> FsResult<bool> {
        self.save_cleanup_rows(
            vec![Row {
                key,
                expected,
                value: job.map(encode).transpose()?,
            }],
            vec![],
            term,
        )
    }

    fn stage_cleanup_job(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        job: &Job,
        term: u64,
    ) -> FsResult<bool> {
        let mut rows = vec![Row {
            key,
            expected,
            value: Some(encode(job)?),
        }];
        {
            let dir = self.fs_dir.read();
            for id in &job.pending {
                let key = repair_key(*id);
                rows.push(Row {
                    expected: dir.get_rocks_store().hsm_get(&key)?,
                    key,
                    value: Some(encode(&Repair {
                        block: *id,
                        observation: token(),
                    })?),
                });
            }
        }
        self.save_cleanup_rows(rows, vec![], term)
    }

    pub(crate) fn run_durable_worker_cleanup(
        &self,
        worker: u32,
        loss: &WorkerLossToken,
        term: u64,
    ) -> FsResult<usize> {
        self.run_cleanup_job(worker, loss, term, |_| Ok(()))
    }

    fn run_cleanup_job(
        &self,
        worker: u32,
        loss: &WorkerLossToken,
        term: u64,
        mut on_page: impl FnMut(&str) -> FsResult<()>,
    ) -> FsResult<usize> {
        let key = job_key(worker);
        let mut processed = 0;
        for _ in 0..MAX_RUN_TURNS {
            let raw = {
                let dir = self.fs_dir.read();
                if !self
                    .worker_manager
                    .read()
                    .worker_loss_is_current(worker, loss)
                {
                    return Ok(processed);
                }
                dir.get_rocks_store().hsm_get(&key)?
            };
            let mut job = match &raw {
                Some(bytes) => {
                    validate_value(&key, bytes)?;
                    decode::<Job>(bytes)?
                }
                None => Job {
                    worker,
                    loss: loss.id().to_owned(),
                    term,
                    cursor: None,
                    pending: vec![],
                    rewind: false,
                },
            };
            if raw.is_none() {
                if !self.cleanup_job_row(key.clone(), raw, Some(&job), term)? {
                    continue;
                }
                on_page("started")?;
                continue;
            }
            if job.term != term || job.loss != loss.id() {
                // Location facts differ between Masters and after replay.
                // Re-execute a staged page, then rewind the local inventory.
                job.term = term;
                job.loss = loss.id().to_owned();
                job.cursor = None;
                job.rewind = !job.pending.is_empty();
                if !self.stage_cleanup_job(key.clone(), raw, &job, term)? {
                    continue;
                }
                continue;
            }
            if job.pending.is_empty() {
                let ids = {
                    let dir = self.fs_dir.read();
                    if !self
                        .worker_manager
                        .read()
                        .worker_loss_is_current(worker, loss)
                    {
                        return Ok(processed);
                    }
                    dir.get_rocks_store()
                        .worker_blocks_page(worker, job.cursor, PAGE)?
                };
                if ids.is_empty() {
                    if self.cleanup_job_row(key.clone(), raw, None, term)? {
                        return Ok(processed);
                    }
                    continue;
                }
                job.pending = ids;
                if !self.stage_cleanup_job(key.clone(), raw, &job, term)? {
                    continue;
                }
                on_page("staged")?;
                continue;
            }
            let applied = self.apply_staged_loss_page(worker, loss, term, &job.pending)?;
            if !applied {
                return Ok(processed);
            }
            on_page("deleted")?;
            let previous = job.pending.len();
            job.cursor = if job.rewind {
                None
            } else {
                job.pending.last().copied()
            };
            job.rewind = false;
            job.pending.clear();
            if self.cleanup_job_row(key.clone(), raw, Some(&job), term)? {
                processed += previous;
            }
            on_page("advanced")?;
        }
        Ok(processed)
    }

    fn apply_staged_loss_page(
        &self,
        worker: u32,
        loss: &WorkerLossToken,
        term: u64,
        ids: &[i64],
    ) -> FsResult<bool> {
        self.committed_background_in_term(
            Some(term),
            || {
                let mut dir = self.fs_dir.write();
                let wm = self.worker_manager.read();
                if !wm.worker_loss_is_current(worker, loss) {
                    return Ok((false, CacheInvalidationResult::default()));
                }
                let mut batch = dir.get_rocks_store().new_batch();
                for id in ids {
                    batch.delete_location(*id, worker)?;
                }
                batch.commit()?;
                // On error the durable pending page survives; do not advance and
                // lose the only IDs from which UFS invalidation can be retried.
                let invalidated = dir.invalidate_lost_cache_files(worker, ids)?;
                Ok((true, invalidated))
            },
            |(_, invalidated)| invalidated.journal_dirty,
            |(_, invalidated)| {
                if !invalidated.delete_result.blocks.is_empty() {
                    self.worker_manager
                        .write()
                        .remove_blocks(&invalidated.delete_result);
                }
            },
        )
        .map(|(applied, _)| applied)
    }
}

/// Per-process scan cursors are only scheduling hints. Persisted jobs and
/// repair rows are scanned again after a term change or process restart.
pub(crate) struct Recovery {
    term: u64,
    since: Instant,
    job_cursor: Option<Vec<u8>>,
    repair_cursor: Option<Vec<u8>>,
    worker_cursor: Option<u32>,
    pub(crate) busy: Arc<AtomicBool>,
}
impl Default for Recovery {
    fn default() -> Self {
        Self {
            term: 0,
            since: Instant::now(),
            job_cursor: None,
            repair_cursor: None,
            worker_cursor: None,
            busy: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl Recovery {
    pub(crate) fn ready(&mut self, term: u64, absence: Duration) -> bool {
        if self.term != term {
            self.term = term;
            self.since = Instant::now();
            self.job_cursor = None;
            self.repair_cursor = None;
            self.worker_cursor = None;
        }
        term != 0 && self.since.elapsed() >= absence
    }
}
pub(crate) struct MaintenanceRun(pub(crate) Arc<AtomicBool>);
impl Drop for MaintenanceRun {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl MasterFilesystem {
    pub(crate) fn recover_worker_cleanups(
        &self,
        recovery: &mut Recovery,
        term: u64,
        absence: Duration,
        mut dispatch: impl FnMut(u32, WorkerLossToken),
    ) -> FsResult<()> {
        let (jobs, workers) = {
            let dir = self.fs_dir.read();
            (
                dir.get_rocks_store()
                    .hsm_scan(JOBS, recovery.job_cursor.as_deref(), 8)?,
                dir.get_rocks_store()
                    .location_workers_page(recovery.worker_cursor, 8)?,
            )
        };
        recovery.job_cursor = if jobs.len() == 8 {
            jobs.last().map(|(key, _)| key.clone())
        } else {
            None
        };
        recovery.worker_cursor = if workers.len() == 8 {
            workers.last().copied()
        } else {
            None
        };
        let mut seen = BTreeSet::new();
        for (key, raw) in jobs {
            validate_value(&key, &raw)?;
            let job: Job = decode(&raw)?;
            seen.insert(job.worker);
            // A recovered job is discarded when the Worker is registered.
            // Repair rows are independent and remain until current health is checked.
            let registered = {
                let wm = self.worker_manager.read();
                wm.get_worker(job.worker).is_some()
                    && wm.worker_contact_since(job.worker, recovery.since)
            };
            if registered {
                self.cleanup_job_row(key, Some(raw), None, term)?;
                continue;
            }
            let loss = self
                .worker_manager
                .write()
                .qualify_worker_loss(job.worker, &job.loss, term, absence);
            if let Some(loss) = loss {
                dispatch(job.worker, loss);
            }
        }
        for worker in workers {
            if seen.contains(&worker) {
                continue;
            }
            let raw = self
                .fs_dir
                .read()
                .get_rocks_store()
                .hsm_get(&job_key(worker))?;
            let id = raw
                .map(|bytes| decode::<Job>(&bytes))
                .transpose()?
                .map(|job| job.loss)
                .unwrap_or_else(token);
            let loss = self
                .worker_manager
                .write()
                .qualify_worker_loss(worker, &id, term, absence);
            if let Some(loss) = loss {
                dispatch(worker, loss);
            }
        }
        Ok(())
    }

    pub(crate) fn replay_worker_repairs(
        &self,
        recovery: &mut Recovery,
        term: u64,
        mut report: impl FnMut(Vec<i64>) -> CommonResult<()>,
    ) -> FsResult<usize> {
        let rows = self.fs_dir.read().get_rocks_store().hsm_scan(
            REPAIRS,
            recovery.repair_cursor.as_deref(),
            64,
        )?;
        recovery.repair_cursor = if rows.len() == 64 {
            rows.last().map(|(key, _)| key.clone())
        } else {
            None
        };
        let count = rows.len();
        let mut ack = vec![];
        let mut inodes = std::collections::BTreeMap::new();
        let mut demand = vec![];
        {
            let dir = self.fs_dir.read();
            let wm = self.worker_manager.read();
            for (key, raw) in rows {
                validate_value(&key, &raw)?;
                let repair: Repair = decode(&raw)?;
                let healthy = match dir.replication_block_state(repair.block)? {
                    None => true,
                    Some(state) => {
                        state
                            .locations
                            .iter()
                            .filter(|loc| {
                                wm.get_worker(loc.worker_id)
                                    .is_some_and(WorkerInfo::is_live)
                                    && wm.worker_contact_since(loc.worker_id, recovery.since)
                            })
                            .count()
                            >= usize::from(state.replicas.max(1))
                    }
                };
                if healthy {
                    let inode = InodeId::get_id(repair.block);
                    inodes.insert(inode, dir.get_rocks_store().inode_bytes(inode)?);
                    ack.push(Row {
                        key,
                        expected: Some(raw),
                        value: None,
                    });
                } else {
                    demand.push(repair.block);
                }
            }
        }
        if !demand.is_empty() {
            report(demand)?;
        }
        if !ack.is_empty() {
            self.save_cleanup_rows(ack, inodes.into_iter().collect(), term)?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::{
        journal::{HsmEntry, JournalSystem},
        Master,
    };
    use curvine_config::ClusterConf;
    use curvine_model::{BlockLocation, WorkerAddress};

    fn fixture() -> (tempfile::TempDir, MasterFilesystem) {
        Master::init_test_metrics();
        let root = tempfile::tempdir().unwrap();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        fs.master_monitor.journal_term.store(7, Ordering::Release);
        fs.fs_dir.write().hsm_enabled = true;
        (root, fs)
    }
    fn command(rows: Vec<Row>) -> Mutation {
        Mutation {
            namespace: None,
            rows,
            inodes: vec![],
        }
    }
    fn publish(fs: &MasterFilesystem, command: &Mutation) -> CommonResult<bool> {
        Ok(apply(&mut fs.fs_dir.write(), &encode(command)?)?["applied"] == true)
    }
    fn sequence(fs: &MasterFilesystem) -> u64 {
        fs.fs_dir
            .read()
            .get_rocks_store()
            .db
            .get_db()
            .latest_sequence_number()
    }
    fn read(fs: &MasterFilesystem, key: &[u8]) -> Option<Vec<u8>> {
        fs.fs_dir.read().get_rocks_store().hsm_get(key).unwrap()
    }

    #[test]
    fn durable_cleanup_pending_page_and_repairs_replay_atomically_before_effects(
    ) -> CommonResult<()> {
        let (_left, left) = fixture();
        let (_right, right) = fixture();
        let id = token();
        let job = Job {
            worker: 7,
            loss: id.clone(),
            term: 7,
            cursor: None,
            pending: (1..=1024).collect(),
            rewind: false,
        };
        let mut rows = vec![Row {
            key: job_key(7),
            expected: None,
            value: Some(encode(&job)?),
        }];
        for block in &job.pending {
            rows.push(Row {
                key: repair_key(*block),
                expected: None,
                value: Some(encode(&Repair {
                    block: *block,
                    observation: token(),
                })?),
            });
        }
        let mutation = command(rows);
        let entry = HsmEntry {
            op_id: 1,
            ticket: token(),
            payload: [MAGIC, encode(&mutation)?.as_slice()].concat(),
        };
        for fs in [&left, &right] {
            let dir = fs.fs_dir.write();
            let mut batch = dir.get_rocks_store().new_batch();
            for block in 1..=2050 {
                batch.add_location(block, &BlockLocation::with_id(7))?;
                batch.add_location(block, &BlockLocation::with_id(8))?;
            }
            batch.commit()?;
            drop(dir);
            fs.fs_dir.write().apply_hsm_entry(&entry)?;
        }
        assert_eq!(read(&left, &job_key(7)), read(&right, &job_key(7)));
        let before = sequence(&left);
        left.fs_dir.write().apply_hsm_entry(&entry)?;
        assert_eq!(
            sequence(&left),
            before,
            "replaying a stale CAS must have no projection effect"
        );
        let loss = left
            .worker_manager
            .write()
            .qualify_worker_loss(7, &id, 7, Duration::ZERO)
            .unwrap();
        assert!(left.apply_staged_loss_page(7, &loss, 7, &job.pending)?);
        assert_eq!(left.fs_dir.read().get_worker_block_ids(7)?.len(), 1026);
        assert_eq!(
            right.fs_dir.read().get_worker_block_ids(7)?.len(),
            2050,
            "Follower must not use its own derived inventory to execute cleanup"
        );
        assert_eq!(
            read(&left, &job_key(7)),
            read(&right, &job_key(7)),
            "a crash before cursor advance must leave the pending page intact"
        );
        for block in 1..=1024 {
            assert_eq!(
                read(&left, &repair_key(block)),
                read(&right, &repair_key(block))
            );
        }
        right
            .master_monitor
            .journal_term
            .store(8, Ordering::Release);
        let recovered = right
            .worker_manager
            .write()
            .qualify_worker_loss(7, &id, 8, Duration::ZERO)
            .unwrap();
        let mut renewed = job.clone();
        renewed.term = 8;
        renewed.cursor = None;
        renewed.rewind = true;
        assert!(publish(
            &right,
            &command(vec![Row {
                key: job_key(7),
                expected: read(&right, &job_key(7)),
                value: Some(encode(&renewed)?)
            }])
        )?);
        assert!(right.apply_staged_loss_page(7, &recovered, 8, &renewed.pending)?);
        assert_eq!(right.fs_dir.read().get_worker_block_ids(7)?.len(), 1026);
        assert_eq!(right.fs_dir.read().get_worker_block_ids(8)?.len(), 2050);
        left.worker_manager.write().cancel_worker_loss(7);
        let dir = left.fs_dir.write();
        let mut batch = dir.get_rocks_store().new_batch();
        batch.add_location(1, &BlockLocation::with_id(7))?;
        batch.commit()?;
        drop(dir);
        let before = sequence(&left);
        assert!(!left.apply_staged_loss_page(7, &loss, 7, &job.pending)?);
        assert_eq!(sequence(&left), before);
        assert!(
            left.fs_dir
                .read()
                .get_block_locations(1)?
                .iter()
                .any(|loc| loc.worker_id == 7),
            "a durable pending page must not authorize deleting fresh locations after rejoin"
        );
        Ok(())
    }

    #[test]
    fn durable_cleanup_stale_ack_cannot_erase_a_new_loss_or_changed_namespace() -> CommonResult<()>
    {
        let (_root, fs) = fixture();
        let first = encode(&Repair {
            block: 9,
            observation: token(),
        })?;
        let next = encode(&Repair {
            block: 9,
            observation: token(),
        })?;
        assert!(publish(
            &fs,
            &command(vec![Row {
                key: repair_key(9),
                expected: None,
                value: Some(first.clone())
            }])
        )?);
        let stale = command(vec![Row {
            key: repair_key(9),
            expected: Some(first.clone()),
            value: None,
        }]);
        assert!(publish(
            &fs,
            &command(vec![Row {
                key: repair_key(9),
                expected: Some(first),
                value: Some(next.clone())
            }])
        )?);
        let before = sequence(&fs);
        assert!(!publish(&fs, &stale)?);
        assert_eq!(sequence(&fs), before);
        assert_eq!(read(&fs, &repair_key(9)), Some(next.clone()));
        let directory = fs.fs_dir.read();
        let mut batch = directory.get_rocks_store().new_batch();
        batch.hsm_put(NAMESPACE, b"different-namespace")?;
        batch.commit()?;
        drop(directory);
        let before = sequence(&fs);
        assert!(!publish(
            &fs,
            &command(vec![Row {
                key: repair_key(9),
                expected: Some(next),
                value: None
            }])
        )?);
        assert_eq!(sequence(&fs), before);
        Ok(())
    }

    #[test]
    fn durable_cleanup_rejects_foreign_duplicate_and_oversized_mutations() -> CommonResult<()> {
        let (_root, fs) = fixture();
        let row = Row {
            key: repair_key(7),
            expected: None,
            value: Some(encode(&Repair {
                block: 7,
                observation: token(),
            })?),
        };
        let before = sequence(&fs);
        for rows in [
            vec![],
            vec![row.clone(), row.clone()],
            vec![Row {
                key: NAMESPACE.to_vec(),
                expected: None,
                value: None,
            }],
            vec![Row {
                key: [JOBS, b"zzzzzzzz"].concat(),
                expected: None,
                value: None,
            }],
            vec![row; MAX_ROWS + 1],
        ] {
            assert!(publish(&fs, &command(rows)).is_err());
        }
        assert!(apply(&mut fs.fs_dir.write(), &vec![b' '; MAX_BYTES + 1]).is_err());
        assert_eq!(sequence(&fs), before);
        let ids: Vec<_> = (0..1024).map(|i| i64::MIN + i).collect();
        let job = Job {
            worker: u32::MAX,
            loss: token(),
            term: 7,
            cursor: None,
            pending: ids.clone(),
            rewind: false,
        };
        let bytes = encode(&job)?;
        let mut rows = vec![Row {
            key: job_key(job.worker),
            expected: Some(bytes.clone()),
            value: Some(bytes),
        }];
        for block in ids {
            let value = encode(&Repair {
                block,
                observation: token(),
            })?;
            rows.push(Row {
                key: repair_key(block),
                expected: Some(value.clone()),
                value: Some(value),
            });
        }
        let worst = command(rows);
        validate(&worst)?;
        assert!(
            encode(&worst)?.len() <= MAX_BYTES,
            "a complete restaged page must fit the protocol byte limit"
        );
        Ok(())
    }

    #[test]
    fn durable_cleanup_health_ack_checks_current_inode_policy() -> CommonResult<()> {
        let (_root, fs) = fixture();
        let file = fs.create("/policy", false)?;
        let bytes = encode(&Repair {
            block: 7,
            observation: token(),
        })?;
        assert!(publish(
            &fs,
            &command(vec![Row {
                key: repair_key(7),
                expected: None,
                value: Some(bytes.clone())
            }])
        )?);
        let mut ack = command(vec![Row {
            key: repair_key(7),
            expected: Some(bytes.clone()),
            value: None,
        }]);
        let directory = fs.fs_dir.write();
        ack.inodes
            .push((file.id, directory.get_rocks_store().inode_bytes(file.id)?));
        let mut inode = directory.store.get_inode(file.id, None)?.unwrap();
        inode.as_file_mut()?.replicas = 2;
        let mut batch = directory.get_rocks_store().new_batch();
        batch.write_inode(&inode)?;
        batch.commit()?;
        drop(directory);
        let before = sequence(&fs);
        assert!(!publish(&fs, &ack)?);
        assert_eq!(sequence(&fs), before);
        assert_eq!(read(&fs, &repair_key(7)), Some(bytes));
        Ok(())
    }

    #[test]
    fn durable_cleanup_qualification_waits_for_fresh_absence_and_releases_failed_runs() {
        let (_root, fs) = fixture();
        let mut recovery = Recovery::default();
        assert!(!recovery.ready(7, Duration::from_secs(60)));
        recovery.since -= Duration::from_secs(61);
        assert!(recovery.ready(7, Duration::from_secs(60)));
        assert!(!recovery.ready(8, Duration::from_secs(60)));
        let id = token();
        let mut wm = fs.worker_manager.write();
        wm.add_test_worker(WorkerInfo::new(
            WorkerAddress {
                worker_id: 7,
                ..Default::default()
            },
            1,
        ));
        assert!(wm.qualify_worker_loss(7, &id, 8, Duration::ZERO).is_none());
        wm.remove_expired_worker(7).unwrap();
        let first = wm.qualify_worker_loss(7, &id, 8, Duration::ZERO).unwrap();
        let running = first.try_run().unwrap();
        assert!(first.try_run().is_none());
        drop(running);
        assert!(first.try_run().is_some());
        let new_term_started = Instant::now();
        wm.cancel_worker_loss(7);
        assert!(wm.worker_contact_since(7, new_term_started));
        assert!(wm
            .qualify_worker_loss(7, &id, 8, Duration::from_secs(60))
            .is_none());
        let renewed = wm.qualify_worker_loss(7, &id, 9, Duration::ZERO).unwrap();
        assert!(!wm.worker_loss_is_current(7, &first));
        assert!(wm.worker_loss_is_current(7, &renewed));
    }

    #[test]
    fn durable_cleanup_full_executor_queue_releases_retry_and_maintenance_guards(
    ) -> CommonResult<()> {
        let (_root, fs) = fixture();
        let executor = curvine_runtime::runtime::GroupExecutor::new("test-cleanup-retry", 1, 1);
        let (started, ready) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel();
        executor.spawn(move || {
            let _ = started.send(());
            let _ = held.recv_timeout(Duration::from_secs(10));
        })?;
        ready.recv_timeout(Duration::from_secs(5))?;
        executor.try_spawn(|| {})?;
        let loss = fs
            .worker_manager
            .write()
            .qualify_worker_loss(7, &token(), 7, Duration::ZERO)
            .unwrap();
        let guard = loss.try_run().unwrap();
        assert!(executor
            .try_spawn(move || {
                let _guard = guard;
            })
            .is_err());
        assert!(
            loss.try_run().is_some(),
            "a full queue must not leave the loss permanently in flight"
        );
        let busy = Arc::new(AtomicBool::new(true));
        let permit = MaintenanceRun(busy.clone());
        assert!(executor
            .try_spawn(move || {
                let _permit = permit;
            })
            .is_err());
        assert!(!busy.load(Ordering::Acquire));
        release.send(())?;
        Ok(())
    }
}
