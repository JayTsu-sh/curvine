// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! A bounded committed RocksDB snapshot performs cheap candidate filtering off
//! the admission gate. Only current, version-matched candidates resolve paths
//! and enter the conditional admission transaction.
use super::*;
use crate::master::meta::store::InodeReadLease;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Semaphore};

const SCAN_BYTES: usize = 2 * 1024 * 1024;
const SCAN_TIME: Duration = Duration::from_millis(10);
const MAX_SCANS: usize = 4;

pub(super) struct ScanSnapshot {
    pub(super) policy_bytes: Option<Vec<u8>>,
    pub(super) run_bytes: Option<Vec<u8>>,
    pub(super) cursor: Option<String>,
    pub(super) state: char,
    pub(super) rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) records: HashMap<i64, Record>,
    pub(super) more: bool,
}

impl MasterFilesystem {
    pub(crate) async fn hsm_policy_scan(&self, request: Value) -> HsmResult<Value> {
        static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let _slot = SLOTS
            .get_or_init(|| Arc::new(Semaphore::new(MAX_SCANS)))
            .clone()
            .acquire_owned()
            .await
            .map_err(io)?;
        let context: OperationContext = field(&request, "context")?;
        context.validate()?;
        self.hsm_ensure_read_namespace().await?;
        let gate = self.fs_dir.read().hsm_mutation_gate.clone();
        let serial = gate
            .enter(crate::master::authority_gate::Class::Background, true)
            .await;
        let readiness = async {
            self.hsm_read_barrier().await?;
            self.fs_dir
                .read()
                .hsm_namespace()
                .map_err(io)?
                .filter(|namespace| namespace.ordinary_format >= 2)
                .ok_or_else(|| HsmError::Unknown("scan namespace requires initialization".into()))
        }
        .await;
        let namespace = match readiness {
            Ok(ns) => ns,
            Err(error) => {
                // Snapshot qualification submitted no mutation. Deliver the
                // failure without fencing/aborting a demoted process.
                return Err(error);
            }
        };
        validate_context(&context, &namespace)?;
        let key = operation_key(&context.operation_id, "policy_scan");
        if let Some(outcome) = self.fs_dir.read().hsm_outcome(&key).map_err(io)? {
            if outcome.fingerprint != request_fingerprint(&request)? {
                return Err(HsmError::Conflict("lifecycle request ID rebound".into()));
            }
            return Ok(outcome.response);
        }
        let lease = self.fs_dir.read().store.snapshot_read_lease();
        let input = request.clone();
        let (ready, pinned) = oneshot::channel();
        let work = tokio::task::spawn_blocking(move || scan_snapshot(lease, &input, ready));
        // Pin the snapshot while the previous native/HSM prefix is committed
        // and excluded from speculative writes. No FsDir pointer crosses here.
        let ready = pinned.await.map_err(io)?;
        drop(serial);
        ready?;
        let page = work.await.map_err(io)??;
        let wait = Instant::now();
        let _serial = gate
            .enter(crate::master::authority_gate::Class::Background, true)
            .await;
        self.hsm_read_barrier().await?;
        let current = self.fs_dir.read().hsm_namespace().map_err(io)?;
        let current = current.ok_or(HsmError::Fenced)?;
        validate_context(&context, &current)?;
        if current.storage != namespace.storage || current.incarnation != namespace.incarnation {
            return Err(HsmError::Fenced);
        }
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_stage_duration
                .with_label_values(&["scan", "admission_gate"])
                .observe(wait.elapsed().as_micros() as f64);
        }
        let result = self
            .hsm_lifecycle_with_scan(&current, &context, &request, Some(page))
            .await;
        if matches!(&result, Err(HsmError::Unknown(_))) {
            if let Err(error) = self.hsm_read_barrier().await {
                log::error!(
                    "uncertain scan admission could not be fenced: {error}; aborting master"
                );
                std::process::abort();
            }
        }
        result
    }
}

fn validate_context(context: &OperationContext, ns: &Namespace) -> HsmResult<()> {
    if context.controller_epoch != ns.epoch {
        return Err(HsmError::Fenced);
    }
    if context.attempt_epoch != 1 {
        return Err(HsmError::Unsupported(
            "first native workflow requires attempt epoch 1".into(),
        ));
    }
    Ok(())
}

fn scan_snapshot(
    lease: InodeReadLease,
    request: &Value,
    ready: oneshot::Sender<HsmResult<()>>,
) -> HsmResult<ScanSnapshot> {
    scan_snapshot_after_pin(lease, request, ready, || {})
}

fn scan_snapshot_after_pin(
    lease: InodeReadLease,
    request: &Value,
    ready: oneshot::Sender<HsmResult<()>>,
    after_pin: impl FnOnce(),
) -> HsmResult<ScanSnapshot> {
    let started = Instant::now();
    let store = lease.store();
    let snapshot = store.db.get_db().snapshot();
    let _ = ready.send(Ok(()));
    after_pin();
    let cf = store
        .db
        .cf(crate::master::meta::store::RocksInodeStore::CF_COMMON)
        .map_err(io)?;
    let mut point_options = curvine_rocksdb::ReadOptions::default();
    point_options.fill_cache(false);
    point_options.set_snapshot(&snapshot);
    let get = |key: &[u8]| -> CommonResult<Option<Vec<u8>>> {
        Ok(store.db.get_db().get_cf_opt(cf, key, &point_options)?)
    };
    let id: String = field(request, "id")?;
    let policy_bytes = get(&policy_key(&id)?).map_err(io)?;
    let policy: LifecyclePolicy = required(policy_bytes.clone())?;
    let limit = request["limit"].as_u64().unwrap_or(16) as usize;
    if !(1..=SCAN_MAX).contains(&limit) {
        return Err(HsmError::Invalid("scan limit must be 1..256".into()));
    }
    let scheduled = request["scheduled"].as_bool().unwrap_or(false);
    let run_bytes =
        get(format!("\u{3}hsm:scan:{}:{}", policy.id, policy.revision).as_bytes()).map_err(io)?;
    let run: PolicyRun = raw(run_bytes.clone())?.unwrap_or_default();
    let now = curvine_runtime::common::LocalTime::mills() as i64;
    let states: &[char] = match policy.action {
        PolicyAction::Archive => &['o'],
        PolicyAction::Release => &['d', 'o'],
        PolicyAction::Recall => &['f'],
    };
    let cursor = if scheduled {
        run.cursor
    } else {
        request["after"].as_str().map(str::to_string)
    };
    let state = cursor
        .as_ref()
        .and_then(|s| s.split(':').nth(2))
        .and_then(|s| s.chars().next())
        .filter(|s| states.contains(s))
        .unwrap_or(states[0]);
    let mut result = ScanSnapshot {
        policy_bytes,
        run_bytes,
        cursor,
        state,
        rows: vec![],
        records: HashMap::new(),
        more: false,
    };
    if scheduled && (run.next_due_ms > now || !policy.enabled) {
        return Ok(result);
    }
    let prefix = format!("\u{3}hsm:candidate:{state}:").into_bytes();
    let mut start = result
        .cursor
        .as_deref()
        .filter(|s| s.as_bytes().starts_with(&prefix))
        .map(str::as_bytes)
        .unwrap_or(&prefix)
        .to_vec();
    if start != prefix {
        start.push(0);
    }
    // A cursor equal to the prefix denotes the beginning of this residency.
    let mut scan_options = curvine_rocksdb::ReadOptions::default();
    scan_options.fill_cache(false);
    scan_options.set_readahead_size(64 * 1024);
    let mut iter = snapshot.raw_iterator_cf_opt(cf, scan_options);
    iter.seek(&start);
    let mut bytes = 0usize;
    while iter.valid() {
        let key = iter
            .key()
            .ok_or_else(|| HsmError::Unknown("missing candidate key".into()))?;
        if !key.starts_with(&prefix) {
            break;
        }
        if result.rows.len() >= limit
            || !result.rows.is_empty() && (bytes >= SCAN_BYTES || started.elapsed() >= SCAN_TIME)
        {
            result.more = true;
            break;
        }
        let value = iter
            .value()
            .ok_or_else(|| HsmError::Unknown("missing candidate value".into()))?;
        let id: String = decode(value).map_err(io)?;
        let inode: i64 = id.parse().map_err(io)?;
        let record = read_record(inode, |key| {
            let value = get(key)?;
            bytes = bytes.saturating_add(value.as_ref().map_or(0, Vec::len));
            Ok(value)
        })
        .map_err(io)?;
        result.rows.push((key.to_vec(), value.to_vec()));
        if let Some(record) = record {
            let too_young = policy.selection.min_access_age_ms > 0
                && record.atime_ms > now.saturating_sub(policy.selection.min_access_age_ms as i64);
            if too_young || selected_metadata(&policy, &record, now) {
                result.records.insert(inode, record);
            }
            if too_young {
                break;
            }
        }
        iter.next();
    }
    iter.status().map_err(io)?;
    if let Ok(metrics) = crate::master::Master::get_metrics() {
        metrics
            .hsm_stage_duration
            .with_label_values(&["scan", "snapshot"])
            .observe(started.elapsed().as_micros() as f64);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_runtime::runtime::RpcRuntime;
    use std::sync::mpsc;

    #[test]
    fn snapshot_filters_old_committed_records_then_rejects_changed_rule() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let mut policy: LifecyclePolicy = serde_json::from_value(serde_json::json!({
            "id":"cold", "revision":1, "enabled":true, "prefix":"/data/",
            "min_size":0,"max_size":null,"min_residency_ms":0,"action":"release","backend":"local",
            "selection":{"owner":"selected"}
        }))?;
        {
            let dir = fs.fs_dir.read();
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(&policy_key("cold")?, &pack(&policy)?)?;
            for id in 1001..1011 {
                let record: Record = serde_json::from_value(serde_json::json!({
                    "version":1,"ordinary":true,"native_blocks":true,"sealed":true,
                    "atime_ms":1000,"mtime_ms":1000,"state_since_ms":1000,
                    "owner": if id % 2 == 0 {"selected"} else {"other"},
                    "state":{"data":{"entity":{"storage":"s","namespace":"n","entity":id.to_string()},"generation":1},
                        "revision":1,"residency":"Online","length":1024,
                        "digest":{"algorithm":"blake3","value":vec![1;32]},"anchors":[]},
                    "path":format!("/data/{id}"),"published":null,"pending":null,"views":{},"garbage":[]
                }))?;
                ordinary::write_record(&mut batch, None, &record)?;
            }
            batch.commit()?;
        }
        let lease = fs.fs_dir.read().store.snapshot_read_lease();
        let request = serde_json::json!({"op":"policy_scan","id":"cold","limit":256,"after":"\u{3}hsm:candidate:o:"});
        let (ready, pinned) = oneshot::channel();
        let (resume, paused) = mpsc::channel();
        let work = std::thread::spawn(move || {
            scan_snapshot_after_pin(lease, &request, ready, || paused.recv().unwrap())
        });
        pinned.blocking_recv().unwrap().map_err(io)?;
        {
            let dir = fs.fs_dir.read();
            let mut batch = dir.get_rocks_store().new_batch();
            policy.revision += 1;
            policy.selection.owner = Some("nobody".into());
            batch.hsm_put(&policy_key("cold")?, &pack(&policy)?)?;
            // All candidates disappear from the live projection after pinning.
            for id in 1001..1011 {
                batch.hsm_delete(&record_key(id))?;
            }
            batch.commit()?;
        }
        resume.send(()).unwrap();
        let page = work.join().unwrap().map_err(io)?;
        assert_eq!(page.rows.len(), 10);
        assert_eq!(page.records.len(), 5);
        assert!(page.records.values().all(|r| r.owner == "selected"));
        let lease = fs.fs_dir.read().store.snapshot_read_lease();
        // The second snapshot sees the modified policy, proving the first
        // policy bytes were pinned rather than read from live storage.
        let (ready, _) = oneshot::channel();
        let newer = scan_snapshot(
            lease,
            &serde_json::json!({"id":"cold","after":"\u{3}hsm:candidate:o:"}),
            ready,
        )
        .map_err(io)?;
        assert!(page.policy_bytes != newer.policy_bytes);
        let namespace = Namespace {
            ordinary_format: 1,
            root_inode: 1,
            storage: "s".into(),
            incarnation: "n".into(),
            epoch: 1,
        };
        let context = OperationContext {
            operation_id: "scan-old".into(),
            controller_epoch: 1,
            attempt_epoch: 1,
        };
        let rt = conf.client_rpc_conf().create_runtime();
        let result = rt.block_on(fs.hsm_lifecycle_with_scan(&namespace, &context, &serde_json::json!({"op":"policy_scan","id":"cold","limit":256,"after":"\u{3}hsm:candidate:o:"}), Some(page)));
        assert!(matches!(result, Err(HsmError::Conflict(_))));
        Ok(())
    }
}
