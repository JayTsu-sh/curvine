// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Bounded retry history for ordinary read commands. The issue timestamp is
//! part of the immutable request identity; a durable, monotonic floor rejects
//! retired identities even after their result rows have been reclaimed.
use super::*;
use crate::master::meta::store::InodeWriteBatch;

pub(super) const RETENTION_MS: i64 = 60 * 60 * 1000;
const FUTURE_SKEW_MS: i64 = 5 * 60 * 1000;
const FLOOR: &[u8] = b"\x03hsm:read-history-floor:v1";
const INDEX: &[u8] = b"\x03hsm:read-history:";
const OUTCOME_PREFIX: &str = "read-request-";

fn timestamp(id: &str) -> Option<i64> {
    if id.len() != 48 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    i64::try_from(u64::from_str_radix(&id[..16], 16).ok()?).ok()
}

fn floor(fs: &FsDir) -> CommonResult<i64> {
    Ok(fs
        .get_rocks_store()
        .hsm_get(FLOOR)?
        .map(|value| decode(&value))
        .transpose()?
        .unwrap_or(0))
}

fn issued(outcome: &str, now: i64) -> CommonResult<Option<i64>> {
    if let Some(id) = outcome.strip_prefix(OUTCOME_PREFIX) {
        return Ok(Some(
            timestamp(id).ok_or("invalid committed read request identity")?,
        ));
    }
    Ok([
        "ordinary-read_path-",
        "ordinary-read_close-",
        "ordinary-read_renew-",
    ]
    .iter()
    .any(|prefix| outcome.starts_with(prefix))
    .then_some(now))
}

pub(super) fn identity(fs: &FsDir, request: &Value, now: i64) -> HsmResult<Option<String>> {
    let Some(id) = request.get("request_id") else {
        return Ok(None);
    };
    let id = id
        .as_str()
        .ok_or_else(|| HsmError::Invalid("invalid read request identity".into()))?;
    let issued = timestamp(id)
        .ok_or_else(|| HsmError::Invalid("invalid timed read request identity".into()))?;
    if issued < floor(fs).map_err(io)?.max(now.saturating_sub(RETENTION_MS)) {
        return Err(HsmError::Fenced);
    }
    if issued > now.saturating_add(FUTURE_SKEW_MS) {
        return Err(HsmError::Invalid(
            "read request clock is too far ahead".into(),
        ));
    }
    Ok(Some(format!("{OUTCOME_PREFIX}{id}")))
}

pub(super) fn admissible(fs: &FsDir, outcome: &str, now: i64) -> CommonResult<bool> {
    if !outcome.starts_with(OUTCOME_PREFIX) {
        return Ok(true);
    }
    admissible_at(floor(fs)?, outcome, now)
}

fn admissible_at(floor: i64, outcome: &str, now: i64) -> CommonResult<bool> {
    let Some(id) = outcome.strip_prefix(OUTCOME_PREFIX) else {
        return Ok(true);
    };
    let issued = timestamp(id).ok_or("invalid committed read request identity")?;
    Ok(issued >= floor.max(now.saturating_sub(RETENTION_MS)))
}

/// One committed write batch observes its own advancing retirement floor and
/// shares a bounded GC page. Wall-clock rollback inside a bundle cannot lower
/// the floor. A bundle scans up to eight rows per command once, rather than
/// repeatedly visiting the same uncommitted deletions.
pub(super) struct HistoryBatch {
    floor: i64,
    garbage: std::collections::VecDeque<(Vec<u8>, Vec<u8>, i64)>,
}
impl HistoryBatch {
    pub(super) fn new(fs: &FsDir, budget: usize) -> CommonResult<Self> {
        let mut garbage = std::collections::VecDeque::new();
        for (key, result) in fs
            .get_rocks_store()
            .hsm_scan(INDEX, None, budget.min(256))?
        {
            let stamp = std::str::from_utf8(
                key.get(INDEX.len()..INDEX.len() + 20)
                    .ok_or("invalid read history index")?,
            )?
            .parse::<i64>()?;
            if !result.starts_with(b"\x03hsm:outcome:") {
                return err_box!("invalid read history outcome reference");
            }
            garbage.push_back((key, result, stamp));
        }
        Ok(Self {
            floor: floor(fs)?,
            garbage,
        })
    }
    pub(super) fn admissible(&self, outcome: &str, now: i64) -> CommonResult<bool> {
        admissible_at(self.floor, outcome, now)
    }
    pub(super) fn record(
        &mut self,
        batch: &mut InodeWriteBatch,
        outcome: &str,
        now: i64,
    ) -> CommonResult<()> {
        let Some(issued) = issued(outcome, now)? else {
            return Ok(());
        };
        let next = self
            .floor
            .max(now.saturating_sub(RETENTION_MS).max(0) / 60_000 * 60_000);
        if next != self.floor {
            batch.hsm_put(FLOOR, &pack(next)?)?;
            self.floor = next;
        }
        let result = outcome_key(outcome);
        let key = [
            INDEX,
            format!("{:020}:{}", issued.max(0), blake3::hash(&result).to_hex()).as_bytes(),
        ]
        .concat();
        batch.hsm_put(&key, &result)?;
        while self
            .garbage
            .front()
            .is_some_and(|(_, _, stamp)| *stamp < next)
        {
            let (key, result, _) = self.garbage.pop_front().unwrap();
            batch.hsm_delete(&result)?;
            batch.hsm_delete(&key)?;
        }
        Ok(())
    }
}

/// Runs inside the same RocksDB batch as the command result. GC touches at
/// most eight expired result/index pairs and never visits workflow outcomes.
pub(super) fn record(
    fs: &FsDir,
    batch: &mut InodeWriteBatch,
    outcome: &str,
    now: i64,
) -> CommonResult<()> {
    if issued(outcome, now)?.is_none() {
        return Ok(());
    }
    HistoryBatch::new(fs, 8)?.record(batch, outcome, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_history_keeps_monotonic_floor_and_reclaims_one_shared_gc_page() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let dir = fs.fs_dir.read();
        let mut batch = dir.get_rocks_store().new_batch();
        let mut old = Vec::new();
        for index in 0..20 {
            let outcome = format!("{OUTCOME_PREFIX}{:016x}{index:032x}", 1000);
            record(&dir, &mut batch, &outcome, 1000)?;
            batch.hsm_put(&outcome_key(&outcome), b"old-result")?;
            old.push(outcome);
        }
        batch.commit()?;
        let mut history = HistoryBatch::new(&dir, 16)?;
        let mut batch = dir.get_rocks_store().new_batch();
        let now = RETENTION_MS + 61_000;
        let outcome = format!("{OUTCOME_PREFIX}{now:016x}{:032x}", 100);
        history.record(&mut batch, &outcome, now)?;
        assert!(!history.admissible(&old[0], 1000)?);
        // A later command in this same bundle has a rolled-back wall clock.
        // It cannot overwrite the pending retirement floor with the old DB value.
        let second = format!("{OUTCOME_PREFIX}{now:016x}{:032x}", 101);
        history.record(&mut batch, &second, 1000)?;
        batch.commit()?;
        assert_eq!(floor(&dir)?, 60_000);
        let remaining = old
            .iter()
            .map(|outcome| dir.get_rocks_store().hsm_get(&outcome_key(outcome)))
            .collect::<CommonResult<Vec<_>>>()?
            .iter()
            .filter(|value| value.is_some())
            .count();
        assert_eq!(remaining, 4, "GC must consume its shared budget once");
        Ok(())
    }

    #[test]
    fn read_results_retire_with_durable_floor_and_preserve_workflow_success() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let mut dir = fs.fs_dir.write();
        dir.hsm_enabled = true;
        let make = |name: String, time| Transaction {
            scan_progress_only: false,
            read_state: None,
            controls: vec![],
            record_checks: vec![],
            workflow: None,
            outcome: name,
            fingerprint: "original".into(),
            response: reply("done", Value::Null),
            inode: None,
            expected_version: None,
            record: None,
            namespace: None,
            expected_epoch: None,
            create: false,
            time,
            data: None,
            action: None,
        };
        let old_id = format!("{:016x}{}", 1000, "a".repeat(32));
        let old = make(format!("{OUTCOME_PREFIX}{old_id}"), 1000);
        dir.apply_hsm(&pack(&old)?)?;
        dir.apply_hsm(&pack(&old)?)?;
        assert_eq!(dir.get_rocks_store().hsm_scan(INDEX, None, 16)?.len(), 1);
        let workflow = make("workflow-original-success".into(), 1000);
        dir.apply_hsm(&pack(&workflow)?)?;
        let next_time = RETENTION_MS + 61_000;
        let new_id = format!("{:016x}{}", next_time, "b".repeat(32));
        dir.apply_hsm(&pack(make(format!("{OUTCOME_PREFIX}{new_id}"), next_time))?)?;
        assert!(dir.hsm_outcome(&old.outcome)?.is_none());
        assert!(dir.hsm_outcome(&workflow.outcome)?.is_some());
        assert!(matches!(
            identity(&dir, &serde_json::json!({"request_id":old_id}), 1000),
            Err(HsmError::Fenced)
        ));
        // An already serialized old request cannot regain authority after its
        // result has been reclaimed, even if the local clock moves backwards.
        dir.apply_hsm(&pack(&old)?)?;
        assert_eq!(
            dir.hsm_outcome(&old.outcome)?.unwrap().response["data"]["code"],
            "fenced"
        );
        drop(dir);
        drop(fs);
        conf.format_master = false;
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let dir = fs.fs_dir.read();
        assert!(matches!(
            identity(&dir, &serde_json::json!({"request_id":old_id}), 1000),
            Err(HsmError::Fenced)
        ));
        assert!(dir.hsm_outcome(&workflow.outcome)?.is_some());
        Ok(())
    }
}
