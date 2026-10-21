// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Native retry sessions. Acknowledgement retires a sequence permanently;
//! time retirement fences an entire session before range GC removes results.
//! Maintenance and the original result join the caller's metadata WriteBatch.
use crate::master::meta::store::{InodeWriteBatch, RocksInodeStore};
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};

pub(super) const RETENTION_MS: i64 = 60 * 60 * 1000;
const FUTURE_MS: i64 = 5 * 60 * 1000;
const WINDOW: u64 = 1024;
const MAX_BYTES: u64 = 32 * 1024 * 1024;
const FLOOR: &[u8] = b"\x03hsm:native-session-floor:v1";
const SESSION: &[u8] = b"\x03hsm:native-session:";
const RESULT: &[u8] = b"\x03hsm:native-result:";
const TIME: &[u8] = b"\x03hsm:native-session-time:";

fn issued(session: &str) -> CommonResult<i64> {
    if session.len() != 48 || !session.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return err_box!("invalid timed native session");
    }
    Ok(i64::try_from(u64::from_str_radix(&session[..16], 16)?)?)
}
fn session_key(session: &str) -> Vec<u8> {
    [SESSION, session.as_bytes()].concat()
}
fn result_prefix(session: &str) -> Vec<u8> {
    [RESULT, session.as_bytes(), b":"].concat()
}
fn result_key(session: &str, sequence: u64) -> Vec<u8> {
    [
        result_prefix(session).as_slice(),
        format!("{sequence:020}").as_bytes(),
    ]
    .concat()
}
fn time_key(session: &str, stamp: i64) -> Vec<u8> {
    [TIME, format!("{stamp:020}:{session}").as_bytes()].concat()
}

#[derive(Default, Serialize, Deserialize)]
struct Metadata {
    acknowledged: u64,
    highest: u64,
    retained_bytes: u64,
}

pub(super) enum Decision {
    Admit,
    Cached(Vec<u8>),
    Fenced,
    Full,
}

pub(super) struct Session {
    id: String,
    sequence: u64,
    metadata: Option<Metadata>,
    floor: Option<i64>,
    deleted: Vec<Vec<u8>>,
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
    index: bool,
    result: Option<Vec<u8>>,
    pub(super) decision: Decision,
}
impl Session {
    pub(super) fn prepare(
        store: &RocksInodeStore,
        id: &str,
        sequence: u64,
        acknowledged: u64,
        now: i64,
    ) -> CommonResult<Self> {
        let stamp = issued(id)?;
        if sequence == 0 || acknowledged >= sequence || now < 0 {
            return err_box!("invalid native sequence or acknowledgement");
        }
        let previous = store
            .hsm_get(FLOOR)?
            .map(|bytes| SerdeUtils::deserialize::<i64>(&bytes))
            .transpose()?
            .unwrap_or(0);
        let floor = previous.max(now.saturating_sub(RETENTION_MS).max(0) / 60_000 * 60_000);
        let mut call = Self {
            id: id.into(),
            sequence,
            metadata: None,
            floor: (floor != previous).then_some(floor),
            deleted: vec![],
            ranges: vec![],
            index: false,
            result: None,
            decision: Decision::Fenced,
        };
        // Whole-session retirement uses range tombstones. No result is
        // interpreted as a fresh request after its session's time was retired.
        for (key, value) in store.hsm_scan(TIME, None, 4)? {
            let session = std::str::from_utf8(&value)?;
            let time = issued(session)?;
            if key != time_key(session, time) {
                return err_box!("invalid native session time index");
            }
            if time >= floor {
                break;
            }
            let begin = result_prefix(session);
            let mut end = begin.clone();
            *end.last_mut().unwrap() = b';';
            call.ranges.push((begin, end));
            call.deleted.push(session_key(session));
            call.deleted.push(key);
        }
        if stamp < previous.max(now.saturating_sub(RETENTION_MS)) {
            return Ok(call);
        }
        let metadata = store.hsm_get(&session_key(id))?;
        if metadata.is_none() && stamp > now.saturating_add(FUTURE_MS) {
            return err_box!("native session clock is too far ahead");
        }
        call.index = metadata.is_none();
        let mut metadata = metadata
            .map(|bytes| SerdeUtils::deserialize::<Metadata>(&bytes))
            .transpose()?
            .unwrap_or_default();
        metadata.acknowledged = metadata.acknowledged.max(acknowledged);
        for (key, result) in store.hsm_scan(&result_prefix(id), None, 8)? {
            let seq = std::str::from_utf8(&key[result_prefix(id).len()..])?.parse::<u64>()?;
            if seq > metadata.acknowledged {
                break;
            }
            metadata.retained_bytes = metadata
                .retained_bytes
                .checked_sub(result.len() as u64)
                .ok_or("native result byte accounting underflow")?;
            call.deleted.push(key);
        }
        call.decision = if sequence <= metadata.acknowledged {
            Decision::Fenced
        } else if let Some(bytes) = store.hsm_get(&result_key(id, sequence))? {
            Decision::Cached(bytes)
        } else if sequence - metadata.acknowledged > WINDOW {
            Decision::Full
        } else {
            Decision::Admit
        };
        call.metadata = Some(metadata);
        Ok(call)
    }

    /// False is an admission limit, not a proof that some earlier copy never
    /// committed. The caller must retain this identity and report uncertainty.
    pub(super) fn record(&mut self, bytes: Vec<u8>) -> CommonResult<bool> {
        if !matches!(self.decision, Decision::Admit) || self.result.is_some() {
            return err_box!("native session result is not admissible");
        }
        let metadata = self
            .metadata
            .as_mut()
            .ok_or("native session metadata missing")?;
        let size = metadata
            .retained_bytes
            .checked_add(bytes.len() as u64)
            .ok_or("native session byte accounting overflow")?;
        if size > MAX_BYTES {
            self.decision = Decision::Full;
            return Ok(false);
        }
        metadata.retained_bytes = size;
        metadata.highest = metadata.highest.max(self.sequence);
        self.result = Some(bytes);
        Ok(true)
    }

    pub(super) fn write(&self, batch: &mut InodeWriteBatch) -> CommonResult<()> {
        for (begin, end) in &self.ranges {
            batch.hsm_delete_range(begin, end)?;
        }
        for key in &self.deleted {
            batch.hsm_delete(key)?;
        }
        if let Some(floor) = self.floor {
            batch.hsm_put(FLOOR, &SerdeUtils::serialize(&floor)?)?;
        }
        if let Some(metadata) = &self.metadata {
            batch.hsm_put(&session_key(&self.id), &SerdeUtils::serialize(metadata)?)?;
            if self.index {
                batch.hsm_put(&time_key(&self.id, issued(&self.id)?), self.id.as_bytes())?;
            }
        }
        if let Some(bytes) = &self.result {
            batch.hsm_put(&result_key(&self.id, self.sequence), bytes)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::{journal::JournalSystem, Master};
    use curvine_config::ClusterConf;

    #[test]
    fn native_sessions_preserve_original_results_retire_acks_and_fence_time_reuse(
    ) -> CommonResult<()> {
        Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let dir = fs.fs_dir.read();
        let store = dir.get_rocks_store();
        let id = format!("{:016x}{}", 1000, "a".repeat(32));
        for seq in 1..=20 {
            let mut call = Session::prepare(store, &id, seq, 0, 1000)?;
            assert!(matches!(call.decision, Decision::Admit));
            assert!(call.record(vec![seq as u8; 100])?);
            let mut batch = store.new_batch();
            call.write(&mut batch)?;
            batch.commit()?;
        }
        let original = Session::prepare(store, &id, 1, 0, 2000)?;
        assert!(matches!(original.decision, Decision::Cached(ref bytes) if bytes == &vec![1; 100]));
        let call = Session::prepare(store, &id, 21, 20, 2000)?;
        assert!(matches!(call.decision, Decision::Admit));
        let mut batch = store.new_batch();
        call.write(&mut batch)?;
        batch.commit()?;
        assert_eq!(store.hsm_scan(&result_prefix(&id), None, 100)?.len(), 12);
        assert!(matches!(
            Session::prepare(store, &id, 1, 0, 1000)?.decision,
            Decision::Fenced
        ));
        // A future sequence outside the window cannot consume namespace state.
        assert!(matches!(
            Session::prepare(store, &id, WINDOW + 22, 20, 2000)?.decision,
            Decision::Full
        ));
        let now = RETENTION_MS + 61_000;
        let new_id = format!("{now:016x}{}", "b".repeat(32));
        let call = Session::prepare(store, &new_id, 1, 0, now)?;
        let mut batch = store.new_batch();
        call.write(&mut batch)?;
        batch.commit()?;
        assert!(store.hsm_scan(&result_prefix(&id), None, 100)?.is_empty());
        assert!(matches!(
            Session::prepare(store, &id, 1, 0, 1000)?.decision,
            Decision::Fenced
        ));
        Ok(())
    }
}
