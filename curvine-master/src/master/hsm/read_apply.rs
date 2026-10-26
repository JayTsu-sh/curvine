// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! A bundle of native online read deltas has one durable publication point.
//! Stable/lifecycle mutations retain their general apply path; this module
//! accepts only the explicitly side-effect-free read-delta shape.
use super::*;
use std::collections::{BTreeMap, HashMap, HashSet};

pub(super) fn merge(mut current: Record, access: &ReadState) -> Option<Record> {
    if access.data != current.state.data
        || access.base_version != current.access_base.unwrap_or(current.version)
        || current.version.checked_add(1) != Some(access.version)
        || access.revision < current.state.revision
        || current
            .state
            .revision
            .checked_add(1)
            .is_none_or(|max| access.revision > max)
        || access.atime_ms < current.atime_ms
    {
        return None;
    }
    current.access_base = Some(access.base_version);
    current.version = access.version;
    current.state.revision = access.revision;
    current.atime_ms = access.atime_ms;
    current.readers = access.readers.clone();
    Some(current)
}

fn eligible(tx: &Transaction) -> bool {
    tx.read_state.is_some()
        && tx.inode.is_some()
        && tx.expected_version.is_some()
        && tx.expected_epoch.is_some()
        && !tx.scan_progress_only
        && !tx.create
        && tx.record.is_none()
        && tx.namespace.is_none()
        && tx.controls.is_empty()
        && tx.record_checks.is_empty()
        && tx.workflow.is_none()
        && tx.data.is_none()
        && tx.action.is_none()
}

pub(super) fn apply(fs: &FsDir, transactions: &[Transaction]) -> CommonResult<Option<Vec<Value>>> {
    if !transactions.iter().all(eligible) {
        return Ok(None);
    }
    let mut records = BTreeMap::new();
    for tx in transactions {
        let id = tx.inode.unwrap();
        if let std::collections::btree_map::Entry::Vacant(entry) = records.entry(id) {
            let record = fs.hsm_record(id)?;
            if record.as_ref().is_some_and(|record| {
                !record.ordinary || !record.native_blocks || record.published.is_some()
            }) {
                return Ok(None);
            }
            entry.insert(record);
        }
    }
    let epoch = fs.hsm_namespace()?.map(|namespace| namespace.epoch);
    let mut history = read_history::HistoryBatch::new(fs, transactions.len().saturating_mul(8))?;
    let old_clock = fs
        .get_rocks_store()
        .hsm_get(read_clock::FLOOR)?
        .map(|bytes| decode::<i64>(&bytes))
        .transpose()?
        .unwrap_or(0);
    let mut clock = old_clock;
    let mut outcomes: HashMap<String, Outcome> = HashMap::new();
    let mut changed = HashSet::new();
    let mut results = Vec::with_capacity(transactions.len());
    let mut batch = fs.get_rocks_store().new_batch();
    for tx in transactions {
        let cached = match outcomes.get(&tx.outcome) {
            Some(value) => Some(value.clone()),
            None => fs.hsm_outcome(&tx.outcome)?,
        };
        if let Some(cached) = cached {
            results.push(if cached.fingerprint == tx.fingerprint {
                cached.response
            } else {
                failure(
                    "conflict",
                    "operation step already committed with different input",
                )
            });
            continue;
        }
        let id = tx.inode.unwrap();
        let old = records.get(&id).and_then(Option::as_ref);
        let mut response = tx.response.clone();
        let mut valid = history.admissible(&tx.outcome, tx.time)?;
        if !valid {
            response = failure("fenced", "read request identity was retired");
        }
        if epoch != tx.expected_epoch {
            valid = false;
            response = failure("fenced", "controller qualification changed");
        }
        if old.map(|record| record.version) != tx.expected_version {
            valid = false;
            response = failure("conflict", "native record changed before apply");
        }
        if valid {
            let access = tx.read_state.as_ref().unwrap();
            if let Some(next) = old.cloned().and_then(|record| merge(record, access)) {
                ordinary::write_access(&mut batch, old.unwrap(), &next, access.clock_ms)?;
                clock = clock.max(access.clock_ms);
                changed.insert(id);
                records.insert(id, Some(next));
            } else {
                response = failure("conflict", "read state no longer matches its base record");
            }
        }
        let outcome = Outcome {
            fingerprint: tx.fingerprint.clone(),
            response: response.clone(),
            data: None,
            action: None,
        };
        let bytes = pack(&outcome)?;
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_record_write_bytes
                .with_label_values(&["outcome"])
                .inc_by(bytes.len() as i64);
        }
        batch.hsm_put(&outcome_key(&tx.outcome), &bytes)?;
        history.record(&mut batch, &tx.outcome, tx.time)?;
        outcomes.insert(tx.outcome.clone(), outcome);
        results.push(response);
    }
    for id in changed {
        // Native files are owned RocksDB values behind lightweight tree
        // entries. Delay their final atime write until the whole bundle is
        // ready, without changing directory pointers or speculative metadata.
        let mut inode = fs
            .store
            .get_inode(id, None)?
            .ok_or("ordinary inode missing")?;
        inode.as_file_mut()?.atime = records[&id].as_ref().unwrap().atime_ms;
        batch.write_inode(&inode)?;
    }
    if clock > old_clock {
        batch.hsm_put(read_clock::FLOOR, &pack(clock)?)?;
    }
    if !outcomes.is_empty() {
        batch.commit()?;
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_batch_size
                .with_label_values(&["read_apply"])
                .observe(transactions.len() as f64);
        }
    }
    Ok(Some(results))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use serde_json::json;

    fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem, Namespace, Vec<i64>)> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let mut ids = vec![];
        for path in ["/a", "/b"] {
            ids.push(fs.create(path, false)?.id);
            fs.complete_file(path, None, 0, vec![], "", false, None)?;
        }
        let namespace = Namespace {
            ordinary_format: 2,
            root_inode: 9999,
            storage: "s".into(),
            incarnation: "n".into(),
            epoch: 1,
        };
        {
            let mut dir = fs.fs_dir.write();
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(NS, &pack(&namespace)?)?;
            batch.commit()?;
            dir.hsm_enabled = true;
            for path in ["/a", "/b"] {
                let input = InodePath::resolve(dir.root_ptr(), path, &dir.store)?;
                dir.hsm_track_file(path, input.get_last_inode().unwrap().as_file_ref()?)?;
            }
        }
        Ok((root, fs, namespace, ids))
    }

    fn open(
        fs: &MasterFilesystem,
        ns: &Namespace,
        path: &str,
        reader: char,
        previous: Option<Record>,
    ) -> CommonResult<(Transaction, Record)> {
        let request = json!({"op":"read_path","path":path,"reader":reader.to_string().repeat(32)});
        let ordinary::OrdinaryPlan::Transaction(tx, projection, _) =
            fs.hsm_prepare_ordinary(ns, &request, previous)?
        else {
            panic!("expected protected read");
        };
        Ok((tx, projection.unwrap()))
    }

    fn apply(fs: &MasterFilesystem, transactions: &[Transaction]) -> CommonResult<Vec<Value>> {
        fs.fs_dir
            .write()
            .apply_hsm_results(&[READ_BATCH_MAGIC, pack(transactions)?.as_slice()].concat())
    }

    #[test]
    fn one_atomic_read_batch_merges_hot_inode_and_deduplicates_replay() -> CommonResult<()> {
        let (_root, fs, ns, ids) = fixture()?;
        let (first, projection) = open(&fs, &ns, "/a", 'a', None)?;
        let (second, _) = open(&fs, &ns, "/a", 'b', Some(projection))?;
        let (other, _) = open(&fs, &ns, "/b", 'c', None)?;
        let txs = vec![first.clone(), first, other, second];
        let before = fs
            .fs_dir
            .read()
            .get_rocks_store()
            .hsm_get(&record_key(ids[0]))?;
        let results = apply(&fs, &txs)?;
        assert!(results.iter().all(|value| value["kind"] == "read"));
        assert_eq!(results[0], results[1]);
        let original = fs.fs_dir.read().hsm_record(ids[0])?.unwrap();
        assert_eq!(original.readers.len(), 2);
        assert_eq!(
            fs.fs_dir.read().hsm_record(ids[1])?.unwrap().readers.len(),
            1
        );
        assert_eq!(
            fs.fs_dir
                .read()
                .get_rocks_store()
                .hsm_get(&record_key(ids[0]))?,
            before
        );
        fs.fs_dir.write().restore_from_rocksdb()?;
        assert_eq!(apply(&fs, &txs)?, results);
        let replayed = fs.fs_dir.read().hsm_record(ids[0])?.unwrap();
        assert_eq!(replayed.version, original.version);
        assert_eq!(replayed.readers, original.readers);
        Ok(())
    }

    #[test]
    fn failed_read_cas_does_not_publish_projected_readers_or_poison_other_inode() -> CommonResult<()>
    {
        let (_root, fs, ns, ids) = fixture()?;
        let (mut first, projection) = open(&fs, &ns, "/a", 'a', None)?;
        first.expected_version = Some(0);
        let (dependent, _) = open(&fs, &ns, "/a", 'b', Some(projection))?;
        let (other, _) = open(&fs, &ns, "/b", 'c', None)?;
        let results = apply(&fs, &[first, dependent, other])?;
        assert_eq!(results[0]["data"]["code"], "conflict");
        assert_eq!(results[1]["data"]["code"], "conflict");
        assert_eq!(results[2]["kind"], "read");
        assert!(fs
            .fs_dir
            .read()
            .hsm_record(ids[0])?
            .unwrap()
            .readers
            .is_empty());
        assert_eq!(
            fs.fs_dir.read().hsm_record(ids[1])?.unwrap().readers.len(),
            1
        );
        Ok(())
    }

    #[test]
    fn read_batch_prepublication_failure_leaves_all_durable_records_unchanged() -> CommonResult<()>
    {
        let (_root, fs, ns, ids) = fixture()?;
        let (first, _) = open(&fs, &ns, "/a", 'a', None)?;
        let (mut broken, mut projection) = open(&fs, &ns, "/b", 'b', None)?;
        let missing = 9998;
        {
            let dir = fs.fs_dir.read();
            let mut original = dir.hsm_record(ids[1])?.unwrap();
            original.state.data.entity.entity = missing.to_string();
            projection.state.data.entity.entity = missing.to_string();
            let mut batch = dir.get_rocks_store().new_batch();
            ordinary::write_record(&mut batch, None, &original)?;
            batch.commit()?;
        }
        broken.inode = Some(missing);
        broken.read_state.as_mut().unwrap().data = projection.state.data;
        assert!(apply(&fs, &[first.clone(), broken.clone()]).is_err());
        let dir = fs.fs_dir.read();
        assert!(dir.hsm_record(ids[0])?.unwrap().readers.is_empty());
        assert!(dir.hsm_record(missing)?.unwrap().readers.is_empty());
        assert!(dir.hsm_outcome(&first.outcome)?.is_none());
        assert!(dir.hsm_outcome(&broken.outcome)?.is_none());
        Ok(())
    }
}
