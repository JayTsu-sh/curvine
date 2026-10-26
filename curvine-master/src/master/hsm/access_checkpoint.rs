// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Aggregate access persistence has its own journal envelope: legacy read
//! commands still advance one event at a time and retain their original replay
//! semantics. Only access observations are writable here, never generations or
//! publication state. The caller owns grant protection and serializes captures.
use super::*;
use serde::Deserialize;

pub(super) const MAGIC: &[u8] = b"hsm-access-checkpoint-v1\n";
pub(super) const ACTIVATE_MAGIC: &[u8] = b"hsm-fast-read-activation-v1\n";
const MODE: &[u8] = b"\x03hsm:fast-read-mode:v1";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Activation {
    pub(super) storage: String,
    pub(super) incarnation: String,
}
pub(super) fn activated(fs: &FsDir, ns: &Namespace) -> CommonResult<bool> {
    Ok(fs.get_rocks_store().hsm_get(MODE)?
        == Some(pack(Activation {
            storage: ns.storage.clone(),
            incarnation: ns.incarnation.clone(),
        })?))
}
pub(super) fn activate(fs: &FsDir, bytes: &[u8]) -> CommonResult<Value> {
    let activation: Activation = decode(bytes)?;
    let Some(ns) = fs.hsm_namespace()? else {
        return Ok(failure("fenced", "missing fast-read namespace"));
    };
    if activation.storage != ns.storage || activation.incarnation != ns.incarnation {
        return Ok(failure("fenced", "fast-read namespace changed"));
    }
    if !activated(fs, &ns)? {
        let mut batch = fs.get_rocks_store().new_batch();
        batch.hsm_put(MODE, bytes)?;
        batch.commit()?;
    }
    Ok(reply("done", Value::Null))
}
pub(super) const FRONTIER: &[u8] = b"\x03hsm:access-checkpoint:v1";
pub(super) const MAX_ROWS: usize = 2048;
const MAX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Stamp {
    pub(super) producer: String,
    pub(super) sequence: u64,
    pub(super) hash: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkpoint {
    pub(super) incarnation: String,
    pub(super) epoch: u64,
    pub(super) expected_frontier: Option<Stamp>,
    pub(super) producer: String,
    pub(super) sequence: u64,
    pub(super) rows: Vec<Row>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Row {
    pub(super) inode: i64,
    pub(super) expected_version: u64,
    pub(super) access: ReadState,
}

pub(super) fn frontier(fs: &FsDir) -> CommonResult<Option<Stamp>> {
    fs.get_rocks_store()
        .hsm_get(FRONTIER)?
        .map(|bytes| decode(&bytes))
        .transpose()
}

#[derive(Deserialize)]
struct RecordHead {
    #[serde(default)]
    ordinary: bool,
    version: u64,
    #[serde(default)]
    atime_ms: i64,
    state: StateHead,
}
#[derive(Deserialize)]
struct StateHead {
    data: DataKey,
    revision: u64,
    residency: Residency,
}
#[derive(Deserialize)]
struct AccessHead {
    data: DataKey,
    base_version: u64,
    version: u64,
    revision: u64,
    atime_ms: i64,
}
struct Current {
    ordinary: bool,
    base: u64,
    version: u64,
    revision: u64,
    atime: i64,
    data: DataKey,
    residency: Residency,
}
struct AccessFields<'a> {
    data: &'a DataKey,
    base: u64,
    version: u64,
    revision: u64,
    atime: i64,
}
impl<'a> From<&'a AccessHead> for AccessFields<'a> {
    fn from(value: &'a AccessHead) -> Self {
        Self {
            data: &value.data,
            base: value.base_version,
            version: value.version,
            revision: value.revision,
            atime: value.atime_ms,
        }
    }
}
impl<'a> From<&'a ReadState> for AccessFields<'a> {
    fn from(value: &'a ReadState) -> Self {
        Self {
            data: &value.data,
            base: value.base_version,
            version: value.version,
            revision: value.revision,
            atime: value.atime_ms,
        }
    }
}
fn merge_head(mut current: Current, access: Option<AccessFields<'_>>) -> CommonResult<Current> {
    if let Some(access) = access {
        if access.base != current.base
            || access.data != &current.data
            || access.version <= current.base
        {
            return err_box!("HSM access state does not match its base generation/version");
        }
        current.version = access.version;
        current.revision = access.revision;
        current.atime = access.atime;
    }
    Ok(current)
}
fn read_head(
    id: i64,
    mut get: impl FnMut(&[u8]) -> CommonResult<Option<Vec<u8>>>,
) -> CommonResult<Option<Current>> {
    let Some(bytes) = get(&record_key(id))? else {
        return Ok(None);
    };
    let base: RecordHead = decode(&bytes)?;
    let access: Option<AccessHead> = get(&access_key(id))?
        .map(|bytes| decode(&bytes))
        .transpose()?;
    merge_head(
        Current {
            ordinary: base.ordinary,
            base: base.version,
            version: base.version,
            revision: base.state.revision,
            atime: base.atime_ms,
            data: base.state.data,
            residency: base.state.residency,
        },
        access.as_ref().map(AccessFields::from),
    )
    .map(Some)
}
fn cached_head(
    store: &crate::master::meta::store::RocksInodeStore,
    id: i64,
) -> Option<CommonResult<Current>> {
    store.with_cached_hsm_parts(id, |base, access| {
        merge_head(
            Current {
                ordinary: base.ordinary,
                base: base.version,
                version: base.version,
                revision: base.state.revision,
                atime: base.atime_ms,
                data: base.state.data.clone(),
                residency: base.state.residency,
            },
            access.map(AccessFields::from),
        )
    })
}
fn matches(current: &Current, row: &Row) -> bool {
    current.version == row.expected_version
        && row.access.data == current.data
        && row.access.base_version == current.base
        && row
            .access
            .version
            .checked_sub(current.version)
            .is_some_and(|versions| {
                versions > 0
                    && row
                        .access
                        .revision
                        .checked_sub(current.revision)
                        .is_some_and(|revisions| revisions <= versions)
            })
        && row.access.atime_ms >= current.atime
        && row.access.clock_ms >= 0
}

/// Called only from serialized committed apply under a namespace guard. It
/// changes RocksDB access projections, not namespace pointers or generations.
/// A stale aggregate
/// writes nothing, including its receipt, and can be recaptured after a barrier.
/// Exact retries must retain the original encoded bytes, not just its identity.
pub(super) fn apply(fs: &FsDir, bytes: &[u8]) -> CommonResult<Value> {
    let _serial = fs.get_rocks_store().lock_hsm_access_apply();
    let started = std::time::Instant::now();
    if bytes.len() > MAX_BYTES {
        return err_box!("HSM access checkpoint exceeds byte bound");
    }
    let checkpoint: Checkpoint = decode(bytes)?;
    if checkpoint.rows.is_empty()
        || checkpoint.rows.len() > MAX_ROWS
        || checkpoint.producer.is_empty()
        || checkpoint.producer.len() > 128
        || checkpoint.sequence == 0
        || checkpoint
            .rows
            .windows(2)
            .any(|rows| rows[0].inode >= rows[1].inode)
        || checkpoint.rows.iter().any(|row| {
            row.inode < ROOT_INODE_ID
                || row.access.data.entity.entity.parse::<i64>().ok() != Some(row.inode)
        })
    {
        return err_box!("invalid bounded HSM access checkpoint");
    }
    let stamp = Stamp {
        producer: checkpoint.producer.clone(),
        sequence: checkpoint.sequence,
        hash: *blake3::hash(bytes).as_bytes(),
    };
    let previous = frontier(fs)?;
    if let Some(previous) = &previous {
        if previous.producer == stamp.producer && previous.sequence == stamp.sequence {
            return Ok(if previous.hash == stamp.hash {
                reply("done", previous)
            } else {
                failure("conflict", "access checkpoint identity rebound")
            });
        }
    }
    if previous != checkpoint.expected_frontier
        || previous.as_ref().is_some_and(|previous| {
            previous.producer == stamp.producer && previous.sequence >= stamp.sequence
        })
    {
        return Ok(failure("busy", "access checkpoint frontier changed"));
    }
    let Some(namespace) = fs.hsm_namespace()? else {
        return Ok(failure("fenced", "access checkpoint namespace is missing"));
    };
    if namespace.incarnation != checkpoint.incarnation || namespace.epoch != checkpoint.epoch {
        return Ok(failure(
            "fenced",
            "access checkpoint namespace qualification changed",
        ));
    }

    // Preflight every row before staging any writes. Inodes are owned RocksDB
    // values, so failure cannot leak a speculative atime into the live tree.
    let mut prepared = Vec::with_capacity(checkpoint.rows.len());
    let mut clock = fs
        .get_rocks_store()
        .hsm_get(read_clock::FLOOR)?
        .map(|bytes| decode::<i64>(&bytes))
        .transpose()?
        .unwrap_or(0);
    let old_clock = clock;
    // One iterator per CF retains RocksDB's range tombstone semantics without
    // rebuilding each memtable's range iterator for every point lookup.
    let store = fs.get_rocks_store();
    let mut options = curvine_rocksdb::ReadOptions::default();
    options.set_total_order_seek(true);
    let mut common = store.db.get_db().raw_iterator_cf_opt(
        store
            .db
            .cf(crate::master::meta::store::RocksInodeStore::CF_COMMON)?,
        options,
    );
    let mut options = curvine_rocksdb::ReadOptions::default();
    options.set_total_order_seek(true);
    let mut inodes = store.db.get_db().raw_iterator_cf_opt(
        store
            .db
            .cf(crate::master::meta::store::RocksInodeStore::CF_INODES)?,
        options,
    );
    for row in &checkpoint.rows {
        let old = if let Some(cached) = cached_head(store, row.inode) {
            Some(cached?)
        } else {
            read_head(row.inode, |key| {
                common.seek(key);
                common.status()?;
                Ok(common
                    .key()
                    .filter(|actual| *actual == key)
                    .and_then(|_| common.value())
                    .map(|value| value.to_vec()))
            })?
        };
        let Some(old) = old else {
            return Ok(failure("busy", "access checkpoint record disappeared"));
        };
        if !matches(&old, row) {
            return Ok(failure("busy", "access checkpoint record changed"));
        }
        let inode = if old.ordinary {
            let inode = if let Some(inode) = store.cached_file_inode(row.inode) {
                Some(inode)
            } else {
                let key = curvine_rocksdb::RocksUtils::i64_to_bytes(row.inode);
                inodes.seek(key);
                inodes.status()?;
                inodes
                    .key()
                    .filter(|actual| *actual == key.as_slice())
                    .and_then(|_| inodes.value())
                    .map(curvine_runtime::common::SerdeUtils::deserialize::<InodeView>)
                    .transpose()?
            };
            let Some(mut inode) = inode else {
                return Ok(failure("busy", "access checkpoint inode disappeared"));
            };
            inode.as_file_mut()?.atime = row.access.atime_ms;
            Some(inode)
        } else {
            None
        };
        clock = clock.max(row.access.clock_ms);
        let previous =
            ordinary::access_index_key(&old.residency, old.atime, &old.data.entity.entity);
        let next = ordinary::access_index_key(
            &old.residency,
            row.access.atime_ms,
            &old.data.entity.entity,
        );
        prepared.push((row, previous, next, inode));
    }

    drop(common);
    drop(inodes);
    if let Ok(metrics) = crate::master::Master::get_metrics() {
        metrics
            .hsm_stage_duration
            .with_label_values(&["access_checkpoint", "apply_preflight"])
            .observe(started.elapsed().as_micros() as f64);
    }
    let writing = std::time::Instant::now();
    let mut batch = fs.get_rocks_store().new_batch();
    for (row, previous, next, inode) in prepared {
        let bytes_len = batch.hsm_put_access(row.inode, &row.access)?;
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_record_write_bytes
                .with_label_values(&["access"])
                .inc_by(bytes_len as i64);
        }
        if previous != next {
            batch.hsm_delete(&previous)?;
            batch.hsm_put(&next, &pack(&row.access.data.entity.entity)?)?;
        }
        if let Some(inode) = inode {
            batch.write_inode(&inode)?;
        }
    }
    if clock > old_clock {
        batch.hsm_put(read_clock::FLOOR, &pack(clock)?)?;
    }
    batch.hsm_put(FRONTIER, &pack(&stamp)?)?;
    batch.commit()?;
    if let Ok(metrics) = crate::master::Master::get_metrics() {
        metrics
            .hsm_stage_duration
            .with_label_values(&["access_checkpoint", "apply_write"])
            .observe(writing.elapsed().as_micros() as f64);
    }
    Ok(reply("done", stamp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;

    fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem, Vec<i64>)> {
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
        {
            let mut dir = fs.fs_dir.write();
            let namespace = Namespace {
                ordinary_format: 2,
                root_inode: 9999,
                storage: "s".into(),
                incarnation: "n".into(),
                epoch: 1,
            };
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(NS, &pack(&namespace)?)?;
            batch.commit()?;
            dir.hsm_enabled = true;
            for path in ["/a", "/b"] {
                let input = InodePath::resolve(dir.root_ptr(), path, &dir.store)?;
                dir.hsm_track_file(path, input.get_last_inode().unwrap().as_file_ref()?)?;
            }
        }
        Ok((root, fs, ids))
    }

    fn capture(fs: &FsDir, ids: &[i64], sequence: u64) -> CommonResult<Checkpoint> {
        let mut rows = Vec::new();
        for id in ids {
            let record = fs.hsm_record(*id)?.unwrap();
            rows.push(Row {
                inode: *id,
                expected_version: record.version,
                access: ReadState {
                    clock_ms: 1000,
                    data: record.state.data.clone(),
                    base_version: record.access_base.unwrap_or(record.version),
                    version: record.version + 3,
                    revision: record.state.revision + 2,
                    atime_ms: record.atime_ms + 100,
                    readers: record.readers.clone(),
                },
            });
        }
        rows.sort_by_key(|row| row.inode);
        Ok(Checkpoint {
            incarnation: "n".into(),
            epoch: 1,
            expected_frontier: frontier(fs)?,
            producer: "test-producer".into(),
            sequence,
            rows,
        })
    }

    fn sequence(fs: &FsDir) -> u64 {
        fs.get_rocks_store().db.get_db().latest_sequence_number()
    }

    #[test]
    fn negative_access_cache_does_not_hide_an_unexpected_persisted_overlay() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let stale_base: Record =
            decode(&dir.get_rocks_store().hsm_get(&record_key(ids[0]))?.unwrap())?;
        let checkpoint = capture(&dir, &ids, 1)?;
        assert_eq!(apply(&dir, &pack(&checkpoint)?)?["kind"], "done");
        let access = dir.get_rocks_store().hsm_get(&access_key(ids[0]))?;
        let mut next = stale_base.clone();
        next.version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        // A stale internal caller has not materialized the access state.
        // Its missing provenance must not turn a real overlay into "absent".
        ordinary::write_record(&mut batch, Some(&stale_base), &next)?;
        batch.commit()?;
        assert_eq!(dir.get_rocks_store().hsm_get(&access_key(ids[0]))?, access);
        assert!(cached_head(dir.get_rocks_store(), ids[0]).is_none());
        assert!(dir.hsm_record(ids[0]).is_err());
        assert!(cached_head(dir.get_rocks_store(), ids[0]).unwrap().is_err());
        Ok(())
    }

    #[test]
    fn stable_record_replacement_atomically_clears_and_caches_access_absence() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let checkpoint = capture(&dir, &ids, 1)?;
        assert_eq!(apply(&dir, &pack(&checkpoint)?)?["kind"], "done");
        let old = dir.hsm_record(ids[0])?.unwrap();
        assert!(old.access_base.is_some());
        let mut next = old.clone();
        next.version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        ordinary::write_record(&mut batch, Some(&old), &next)?;
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, old.version);
        batch.commit()?;
        assert!(dir
            .get_rocks_store()
            .hsm_get(&access_key(ids[0]))?
            .is_none());
        let cached = dir
            .get_rocks_store()
            .with_cached_hsm_parts(ids[0], |base, access| {
                (base.version, base.access_base, access.is_none())
            })
            .unwrap();
        assert_eq!(cached, (next.version, None, true));
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, next.version);
        Ok(())
    }

    #[test]
    fn first_checkpoint_after_replicated_close_needs_no_leader_cache_warmup() -> CommonResult<()> {
        use curvine_rocksdb::perf::{set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        // A Follower applies the same writes but has never served hsm_record.
        // Obtain this test input from raw DB bytes, preserving that cold state.
        let mut checkpoint = Checkpoint {
            incarnation: "n".into(),
            epoch: 1,
            expected_frontier: None,
            producer: "follower-test".into(),
            sequence: 1,
            rows: vec![],
        };
        for id in ids {
            let record: Record = decode(&dir.get_rocks_store().hsm_get(&record_key(id))?.unwrap())?;
            checkpoint.rows.push(Row {
                inode: id,
                expected_version: record.version,
                access: ReadState {
                    clock_ms: 1000,
                    data: record.state.data,
                    base_version: record.version,
                    version: record.version + 1,
                    revision: record.state.revision + 1,
                    atime_ms: record.atime_ms + 100,
                    readers: record.readers,
                },
            });
        }
        checkpoint.rows.sort_by_key(|row| row.inode);
        set_perf_stats(PerfStatsLevel::EnableCount);
        let mut perf = PerfContext::default();
        perf.reset();
        let result = apply(&dir, &pack(&checkpoint)?)?;
        let seeks = perf.metric(PerfMetric::SeekOnMemtableCount);
        set_perf_stats(PerfStatsLevel::Disable);
        assert_eq!(result["kind"], "done");
        assert_eq!(
            seeks, 0,
            "Follower must know committed access absence after close"
        );
        Ok(())
    }

    #[test]
    fn cached_checkpoint_head_still_rejects_mismatched_access_versions() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let checkpoint = capture(&dir, &ids, 1)?;
        let mut access = checkpoint.rows[0].access.clone();
        access.base_version += 1;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put_access(ids[0], &access)?;
        batch.commit()?;
        let before = sequence(&dir);
        assert!(cached_head(dir.get_rocks_store(), ids[0]).unwrap().is_err());
        assert!(apply(&dir, &pack(&checkpoint)?).is_err());
        assert_eq!(sequence(&dir), before);
        Ok(())
    }

    #[test]
    fn warm_checkpoint_preflight_avoids_repeated_projection_seeks() -> CommonResult<()> {
        use curvine_rocksdb::perf::{set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let checkpoint = capture(&dir, &ids, 1)?;
        set_perf_stats(PerfStatsLevel::EnableCount);
        let mut perf = PerfContext::default();
        perf.reset();
        let result = apply(&dir, &pack(&checkpoint)?)?;
        let seeks = perf.metric(PerfMetric::SeekOnMemtableCount);
        set_perf_stats(PerfStatsLevel::Disable);
        assert_eq!(result["kind"], "done");
        assert_eq!(seeks, 0, "cached committed projections still seek per row");
        Ok(())
    }

    #[test]
    fn parsed_projection_cache_matches_raw_writes_errors_and_range_reset() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let original = dir.hsm_record(ids[0])?.unwrap();
        let mut changed = original.clone();
        changed.version += 1;
        changed.atime_ms += 100;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(&record_key(ids[0]), &pack(&changed)?)?;
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, original.version);
        batch.commit()?;
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, changed.version);
        let mut corrupt = dir.get_rocks_store().new_batch();
        corrupt.hsm_put(&record_key(ids[0]), b"invalid-json")?;
        corrupt.commit()?;
        assert!(dir.hsm_record(ids[0]).is_err());
        let mut restored = dir.get_rocks_store().new_batch();
        restored.hsm_put_record(ids[0], &original)?;
        restored.commit()?;
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, original.version);
        let mut removed = dir.get_rocks_store().new_batch();
        removed.hsm_delete_range(b"\x03hsm:record:", b"\x03hsm:record;")?;
        removed.commit()?;
        assert!(dir.hsm_record(ids[0])?.is_none());
        let mut put_then_delete = dir.get_rocks_store().new_batch();
        put_then_delete.hsm_put_record(ids[0], &original)?;
        put_then_delete.hsm_delete_range(b"\x03hsm:record:", b"\x03hsm:record;")?;
        put_then_delete.commit()?;
        assert!(dir.hsm_record(ids[0])?.is_none());
        let mut delete_then_put = dir.get_rocks_store().new_batch();
        delete_then_put.hsm_delete_range(b"\x03hsm:record:", b"\x03hsm:record;")?;
        delete_then_put.hsm_put_record(ids[0], &original)?;
        delete_then_put.commit()?;
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, original.version);
        Ok(())
    }

    #[test]
    fn maximum_row_batch_is_atomic_and_row_overflow_is_rejected() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let template = dir.hsm_record(ids[0])?.unwrap();
        let ids: Vec<_> = (10_000..10_000 + MAX_ROWS as i64).collect();
        let mut batch = dir.get_rocks_store().new_batch();
        for id in &ids {
            let mut record = template.clone();
            record.ordinary = false;
            record.state.data.entity.entity = id.to_string();
            batch.hsm_put(&record_key(*id), &pack(&record)?)?;
        }
        batch.commit()?;
        let mut checkpoint = capture(&dir, &ids, 1)?;
        let bytes = pack(&checkpoint)?;
        assert!(bytes.len() < MAX_BYTES);
        assert_eq!(apply(&dir, &bytes)?["kind"], "done");
        for id in &ids {
            assert_eq!(dir.hsm_record(*id)?.unwrap().version, template.version + 3);
        }
        let before = sequence(&dir);
        checkpoint
            .rows
            .push(checkpoint.rows.last().unwrap().clone());
        assert!(apply(&dir, &pack(&checkpoint)?).is_err());
        assert_eq!(sequence(&dir), before);
        Ok(())
    }

    #[test]
    fn fast_format_activation_is_committed_idempotent_and_namespace_bound() -> CommonResult<()> {
        let (_root, fs, _ids) = fixture()?;
        let mut dir = fs.fs_dir.write();
        let ns = dir.hsm_namespace()?.unwrap();
        assert!(!activated(&dir, &ns)?);
        let activation = Activation {
            storage: ns.storage.clone(),
            incarnation: ns.incarnation.clone(),
        };
        let bytes = pack(&activation)?;
        dir.apply_hsm(&[ACTIVATE_MAGIC, bytes.as_slice()].concat())?;
        assert!(activated(&dir, &ns)?);
        let committed = sequence(&dir);
        assert_eq!(activate(&dir, &bytes)?["kind"], "done");
        assert_eq!(sequence(&dir), committed);
        let stale = pack(Activation {
            storage: ns.storage.clone(),
            incarnation: "rebound".into(),
        })?;
        assert_eq!(activate(&dir, &stale)?["data"]["code"], "fenced");
        assert_eq!(sequence(&dir), committed);
        dir.restore_from_rocksdb()?;
        assert!(activated(&dir, &ns)?);
        Ok(())
    }
    #[test]
    fn aggregate_jumps_preserve_structural_state_and_update_native_atime() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let checkpoint = capture(&dir, &ids, 1)?;
        let base = dir.get_rocks_store().hsm_get(&record_key(ids[0]))?;
        let old = dir.hsm_record(ids[0])?.unwrap();
        assert_eq!(apply(&dir, &pack(&checkpoint)?)?["kind"], "done");
        let next = dir.hsm_record(ids[0])?.unwrap();
        assert_eq!(next.version, old.version + 3);
        assert_eq!(next.state.revision, old.state.revision + 2);
        assert_eq!(next.state.data, old.state.data);
        assert_eq!(next.state.residency, old.state.residency);
        assert_eq!(next.published.is_some(), old.published.is_some());
        assert_eq!(dir.get_rocks_store().hsm_get(&record_key(ids[0]))?, base);
        assert_eq!(
            dir.store
                .get_inode(ids[0], None)?
                .unwrap()
                .as_file_ref()?
                .atime,
            next.atime_ms
        );
        assert_eq!(
            decode::<i64>(&dir.get_rocks_store().hsm_get(read_clock::FLOOR)?.unwrap())?,
            1000
        );
        Ok(())
    }

    #[test]
    fn shared_iterators_respect_range_reset_and_do_not_revive_hidden_rows() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let checkpoint = capture(&dir, &ids, 1)?;
        let ns = dir.hsm_namespace()?.unwrap();
        let retained = dir.hsm_record(ids[0])?.unwrap();
        dir.get_rocks_store().reset_hsm_projection()?;
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(NS, &pack(&ns)?)?;
        batch.hsm_put(&record_key(ids[0]), &pack(&retained)?)?;
        batch.commit()?;
        assert!(dir.hsm_record(ids[1])?.is_none());
        let before = sequence(&dir);
        assert_eq!(apply(&dir, &pack(&checkpoint)?)?["data"]["code"], "busy");
        assert_eq!(sequence(&dir), before);
        assert!(frontier(&dir)?.is_none());
        assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, retained.version);
        Ok(())
    }

    #[test]
    fn stale_row_and_frontier_cas_have_no_partial_writes() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let mut stale = capture(&dir, &ids, 1)?;
        stale.rows[1].expected_version += 1;
        let before = sequence(&dir);
        assert_eq!(apply(&dir, &pack(&stale)?)?["data"]["code"], "busy");
        assert_eq!(sequence(&dir), before);
        assert!(frontier(&dir)?.is_none());
        let first = capture(&dir, &ids, 1)?;
        assert_eq!(apply(&dir, &pack(&first)?)?["kind"], "done");
        let mut second = capture(&dir, &ids, 2)?;
        second.expected_frontier = None;
        let before = sequence(&dir);
        assert_eq!(apply(&dir, &pack(&second)?)?["data"]["code"], "busy");
        assert_eq!(sequence(&dir), before);
        Ok(())
    }

    #[test]
    fn exact_duplicate_is_read_only_and_rebound_identity_conflicts() -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let dir = fs.fs_dir.write();
        let mut checkpoint = capture(&dir, &ids, 1)?;
        let bytes = pack(&checkpoint)?;
        let original = apply(&dir, &bytes)?;
        let before = sequence(&dir);
        assert_eq!(apply(&dir, &bytes)?, original);
        assert_eq!(sequence(&dir), before);
        checkpoint.rows[0].access.atime_ms += 1;
        assert_eq!(
            apply(&dir, &pack(&checkpoint)?)?["data"]["code"],
            "conflict"
        );
        assert_eq!(sequence(&dir), before);
        Ok(())
    }

    #[test]
    fn later_close_checkpoint_survives_restore_and_old_retry_does_not_restore_reader(
    ) -> CommonResult<()> {
        let (_root, fs, ids) = fixture()?;
        let first_bytes = {
            let dir = fs.fs_dir.write();
            let mut opened = capture(&dir, &ids[..1], 1)?;
            opened.rows[0]
                .access
                .readers
                .insert("reader".into(), 121_000);
            let bytes = pack(&opened)?;
            assert_eq!(apply(&dir, &bytes)?["kind"], "done");
            let mut closed = capture(&dir, &ids[..1], 2)?;
            closed.rows[0].access.readers.clear();
            assert_eq!(apply(&dir, &pack(&closed)?)?["kind"], "done");
            bytes
        };
        fs.fs_dir.write().restore_from_rocksdb()?;
        let dir = fs.fs_dir.write();
        let before = sequence(&dir);
        assert_eq!(apply(&dir, &first_bytes)?["data"]["code"], "busy");
        assert_eq!(sequence(&dir), before);
        assert!(dir.hsm_record(ids[0])?.unwrap().readers.is_empty());
        assert_eq!(frontier(&dir)?.unwrap().sequence, 2);
        Ok(())
    }
}
