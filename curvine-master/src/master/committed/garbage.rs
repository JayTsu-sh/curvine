// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Durable native block retirement, independent of inode/HSM record lifetime.
use super::transport::{NativeBatch, JOURNAL_VERSION};
use crate::master::{
    fs::{DeleteResult, MasterFilesystem},
    meta::FsDir,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{hsm::native::Namespace, BlockLocation};
use curvine_runtime::common::SerdeUtils;
use serde::{Deserialize, Serialize};

pub(super) const PREFIX: &[u8] = b"\x03hsm:native-gc:v1:";
pub(super) fn key(block: i64) -> Vec<u8> {
    format!("\u{3}hsm:native-gc:v1:{:016x}", block as u64).into_bytes()
}
#[derive(Serialize, Deserialize)]
pub(super) struct Garbage {
    pub(super) incarnation: String,
    pub(super) block: i64,
    pub(super) locations: Vec<BlockLocation>,
}
#[derive(Serialize, Deserialize)]
pub(super) struct GarbageAck {
    pub(super) incarnation: String,
    pub(super) epoch: u64,
    pub(super) items: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) cursor_expected: Option<Vec<u8>>,
    pub(super) cursor_next: Option<Vec<u8>>,
}
const CURSOR: &[u8] = b"\x03hsm:native-gc-cursor:v1";
fn valid_cursor(value: &Option<Vec<u8>>) -> bool {
    value.as_ref().is_none_or(|key| {
        key.starts_with(PREFIX)
            && key.len() == PREFIX.len() + 16
            && key[PREFIX.len()..].iter().all(u8::is_ascii_hexdigit)
    })
}

pub(super) fn validate(key_bytes: &[u8], value: &[u8], incarnation: &str) -> CommonResult<Garbage> {
    let garbage: Garbage = SerdeUtils::deserialize(value)?;
    if key_bytes != key(garbage.block) || garbage.incarnation != incarnation {
        return err_box!("invalid native garbage identity");
    }
    Ok(garbage)
}
pub(super) fn apply_ack(fs: &mut FsDir, ack: &GarbageAck) -> CommonResult<()> {
    if ack.items.len() > 16
        || !valid_cursor(&ack.cursor_expected)
        || !valid_cursor(&ack.cursor_next)
    {
        return err_box!("invalid native GC acknowledgement size");
    }
    let namespace = fs.hsm_namespace()?.ok_or("native GC has no namespace")?;
    if ack.incarnation != namespace.incarnation || ack.epoch != namespace.epoch {
        return Ok(());
    }
    let store = fs.get_rocks_store();
    let mut batch = store.new_batch();
    for (key, expected) in &ack.items {
        let garbage = validate(key, expected, &ack.incarnation)?;
        if store.hsm_get(key)?.as_ref() == Some(expected) {
            // No outcome/session is required: this only retires an immutable
            // acknowledged fact. Reapplying deletion has the same effect.
            batch.hsm_delete(key)?;
            for location in store.get_locations(garbage.block)? {
                batch.delete_location(garbage.block, location.worker_id)?;
            }
        }
    }
    if store.hsm_get(CURSOR)? == ack.cursor_expected {
        match &ack.cursor_next {
            Some(value) => batch.hsm_put(CURSOR, value)?,
            None => batch.hsm_delete(CURSOR)?,
        }
    }
    batch.commit()
}
/// Record-backed Release and native overwrite use the same per-block lookup.
/// The Record remains the source for legacy Release cleanup; these facts also
/// provide a bounded lookup instead of decoding a large Record per report.
pub(crate) fn stage_record_retirements(
    batch: &mut crate::master::meta::store::InodeWriteBatch,
    old: Option<&curvine_model::hsm::native::Record>,
    record: &curvine_model::hsm::native::Record,
) -> CommonResult<()> {
    for (block, locations) in &record.native_garbage {
        let previous = old.and_then(|record| record.native_garbage.get(block));
        let changed = previous.is_none_or(|previous| {
            previous.len() != locations.len()
                || previous.iter().zip(locations).any(|(old, new)| {
                    old.worker_id != new.worker_id || old.storage_type != new.storage_type
                })
        });
        if changed {
            batch.hsm_put(
                &key(*block),
                &SerdeUtils::serialize(&Garbage {
                    incarnation: record.state.data.entity.namespace.clone(),
                    block: *block,
                    locations: locations.clone(),
                })?,
            )?;
        }
        for location in locations {
            if previous.is_none_or(|old| {
                !old.iter()
                    .any(|prior| prior.worker_id == location.worker_id)
            }) {
                batch.add_location(*block, location)?;
            }
        }
    }
    if let Some(old) = old {
        for block in old.native_garbage.keys() {
            if !record.native_garbage.contains_key(block) {
                batch.hsm_delete(&key(*block))?;
            }
        }
    }
    Ok(())
}

pub(crate) fn has_retirement(fs: &FsDir, block: i64) -> CommonResult<bool> {
    Ok(fs.get_rocks_store().hsm_get(&key(block))?.is_some())
}

pub(crate) struct RetirementSnapshot {
    pub(crate) locations: Vec<BlockLocation>,
    pub(crate) acknowledged: bool,
}
/// Replica placement can change after preparation. Deletion must cover both
/// the durable manifest and all currently reported copies of this retired ID.
pub(crate) fn retirement_snapshot(
    fs: &FsDir,
    block: i64,
    known: &[BlockLocation],
) -> CommonResult<RetirementSnapshot> {
    let reported = fs.get_block_locations(block)?;
    let acknowledged = reported.is_empty();
    let mut locations: std::collections::BTreeMap<_, _> = known
        .iter()
        .cloned()
        .map(|location| (location.worker_id, location))
        .collect();
    locations.extend(
        reported
            .into_iter()
            .map(|location| (location.worker_id, location)),
    );
    Ok(RetirementSnapshot {
        locations: locations.into_values().collect(),
        acknowledged,
    })
}

impl MasterFilesystem {
    /// Called under the existing qualified HSM mutation gate. Worker deletion
    /// commands are regenerated after a leader restart from persisted facts.
    pub(crate) async fn native_gc_batch(&self, namespace: &Namespace) -> CommonResult<usize> {
        let cursor = self.fs_dir.read().get_rocks_store().hsm_get(CURSOR)?;
        let page = self
            .fs_dir
            .read()
            .get_rocks_store()
            .hsm_scan(PREFIX, cursor.as_deref(), 16)?;
        let next = if page.len() < 16 {
            None
        } else {
            page.last().map(|(key, _)| key.clone())
        };
        let mut ready = vec![];
        for (key, bytes) in &page {
            let garbage = validate(key, bytes, &namespace.incarnation)?;
            let snapshot =
                retirement_snapshot(&self.fs_dir.read(), garbage.block, &garbage.locations)?;
            self.worker_manager.write().remove_blocks(&DeleteResult {
                inodes: 0,
                bytes: 0,
                blocks: [(garbage.block, snapshot.locations)].into_iter().collect(),
                trashed: false,
            });
            if snapshot.acknowledged {
                ready.push((key.clone(), bytes.clone()));
            }
        }
        if !ready.is_empty() || next != cursor {
            let term = self.fs_dir.read().journal_writer.current_term();
            let payload = SerdeUtils::serialize(&NativeBatch {
                version: JOURNAL_VERSION,
                commands: vec![],
                garbage_ack: Some(GarbageAck {
                    incarnation: namespace.incarnation.clone(),
                    epoch: namespace.epoch,
                    items: ready,
                    cursor_expected: cursor,
                    cursor_next: next,
                }),
            })?;
            if let Err(error) = self.submit_native_payload(payload, 0, term).await {
                if let Err(fence) = self.hsm_read_barrier().await {
                    log::error!(
                        "uncertain native GC could not be fenced: {fence}; aborting master"
                    );
                    std::process::abort();
                }
                return Err(Box::new(error));
            }
        }
        Ok(page.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gc_schedules_replicas_reported_after_the_retirement_plan() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let namespace = Namespace {
            storage: "source".into(),
            incarnation: "incarnation".into(),
            epoch: 1,
            ordinary_format: 2,
            root_inode: 1,
        };
        {
            let dir = fs.fs_dir.read();
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(b"\x03hsm:namespace:v1", &serde_json::to_vec(&namespace)?)?;
            batch.hsm_put(
                &key(42),
                &SerdeUtils::serialize(&Garbage {
                    incarnation: namespace.incarnation.clone(),
                    block: 42,
                    locations: vec![BlockLocation::with_id(1)],
                })?,
            )?;
            // Worker 2 reported a copy after preparation took its manifest.
            batch.add_location(42, &BlockLocation::with_id(2))?;
            batch.commit()?;
        }
        assert_eq!(
            futures::executor::block_on(fs.native_gc_batch(&namespace))?,
            1
        );
        let commands = fs.worker_manager.write().block_map.handle_heartbeat(2);
        assert!(commands.iter().any(|command| matches!(command, curvine_model::WorkerCommand::DeleteBlock(cmd) if cmd.blocks.contains(&42))),
            "GC waited on a replica without ever scheduling its deletion");
        assert!(fs
            .fs_dir
            .read()
            .get_rocks_store()
            .hsm_get(&key(42))?
            .is_some());
        assert_eq!(fs.fs_dir.read().get_block_locations(42)?.len(), 1);
        Ok(())
    }
}
