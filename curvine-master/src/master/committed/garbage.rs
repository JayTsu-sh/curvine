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
            self.worker_manager.write().remove_blocks(&DeleteResult {
                inodes: 0,
                bytes: 0,
                blocks: [(garbage.block, garbage.locations)].into_iter().collect(),
            });
            if self
                .fs_dir
                .read()
                .get_block_locations(garbage.block)?
                .is_empty()
            {
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
