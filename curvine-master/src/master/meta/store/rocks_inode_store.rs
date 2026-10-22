// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::master::meta::inode::InodeView;
use crate::master::meta::LockMeta;
use curvine_core_error::CommonResult;
use curvine_model::{BlockLocation, FileLock, MountInfo};
use curvine_rocksdb::{DBConf, DBEngine, ReadOptions, RocksIterator, RocksUtils};
use curvine_rocksdb::{
    DBIteratorWithThreadMode, DBPinnableSlice, Error, WriteBatchWithTransaction, DB,
};
use curvine_runtime::common::SerdeUtils as Serde;
use std::collections::HashMap;

#[cfg(test)]
thread_local! {
    static LOCATION_FALLBACKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct RocksInodeStore {
    pub(crate) db: DBEngine,
}

impl RocksInodeStore {
    pub const CF_INODES: &'static str = "inodes";
    pub const CF_EDGES: &'static str = "edges";
    pub const CF_BLOCK: &'static str = "block";
    pub const CF_LOCATION: &'static str = "location";
    pub const CF_COMMON: &'static str = "common";
    // An explicit RocksDB schema fence: an older binary omitting this family
    // cannot reopen a checkpoint containing access overlays and native facts.
    pub(crate) const CF_HSM_FORMAT: &'static str = "hsm_native_v2";
    pub(crate) const CF_NATIVE_FORMAT: &'static str = "hsm_native_v3";
    pub(crate) const CF_NATIVE_GC_FORMAT: &'static str = "hsm_native_v4";
    pub(crate) const CF_NATIVE_DELETE_FORMAT: &'static str = "hsm_native_v5";

    pub(crate) const CF_NATIVE_NAMESPACE_FORMAT: &'static str = "hsm_native_v6";
    pub(crate) const CF_HSM_CONDITIONAL_FORMAT: &'static str = "hsm_native_v7";
    pub(crate) const CF_LIFECYCLE_CONDITIONAL_FORMAT: &'static str = "hsm_native_v8";
    pub(crate) const CF_WORKER_CLEANUP_FORMAT: &'static str = "hsm_native_v9";

    pub const PREFIX_MOUNT: u8 = 0x01;
    pub const PREFIX_LOCK: u8 = 0x02;
    pub(crate) const NATIVE_INODE_FLOOR: &'static [u8] = b"\x03hsm:native-inode-floor:v1";

    pub(crate) fn native_inode_floor(&self) -> CommonResult<Option<i64>> {
        self.hsm_get(Self::NATIVE_INODE_FLOOR)?
            .map(|bytes| {
                let value: i64 = Serde::deserialize(&bytes)?;
                if !(crate::master::meta::inode::ROOT_INODE_ID
                    ..=crate::master::meta::InodeId::ID_MASK)
                    .contains(&value)
                {
                    return Err("invalid native inode allocation floor".into());
                }
                Ok(value)
            })
            .transpose()
    }

    /// Callers serialize committed namespace application. The candidate comes
    /// from the committed command, never a speculative leader allocator.
    pub(crate) fn append_native_inode_floor(
        &self,
        batch: &mut InodeWriteBatch<'_>,
        id: i64,
    ) -> CommonResult<bool> {
        if !(crate::master::meta::inode::ROOT_INODE_ID..=crate::master::meta::InodeId::ID_MASK)
            .contains(&id)
        {
            return Err("invalid committed inode allocation".into());
        }
        if self.native_inode_floor()?.is_some_and(|old| old >= id) {
            return Ok(false);
        }
        batch.hsm_put(Self::NATIVE_INODE_FLOOR, &Serde::serialize(&id)?)?;
        Ok(true)
    }

    pub fn new(conf: DBConf, format: bool) -> CommonResult<Self> {
        let conf = conf
            .set_disable_wal(true)
            .add_cf(Self::CF_INODES)
            .add_cf(Self::CF_EDGES)
            .add_cf(Self::CF_BLOCK)
            .add_cf(Self::CF_LOCATION)
            .add_cf(Self::CF_COMMON);
        let db = DBEngine::new(conf, format)?;
        Ok(Self { db })
    }

    pub fn get_child_ids(
        &self,
        id: i64,
        prefix: Option<&str>,
    ) -> CommonResult<InodeChildrenIter<'_>> {
        let iter = match prefix {
            None => {
                let key = RocksUtils::i64_to_bytes(id);
                self.db.prefix_scan(Self::CF_EDGES, key)?
            }

            Some(v) => {
                let key = RocksUtils::i64_str_to_bytes(id, v);
                self.db.prefix_scan(Self::CF_EDGES, key)?
            }
        };

        Ok(InodeChildrenIter { inner: iter })
    }

    // Get all location information for all block ids.
    pub fn get_locations(&self, block_id: i64) -> CommonResult<Vec<BlockLocation>> {
        let prefix = RocksUtils::i64_to_bytes(block_id);
        let iter = self.db.prefix_scan(Self::CF_BLOCK, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = Serde::deserialize::<BlockLocation>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    /// Reuse one iterator only within the caller's namespace guard.
    /// Results are owned; no iterator, pinned value, or cache escapes the call.
    pub fn get_locations_many(
        &self,
        ids: impl IntoIterator<Item = i64>,
    ) -> CommonResult<HashMap<i64, Vec<BlockLocation>>> {
        let ids: Vec<i64> = ids.into_iter().collect();
        // Bound the shared iterator to the requested range so seeking past the
        // last requested prefix cannot walk tombstones of neighbouring blocks.
        // Keys are big-endian, so byte order is the unsigned order of the id.
        let mut opt = ReadOptions::default();
        let bounded = ids.iter().copied().filter(|id| *id != -1);
        if let (Some(min), Some(max)) = (
            bounded.clone().min_by_key(|id| *id as u64),
            bounded.max_by_key(|id| *id as u64),
        ) {
            opt.set_iterate_lower_bound(RocksUtils::i64_to_bytes(min));
            let upper = RocksUtils::calculate_end_bytes(&RocksUtils::i64_to_bytes(max));
            if !upper.is_empty() {
                opt.set_iterate_upper_bound(upper);
            }
        }
        // DBConf currently uses the default ordered memtable and no prefix extractor.
        let mut iter = self
            .db
            .get_db()
            .raw_iterator_cf_opt(self.db.cf(Self::CF_BLOCK)?, opt);
        let mut result = HashMap::with_capacity(ids.len());
        for id in ids {
            // Preserve the existing all-0xff prefix boundary behavior.
            if id == -1 {
                result.insert(id, self.get_locations(id)?);
                continue;
            }
            let prefix = RocksUtils::i64_to_bytes(id);
            iter.seek(prefix);
            let mut locations = Vec::with_capacity(8);
            while let Some((key, value)) = iter.item() {
                if !key.starts_with(&prefix) {
                    break;
                }
                locations.push(Serde::deserialize::<BlockLocation>(value)?);
                iter.next();
            }
            if iter.status().is_err() {
                // The shared iterator spans every requested prefix and may touch
                // a corrupt SST outside this one. Retry the per-block query to preserve its
                // error scope; corruption inside the requested prefix still
                // propagates. No partial location list is published.
                #[cfg(test)]
                LOCATION_FALLBACKS.with(|count| count.set(count.get() + 1));
                locations = self.get_locations(id)?;
            }
            result.insert(id, locations);
        }
        Ok(result)
    }

    /// Discard the untrusted projection before committed journal recovery.
    /// A range tombstone bounds this operation independently of record count.
    /// It is ordered before replay writes, so replayed keys inside the range
    /// remain visible. Physical reclamation belongs to RocksDB compaction.
    pub fn reset_hsm_projection(&self) -> CommonResult<()> {
        let started = std::time::Instant::now();
        let mut batch = WriteBatchWithTransaction::<false>::default();
        let cf = self.db.cf(Self::CF_COMMON)?;
        batch.delete_range_cf(cf, b"\x03hsm:", b"\x03hsm;");
        self.db.write_batch(batch)?;
        log::info!(
            "reset HSM projection: one range tombstone, elapsed_us={}",
            started.elapsed().as_micros()
        );
        Ok(())
    }

    /// A cold HSM boot rebuilds the native tree from checkpoint/journal too.
    /// Its RocksDB rows cannot retain a prior process's speculative tail while
    /// committed preparation relies on conditional reads of the same store.
    /// Fixed range tombstones avoid a record-count-dependent cleanup pass.
    pub(crate) fn reset_committed_projection(&self) -> CommonResult<()> {
        let started = std::time::Instant::now();
        let mut batch = WriteBatchWithTransaction::<false>::default();
        // Native inode/block/location keys are <= 12 bytes. Edge keys start
        // with an 8-byte inode ID followed by UTF-8, whose bytes cannot all be
        // 0xff. This upper bound covers the complete supported native schema,
        // including the maximum binary block/location key.
        let end = [u8::MAX; 33];
        for name in [
            Self::CF_INODES,
            Self::CF_EDGES,
            Self::CF_BLOCK,
            Self::CF_LOCATION,
        ] {
            batch.delete_range_cf(self.db.cf(name)?, b"".as_slice(), end.as_slice());
        }
        let common = self.db.cf(Self::CF_COMMON)?;
        batch.delete_range_cf(common, b"\x03hsm:", b"\x03hsm;");
        for prefix in [Self::PREFIX_MOUNT, Self::PREFIX_LOCK] {
            batch.delete_range_cf(common, [prefix], [prefix + 1]);
        }
        self.db.write_batch(batch)?;
        log::info!(
            "reset committed metadata projection: seven range tombstones, elapsed_us={}",
            started.elapsed().as_micros()
        );
        Ok(())
    }
    pub fn hsm_scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> CommonResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut end = prefix.to_vec();
        *end.last_mut().ok_or("empty HSM prefix")? += 1;
        let mut start = after.unwrap_or(prefix).to_vec();
        if after.is_some() {
            start.push(0);
        }
        self.db
            .range_scan(Self::CF_COMMON, start, end)?
            .take(limit)
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(Into::into))
            .collect()
    }
    pub fn hsm_get(&self, key: &[u8]) -> CommonResult<Option<Vec<u8>>> {
        Ok(self.db.get_cf(Self::CF_COMMON, key)?.map(|v| v.to_vec()))
    }

    pub fn new_batch(&self) -> InodeWriteBatch<'_> {
        InodeWriteBatch::new(&self.db)
    }

    pub fn inodes_iter(&self) -> CommonResult<RocksIterator<'_>> {
        self.db.scan(Self::CF_INODES)
    }

    pub fn edges_iter(&self, id: i64) -> CommonResult<RocksIterator<'_>> {
        self.db
            .prefix_scan(Self::CF_EDGES, RocksUtils::i64_to_bytes(id))
    }

    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>> {
        let bytes = self
            .db
            .get_cf(Self::CF_INODES, RocksUtils::i64_to_bytes(id))?;
        match bytes {
            None => Ok(None),

            Some(v) => {
                let inode: InodeView = Serde::deserialize(&v)?;
                Ok(Some(inode))
            }
        }
    }

    pub fn batched_multi_get_inodes<'a, K, I>(
        &'a self,
        keys: I,
        sorted_input: bool,
    ) -> CommonResult<Vec<Result<Option<DBPinnableSlice<'a>>, Error>>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        self.db
            .batched_multi_get_cf(Self::CF_INODES, keys, sorted_input)
    }

    pub fn iter_cf<'a: 'b, 'b>(
        &'a self,
        cf: &str,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.iter_cf_opt(cf)
    }

    /// Bulk-scan the entire `inodes` CF with tuned ReadOptions (total_order_seek +
    /// fill_cache(false) + 64 MiB readahead).  Use for snapshot restore.
    pub fn bulk_scan_inodes<'a: 'b, 'b>(
        &'a self,
    ) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.bulk_scan(Self::CF_INODES)
    }

    /// Bulk-scan the entire `edges` CF with tuned ReadOptions.
    pub fn bulk_scan_edges<'a: 'b, 'b>(&'a self) -> CommonResult<DBIteratorWithThreadMode<'b, DB>> {
        self.db.bulk_scan(Self::CF_EDGES)
    }

    pub fn delete_locations(&self, worker_id: u32) -> CommonResult<Vec<i64>> {
        let mut block_ids = Vec::new();
        let mut after = None;
        loop {
            let page = self.delete_locations_page(worker_id, after, 1024)?;
            let Some(last) = page.last().copied() else {
                break;
            };
            after = Some(last);
            block_ids.extend(page);
        }
        Ok(block_ids)
    }

    /// Delete one bounded page in byte-key order, excluding the previous key.
    /// The caller serializes location writers and checks the Worker incarnation.
    /// No iterator or full Worker inventory survives this call.
    pub(crate) fn delete_locations_page(
        &self,
        worker_id: u32,
        after: Option<i64>,
        limit: usize,
    ) -> CommonResult<Vec<i64>> {
        let block_ids = self.worker_blocks_page(worker_id, after, limit)?;
        if !block_ids.is_empty() {
            let mut batch = self.new_batch();
            for id in &block_ids {
                batch.delete_location(*id, worker_id)?;
            }
            batch.commit()?;
        }
        Ok(block_ids)
    }

    pub(crate) fn worker_blocks_page(
        &self,
        worker_id: u32,
        after: Option<i64>,
        limit: usize,
    ) -> CommonResult<Vec<i64>> {
        if limit == 0 || limit > 1024 {
            return Err("lost Worker location page limit must be in 1..=1024".into());
        }
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        let start = after
            .map(|id| RocksUtils::u32_i64_to_bytes(worker_id, id).to_vec())
            .unwrap_or_else(|| prefix.to_vec());
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_lower_bound(start.clone());
        let end = RocksUtils::calculate_end_bytes(&prefix);
        if !end.is_empty() {
            options.set_iterate_upper_bound(end);
        }
        let mut iter = self
            .db
            .get_db()
            .raw_iterator_cf_opt(self.db.cf(Self::CF_LOCATION)?, options);
        iter.seek(&start);
        let mut block_ids = Vec::with_capacity(limit);
        while let Some((key, value)) = iter.item() {
            if !key.starts_with(&prefix) || block_ids.len() == limit {
                break;
            }
            if after.is_none() || key != start.as_slice() {
                block_ids.push(Serde::deserialize::<i64>(value)?);
            }
            iter.next();
        }
        iter.status()?;
        Ok(block_ids)
    }

    /// One seek per Worker, never a walk of its complete block inventory.
    pub(crate) fn location_workers_page(
        &self,
        after: Option<u32>,
        limit: usize,
    ) -> CommonResult<Vec<u32>> {
        if limit == 0 || limit > 64 {
            return Err("invalid Worker inventory page limit".into());
        }
        let Some(mut next) = after.map_or(Some(0), |id| id.checked_add(1)) else {
            return Ok(vec![]);
        };
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        let mut iter = self
            .db
            .get_db()
            .raw_iterator_cf_opt(self.db.cf(Self::CF_LOCATION)?, options);
        let mut ids = Vec::with_capacity(limit);
        loop {
            iter.seek(RocksUtils::u32_to_bytes(next));
            let Some((key, _)) = iter.item() else {
                break;
            };
            if key.len() != 12 {
                return Err("invalid reverse location key".into());
            }
            let id = u32::from_be_bytes(key[..4].try_into()?);
            ids.push(id);
            if ids.len() == limit {
                break;
            }
            match id.checked_add(1) {
                Some(id) => next = id,
                None => break,
            }
        }
        iter.status()?;
        Ok(ids)
    }

    pub(crate) fn inode_bytes(&self, id: i64) -> CommonResult<Option<Vec<u8>>> {
        Ok(self
            .db
            .get_cf(Self::CF_INODES, RocksUtils::i64_to_bytes(id))?
            .map(|value| value.to_vec()))
    }

    pub fn get_block_ids(&self, worker_id: u32) -> CommonResult<Vec<i64>> {
        let prefix = RocksUtils::u32_to_bytes(worker_id);
        let iter = self.db.prefix_scan(Self::CF_LOCATION, prefix)?;

        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let location = Serde::deserialize::<i64>(&bytes.1)?;
            vec.push(location);
        }

        Ok(vec)
    }

    pub fn add_mountpoint(&self, id: u32, entry: &MountInfo) -> CommonResult<()> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);
        let value = Serde::serialize(entry)?;
        self.db.put_cf(Self::CF_COMMON, key, value)
    }

    pub fn remove_mountpoint(&self, id: u32) -> CommonResult<()> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);
        self.db.delete_cf(Self::CF_COMMON, key)
    }

    pub fn get_mount_info(&self, id: u32) -> CommonResult<Option<MountInfo>> {
        let key = RocksUtils::u8_u32_to_bytes(Self::PREFIX_MOUNT, id);

        let bytes = self.db.get_cf(Self::CF_COMMON, key)?;

        match bytes {
            None => Ok(None),

            Some(v) => {
                let info = MountInfo::decode_persisted(&v)?;
                Ok(Some(info))
            }
        }
    }

    pub fn get_mount_table(&self) -> CommonResult<Vec<MountInfo>> {
        let iter = self.db.prefix_scan(Self::CF_COMMON, [Self::PREFIX_MOUNT])?;
        let mut vec = Vec::with_capacity(8);
        for item in iter {
            let bytes = item?;
            let mnt = MountInfo::decode_persisted(&bytes.1)?;
            vec.push(mnt);
        }

        Ok(vec)
    }

    pub fn get_locks(&self, id: i64) -> CommonResult<LockMeta> {
        let key = RocksUtils::u8_i64_to_bytes(Self::PREFIX_LOCK, id);
        let bytes = self.db.get_cf(Self::CF_COMMON, key)?;

        if let Some(bytes) = bytes {
            let locks: Vec<FileLock> = Serde::deserialize(&bytes)?;
            Ok(LockMeta::with_vec(locks))
        } else {
            Ok(LockMeta::default())
        }
    }

    pub fn set_locks(&self, id: i64, lock: &[FileLock]) -> CommonResult<()> {
        let key = RocksUtils::u8_i64_to_bytes(Self::PREFIX_LOCK, id);
        if lock.is_empty() {
            self.db.delete_cf(RocksInodeStore::CF_COMMON, key)
        } else {
            let value = Serde::serialize(&lock)?;
            self.db.put_cf(RocksInodeStore::CF_COMMON, key, value)
        }
    }

    pub fn get_rocksdb_metrics(&self) -> CommonResult<HashMap<String, u64>> {
        self.db.get_rocksdb_metrics()
    }
}

pub struct InodeChildrenIter<'a> {
    inner: RocksIterator<'a>,
}

impl Iterator for InodeChildrenIter<'_> {
    type Item = CommonResult<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v) = self.inner.next() {
            match v {
                Err(e) => Some(Err(e.into())),

                Ok(bytes) => {
                    let id = match RocksUtils::i64_from_bytes(&bytes.1) {
                        Ok(id) => id,
                        Err(e) => return Some(Err(e)),
                    };
                    Some(Ok(id))
                }
            }
        } else {
            None
        }
    }
}

pub struct InodeWriteBatch<'a> {
    db: &'a DBEngine,
    batch: WriteBatchWithTransaction<false>,
}

impl<'a> InodeWriteBatch<'a> {
    pub fn new(db: &'a DBEngine) -> Self {
        Self {
            db,
            batch: WriteBatchWithTransaction::<false>::default(),
        }
    }

    fn put_cf<K, V>(&mut self, cf: &str, key: K, value: V) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.put_cf(cf, key, value);
        Ok(())
    }

    fn delete_cf<K>(&mut self, cf: &str, key: K) -> CommonResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self.db.cf(cf)?;
        self.batch.delete_cf(cf, key);
        Ok(())
    }

    pub fn add_location(&mut self, id: i64, loc: &BlockLocation) -> CommonResult<()> {
        // store with the key of  (block_id, worker_id)
        let key = RocksUtils::i64_u32_to_bytes(id, loc.worker_id);
        let value = Serde::serialize(loc)?;
        self.put_cf(RocksInodeStore::CF_BLOCK, key, value)?;

        // store with the key of (worker_id, block_id)
        let key = RocksUtils::u32_i64_to_bytes(loc.worker_id, id);
        let value = Serde::serialize(&id)?;
        self.put_cf(RocksInodeStore::CF_LOCATION, key, value)
    }

    // Add an inode.
    pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(inode.id());
        let value = Serde::serialize(inode)?;
        self.put_cf(RocksInodeStore::CF_INODES, key, value)
    }

    // Add an edge to identify the subordinate relationship between inodes
    pub fn add_child(
        &mut self,
        parent_id: i64,
        child_name: &str,
        child_id: i64,
    ) -> CommonResult<()> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
        let value = RocksUtils::i64_to_bytes(child_id);
        self.put_cf(RocksInodeStore::CF_EDGES, key, value)
    }

    pub fn delete_inode(&mut self, id: i64) -> CommonResult<()> {
        let key = RocksUtils::i64_to_bytes(id);
        self.delete_cf(RocksInodeStore::CF_INODES, key)
    }

    // Delete a subordinate relationship between an inode
    pub fn delete_child(&mut self, parent_id: i64, child_name: &str) -> CommonResult<()> {
        let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
        self.delete_cf(RocksInodeStore::CF_EDGES, key)
    }

    // Delete 1 block to store information
    pub fn delete_location(&mut self, id: i64, worker_id: u32) -> CommonResult<()> {
        let key = RocksUtils::i64_u32_to_bytes(id, worker_id);
        self.delete_cf(RocksInodeStore::CF_BLOCK, key)?;

        let key = RocksUtils::u32_i64_to_bytes(worker_id, id);
        self.delete_cf(RocksInodeStore::CF_LOCATION, key)
    }

    pub fn hsm_delete(&mut self, key: &[u8]) -> CommonResult<()> {
        self.delete_cf(RocksInodeStore::CF_COMMON, key)
    }
    pub fn hsm_put(&mut self, key: &[u8], value: &[u8]) -> CommonResult<()> {
        self.put_cf(RocksInodeStore::CF_COMMON, key, value)
    }

    pub(crate) fn hsm_delete_range(&mut self, begin: &[u8], end: &[u8]) -> CommonResult<()> {
        self.batch
            .delete_range_cf(self.db.cf(RocksInodeStore::CF_COMMON)?, begin, end);
        Ok(())
    }

    pub fn commit(self) -> CommonResult<()> {
        self.db.write_batch(self.batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::StorageType;
    use curvine_rocksdb::perf::{set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
    use curvine_runtime::common::Utils;

    #[test]
    fn worker_inventory_discovery_seeks_past_dense_block_prefixes() -> CommonResult<()> {
        let directory = tempfile::tempdir()?;
        let store = RocksInodeStore::new(
            DBConf::new(directory.path().to_string_lossy().into_owned()),
            true,
        )?;
        let mut batch = store.new_batch();
        for worker in [0, 7, 9, u32::MAX] {
            for block in 1..=2050 {
                batch.add_location(block, &BlockLocation::with_id(worker))?;
            }
        }
        batch.commit()?;
        let before = store.db.get_db().latest_sequence_number();
        assert_eq!(store.location_workers_page(None, 2)?, [0, 7]);
        assert_eq!(store.location_workers_page(Some(7), 2)?, [9, u32::MAX]);
        assert_eq!(store.location_workers_page(Some(10), 2)?, [u32::MAX]);
        assert!(store.location_workers_page(Some(u32::MAX), 2)?.is_empty());
        assert!(store.location_workers_page(None, 0).is_err());
        assert!(store.location_workers_page(None, 65).is_err());
        assert_eq!(store.db.get_db().latest_sequence_number(), before);
        Ok(())
    }

    #[test]
    fn lost_worker_location_pages_follow_byte_order_and_bound_the_last_worker_prefix(
    ) -> CommonResult<()> {
        let directory = tempfile::tempdir()?;
        let store = RocksInodeStore::new(
            DBConf::new(directory.path().to_string_lossy().into_owned()),
            true,
        )?;
        let ids = [0, 1, i64::MAX, i64::MIN, -2, -1];
        let mut batch = store.new_batch();
        for id in ids {
            batch.add_location(id, &BlockLocation::with_id(u32::MAX))?;
            batch.add_location(id, &BlockLocation::with_id(u32::MAX - 1))?;
        }
        batch.commit()?;
        let sequence = store.db.get_db().latest_sequence_number();
        assert!(store.delete_locations_page(u32::MAX, None, 0).is_err());
        assert!(store.delete_locations_page(u32::MAX, None, 1025).is_err());
        assert_eq!(store.db.get_db().latest_sequence_number(), sequence);
        assert_eq!(store.delete_locations_page(u32::MAX, None, 2)?, ids[..2]);
        // Cursor exclusion must also work when a duplicate report put the
        // last key back. It is not an offset into the changing inventory.
        let mut batch = store.new_batch();
        batch.add_location(1, &BlockLocation::with_id(u32::MAX))?;
        batch.commit()?;
        assert_eq!(
            store.delete_locations_page(u32::MAX, Some(1), 2)?,
            ids[2..4]
        );
        assert_eq!(
            store.delete_locations_page(u32::MAX, Some(i64::MIN), 2)?,
            ids[4..]
        );
        assert!(store
            .delete_locations_page(u32::MAX, Some(-1), 2)?
            .is_empty());
        for id in ids {
            assert!(store
                .db
                .get_cf(
                    RocksInodeStore::CF_BLOCK,
                    RocksUtils::i64_u32_to_bytes(id, u32::MAX - 1)
                )?
                .is_some());
            assert_eq!(
                store
                    .db
                    .get_cf(
                        RocksInodeStore::CF_BLOCK,
                        RocksUtils::i64_u32_to_bytes(id, u32::MAX)
                    )?
                    .is_some(),
                id == 1
            );
        }
        Ok(())
    }

    #[test]
    fn lost_worker_locations_remove_both_indexes_across_chunks_without_touching_other_replicas(
    ) -> CommonResult<()> {
        let directory = tempfile::tempdir()?;
        let store = RocksInodeStore::new(
            DBConf::new(directory.path().to_string_lossy().into_owned()),
            true,
        )?;
        let mut batch = store.new_batch();
        for block in 1..=2050 {
            batch.add_location(block, &curvine_model::BlockLocation::with_id(7))?;
            batch.add_location(block, &curvine_model::BlockLocation::with_id(8))?;
        }
        batch.commit()?;
        assert_eq!(store.delete_locations(7)?.len(), 2050);
        assert!(store.get_block_ids(7)?.is_empty());
        assert_eq!(store.get_block_ids(8)?.len(), 2050);
        for block in [1, 1024, 1025, 2048, 2050] {
            let replicas = store.get_locations(block)?;
            assert_eq!(replicas.len(), 1);
            assert_eq!(replicas[0].worker_id, 8);
        }
        Ok(())
    }

    #[test]
    fn hsm_schema_fence_survives_checkpoint_and_rejects_legacy_open() -> CommonResult<()> {
        let directory = tempfile::tempdir()?;
        let legacy = DBConf::new(directory.path().to_string_lossy().into_owned());
        let overlay_only = legacy.clone().add_cf(RocksInodeStore::CF_HSM_FORMAT);
        let native_v3 = overlay_only
            .clone()
            .add_cf(RocksInodeStore::CF_NATIVE_FORMAT);
        let native_v4 = native_v3
            .clone()
            .add_cf(RocksInodeStore::CF_NATIVE_GC_FORMAT);
        let native_v5 = native_v4
            .clone()
            .add_cf(RocksInodeStore::CF_NATIVE_DELETE_FORMAT);
        let native_v6 = native_v5
            .clone()
            .add_cf(RocksInodeStore::CF_NATIVE_NAMESPACE_FORMAT);
        let native_v7 = native_v6
            .clone()
            .add_cf(RocksInodeStore::CF_HSM_CONDITIONAL_FORMAT);
        let native_v8 = native_v7
            .clone()
            .add_cf(RocksInodeStore::CF_LIFECYCLE_CONDITIONAL_FORMAT);
        let upgraded = native_v8
            .clone()
            .add_cf(RocksInodeStore::CF_WORKER_CLEANUP_FORMAT);
        let store = RocksInodeStore::new(upgraded.clone(), true)?;
        let checkpoint = store.db.create_checkpoint(1)?;
        drop(store);
        // The pre-overlay opener's exact family list cannot interpret this DB.
        assert!(RocksInodeStore::new(legacy, false).is_err());
        assert!(RocksInodeStore::new(overlay_only, false).is_err());
        assert!(RocksInodeStore::new(native_v3, false).is_err());
        assert!(RocksInodeStore::new(native_v4, false).is_err());
        assert!(RocksInodeStore::new(native_v5, false).is_err());
        assert!(RocksInodeStore::new(native_v6, false).is_err());
        assert!(RocksInodeStore::new(native_v7, false).is_err());
        assert!(RocksInodeStore::new(native_v8, false).is_err());
        let mut store = RocksInodeStore::new(upgraded, false)?;
        store.db.restore(checkpoint)?;
        assert!(store
            .db
            .cf(RocksInodeStore::CF_WORKER_CLEANUP_FORMAT)
            .is_ok());
        assert!(store
            .db
            .cf(RocksInodeStore::CF_LIFECYCLE_CONDITIONAL_FORMAT)
            .is_ok());
        assert!(store
            .db
            .cf(RocksInodeStore::CF_HSM_CONDITIONAL_FORMAT)
            .is_ok());
        assert!(store.db.cf(RocksInodeStore::CF_HSM_FORMAT).is_ok());
        assert!(store.db.cf(RocksInodeStore::CF_NATIVE_FORMAT).is_ok());
        assert!(store.db.cf(RocksInodeStore::CF_NATIVE_GC_FORMAT).is_ok());
        assert!(store
            .db
            .cf(RocksInodeStore::CF_NATIVE_DELETE_FORMAT)
            .is_ok());
        Ok(())
    }

    #[test]
    fn hsm_committed_reset_discards_native_tail_and_replay_can_reuse_its_keys() -> CommonResult<()>
    {
        let directory = tempfile::tempdir()?;
        let store = RocksInodeStore::new(
            DBConf::new(directory.path().to_string_lossy().into_owned()),
            true,
        )?;
        let mut batch = store.new_batch();
        let root = crate::master::meta::FsDir::create_root();
        batch.write_inode(&root)?;
        batch.add_child(root.id(), "uncommitted", 2000)?;
        // Exercise the largest binary location keys, not just positive IDs.
        batch.add_location(-1, &BlockLocation::new(u32::MAX, StorageType::Disk))?;
        batch.hsm_put(b"\x01mount", b"stale")?;
        batch.hsm_put(b"\x02lock", b"stale")?;
        batch.hsm_put(b"\x03hsm:outcome:old", b"stale")?;
        batch.hsm_put(b"other-system-fact", b"keep")?;
        batch.commit()?;
        store.db.flush(true)?;
        store.reset_committed_projection()?;
        for name in [
            RocksInodeStore::CF_INODES,
            RocksInodeStore::CF_EDGES,
            RocksInodeStore::CF_BLOCK,
            RocksInodeStore::CF_LOCATION,
        ] {
            assert!(store.iter_cf(name)?.next().is_none());
        }
        assert!(store.hsm_get(b"\x01mount")?.is_none());
        assert!(store.hsm_get(b"\x02lock")?.is_none());
        assert!(store.hsm_scan(b"\x03hsm:", None, 1)?.is_empty());
        assert_eq!(
            store.hsm_get(b"other-system-fact")?.as_deref(),
            Some(b"keep".as_slice())
        );
        let mut batch = store.new_batch();
        batch.write_inode(&root)?;
        batch.add_child(root.id(), "committed", 2001)?;
        batch.add_location(-1, &BlockLocation::new(u32::MAX, StorageType::Disk))?;
        batch.commit()?;
        assert_eq!(store.get_inode(root.id())?.unwrap().id(), root.id());
        assert!(store
            .db
            .get_cf(
                RocksInodeStore::CF_BLOCK,
                RocksUtils::i64_u32_to_bytes(-1, u32::MAX)
            )?
            .is_some());
        assert!(store
            .db
            .get_cf(
                RocksInodeStore::CF_LOCATION,
                RocksUtils::u32_i64_to_bytes(u32::MAX, -1)
            )?
            .is_some());
        Ok(())
    }

    #[test]
    fn hsm_range_reset_preserves_neighbors_and_replayed_keys_across_reopen() -> CommonResult<()> {
        let directory = tempfile::tempdir()?;
        let conf = DBConf::new(directory.path().to_string_lossy().into_owned());
        let store = RocksInodeStore::new(conf.clone(), true)?;
        let keep: &[&[u8]] = &[b"\x02hsm:keep", b"\x03hsm", b"\x03hsm;", b"\x04hsm:keep"];
        let mut batch = store.new_batch();
        for key in keep {
            batch.hsm_put(key, b"keep")?;
        }
        batch.put_cf(RocksInodeStore::CF_INODES, b"inode", b"native")?;
        for id in 0..113 {
            batch.hsm_put(format!("\u{3}hsm:record:{id:06}").as_bytes(), &[1; 8192])?;
        }
        batch.commit()?;
        store.db.flush(true)?;
        drop(store);
        let store = RocksInodeStore::new(conf.clone(), false)?;
        store.reset_hsm_projection()?;
        assert!(store.hsm_scan(b"\x03hsm:", None, 1)?.is_empty());
        // Point reads and bounded scans must both respect the tombstone.
        assert!(store.hsm_get(b"\x03hsm:record:000001")?.is_none());
        for key in keep {
            assert_eq!(store.hsm_get(key)?.as_deref(), Some(b"keep".as_slice()));
        }
        assert_eq!(
            store
                .db
                .get_cf(RocksInodeStore::CF_INODES, b"inode")?
                .as_deref(),
            Some(b"native".as_slice())
        );
        // Idempotent interrupted recovery starts over, then committed replay
        // may reuse precisely the same keys without resurrecting old rows.
        store.reset_hsm_projection()?;
        let mut batch = store.new_batch();
        batch.hsm_put(b"\x03hsm:record:000001", b"committed")?;
        batch.hsm_put(b"\x03hsm:namespace:v1", b"restored")?;
        batch.commit()?;
        store.db.flush(true)?;
        drop(store);
        let store = RocksInodeStore::new(conf, false)?;
        assert_eq!(
            store.hsm_get(b"\x03hsm:record:000001")?.as_deref(),
            Some(b"committed".as_slice())
        );
        assert!(store.hsm_get(b"\x03hsm:record:000002")?.is_none());
        assert_eq!(store.hsm_scan(b"\x03hsm:", None, 256)?.len(), 2);
        Ok(())
    }

    #[test]
    fn reused_location_iterator_does_not_expand_corrupt_sst_scope() {
        use std::io::{Read, Seek, SeekFrom, Write};
        for compact in [false, true] {
            let mut conf = DBConf::new(Utils::test_sub_dir(format!(
                "location-corrupt-neighbor-{}",
                Utils::rand_str(8)
            )));
            conf.disable_block_cache = true;
            let store = RocksInodeStore::new(conf.clone(), true).unwrap();
            for id in [100, 5000, 10000] {
                let mut batch = store.new_batch();
                batch
                    .add_location(id, &BlockLocation::new(100, StorageType::Disk))
                    .unwrap();
                batch.commit().unwrap();
                store.db.flush(true).unwrap();
                if compact {
                    store.db.get_db().compact_range_cf(
                        store.db.cf(RocksInodeStore::CF_BLOCK).unwrap(),
                        Some(RocksUtils::i64_to_bytes(id)),
                        Some(RocksUtils::i64_to_bytes(id + 1)),
                    );
                }
            }
            // Keep the corrupt key in the iterator's range, but in a separate
            // SST from both requested prefixes.
            let bad_key = RocksUtils::i64_u32_to_bytes(5000, 100);
            let live = store.db.get_db().live_files().unwrap();
            let bad = live
                .iter()
                .find(|file| {
                    file.column_family_name == RocksInodeStore::CF_BLOCK
                        && file.start_key.as_deref() == Some(bad_key.as_slice())
                        && file.end_key.as_deref() == Some(bad_key.as_slice())
                })
                .unwrap();
            let bad_level = bad.level;
            if compact {
                assert!(bad_level > 0, "test must exercise a non-L0 SST");
            }
            let bad_path = store
                .db
                .get_db()
                .path()
                .join(bad.name.trim_start_matches('/'));
            drop(store);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&bad_path)
                .unwrap();
            let mut byte = [0u8];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0x80;
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let reopened = RocksInodeStore::new(conf, false).unwrap();
            assert!(
                reopened.get_locations(5000).is_err(),
                "fault must be detected"
            );
            let outside_a = reopened.get_locations(100);
            let outside_b = reopened.get_locations(101);
            let outside = reopened.get_locations_many([100, 101]);
            assert_eq!(
                outside.is_ok(),
                outside_a.is_ok() && outside_b.is_ok(),
                "corruption beyond the upper bound must preserve per-block error scope"
            );
            let a = reopened.get_locations(100);
            let b = reopened.get_locations(10000);
            let baseline_ok = a.is_ok() && b.is_ok();
            assert!(
                reopened.get_locations_many([100, 5000]).is_err(),
                "requested SST corruption must propagate"
            );
            LOCATION_FALLBACKS.with(|count| count.set(0));
            let candidate = reopened.get_locations_many([100, 10000]);
            assert_eq!(
                candidate.is_ok(),
                baseline_ok,
                "corruption between requested blocks must preserve per-block error scope"
            );
            if compact {
                assert!(baseline_ok, "compacted good SSTs must remain readable");
                assert!(
                    LOCATION_FALLBACKS.with(|count| count.get()) > 0,
                    "the in-range corrupt SST must exercise the per-block fallback"
                );
            }
            let workers = |list: &[BlockLocation]| {
                list.iter()
                    .map(|x| (x.worker_id, x.storage_type))
                    .collect::<Vec<_>>()
            };
            if let Ok(outside) = outside {
                assert_eq!(workers(&outside[&100]), workers(&outside_a.unwrap()));
                assert_eq!(workers(&outside[&101]), workers(&outside_b.unwrap()));
            }
            if let Ok(candidate) = candidate {
                assert_eq!(workers(&candidate[&100]), workers(&a.unwrap()));
                assert_eq!(workers(&candidate[&10000]), workers(&b.unwrap()));
            }
        }
    }

    #[test]
    fn reused_location_iterator_matches_prefix_queries() {
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "location-iterator-{}",
            Utils::rand_str(8)
        )));
        let store = RocksInodeStore::new(conf, true).unwrap();
        let stored = [i64::MIN, -42, -1, 0, 1, 8192, i64::MAX];
        let mut batch = store.new_batch();
        for (i, id) in stored.iter().enumerate() {
            batch
                .add_location(*id, &BlockLocation::new(100, StorageType::Ssd))
                .unwrap();
            if i % 2 == 0 {
                batch
                    .add_location(*id, &BlockLocation::new(101, StorageType::Disk))
                    .unwrap();
            }
        }
        batch.commit().unwrap();
        let requested = [8192, -42, -1, 2, i64::MAX, i64::MIN, 0, 1, 8192];
        for flush in [false, true] {
            if flush {
                store.db.flush(true).unwrap();
            }
            let actual = store.get_locations_many(requested).unwrap();
            for id in requested {
                let expected = store.get_locations(id).unwrap();
                let pairs = |v: &[BlockLocation]| {
                    v.iter()
                        .map(|x| (x.worker_id, x.storage_type))
                        .collect::<Vec<_>>()
                };
                assert_eq!(pairs(&actual[&id]), pairs(&expected));
            }
        }
        assert!(store.get_locations_many([]).unwrap().is_empty());
        store.delete_locations(100).unwrap();
        let actual = store.get_locations_many(requested).unwrap();
        for id in requested {
            let expected = store.get_locations(id).unwrap();
            assert_eq!(
                actual[&id].iter().map(|x| x.worker_id).collect::<Vec<_>>(),
                expected.iter().map(|x| x.worker_id).collect::<Vec<_>>()
            );
        }
        // Corrupt a requested value: the candidate must propagate decode failure.
        store
            .db
            .put_cf(
                RocksInodeStore::CF_BLOCK,
                RocksUtils::i64_u32_to_bytes(2, 101),
                [0xff],
            )
            .unwrap();
        assert!(store.get_locations(2).is_err());
        assert!(store.get_locations_many([2]).is_err());
    }

    #[test]
    fn reused_location_iterator_skips_no_trailing_tombstones() {
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "location-trailing-tombstones-{}",
            Utils::rand_str(8)
        )));
        let store = RocksInodeStore::new(conf, true).unwrap();
        let block_id = |inode: i64, seq: i64| (inode << 20) | seq;
        let requested = (0..4).map(|seq| block_id(1000, seq)).collect::<Vec<_>>();
        let mut batch = store.new_batch();
        for id in &requested {
            batch
                .add_location(*id, &BlockLocation::new(100, StorageType::Disk))
                .unwrap();
        }
        for seq in 0..1024 {
            batch
                .add_location(
                    block_id(1001, seq),
                    &BlockLocation::new(100, StorageType::Disk),
                )
                .unwrap();
        }
        batch
            .add_location(
                block_id(1002, 0),
                &BlockLocation::new(100, StorageType::Disk),
            )
            .unwrap();
        batch.commit().unwrap();
        let mut batch = store.new_batch();
        for seq in 0..1024 {
            batch.delete_location(block_id(1001, seq), 100).unwrap();
        }
        batch.commit().unwrap();

        set_perf_stats(PerfStatsLevel::EnableCount);
        let mut perf = PerfContext::default();
        perf.reset();
        let actual = store.get_locations_many(requested.iter().copied()).unwrap();
        let skipped = perf.metric(PerfMetric::InternalDeleteSkippedCount);
        set_perf_stats(PerfStatsLevel::Disable);

        assert_eq!(actual.len(), requested.len());
        for id in &requested {
            assert_eq!(actual[id].len(), 1);
        }
        assert_eq!(skipped, 0, "iterator walked past the requested blocks");
    }
}
