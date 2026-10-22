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

use std::sync::Arc;

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::ttl::InodeTtlExecutor;
use crate::master::meta::inode::InodeView;
use crate::master::quota::eviction::evictor::Evictor;
use crate::master::quota::eviction::types::EvictPlan;
use crate::master::quota::eviction::EvictionConf;
use crate::master::quota::eviction::EvictionMode;
use crate::master::Master;
use curvine_model::FilesystemInfo;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct QuotaManager {
    eviction_conf: EvictionConf,
    fs: MasterFilesystem,
    evictor: Arc<dyn Evictor>,
    ttl_executor: RwLock<Option<InodeTtlExecutor>>,
    tx: Sender<Option<FilesystemInfo>>,
}

impl QuotaManager {
    pub fn new(
        eviction_conf: EvictionConf,
        fs: MasterFilesystem,
        evictor: Arc<dyn Evictor>,
        rt: Arc<Runtime>,
        testing: bool,
    ) -> Arc<Self> {
        let (tx, mut rx): (
            Sender<Option<FilesystemInfo>>,
            Receiver<Option<FilesystemInfo>>,
        ) = mpsc::channel(1024);

        let manager = Arc::new(QuotaManager {
            evictor,
            eviction_conf,
            fs: fs.clone(),
            ttl_executor: RwLock::new(None),
            tx,
        });

        let mgr = manager.clone();

        if !testing {
            rt.spawn(async move {
                while let Some(cluster_info) = rx.recv().await {
                    let manager = mgr.clone();
                    // Metadata scans and the OS-side completion wait must not
                    // block RPC workers or call blocking_write from async code.
                    if let Err(error) =
                        tokio::task::spawn_blocking(move || manager.handle_trigger(cluster_info))
                            .await
                    {
                        log::error!("quota eviction worker failed: {error}");
                    }
                }
            });
        }

        manager
    }

    /// Set the TTL executor after initialization (called when MountManager, UfsFactory, JobManager are ready)
    pub fn set_ttl_executor(&self, executor: InodeTtlExecutor) {
        let mut guard = self.ttl_executor.write();
        *guard = Some(executor);
    }

    fn is_eviction_enabled(&self) -> bool {
        self.eviction_conf.enable_quota_eviction
    }

    pub fn detector(&self, info: Option<FilesystemInfo>) {
        let _ = self.tx.try_send(info);
    }

    fn handle_trigger(&self, cluster_info: Option<FilesystemInfo>) {
        if !self.is_eviction_enabled() {
            return;
        }

        // Update LRU cache size metric
        let metrics = match Master::get_metrics() {
            Ok(metrics) => metrics,
            Err(e) => {
                log::warn!("cluster-evict: master metrics unavailable: {}", e);
                return;
            }
        };
        metrics
            .eviction_lru_cache_size
            .set(self.evictor.cache_size() as i64);

        let Some(info) = cluster_info else {
            log::warn!("cluster-evict: failed to fetch filesystem_info");
            return;
        };

        let curvine_used = info.fs_used;
        let curvine_quota = info.available + info.fs_used;

        if curvine_quota <= 0 {
            return;
        }

        if curvine_used <= 0 {
            log::debug!("cluster-evict: curvine_used <= 0, stopping eviction");
            return;
        }

        let Some(mut plan) = self.create_evict_plan(curvine_used, curvine_quota) else {
            log::debug!(
                "cluster-evict: no eviction needed, curvine_used={}, curvine_quota={}, usage_ratio={:.2}%", 
                curvine_used, curvine_quota, (curvine_used as f64 / curvine_quota as f64) * 100.0
            );
            return;
        };

        // Increment eviction trigger counter
        metrics.eviction_trigger_count.inc();

        log::info!(
            "cluster-evict: starting eviction, curvine_used={}, curvine_quota={}, usage_ratio={:.2}%, target_free={}",
            curvine_used, curvine_quota, (curvine_used as f64 / curvine_quota as f64) * 100.0, plan.target_free_bytes
        );

        loop {
            let step_free = plan.target_free_bytes;

            if step_free <= 0 {
                log::debug!("cluster-evict: step_free <= 0, stopping eviction");
                break;
            }

            let inode_ids = self
                .evictor
                .select_victims(self.eviction_conf.candidate_scan_page);

            if inode_ids.is_empty() {
                log::debug!("cluster-evict: no more victims available, stopping eviction");
                break;
            }

            if self.eviction_conf.dry_run {
                log::debug!(
                    "cluster-evict: dry_run=true, would process inode_ids_step={}",
                    inode_ids.len()
                );
                break;
            }

            let total_freed = self.execute_eviction(self.eviction_conf.eviction_mode, &inode_ids);
            if total_freed <= 0 {
                log::debug!("cluster-evict: candidates made no progress; retain them for later");
                break;
            }

            plan.target_free_bytes = plan.target_free_bytes.saturating_sub(total_freed);

            if plan.target_free_bytes <= 0 {
                log::info!("cluster-evict: reached target_free_bytes, stopping eviction");
                break;
            }
        }
    }

    fn create_evict_plan(&self, used: i64, quota: i64) -> Option<EvictPlan> {
        if quota <= 0 {
            return None;
        }

        let usage_ratio = used as f64 / quota as f64;
        if usage_ratio < self.eviction_conf.high_watermark {
            return None;
        }

        let target_ratio = self
            .eviction_conf
            .low_watermark
            .min(self.eviction_conf.high_watermark)
            .min(1.0);

        let target_used = (target_ratio * quota as f64) as i64;
        let target_free_bytes = (used - target_used).max(0);

        Some(EvictPlan {
            trigger_used: used,
            quota_size: quota,
            target_free_bytes,
        })
    }

    fn execute_eviction(&self, _mode: EvictionMode, inode_ids: &[i64]) -> i64 {
        let ttl_executor_guard = self.ttl_executor.read();
        let Some(ttl_executor) = ttl_executor_guard.as_ref() else {
            log::warn!("cluster-evict: ttl_executor not initialized, skipping eviction");
            return 0;
        };

        let metrics = match Master::get_metrics() {
            Ok(metrics) => metrics,
            Err(e) => {
                log::warn!("cluster-evict: master metrics unavailable: {}", e);
                return 0;
            }
        };
        let mut successfully_evicted = Vec::with_capacity(inode_ids.len());
        let mut total_bytes_freed = 0_i64;

        // Get file sizes before deletion
        let file_sizes: Vec<(i64, i64)> = {
            let fs_guard = self.fs.fs_dir.read();
            inode_ids
                .iter()
                .filter_map(|&inode_id| {
                    fs_guard
                        .store
                        .get_inode(inode_id, None)
                        .ok()
                        .flatten()
                        .and_then(|inode_view| match &inode_view {
                            InodeView::File(f) => Some((inode_id, f.len.max(0))),
                            _ => None,
                        })
                })
                .collect()
        };

        for (inode_id, file_size) in file_sizes {
            let res = ttl_executor.execute_by_id(inode_id);

            match res {
                Ok((true, _)) => {
                    successfully_evicted.push(inode_id);
                    total_bytes_freed += file_size;
                }
                Ok((false, _)) => (),
                Err(e) => {
                    log::warn!(
                        "cluster-evict: executor failed for inode_id={}, err={}",
                        inode_id,
                        e
                    );
                }
            }
        }

        if !successfully_evicted.is_empty() {
            self.evictor.remove_victims(&successfully_evicted);

            // Update metrics
            metrics
                .eviction_files_deleted
                .inc_by(successfully_evicted.len() as i64);
            metrics.eviction_bytes_freed.inc_by(total_bytes_freed);

            log::info!(
                "cluster-evict: deleted {} files, freed {} bytes",
                successfully_evicted.len(),
                total_bytes_freed
            );
        }
        total_bytes_freed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use curvine_core_error::CommonResult;
    #[test]
    fn deferred_ttl_candidates_do_not_count_as_evicted_or_advance_capacity_progress(
    ) -> CommonResult<()> {
        Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.enable_quota_eviction = true;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let status = fs.create("/not-expired", false)?;
        fs.complete_file("/not-expired", Some(status.id), 0, vec![], "", false, None)?;
        {
            let mut dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(status.id, None)?.unwrap();
            inode.as_file_mut()?.len = 4096;
            let mut batch = dir.get_rocks_store().new_batch();
            batch.write_inode(&inode)?;
            batch.commit()?;
            dir.hsm_enabled = true;
        }
        let evictor = fs.fs_dir.read().evictor.clone();
        evictor.on_access(status.id);
        let manager = QuotaManager::new(
            EvictionConf::from_conf(&conf),
            fs.clone(),
            evictor.clone(),
            Arc::new(conf.client_rpc_conf().create_runtime()),
            true,
        );
        manager.set_ttl_executor(InodeTtlExecutor::with_managers(fs.clone()));
        assert_eq!(
            manager.execute_eviction(EvictionMode::FreeFile, &[status.id]),
            0
        );
        assert!(fs.exists("/not-expired")?);
        assert!(
            evictor.select_victims(16).contains(&status.id),
            "a deferred candidate was treated as reclaimed"
        );
        Ok(())
    }
}
