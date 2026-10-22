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

use super::worker_cleanup::{MaintenanceRun, Recovery};
use crate::master::fs::MasterFilesystem;
use crate::master::quota::QuotaManager;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MasterMonitor;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use log::{error, info, warn};
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub struct HeartbeatChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
    worker_blacklist_ms: u64,
    worker_lost_ms: u64,
    replication_manager: Arc<MasterReplicationManager>,
    quota_manager: Arc<QuotaManager>,
    recovery: Arc<Mutex<Recovery>>,
}

impl HeartbeatChecker {
    pub fn new(
        fs: MasterFilesystem,
        monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        quota_manager: Arc<QuotaManager>,
    ) -> Self {
        let worker_blacklist_ms = fs.conf.worker_blacklist_interval_ms();
        let worker_lost_ms = fs.conf.worker_lost_interval_ms();
        Self {
            fs,
            monitor,
            executor,
            worker_blacklist_ms,
            worker_lost_ms,
            replication_manager,
            quota_manager,
            recovery: Default::default(),
        }
    }
}

impl LoopTask for HeartbeatChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() {
            return Ok(());
        }
        let originating_term = self
            .monitor
            .journal_term
            .load(std::sync::atomic::Ordering::Acquire);
        let durable = self.fs.fs_dir.read().hsm_enabled;
        let absence = Duration::from_millis(self.worker_lost_ms);
        let recovery_ready = !durable || self.recovery.lock().ready(originating_term, absence);

        let mut blacklisted_workers = Vec::new();
        let mut removed_workers = Vec::new();
        {
            let mut wm = self.fs.worker_manager.write();
            let workers = wm.get_last_heartbeat();
            let now = LocalTime::mills();

            for (id, last_update) in workers {
                if now > last_update + self.worker_blacklist_ms {
                    // Worker blacklist timeout
                    if let Some(worker) = wm.add_blacklist_worker(id) {
                        blacklisted_workers.push((id, worker.address, worker.last_update));
                    }
                }

                if recovery_ready && now > last_update + self.worker_lost_ms {
                    // Heartbeat timeout
                    if let Some(worker) = wm.remove_expired_worker(id) {
                        let token = wm
                            .lost_worker_token(id)
                            .expect("removed Worker has a loss token");
                        let token = if durable {
                            let identity = token.id().to_owned();
                            wm.qualify_worker_loss(id, &identity, originating_term, absence)
                        } else {
                            Some(token)
                        };
                        let Some(token) = token else {
                            continue;
                        };
                        removed_workers.push((id, worker.address, worker.last_update, token));
                    }
                }
            }
        }

        for (id, address, last_update) in blacklisted_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded blacklist timeout {} ms",
                id, address, last_update, self.worker_blacklist_ms
            );
        }

        for (id, address, last_update, token) in removed_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded lost timeout {} ms and will be removed",
                id, address, last_update, self.worker_lost_ms
            );
            // Asynchronously delete all block location data.
            let fs = self.fs.clone();
            let rm = self.replication_manager.clone();
            let Some(running) = token.try_run() else {
                continue;
            };
            let task = move || {
                let _running = running;
                let spend = TimeSpent::new();
                let cleanup = if durable {
                    fs.run_durable_worker_cleanup(id, &token, originating_term)
                        .map(|_| ())
                } else {
                    fs.delete_lost_worker_locations(id, &token, originating_term, |page| {
                        let count = page.replication_block_ids.len();
                        if let Err(error) =
                            rm.report_under_replicated_blocks(id, page.replication_block_ids)
                        {
                            error!(
                                "Errors on reporting under-replicated {count} blocks: {error:?}"
                            );
                        }
                    })
                    .map(|_| ())
                };
                if let Err(error) = cleanup {
                    warn!("lost Worker {id} cleanup stopped: {error}");
                }
                info!("Worker {} location cleanup used {} ms", id, spend.used_ms());
            };
            let res = if durable {
                self.executor.try_spawn(task)
            } else {
                self.executor.spawn(task)
            };
            if let Err(e) = &res {
                warn!("{}", e);
            }
        }

        if durable && recovery_ready {
            let busy = self.recovery.lock().busy.clone();
            if busy
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let permit = MaintenanceRun(busy);
                let recovery = self.recovery.clone();
                let mut scan = self.recovery.lock().clone();
                let fs = self.fs.clone();
                let executor = self.executor.clone();
                let rm = self.replication_manager.clone();
                let result = self.executor.try_spawn(move || {
                    let _permit = permit;
                    let result = fs.recover_worker_cleanups(
                        &mut scan,
                        originating_term,
                        absence,
                        |worker, loss| {
                            let Some(running) = loss.try_run() else {
                                return;
                            };
                            let fs = fs.clone();
                            if let Err(error) = executor.try_spawn(move || {
                                let _running = running;
                                if let Err(error) =
                                    fs.run_durable_worker_cleanup(worker, &loss, originating_term)
                                {
                                    warn!("recovered Worker {worker} cleanup deferred: {error}");
                                }
                            }) {
                                warn!("queue recovered Worker {worker} cleanup: {error}");
                            }
                        },
                    );
                    if let Err(error) = result {
                        warn!("Worker cleanup recovery deferred: {error}");
                    }
                    if let Err(error) =
                        fs.replay_worker_repairs(&mut scan, originating_term, |ids| {
                            rm.report_under_replicated_blocks(0, ids)
                        })
                    {
                        warn!("durable Worker repair replay deferred: {error}");
                    }
                    // Journal waits never hold the heartbeat checker's recovery
                    // mutex. A new term may reset its grace/cursors meanwhile.
                    recovery.lock().merge_cursors(&scan);
                });
                if let Err(error) = result {
                    warn!("queue Worker cleanup recovery: {error}");
                }
            }
        }

        if let Ok(info) = self.fs.filesystem_info() {
            self.quota_manager.detector(Some(info));
        };

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}
