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

use crate::master::journal::{JournalBatch, JournalEntry};
use crate::master::{Master, MasterMetrics};
use curvine_config::JournalConf;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::channel::BlockingReceiver;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub struct SenderTask {
    pub(crate) client: RaftClient,
    pub(crate) hsm_waiters: super::HsmWaiters,
    pub(crate) batch: JournalBatch,
    pub(crate) flush_batch_ms: u64,
    pub(crate) flush_batch_size: u64,
    pub(crate) last_flush_ms: u64,
    pub(crate) metrics: &'static MasterMetrics,
}

impl SenderTask {
    pub fn new(client: RaftClient, conf: &JournalConf, batch_seq_id: u64) -> CommonResult<Self> {
        let sender = Self {
            client,
            hsm_waiters: Default::default(),
            batch: JournalBatch::new(batch_seq_id),
            flush_batch_ms: conf.writer_flush_batch_ms,
            flush_batch_size: conf.writer_flush_batch_size,
            last_flush_ms: LocalTime::mills(),
            metrics: Master::get_metrics()?,
        };

        Ok(sender)
    }

    // Start a thread to execute sender task
    pub fn spawn(self, receiver: BlockingReceiver<JournalEntry>) -> FsResult<()> {
        let poll = Duration::from_millis(self.flush_batch_ms);
        let name = "journal-writer".to_string();
        let task = self;
        thread::Builder::new().name(name.clone()).spawn(move || {
            if let Err(e) = Self::loop0(receiver, poll, task) {
                log::error!("thread {} stop: {:?}; aborting master", name, e);
                std::process::abort();
            }
        })?;
        Ok(())
    }

    fn loop0(
        receiver: BlockingReceiver<JournalEntry>,
        poll: Duration,
        mut task: SenderTask,
    ) -> FsResult<()> {
        // The thread should not end.
        loop {
            let event = match receiver.recv_timeout(poll) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    task.flush_pending()?;
                    return Ok(());
                }
            };
            task.handle(event)?;
        }
    }

    pub fn handle(&mut self, entry: Option<JournalEntry>) -> FsResult<()> {
        let force = if let Some(v) = entry {
            if matches!(v, JournalEntry::Hsm(_)) {
                self.flush_pending()?;
            }
            let force = matches!(v, JournalEntry::Snapshot(_) | JournalEntry::Hsm(_));
            self.batch.push(v);
            self.metrics.journal_queue_len.dec();
            force
        } else {
            false
        };

        let len = self.batch.len() as u64;
        if len == 0 {
            return Ok(());
        }

        if force || len >= self.flush_batch_size || self.flush_interval_elapsed() {
            self.flush_pending()?;
        }

        Ok(())
    }

    fn flush_interval_elapsed(&self) -> bool {
        LocalTime::mills() - self.last_flush_ms >= self.flush_batch_ms
    }

    fn flush_pending(&mut self) -> FsResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let spend = TimeSpent::new();

        let bytes = SerdeUtils::serialize(&self.batch)?;
        let tickets: Vec<_> = self
            .batch
            .batch
            .iter()
            .filter_map(|e| match e {
                JournalEntry::Hsm(e) => Some(e.ticket.clone()),
                _ => None,
            })
            .collect();
        if tickets.is_empty() {
            self.client.block_on_send_propose(bytes)?;
        } else {
            let result = self.client.block_on_send_propose_response(bytes);
            let ack = match &result {
                Ok(reply) if reply.applied_index.is_some_and(|index| index > 0) => Ok(()),
                Ok(_) => Err(
                    "proposal returned no committed apply index; query original outcome"
                        .to_string(),
                ),
                Err(e) => Err(format!("proposal outcome unknown: {}", e)),
            };
            for ticket in tickets {
                if let Some((_, tx)) = self.hsm_waiters.remove(&ticket) {
                    let _ = tx.send(ack.clone());
                }
            }
            result?;
        }

        self.metrics.journal_flush_count.inc();
        self.metrics
            .journal_flush_time
            .inc_by(spend.used_us() as i64);

        // Scroll to the next batch.
        self.batch.next();
        self.last_flush_ms = LocalTime::mills();

        Ok(())
    }
}
