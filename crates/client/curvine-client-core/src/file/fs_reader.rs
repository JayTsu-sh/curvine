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

use crate::file::{FsContext, FsReaderBuffer, ReadDetector};
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_fs_api::{Path, Reader};
use curvine_io::DataSlice;
use curvine_model::{FileBlocks, FileStatus};
use curvine_runtime::common::ByteUnit;
use log::debug;
use std::sync::Arc;

type Inner = FsReaderBuffer;

pub struct FsReader {
    inner: Option<Inner>,
    path: Path,
    closed: bool,
    hsm_fast: bool,
    hsm: Option<curvine_model::hsm::native::ReadHandle>,
    hsm_offset: u64,
    hsm_renew_at: Option<std::time::Instant>,
    hsm_deadline: Option<std::time::Instant>,
    hsm_boot_deadline: Option<u64>,
    hsm_boot_renew_at: Option<u64>,
    fs_context: Arc<FsContext>,
    chunk: DataSlice,
    chunk_size: usize,
    pos: i64,
    len: i64,
    file_blocks: FileBlocks,
    metrics: &'static crate::ClientMetrics,
}

impl FsReader {
    pub fn new(path: Path, fs_context: Arc<FsContext>, file_blocks: FileBlocks) -> FsResult<Self> {
        Self::new0(path, fs_context, file_blocks, false)
    }

    fn new0(
        path: Path,
        fs_context: Arc<FsContext>,
        file_blocks: FileBlocks,
        fast: bool,
    ) -> FsResult<Self> {
        let chunk_size = fs_context.read_chunk_size();
        let len = file_blocks.status.len;
        let conf = &fs_context.conf.client;

        let read_detector = ReadDetector::with_conf(conf, len);

        debug!(
            "Create reader, path={}, len={}, blocks={}, chunk_size={}, chunk_number={}, read_parallel={}, slice_size={}, read_ahead={}-{}",
            file_blocks.status.path,
            ByteUnit::byte_to_string(len as u64),
            file_blocks.block_locs.len(),
            chunk_size,
            conf.read_chunk_num,
            read_detector.read_parallel(),
            conf.read_slice_size,
            conf.enable_read_ahead,
            conf.read_ahead_len
        );

        let inner = FsReaderBuffer::new_with_policy(
            path.clone(),
            fs_context.clone(),
            file_blocks.clone(),
            read_detector,
            fast,
        )?;
        let metrics = FsContext::get_metrics();
        let reader = Self {
            inner: Some(inner),
            path,
            closed: false,
            hsm_fast: fast,
            hsm: None,
            hsm_offset: 0,
            hsm_renew_at: None,
            hsm_deadline: None,
            hsm_boot_deadline: None,
            hsm_boot_renew_at: None,
            fs_context,
            chunk: DataSlice::Empty,
            chunk_size,
            pos: 0,
            len,
            file_blocks,
            metrics,
        };
        Ok(reader)
    }

    pub fn new_managed(
        path: Path,
        context: Arc<FsContext>,
        handle: curvine_model::hsm::native::ReadHandle,
        deadline: std::time::Instant,
    ) -> FsResult<Self> {
        let mut reader = Self::new(path, context, handle.blocks.clone())?;
        reader.hsm_deadline = Some(deadline);
        reader.hsm_renew_at =
            Some((std::time::Instant::now() + std::time::Duration::from_secs(30)).min(deadline));
        reader.hsm = Some(handle);
        Ok(reader)
    }

    pub(crate) fn new_managed_fast(
        path: Path,
        context: Arc<FsContext>,
        handle: curvine_model::hsm::native::ReadHandle,
        deadline: super::hsm_io::LeaseDeadline,
    ) -> FsResult<Self> {
        let mut reader = Self::new0(path, context, handle.blocks.clone(), true)?;
        reader.hsm_deadline = Some(deadline.instant);
        reader.hsm_renew_at = Some(
            (std::time::Instant::now() + std::time::Duration::from_secs(30)).min(deadline.instant),
        );
        reader.hsm_boot_deadline = deadline.boot_ms;
        let now = curvine_sys::lease_time_millis()
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        reader.hsm_boot_renew_at = deadline
            .boot_ms
            .map(|deadline| now.saturating_add(30_000).min(deadline));
        reader.hsm = Some(handle);
        Ok(reader)
    }
    async fn renew_hsm(&mut self) -> FsResult<()> {
        let now = std::time::Instant::now();
        let boot_now = self.check_fast_expiry()?;
        if !self.hsm_fast && self.hsm_deadline.is_some_and(|deadline| now >= deadline) {
            return err_box!("HSM read authorization expired before data access");
        }
        let renew = if self.hsm_fast {
            self.hsm_boot_renew_at
                .is_some_and(|at| boot_now.unwrap() >= at)
        } else {
            self.hsm_renew_at.is_some_and(|renew| now >= renew)
        };
        if renew {
            if let Some(handle) = &self.hsm {
                let deadline = super::hsm_io::reader_control_mode(
                    self.fs_context.clone(),
                    handle,
                    "read_renew",
                    self.hsm_fast,
                )
                .await?
                .ok_or_else(|| curvine_error::FsError::common("missing renewed read deadline"))?;
                self.hsm_deadline = Some(deadline.instant);
                self.hsm_renew_at = Some(
                    (std::time::Instant::now() + std::time::Duration::from_secs(30))
                        .min(deadline.instant),
                );
                self.hsm_boot_deadline = deadline.boot_ms;
                if self.hsm_fast {
                    let now = self.check_fast_expiry()?.unwrap();
                    self.hsm_boot_renew_at = deadline
                        .boot_ms
                        .map(|deadline| now.saturating_add(30_000).min(deadline));
                }
            }
        }
        Ok(())
    }

    fn check_fast_expiry(&self) -> FsResult<Option<u64>> {
        if !self.hsm_fast {
            return Ok(None);
        }
        let now = curvine_sys::lease_time_millis()
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        if self
            .hsm_boot_deadline
            .is_none_or(|deadline| now >= deadline)
        {
            return err_box!("HSM read authorization expired before data access");
        }
        Ok(Some(now))
    }
    pub fn file_blocks(&self) -> &FileBlocks {
        &self.file_blocks
    }
}

impl Reader for FsReader {
    fn status(&self) -> &FileStatus {
        &self.file_blocks.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.len
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        if self.closed {
            return err_box!("reader is complete");
        }
        self.renew_hsm().await?;
        if let Some(endpoint) = self.hsm.as_ref().and_then(|h| h.endpoint.as_ref()) {
            use curvine_model::hsm::native::{WorkerReply, WorkerRequest, CHUNK_BYTES};
            let response = super::hsm_io::hsm_worker_rpc(
                self.fs_context.rpc_conf(),
                &endpoint.worker,
                &WorkerRequest::Read {
                    endpoint: endpoint.clone(),
                    offset: self.hsm_offset,
                    length: CHUNK_BYTES,
                },
            )
            .await
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
            let WorkerReply::Bytes(bytes) = response else {
                return err_box!("invalid managed read reply");
            };
            if bytes.is_empty() && self.hsm_offset < self.len as u64 {
                return err_box!("unexpected EOF in managed file");
            }
            self.hsm_offset += bytes.len() as u64;
            self.check_fast_expiry()?;
            return Ok(DataSlice::bytes(bytes.into()));
        }
        let result = self
            .metrics
            .track_read(self.inner.as_mut().unwrap().read())
            .await?;
        self.check_fast_expiry()?;
        Ok(result)
    }

    async fn read_chunk(&mut self, len: Option<usize>) -> FsResult<DataSlice> {
        if self.closed {
            return err_box!("reader is complete");
        }
        // Cached chunks also cross the caller Interface after host resume.
        if self.hsm_fast {
            self.renew_hsm().await?;
        }
        if self.chunk.is_empty() {
            self.chunk = self.read_chunk0().await?;
        }
        self.check_fast_expiry()?;
        let read_len = len.map_or(self.chunk.len(), |len| len.min(self.chunk.len()));
        Ok(self.chunk.split_to(read_len))
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if self.closed {
            return err_box!("reader is complete");
        }
        if self.hsm.as_ref().is_some_and(|h| h.endpoint.is_some()) {
            if pos < 0 {
                return err_box!("negative managed read offset");
            }
            if pos != self.pos {
                self.chunk.clear();
                self.hsm_offset = pos as u64;
                self.pos = pos;
            }
            return Ok(());
        }
        if pos < 0 {
            return err_box!("Cannot seek to negative offset");
        } else if self.pos == pos {
            return Ok(());
        }

        let to_skip = pos - self.pos;
        if to_skip >= 0 && to_skip <= self.chunk.len() as i64 {
            self.chunk.advance(to_skip as usize);
        } else {
            self.chunk.clear();
            self.inner.as_mut().unwrap().seek(pos).await?;
        }

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        if self.closed {
            return Ok(());
        }
        self.inner.as_mut().unwrap().complete().await?;
        self.inner.take();
        self.chunk.clear();
        self.closed = true;
        if let Some(handle) = self.hsm.take() {
            if self.hsm_fast {
                self.fs_context
                    .enqueue_hsm_read_close(handle.data, handle.reader)
                    .await?;
            } else {
                super::hsm_io::reader_control(self.fs_context.clone(), &handle, "read_close")
                    .await?;
            }
        }
        Ok(())
    }
}

impl Drop for FsReader {
    fn drop(&mut self) {
        debug!("Close reader, path={}", self.path());
        if let Some(handle) = self.hsm.take() {
            if self.hsm_fast {
                // Fast readers own no detached prefetch or local blocking I/O.
                // Dispose their remote sessions before publishing the close.
                self.inner.take();
                self.chunk.clear();
                self.fs_context
                    .try_enqueue_hsm_read_close(handle.data, handle.reader);
                return;
            }
            let context = self.fs_context.clone();
            use curvine_runtime::runtime::RpcRuntime;
            context.clone_runtime().spawn(async move {
                let _ = super::hsm_io::reader_control(context, &handle, "read_close").await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::hsm::native::ReadHandle;
    use curvine_runtime::runtime::RpcRuntime;

    #[test]
    fn expired_fast_lease_rejects_already_buffered_bytes() {
        let Ok(now) = curvine_sys::lease_time_millis() else {
            return;
        };
        let mut conf = curvine_config::ClusterConf::default();
        conf.client.init().unwrap();
        let context = Arc::new(FsContext::new(conf).unwrap());
        let rt = context.clone_runtime();
        let handle = ReadHandle {
            blocks: FileBlocks::new(
                FileStatus {
                    len: 4096,
                    ..Default::default()
                },
                vec![],
            ),
            data: serde_json::from_value(serde_json::json!({
                "entity":{"storage":"s","namespace":"n","entity":"1"},"generation":1,
            }))
            .unwrap(),
            reader: "a".repeat(32),
            endpoint: None,
            granted_ms: 100,
            expires_ms: 119_100,
        };
        let mut reader = FsReader::new_managed_fast(
            Path::from_str("/cached").unwrap(),
            context,
            handle,
            super::super::hsm_io::LeaseDeadline {
                instant: std::time::Instant::now() + std::time::Duration::from_secs(119),
                boot_ms: Some(now.saturating_add(119_000)),
            },
        )
        .unwrap();
        reader.chunk = DataSlice::bytes(bytes::Bytes::from_static(b"cached"));
        // Model resume beyond the boot deadline while Instant remains valid.
        reader.hsm_boot_deadline = Some(0);
        assert!(rt.block_on(reader.read_chunk(Some(1))).is_err());
        assert_eq!(reader.chunk.len(), 6);
        // This test creates no real server grant requiring advisory cleanup.
        reader.hsm.take();
    }
}
