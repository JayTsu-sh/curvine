//! Opt-in, isolated-test namespace prefill. Not a production RPC or a measure
//! of ordinary-client ingestion throughput. Uses normal create/complete hooks
//! and waits for replicated commits at bounded batch boundaries.
use super::*;

impl MasterFilesystem {
    /// Pause only periodic access persistence to exercise uncheckpointed grants.
    pub fn hsm_test_pause_access_checkpoints(&self, paused: bool) {
        self.fs_dir
            .read()
            .hsm_read_protection
            .pause_checkpoints(paused);
    }

    /// Build empty files for scale tests under a fresh, reserved test prefix.
    /// Call only while the test cluster has no application clients/controllers.
    pub async fn hsm_test_prefill(&self, root: &str, files: usize) -> HsmResult<Value> {
        let path = curvine_fs_api::Path::from_str(root).map_err(io)?;
        if !(1..=1_000_000).contains(&files)
            || !root.starts_with("/__hsm_scale_fixture__/")
            || root.ends_with('/')
            || path.path() != root
        {
            return Err(HsmError::Invalid("invalid isolated scale fixture".into()));
        }
        if !self.master_monitor.is_active() || !self.fs_dir.read().hsm_enabled {
            return Err(HsmError::Unsupported(
                "active HSM test Master required".into(),
            ));
        }
        let initialized = self
            .hsm_control(HsmControlRequest {
                protocol_version: 1,
                payload: pack(serde_json::json!({"op":"namespace"})).map_err(io)?,
            })
            .await;
        let reply: Value = decode(&initialized.payload).map_err(io)?;
        if reply["kind"] != "namespace" {
            return Err(HsmError::Unknown(
                "failed to initialize scale namespace".into(),
            ));
        }
        let gate = self.fs_dir.read().hsm_mutation_gate.clone();
        let _serial = gate.write().await;
        if self.exists(root).map_err(io)? {
            return Err(HsmError::Conflict(
                "scale fixture prefix already exists".into(),
            ));
        }
        let started = std::time::Instant::now();
        self.mkdir(root, true).map_err(io)?;
        for first in (0..files).step_by(1000) {
            if !self.master_monitor.is_active() {
                return Err(HsmError::Unknown(
                    "leadership changed during scale prefill".into(),
                ));
            }
            let dir = format!("{root}/part-{:06}", first / 1000);
            self.mkdir(&dir, false).map_err(io)?;
            for id in first..(first + 1000).min(files) {
                let file = format!("{dir}/item-{id:012}");
                let status = self.create(file.as_str(), false).map_err(io)?;
                self.complete_file(file.as_str(), Some(status.id), 0, vec![], "", false, None)
                    .map_err(io)?;
            }
            // No HSM control/selection can interleave speculative metadata.
            // Queue and follower lag are bounded by this 1,000-file cohort;
            // each committed journal batch still uses the regular sender/apply.
            self.hsm_read_barrier().await?;
            log::info!(
                "HSM_SCALE_PREFILL files={} elapsed_ms={}",
                (first + 1000).min(files),
                started.elapsed().as_millis()
            );
        }
        if !self.master_monitor.is_active() {
            return Err(HsmError::Unknown(
                "leadership changed before scale fixture publication".into(),
            ));
        }
        Ok(
            serde_json::json!({"mode":"isolated-internal-batch-prefill","files":files,"prefix":root,
            "files_per_batch":1000,"elapsed_ms":started.elapsed().as_millis(),"normal_client_ingestion_benchmark":false}),
        )
    }
}
