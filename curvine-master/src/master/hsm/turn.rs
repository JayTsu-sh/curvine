// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Release coordination only for effects authorized by committed immutable
//! reservations/retirement facts. Requalify before touching authority again.
use crate::master::fs::MasterFilesystem;
use curvine_model::hsm::native::*;
use std::{future::Future, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};

pub(super) struct AuthorityTurn {
    gate: Arc<Mutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
    invalidated: bool,
}
impl AuthorityTurn {
    pub(super) async fn acquire(fs: &MasterFilesystem) -> Self {
        let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
        let guard = gate.clone().lock_owned().await;
        Self {
            gate,
            guard: Some(guard),
            invalidated: false,
        }
    }
    pub(super) fn lost_qualification(&self) -> bool {
        self.invalidated
    }

    pub(super) async fn external<T>(
        &mut self,
        fs: &MasterFilesystem,
        namespace: &Namespace,
        effect: impl Future<Output = HsmResult<T>>,
    ) -> HsmResult<T> {
        assert!(
            self.guard.is_some() && !self.invalidated,
            "unqualified authority effect"
        );
        self.invalidated = true;
        drop(self.guard.take());
        let started = std::time::Instant::now();
        let result = effect.await;
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_stage_duration
                .with_label_values(&["hsm_control", "worker_io"])
                .observe(started.elapsed().as_micros() as f64);
        }
        let waiting = std::time::Instant::now();
        self.guard = Some(self.gate.clone().lock_owned().await);
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .hsm_stage_duration
                .with_label_values(&["hsm_control", "reenter"])
                .observe(waiting.elapsed().as_micros() as f64);
        }
        if !fs.master_monitor.is_active() {
            return Err(HsmError::Busy("leader changed during Worker I/O".into()));
        }
        // No unconfirmed metadata submission spans the released interval.
        // Failed requalification is retryable and cannot publish an old plan.
        fs.hsm_read_barrier().await.map_err(|error| {
            HsmError::Busy(format!(
                "metadata prefix unavailable after Worker I/O: {error}"
            ))
        })?;
        let current = fs.fs_dir.read().hsm_namespace().map_err(super::io)?;
        if !current.is_some_and(|current| {
            current.storage == namespace.storage
                && current.incarnation == namespace.incarnation
                && current.epoch == namespace.epoch
                && current.root_inode == namespace.root_inode
                && current.ordinary_format == namespace.ordinary_format
        }) {
            return Err(HsmError::Fenced);
        }
        self.invalidated = false;
        result
    }
}
