// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Release coordination only for effects authorized by committed immutable
//! reservations/retirement facts. Requalify before touching authority again.
use crate::master::authority_gate::{AuthorityGate, AuthorityGuard, Class};
use crate::master::fs::MasterFilesystem;
use curvine_model::hsm::native::*;
use std::{future::Future, sync::Arc};

pub(super) struct AuthorityTurn {
    gate: Arc<AuthorityGate>,
    guard: Option<AuthorityGuard>,
    class: Class,
    shared: bool,
    invalidated: bool,
}
impl AuthorityTurn {
    pub(super) async fn acquire(fs: &MasterFilesystem, class: Class, shared: bool) -> Self {
        let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
        let guard = gate.enter(class, shared).await;
        Self {
            gate,
            class,
            shared,
            guard: Some(guard),
            invalidated: false,
        }
    }
    pub(super) fn shared(&self) -> bool {
        self.shared
    }

    /// Business rejections still use the exclusive path. Drop/reenter and
    /// requalify, then the caller MUST re-plan the whole request from scratch.
    pub(super) async fn exclusive(
        &mut self,
        fs: &MasterFilesystem,
        namespace: &Namespace,
    ) -> HsmResult<()> {
        assert!(self.shared && !self.invalidated);
        self.shared = false;
        self.external(fs, namespace, async { Ok(()) }).await
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
        self.guard = Some(self.gate.enter(self.class, self.shared).await);
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
        if !self.shared {
            fs.hsm_before_stable_underheld().await?;
        }
        self.invalidated = false;
        result
    }
}
