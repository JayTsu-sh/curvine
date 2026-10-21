// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Immutable managed-file authority: committed native records and outcomes.
use crate::master::{
    fs::MasterFilesystem,
    journal::HsmEntry,
    meta::{
        inode::{InodeDir, InodeFile, InodePath, InodeView, ROOT_INODE_ID},
        FsDir,
    },
};
use curvine_client_core::file::hsm_io::hsm_worker_rpc;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::hsm::{native::*, HsmCapabilities, HSM_MAX_CONTROL_BYTES};
use curvine_proto::{HsmControlRequest, HsmControlResponse};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// Validate the native format before opening or formatting any database.
pub fn validate_boot(conf: &curvine_config::ClusterConf) -> curvine_error::FsResult<()> {
    let marker = std::path::Path::new(&conf.master.meta_dir).join("hsm-format-v1");
    if marker.exists() && !conf.master.hsm_enabled {
        return err_box!("native HSM volume requires hsm_enabled=true");
    }
    if conf.master.hsm_enabled
        && (conf.testing
            || !conf.journal.enable
            || conf.journal.writer_channel_size != 0
            || conf.journal.leader_creates_snapshot())
    {
        return err_box!("HSM requires testing=false, journal.enable=true, writer_channel_size=0 and leader_create_snapshot=false");
    }
    Ok(())
}

const LIFECYCLE_MAGIC: &[u8] = b"hsm-lifecycle-v1\n";
const NS: &[u8] = b"\x03hsm:namespace:v1";
fn record_key(id: i64) -> Vec<u8> {
    format!("\u{3}hsm:record:{id}").into_bytes()
}
fn operation_key(id: &str, step: &str) -> String {
    serde_json::to_string(&(id, step)).expect("serializable key")
}
fn binding_key(id: &str) -> Vec<u8> {
    format!("\u{3}hsm:binding:{}", blake3::hash(id.as_bytes()).to_hex()).into_bytes()
}
fn outcome_key(key: &str) -> Vec<u8> {
    format!("\u{3}hsm:outcome:{}", blake3::hash(key.as_bytes()).to_hex()).into_bytes()
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> CommonResult<T> {
    Ok(serde_json::from_slice(bytes)?)
}
fn field<T: DeserializeOwned>(request: &Value, key: &str) -> HsmResult<T> {
    serde_json::from_value(
        request
            .get(key)
            .cloned()
            .ok_or_else(|| HsmError::Invalid(format!("missing {key}")))?,
    )
    .map_err(|e| HsmError::Invalid(e.to_string()))
}
fn io(e: impl std::fmt::Display) -> HsmError {
    HsmError::Unknown(e.to_string())
}
fn pack(value: impl Serialize) -> CommonResult<Vec<u8>> {
    Ok(serde_json::to_vec(&value)?)
}

impl FsDir {
    pub fn hsm_record(&self, id: i64) -> CommonResult<Option<Record>> {
        self.get_rocks_store()
            .hsm_get(&record_key(id))?
            .map(|v| decode(&v))
            .transpose()
    }
    fn hsm_namespace(&self) -> CommonResult<Option<Namespace>> {
        self.get_rocks_store()
            .hsm_get(NS)?
            .map(|v| decode(&v))
            .transpose()
    }
    fn hsm_binding(&self, id: &str) -> CommonResult<Option<WorkflowBinding>> {
        self.get_rocks_store()
            .hsm_get(&binding_key(id))?
            .map(|v| decode(&v))
            .transpose()
    }
    fn hsm_outcome(&self, key: &str) -> CommonResult<Option<Outcome>> {
        self.get_rocks_store()
            .hsm_get(&outcome_key(key))?
            .map(|v| decode(&v))
            .transpose()
    }
    pub fn hsm_guard_path(&self, path: &str) -> curvine_error::FsResult<()> {
        if self.hsm_enabled {
            let path = curvine_fs_api::Path::from_str(path)?;
            if path.path() == HSM_ROOT || path.path().starts_with(&format!("{HSM_ROOT}/")) {
                return err_box!("reserved managed HSM namespace requires native control");
            }
        }
        Ok(())
    }
    pub fn apply_hsm_entry(&mut self, entry: &HsmEntry) -> CommonResult<()> {
        if !self.hsm_enabled {
            return err_box!("HSM journal requires hsm_enabled=true on every Master");
        }
        if entry.payload.is_empty() {
            if let Some((_, tx)) = self.hsm_local_apply.remove(&entry.ticket) {
                let _ = tx.send(());
            }
            return Ok(());
        }
        self.apply_hsm(&entry.payload)
    }
    pub fn hsm_guard_id(&self, id: i64) -> curvine_error::FsResult<()> {
        if self.hsm_replaying || !self.hsm_enabled {
            return Ok(());
        }
        if let Some(record) = self.hsm_record(id)? {
            if record.ordinary
                && record.native_blocks
                && record.seal_token.is_none()
                && record.views.is_empty()
                && record.pending.is_none()
                && !ordinary::readers_active(
                    &record,
                    curvine_runtime::common::LocalTime::mills() as i64,
                )
            {
                return Ok(());
            }
            return err_box!("HSM operation or active read protects this generation");
        }
        if self.hsm_namespace()?.is_some_and(|ns| ns.root_inode == id) {
            return err_box!("managed HSM inode requires the native HSM Interface");
        }
        Ok(())
    }
    pub fn hsm_guard_tree(
        &self,
        inode: crate::master::meta::inode::InodePtr,
    ) -> curvine_error::FsResult<()> {
        if self.hsm_replaying || !self.hsm_enabled {
            return Ok(());
        }
        if self.hsm_enabled && inode.id() == ROOT_INODE_ID {
            return err_box!("recursive mutation crosses the reserved HSM namespace");
        }
        let mut stack = vec![inode];
        while let Some(inode) = stack.pop() {
            self.hsm_guard_id(inode.id())?;
            if let InodeView::Dir(dir) = inode.as_ref() {
                for child in dir.children_iter() {
                    stack.push(crate::master::meta::inode::InodePtr::from_ref(child));
                }
            }
        }
        Ok(())
    }
    /// Executed for BOTH leader and follower, only from committed journal apply.
    pub fn apply_hsm(&mut self, payload: &[u8]) -> CommonResult<()> {
        let tx: Transaction = decode(payload.strip_prefix(LIFECYCLE_MAGIC).unwrap_or(payload))?;
        if self.hsm_outcome(&tx.outcome)?.is_some() {
            return Ok(());
        }
        let mut response = tx.response.clone();
        let namespace = self.hsm_namespace()?;
        let mut valid = true;
        if let Some(expected) = tx.expected_epoch {
            if namespace.as_ref().map(|n| n.epoch) != Some(expected) {
                valid = false;
                response = failure("fenced", "controller qualification changed");
            }
        }
        if let Some(id) = tx.inode {
            if self.hsm_record(id)?.as_ref().map(|r| r.version) != tx.expected_version {
                valid = false;
                response = failure("conflict", "native record changed before apply");
            }
        }
        if let Some(binding) = &tx.workflow {
            if self
                .hsm_binding(&binding.operation_id)?
                .is_some_and(|old| old != *binding)
            {
                valid = false;
                response = failure("conflict", "operation ID already bound to another workflow");
            }
        }
        for change in &tx.controls {
            if self.get_rocks_store().hsm_get(&change.key)? != change.expected {
                valid = false;
                response = failure("conflict", "lifecycle control changed before apply");
            }
        }
        for (id, version) in &tx.record_checks {
            if self.hsm_record(*id)?.as_ref().map(|r| r.version) != Some(*version) {
                valid = false;
                response = failure("conflict", "candidate changed before admission");
            }
        }
        // Reserve the inode ID in queue order even if the conditional create fails.
        if tx.create {
            if let Some(id) = tx.inode {
                self.update_last_inode_id(id)?;
            }
        }
        let mut changed_inode = None;
        let mut parent = None;
        if valid && tx.create {
            let record = tx.record.as_ref().ok_or("create without record")?;
            let inp = InodePath::resolve(self.root_ptr(), &record.path, &self.store)?;
            if inp.is_full() || inp.existing_len() + 1 != inp.len() {
                valid = false;
                response = failure("conflict", "create target exists or parent is missing");
            } else {
                let mut parent_inode = inp
                    .get_inode(inp.existing_len() as i32 - 1)
                    .ok_or("missing parent")?;
                let mut file = InodeFile::new(tx.inode.unwrap(), tx.time);
                file.parent_id = parent_inode.id();
                file.len = record.state.length as i64;
                file.features.add_attr(MANAGED_ATTR, b"1");
                file.features.set_writing("hsm-import".into());
                let child = InodeView::new_file(inp.name().to_string(), file);
                // The namespace write lock excludes all ordinary mutations here.
                let child = parent_inode.add_child(child)?;
                parent_inode.update_mtime(tx.time);
                changed_inode = Some(child);
                parent = Some(parent_inode);
            }
        }
        if valid && tx.expected_epoch.is_none() && namespace.is_none() {
            if let Some(ns) = &tx.namespace {
                let inp = InodePath::resolve(self.root_ptr(), HSM_ROOT, &self.store)?;
                if inp.is_full() {
                    valid = false;
                    response = failure("conflict", "HSM requires a fresh reserved namespace");
                } else {
                    self.update_last_inode_id(ns.root_inode)?;
                    let mut root = self.root_ptr();
                    let mut dir = InodeDir::new(ns.root_inode, tx.time);
                    dir.features
                        .x_attr
                        .insert(MANAGED_ATTR.into(), b"namespace-v1".to_vec());
                    let child = root.add_child(InodeView::new_dir(
                        HSM_ROOT.trim_start_matches('/').into(),
                        dir,
                    ))?;
                    root.update_mtime(tx.time);
                    changed_inode = Some(child);
                    parent = Some(root);
                }
            }
        }
        if valid && !tx.create {
            if let Some(record) = &tx.record {
                if record.ordinary {
                    let inp = InodePath::resolve(self.root_ptr(), &record.path, &self.store)?;
                    let mut inode = inp.task_last().ok_or("ordinary inode missing")?;
                    inode.as_file_mut()?.atime = record.atime_ms;
                    changed_inode = Some(inode);
                }
                if record.published.is_some() {
                    let inp = InodePath::resolve(self.root_ptr(), &record.path, &self.store)?;
                    // Transfer the owning pointer out of InodePath. Cloning it
                    // would leave an alias dangling when the path is dropped.
                    let mut inode = inp
                        .task_last()
                        .ok_or("managed inode missing during publication")?;
                    if inode.as_file_ref()?.features.file_write.is_some() {
                        inode.as_file_mut()?.features.file_write = None;
                        changed_inode = Some(inode);
                    }
                }
            }
        }
        if valid
            && tx.record.as_ref().is_some_and(|r| {
                r.ordinary
                    && r.state.residency == Residency::Offline
                    && !r.native_garbage.is_empty()
            })
        {
            let record = tx.record.as_ref().unwrap();
            let inp = InodePath::resolve(self.root_ptr(), &record.path, &self.store)?;
            let mut inode = inp.task_last().ok_or("release inode missing")?;
            inode.as_file_mut()?.blocks.clear();
            changed_inode = Some(inode);
        }
        let mut batch = self.get_rocks_store().new_batch();
        if let Some(binding) = &tx.workflow {
            if self.hsm_binding(&binding.operation_id)?.is_none() {
                batch.hsm_put(&binding_key(&binding.operation_id), &pack(binding)?)?;
            }
        }
        if valid {
            if let Some(namespace) = &tx.namespace {
                if tx.expected_epoch.is_none() {
                    if let Some(current) = self.hsm_namespace()? {
                        response = reply("namespace", current);
                    } else {
                        batch.hsm_put(NS, &pack(namespace)?)?;
                    }
                } else {
                    batch.hsm_put(NS, &pack(namespace)?)?;
                }
            }
            if let Some(record) = &tx.record {
                let mut record = record.clone();
                let old = self.hsm_record(tx.inode.unwrap())?;
                if record.published.is_some() && old.as_ref().is_none_or(|r| r.published.is_none())
                    || old
                        .as_ref()
                        .is_some_and(|r| r.state.residency != record.state.residency)
                {
                    record.state_since_ms = tx.time;
                }
                ordinary::write_record(&mut batch, old.as_ref(), &record)?;
            }
            for change in &tx.controls {
                if change.key.starts_with(b"\x03hsm:intent:") {
                    if let Some(value) = &change.value {
                        let intent: LifecycleIntent = decode(value)?;
                        let key = lifecycle::work_key(&intent);
                        let reserved =
                            format!("\u{3}hsm:reserved:online:{}", intent.id).into_bytes();
                        if matches!(intent.status, IntentStatus::Ready | IntentStatus::Running)
                            && !matches!(intent.action, ManagedAction::Recall(_))
                        {
                            if self.get_rocks_store().hsm_get(&reserved)?.is_none() {
                                let id: i64 = intent.expected.data.entity.entity.parse()?;
                                let copies = if self
                                    .hsm_record(id)?
                                    .is_some_and(|r| r.native_blocks)
                                {
                                    self.store
                                        .get_inode(id, None)?
                                        .and_then(|i| {
                                            i.as_file_ref().ok().map(|f| f.replicas.max(1) as u64)
                                        })
                                        .unwrap_or(1)
                                } else {
                                    1
                                };
                                batch.hsm_put(
                                    &reserved,
                                    &pack(intent.expected.length.saturating_mul(copies))?,
                                )?;
                            }
                        } else {
                            batch.hsm_delete(&reserved)?;
                        }
                        if matches!(intent.status, IntentStatus::Ready | IntentStatus::Running) {
                            batch.hsm_put(&key, &pack(&intent.id)?)?;
                        } else {
                            batch.hsm_delete(&key)?;
                        }
                    }
                }
                match &change.value {
                    Some(value) => batch.hsm_put(&change.key, value)?,
                    None => batch.hsm_delete(&change.key)?,
                }
            }
            if let Some(child) = &changed_inode {
                batch.write_inode(child.as_ref())?;
            }
            if let Some(parent) = &parent {
                batch.write_inode(parent.as_ref())?;
                let child = changed_inode.as_ref().unwrap();
                batch.add_child(parent.id(), child.name(), child.id())?;
            }
        }
        let outcome = Outcome {
            fingerprint: tx.fingerprint,
            response,
            data: tx.data,
            action: tx.action,
        };
        batch.hsm_put(&outcome_key(&tx.outcome), &pack(outcome)?)?;
        batch.commit()?;
        if valid && tx.create {
            self.store.fs_stats.increment_file_count();
        }
        if valid && tx.expected_epoch.is_none() && namespace.is_none() && tx.namespace.is_some() {
            self.store.fs_stats.increment_dir_count();
            self.hsm_enroll_pending()?;
        }
        Ok(())
    }
}

struct Plan {
    tx: Transaction,
    allocate: Option<Layout>,
    gc: Option<EntityKey>,
}
impl MasterFilesystem {
    pub(crate) async fn hsm_read_barrier(&self) -> HsmResult<()> {
        let ticket = token();
        let (writer, op_id, local) = {
            let fs = self.fs_dir.read();
            (
                fs.journal_writer.clone(),
                fs.next_op_id(),
                fs.hsm_local_apply.clone(),
            )
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        local.insert(ticket.clone(), tx);
        let _cleanup = BarrierWait {
            ticket: ticket.clone(),
            waiters: local,
        };
        let remote = writer
            .submit_hsm(HsmEntry {
                op_id,
                ticket,
                payload: vec![],
            })
            .map_err(io)?;
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            remote.await.map_err(io)?.map_err(HsmError::Unknown)?;
            rx.await.map_err(io)
        })
        .await
        .map_err(|_| HsmError::Unknown("HSM read barrier outcome unknown".into()))??;
        Ok(())
    }
    async fn hsm_submit(&self, tx: &Transaction) -> HsmResult<Value> {
        let (writer, op_id) = {
            let fs = self.fs_dir.read();
            (fs.journal_writer.clone(), fs.next_op_id())
        };
        let entry = HsmEntry {
            op_id,
            ticket: token(),
            payload: {
                let bytes = pack(tx).map_err(io)?;
                if tx.controls.is_empty() {
                    bytes
                } else {
                    [LIFECYCLE_MAGIC, bytes.as_slice()].concat()
                }
            },
        };
        let waiter = writer.submit_hsm(entry).map_err(io)?;
        let ack = tokio::time::timeout(std::time::Duration::from_secs(60), waiter)
            .await
            .map_err(|_| HsmError::Unknown("HSM committed apply timeout; query outcome".into()))?
            .map_err(io)?;
        ack.map_err(HsmError::Unknown)?;
        let outcome = self
            .fs_dir
            .read()
            .hsm_outcome(&tx.outcome)
            .map_err(io)?
            .ok_or_else(|| {
                HsmError::Unknown("committed outcome not visible on this leader".into())
            })?;
        if outcome.fingerprint != tx.fingerprint {
            return Err(HsmError::Conflict(
                "operation step already committed with different input".into(),
            ));
        }
        Ok(outcome.response)
    }
    async fn hsm_namespace_init(&self) -> HsmResult<Namespace> {
        let current = self.fs_dir.read().hsm_namespace().map_err(io)?;
        if let Some(namespace) = current {
            if namespace.ordinary_format < 2 {
                let mut upgraded = namespace.clone();
                upgraded.ordinary_format = 2;
                let tx = Transaction {
                    controls: vec![],
                    record_checks: vec![],
                    workflow: None,
                    outcome: format!("namespace:ordinary-format:2:{}", namespace.storage),
                    fingerprint: "ordinary-format-2".into(),
                    response: reply("namespace", &upgraded),
                    inode: None,
                    expected_version: None,
                    record: None,
                    namespace: Some(upgraded),
                    expected_epoch: Some(namespace.epoch),
                    create: false,
                    time: curvine_runtime::common::LocalTime::mills() as i64,
                    data: None,
                    action: None,
                };
                let response = self.hsm_submit(&tx).await?;
                return field(&response, "data");
            }
            return Ok(namespace);
        }
        let root_inode = self.fs_dir.read().inode_id.next().map_err(io)?;
        let ns = Namespace {
            ordinary_format: 2,
            root_inode,
            storage: token(),
            incarnation: token(),
            epoch: 1,
        };
        let tx = Transaction {
            controls: vec![],
            record_checks: vec![],
            workflow: None,
            outcome: format!("namespace:init:{}", token()),
            fingerprint: "init-v1".into(),
            response: reply("namespace", &ns),
            inode: None,
            expected_version: None,
            record: None,
            namespace: Some(ns),
            expected_epoch: None,
            create: false,
            time: curvine_runtime::common::LocalTime::mills() as i64,
            data: None,
            action: None,
        };
        let response = self.hsm_submit(&tx).await?;
        field(&response, "data")
    }
    pub async fn hsm_control(&self, request: HsmControlRequest) -> HsmControlResponse {
        let enabled = self.fs_dir.read().hsm_enabled;
        if !enabled
            || request.protocol_version != 1
            || request.payload.len() > HSM_MAX_CONTROL_BYTES
        {
            return curvine_model::hsm::control_response(request);
        }
        // Normal metadata RPCs use this same gate until their journal barrier completes.
        // This prevents speculative leader metadata from changing a committed HSM CAS.
        let gate = self.fs_dir.read().hsm_mutation_gate.clone();
        let _serial = gate.lock().await;
        let value = match serde_json::from_slice::<Value>(&request.payload) {
            Ok(value) => self.hsm_execute(value).await.unwrap_or_else(error_value),
            Err(_) => failure("invalid", "invalid HSM request"),
        };
        HsmControlResponse {
            protocol_version: 1,
            payload: value.to_string().into_bytes(),
        }
    }
    async fn hsm_execute(&self, request: Value) -> HsmResult<Value> {
        let op = request["op"]
            .as_str()
            .ok_or_else(|| HsmError::Invalid("missing operation".into()))?;
        if op == "capabilities" {
            return Ok(reply(
                "capabilities",
                HsmCapabilities {
                    native_records: true,
                    stable_views: true,
                    conditional_release: true,
                    private_recall: true,
                    durable_success_results: true,
                    distributed_authority: self.fs_dir.read().hsm_distributed,
                    capacity_reservation: false,
                    transparent_recall: true,
                },
            ));
        }
        self.hsm_read_barrier().await?;
        let namespace = self.hsm_namespace_init().await?;
        if matches!(
            op,
            "read_path" | "read_close" | "read_renew" | "seal_begin" | "seal_commit"
        ) {
            return self.hsm_ordinary(&namespace, &request).await;
        }
        if op == "namespace" {
            return Ok(reply("namespace", namespace));
        }
        if op == "inspect" || op == "resolve" {
            let fs = self.fs_dir.read();
            let id = if op == "resolve" {
                let path: String = field(&request, "path")?;
                let inp = InodePath::resolve(fs.root_ptr(), path, &fs.store).map_err(io)?;
                inp.get_last_inode()
                    .ok_or_else(|| HsmError::NotFound("managed path".into()))?
                    .id()
            } else {
                let entity: EntityKey = field(&request, "entity")?;
                entity_id(&entity, &namespace)?
            };
            let record = fs
                .hsm_record(id)
                .map_err(io)?
                .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
            if record.state.residency == Residency::Online
                && record.published.is_none()
                && !record.native_blocks
            {
                return Err(HsmError::Busy("managed import is private".into()));
            }
            return Ok(reply("entity", record.state));
        }
        if op == "view_blocks" {
            let capability: Vec<u8> = field(&request, "capability")?;
            let reference: Value = decode(&capability).map_err(io)?;
            let data: DataKey = field(&reference, "data")?;
            let operation: String = field(&reference, "operation_id")?;
            let view_id: String = field(&reference, "view_id")?;
            let fs = self.fs_dir.read();
            let record = fs
                .hsm_record(entity_id(&data.entity, &namespace)?)
                .map_err(io)?
                .ok_or_else(|| HsmError::NotFound("view entity".into()))?;
            let view = record.views.get(&operation).ok_or(HsmError::Fenced)?;
            if !record.native_blocks
                || record.state.data != data
                || view.data != data
                || view.view_id != view_id
                || view.source.adapter != "curvine-block-view-v2"
                || view.source.capability != capability
            {
                return Err(HsmError::Fenced);
            }
            let inp = InodePath::resolve(fs.root_ptr(), &record.path, &fs.store).map_err(io)?;
            return Ok(reply(
                "blocks",
                self.get_file_blocks(&record.path, &fs, &inp).map_err(io)?,
            ));
        }
        if op == "pending_seals" {
            let page = self
                .fs_dir
                .read()
                .get_rocks_store()
                .hsm_scan(b"\x03hsm:seal:", None, 16)
                .map_err(io)?;
            let items: Vec<DataKey> = page
                .iter()
                .map(|(_, bytes)| decode(bytes).map_err(io))
                .collect::<HsmResult<_>>()?;
            return Ok(reply("seals", items));
        }
        if op == "gc_batch" {
            let page = self
                .fs_dir
                .read()
                .get_rocks_store()
                .hsm_scan(b"\x03hsm:gc:", None, 16)
                .map_err(io)?;
            let mut processed = 0;
            for (_, bytes) in page {
                let entity: EntityKey = decode(&bytes).map_err(io)?;
                self.hsm_gc(&namespace, &entity).await?;
                processed += 1;
            }
            return Ok(reply("gc", processed));
        }
        if op == "collect_garbage" {
            let entity: EntityKey = field(&request, "entity")?;
            self.hsm_gc(&namespace, &entity).await?;
            let fs = self.fs_dir.read();
            let record = fs
                .hsm_record(entity_id(&entity, &namespace)?)
                .map_err(io)?
                .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
            return Ok(reply("entity", record.state));
        }
        let context: OperationContext = field(&request, "context")?;
        context.validate()?;
        if op == "claim_controller" {
            let key = operation_key(&context.operation_id, op);
            if let Some(outcome) = self.fs_dir.read().hsm_outcome(&key).map_err(io)? {
                if outcome.fingerprint != request_fingerprint(&request)? {
                    return Err(HsmError::Conflict("controller claim rebound".into()));
                }
                return Ok(outcome.response);
            }
        }
        if context.controller_epoch != namespace.epoch {
            return Err(HsmError::Fenced);
        }
        if context.attempt_epoch != 1 {
            return Err(HsmError::Unsupported(
                "first native workflow requires attempt epoch 1".into(),
            ));
        }
        if op.starts_with("policy_") || op.starts_with("intent_") {
            return self.hsm_lifecycle(&namespace, &context, &request).await;
        }
        if op == "completed_import" {
            let old = self
                .fs_dir
                .read()
                .hsm_outcome(&operation_key(&context.operation_id, "commit_import"))
                .map_err(io)?;
            if let Some(outcome) = old {
                if outcome.response["kind"] == "error" {
                    return Ok(outcome.response);
                }
                let state: EntityState = field(&outcome.response, "data")?;
                let path: String = field(&request, "path")?;
                let length: u64 = field(&request, "length")?;
                let digest: ContentDigest = field(&request, "digest")?;
                let record = self
                    .fs_dir
                    .read()
                    .hsm_record(entity_id(&state.data.entity, &namespace)?)
                    .map_err(io)?
                    .ok_or_else(|| HsmError::NotFound("managed import".into()))?;
                if record.path != path || state.length != length || state.digest != digest {
                    return Err(HsmError::Conflict("import operation rebound".into()));
                }
                return Ok(reply("completed", state));
            }
            return Ok(reply("completed", Value::Null));
        }
        if matches!(op, "pending_recall" | "pending_view") {
            let entity: EntityKey = field(&request, "entity")?;
            let record = self
                .fs_dir
                .read()
                .hsm_record(entity_id(&entity, &namespace)?)
                .map_err(io)?
                .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
            if op == "pending_recall" {
                let (owner, target, _) = record
                    .pending
                    .ok_or_else(|| HsmError::NotFound("private Recall".into()))?;
                if owner.operation_id != context.operation_id {
                    return Err(HsmError::Conflict(
                        "different workflow owns private layout".into(),
                    ));
                }
                return Ok(reply("recall", target));
            }
            let view = record
                .views
                .get(&context.operation_id)
                .ok_or_else(|| HsmError::NotFound("source view".into()))?;
            return Ok(reply("view", view));
        }
        if op == "completed" {
            let data: DataKey = field(&request, "data")?;
            entity_id(&data.entity, &namespace)?;
            let action: ManagedAction = field(&request, "action")?;
            let step = match action {
                ManagedAction::Archive(_) => "commit_archive",
                ManagedAction::Release => "release",
                ManagedAction::Recall(_) => "commit_recall",
            };
            let old = self
                .fs_dir
                .read()
                .hsm_outcome(&operation_key(&context.operation_id, step))
                .map_err(io)?;
            if let Some(outcome) = old {
                if outcome.data.as_ref() != Some(&data) || outcome.action.as_ref() != Some(&action)
                {
                    return Err(HsmError::Conflict(
                        "operation ID has different input".into(),
                    ));
                }
                if outcome.response["kind"] == "error" {
                    return Ok(outcome.response);
                }
                return Ok(reply("completed", outcome.response["data"].clone()));
            }
            return Ok(reply("completed", Value::Null));
        }
        let existing = self
            .fs_dir
            .read()
            .hsm_binding(&context.operation_id)
            .map_err(io)?;
        if let Some(binding) = &existing {
            if op != "close_view" && binding.kind != workflow_kind(op) {
                return Err(HsmError::Conflict(
                    "operation ID belongs to a different workflow kind".into(),
                ));
            }
        }
        let key = operation_key(&context.operation_id, op);
        let fingerprint = request_fingerprint(&request)?;
        let old = self.fs_dir.read().hsm_outcome(&key).map_err(io)?;
        if let Some(outcome) = old {
            if outcome.fingerprint != fingerprint {
                return Err(HsmError::Conflict(
                    "operation step already bound to different input".into(),
                ));
            }
            // Preparation records survive a lost response before Worker allocation.
            if matches!(op, "prepare_import" | "prepare_recall")
                && outcome.response["kind"] != "error"
            {
                let target: RecallTarget = field(&outcome.response, "data")?;
                let record = self
                    .fs_dir
                    .read()
                    .hsm_record(entity_id(&target.data.entity, &namespace)?)
                    .map_err(io)?
                    .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
                if let Some((_, _, layout)) = &record.pending {
                    self.allocate(layout).await?;
                }
            }
            return Ok(outcome.response);
        }
        // Definite seal failures are durable outcomes; network uncertainty is not.
        let preflight: HsmResult<()> = async {
            if matches!(op, "commit_import" | "commit_recall") {
                let target: RecallTarget = field(&request, "target")?;
                let record = self
                    .fs_dir
                    .read()
                    .hsm_record(entity_id(&target.data.entity, &namespace)?)
                    .map_err(io)?
                    .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
                if let Some((owner, pending, layout)) = &record.pending {
                    if !same_attempt(owner, &context) || pending != &target {
                        return Err(HsmError::Fenced);
                    }
                    let evidence: TransferEvidence = field(&request, "evidence")?;
                    if evidence.length != record.state.length
                        || evidence.digest != record.state.digest
                    {
                        return Err(HsmError::Integrity(
                            "transfer evidence differs from native generation".into(),
                        ));
                    }
                    match hsm_worker_rpc(
                        &self.hsm_rpc_conf(),
                        &layout.worker,
                        &WorkerRequest::Seal {
                            allocation: layout.id.clone(),
                            admin: layout.admin.clone(),
                        },
                    )
                    .await?
                    {
                        WorkerReply::Sealed(actual) if actual == evidence => (),
                        _ => {
                            return Err(HsmError::Integrity(
                                "Worker did not seal exact Recall content".into(),
                            ))
                        }
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = preflight {
            if matches!(
                error,
                HsmError::Unknown(_) | HsmError::Io(_) | HsmError::Busy(_)
            ) {
                return Err(error);
            }
            return self
                .hsm_submit(&rejected(&namespace, &request, &key, &fingerprint, error))
                .await;
        }
        let plan = self.hsm_plan(&namespace, &context, &request, &key, &fingerprint);
        let plan = match plan {
            Ok(plan) => plan,
            Err(e)
                if matches!(
                    e,
                    HsmError::Unknown(_) | HsmError::Io(_) | HsmError::Busy(_)
                ) =>
            {
                return Err(e)
            }
            Err(e) => Plan {
                tx: rejected(&namespace, &request, &key, &fingerprint, e),
                allocate: None,
                gc: None,
            },
        };
        let response = self.hsm_submit(&plan.tx).await?;
        if response["kind"] != "error" {
            if plan.allocate.is_some() {
                let target: RecallTarget = field(&response, "data")?;
                let record = self
                    .fs_dir
                    .read()
                    .hsm_record(entity_id(&target.data.entity, &namespace)?)
                    .map_err(io)?
                    .ok_or_else(|| HsmError::NotFound("managed allocation".into()))?;
                if let Some((_, pending, layout)) = &record.pending {
                    if pending == &target {
                        self.allocate(layout).await?;
                    }
                }
            }
            // Cleanup failure leaves the committed GC outbox, never rolls back Release.
            if let Some(entity) = &plan.gc {
                let _ = self.hsm_gc(&namespace, entity).await;
            }
        }
        Ok(response)
    }
    fn hsm_rpc_conf(&self) -> curvine_rpc::client::ClientConf {
        self.fs_dir.read().hsm_rpc_conf.clone()
    }
    async fn allocate(&self, layout: &Layout) -> HsmResult<()> {
        match hsm_worker_rpc(
            &self.hsm_rpc_conf(),
            &layout.worker,
            &WorkerRequest::Allocate {
                layout: layout.clone(),
            },
        )
        .await?
        {
            WorkerReply::Done => Ok(()),
            _ => Err(HsmError::Unknown(
                "Worker allocation not acknowledged".into(),
            )),
        }
    }
    fn hsm_plan(
        &self,
        namespace: &Namespace,
        ctx: &OperationContext,
        request: &Value,
        key: &str,
        fingerprint: &str,
    ) -> HsmResult<Plan> {
        let op = request["op"].as_str().unwrap();
        let fs = self.fs_dir.read();
        let mut tx = Transaction {
            controls: vec![],
            record_checks: vec![],
            workflow: None,
            outcome: key.into(),
            fingerprint: fingerprint.into(),
            response: Value::Null,
            inode: None,
            expected_version: None,
            record: None,
            namespace: None,
            expected_epoch: Some(namespace.epoch),
            create: false,
            time: curvine_runtime::common::LocalTime::mills() as i64,
            data: None,
            action: None,
        };
        if op == "claim_controller" {
            let expected: u64 = field(request, "expected_epoch")?;
            if expected != namespace.epoch {
                return Err(HsmError::Fenced);
            }
            let mut next = namespace.clone();
            next.epoch = next
                .epoch
                .checked_add(1)
                .ok_or_else(|| HsmError::Conflict("epoch exhausted".into()))?;
            tx.workflow = Some(WorkflowBinding {
                operation_id: ctx.operation_id.clone(),
                kind: "controller".into(),
                data: None,
            });
            tx.response = reply("epoch", next.epoch);
            tx.namespace = Some(next);
            return Ok(Plan {
                tx,
                allocate: None,
                gc: None,
            });
        }
        let mut allocate = None;
        let mut record = if op == "prepare_import" {
            let path: String = field(request, "path")?;
            if !path
                .strip_prefix(&format!("{HSM_ROOT}/"))
                .is_some_and(|name| !name.is_empty() && !name.contains('/'))
            {
                return Err(HsmError::Unsupported(
                    "v1 import requires a direct child of /.hsm".into(),
                ));
            }

            let name = path.rsplit('/').next().unwrap_or("");
            if matches!(name, "." | "..") || name.contains('\0') || name.len() > 255 {
                return Err(HsmError::Invalid("invalid managed file name".into()));
            }
            let length: u64 = field(request, "length")?;
            let digest: ContentDigest = field(request, "digest")?;
            if length > i64::MAX as u64 || digest.algorithm != "blake3" || digest.value.len() != 32
            {
                return Err(HsmError::Invalid("unsupported managed content".into()));
            }
            let inp = InodePath::resolve(fs.root_ptr(), &path, &fs.store).map_err(io)?;
            if inp.is_full() || inp.existing_len() + 1 != inp.len() {
                return Err(HsmError::Conflict(
                    "create-only import requires an existing parent".into(),
                ));
            }
            let worker = self
                .filesystem_info()
                .map_err(io)?
                .live_workers
                .first()
                .ok_or_else(|| HsmError::Busy("no live Worker".into()))?
                .address
                .clone();
            let id = fs.inode_id.next().map_err(io)?;
            let data = DataKey {
                entity: EntityKey {
                    storage: namespace.storage.clone(),
                    namespace: namespace.incarnation.clone(),
                    entity: id.to_string(),
                },
                generation: 1,
            };
            let layout = Layout::new(worker, length, digest.clone());
            let target = RecallTarget {
                data: data.clone(),
                layout_id: layout.id.clone(),
                archive: ArchiveKey {
                    backend: "native-import".into(),
                    object: ctx.operation_id.clone(),
                },
                destination: layout.endpoint(true),
            };
            tx.create = true;
            tx.inode = Some(id);
            tx.response = reply("recall", &target);
            allocate = Some(layout.clone());
            Record {
                ordinary: false,
                native_blocks: false,
                sealed: true,
                seal_token: None,
                atime_ms: tx.time,
                mtime_ms: tx.time,
                owner: String::new(),
                group: String::new(),
                writing: false,
                readers: Default::default(),
                native_garbage: Default::default(),
                state_since_ms: 0,
                version: 1,
                state: EntityState {
                    data,
                    revision: 1,
                    residency: Residency::Online,
                    length,
                    digest,
                    anchors: vec![],
                },
                path,
                published: None,
                pending: Some((ctx.clone(), target, layout)),
                views: Default::default(),
                garbage: vec![],
            }
        } else {
            let data: DataKey = match op {
                "prepare_archive" | "open_view" => field(request, "data")?,
                "commit_archive" | "close_view" => field::<DataView>(request, "view")?.data,
                "release" | "prepare_recall" => field::<EntityState>(request, "expected")?.data,
                "commit_import" | "commit_recall" | "abort_recall" => {
                    field::<RecallTarget>(request, "target")?.data
                }
                _ => return Err(HsmError::Unsupported(format!("native HSM operation {op}"))),
            };
            let id = entity_id(&data.entity, namespace)?;
            let mut record = fs
                .hsm_record(id)
                .map_err(io)?
                .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
            record.path = fs.get_inode_path(id).map_err(io)?;
            if record.state.data != data {
                return Err(HsmError::Conflict("content generation changed".into()));
            }
            tx.inode = Some(id);
            tx.expected_version = Some(record.version);
            record
        };
        match op {
            "prepare_import" => (),
            "commit_import" | "commit_recall" => {
                let target: RecallTarget = field(request, "target")?;
                let (owner, pending, layout) = record
                    .pending
                    .take()
                    .ok_or_else(|| HsmError::Conflict("no private layout".into()))?;
                if !same_attempt(&owner, ctx) || pending != target {
                    return Err(HsmError::Fenced);
                }
                if op == "commit_recall" {
                    tx.action = Some(ManagedAction::Recall(target.archive.clone()));
                    record.state.residency = Residency::Dual;
                }
                record.published = Some(layout);
                record.state.revision += 1;
                tx.response = reply("entity", &record.state);
                tx.data = Some(record.state.data.clone());
            }
            "prepare_archive" | "open_view" => {
                if record.pending.is_some() {
                    return Err(HsmError::Busy("private import/Recall active".into()));
                }
                if !record.sealed || record.writing {
                    return Err(HsmError::Busy("source has no sealed generation".into()));
                }
                let view_id = token();
                let source = if record.native_blocks {
                    DataEndpoint {
                        adapter: "curvine-block-view-v2".into(),
                        capability: pack(&serde_json::json!({"data":record.state.data,"operation_id":ctx.operation_id,"view_id":view_id})).map_err(io)?,
                    }
                } else {
                    record
                        .published
                        .as_ref()
                        .ok_or_else(|| HsmError::Busy("entity is offline; Recall required".into()))?
                        .endpoint(false)
                };
                let view = DataView {
                    data: record.state.data.clone(),
                    view_id,
                    length: record.state.length,
                    digest: record.state.digest.clone(),
                    source,
                };
                record.views.insert(ctx.operation_id.clone(), view.clone());
                tx.response = reply("view", view);
            }
            "commit_archive" | "close_view" => {
                let view: DataView = field(request, "view")?;
                if record.views.get(&ctx.operation_id) != Some(&view) {
                    return Err(HsmError::Fenced);
                }
                if op == "commit_archive" {
                    let receipt: ArchiveReceipt = field(request, "receipt")?;
                    if receipt.data != record.state.data
                        || receipt.length != record.state.length
                        || receipt.digest != record.state.digest
                        || receipt.key.backend.is_empty()
                        || receipt.key.object.is_empty()
                        || receipt.receipt_id.is_empty()
                        || receipt.durability_profile.is_empty()
                    {
                        return Err(HsmError::Integrity(
                            "archive receipt differs from native view".into(),
                        ));
                    }
                    if record
                        .state
                        .anchors
                        .iter()
                        .any(|a| a.key == receipt.key && a != &receipt)
                    {
                        return Err(HsmError::Conflict("archive key rebound".into()));
                    }
                    tx.action = Some(ManagedAction::Archive(receipt.key.clone()));
                    tx.data = Some(record.state.data.clone());
                    if !record.state.anchors.contains(&receipt) {
                        record.state.anchors.push(receipt);
                    }
                    record.state.residency = Residency::Dual;
                    record.state.revision += 1;
                }
                record.views.remove(&ctx.operation_id);
                tx.response = reply("entity", &record.state);
            }
            "release" => {
                let expected: EntityState = field(request, "expected")?;
                if expected != record.state {
                    return Err(HsmError::Conflict("Release observation changed".into()));
                }
                if record.state.anchors.is_empty() {
                    return Err(HsmError::Conflict("no committed recovery anchor".into()));
                }
                if !record.views.is_empty() || record.pending.is_some() {
                    return Err(HsmError::Busy("view or private layout active".into()));
                }
                if ordinary::readers_active(&record, tx.time)
                    || record.writing
                    || record.seal_token.is_some()
                {
                    return Err(HsmError::Busy("reader or writer protects source".into()));
                }
                if record.native_blocks {
                    let inp =
                        InodePath::resolve(fs.root_ptr(), &record.path, &fs.store).map_err(io)?;
                    let file = inp
                        .get_last_inode()
                        .ok_or_else(|| HsmError::NotFound("source inode".into()))?;
                    for (block, locations) in fs
                        .store
                        .get_file_locations(file.as_file_ref().map_err(io)?)
                        .map_err(io)?
                    {
                        record.native_garbage.insert(block, locations);
                    }
                    record.native_blocks = false;
                } else {
                    let old = record
                        .published
                        .take()
                        .ok_or_else(|| HsmError::Conflict("already offline".into()))?;
                    record.garbage.push(old);
                }
                record.state.residency = Residency::Offline;
                record.state.revision += 1;
                tx.response = reply("entity", &record.state);
                tx.data = Some(record.state.data.clone());
                tx.action = Some(ManagedAction::Release);
            }
            "abort_recall" => {
                let target: RecallTarget = field(request, "target")?;
                let (owner, pending, layout) = record
                    .pending
                    .take()
                    .ok_or_else(|| HsmError::Conflict("no private Recall".into()))?;
                if owner.operation_id != ctx.operation_id
                    || owner.attempt_epoch != ctx.attempt_epoch
                    || pending != target
                    || record.state.residency != Residency::Offline
                {
                    return Err(HsmError::Fenced);
                }
                record.garbage.push(layout);
                tx.response = reply("entity", &record.state);
            }
            "prepare_recall" => {
                let expected: EntityState = field(request, "expected")?;
                let anchor: ArchiveReceipt = field(request, "anchor")?;
                if record.state != expected || expected.residency != Residency::Offline {
                    return Err(HsmError::Conflict(
                        "Recall requires current offline state".into(),
                    ));
                }
                if !record.state.anchors.contains(&anchor) {
                    return Err(HsmError::Integrity("uncommitted Recall anchor".into()));
                }
                if record.pending.is_some() {
                    return Err(HsmError::Busy("private Recall already active".into()));
                }
                let worker = self
                    .filesystem_info()
                    .map_err(io)?
                    .live_workers
                    .first()
                    .ok_or_else(|| HsmError::Busy("no live Worker".into()))?
                    .address
                    .clone();
                let layout = Layout::new(worker, record.state.length, record.state.digest.clone());
                let target = RecallTarget {
                    data: record.state.data.clone(),
                    layout_id: layout.id.clone(),
                    archive: anchor.key,
                    destination: layout.endpoint(true),
                };
                record.pending = Some((ctx.clone(), target.clone(), layout.clone()));
                allocate = Some(layout);
                tx.response = reply("recall", target);
            }
            _ => unreachable!(),
        }
        let gc = if matches!(op, "release" | "abort_recall") {
            Some(record.state.data.entity.clone())
        } else {
            None
        };
        if !tx.create {
            record.version += 1;
        }
        let kind = if op == "close_view" {
            fs.hsm_binding(&ctx.operation_id)
                .map_err(io)?
                .map(|b| b.kind)
                .unwrap_or_else(|| "read".into())
        } else {
            workflow_kind(op).into()
        };
        tx.workflow = Some(WorkflowBinding {
            operation_id: ctx.operation_id.clone(),
            kind,
            data: Some(record.state.data.clone()),
        });
        tx.record = Some(record);
        Ok(Plan { tx, allocate, gc })
    }
    async fn hsm_gc(&self, namespace: &Namespace, entity: &EntityKey) -> HsmResult<()> {
        let id = entity_id(entity, namespace)?;
        loop {
            let mut record = self
                .fs_dir
                .read()
                .hsm_record(id)
                .map_err(io)?
                .ok_or_else(|| HsmError::NotFound("managed entity".into()))?;
            if !record.native_garbage.is_empty() {
                self.worker_manager
                    .write()
                    .remove_blocks(&crate::master::fs::DeleteResult {
                        inodes: 0,
                        bytes: 0,
                        blocks: record
                            .native_garbage
                            .iter()
                            .map(|(k, v)| (*k, v.clone()))
                            .collect(),
                    });
                let acknowledged = {
                    let fs = self.fs_dir.read();
                    record.native_garbage.keys().all(|id| {
                        fs.get_block_locations(*id)
                            .is_ok_and(|locations| locations.is_empty())
                    })
                };
                if acknowledged {
                    record.native_garbage.clear();
                    let previous = record.version;
                    record.version += 1;
                    let tx = Transaction {
                        controls: vec![],
                        record_checks: vec![],
                        workflow: None,
                        outcome: format!("native-gc-{}", token()),
                        fingerprint: "native-gc".into(),
                        response: reply("entity", &record.state),
                        inode: Some(id),
                        expected_version: Some(previous),
                        record: Some(record.clone()),
                        namespace: None,
                        expected_epoch: Some(namespace.epoch),
                        create: false,
                        time: curvine_runtime::common::LocalTime::mills() as i64,
                        data: None,
                        action: None,
                    };
                    let response = self.hsm_submit(&tx).await?;
                    if response["kind"] == "error" {
                        return Err(HsmError::Busy("GC raced a record update".into()));
                    }
                }
            }
            let Some(layout) = record.garbage.first().cloned() else {
                return Ok(());
            };
            if record.published.as_ref().is_some_and(|p| p.id == layout.id) {
                return Err(HsmError::Integrity("GC references published layout".into()));
            }
            match hsm_worker_rpc(
                &self.hsm_rpc_conf(),
                &layout.worker,
                &WorkerRequest::Delete {
                    allocation: layout.id.clone(),
                    admin: layout.admin.clone(),
                },
            )
            .await?
            {
                WorkerReply::Done => (),
                _ => return Err(HsmError::Unknown("GC not acknowledged".into())),
            }
            record.garbage.remove(0);
            let expected = record.version;
            record.version += 1;
            let tx = Transaction {
                controls: vec![],
                record_checks: vec![],
                workflow: None,
                outcome: format!("gc:{}:{}", layout.id, token()),
                fingerprint: layout.id,
                response: reply("entity", &record.state),
                inode: Some(id),
                expected_version: Some(expected),
                record: Some(record),
                namespace: None,
                expected_epoch: Some(namespace.epoch),
                create: false,
                time: 0,
                data: None,
                action: None,
            };
            let result = self.hsm_submit(&tx).await?;
            if result["kind"] == "error" {
                return Err(HsmError::Unknown(
                    "GC acknowledgement needs reconciliation".into(),
                ));
            }
        }
    }
}
fn entity_id(entity: &EntityKey, ns: &Namespace) -> HsmResult<i64> {
    if entity.storage != ns.storage || entity.namespace != ns.incarnation {
        return Err(HsmError::Conflict("wrong storage incarnation".into()));
    }
    entity
        .entity
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| HsmError::Invalid("invalid native inode".into()))
}

fn rejected(
    ns: &Namespace,
    request: &Value,
    key: &str,
    fingerprint: &str,
    error: HsmError,
) -> Transaction {
    let op = request["op"].as_str().unwrap_or("");
    let pair = match op {
        "release" => field::<EntityState>(request, "expected")
            .ok()
            .map(|s| (s.data, ManagedAction::Release)),
        "commit_archive" => field::<ArchiveReceipt>(request, "receipt")
            .ok()
            .map(|r| (r.data, ManagedAction::Archive(r.key))),
        "commit_recall" => field::<RecallTarget>(request, "target")
            .ok()
            .map(|t| (t.data, ManagedAction::Recall(t.archive))),
        _ => None,
    };
    let data = pair
        .as_ref()
        .map(|p| p.0.clone())
        .or_else(|| field::<DataKey>(request, "data").ok())
        .or_else(|| {
            field::<EntityState>(request, "expected")
                .ok()
                .map(|s| s.data)
        })
        .or_else(|| field::<DataView>(request, "view").ok().map(|v| v.data))
        .or_else(|| {
            field::<RecallTarget>(request, "target")
                .ok()
                .map(|t| t.data)
        });
    let workflow = field::<OperationContext>(request, "context")
        .ok()
        .map(|context| WorkflowBinding {
            operation_id: context.operation_id,
            kind: workflow_kind(op).into(),
            data,
        });
    Transaction {
        controls: vec![],
        record_checks: vec![],
        workflow,
        outcome: key.into(),
        fingerprint: fingerprint.into(),
        response: error_value(error),
        inode: None,
        expected_version: None,
        record: None,
        namespace: None,
        expected_epoch: Some(ns.epoch),
        create: false,
        time: 0,
        data: pair.as_ref().map(|p| p.0.clone()),
        action: pair.map(|p| p.1),
    }
}

fn workflow_kind(op: &str) -> &str {
    match op {
        "prepare_import" | "commit_import" => "import",
        "prepare_archive" | "commit_archive" => "archive",
        "open_view" | "close_view" => "read",
        "prepare_recall" | "commit_recall" | "abort_recall" => "recall",
        "claim_controller" => "controller",
        _ => op,
    }
}

fn request_fingerprint(request: &Value) -> HsmResult<String> {
    let mut canonical = request.clone();
    // Qualification fences admission/apply, but is not the logical effect identity.
    // Canonical epoch 1 also preserves fingerprints from the initial implementation.
    if let Some(context) = canonical.get_mut("context").and_then(Value::as_object_mut) {
        context.insert("controller_epoch".into(), Value::from(1));
    }
    canonical.sort_all_objects();
    Ok(blake3::hash(&serde_json::to_vec(&canonical).map_err(io)?)
        .to_hex()
        .to_string())
}

struct BarrierWait {
    ticket: String,
    waiters: std::sync::Arc<dashmap::DashMap<String, tokio::sync::oneshot::Sender<()>>>,
}
impl Drop for BarrierWait {
    fn drop(&mut self) {
        self.waiters.remove(&self.ticket);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn hsm_format_refusal_does_not_format_existing_metadata() {
        let root = tempfile::tempdir().unwrap();
        let mut conf = curvine_config::ClusterConf::format();
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        std::fs::create_dir_all(root.path().join("meta/data")).unwrap();
        std::fs::write(root.path().join("meta/hsm-format-v1"), []).unwrap();
        std::fs::write(root.path().join("meta/data/sentinel"), b"existing metadata").unwrap();
        assert!(crate::master::journal::JournalSystem::from_conf(&conf).is_err());
        assert_eq!(
            std::fs::read(root.path().join("meta/data/sentinel")).unwrap(),
            b"existing metadata"
        );
        assert!(!root.path().join("journal").exists());
    }
}

fn same_attempt(a: &OperationContext, b: &OperationContext) -> bool {
    a.operation_id == b.operation_id && a.attempt_epoch == b.attempt_epoch
}
mod lifecycle;

mod ordinary;

#[cfg(test)]
mod scale_tests;

#[cfg(feature = "hsm-scale-test")]
mod scale_fixture;
