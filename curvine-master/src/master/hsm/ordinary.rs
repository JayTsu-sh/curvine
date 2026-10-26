//! Ordinary namespace integration. Indexes are derived from authoritative records.
use super::*;
use crate::master::meta::{inode::Inode, store::InodeWriteBatch};
use curvine_model::{FileBlocks, FileType};

pub(super) fn index_key(record: &Record) -> Vec<u8> {
    access_index_key(
        &record.state.residency,
        record.atime_ms,
        &record.state.data.entity.entity,
    )
}
pub(super) fn access_index_key(residency: &Residency, atime: i64, entity: &str) -> Vec<u8> {
    let state = match residency {
        Residency::Online => 'o',
        Residency::Dual => 'd',
        Residency::Offline => 'f',
    };
    format!("\u{3}hsm:candidate:{state}:{:020}:{entity}", atime.max(0)).into_bytes()
}
pub(crate) fn write_record(
    batch: &mut InodeWriteBatch,
    old: Option<&Record>,
    record: &Record,
) -> CommonResult<()> {
    crate::master::committed::stage_record_retirements(batch, old, record)?;
    let index = index_key(record);
    let old_index = old.map(index_key);
    let id = record.state.data.entity.entity.parse()?;
    let bytes_len = batch.hsm_put_record(id, record)?;
    if let Ok(metrics) = crate::master::Master::get_metrics() {
        metrics
            .hsm_record_write_bytes
            .with_label_values(&["base"])
            .inc_by(bytes_len as i64);
    }
    let deleted_access = old.is_some_and(|old| old.access_base.is_some());
    if deleted_access {
        batch.hsm_delete(&access_key(id))?;
    }
    batch.cache_hsm_access_absence(id, deleted_access)?;
    if old_index.as_ref() != Some(&index) {
        if let Some(old_index) = old_index {
            batch.hsm_delete(&old_index)?;
        }
        batch.hsm_put(&index, &pack(&record.state.data.entity.entity)?)?;
    }
    let gc = format!("\u{3}hsm:gc:{}", record.state.data.entity.entity).into_bytes();
    let seal = format!("\u{3}hsm:seal:{}", record.state.data.entity.entity).into_bytes();
    let old_seal = old
        .filter(|r| r.seal_token.is_some() && !r.sealed)
        .map(|r| &r.state.data);
    let new_seal = (record.seal_token.is_some() && !record.sealed).then_some(&record.state.data);
    if old_seal != new_seal {
        match new_seal {
            Some(data) => batch.hsm_put(&seal, &pack(data)?)?,
            None => batch.hsm_delete(&seal)?,
        }
    }
    let old_gc = old
        .filter(|r| !r.garbage.is_empty() || !r.native_garbage.is_empty())
        .map(|r| &r.state.data.entity);
    let new_gc = (!record.garbage.is_empty() || !record.native_garbage.is_empty())
        .then_some(&record.state.data.entity);
    if old_gc != new_gc {
        match new_gc {
            Some(entity) => batch.hsm_put(&gc, &pack(entity)?)?,
            None => batch.hsm_delete(&gc)?,
        }
    }
    Ok(())
}
pub(super) fn write_access(
    batch: &mut InodeWriteBatch,
    old: &Record,
    record: &Record,
    clock_ms: i64,
) -> CommonResult<()> {
    let id = record.state.data.entity.entity.parse()?;
    let access = ReadState {
        clock_ms,
        data: record.state.data.clone(),
        base_version: old.access_base.unwrap_or(old.version),
        version: record.version,
        revision: record.state.revision,
        atime_ms: record.atime_ms,
        readers: record.readers.clone(),
    };
    let bytes_len = batch.hsm_put_access(id, &access)?;
    if let Ok(metrics) = crate::master::Master::get_metrics() {
        metrics
            .hsm_record_write_bytes
            .with_label_values(&["access"])
            .inc_by(bytes_len as i64);
    }
    let previous = index_key(old);
    let next = index_key(record);
    if previous != next {
        batch.hsm_delete(&previous)?;
        batch.hsm_put(&next, &pack(&record.state.data.entity.entity)?)?;
    }
    Ok(())
}
/// Caller has checked generation protection and pending cleanup, and includes
/// this deletion in the same atomic namespace/result batch.
pub(crate) fn forget_record(batch: &mut InodeWriteBatch, record: &Record) -> CommonResult<()> {
    let id = record.state.data.entity.entity.parse()?;
    batch.hsm_delete(&index_key(record))?;
    batch.hsm_delete(&record_key(id))?;
    batch.hsm_delete(&access_key(id))?;
    batch.hsm_delete(format!("\u{3}hsm:gc:{id}").as_bytes())?;
    batch.hsm_delete(format!("\u{3}hsm:seal:{id}").as_bytes())?;
    Ok(())
}

pub(super) fn readers_active(record: &Record, now: i64) -> bool {
    record.readers.values().any(|deadline| *deadline > now)
}
fn trackable(path: &str, file: &InodeFile) -> bool {
    path != HSM_ROOT
        && !path.starts_with("/.hsm/")
        && file.file_type == FileType::File
        && file.nlink == 1
        && !file.ufs_exists()
}

/// Build a stable HSM generation without authority writes or Worker actions.
/// Legacy hooks and owned native preparation use exactly these same rules.
pub(crate) fn native_record(
    ns: &Namespace,
    path: &str,
    file: &InodeFile,
    old: Option<&Record>,
) -> CommonResult<Option<Record>> {
    build_native_record(ns, path, file, old, false)
}
pub(crate) fn native_overwrite_record(
    ns: &Namespace,
    path: &str,
    file: &InodeFile,
    old: Option<&Record>,
) -> CommonResult<Option<Record>> {
    build_native_record(ns, path, file, old, true)
}
fn build_native_record(
    ns: &Namespace,
    path: &str,
    file: &InodeFile,
    old: Option<&Record>,
    force_generation: bool,
) -> CommonResult<Option<Record>> {
    if !trackable(path, file) {
        return Ok(None);
    }
    let modern = ns.ordinary_format >= 2;
    if old.is_some_and(|r| !r.ordinary) {
        return Ok(None);
    }
    let writing = file.features.file_write.is_some();
    let mut record = old.cloned().unwrap_or_else(|| Record {
        access_base: None,
        ordinary: true,
        native_blocks: true,
        sealed: false,
        seal_token: None,
        atime_ms: file.atime,
        mtime_ms: file.mtime,
        owner: file.features.acl.owner.clone(),
        group: file.features.acl.group.clone(),
        writing,
        readers: Default::default(),
        native_garbage: Default::default(),
        state_since_ms: file.mtime,
        version: 0,
        state: EntityState {
            data: DataKey {
                entity: EntityKey {
                    storage: ns.storage.clone(),
                    namespace: ns.incarnation.clone(),
                    entity: file.id().to_string(),
                },
                generation: 1,
            },
            revision: 0,
            residency: Residency::Online,
            length: file.len.max(0) as u64,
            digest: ContentDigest {
                algorithm: "unsealed".into(),
                value: vec![],
            },
            anchors: vec![],
        },
        path: path.into(),
        published: None,
        pending: None,
        views: Default::default(),
        garbage: vec![],
    });
    // Invalidate the archive generation at write-open, before any block changes.
    if old.is_some_and(|r| {
        force_generation
            || !r.writing
                && (writing
                    || modern
                        && (r.mtime_ms != file.mtime || r.state.length != file.len.max(0) as u64))
    }) {
        record.state.data.generation = record
            .state
            .data
            .generation
            .checked_add(1)
            .ok_or("HSM generation exhausted")?;
        record.state.anchors.clear();
        record.state.digest = ContentDigest {
            algorithm: "unsealed".into(),
            value: vec![],
        };
        if modern && record.state.residency != Residency::Online {
            record.state_since_ms = file.mtime;
        }
        record.state.residency = Residency::Online;
        record.sealed = false;
        record.seal_token = None;
        if let Some(layout) = record.published.take() {
            record.garbage.push(layout);
        }
    }
    record.native_blocks = true;
    record.writing = writing;
    record.path = path.into();
    if modern {
        record.atime_ms = record.atime_ms.max(file.atime);
    }
    record.mtime_ms = file.mtime;
    record.owner = file.features.acl.owner.clone();
    record.group = file.features.acl.group.clone();
    record.state.length = file.len.max(0) as u64;
    record.state.revision = record
        .state
        .revision
        .checked_add(1)
        .ok_or("HSM revision exhausted")?;
    record.version = record
        .version
        .checked_add(1)
        .ok_or("HSM record version exhausted")?;
    record.access_base = None;
    Ok(Some(record))
}

impl FsDir {
    /// Called for both normal journal replay and the leader's normal close path.
    pub fn hsm_track_file(&self, path: &str, file: &InodeFile) -> CommonResult<()> {
        if !self.hsm_enabled || !trackable(path, file) {
            return Ok(());
        }
        let Some(ns) = self.hsm_namespace()? else {
            let mut batch = self.get_rocks_store().new_batch();
            batch.hsm_put(
                format!("\u{3}hsm:enroll:{}", file.id()).as_bytes(),
                &pack(file.id())?,
            )?;
            return batch.commit();
        };
        let old = self.hsm_record(file.id())?;
        let Some(record) = native_record(&ns, path, file, old.as_ref())? else {
            return Ok(());
        };
        let mut batch = self.get_rocks_store().new_batch();
        write_record(&mut batch, old.as_ref(), &record)?;
        batch.hsm_delete(format!("\u{3}hsm:enroll:{}", file.id()).as_bytes())?;
        batch.commit()
    }
    /// Namespace and metadata updates advance the same authority version as policy admission.
    pub fn hsm_refresh_tree(
        &self,
        inode: crate::master::meta::inode::InodePtr,
    ) -> CommonResult<()> {
        if !self.hsm_enabled
            || self
                .hsm_namespace()?
                .is_none_or(|ns| ns.ordinary_format < 2)
        {
            return Ok(());
        }
        let mut stack = vec![inode];
        while let Some(inode) = stack.pop() {
            match inode.as_ref() {
                InodeView::File(file) => {
                    let path = self.get_inode_path(file.id())?;
                    self.hsm_track_file(&path, file)?;
                }
                InodeView::Dir(dir) => {
                    for child in dir.children_iter() {
                        stack.push(crate::master::meta::inode::InodePtr::from_ref(child));
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
    pub fn hsm_forget_tree(&self, inode: crate::master::meta::inode::InodePtr) -> CommonResult<()> {
        if !self.hsm_enabled
            || self
                .hsm_namespace()?
                .is_none_or(|ns| ns.ordinary_format < 2)
        {
            return Ok(());
        }
        let mut stack = vec![inode];
        let mut records = vec![];
        while let Some(inode) = stack.pop() {
            if let Some(record) = self.hsm_record(inode.id())? {
                if !record.garbage.is_empty() || !record.native_garbage.is_empty() {
                    return err_box!("HSM physical cleanup must complete before deletion");
                }
                records.push(record);
            }
            if let InodeView::Dir(dir) = inode.as_ref() {
                for child in dir.children_iter() {
                    stack.push(crate::master::meta::inode::InodePtr::from_ref(child));
                }
            }
        }
        let mut batch = self.get_rocks_store().new_batch();
        for record in records {
            batch.hsm_delete(&index_key(&record))?;
            batch.hsm_delete(&record_key(record.state.data.entity.entity.parse()?))?;
            if record.access_base.is_some() {
                batch.hsm_delete(&access_key(record.state.data.entity.entity.parse()?))?;
            }
        }
        batch.commit()
    }
    pub fn hsm_enroll_pending(&self) -> CommonResult<()> {
        loop {
            let page = self
                .get_rocks_store()
                .hsm_scan(b"\x03hsm:enroll:", None, 128)?;
            if page.is_empty() {
                return Ok(());
            }
            for (key, bytes) in page {
                let id: i64 = decode(&bytes)?;
                match (self.get_inode_path(id), self.store.get_inode(id, None)?) {
                    (Ok(path), Some(inode)) if inode.is_file() => {
                        self.hsm_track_file(&path, inode.as_file_ref()?)?;
                        let mut batch = self.get_rocks_store().new_batch();
                        batch.hsm_delete(&key)?;
                        batch.commit()?;
                    }
                    _ => {
                        let mut batch = self.get_rocks_store().new_batch();
                        batch.hsm_delete(&key)?;
                        batch.commit()?;
                    }
                }
            }
        }
    }
    pub fn hsm_guard_read(&self, id: i64) -> curvine_error::FsResult<()> {
        if !self.hsm_enabled {
            return Ok(());
        }
        if let Some(record) = self.hsm_record(id)? {
            if !record.native_blocks || record.state.residency == Residency::Offline {
                return err_box!("managed data requires the HSM-aware reader");
            }
        }
        Ok(())
    }
    pub fn hsm_guard_overwrite(&self, id: i64) -> curvine_error::FsResult<()> {
        if !self.hsm_enabled {
            return Ok(());
        }
        if let Some(record) = self.hsm_record(id)? {
            if record.ordinary
                && record.pending.is_none()
                && record.views.is_empty()
                && record.seal_token.is_none()
                && !self.hsm_readers_active(&record)?
            {
                return Ok(());
            }
        }
        self.hsm_guard_id(id)
    }
}
// This short-lived preparation value moves directly into the batch. Keep the
// owned transaction together rather than adding allocations before submission.
#[allow(clippy::large_enum_variant)]
pub(super) enum OrdinaryPlan {
    Immediate(Value),
    Transaction(Transaction, Option<Record>, Option<Value>),
}

impl MasterFilesystem {
    pub(super) async fn hsm_ordinary(&self, ns: &Namespace, request: &Value) -> HsmResult<Value> {
        match self.hsm_prepare_ordinary(ns, request, None)? {
            OrdinaryPlan::Immediate(value) => Ok(value),
            OrdinaryPlan::Transaction(tx, _, public) => {
                let applied =
                    if matches!(request["op"].as_str(), Some("seal_begin" | "seal_commit")) {
                        self.hsm_submit_conditional(&tx).await?
                    } else {
                        self.hsm_submit(&tx).await?
                    };
                Ok(read_grant::publish(&tx.response, applied, public.as_ref()))
            }
        }
    }
    pub(super) fn hsm_prepare_ordinary(
        &self,
        ns: &Namespace,
        request: &Value,
        projected: Option<Record>,
    ) -> HsmResult<OrdinaryPlan> {
        let op = request["op"].as_str().unwrap();
        let now = curvine_runtime::common::LocalTime::mills() as i64;
        let read_time = self.fs_dir.read().hsm_read_time().map_err(io)?;
        let lease_now = read_time.now;
        let identity = if matches!(op, "read_path" | "read_close" | "read_renew") {
            let fs = self.fs_dir.read();
            let identity = read_history::identity(&fs, request, now)?;
            if let Some(key) = &identity {
                if let Some(outcome) = fs.hsm_outcome(key).map_err(io)? {
                    if outcome.fingerprint != request_fingerprint(request)? {
                        return Err(HsmError::Conflict("read request identity rebound".into()));
                    }
                    if read_grant::is_grant(&outcome.response) {
                        let handle = read_grant::handle(&outcome.response)?;
                        let id = entity_id(&handle.data.entity, ns)?;
                        let current = match projected.as_ref() {
                            Some(record) if record.state.data.entity.entity == id.to_string() => {
                                Some(record.clone())
                            }
                            _ => fs.hsm_record(id).map_err(io)?,
                        };
                        if handle.expires_ms <= lease_now
                            || current.is_none_or(|record| {
                                record.state.data != handle.data
                                    || record
                                        .readers
                                        .get(&handle.reader)
                                        .is_none_or(|deadline| !read_time.active(*deadline))
                            })
                        {
                            return Err(HsmError::Fenced);
                        }
                    }
                    if op == "read_renew" {
                        let entity: EntityKey = field(request, "entity")?;
                        let reader: String = field(request, "reader")?;
                        let id = entity_id(&entity, ns)?;
                        let current = match projected.as_ref() {
                            Some(record) if record.state.data.entity.entity == id.to_string() => {
                                Some(record.clone())
                            }
                            _ => fs.hsm_record(id).map_err(io)?,
                        };
                        if current.is_none_or(|record| {
                            record
                                .readers
                                .get(&reader)
                                .is_none_or(|deadline| !read_time.active(*deadline))
                        }) {
                            return Err(HsmError::Fenced);
                        }
                    }
                    // Expansion resolves native placement under its own
                    // namespace guard. All cached-lease checks above are
                    // complete; do not recursively acquire this read lock.
                    drop(fs);
                    return Ok(OrdinaryPlan::Immediate(
                        self.hsm_expand_read_grant(ns, outcome.response)?,
                    ));
                }
            }
            identity
        } else {
            None
        };

        let (mut record, id) = {
            let fs = self.fs_dir.read();
            let id = if op == "read_path" {
                let path: String = field(request, "path")?;
                let inp = InodePath::resolve(fs.root_ptr(), path, &fs.store).map_err(io)?;
                let Some(inode) = inp.get_last_inode() else {
                    return Ok(OrdinaryPlan::Immediate(reply("normal", Value::Null)));
                };
                inode.id()
            } else {
                entity_id(&field::<EntityKey>(request, "entity")?, ns)?
            };
            let record = match projected {
                Some(record) => Some(record),
                None => fs.hsm_record(id).map_err(io)?,
            };
            let Some(record) = record else {
                return Ok(OrdinaryPlan::Immediate(reply("normal", Value::Null)));
            };
            (record, id)
        };
        record.path = self.fs_dir.read().get_inode_path(id).map_err(io)?;
        let previous = record.version;
        let mut controls = vec![];
        let response;
        let mut public_response = None;
        if op == "read_path" {
            let reader: String = field(request, "reader")?;
            if reader.len() != 32 || !reader.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(HsmError::Invalid("invalid read session".into()));
            }
            if record.state.residency == Residency::Offline {
                // One Recall per offline state revision, shared by concurrent readers.
                let iid = blake3::hash(
                    &pack((&record.state.data, record.state.revision, "access-recall"))
                        .map_err(io)?,
                )
                .to_hex()
                .to_string();
                let key = lifecycle::intent_key(&iid)?;
                let fs = self.fs_dir.read();
                let existing = fs.get_rocks_store().hsm_get(&key).map_err(io)?;
                if let Some(bytes) = &existing {
                    let intent: LifecycleIntent = decode(bytes).map_err(io)?;
                    if matches!(
                        intent.status,
                        IntentStatus::Failed | IntentStatus::Superseded
                    ) {
                        return Err(HsmError::Io(std::io::Error::other(format!(
                            "automatic recall failed: {}",
                            intent.last_error.unwrap_or_default()
                        ))));
                    }
                    return Ok(OrdinaryPlan::Immediate(reply("pending", iid)));
                } else {
                    let anchor =
                        record.state.anchors.first().ok_or_else(|| {
                            HsmError::Integrity("offline file has no archive".into())
                        })?;
                    let intent = LifecycleIntent {
                        id: iid.clone(),
                        policy: "__access_recall".into(),
                        policy_revision: 1,
                        version: 1,
                        expected: record.state.clone(),
                        action: ManagedAction::Recall(anchor.key.clone()),
                        context: OperationContext {
                            operation_id: format!("intent-{iid}"),
                            controller_epoch: ns.epoch,
                            attempt_epoch: 1,
                        },
                        status: IntentStatus::Ready,
                        claim: None,
                        last_error: None,
                        result: None,
                    };
                    let active = lifecycle::active_key(&record.state.data);
                    if fs.get_rocks_store().hsm_get(&active).map_err(io)?.is_none() {
                        controls.push(ControlChange {
                            key,
                            expected: None,
                            value: Some(pack(&intent).map_err(io)?),
                        });
                        controls.push(ControlChange {
                            key: active,
                            expected: None,
                            value: Some(pack(&iid).map_err(io)?),
                        });
                    }
                }
                response = reply("pending", iid);
            } else {
                if record.writing {
                    return Ok(OrdinaryPlan::Immediate(reply("normal", Value::Null)));
                }
                let mut blocks = if record.native_blocks {
                    self.get_block_locations(&record.path).map_err(io)?
                } else {
                    let fs = self.fs_dir.read();
                    let inp =
                        InodePath::resolve(fs.root_ptr(), &record.path, &fs.store).map_err(io)?;
                    FileBlocks::new(
                        inp.get_last_inode()
                            .ok_or_else(|| HsmError::NotFound("read inode".into()))?
                            .to_file_status(&record.path)
                            .map_err(io)?,
                        vec![],
                    )
                };
                let endpoint = record.published.as_ref().map(|l| Endpoint {
                    allocation: l.id.clone(),
                    worker: l.worker.clone(),
                    token: l.read.clone(),
                });
                if !record.native_blocks && endpoint.is_none() {
                    return Err(HsmError::Busy("private import".into()));
                }
                record.atime_ms = record.atime_ms.max(now);
                record.state.revision += 1;
                blocks.status.atime = record.atime_ms;
                record
                    .readers
                    .retain(|_, expiry| read_time.protection().active(*expiry));
                record
                    .readers
                    .insert(reader.clone(), lease_now + read_clock::LEASE_MS);
                (response, public_response) = read_grant::prepare(ReadHandle {
                    blocks,
                    data: record.state.data.clone(),
                    reader,
                    endpoint,
                    granted_ms: lease_now,
                    expires_ms: lease_now + read_clock::LEASE_MS,
                })?;
            }
        } else if op == "read_close" || op == "read_renew" {
            let reader: String = field(request, "reader")?;
            if let Some(data) = request.get("data") {
                let data: DataKey = serde_json::from_value(data.clone()).map_err(io)?;
                if data != record.state.data {
                    return if op == "read_close" {
                        Ok(OrdinaryPlan::Immediate(reply("done", Value::Null)))
                    } else {
                        Err(HsmError::Fenced)
                    };
                }
            }
            if op == "read_close" {
                if record
                    .readers
                    .remove(&reader)
                    .is_some_and(|deadline| read_time.active(deadline))
                    && record.state.residency != Residency::Offline
                {
                    record.atime_ms = record.atime_ms.max(now);
                    record.state.revision += 1;
                }
            } else {
                if record
                    .readers
                    .get(&reader)
                    .is_none_or(|expiry| !read_time.active(*expiry))
                {
                    return Err(HsmError::Fenced);
                }
                record.atime_ms = record.atime_ms.max(now);
                record.state.revision += 1;
                record
                    .readers
                    .insert(reader, lease_now + read_clock::LEASE_MS);
            }
            response = reply(
                "done",
                if op == "read_renew" {
                    serde_json::json!({"granted_ms":lease_now,"expires_ms":lease_now + read_clock::LEASE_MS})
                } else {
                    Value::Null
                },
            );
        } else if op == "seal_begin" {
            if !record.native_blocks || record.writing {
                return Err(HsmError::Busy(
                    "file is not closed in the native block layout".into(),
                ));
            }
            if record.sealed {
                return Ok(OrdinaryPlan::Immediate(reply("sealed", record.state)));
            }
            let requested: DataKey = field(request, "data")?;
            if requested != record.state.data {
                return Err(HsmError::Conflict("seal generation changed".into()));
            }
            let token: String = field(request, "token")?;
            if record.seal_token.as_ref().is_some_and(|old| old != &token) {
                return Err(HsmError::Busy("another seal owns file".into()));
            }
            let blocks = self.get_block_locations(&record.path).map_err(io)?;
            record.seal_token = Some(token);
            response = reply("blocks", blocks);
        } else if op == "seal_commit" {
            let token: String = field(request, "token")?;
            let data: DataKey = field(request, "data")?;
            let evidence: TransferEvidence = field(request, "evidence")?;
            if record.seal_token.as_ref() != Some(&token)
                || record.state.data != data
                || record.state.length != evidence.length
                || evidence.digest.algorithm != "blake3"
                || evidence.digest.value.len() != 32
            {
                return Err(HsmError::Conflict(
                    "seal content or generation differs".into(),
                ));
            }
            record.sealed = true;
            record.seal_token = None;
            record.state.digest = evidence.digest;
            record.state.revision += 1;
            response = reply("entity", &record.state);
        } else {
            return Err(HsmError::Unsupported(op.into()));
        }
        record.version += 1;
        let access_only = matches!(op, "read_path" | "read_close" | "read_renew");
        let read_state = access_only.then(|| ReadState {
            clock_ms: lease_now,
            data: record.state.data.clone(),
            base_version: record.access_base.unwrap_or(previous),
            version: record.version,
            revision: record.state.revision,
            atime_ms: record.atime_ms,
            readers: record.readers.clone(),
        });
        let projected = read_state.as_ref().map(|access| {
            let mut projected = record.clone();
            projected.access_base = Some(access.base_version);
            projected
        });
        let tx = Transaction {
            scan_progress_only: false,
            read_state,
            controls,
            record_checks: vec![],
            workflow: None,
            outcome: identity.unwrap_or_else(|| format!("ordinary-{op}-{}", token())),
            fingerprint: request_fingerprint(request)?,
            response,
            inode: Some(id),
            expected_version: Some(previous),
            record: (!access_only).then_some(record),
            namespace: None,
            expected_epoch: Some(ns.epoch),
            create: false,
            time: now,
            data: None,
            action: None,
        };
        Ok(OrdinaryPlan::Transaction(tx, projected, public_response))
    }
}
