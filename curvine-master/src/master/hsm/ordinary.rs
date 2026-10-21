//! Ordinary namespace integration. Indexes are derived from authoritative records.
use super::*;
use crate::master::meta::{inode::Inode, store::InodeWriteBatch};
use curvine_model::{FileBlocks, FileType};

pub(super) fn index_key(record: &Record) -> Vec<u8> {
    let state = match record.state.residency {
        Residency::Online => 'o',
        Residency::Dual => 'd',
        Residency::Offline => 'f',
    };
    format!(
        "\u{3}hsm:candidate:{state}:{:020}:{}",
        record.atime_ms.max(0),
        record.state.data.entity.entity
    )
    .into_bytes()
}
pub(super) fn write_record(
    batch: &mut InodeWriteBatch,
    old: Option<&Record>,
    record: &Record,
) -> CommonResult<()> {
    let index = index_key(record);
    let old_index = old.map(index_key);
    batch.hsm_put(
        &record_key(record.state.data.entity.entity.parse()?),
        &pack(record)?,
    )?;
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
pub(super) fn readers_active(record: &Record, now: i64) -> bool {
    record.readers.values().any(|deadline| *deadline > now)
}
impl FsDir {
    /// Called for both normal journal replay and the leader's normal close path.
    pub fn hsm_track_file(&self, path: &str, file: &InodeFile) -> CommonResult<()> {
        if !self.hsm_enabled
            || path == HSM_ROOT
            || path.starts_with("/.hsm/")
            || file.file_type != FileType::File
            || file.nlink != 1
            || file.ufs_exists()
        {
            return Ok(());
        }
        let Some(ns) = self.hsm_namespace()? else {
            let mut batch = self.get_rocks_store().new_batch();
            batch.hsm_put(
                format!("\u{3}hsm:enroll:{}", file.id()).as_bytes(),
                &pack(&file.id())?,
            )?;
            return batch.commit();
        };
        let modern = ns.ordinary_format >= 2;
        let old = self.hsm_record(file.id())?;
        if old.as_ref().is_some_and(|r| !r.ordinary) {
            return Ok(());
        }
        let writing = file.features.file_write.is_some();
        let mut record = old.clone().unwrap_or_else(|| Record {
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
                        storage: ns.storage,
                        namespace: ns.incarnation,
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
        if old.as_ref().is_some_and(|r| {
            !r.writing
                && (writing
                    || modern
                        && (r.mtime_ms != file.mtime || r.state.length != file.len.max(0) as u64))
        }) {
            record.state.data.generation += 1;
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
        record.state.revision += 1;
        record.version += 1;
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
                && !readers_active(&record, curvine_runtime::common::LocalTime::mills() as i64)
            {
                return Ok(());
            }
        }
        self.hsm_guard_id(id)
    }
}
impl MasterFilesystem {
    pub(super) async fn hsm_ordinary(&self, ns: &Namespace, request: &Value) -> HsmResult<Value> {
        let op = request["op"].as_str().unwrap();
        let (mut record, id) = {
            let fs = self.fs_dir.read();
            let id = if op == "read_path" {
                let path: String = field(request, "path")?;
                let inp = InodePath::resolve(fs.root_ptr(), path, &fs.store).map_err(io)?;
                let Some(inode) = inp.get_last_inode() else {
                    return Ok(reply("normal", Value::Null));
                };
                inode.id()
            } else {
                entity_id(&field::<EntityKey>(request, "entity")?, ns)?
            };
            let Some(record) = fs.hsm_record(id).map_err(io)? else {
                return Ok(reply("normal", Value::Null));
            };
            (record, id)
        };
        record.path = self.fs_dir.read().get_inode_path(id).map_err(io)?;
        let now = curvine_runtime::common::LocalTime::mills() as i64;
        let previous = record.version;
        let mut controls = vec![];
        let response;
        if op == "read_path" {
            let reader: String = field(request, "reader")?;
            if reader.len() != 32 || !reader.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(HsmError::Invalid("invalid read session".into()));
            }
            if record.state.residency == Residency::Offline {
                // One Recall per offline state revision, shared by concurrent readers.
                let iid = blake3::hash(
                    &pack(&(&record.state.data, record.state.revision, "access-recall"))
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
                    return Ok(reply("pending", iid));
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
                    return Ok(reply("normal", Value::Null));
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
                record.atime_ms = now;
                record.state.revision += 1;
                blocks.status.atime = now;
                record.readers.retain(|_, expiry| *expiry > now);
                record.readers.insert(reader.clone(), now + 120_000);
                response = reply(
                    "read",
                    ReadHandle {
                        blocks,
                        data: record.state.data.clone(),
                        reader,
                        endpoint,
                        expires_ms: now + 120_000,
                    },
                );
            }
        } else if op == "read_close" || op == "read_renew" {
            let reader: String = field(request, "reader")?;
            if op == "read_close" {
                if record
                    .readers
                    .remove(&reader)
                    .is_some_and(|deadline| deadline > now)
                    && record.state.residency != Residency::Offline
                {
                    record.atime_ms = now;
                    record.state.revision += 1;
                }
            } else {
                if record
                    .readers
                    .get(&reader)
                    .is_none_or(|expiry| *expiry <= now)
                {
                    return Err(HsmError::Fenced);
                }
                record.atime_ms = now;
                record.state.revision += 1;
                record.readers.insert(reader, now + 120_000);
            }
            response = reply("done", Value::Null);
        } else if op == "seal_begin" {
            if !record.native_blocks || record.writing {
                return Err(HsmError::Busy(
                    "file is not closed in the native block layout".into(),
                ));
            }
            if record.sealed {
                return Ok(reply("sealed", record.state));
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
        let tx = Transaction {
            controls,
            record_checks: vec![],
            workflow: None,
            outcome: format!("ordinary-{op}-{}", token()),
            fingerprint: request_fingerprint(request)?,
            response,
            inode: Some(id),
            expected_version: Some(previous),
            record: Some(record),
            namespace: None,
            expected_epoch: Some(ns.epoch),
            create: false,
            time: now,
            data: None,
            action: None,
        };
        self.hsm_submit(&tx).await
    }
}
