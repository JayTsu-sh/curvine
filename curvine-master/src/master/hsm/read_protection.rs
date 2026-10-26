// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Leader-local observations protected by a bounded grant horizon. A successor
//! fences structural mutations for a full horizon, including absent reader rows.
use super::*;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

pub(super) enum FastReply {
    Read(Box<ReadHandle>),
    Json(Value),
}
impl FastReply {
    pub(super) fn encode(self, protobuf: bool) -> Vec<u8> {
        match self {
            Self::Read(handle) if protobuf => read_wire::encode(*handle),
            Self::Read(handle) => reply("read_fast", handle).to_string().into_bytes(),
            Self::Json(value) => value.to_string().into_bytes(),
        }
    }
}
#[derive(serde::Deserialize)]
pub(super) struct Close {
    pub(super) data: DataKey,
    pub(super) reader: String,
}
// Cached ReadIndex authorizes only positive protected opens. It is not a
// general query lease: every successor must quarantine structural writes.
// The 119-second horizon starts BEFORE qualification, never at RPC reply.
const PROOF_MS: u64 = 1000;
#[derive(Clone, Copy)]
struct Proof {
    started: u64,
    expires: i64,
}
impl Proof {
    fn valid(self, boot: u64) -> bool {
        boot.checked_sub(self.started)
            .is_some_and(|age| age < PROOF_MS)
    }
}
const GRANT_MS: u64 = 119_000;
const MAX_HISTORY: usize = 65_536;
struct Entry {
    durable: u64,
    record: Record,
    clock: i64,
    dirty_since: std::time::Instant,
    flight: Option<u64>,
    tail_since: Option<std::time::Instant>,
}
impl Entry {
    fn observed(
        durable: u64,
        record: Record,
        clock: i64,
        dirty_since: std::time::Instant,
        previous: Option<&Entry>,
    ) -> Self {
        let flight = previous.and_then(|entry| entry.flight);
        let tail_since = if flight.is_some() {
            previous
                .and_then(|entry| entry.tail_since)
                .or_else(|| Some(std::time::Instant::now()))
        } else {
            None
        };
        Self {
            durable,
            record,
            clock,
            dirty_since,
            flight,
            tail_since,
        }
    }
    fn capture(&mut self) {
        self.flight = Some(self.record.version);
        self.tail_since = None;
    }
    fn acknowledge(&mut self, version: u64, base: u64) {
        self.durable = version;
        self.record.access_base = Some(base);
        if self.record.version > version {
            self.dirty_since = self
                .tail_since
                .take()
                .unwrap_or_else(std::time::Instant::now);
        }
        self.flight = None;
        self.tail_since = None;
    }
}
struct Grant {
    handle: ReadHandle,
    issued: u64,
    request_id: String,
    path: String,
    deadline: i64,
    renewal: Option<Renewal>,
}
struct Renewal {
    request_id: String,
    issued: u64,
    expires: i64,
}
fn request_identity(request: &Value) -> HsmResult<(String, u64)> {
    let identity: String = field(request, "request_id")?;
    if identity.len() != 48 || !identity.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HsmError::Invalid("invalid fast request identity".into()));
    }
    let issued = u64::from_str_radix(&identity[..16], 16).map_err(io)?;
    Ok((identity, issued))
}
struct State {
    term: u64,
    producer: String,
    proof: Option<Proof>,
    sequence: u64,
    namespace: Option<Namespace>,
    oldest_dirty: Option<std::time::Instant>,
    last_activity_boot: u64,
    checkpoint_cursor: i64,
    entries: BTreeMap<i64, Entry>,
    grants: HashMap<String, Grant>,
    expirations: BTreeMap<(i64, String), ()>,
    closed: HashMap<String, u64>,
    retirement: VecDeque<(String, u64)>,
    floor: u64,
}
impl Default for State {
    fn default() -> Self {
        Self {
            term: 0,
            producer: token(),
            proof: None,
            sequence: 0,
            namespace: None,
            oldest_dirty: None,
            last_activity_boot: 0,
            checkpoint_cursor: 0,
            entries: BTreeMap::new(),
            grants: HashMap::new(),
            expirations: BTreeMap::new(),
            closed: HashMap::new(),
            retirement: VecDeque::new(),
            floor: 0,
        }
    }
}
impl State {
    fn reserve_entry(&mut self, id: i64, limit: usize) -> HsmResult<()> {
        if self.entries.len() < limit || self.entries.contains_key(&id) {
            return Ok(());
        }
        while self.entries.len() >= limit {
            let victim = self
                .entries
                .iter()
                .find(|(_, entry)| entry.record.version == entry.durable)
                .map(|(id, _)| *id)
                .ok_or_else(|| HsmError::Busy("access cache backpressure".into()))?;
            self.entries.remove(&victim);
        }
        Ok(())
    }

    /// Retiring a closed identity only replaces its tombstone with the request
    /// timestamp floor. Keep the foreground work bounded even after an idle
    /// period; active grants are never removed or extended by this cleanup.
    fn retire_closed(&mut self, now: u64, budget: usize) -> usize {
        let mut retired = 0;
        while retired < budget
            && self
                .retirement
                .front()
                .is_some_and(|(_, issued)| *issued <= now.saturating_sub(1000))
        {
            let (key, issued) = self.retirement.pop_front().unwrap();
            self.closed.remove(&key);
            self.floor = self.floor.max(issued.saturating_add(1));
            retired += 1;
        }
        retired
    }
}
#[derive(Default)]
pub(crate) struct ReadProtection {
    state: Mutex<State>,
    checkpoint: tokio::sync::Mutex<()>,
    started: AtomicBool,
    fast_ready: AtomicBool,
    #[cfg(any(test, feature = "hsm-scale-test"))]
    checkpoint_paused: AtomicBool,
}
impl ReadProtection {
    pub(super) fn nonce(&self) -> String {
        self.state.lock().unwrap().producer.clone()
    }
    #[cfg(feature = "hsm-scale-test")]
    pub(crate) fn pause_checkpoints(&self, paused: bool) {
        self.checkpoint_paused.store(paused, Ordering::Release);
    }
    pub(crate) fn reset(&self) {
        self.fast_ready.store(false, Ordering::Release);
        *self.state.lock().unwrap() = State::default();
    }
    pub(crate) fn status(
        &self,
        mut status: curvine_model::FileStatus,
    ) -> curvine_model::FileStatus {
        if let Some(entry) = self.state.lock().unwrap().entries.get(&status.id) {
            status.atime = status.atime.max(entry.record.atime_ms);
        }
        status
    }
    pub(super) fn observed(&self, mut record: Record) -> Record {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap()
            .entries
            .get(&record.state.data.entity.entity.parse::<i64>().unwrap_or(-1))
        {
            // Committed apply can publish a captured prefix before its waiter
            // advances durable. Keep observations after that prefix visible.
            if (entry.durable == record.version || entry.flight == Some(record.version))
                && entry.record.state.data == record.state.data
            {
                record = entry.record.clone();
            }
        }
        record
    }
    fn epoch(state: &mut State, term: u64) {
        if state.term != term {
            *state = State {
                term,
                ..State::default()
            };
        }
    }
    fn start(self: &Arc<Self>, fs: &MasterFilesystem) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let monitor = fs.master_monitor.clone();
        let template = fs.clone();
        let protection = self.clone();
        crate::master::spawn_authority("read-access-checkpoint", async move {
            let mut last_checkpoint = std::time::Instant::now();
            // The task shares the server runtime and refreshes only while
            // accesses are recent or their persistence tail remains dirty.
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                if !monitor.is_active() {
                    protection.state.lock().unwrap().proof = None;
                    continue;
                }
                let Ok(boot) = curvine_sys::lease_time_millis() else {
                    protection.state.lock().unwrap().proof = None;
                    continue;
                };
                {
                    let mut state = protection.state.lock().unwrap();
                    state.retire_closed(curvine_runtime::common::LocalTime::mills(), 1024);
                    if state.oldest_dirty.is_none()
                        && boot.saturating_sub(state.last_activity_boot) >= 250
                    {
                        state.proof = None;
                        continue;
                    }
                }
                let gate = template.fs_dir.read().hsm_mutation_gate.clone();
                let _guard = gate.read().await;
                if let Err(error) = protection.qualify(&template).await {
                    protection.state.lock().unwrap().proof = None;
                    log::debug!("fast read qualification unavailable: {error}");
                    continue;
                }
                #[cfg(any(test, feature = "hsm-scale-test"))]
                if protection.checkpoint_paused.load(Ordering::Acquire) {
                    continue;
                }
                if last_checkpoint.elapsed() >= std::time::Duration::from_millis(500) {
                    last_checkpoint = std::time::Instant::now();
                    if let Err(error) = protection.flush(&template, false).await {
                        log::warn!("access checkpoint failed: {error}");
                        // A failed submission may still apply. Requalify before
                        // releasing captured versions to another authority owner.
                        if template.hsm_read_barrier().await.is_err() {
                            std::process::abort();
                        }
                    }
                }
            }
        });
    }
    pub(super) async fn close_batch(
        &self,
        fs: &MasterFilesystem,
        closes: Vec<Close>,
    ) -> HsmResult<()> {
        self.close_batch_with_limit(fs, closes, MAX_HISTORY).await
    }

    async fn close_batch_with_limit(
        &self,
        fs: &MasterFilesystem,
        closes: Vec<Close>,
        max_entries: usize,
    ) -> HsmResult<()> {
        if closes.len() > 512 {
            return Err(HsmError::Invalid("close batch exceeds bound".into()));
        }
        let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
        let _guard = gate.read().await;
        let dir = fs.fs_dir.read();
        let term = dir.journal_writer.current_term();
        if term == 0 || !fs.master_monitor.is_active() {
            return Err(HsmError::Fenced);
        }
        let time = dir.hsm_read_time().map_err(io)?;
        let now = curvine_runtime::common::LocalTime::mills() as i64;
        let mut state = self.state.lock().unwrap();
        Self::epoch(&mut state, term);
        for close in closes {
            let Some(grant) = state.grants.get(&close.reader) else {
                continue;
            };
            if grant.handle.data != close.data {
                continue;
            }
            let id = close.data.entity.entity.parse::<i64>().map_err(io)?;
            if !state.entries.contains_key(&id) {
                if let Some(record) = dir.hsm_record(id).map_err(io)? {
                    if record.state.data == close.data {
                        // Failure leaves the grant intact so advisory close
                        // delivery can retry after a checkpoint frees a slot.
                        state.reserve_entry(id, max_entries)?;
                        state.entries.insert(
                            id,
                            Entry {
                                durable: record.version,
                                record,
                                clock: time.now,
                                dirty_since: std::time::Instant::now(),
                                flight: None,
                                tail_since: None,
                            },
                        );
                    }
                }
            }
            let grant = state.grants.remove(&close.reader).unwrap();
            state
                .expirations
                .remove(&(grant.deadline, close.reader.clone()));
            state.closed.insert(close.reader.clone(), grant.issued);
            state
                .retirement
                .push_back((close.reader.clone(), grant.issued));
            let mut since = None;
            if let Some(entry) = state
                .entries
                .get_mut(&id)
                .filter(|entry| entry.record.state.data == close.data)
            {
                if let Some(deadline) = entry.record.readers.remove(&close.reader) {
                    if entry.flight.is_some() {
                        entry.tail_since.get_or_insert_with(std::time::Instant::now);
                    }
                    if entry.record.version == entry.durable {
                        entry.dirty_since = std::time::Instant::now();
                    }
                    entry.record.version += 1;
                    if time.active(deadline) {
                        entry.record.state.revision += 1;
                        entry.record.atime_ms = entry.record.atime_ms.max(now);
                    }
                    entry.clock = time.now;
                    since = Some(entry.dirty_since);
                }
            }
            if let Some(since) = since {
                state.oldest_dirty.get_or_insert(since);
            }
        }
        Ok(())
    }
    async fn ensure_activated(self: &Arc<Self>, fs: &MasterFilesystem) -> HsmResult<bool> {
        if self.fast_ready.load(Ordering::Acquire) {
            return Ok(true);
        }
        {
            let dir = fs.fs_dir.read();
            let Some(ns) = dir
                .hsm_namespace()
                .map_err(io)?
                .filter(|ns| ns.ordinary_format >= 2)
            else {
                return Ok(false);
            };
            if access_checkpoint::activated(&dir, &ns).map_err(io)? {
                self.fast_ready.store(true, Ordering::Release);
                return Ok(true);
            }
        }
        let owned = fs.clone();
        let protection = self.clone();
        let response = fs
            .hsm_run_owned(
                crate::master::authority_gate::Class::Foreground,
                async move {
                    let gate = owned.fs_dir.read().hsm_mutation_gate.clone();
                    let _guard = gate.write().await;
                    owned.hsm_read_barrier().await?;
                    let (ns, term) = {
                        let dir = owned.fs_dir.read();
                        (
                            dir.hsm_namespace().map_err(io)?.ok_or(HsmError::Fenced)?,
                            dir.journal_writer.current_term(),
                        )
                    };
                    let activated =
                        access_checkpoint::activated(&owned.fs_dir.read(), &ns).map_err(io)?;
                    if !activated {
                        let activation = access_checkpoint::Activation {
                            storage: ns.storage.clone(),
                            incarnation: ns.incarnation.clone(),
                        };
                        let payload = [
                            access_checkpoint::ACTIVATE_MAGIC,
                            pack(&activation).map_err(io)?.as_slice(),
                        ]
                        .concat();
                        let result = owned.hsm_submit_payload(payload, 1, Some(term)).await;
                        if let Err(error) = &result {
                            if let Err(fence) = owned.hsm_read_barrier().await {
                                log::error!(
                                    "fast-read format activation cannot be fenced: {error}; {fence}"
                                );
                                std::process::abort();
                            }
                        }
                        let response = result?.remove(0);
                        if response["kind"] != "done" {
                            return Err(HsmError::Fenced);
                        }
                    }
                    let dir = owned.fs_dir.read();
                    let current = dir.hsm_namespace().map_err(io)?.ok_or(HsmError::Fenced)?;
                    if !access_checkpoint::activated(&dir, &current).map_err(io)? {
                        return Err(HsmError::Fenced);
                    }
                    protection.fast_ready.store(true, Ordering::Release);
                    Ok(reply("done", Value::Null))
                },
            )
            .await;
        if response["kind"] != "done" {
            return Err(HsmError::Busy(format!(
                "fast-read format activation unavailable: {response}"
            )));
        }
        Ok(true)
    }
    async fn qualify(&self, fs: &MasterFilesystem) -> HsmResult<()> {
        let (term, producer, started) = {
            let dir = fs.fs_dir.read();
            let term = dir.journal_writer.current_term();
            let mut state = self.state.lock().unwrap();
            Self::epoch(&mut state, term);
            (
                term,
                state.producer.clone(),
                curvine_sys::lease_time_millis().map_err(io)?,
            )
        };
        fs.hsm_read_barrier().await?;
        let dir = fs.fs_dir.read();
        let mut state = self.state.lock().unwrap();
        if !fs.master_monitor.is_active()
            || term == 0
            || dir.journal_writer.current_term() != term
            || state.producer != producer
        {
            return Err(HsmError::Fenced);
        }
        let time = dir.hsm_read_time().map_err(io)?;
        let boot = curvine_sys::lease_time_millis().map_err(io)?;
        let elapsed = boot.checked_sub(started).ok_or(HsmError::Fenced)?;
        if elapsed >= PROOF_MS {
            return Err(HsmError::Busy("read proof qualification expired".into()));
        }
        state.proof = Some(Proof {
            started,
            expires: time
                .now
                .saturating_add(GRANT_MS.saturating_sub(elapsed) as i64),
        });
        Ok(())
    }
    pub(super) async fn execute(
        self: &Arc<Self>,
        fs: &MasterFilesystem,
        request: &Value,
    ) -> HsmResult<Option<FastReply>> {
        if request["read_mode"] != "fast_v1" {
            return Ok(None);
        }
        if request["op"] == "read_close" {
            let close = Close {
                data: field(request, "data")?,
                reader: field(request, "reader")?,
            };
            self.close_batch(fs, vec![close]).await?;
            return Ok(Some(FastReply::Json(reply("done", Value::Null))));
        }
        if curvine_sys::lease_time_millis().is_err() {
            return Ok(None);
        }
        if !self.ensure_activated(fs).await? {
            return Ok(None);
        }
        self.start(fs);
        let gate = fs.fs_dir.read().hsm_mutation_gate.clone();
        let _guard = gate.read().await;
        let term = fs.fs_dir.read().journal_writer.current_term();
        let boot = curvine_sys::lease_time_millis().map_err(io)?;
        let valid = {
            let mut state = self.state.lock().unwrap();
            Self::epoch(&mut state, term);
            state.proof.is_some_and(|proof| proof.valid(boot))
        };
        if !valid {
            self.qualify(fs).await?;
        }
        if !fs.master_monitor.is_active() || term == 0 {
            return Err(HsmError::Fenced);
        }
        let dir = fs.fs_dir.read();
        if !self.fast_ready.load(Ordering::Acquire) {
            return Ok(None);
        }
        let time = dir.hsm_read_time().map_err(io)?;
        let now = curvine_runtime::common::LocalTime::mills();
        let mut state = self.state.lock().unwrap();
        if state.term != term || dir.journal_writer.current_term() != term {
            return Err(HsmError::Fenced);
        }
        state.last_activity_boot = boot;
        let proof = state.proof.ok_or(HsmError::Fenced)?;
        let boot = curvine_sys::lease_time_millis().map_err(io)?;
        if !proof.valid(boot) {
            return Err(HsmError::Busy("read proof expired".into()));
        }
        let expiry = proof.expires;
        if state.namespace.is_none() {
            state.namespace = dir
                .hsm_namespace()
                .map_err(io)?
                .filter(|namespace| namespace.ordinary_format >= 2);
        }
        let Some(ns) = state.namespace.clone() else {
            return Ok(None);
        };
        let op = request["op"].as_str().unwrap_or("");
        let open = matches!(op, "open_path" | "read_path");
        if open && request["issuer"].as_str() != Some(state.producer.as_str()) {
            return Ok(Some(FastReply::Json(reply(
                "read_fast_bind",
                &state.producer,
            ))));
        }
        let reader: String = field(request, "reader")?;
        let reader = if open {
            if reader.len() != 32 || !reader.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(HsmError::Invalid("invalid fast reader".into()));
            }
            format!("{}:{reader}", state.producer)
        } else {
            reader
        };
        if !reader.starts_with(&format!("{}:", state.producer)) {
            return Err(HsmError::Fenced);
        }
        let mut opened_path = None;
        let mut opened_input = None;
        let id = if open {
            let path: String = field(request, "path")?;
            let input = InodePath::resolve(dir.root_ptr(), &path, &dir.store).map_err(io)?;
            let Some(inode) = input.get_last_inode() else {
                return Ok(None);
            };
            let id = inode.id();
            opened_path = Some(path);
            opened_input = Some(input);
            id
        } else {
            entity_id(&field::<EntityKey>(request, "entity")?, &ns)?
        };
        let (mut record, durable_version) = match state.entries.get(&id) {
            Some(entry) => (entry.record.clone(), entry.durable),
            None => {
                let Some(record) = dir.hsm_record(id).map_err(io)? else {
                    return Ok(None);
                };
                let version = record.version;
                (record, version)
            }
        };
        if record.state.residency == Residency::Offline
            || record.writing
            || (!record.native_blocks && record.published.is_none())
        {
            return Ok(None);
        }
        let dirty_since = state
            .entries
            .get(&id)
            .filter(|entry| entry.record.version > entry.durable)
            .map(|entry| entry.dirty_since)
            .unwrap_or_else(std::time::Instant::now);
        if open {
            while state
                .expirations
                .first_key_value()
                .is_some_and(|((deadline, _), _)| *deadline <= time.now)
            {
                let ((_, reader), _) = state.expirations.pop_first().unwrap();
                if let Some(grant) = state.grants.remove(&reader) {
                    state.expirations.remove(&(grant.deadline, reader.clone()));
                    state.closed.insert(reader.clone(), grant.issued);
                    state.retirement.push_back((reader, grant.issued));
                }
            }
            state.retire_closed(now, 128);
            if state.closed.contains_key(&reader) {
                return Err(HsmError::Fenced);
            }
            let (identity, issued) = request_identity(request)?;
            if let Some(grant) = state.grants.get(&reader) {
                if grant.request_id != identity
                    || opened_path.as_ref() != Some(&grant.path)
                    || grant.handle.data != record.state.data
                    || grant.handle.expires_ms <= time.now
                    || !record.readers.contains_key(&reader)
                {
                    return Err(HsmError::Fenced);
                }
                let mut handle = grant.handle.clone();
                handle.granted_ms = time.now;
                handle.expires_ms = handle.expires_ms.min(expiry);
                return Ok(Some(FastReply::Read(Box::new(handle))));
            }
            if issued < state.floor || issued.abs_diff(now) > 300_000 {
                return Err(HsmError::Fenced);
            }
            record
                .readers
                .retain(|_, deadline| time.protection().active(*deadline));
            if record.readers.len() >= 1024
                || state
                    .oldest_dirty
                    .is_some_and(|since| since.elapsed().as_millis() >= 1000)
                || state.grants.len() + state.closed.len() >= MAX_HISTORY
            {
                return Err(HsmError::Busy("access checkpoint backpressure".into()));
            }
            let mut blocks = if record.native_blocks {
                fs.get_protected_file_blocks(
                    opened_path.as_ref().unwrap(),
                    &dir,
                    opened_input.as_ref().unwrap(),
                    &record,
                )
                .map_err(io)?
            } else {
                curvine_model::FileBlocks::new(
                    opened_input
                        .as_ref()
                        .unwrap()
                        .get_last_inode()
                        .ok_or_else(|| HsmError::NotFound("private read inode".into()))?
                        .to_file_status(opened_path.as_ref().unwrap())
                        .map_err(io)?,
                    vec![],
                )
            };
            state.reserve_entry(id, MAX_HISTORY)?;
            state.oldest_dirty.get_or_insert(dirty_since);
            record.atime_ms = record.atime_ms.max(now as i64);
            record.state.revision += 1;
            record.version += 1;
            record.readers.insert(reader.clone(), expiry);
            blocks.status.atime = record.atime_ms;
            let handle = ReadHandle {
                blocks,
                data: record.state.data.clone(),
                reader: reader.clone(),
                endpoint: record.published.as_ref().map(|layout| Endpoint {
                    allocation: layout.id.clone(),
                    worker: layout.worker.clone(),
                    token: layout.read.clone(),
                }),
                granted_ms: time.now,
                expires_ms: expiry,
            };
            state.expirations.insert((expiry, reader.clone()), ());
            state.grants.insert(
                reader,
                Grant {
                    handle: handle.clone(),
                    issued,
                    request_id: identity,
                    path: opened_path.unwrap(),
                    deadline: expiry,
                    renewal: None,
                },
            );
            if record.version > durable_version {
                state.oldest_dirty.get_or_insert(dirty_since);
            }
            let next = Entry::observed(
                durable_version,
                record,
                time.now,
                dirty_since,
                state.entries.get(&id),
            );
            state.entries.insert(id, next);
            Ok(Some(FastReply::Read(Box::new(handle))))
        } else {
            let data: DataKey = field(request, "data")?;
            if data != record.state.data {
                return Err(HsmError::Fenced);
            }
            if !state.grants.contains_key(&reader)
                || record
                    .readers
                    .get(&reader)
                    .is_none_or(|deadline| *deadline <= time.now)
            {
                return Err(HsmError::Fenced);
            }
            let (identity, issued) = request_identity(request)?;
            if let Some(renewal) = state.grants.get(&reader).unwrap().renewal.as_ref() {
                if renewal.request_id == identity {
                    if renewal.expires <= time.now {
                        return Err(HsmError::Fenced);
                    }
                    return Ok(Some(FastReply::Json(reply(
                        "done",
                        serde_json::json!({"granted_ms":time.now,"expires_ms":renewal.expires.min(expiry)}),
                    ))));
                }
                if issued <= renewal.issued {
                    return Err(HsmError::Fenced);
                }
            }
            if issued.abs_diff(now) > 300_000 {
                return Err(HsmError::Fenced);
            }
            if state
                .oldest_dirty
                .is_some_and(|since| since.elapsed().as_millis() >= 1000)
            {
                return Err(HsmError::Busy("access checkpoint backpressure".into()));
            }
            state.reserve_entry(id, MAX_HISTORY)?;
            let previous = state.grants.get(&reader).unwrap().deadline;
            state.expirations.remove(&(previous, reader.clone()));
            state.expirations.insert((expiry, reader.clone()), ());
            record.readers.insert(reader.clone(), expiry);
            record.atime_ms = record.atime_ms.max(now as i64);
            record.state.revision += 1;
            record.version += 1;
            let grant = state.grants.get_mut(&reader).unwrap();
            grant.deadline = expiry;
            grant.renewal = Some(Renewal {
                request_id: identity,
                issued,
                expires: expiry,
            });
            let result = reply(
                "done",
                serde_json::json!({"granted_ms":time.now,"expires_ms":expiry}),
            );
            if record.version > durable_version {
                state.oldest_dirty.get_or_insert(dirty_since);
            }
            let next = Entry::observed(
                durable_version,
                record,
                time.now,
                dirty_since,
                state.entries.get(&id),
            );
            state.entries.insert(id, next);
            Ok(Some(FastReply::Json(result)))
        }
    }
    pub(super) async fn flush(&self, fs: &MasterFilesystem, drain: bool) -> HsmResult<()> {
        let _owner = self.checkpoint.lock().await;
        let mut captured = 0;
        let mut pending = {
            let dir = fs.fs_dir.read();
            let mut state = self.state.lock().unwrap();
            Self::epoch(&mut state, dir.journal_writer.current_term());
            let mut ids: Vec<_> = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.record.version > entry.durable)
                .map(|(id, _)| *id)
                .collect();
            if !drain {
                let pivot = ids.partition_point(|id| *id <= state.checkpoint_cursor);
                ids.rotate_left(pivot);
            }
            VecDeque::from(ids)
        };
        loop {
            let capture = std::time::Instant::now();
            let (checkpoint, producer) = {
                let dir = fs.fs_dir.read();
                let mut state = self.state.lock().unwrap();
                Self::epoch(&mut state, dir.journal_writer.current_term());
                let mut rows = Vec::new();
                let mut bytes = 0;
                while let Some(id) = pending.pop_front() {
                    let Some(entry) = state.entries.get_mut(&id) else {
                        continue;
                    };
                    if entry.record.version == entry.durable {
                        continue;
                    }
                    let row = access_checkpoint::Row {
                        inode: id,
                        expected_version: entry.durable,
                        access: ReadState {
                            clock_ms: entry.clock,
                            data: entry.record.state.data.clone(),
                            base_version: entry.record.access_base.unwrap_or(entry.durable),
                            version: entry.record.version,
                            revision: entry.record.state.revision,
                            atime_ms: entry.record.atime_ms,
                            readers: entry.record.readers.clone(),
                        },
                    };
                    let length = pack(&row).map_err(io)?.len();
                    if bytes + length > 768 * 1024 {
                        if rows.is_empty() {
                            return Err(HsmError::Busy(
                                "access row exceeds checkpoint bound".into(),
                            ));
                        }
                        pending.push_front(id);
                        break;
                    }
                    bytes += length;
                    entry.capture();
                    rows.push(row);
                    if !drain {
                        state.checkpoint_cursor = id;
                    }
                    if rows.len() == access_checkpoint::MAX_ROWS {
                        break;
                    }
                }
                if rows.is_empty() {
                    state.oldest_dirty = state
                        .entries
                        .values()
                        .filter(|entry| entry.record.version > entry.durable)
                        .map(|entry| entry.dirty_since)
                        .min();
                    return Ok(());
                }
                rows.sort_by_key(|row| row.inode);
                let ns = dir.hsm_namespace().map_err(io)?.ok_or(HsmError::Fenced)?;
                state.sequence += 1;
                (
                    access_checkpoint::Checkpoint {
                        incarnation: ns.incarnation,
                        epoch: ns.epoch,
                        expected_frontier: access_checkpoint::frontier(&dir).map_err(io)?,
                        producer: state.producer.clone(),
                        sequence: state.sequence,
                        rows,
                    },
                    state.producer.clone(),
                )
            };
            if let Ok(metrics) = crate::master::Master::get_metrics() {
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["access_checkpoint", "capture"])
                    .observe(capture.elapsed().as_micros() as f64);
                metrics
                    .hsm_batch_size
                    .with_label_values(&["access_checkpoint"])
                    .observe(checkpoint.rows.len() as f64);
            }
            let encoding = std::time::Instant::now();
            let bytes = pack(&checkpoint).map_err(io)?;
            let expected_hash = *blake3::hash(&bytes).as_bytes();
            let payload = [access_checkpoint::MAGIC, bytes.as_slice()].concat();
            if let Ok(metrics) = crate::master::Master::get_metrics() {
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["access_checkpoint", "encode"])
                    .observe(encoding.elapsed().as_micros() as f64);
            }
            let term = self.state.lock().unwrap().term;
            let committing = std::time::Instant::now();
            let response = match fs.hsm_submit_payload(payload, 1, Some(term)).await {
                Ok(mut responses) => responses.remove(0),
                Err(error) => {
                    if let Err(fence) = fs.hsm_read_barrier().await {
                        log::error!("uncertain access checkpoint cannot be fenced: {error}; {fence}; aborting master");
                        std::process::abort();
                    }
                    let durable = access_checkpoint::frontier(&fs.fs_dir.read()).map_err(io)?;
                    if let Some(stamp) = durable.filter(|stamp| {
                        stamp.producer == producer
                            && stamp.sequence == checkpoint.sequence
                            && stamp.hash == expected_hash
                    }) {
                        reply("done", stamp)
                    } else {
                        return Err(HsmError::Busy(format!(
                            "access checkpoint not confirmed after fence: {error}"
                        )));
                    }
                }
            };
            if let Ok(metrics) = crate::master::Master::get_metrics() {
                metrics
                    .hsm_stage_duration
                    .with_label_values(&["access_checkpoint", "commit"])
                    .observe(committing.elapsed().as_micros() as f64);
            }
            if response["kind"] != "done" {
                return Err(HsmError::Busy(format!(
                    "access checkpoint rejected: {response}"
                )));
            }
            let mut state = self.state.lock().unwrap();
            if state.producer != producer {
                return Err(HsmError::Fenced);
            }
            for row in checkpoint.rows {
                if let Some(entry) = state.entries.get_mut(&row.inode) {
                    entry.acknowledge(row.access.version, row.access.base_version);
                }
            }
            state.oldest_dirty = state
                .entries
                .values()
                .filter(|entry| entry.record.version > entry.durable)
                .map(|entry| entry.dirty_since)
                .min();
            captured += 1;
            // Never chase updates accepted after this turn's captured prefix.
            // The next periodic turn owns those tails; exclusive owners admit
            // no observations while draining their complete initial prefix.
            if pending.is_empty() || (!drain && captured >= 64) {
                return Ok(());
            }
        }
    }
}
impl MasterFilesystem {
    /// Caller owns an exclusive authority guard through subsequent committed apply.
    pub(crate) async fn hsm_before_stable_underheld(&self) -> HsmResult<()> {
        if !self.fs_dir.read().hsm_enabled {
            return Ok(());
        }
        {
            let dir = self.fs_dir.read();
            if dir.hsm_namespace().map_err(io)?.is_some()
                && dir.hsm_read_time().map_err(io)?.recovering()
            {
                return Err(HsmError::Busy(
                    "leader recovery protects prior read grants for 120 seconds".into(),
                ));
            }
        }
        let protection = self.fs_dir.read().hsm_read_protection.clone();
        protection.flush(self, true).await?;
        let mut state = protection.state.lock().unwrap();
        state.entries.clear();
        state.namespace = None;
        state.oldest_dirty = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem, i64)> {
        use crate::master::journal::{JournalSystem, JournalWriter};

        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let id = fs.create("/file", false)?.id;
        fs.complete_file("/file", None, 0, vec![], "", false, None)?;
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        fs.master_monitor.journal_term.store(7, Ordering::Release);
        {
            let mut dir = fs.fs_dir.write();
            // Supply a current term without a live Raft cluster. These tests
            // seed an already-qualified proof and exercise admission/apply.
            dir.journal_writer = Arc::new(JournalWriter::new_with_term(
                true,
                curvine_raft::raft::RaftClient::from_conf(
                    conf.journal.create_runtime(),
                    &conf.journal,
                ),
                &conf.journal,
                Some(fs.master_monitor.journal_term.clone()),
            )?);
            let namespace = Namespace {
                ordinary_format: 2,
                root_inode: 9999,
                storage: "s".into(),
                incarnation: "n".into(),
                epoch: 1,
            };
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(NS, &pack(&namespace)?)?;
            batch.commit()?;
            dir.hsm_enabled = true;
            let input = InodePath::resolve(dir.root_ptr(), "/file", &dir.store)?;
            dir.hsm_track_file("/file", input.get_last_inode().unwrap().as_file_ref()?)?;
            let _ = dir.hsm_read_time()?;
            dir.hsm_read_clock.mark_fresh(7);
            let protection = &dir.hsm_read_protection;
            protection.fast_ready.store(true, Ordering::Release);
            protection.started.store(true, Ordering::Release);
            ReadProtection::epoch(&mut protection.state.lock().unwrap(), 7);
        }
        Ok((root, fs, id))
    }

    fn open_request(protection: &ReadProtection) -> Value {
        serde_json::json!({
            "op":"open_path", "path":"/file", "read_mode":"fast_v1",
            "issuer":protection.nonce(), "reader":token(),
            "request_id":format!("{:016x}{}", curvine_runtime::common::LocalTime::mills(), token()),
        })
    }

    fn seed_proof(fs: &MasterFilesystem, protection: &ReadProtection) {
        let time = fs.fs_dir.read().hsm_read_time().unwrap();
        protection.state.lock().unwrap().proof = Some(Proof {
            started: curvine_sys::lease_time_millis().unwrap(),
            expires: time.now + GRANT_MS as i64,
        });
    }

    async fn open(fs: &MasterFilesystem, protection: &Arc<ReadProtection>) -> ReadHandle {
        seed_proof(fs, protection);
        match protection
            .execute(fs, &open_request(protection))
            .await
            .unwrap()
        {
            Some(FastReply::Read(handle)) => *handle,
            _ => panic!("expected protected open"),
        }
    }

    #[test]
    fn applied_checkpoint_keeps_open_and_close_tail_visible_before_ack() -> CommonResult<()> {
        if curvine_sys::lease_time_millis().is_err() {
            return Ok(());
        }
        let (_root, fs, id) = fixture()?;
        let protection = fs.fs_dir.read().hsm_read_protection.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let first = open(&fs, &protection).await;
            let checkpoint = {
                let mut state = protection.state.lock().unwrap();
                let producer = state.producer.clone();
                let entry = state.entries.get_mut(&id).unwrap();
                entry.capture();
                access_checkpoint::Checkpoint {
                    incarnation: "n".into(),
                    epoch: 1,
                    expected_frontier: None,
                    producer,
                    sequence: 1,
                    rows: vec![access_checkpoint::Row {
                        inode: id,
                        expected_version: entry.durable,
                        access: ReadState {
                            clock_ms: entry.clock,
                            data: entry.record.state.data.clone(),
                            base_version: entry.durable,
                            version: entry.record.version,
                            revision: entry.record.state.revision,
                            atime_ms: entry.record.atime_ms,
                            readers: entry.record.readers.clone(),
                        },
                    }],
                }
            };
            assert_eq!(
                access_checkpoint::apply(&fs.fs_dir.read(), &pack(&checkpoint)?)?["kind"],
                "done"
            );
            let second = open(&fs, &protection).await;
            let committed = fs.fs_dir.read().hsm_record(id)?.unwrap();
            let observed = protection.observed(committed.clone());
            assert_eq!(observed.state.revision, committed.state.revision + 1);
            assert!(observed.readers.contains_key(&first.reader));
            assert!(observed.readers.contains_key(&second.reader));
            protection
                .close_batch(
                    &fs,
                    vec![Close {
                        data: first.data,
                        reader: first.reader.clone(),
                    }],
                )
                .await
                .unwrap();
            let observed = protection.observed(committed.clone());
            assert_eq!(observed.state.revision, committed.state.revision + 2);
            assert!(!observed.readers.contains_key(&first.reader));
            assert!(observed.readers.contains_key(&second.reader));
            let mut different_generation = committed.clone();
            different_generation.state.data.generation += 1;
            assert_eq!(
                protection.observed(different_generation.clone()).state.data,
                different_generation.state.data
            );
            let mut unrelated_prefix = committed;
            unrelated_prefix.version += 100;
            assert_eq!(
                protection.observed(unrelated_prefix.clone()).version,
                unrelated_prefix.version
            );
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        Ok(())
    }

    #[test]
    fn full_reader_map_admits_only_after_conservative_expiry() -> CommonResult<()> {
        if curvine_sys::lease_time_millis().is_err() {
            return Ok(());
        }
        let (_root, fs, id) = fixture()?;
        let protection = fs.fs_dir.read().hsm_read_protection.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            for expired in [0, 1, 1024] {
                let dir = fs.fs_dir.read();
                let time = dir.hsm_read_time()?;
                let mut record = dir.hsm_record(id)?.unwrap();
                record.readers = (0..1024)
                    .map(|index| {
                        let deadline = if index < expired {
                            time.now - 2000
                        } else {
                            time.now + 60_000
                        };
                        (format!("old-{index}"), deadline)
                    })
                    .collect();
                // A raw-expired reader remains protected by the clock margin.
                if expired < 1024 {
                    record.readers.insert("old-1023".into(), time.now - 500);
                }
                let mut batch = dir.get_rocks_store().new_batch();
                batch.hsm_put_record(id, &record)?;
                batch.commit()?;
                drop(dir);
                {
                    let mut state = protection.state.lock().unwrap();
                    state.entries.clear();
                    state.oldest_dirty = None;
                }
                seed_proof(&fs, &protection);
                let result = protection.execute(&fs, &open_request(&protection)).await;
                if expired == 0 {
                    assert!(matches!(result, Err(HsmError::Busy(_))));
                } else {
                    let handle = match result.unwrap() {
                        Some(FastReply::Read(handle)) => handle,
                        _ => panic!("expired readers blocked a protected open"),
                    };
                    let state = protection.state.lock().unwrap();
                    let readers = &state.entries.get(&id).unwrap().record.readers;
                    assert_eq!(readers.len(), 1025 - expired);
                    assert!(readers.contains_key(&handle.reader));
                    assert!(!readers.contains_key("old-0"));
                    if expired < 1024 {
                        assert!(readers.contains_key("old-1023"));
                    }
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        Ok(())
    }

    #[test]
    fn close_of_evicted_grant_respects_cache_bound_and_retries_after_checkpoint() -> CommonResult<()>
    {
        if curvine_sys::lease_time_millis().is_err() {
            return Ok(());
        }
        let (_root, fs, id) = fixture()?;
        let protection = fs.fs_dir.read().hsm_read_protection.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            for dirty in [false, true] {
                let handle = open(&fs, &protection).await;
                let record = protection.state.lock().unwrap().entries[&id].record.clone();
                // Make the active grant durable, then model clean-cache
                // eviction by other inodes without retiring its receipt.
                {
                    let dir = fs.fs_dir.read();
                    let mut batch = dir.get_rocks_store().new_batch();
                    batch.hsm_put_record(id, &record)?;
                    batch.commit()?;
                }
                {
                    let mut state = protection.state.lock().unwrap();
                    state.entries.clear();
                    state.oldest_dirty = None;
                    for other in [id + 1, id + 2] {
                        let mut cached = record.clone();
                        cached.state.data.entity.entity = other.to_string();
                        let durable = cached.version - u64::from(dirty);
                        state.entries.insert(
                            other,
                            Entry::observed(durable, cached, 1000, std::time::Instant::now(), None),
                        );
                    }
                }
                let close = || Close {
                    data: handle.data.clone(),
                    reader: handle.reader.clone(),
                };
                let result = protection
                    .close_batch_with_limit(&fs, vec![close()], 2)
                    .await;
                if dirty {
                    assert!(matches!(result, Err(HsmError::Busy(_))));
                    {
                        let mut state = protection.state.lock().unwrap();
                        assert_eq!(state.entries.len(), 2);
                        assert!(!state.entries.contains_key(&id));
                        assert!(state.grants.contains_key(&handle.reader));
                        assert!(!state.closed.contains_key(&handle.reader));
                        // A checkpoint acknowledgement makes one slot
                        // evictable; the same failed close must now succeed.
                        let entry = state.entries.get_mut(&(id + 1)).unwrap();
                        entry.durable = entry.record.version;
                    }
                    protection
                        .close_batch_with_limit(&fs, vec![close()], 2)
                        .await
                        .unwrap();
                } else {
                    result.unwrap();
                }
                let state = protection.state.lock().unwrap();
                assert_eq!(state.entries.len(), 2);
                assert!(state.entries[&id].record.readers.is_empty());
                assert!(!state.grants.contains_key(&handle.reader));
                assert!(state.closed.contains_key(&handle.reader));
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        Ok(())
    }

    #[test]
    fn renewal_of_evicted_grant_respects_cache_bound_and_preserves_failed_receipt(
    ) -> CommonResult<()> {
        if curvine_sys::lease_time_millis().is_err() {
            return Ok(());
        }
        let (_root, fs, id) = fixture()?;
        let protection = fs.fs_dir.read().hsm_read_protection.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let handle = open(&fs, &protection).await;
            let issued = curvine_runtime::common::LocalTime::mills();
            let request = serde_json::json!({
                "op":"read_renew", "read_mode":"fast_v1", "entity":handle.data.entity,
                "data":handle.data, "reader":handle.reader,
                "request_id":format!("{issued:016x}{}", token()),
            });
            seed_proof(&fs, &protection);
            let original = match protection.execute(&fs, &request).await.unwrap() {
                Some(FastReply::Json(value)) => value,
                _ => panic!("expected renewal receipt"),
            };
            let record = protection.state.lock().unwrap().entries[&id].record.clone();
            {
                let dir = fs.fs_dir.read();
                let mut batch = dir.get_rocks_store().new_batch();
                batch.hsm_put_record(id, &record)?;
                batch.commit()?;
            }
            let (deadline, original_identity) = {
                let mut state = protection.state.lock().unwrap();
                state.entries.clear();
                state.oldest_dirty = None;
                let mut cached = record.clone();
                cached.readers.clear();
                for index in 1..=MAX_HISTORY {
                    let other = id + index as i64;
                    cached.state.data.entity.entity = other.to_string();
                    state.entries.insert(
                        other,
                        Entry::observed(
                            cached.version - 1,
                            cached.clone(),
                            1000,
                            std::time::Instant::now(),
                            None,
                        ),
                    );
                }
                let grant = &state.grants[&handle.reader];
                (
                    grant.deadline,
                    grant.renewal.as_ref().unwrap().request_id.clone(),
                )
            };
            // Replaying a completed renewal needs no new cache slot.
            seed_proof(&fs, &protection);
            let repeated = match protection.execute(&fs, &request).await.unwrap() {
                Some(FastReply::Json(value)) => value,
                _ => panic!("expected duplicate renewal receipt"),
            };
            assert_eq!(
                repeated["data"]["expires_ms"],
                original["data"]["expires_ms"]
            );
            let mut newer = request;
            newer["request_id"] = serde_json::json!(format!("{:016x}{}", issued + 1, token()));
            seed_proof(&fs, &protection);
            assert!(matches!(
                protection.execute(&fs, &newer).await,
                Err(HsmError::Busy(_))
            ));
            {
                let mut state = protection.state.lock().unwrap();
                assert_eq!(state.entries.len(), MAX_HISTORY);
                assert!(!state.entries.contains_key(&id));
                let grant = &state.grants[&handle.reader];
                assert_eq!(grant.deadline, deadline);
                assert_eq!(
                    grant.renewal.as_ref().unwrap().request_id,
                    original_identity
                );
                assert_eq!(state.expirations.len(), 1);
                assert!(state
                    .expirations
                    .contains_key(&(deadline, handle.reader.clone())));
                assert!(!state.closed.contains_key(&handle.reader));
                let entry = state.entries.get_mut(&(id + 1)).unwrap();
                entry.durable = entry.record.version;
            }
            seed_proof(&fs, &protection);
            match protection.execute(&fs, &newer).await.unwrap() {
                Some(FastReply::Json(value)) => assert_eq!(value["kind"], "done"),
                _ => panic!("renewal did not retry after a clean slot became available"),
            }
            let state = protection.state.lock().unwrap();
            assert_eq!(state.entries.len(), MAX_HISTORY);
            assert_eq!(state.entries[&id].durable, record.version);
            assert_eq!(state.entries[&id].record.version, record.version + 1);
            let grant = &state.grants[&handle.reader];
            assert_eq!(
                grant.renewal.as_ref().unwrap().request_id,
                request_identity(&newer).unwrap().0
            );
            assert_eq!(state.expirations.len(), 1);
            assert!(state
                .expirations
                .contains_key(&(grant.deadline, handle.reader.clone())));
            assert!(!state.closed.contains_key(&handle.reader));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        Ok(())
    }

    #[test]
    fn stalled_checkpoint_rejects_new_renewal_but_preserves_exact_retry() -> CommonResult<()> {
        if curvine_sys::lease_time_millis().is_err() {
            return Ok(());
        }
        let (_root, fs, id) = fixture()?;
        let protection = fs.fs_dir.read().hsm_read_protection.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let handle = open(&fs, &protection).await;
            let issued = curvine_runtime::common::LocalTime::mills();
            let request = serde_json::json!({
                "op":"read_renew", "read_mode":"fast_v1", "entity":handle.data.entity,
                "data":handle.data, "reader":handle.reader,
                "request_id":format!("{issued:016x}{}", token()),
            });
            let original_identity = request_identity(&request).unwrap().0;
            seed_proof(&fs, &protection);
            let original = match protection.execute(&fs, &request).await.unwrap() {
                Some(FastReply::Json(value)) => value,
                _ => panic!("expected renewal receipt"),
            };
            let old = std::time::Instant::now() - std::time::Duration::from_secs(2);
            let version = {
                let mut state = protection.state.lock().unwrap();
                state.oldest_dirty = Some(old);
                let entry = state.entries.get_mut(&id).unwrap();
                entry.dirty_since = old;
                entry.record.version
            };
            seed_proof(&fs, &protection);
            let repeated = match protection.execute(&fs, &request).await.unwrap() {
                Some(FastReply::Json(value)) => value,
                _ => panic!("expected duplicate renewal receipt"),
            };
            assert_eq!(
                repeated["data"]["expires_ms"],
                original["data"]["expires_ms"]
            );
            let mut newer = request;
            newer["request_id"] = serde_json::json!(format!("{:016x}{}", issued + 1, token()));
            seed_proof(&fs, &protection);
            assert!(matches!(
                protection.execute(&fs, &newer).await,
                Err(HsmError::Busy(_))
            ));
            let state = protection.state.lock().unwrap();
            assert_eq!(state.entries.get(&id).unwrap().record.version, version);
            assert_eq!(
                state
                    .grants
                    .get(&handle.reader)
                    .unwrap()
                    .renewal
                    .as_ref()
                    .unwrap()
                    .request_id,
                original_identity
            );
        });
        Ok(())
    }

    #[test]
    fn closed_history_retirement_is_bounded_and_keeps_a_monotonic_replay_floor() {
        let mut state = State::default();
        for issued in 1..=300 {
            let key = issued.to_string();
            state.closed.insert(key.clone(), issued);
            state.retirement.push_back((key, issued));
        }
        assert_eq!(state.retire_closed(10_000, 128), 128);
        assert_eq!(state.floor, 129);
        assert_eq!(state.closed.len(), 172);
        assert!(!state.closed.contains_key("1"));
        assert!(state.closed.contains_key("129"));
        assert_eq!(state.retire_closed(10_000, 1024), 172);
        assert_eq!(state.floor, 301);
        assert!(state.closed.is_empty());

        state.closed.insert("future".into(), 20_000);
        state.retirement.push_back(("future".into(), 20_000));
        assert_eq!(state.retire_closed(10_000, 1024), 0);
        assert_eq!(state.floor, 301);
        assert!(state.closed.contains_key("future"));
        assert_eq!(state.retire_closed(21_000, 1024), 1);
        assert_eq!(state.floor, 20_001);
        assert_eq!(state.retire_closed(10_000, 1024), 0);
        assert_eq!(state.floor, 20_001);
    }
    #[test]
    fn checkpoint_ack_preserves_close_tail_and_its_first_dirty_time() -> CommonResult<()> {
        let ns = Namespace {
            ordinary_format: 2,
            root_inode: 999,
            storage: "s".into(),
            incarnation: "n".into(),
            epoch: 1,
        };
        let file = InodeFile::new(42, 1000);
        let mut record = ordinary::native_record(&ns, "/file", &file, None)?.unwrap();
        let base = record.version;
        record.version += 1;
        record.state.revision += 1;
        record.readers.insert("reader".into(), 120_000);
        let captured = record.version;
        let old = std::time::Instant::now() - std::time::Duration::from_secs(2);
        let mut entry = Entry::observed(base, record, 1000, old, None);
        entry.capture();
        let before_close = std::time::Instant::now();
        let mut closed = entry.record.clone();
        closed.version += 1;
        closed.state.revision += 1;
        closed.readers.clear();
        entry = Entry::observed(base, closed, 1001, old, Some(&entry));
        entry.acknowledge(captured, base);
        assert_eq!(entry.durable, captured);
        assert_eq!(entry.record.version, captured + 1);
        assert!(
            entry.record.readers.is_empty(),
            "old checkpoint resurrected a closed reader"
        );
        assert!(
            entry.dirty_since >= before_close,
            "ack retained the old prefix's age for a new tail"
        );
        entry.capture();
        entry.acknowledge(captured + 1, base);
        assert_eq!(entry.record.version, entry.durable);
        assert!(entry.flight.is_none() && entry.tail_since.is_none());
        Ok(())
    }
}
