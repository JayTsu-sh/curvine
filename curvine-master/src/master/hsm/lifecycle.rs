// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Source-owned lifecycle: bounded native scans, atomic admission and durable claims.
use super::*;

const POLICIES: &[u8] = b"\x03hsm:policy:";
const INTENTS: &[u8] = b"\x03hsm:intent:";
const PAGE_MAX: usize = 16;
const SCAN_MAX: usize = 256;
const ADMISSION_MAX: usize = 16;
#[path = "scan.rs"]
mod scan;
use scan::ScanSnapshot;

fn policy_key(id: &str) -> HsmResult<Vec<u8>> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(HsmError::Invalid("invalid policy ID".into()));
    }
    Ok([POLICIES, id.as_bytes()].concat())
}
pub(super) fn intent_key(id: &str) -> HsmResult<Vec<u8>> {
    if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(HsmError::Invalid("invalid intent ID".into()));
    }
    Ok([INTENTS, id.as_bytes()].concat())
}
pub(super) fn active_key(data: &DataKey) -> Vec<u8> {
    format!("\u{3}hsm:active:{}", data.entity.entity).into_bytes()
}
fn encoded(value: &impl Serialize) -> HsmResult<Vec<u8>> {
    pack(value).map_err(io)
}
fn raw<T: DeserializeOwned>(value: Option<Vec<u8>>) -> HsmResult<Option<T>> {
    value.map(|v| decode(&v).map_err(io)).transpose()
}
fn required<T: DeserializeOwned>(value: Option<Vec<u8>>) -> HsmResult<T> {
    raw(value)?.ok_or_else(|| HsmError::NotFound("lifecycle record".into()))
}
fn effect_step(action: &ManagedAction) -> &'static str {
    match action {
        ManagedAction::Archive(_) => "commit_archive",
        ManagedAction::Release => "release",
        ManagedAction::Recall(_) => "commit_recall",
    }
}
pub(super) fn selected_metadata(policy: &LifecyclePolicy, record: &Record, now: i64) -> bool {
    if !policy.enabled
        || record.writing
        || record.pending.is_some()
        || policy
            .selection
            .owner
            .as_ref()
            .is_some_and(|o| o != &record.owner)
        || policy
            .selection
            .group
            .as_ref()
            .is_some_and(|g| g != &record.group)
        || now.saturating_sub(record.atime_ms) < policy.selection.min_access_age_ms as i64
        || now.saturating_sub(record.mtime_ms) < policy.selection.min_modify_age_ms as i64
        || record.state.length < policy.min_size
        || policy.max_size.is_some_and(|m| record.state.length > m)
        || policy.min_residency_ms > 0
            && (record.state_since_ms <= 0
                || now.saturating_sub(record.state_since_ms) < policy.min_residency_ms as i64)
    {
        return false;
    }
    match policy.action {
        PolicyAction::Archive => {
            record.state.residency == Residency::Online
                && (record.published.is_some() || record.native_blocks)
        }
        PolicyAction::Release => {
            matches!(record.state.residency, Residency::Online | Residency::Dual)
                && (record.published.is_some() || record.native_blocks)
                && record.views.is_empty()
                && (record.seal_token.is_none() || !record.sealed)
                && !ordinary::readers_active(record, now)
                && (record.state.residency == Residency::Online
                    || record
                        .state
                        .anchors
                        .iter()
                        .any(|a| a.key.backend == policy.backend))
        }
        PolicyAction::Recall => {
            record.state.residency == Residency::Offline
                && record
                    .state
                    .anchors
                    .iter()
                    .any(|a| a.key.backend == policy.backend)
        }
    }
}
fn selected_path(policy: &LifecyclePolicy, record: &Record) -> bool {
    !crate::master::meta::trash::is_trash_path(&record.path)
        && record.path.starts_with(&policy.prefix)
        && !policy
            .selection
            .exclude_prefixes
            .iter()
            .any(|p| record.path.starts_with(p))
}
fn selected(policy: &LifecyclePolicy, record: &Record, now: i64) -> bool {
    selected_metadata(policy, record, now) && selected_path(policy, record)
}

/// Only observational scan progress can omit durable request history. Actual
/// task admission and every file/workflow transition retain their outcomes.
pub(super) fn validate_scan_progress(tx: &Transaction) -> CommonResult<()> {
    if !tx.scan_progress_only {
        return Ok(());
    }
    if tx.workflow.is_some()
        || tx.inode.is_some()
        || tx.record.is_some()
        || tx.read_state.is_some()
        || tx.namespace.is_some()
        || tx.create
        || !tx.record_checks.is_empty()
        || tx.expected_version.is_some()
        || tx.expected_epoch.is_none()
        || tx.data.is_some()
        || tx.action.is_some()
        || tx.controls.is_empty()
        || tx.controls.len() > 2
        || tx.response["kind"] != "scan"
    {
        return Err("invalid scan-only transaction".into());
    }
    let result: ScanBatch = serde_json::from_value(tx.response["data"].clone())?;
    if !result.admitted.is_empty()
        || result.examined > SCAN_MAX
        || result.to_seal.len() > ADMISSION_MAX
    {
        return Err("scan-only transaction cannot admit work".into());
    }
    let policy = &tx.controls[0];
    if !policy.key.starts_with(POLICIES) || policy.expected != policy.value {
        return Err("scan-only transaction must assert its policy".into());
    }
    let value: LifecyclePolicy = required(policy.expected.clone())?;
    if policy.key != policy_key(&value.id)? {
        return Err("scan-only policy identity mismatch".into());
    }
    if let Some(progress) = tx.controls.get(1) {
        if progress.key != format!("\u{3}hsm:scan:{}:{}", value.id, value.revision).as_bytes()
            || progress.value.is_none()
        {
            return Err("scan-only transaction modified another control".into());
        }
        let _: PolicyRun = required(progress.value.clone())?;
    }
    Ok(())
}
pub(super) fn selected_current_path(
    policy: &LifecyclePolicy,
    record: &mut Record,
    now: i64,
    resolve: impl FnOnce() -> Option<String>,
) -> bool {
    if !selected_metadata(policy, record, now) {
        return false;
    }
    let Some(path) = resolve() else {
        return false;
    };
    record.path = path;
    selected_path(policy, record)
}
impl MasterFilesystem {
    pub(super) async fn hsm_lifecycle(
        &self,
        ns: &Namespace,
        ctx: &OperationContext,
        request: &Value,
    ) -> HsmResult<Value> {
        self.hsm_lifecycle_with_scan(ns, ctx, request, None).await
    }
    async fn hsm_lifecycle_with_scan(
        &self,
        ns: &Namespace,
        ctx: &OperationContext,
        request: &Value,
        snapshot: Option<ScanSnapshot>,
    ) -> HsmResult<Value> {
        let op = request["op"].as_str().unwrap();
        let key = operation_key(&ctx.operation_id, op);
        let fingerprint = request_fingerprint(request)?;
        let term = self
            .master_monitor
            .journal_term
            .load(std::sync::atomic::Ordering::Acquire);
        let (tx, checks) = {
            let fs = self.fs_dir.read();
            let store = fs.get_rocks_store();
            if matches!(op, "policy_list" | "intent_list") {
                let limit = request["limit"].as_u64().unwrap_or(16) as usize;
                if !(1..=PAGE_MAX).contains(&limit) {
                    return Err(HsmError::Invalid("page limit must be 1..16".into()));
                }
                let cursor = request.get("after").and_then(Value::as_str);
                let ready = op == "intent_list" && request["ready_only"].as_bool().unwrap_or(false);
                let prefix = if ready {
                    match request["work_class"].as_str() {
                        None if request["work_class"].is_null() => b"\x03hsm:work:".as_slice(),
                        Some("recall") => b"\x03hsm:work:0:".as_slice(),
                        Some("background") => b"\x03hsm:work:1:".as_slice(),
                        _ => {
                            return Err(HsmError::Invalid(
                                "work_class must be recall or background".into(),
                            ))
                        }
                    }
                } else if !request["work_class"].is_null() {
                    return Err(HsmError::Invalid(
                        "work_class requires a ready intent page".into(),
                    ));
                } else if op == "policy_list" {
                    POLICIES
                } else {
                    INTENTS
                };
                // Keep opaque work cursors in the existing priority:id format,
                // including class-filtered pages. Cross-class cursors reject.
                let cursor_prefix = if ready {
                    b"\x03hsm:work:".as_slice()
                } else {
                    prefix
                };
                let after = cursor
                    .map(|id| {
                        if op == "policy_list" {
                            policy_key(id)
                        } else if ready {
                            let key = [cursor_prefix, id.as_bytes()].concat();
                            if !key.starts_with(prefix) {
                                return Err(HsmError::Invalid(
                                    "cursor belongs to another work class".into(),
                                ));
                            }
                            Ok(key)
                        } else {
                            intent_key(id)
                        }
                    })
                    .transpose()?;
                let mut page = store
                    .hsm_scan(prefix, after.as_deref(), limit + 1)
                    .map_err(io)?;
                let more = page.len() > limit;
                page.truncate(limit);
                let next = if more {
                    page.last().map(|(k, _)| {
                        String::from_utf8_lossy(&k[cursor_prefix.len()..]).into_owned()
                    })
                } else {
                    None
                };
                let items: Vec<Value> = page
                    .iter()
                    .map(|(_, v)| {
                        if ready {
                            let id: String = decode(v).map_err(io)?;
                            let bytes = store
                                .hsm_get(&intent_key(&id)?)
                                .map_err(io)?
                                .ok_or_else(|| HsmError::NotFound("queued task".into()))?;
                            decode(&bytes).map_err(io)
                        } else {
                            decode(v).map_err(io)
                        }
                    })
                    .collect::<HsmResult<_>>()?;
                return Ok(reply("page", LifecyclePage { items, next }));
            }
            if op == "intent_get" {
                let id: String = field(request, "id")?;
                let intent: LifecycleIntent =
                    required(store.hsm_get(&intent_key(&id)?).map_err(io)?)?;
                return Ok(reply("intent", intent));
            }
            if let Some(old) = fs.hsm_outcome(&key).map_err(io)? {
                if old.fingerprint != fingerprint {
                    return Err(HsmError::Conflict("lifecycle request ID rebound".into()));
                }
                return Ok(old.response);
            }
            let now = curvine_runtime::common::LocalTime::mills() as i64;
            let mut checks = lifecycle_commit::Checks::new(&fs)?;
            let mut tx = Transaction {
                scan_progress_only: false,
                read_state: None,
                controls: vec![],
                record_checks: vec![],
                workflow: Some(WorkflowBinding {
                    operation_id: ctx.operation_id.clone(),
                    kind: "lifecycle".into(),
                    data: None,
                }),
                outcome: key,
                fingerprint,
                response: Value::Null,
                inode: None,
                expected_version: None,
                record: None,
                namespace: None,
                expected_epoch: Some(ns.epoch),
                create: false,
                time: now,
                data: None,
                action: None,
            };
            match op {
                "policy_put" => {
                    let policy: LifecyclePolicy = field(request, "policy")?;
                    policy.validate()?;
                    if !policy.prefix.starts_with('/')
                        || policy.prefix.contains('\0')
                        || policy.prefix.len() > 264
                    {
                        return Err(HsmError::Invalid(
                            "policy prefix must be an absolute path prefix".into(),
                        ));
                    }
                    let key = policy_key(&policy.id)?;
                    let previous = store.hsm_get(&key).map_err(io)?;
                    let old: Option<LifecyclePolicy> = raw(previous.clone())?;
                    let expected: Option<u64> = field(request, "expected_revision")?;
                    if old.as_ref().map(|p| p.revision) != expected
                        || policy.revision
                            != expected.unwrap_or(0).checked_add(1).ok_or_else(|| {
                                HsmError::Invalid("policy revision overflow".into())
                            })?
                    {
                        return Err(HsmError::Conflict("policy revision changed".into()));
                    }
                    tx.controls.push(ControlChange {
                        key,
                        expected: previous,
                        value: Some(encoded(&policy)?),
                    });
                    tx.response = reply("policy", policy);
                }
                "policy_scan" => {
                    let id: String = field(request, "id")?;
                    let pkey = policy_key(&id)?;
                    let pbytes = store.hsm_get(&pkey).map_err(io)?;
                    if snapshot.as_ref().is_some_and(|s| s.policy_bytes != pbytes) {
                        return Err(HsmError::Conflict(
                            "policy changed during snapshot scan".into(),
                        ));
                    }
                    let policy: LifecyclePolicy = required(pbytes.clone())?;
                    let limit = request["limit"].as_u64().unwrap_or(16) as usize;
                    if !(1..=SCAN_MAX).contains(&limit) {
                        return Err(HsmError::Invalid("scan limit must be 1..256".into()));
                    }
                    let scheduled = request["scheduled"].as_bool().unwrap_or(false);
                    let rkey =
                        format!("\u{3}hsm:scan:{}:{}", policy.id, policy.revision).into_bytes();
                    let runbytes = store.hsm_get(&rkey).map_err(io)?;
                    if snapshot.as_ref().is_some_and(|s| s.run_bytes != runbytes) {
                        return Err(HsmError::Conflict(
                            "scan progress changed during snapshot scan".into(),
                        ));
                    }
                    let mut run: PolicyRun = raw(runbytes.clone())?.unwrap_or_default();
                    if scheduled && (run.next_due_ms > now || !policy.enabled) {
                        return Ok(reply(
                            "scan",
                            ScanBatch {
                                examined: 0,
                                admitted: vec![],
                                next: None,
                                to_seal: vec![],
                            },
                        ));
                    }
                    let mut budget_bytes = u64::MAX;
                    if let Some(w) = &policy.selection.capacity {
                        let info = self.filesystem_info().map_err(io)?;
                        let capacity = info.allocatable_capacity.max(0) as u64;
                        checks.capacity(capacity, info.allocatable_available.max(0) as u64);
                        checks.common(&fs, RESERVATION_TOTAL)?;
                        // Actual worker disk usage includes replicas and non-Curvine occupants.
                        let (active, target) = pressure_target(
                            run.pressure,
                            capacity,
                            info.allocatable_available.max(0) as u64,
                            w.low_basis_points,
                            w.high_basis_points,
                        );
                        run.pressure = active;
                        if !active {
                            run.cursor = None;
                            run.admitted = 0;
                            budget_bytes = 0;
                        } else {
                            budget_bytes = target.saturating_sub(outstanding_bytes(&fs, &policy)?);
                        }
                    }
                    let states: &[char] = match policy.action {
                        PolicyAction::Archive => &['o'],
                        PolicyAction::Release => &['d', 'o'],
                        PolicyAction::Recall => &['f'],
                    };
                    let requested = request
                        .get("after")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    let cursor = if scheduled {
                        run.cursor.clone()
                    } else {
                        requested
                    };
                    let state = cursor
                        .as_ref()
                        .and_then(|s| s.split(':').nth(2))
                        .and_then(|s| s.chars().next())
                        .filter(|s| states.contains(s))
                        .unwrap_or(states[0]);
                    let prefix = format!("\u{3}hsm:candidate:{state}:").into_bytes();
                    if snapshot
                        .as_ref()
                        .is_some_and(|s| s.cursor != cursor || s.state != state)
                    {
                        return Err(HsmError::Conflict(
                            "scan cursor changed during snapshot scan".into(),
                        ));
                    }
                    let mut page = if budget_bytes == 0 {
                        vec![]
                    } else if let Some(snapshot) = &snapshot {
                        snapshot.rows.clone()
                    } else {
                        store
                            .hsm_scan(
                                &prefix,
                                cursor
                                    .as_deref()
                                    .filter(|s| s.as_bytes().starts_with(&prefix))
                                    .map(str::as_bytes),
                                limit + 1,
                            )
                            .map_err(io)?
                    };
                    let more = budget_bytes != 0
                        && snapshot.as_ref().map_or(page.len() > limit, |s| s.more);
                    page.truncate(limit);
                    let mut next = if more {
                        page.last()
                            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
                    } else {
                        states
                            .iter()
                            .position(|s| *s == state)
                            .and_then(|i| states.get(i + 1))
                            .map(|s| format!("\u{3}hsm:candidate:{s}:"))
                    };
                    let mut admitted = vec![];
                    let mut to_seal = vec![];
                    tx.controls.push(ControlChange {
                        key: pkey,
                        expected: pbytes.clone(),
                        value: pbytes,
                    });
                    let mut visited: Option<String> = None;
                    for (candidate_key, bytes) in &page {
                        if admitted.len() + to_seal.len() >= ADMISSION_MAX {
                            next = visited.clone();
                            break;
                        }
                        if run.admitted + to_seal.len() >= policy.selection.max_candidates
                            || budget_bytes == 0
                        {
                            break;
                        }
                        visited = Some(String::from_utf8_lossy(candidate_key).into_owned());
                        let rid: String = decode(bytes).map_err(io)?;
                        let inode_id = rid.parse().map_err(io)?;
                        let mut record = if let Some(snapshot) = &snapshot {
                            let Some(record) = snapshot.records.get(&inode_id) else {
                                continue;
                            };
                            // Snapshot filtering never authorizes admission: reads,
                            // writes and tier transitions may have happened since.
                            if fs
                                .hsm_record(inode_id)
                                .map_err(io)?
                                .as_ref()
                                .map(|r| r.version)
                                != Some(record.version)
                            {
                                continue;
                            }
                            record.clone()
                        } else {
                            let Some(record) = fs.hsm_record(inode_id).map_err(io)? else {
                                continue;
                            };
                            record
                        };
                        if policy.selection.min_access_age_ms > 0
                            && record.atime_ms
                                > now.saturating_sub(policy.selection.min_access_age_ms as i64)
                        {
                            next = states
                                .iter()
                                .position(|s| *s == state)
                                .and_then(|i| states.get(i + 1))
                                .map(|s| format!("\u{3}hsm:candidate:{s}:"));
                            break;
                        }
                        // Reject cheap metadata conditions before resolving the
                        // current namespace path. Never trust the cached path
                        // for directory renames or membership decisions.
                        let inode_id = rid.parse().map_err(io)?;
                        if !selected_current_path(&policy, &mut record, now, || {
                            fs.get_inode_path(inode_id).ok()
                        }) {
                            continue;
                        }
                        if policy.action == PolicyAction::Release
                            && fs.hsm_readers_active(&record).map_err(io)?
                        {
                            continue;
                        }
                        if !record.sealed {
                            to_seal.push(record.state.data.clone());
                            continue;
                        }
                        let data = &record.state.data;
                        let archive_phase = policy.action == PolicyAction::Archive
                            || policy.action == PolicyAction::Release
                                && record.state.residency == Residency::Online;
                        let phase = if archive_phase {
                            "archive"
                        } else if policy.action == PolicyAction::Release {
                            "release"
                        } else {
                            "recall"
                        };
                        // A new residency interval permits a later cold/hot cycle under the same policy revision.
                        let iid = blake3::hash(&encoded(&(
                            &policy.id,
                            policy.revision,
                            data,
                            phase,
                            record.state_since_ms,
                            record.atime_ms,
                            record.state.revision,
                        ))?)
                        .to_hex()
                        .to_string();
                        let ikey = intent_key(&iid)?;
                        let active = active_key(data);
                        if store.hsm_get(&ikey).map_err(io)?.is_some()
                            || store.hsm_get(&active).map_err(io)?.is_some()
                        {
                            continue;
                        }
                        let archive = ArchiveKey {
                            backend: policy.backend.clone(),
                            object: format!("policy-{iid}"),
                        };
                        let action = if archive_phase {
                            ManagedAction::Archive(archive)
                        } else {
                            match policy.action {
                                PolicyAction::Release => ManagedAction::Release,
                                PolicyAction::Recall => ManagedAction::Recall(
                                    record
                                        .state
                                        .anchors
                                        .iter()
                                        .find(|a| a.key.backend == policy.backend)
                                        .unwrap()
                                        .key
                                        .clone(),
                                ),
                                _ => unreachable!(),
                            }
                        };
                        let intent = LifecycleIntent {
                            id: iid.clone(),
                            policy: policy.id.clone(),
                            policy_revision: policy.revision,
                            version: 1,
                            expected: record.state.clone(),
                            action,
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
                        tx.record_checks
                            .push((entity_id(&data.entity, ns)?, record.version));
                        tx.controls.push(ControlChange {
                            key: ikey,
                            expected: None,
                            value: Some(encoded(&intent)?),
                        });
                        tx.controls.push(ControlChange {
                            key: active,
                            expected: None,
                            value: Some(encoded(&iid)?),
                        });
                        let physical =
                            record.state.length.saturating_mul(if record.native_blocks {
                                fs.store
                                    .get_inode(rid.parse().map_err(io)?, None)
                                    .map_err(io)?
                                    .and_then(|i| {
                                        i.as_file_ref().ok().map(|f| f.replicas.max(1) as u64)
                                    })
                                    .unwrap_or(1)
                            } else {
                                1
                            });
                        checks.file(
                            inode_id,
                            Some(&record),
                            Some(record.path.clone()),
                            lifecycle_commit::copies(&fs, inode_id).map_err(io)?,
                        )?;
                        budget_bytes = budget_bytes.saturating_sub(physical);
                        run.admitted += 1;
                        admitted.push(iid);
                    }
                    if budget_bytes == 0 {
                        next = None;
                    }
                    if !to_seal.is_empty() {
                        next = cursor
                            .clone()
                            .or_else(|| Some(format!("\u{3}hsm:candidate:{state}:")));
                    }
                    if admitted.is_empty() && !scheduled && policy.selection.capacity.is_none() {
                        // Observational manual pages have no durable effect or
                        // workflow success to retain. Retrying simply rescans.
                        return Ok(reply(
                            "scan",
                            ScanBatch {
                                examined: page.len(),
                                admitted,
                                next,
                                to_seal,
                            },
                        ));
                    }
                    if scheduled || policy.selection.capacity.is_some() {
                        run.cursor = next.clone();
                        if next.is_none() || run.admitted >= policy.selection.max_candidates {
                            run.next_due_ms = now + policy.selection.scan_interval_ms as i64;
                            run.cursor = None;
                            run.admitted = 0;
                            next = None;
                        }
                        tx.controls.push(ControlChange {
                            key: rkey,
                            expected: runbytes,
                            value: Some(encoded(&run)?),
                        });
                    }
                    tx.response = reply(
                        "scan",
                        ScanBatch {
                            examined: page.len(),
                            admitted,
                            next,
                            to_seal,
                        },
                    );
                    if tx.record_checks.is_empty() {
                        tx.scan_progress_only = true;
                        tx.workflow = None;
                    }
                }
                "intent_claim" | "intent_finish" => {
                    let id: String = field(request, "id")?;
                    let ikey = intent_key(&id)?;
                    let previous = store.hsm_get(&ikey).map_err(io)?;
                    let mut intent: LifecycleIntent = required(previous.clone())?;
                    if matches!(
                        intent.status,
                        IntentStatus::Done | IntentStatus::Failed | IntentStatus::Superseded
                    ) {
                        return Ok(reply("intent", intent));
                    }
                    if op == "intent_claim" {
                        let owner: String = field(request, "owner")?;
                        if owner.is_empty() || owner.len() > 128 {
                            return Err(HsmError::Invalid(
                                "worker owner length must be 1..128".into(),
                            ));
                        }
                        if intent.claim.as_ref().is_some_and(|c| c.epoch == ns.epoch) {
                            return Err(HsmError::Busy("intent owned in current controller epoch; takeover requires a new epoch".into()));
                        }
                        let policy: Option<LifecyclePolicy> = if intent.policy == "__access_recall"
                        {
                            None
                        } else {
                            let key = policy_key(&intent.policy)?;
                            let bytes = store.hsm_get(&key).map_err(io)?;
                            let p = required(bytes.clone())?;
                            tx.controls.push(ControlChange {
                                key,
                                expected: bytes.clone(),
                                value: bytes,
                            });
                            Some(p)
                        };
                        let effect = outcome_key(&operation_key(
                            &intent.context.operation_id,
                            effect_step(&intent.action),
                        ));
                        let observed: Option<Outcome> = raw(checks.common(&fs, &effect)?)?;
                        let success = observed.filter(|o| {
                            o.data.as_ref() == Some(&intent.expected.data)
                                && o.action.as_ref() == Some(&intent.action)
                                && o.response["kind"] == "entity"
                        });
                        let observed_record = if success.is_none() {
                            checks.record(&fs, entity_id(&intent.expected.data.entity, ns)?)?
                        } else {
                            None
                        };
                        if let Some(outcome) = success {
                            intent.status = IntentStatus::Done;
                            intent.result = Some(field(&outcome.response, "data")?);
                            intent.last_error = None;
                        } else if observed_record
                            .as_ref()
                            .is_none_or(|record| record.state != intent.expected)
                        {
                            intent.status = IntentStatus::Superseded;
                            intent.last_error = Some("source observation changed".into());
                        } else if policy
                            .as_ref()
                            .is_some_and(|p| !p.enabled || p.revision != intent.policy_revision)
                        {
                            intent.status = IntentStatus::Superseded;
                            intent.last_error = Some("admitting policy disabled or revised".into());
                        } else {
                            if let Some(policy) = &policy {
                                let record = observed_record
                                    .as_ref()
                                    .ok_or_else(|| HsmError::NotFound("intent record".into()))?;
                                if !selected(policy, record, now) {
                                    return Err(HsmError::Busy(
                                        "policy eligibility changed; defer candidate".into(),
                                    ));
                                }
                                if let Some(w) = &policy.selection.capacity {
                                    let info = self.filesystem_info().map_err(io)?;
                                    let capacity = info.allocatable_capacity.max(0) as u64;
                                    checks.capacity(
                                        capacity,
                                        info.allocatable_available.max(0) as u64,
                                    );
                                    let used = capacity
                                        .saturating_sub(info.allocatable_available.max(0) as u64);
                                    if capacity == 0
                                        || used as u128 * 10000
                                            <= capacity as u128 * w.low_basis_points as u128
                                    {
                                        return Err(HsmError::Busy(
                                            "online usage reached low watermark".into(),
                                        ));
                                    }
                                }
                            }
                            intent.context.controller_epoch = ns.epoch;
                            intent.claim = Some(IntentClaim {
                                owner,
                                id: token(),
                                epoch: ns.epoch,
                            });
                            intent.status = IntentStatus::Running;
                        }
                    } else {
                        let version: u64 = field(request, "version")?;
                        let claim: IntentClaim = field(request, "claim")?;
                        if intent.version != version
                            || intent.claim.as_ref() != Some(&claim)
                            || claim.epoch != ns.epoch
                            || intent.status != IntentStatus::Running
                        {
                            return Err(HsmError::Fenced);
                        }
                        let report: IntentCompletion = field(request, "report")?;
                        let effect = outcome_key(&operation_key(
                            &intent.context.operation_id,
                            effect_step(&intent.action),
                        ));
                        let outcome: Option<Outcome> = raw(checks.common(&fs, &effect)?)?;
                        let success = outcome.filter(|o| {
                            o.data.as_ref() == Some(&intent.expected.data)
                                && o.action.as_ref() == Some(&intent.action)
                                && o.response["kind"] == "entity"
                        });
                        if let Some(outcome) = success {
                            intent.result = Some(field(&outcome.response, "data")?);
                            intent.status = IntentStatus::Done;
                            intent.last_error = None;
                        } else {
                            let (status, message) = match report {
                                IntentCompletion::Done => {
                                    return Err(HsmError::Busy(
                                        "source has no confirmed successful effect".into(),
                                    ))
                                }
                                IntentCompletion::Retry(message) => (IntentStatus::Ready, message),
                                IntentCompletion::Failed(message) => {
                                    (IntentStatus::Failed, message)
                                }
                                IntentCompletion::Superseded(message) => {
                                    (IntentStatus::Superseded, message)
                                }
                            };
                            if message.len() > 2048 {
                                return Err(HsmError::Invalid("error message too long".into()));
                            }
                            intent.status = status;
                            intent.last_error = Some(message);
                        }
                        intent.claim = None;
                    }
                    if matches!(
                        intent.status,
                        IntentStatus::Done | IntentStatus::Failed | IntentStatus::Superseded
                    ) {
                        intent.claim = None;
                        tx.controls.push(ControlChange {
                            key: active_key(&intent.expected.data),
                            expected: Some(encoded(&id)?),
                            value: None,
                        });
                    }
                    intent.version = intent
                        .version
                        .checked_add(1)
                        .ok_or_else(|| HsmError::Conflict("intent version overflow".into()))?;
                    tx.controls.push(ControlChange {
                        key: ikey,
                        expected: previous,
                        value: Some(encoded(&intent)?),
                    });
                    tx.response = reply("intent", intent);
                }
                _ => return Err(HsmError::Unsupported(format!("lifecycle operation {op}"))),
            }
            (tx, checks)
        };
        self.hsm_submit_lifecycle(lifecycle_commit::Command { tx, checks }, term)
            .await
    }
}

#[derive(Default, Serialize, serde::Deserialize)]
struct PolicyRun {
    cursor: Option<String>,
    next_due_ms: i64,
    admitted: usize,
    pressure: bool,
}
pub(super) const RESERVATION_TOTAL: &[u8] = b"\x03hsm:budget:online:v1";

fn outstanding_bytes(fs: &FsDir, _policy: &LifecyclePolicy) -> HsmResult<u64> {
    reservation_total(fs).map_err(io)
}

/// Derived shared budget, updated atomically with reservation rows. Old
/// checkpoints without the aggregate rebuild in bounded pages once, then the
/// next committed lifecycle command installs the aggregate.
pub(super) fn reservation_total(fs: &FsDir) -> CommonResult<u64> {
    if let Some(value) = fs.get_rocks_store().hsm_get(RESERVATION_TOTAL)? {
        return decode(&value);
    }
    let prefix = b"\x03hsm:reserved:online:".to_vec();
    let mut bytes = 0u64;
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = fs
            .get_rocks_store()
            .hsm_scan(&prefix, cursor.as_deref(), 128)?;
        if page.is_empty() {
            break;
        }
        for (_, value) in &page {
            bytes = bytes
                .checked_add(decode::<u64>(value)?)
                .ok_or("HSM reservation budget overflow")?;
        }
        cursor = page.last().map(|(key, _)| key.clone());
    }
    Ok(bytes)
}

fn pressure_target(
    active: bool,
    capacity: u64,
    available: u64,
    low_bps: u16,
    high_bps: u16,
) -> (bool, u64) {
    let used = capacity.saturating_sub(available);
    let low = (capacity as u128 * low_bps as u128 / 10000) as u64;
    let high = (capacity as u128 * high_bps as u128 / 10000) as u64;
    let active = capacity != 0 && used > low && (active || used >= high);
    (active, if active { used.saturating_sub(low) } else { 0 })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scan_progress_returns_apply_result_without_retaining_workflow_history() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let mut dir = fs.fs_dir.write();
        dir.hsm_enabled = true;
        let namespace = Namespace {
            ordinary_format: 2,
            root_inode: 1,
            storage: "s".into(),
            incarnation: "n".into(),
            epoch: 1,
        };
        let policy: LifecyclePolicy = serde_json::from_value(serde_json::json!({
            "id":"cold", "revision":1, "enabled":true, "prefix":"/data/",
            "min_size":0,"max_size":null,"min_residency_ms":0,"action":"release","backend":"local"
        }))?;
        let pkey = policy_key(&policy.id)?;
        let policy = pack(&policy)?;
        let progress = b"\x03hsm:scan:cold:1".to_vec();
        let run = PolicyRun {
            cursor: Some("after".into()),
            next_due_ms: 0,
            admitted: 0,
            pressure: false,
        };
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(NS, &pack(&namespace)?)?;
        batch.hsm_put(&pkey, &policy)?;
        batch.commit()?;
        let mut tx = Transaction {
            scan_progress_only: true,
            read_state: None,
            controls: vec![
                ControlChange {
                    key: pkey,
                    expected: Some(policy.clone()),
                    value: Some(policy),
                },
                ControlChange {
                    key: progress.clone(),
                    expected: None,
                    value: Some(pack(&run)?),
                },
            ],
            record_checks: vec![],
            workflow: None,
            outcome: "empty-page".into(),
            fingerprint: "scan".into(),
            response: reply(
                "scan",
                ScanBatch {
                    examined: 256,
                    admitted: vec![],
                    next: Some("after".into()),
                    to_seal: vec![],
                },
            ),
            inode: None,
            expected_version: None,
            record: None,
            namespace: None,
            expected_epoch: Some(1),
            create: false,
            time: 1000,
            data: None,
            action: None,
        };
        let (reply_tx, result) = tokio::sync::oneshot::channel();
        dir.hsm_local_apply.insert("receipt".into(), reply_tx);
        dir.apply_hsm_entry(&HsmEntry {
            op_id: 1,
            ticket: "receipt".into(),
            payload: pack(&tx)?,
        })?;
        assert_eq!(result.blocking_recv().unwrap()[0]["kind"], "scan");
        assert_eq!(dir.get_rocks_store().hsm_get(&progress)?, Some(pack(&run)?));
        assert!(dir.hsm_outcome("empty-page")?.is_none());
        assert!(dir
            .get_rocks_store()
            .hsm_scan(b"\x03hsm:binding:", None, 1)?
            .is_empty());
        // A lost reply can cause progress to be retried. CAS prevents stale
        // progress from moving the cursor backwards, without retaining a row.
        assert_eq!(
            dir.apply_hsm_results(&pack(&tx)?)?[0]["data"]["code"],
            "conflict"
        );
        assert!(dir.hsm_outcome("empty-page")?.is_none());
        tx.controls[1].key = b"\x03hsm:intent:forbidden".to_vec();
        assert!(dir.apply_hsm(&pack(&tx)?).is_err());
        assert!(dir
            .get_rocks_store()
            .hsm_get(&tx.controls[1].key)?
            .is_none());
        Ok(())
    }

    #[test]
    fn closed_unpinned_files_are_filtered_by_access_age_size_and_exclusions() {
        let policy: LifecyclePolicy = serde_json::from_value(serde_json::json!({
            "id":"cold", "revision":1, "enabled":true, "prefix":"/data/",
            "min_size":1048576, "max_size":null, "min_residency_ms":0,
            "action":"release", "backend":"local",
            "selection":{"min_access_age_ms":5000, "min_modify_age_ms":1000,
                "exclude_prefixes":["/data/tmp/"]}
        }))
        .unwrap();
        let mut record: Record = serde_json::from_value(serde_json::json!({
            "version":1, "ordinary":true, "native_blocks":true, "sealed":true,
            "atime_ms":9900, "mtime_ms":1000, "state_since_ms":1000,
            "state":{"data":{"entity":{"storage":"s","namespace":"n","entity":"1001"},"generation":1},
                "revision":1,"residency":"Online","length":2097152,
                "digest":{"algorithm":"blake3","value":vec![1;32]}, "anchors":[]},
            "path":"/data/a", "published":null,"pending":null,"views":{},"garbage":[]
        })).unwrap();
        assert!(record.readers.is_empty());
        assert!(!selected(&policy, &record, 10000));
        record.atime_ms = 1000;
        assert!(selected(&policy, &record, 10000));
        record.state.length = 1024;
        assert!(!selected(&policy, &record, 10000));
        record.state.length = 2097152;
        record.path = "/data/tmp/a".into();
        assert!(!selected(&policy, &record, 10000));

        let mut all_paths = policy.clone();
        all_paths.prefix = "/".into();
        all_paths.selection.exclude_prefixes.clear();
        record.path = "/data/a".into();
        assert!(!selected_current_path(
            &all_paths,
            &mut record,
            10000,
            || { Some("/.Trash/123-1001/nested/a".into()) }
        ));
        assert!(!selected(&all_paths, &record, 10000));
        assert!(selected_current_path(
            &all_paths,
            &mut record,
            10000,
            || { Some("/data/restored".into()) }
        ));
    }
    #[test]
    fn watermarks_start_at_high_continue_between_and_stop_at_low() {
        assert_eq!(pressure_target(false, 1000, 201, 6000, 8000), (false, 0));
        assert_eq!(pressure_target(false, 1000, 200, 6000, 8000), (true, 200));
        assert_eq!(pressure_target(true, 1000, 300, 6000, 8000), (true, 100));
        assert_eq!(pressure_target(false, 1000, 300, 6000, 8000), (false, 0));
        assert_eq!(pressure_target(true, 1000, 400, 6000, 8000), (false, 0));
        assert_eq!(pressure_target(true, 0, 0, 6000, 8000), (false, 0));
    }
}

pub(super) fn work_key(intent: &LifecycleIntent) -> Vec<u8> {
    let priority = if matches!(intent.action, ManagedAction::Recall(_)) {
        0
    } else {
        1
    };
    format!("\u{3}hsm:work:{priority}:{}", intent.id).into_bytes()
}

#[cfg(test)]
mod work_class_tests {
    use super::*;
    #[test]
    fn work_class_pages_seek_the_selected_index_and_preserve_opaque_cursors() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let ns = Namespace {
            ordinary_format: 2,
            root_inode: 1,
            storage: "s".into(),
            incarnation: "n".into(),
            epoch: 1,
        };
        {
            let mut dir = fs.fs_dir.write();
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(NS, &pack(&ns)?)?;
            batch.commit()?;
            dir.hsm_enabled = true;
        }
        let file = fs.create("/source", false)?;
        fs.complete_file("/source", Some(file.id), 0, vec![], "", false, None)?;
        let expected = fs.fs_dir.read().hsm_record(file.id)?.unwrap().state;
        let ctx = OperationContext {
            operation_id: "query-work".into(),
            controller_epoch: 1,
            attempt_epoch: 1,
        };
        {
            let dir = fs.fs_dir.read();
            let mut batch = dir.get_rocks_store().new_batch();
            for (class, count) in [("recall", 1000), ("background", 2)] {
                for index in 0..count {
                    let intent = LifecycleIntent {
                        id: format!("{}{index:063x}", if class == "recall" { "0" } else { "1" }),
                        policy: "policy".into(),
                        policy_revision: 1,
                        version: 1,
                        expected: expected.clone(),
                        action: if class == "recall" {
                            ManagedAction::Recall(ArchiveKey {
                                backend: "local".into(),
                                object: format!("object-{index}"),
                            })
                        } else {
                            ManagedAction::Release
                        },
                        context: ctx.clone(),
                        status: IntentStatus::Ready,
                        claim: None,
                        last_error: None,
                        result: None,
                    };
                    batch.hsm_put(&intent_key(&intent.id)?, &pack(&intent)?)?;
                    batch.hsm_put(&work_key(&intent), &pack(&intent.id)?)?;
                }
            }
            batch.commit()?;
        }
        let before = fs
            .fs_dir
            .read()
            .get_rocks_store()
            .db
            .get_db()
            .latest_sequence_number();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let request = serde_json::json!({"op":"intent_list", "ready_only":true, "work_class":"background", "limit":1});
            let first = fs.hsm_lifecycle(&ns, &ctx, &request).await?;
            assert_eq!(first["data"]["items"][0]["id"], format!("1{:063x}", 0));
            assert_eq!(first["data"]["next"], format!("1:1{:063x}", 0));
            let mut next = request.clone();
            next["after"] = first["data"]["next"].clone();
            let second = fs.hsm_lifecycle(&ns, &ctx, &next).await?;
            assert_eq!(second["data"]["items"][0]["id"], format!("1{:063x}", 1));
            assert!(second["data"]["next"].is_null());
            let mut recall = request.clone();
            recall["work_class"] = Value::String("recall".into());
            let first_recall = fs.hsm_lifecycle(&ns, &ctx, &recall).await?;
            assert_eq!(first_recall["data"]["items"][0]["id"], format!("0{:063x}", 0));
            recall["after"] = first["data"]["next"].clone();
            assert!(matches!(fs.hsm_lifecycle(&ns, &ctx, &recall).await, Err(HsmError::Invalid(_))));
            recall["after"] = Value::Null;
            recall["work_class"] = Value::String("bad-class".into());
            assert!(matches!(fs.hsm_lifecycle(&ns, &ctx, &recall).await, Err(HsmError::Invalid(_))));
            let mut all = request.clone();
            all["work_class"] = Value::Null;
            assert_eq!(fs.hsm_lifecycle(&ns, &ctx, &all).await?["data"]["items"][0]["id"], format!("0{:063x}", 0));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })?;
        assert_eq!(
            fs.fs_dir
                .read()
                .get_rocks_store()
                .db
                .get_db()
                .latest_sequence_number(),
            before
        );
        Ok(())
    }
}
