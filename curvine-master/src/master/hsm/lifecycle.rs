// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Source-owned lifecycle: bounded native scans, atomic admission and durable claims.
use super::*;

const POLICIES: &[u8] = b"\x03hsm:policy:";
const INTENTS: &[u8] = b"\x03hsm:intent:";
const PAGE_MAX: usize = 16;
const SCAN_MAX: usize = 256;

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
fn selected(policy: &LifecyclePolicy, record: &Record, now: i64) -> bool {
    if !policy.enabled
        || record.writing
        || record.pending.is_some()
        || policy
            .selection
            .exclude_prefixes
            .iter()
            .any(|p| record.path.starts_with(p))
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
        || !record.path.starts_with(&policy.prefix)
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
impl MasterFilesystem {
    pub(super) async fn hsm_lifecycle(
        &self,
        ns: &Namespace,
        ctx: &OperationContext,
        request: &Value,
    ) -> HsmResult<Value> {
        let op = request["op"].as_str().unwrap();
        let key = operation_key(&ctx.operation_id, op);
        let fingerprint = request_fingerprint(request)?;
        let tx = {
            let fs = self.fs_dir.read();
            let store = fs.get_rocks_store();
            if matches!(op, "policy_list" | "intent_list") {
                let limit = request["limit"].as_u64().unwrap_or(16) as usize;
                if !(1..=PAGE_MAX).contains(&limit) {
                    return Err(HsmError::Invalid("page limit must be 1..16".into()));
                }
                let cursor = request.get("after").and_then(Value::as_str);
                let prefix =
                    if op == "intent_list" && request["ready_only"].as_bool().unwrap_or(false) {
                        b"\x03hsm:work:".as_slice()
                    } else if op == "policy_list" {
                        POLICIES
                    } else {
                        INTENTS
                    };
                let after = cursor
                    .map(|id| {
                        if op == "policy_list" {
                            policy_key(id)
                        } else if prefix == b"\x03hsm:work:" {
                            Ok([prefix, id.as_bytes()].concat())
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
                    page.last()
                        .map(|(k, _)| String::from_utf8_lossy(&k[prefix.len()..]).into_owned())
                } else {
                    None
                };
                let items: Vec<Value> = page
                    .iter()
                    .map(|(_, v)| {
                        if prefix == b"\x03hsm:work:" {
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
            let mut tx = Transaction {
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
                    let policy: LifecyclePolicy = required(pbytes.clone())?;
                    let limit = request["limit"].as_u64().unwrap_or(16) as usize;
                    if !(1..=SCAN_MAX).contains(&limit) {
                        return Err(HsmError::Invalid("scan limit must be 1..256".into()));
                    }
                    let scheduled = request["scheduled"].as_bool().unwrap_or(false);
                    let rkey =
                        format!("\u{3}hsm:scan:{}:{}", policy.id, policy.revision).into_bytes();
                    let runbytes = store.hsm_get(&rkey).map_err(io)?;
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
                    let mut page = if budget_bytes == 0 {
                        vec![]
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
                    let more = page.len() > limit;
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
                    for (_, bytes) in &page {
                        if run.admitted + to_seal.len() >= policy.selection.max_candidates
                            || budget_bytes == 0
                        {
                            break;
                        }
                        let rid: String = decode(bytes).map_err(io)?;
                        let Some(bytes) = store
                            .hsm_get(&record_key(rid.parse().map_err(io)?))
                            .map_err(io)?
                        else {
                            continue;
                        };
                        let mut record: Record = decode(&bytes).map_err(io)?;
                        // Resolve the authoritative inode identity; namespace renames do not need an external mirror.
                        record.path = match fs.get_inode_path(rid.parse().map_err(io)?) {
                            Ok(p) => p,
                            Err(_) => continue,
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
                        if !selected(&policy, &record, now) {
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
                        let success = fs
                            .hsm_outcome(&operation_key(
                                &intent.context.operation_id,
                                effect_step(&intent.action),
                            ))
                            .map_err(io)?
                            .filter(|o| {
                                o.data.as_ref() == Some(&intent.expected.data)
                                    && o.action.as_ref() == Some(&intent.action)
                                    && o.response["kind"] == "entity"
                            });
                        if let Some(outcome) = success {
                            intent.status = IntentStatus::Done;
                            intent.result = Some(field(&outcome.response, "data")?);
                            intent.last_error = None;
                        } else if fs
                            .hsm_record(entity_id(&intent.expected.data.entity, ns)?)
                            .map_err(io)?
                            .is_none_or(|r| r.state != intent.expected)
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
                                let record: Record = required(
                                    store
                                        .hsm_get(&record_key(entity_id(
                                            &intent.expected.data.entity,
                                            ns,
                                        )?))
                                        .map_err(io)?,
                                )?;
                                if !selected(policy, &record, now) {
                                    return Err(HsmError::Busy(
                                        "policy eligibility changed; defer candidate".into(),
                                    ));
                                }
                                if let Some(w) = &policy.selection.capacity {
                                    let info = self.filesystem_info().map_err(io)?;
                                    let capacity = info.allocatable_capacity.max(0) as u64;
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
                        let outcome = fs
                            .hsm_outcome(&operation_key(
                                &intent.context.operation_id,
                                effect_step(&intent.action),
                            ))
                            .map_err(io)?;
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
            tx
        };
        self.hsm_submit(&tx).await
    }
}

#[derive(Default, Serialize, serde::Deserialize)]
struct PolicyRun {
    cursor: Option<String>,
    next_due_ms: i64,
    admitted: usize,
    pressure: bool,
}
fn outstanding_bytes(fs: &FsDir, _policy: &LifecyclePolicy) -> HsmResult<u64> {
    // All rules target the same online worker pool; reserve against a shared budget.
    let prefix = b"\x03hsm:reserved:online:".to_vec();
    let mut bytes = 0u64;
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = fs
            .get_rocks_store()
            .hsm_scan(&prefix, cursor.as_deref(), 128)
            .map_err(io)?;
        if page.is_empty() {
            break;
        }
        for (_, value) in &page {
            bytes = bytes.saturating_add(decode::<u64>(value).map_err(io)?);
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
