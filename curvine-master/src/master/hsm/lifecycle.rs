// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Source-owned lifecycle: bounded native scans, atomic admission and durable claims.
use super::*;

const POLICIES: &[u8] = b"\x03hsm:policy:";
const INTENTS: &[u8] = b"\x03hsm:intent:";
const RECORDS: &[u8] = b"\x03hsm:record:";
const PAGE_MAX: usize = 16;

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
fn intent_key(id: &str) -> HsmResult<Vec<u8>> {
    if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(HsmError::Invalid("invalid intent ID".into()));
    }
    Ok([INTENTS, id.as_bytes()].concat())
}
fn active_key(data: &DataKey) -> Vec<u8> {
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
        || record.pending.is_some()
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
            record.state.residency == Residency::Online && record.published.is_some()
        }
        PolicyAction::Release => {
            record.state.residency == Residency::Dual
                && record.published.is_some()
                && record.views.is_empty()
                && record
                    .state
                    .anchors
                    .iter()
                    .any(|a| a.key.backend == policy.backend)
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
                let prefix = if op == "policy_list" {
                    POLICIES
                } else {
                    INTENTS
                };
                let after = cursor
                    .map(|id| {
                        if op == "policy_list" {
                            policy_key(id)
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
                    .map(|(_, v)| decode(v).map_err(io))
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
                    if !policy.prefix.starts_with("/.hsm/")
                        || policy.prefix.contains('\0')
                        || policy.prefix.len() > 264
                    {
                        return Err(HsmError::Invalid(
                            "policy prefix must select /.hsm direct children".into(),
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
                    if !(1..=PAGE_MAX).contains(&limit) {
                        return Err(HsmError::Invalid("scan limit must be 1..16".into()));
                    }
                    let after = request
                        .get("after")
                        .and_then(Value::as_str)
                        .map(|s| {
                            s.parse::<i64>()
                                .map(record_key)
                                .map_err(|_| HsmError::Invalid("invalid record cursor".into()))
                        })
                        .transpose()?;
                    let mut page = store
                        .hsm_scan(RECORDS, after.as_deref(), limit + 1)
                        .map_err(io)?;
                    let more = page.len() > limit;
                    page.truncate(limit);
                    let next = if more {
                        page.last()
                            .map(|(k, _)| String::from_utf8_lossy(&k[RECORDS.len()..]).into_owned())
                    } else {
                        None
                    };
                    let mut admitted = vec![];
                    // A concurrent edit invalidates the WHOLE bounded admission batch.
                    tx.controls.push(ControlChange {
                        key: pkey,
                        expected: pbytes.clone(),
                        value: pbytes,
                    });
                    for (_, bytes) in &page {
                        let record: Record = decode(bytes).map_err(io)?;
                        if !selected(&policy, &record, now) {
                            continue;
                        }
                        let data = &record.state.data;
                        let id = blake3::hash(&encoded(&(&policy.id, policy.revision, data))?)
                            .to_hex()
                            .to_string();
                        let ikey = intent_key(&id)?;
                        let active = active_key(data);
                        if store.hsm_get(&ikey).map_err(io)?.is_some()
                            || store.hsm_get(&active).map_err(io)?.is_some()
                        {
                            continue;
                        }
                        let archive = ArchiveKey {
                            backend: policy.backend.clone(),
                            object: format!("policy-{id}"),
                        };
                        let action = match policy.action {
                            PolicyAction::Archive => ManagedAction::Archive(archive),
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
                        };
                        let intent = LifecycleIntent {
                            id: id.clone(),
                            policy: policy.id.clone(),
                            policy_revision: policy.revision,
                            version: 1,
                            expected: record.state.clone(),
                            action,
                            context: OperationContext {
                                operation_id: format!("intent-{id}"),
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
                            value: Some(encoded(&id)?),
                        });
                        admitted.push(id);
                    }
                    tx.response = reply(
                        "scan",
                        ScanBatch {
                            examined: page.len(),
                            admitted,
                            next,
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
                        let pbytes = store.hsm_get(&policy_key(&intent.policy)?).map_err(io)?;
                        let policy: LifecyclePolicy = required(pbytes.clone())?;
                        tx.controls.push(ControlChange {
                            key: policy_key(&intent.policy)?,
                            expected: pbytes.clone(),
                            value: pbytes,
                        });
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
                        } else if !policy.enabled || policy.revision != intent.policy_revision {
                            intent.status = IntentStatus::Superseded;
                            intent.last_error = Some("admitting policy disabled or revised".into());
                        } else {
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
