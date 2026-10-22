use super::*;
use crate::master::{journal::JournalSystem, Master};

fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem, [i64; 2])> {
    Master::init_test_metrics();
    let root = tempfile::tempdir()?;
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
    conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
    let fs = JournalSystem::fs_only_for_test(&conf)?;
    let ids = [fs.create("/one", false)?.id, fs.create("/two", false)?.id];
    fs.complete_file("/one", None, 100, vec![], "", false, None)?;
    fs.complete_file("/two", None, 100, vec![], "", false, None)?;
    {
        let mut dir = fs.fs_dir.write();
        let ns = Namespace {
            storage: "source".into(),
            incarnation: "namespace".into(),
            root_inode: 9999,
            ordinary_format: 2,
            epoch: 1,
        };
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(NS, &pack(&ns)?)?;
        batch.hsm_put(lifecycle::RESERVATION_TOTAL, &pack(0u64)?)?;
        batch.commit()?;
        dir.hsm_enabled = true;
        for path in ["/one", "/two"] {
            let input = InodePath::resolve(dir.root_ptr(), path, &dir.store)?;
            dir.hsm_track_file(path, input.get_last_inode().unwrap().as_file_ref()?)?;
        }
    }
    Ok((root, fs, ids))
}
fn intent_key(intent: &LifecycleIntent) -> Vec<u8> {
    lifecycle::intent_key(&intent.id).unwrap()
}
fn command(dir: &FsDir, inode: i64, operation: &str, capacity: bool) -> HsmResult<Command> {
    let mut checks = Checks::new(dir)?;
    let record = checks.record(dir, inode)?.unwrap();
    if capacity {
        checks.capacity(1000, 800);
        checks.common(dir, lifecycle::RESERVATION_TOTAL)?;
    }
    let id = blake3::hash(operation.as_bytes()).to_hex().to_string();
    let intent = LifecycleIntent {
        id: id.clone(),
        policy: "cold".into(),
        policy_revision: 1,
        version: 1,
        expected: record.state.clone(),
        action: ManagedAction::Archive(ArchiveKey {
            backend: "local".into(),
            object: id.clone(),
        }),
        context: OperationContext {
            operation_id: format!("intent-{id}"),
            controller_epoch: 1,
            attempt_epoch: 1,
        },
        status: IntentStatus::Ready,
        claim: None,
        last_error: None,
        result: None,
    };
    let controls = vec![
        ControlChange {
            key: intent_key(&intent),
            expected: None,
            value: Some(pack(&intent).map_err(io)?),
        },
        ControlChange {
            key: lifecycle::active_key(&record.state.data),
            expected: None,
            value: Some(pack(&id).map_err(io)?),
        },
    ];
    Ok(Command {
        checks,
        tx: Transaction {
            scan_progress_only: false,
            read_state: None,
            controls,
            record_checks: vec![(inode, record.version)],
            workflow: Some(WorkflowBinding {
                operation_id: operation.into(),
                kind: "lifecycle".into(),
                data: None,
            }),
            outcome: operation.into(),
            fingerprint: format!("input-{operation}"),
            response: reply(
                "scan",
                ScanBatch {
                    examined: 1,
                    admitted: vec![id],
                    next: None,
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
        },
    })
}
fn apply(dir: &mut FsDir, command: &Command) -> CommonResult<Value> {
    Ok(dir
        .apply_hsm_results(&[MAGIC, pack(command)?.as_slice()].concat())?
        .remove(0))
}
fn sequence(dir: &FsDir) -> u64 {
    dir.get_rocks_store().db.get_db().latest_sequence_number()
}
fn queued(command: &Command) -> LifecycleIntent {
    decode(command.tx.controls[0].value.as_ref().unwrap()).unwrap()
}
fn total(dir: &FsDir) -> u64 {
    lifecycle::reservation_total(dir).unwrap()
}

#[test]
fn disjoint_admissions_overlap_and_competing_capacity_samples_revalidate_the_budget(
) -> CommonResult<()> {
    let (_root, fs, ids) = fixture()?;
    let mut dir = fs.fs_dir.write();
    let a = command(&dir, ids[0], "capacity-a", true)?;
    let b = command(&dir, ids[1], "capacity-b", true)?;
    let expected_a = apply(&mut dir, &a)?;
    assert_eq!(expected_a["kind"], "scan");
    assert!(total(&dir) > 0);
    let before = sequence(&dir);
    assert_eq!(apply(&mut dir, &b)?["data"]["code"], "busy");
    assert_eq!(sequence(&dir), before);
    assert!(dir.hsm_outcome("capacity-b")?.is_none());
    assert!(dir.hsm_binding("capacity-b")?.is_none());
    assert!(dir
        .get_rocks_store()
        .hsm_get(&intent_key(&queued(&b)))?
        .is_none());
    // A fresh preparation sees the new outstanding budget. The unwatermarked
    // policy here explicitly has no shared capacity constraint.
    let retry = command(&dir, ids[1], "capacity-b", false)?;
    assert_eq!(apply(&mut dir, &retry)?["kind"], "scan");
    assert_eq!(apply(&mut dir, &a)?, expected_a);

    let (_root2, fs2, ids) = fixture()?;
    let mut dir = fs2.fs_dir.write();
    let a = command(&dir, ids[0], "disjoint-a", false)?;
    let b = command(&dir, ids[1], "disjoint-b", false)?;
    assert_eq!(apply(&mut dir, &a)?["kind"], "scan");
    assert_eq!(apply(&mut dir, &b)?["kind"], "scan");
    let a_bytes = queued(&a).expected.length * a.checks.files[0].copies;
    let b_bytes = queued(&b).expected.length * b.checks.files[0].copies;
    assert_eq!(total(&dir), a_bytes + b_bytes);
    Ok(())
}

#[test]
fn a_new_reader_invalidates_admission_without_creating_task_or_result() -> CommonResult<()> {
    let (_root, fs, ids) = fixture()?;
    let mut dir = fs.fs_dir.write();
    let admission = command(&dir, ids[0], "read-race", false)?;
    let base = dir.hsm_record(ids[0])?.unwrap();
    let mut current = base.clone();
    current.version += 1;
    current.state.revision += 1;
    current.readers.insert("a".repeat(32), i64::MAX);
    let mut batch = dir.get_rocks_store().new_batch();
    ordinary::write_access(&mut batch, &base, &current, 1000)?;
    batch.commit()?;
    let before = sequence(&dir);
    assert_eq!(apply(&mut dir, &admission)?["data"]["code"], "busy");
    assert_eq!(sequence(&dir), before);
    assert_eq!(dir.hsm_record(ids[0])?.unwrap().readers, current.readers);
    assert!(dir.hsm_outcome("read-race")?.is_none());
    assert!(dir
        .get_rocks_store()
        .hsm_get(&intent_key(&queued(&admission)))?
        .is_none());
    assert_eq!(total(&dir), 0);
    Ok(())
}

#[test]
fn current_path_and_replica_count_are_guarded_even_without_a_record_version_change(
) -> CommonResult<()> {
    let (_root, fs, ids) = fixture()?;
    fs.rename("/one", "/moved", curvine_model::RenameFlags::new(0))?;
    let mut dir = fs.fs_dir.write();
    let mut admission = command(&dir, ids[0], "path-race", false)?;
    admission.checks.files[0].path = Some("/one".into());
    let before = sequence(&dir);
    assert_eq!(apply(&mut dir, &admission)?["data"]["code"], "busy");
    assert_eq!(sequence(&dir), before);
    let admission = command(&dir, ids[0], "path-race", false)?;
    let version = dir.hsm_record(ids[0])?.unwrap().version;
    let mut inode = dir.store.get_inode(ids[0], None)?.unwrap();
    inode.as_file_mut()?.replicas += 1;
    let mut batch = dir.get_rocks_store().new_batch();
    batch.write_inode(&inode)?;
    batch.commit()?;
    assert_eq!(dir.hsm_record(ids[0])?.unwrap().version, version);
    let before = sequence(&dir);
    assert_eq!(apply(&mut dir, &admission)?["data"]["code"], "busy");
    assert_eq!(sequence(&dir), before);
    let retry = command(&dir, ids[0], "path-race", false)?;
    assert_eq!(apply(&mut dir, &retry)?["kind"], "scan");
    Ok(())
}

#[test]
fn an_effect_committed_after_a_failure_report_was_prepared_wins_on_retry() -> CommonResult<()> {
    let (_root, fs, ids) = fixture()?;
    let mut dir = fs.fs_dir.write();
    let admission = command(&dir, ids[0], "admission", false)?;
    apply(&mut dir, &admission)?;
    let original = queued(&admission);
    let effect = outcome_key(&operation_key(
        &original.context.operation_id,
        "commit_archive",
    ));
    let mut terminal = original.clone();
    terminal.version += 1;
    terminal.status = IntentStatus::Failed;
    terminal.last_error = Some("executor lost reply".into());
    let mut finish = command(&dir, ids[1], "finish", false)?;
    finish.checks.files.clear();
    finish.checks.common(&dir, &effect)?;
    finish.tx.record_checks.clear();
    finish.tx.controls = vec![
        ControlChange {
            key: intent_key(&original),
            expected: Some(pack(&original)?),
            value: Some(pack(&terminal)?),
        },
        ControlChange {
            key: lifecycle::active_key(&original.expected.data),
            expected: Some(pack(&original.id)?),
            value: None,
        },
    ];
    finish.tx.response = reply("intent", &terminal);
    let outcome = Outcome {
        fingerprint: "effect-input".into(),
        response: reply("entity", &original.expected),
        data: Some(original.expected.data.clone()),
        action: Some(original.action.clone()),
    };
    let mut batch = dir.get_rocks_store().new_batch();
    batch.hsm_put(&effect, &pack(&outcome)?)?;
    batch.commit()?;
    let before = sequence(&dir);
    assert_eq!(apply(&mut dir, &finish)?["data"]["code"], "busy");
    assert_eq!(sequence(&dir), before);
    assert!(dir.hsm_outcome("finish")?.is_none());
    terminal.status = IntentStatus::Done;
    terminal.last_error = None;
    terminal.result = Some(original.expected);
    finish.checks = Checks::new(&dir)?;
    finish.checks.common(&dir, &effect)?;
    finish.tx.controls[0].value = Some(pack(&terminal)?);
    finish.tx.response = reply("intent", &terminal);
    assert_eq!(apply(&mut dir, &finish)?["data"]["status"], "done");
    assert_eq!(total(&dir), 0);
    let persisted: LifecycleIntent = decode(
        &dir.get_rocks_store()
            .hsm_get(&intent_key(&terminal))?
            .unwrap(),
    )?;
    assert_eq!(persisted.status, IntentStatus::Done);
    Ok(())
}

#[test]
fn namespace_changes_fence_new_commands_without_reinterpreting_legacy_conflicts() -> CommonResult<()>
{
    let (_root, fs, ids) = fixture()?;
    let mut dir = fs.fs_dir.write();
    let admission = command(&dir, ids[0], "epoch", false)?;
    let mut ns = dir.hsm_namespace()?.unwrap();
    ns.epoch += 1;
    let mut batch = dir.get_rocks_store().new_batch();
    batch.hsm_put(NS, &pack(&ns)?)?;
    batch.commit()?;
    let before = sequence(&dir);
    assert_eq!(apply(&mut dir, &admission)?["data"]["code"], "fenced");
    assert_eq!(sequence(&dir), before);
    assert!(dir.hsm_outcome("epoch")?.is_none());
    let legacy = dir
        .apply_hsm_results(&[LIFECYCLE_MAGIC, pack(&admission.tx)?.as_slice()].concat())?
        .remove(0);
    assert_eq!(legacy["data"]["code"], "fenced");
    assert!(dir.hsm_outcome("epoch")?.is_some());
    Ok(())
}

#[test]
fn malformed_or_unbounded_lifecycle_read_sets_are_rejected_before_publication() -> CommonResult<()>
{
    let (_root, fs, ids) = fixture()?;
    let dir = fs.fs_dir.read();
    let mut admission = command(&dir, ids[0], "invalid", false)?;
    admission.checks.files.clear();
    assert!(admission.validate().is_err());
    let mut admission = command(&dir, ids[0], "invalid", false)?;
    admission.tx.controls.push(ControlChange {
        key: NS.to_vec(),
        expected: None,
        value: None,
    });
    assert!(admission.validate().is_err());
    assert!(decode_command(&vec![b' '; MAX_BYTES + 1]).is_err());
    assert!(dir.hsm_outcome("invalid")?.is_none());
    Ok(())
}

#[test]
fn recall_claim_finish_and_detail_keep_recall_admission_when_background_is_full() -> CommonResult<()>
{
    use crate::master::authority_gate::Class;
    use serde_json::json;
    let (_root, fs, ids) = fixture()?;
    let mut intent = queued(&command(&fs.fs_dir.read(), ids[0], "priority", false)?);
    let persist = |intent: &LifecycleIntent| -> CommonResult<()> {
        let dir = fs.fs_dir.read();
        let mut batch = dir.get_rocks_store().new_batch();
        batch.hsm_put(&intent_key(intent), &pack(intent)?)?;
        batch.commit()
    };
    persist(&intent)?;
    assert_eq!(
        fs.hsm_control_class(&json!({"op":"intent_claim","id":intent.id,"work_class":"recall"})),
        Class::Background
    );
    intent.action = ManagedAction::Recall(ArchiveKey {
        backend: "local".into(),
        object: "archived".into(),
    });
    persist(&intent)?;
    for op in ["intent_claim", "intent_finish", "intent_get"] {
        assert_eq!(
            fs.hsm_control_class(&json!({"op":op,"id":intent.id})),
            Class::Recall
        );
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let background = fs.fs_dir.read().hsm_control_slots[Class::Background as usize].clone();
        let _full = background.acquire_many_owned(128).await?;
        let class = fs.hsm_control_class(&json!({"op":"intent_claim","id":intent.id}));
        let admitted = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            fs.hsm_run_owned(class, async { Ok(reply("admitted", true)) }),
        )
        .await?;
        assert_eq!(admitted["kind"], "admitted");
        Ok(())
    })
}
