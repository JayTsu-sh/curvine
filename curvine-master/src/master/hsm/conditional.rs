// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Optimistic file-control publication. A stale preparation is not a logical
//! workflow result. The explicit envelope preserves legacy replay semantics.
use super::*;

pub(super) const MAGIC: &[u8] = b"hsm-conditional-v1\n";

pub(super) fn shared_request(request: &Value) -> bool {
    matches!(
        request["op"].as_str(),
        Some(
            "capabilities"
                | "namespace"
                | "inspect"
                | "resolve"
                | "view_blocks"
                | "pending_seals"
                | "pending_recall"
                | "pending_view"
                | "completed_import"
                | "completed"
                | "policy_list"
                | "intent_list"
                | "intent_get"
        )
    )
}

pub(super) fn validate(tx: &Transaction) -> CommonResult<()> {
    let record = tx
        .record
        .as_ref()
        .ok_or("conditional control requires a record")?;
    if tx.inode != Some(record.state.data.entity.entity.parse()?)
        || tx.expected_epoch.is_none()
        || tx.create != tx.expected_version.is_none()
        || tx.namespace.is_some()
        || tx.read_state.is_some()
        || tx.scan_progress_only
        || !tx.controls.is_empty()
        || !tx.record_checks.is_empty()
        || tx.response["kind"]
            .as_str()
            .is_none_or(|kind| kind == "error")
    {
        return err_box!("invalid conditional file-control transaction");
    }
    Ok(())
}

/// Called under the committed apply write lock, before ANY writes or namespace
/// projection. On retry, the caller re-plans with the SAME logical identity.
/// A successful prior result is checked before this function by the caller.
pub(super) fn stale(fs: &FsDir, tx: &Transaction) -> CommonResult<Option<Value>> {
    let id = tx.inode.ok_or("conditional control without inode")?;
    let record = tx
        .record
        .as_ref()
        .ok_or("conditional control without record")?;
    let changed = fs.hsm_record(id)?.as_ref().map(|r| r.version) != tx.expected_version;
    let path_changed = if tx.create {
        let target = InodePath::resolve(fs.root_ptr(), &record.path, &fs.store)?;
        fs.store.get_inode(id, None)?.is_some()
            || target.is_full()
            || target.existing_len() + 1 != target.len()
    } else if !changed {
        fs.get_inode_path(id)? != record.path
    } else {
        false
    };
    Ok((changed || path_changed).then(|| {
        failure(
            "busy",
            "HSM preparation changed; retry the same operation identity",
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::{authority_gate::Class, journal::JournalSystem, Master};
    use serde_json::json;

    fn fixture() -> CommonResult<(tempfile::TempDir, MasterFilesystem, i64)> {
        Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let status = fs.create("/file", false)?;
        fs.complete_file("/file", None, 0, vec![], "", false, None)?;
        {
            let mut dir = fs.fs_dir.write();
            let ns = Namespace {
                storage: "source".into(),
                incarnation: "ns".into(),
                epoch: 1,
                root_inode: 9999,
                ordinary_format: 2,
            };
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put(NS, &pack(&ns)?)?;
            batch.commit()?;
            dir.hsm_enabled = true;
            let input = InodePath::resolve(dir.root_ptr(), "/file", &dir.store)?;
            dir.hsm_track_file("/file", input.get_last_inode().unwrap().as_file_ref()?)?;
        }
        Ok((root, fs, status.id))
    }
    fn transaction(dir: &FsDir, id: i64, operation: &str) -> CommonResult<Transaction> {
        let mut record = dir.hsm_record(id)?.unwrap();
        let previous = record.version;
        record.version += 1;
        record.state.revision += 1;
        Ok(Transaction {
            scan_progress_only: false,
            read_state: None,
            controls: vec![],
            record_checks: vec![],
            workflow: Some(WorkflowBinding {
                operation_id: operation.into(),
                kind: "test".into(),
                data: Some(record.state.data.clone()),
            }),
            outcome: operation.into(),
            fingerprint: "same-input".into(),
            response: reply("entity", &record.state),
            inode: Some(id),
            expected_version: Some(previous),
            record: Some(record),
            namespace: None,
            expected_epoch: Some(1),
            create: false,
            time: 1000,
            data: None,
            action: None,
        })
    }
    fn apply(dir: &mut FsDir, tx: &Transaction) -> CommonResult<Value> {
        Ok(dir
            .apply_hsm_results(&[MAGIC, pack(tx)?.as_slice()].concat())?
            .remove(0))
    }

    #[test]
    fn stale_control_does_not_write_or_bind_and_same_identity_can_succeed() -> CommonResult<()> {
        let (_root, fs, id) = fixture()?;
        let mut dir = fs.fs_dir.write();
        let first = transaction(&dir, id, "first")?;
        let stale = transaction(&dir, id, "second")?;
        let first_result = apply(&mut dir, &first)?;
        assert_eq!(first_result["kind"], "entity");
        let sequence = dir.get_rocks_store().db.get_db().latest_sequence_number();
        let conflict = apply(&mut dir, &stale)?;
        assert_eq!(conflict["data"]["code"], "busy", "{conflict}");
        assert_eq!(
            dir.get_rocks_store().db.get_db().latest_sequence_number(),
            sequence
        );
        assert!(dir.hsm_outcome("second")?.is_none());
        assert!(dir.hsm_binding("second")?.is_none());
        let retry = transaction(&dir, id, "second")?;
        let committed = apply(&mut dir, &retry)?;
        assert_eq!(committed["kind"], "entity");
        assert!(dir.hsm_binding("second")?.is_some());
        // Old successful plans return their original bytes despite a new record.
        assert_eq!(apply(&mut dir, &first)?, first_result);
        assert_eq!(apply(&mut dir, &stale)?, committed);
        let mut rebound = retry;
        rebound.fingerprint = "different-input".into();
        assert_eq!(apply(&mut dir, &rebound)?["data"]["code"], "conflict");
        // Unwrapped old journal entries preserve their durable CAS failures.
        let mut legacy = stale;
        legacy.outcome = "legacy".into();
        let result = dir.apply_hsm_results(&pack(&legacy)?)?.remove(0);
        assert_eq!(result["data"]["code"], "conflict");
        assert!(dir.hsm_outcome("legacy")?.is_some());
        Ok(())
    }

    #[test]
    fn moved_inode_cannot_be_published_through_an_old_path_and_rejection_is_retryable(
    ) -> CommonResult<()> {
        let (_root, fs, id) = fixture()?;
        fs.rename("/file", "/moved", curvine_model::RenameFlags::new(0))?;
        let mut dir = fs.fs_dir.write();
        let mut stale = transaction(&dir, id, "moved")?;
        // Isolate the path check from the version check: emulate a cached
        // descendant path after a move, with an otherwise current version.
        stale.record.as_mut().unwrap().path = "/file".into();
        assert_eq!(
            dir.hsm_record(id)?.unwrap().version,
            stale.expected_version.unwrap()
        );
        let sequence = dir.get_rocks_store().db.get_db().latest_sequence_number();
        assert_eq!(apply(&mut dir, &stale)?["data"]["code"], "busy");
        assert_eq!(
            dir.get_rocks_store().db.get_db().latest_sequence_number(),
            sequence
        );
        let mut retry = stale;
        retry.record.as_mut().unwrap().path = "/moved".into();
        assert_eq!(apply(&mut dir, &retry)?["kind"], "entity");
        assert!(dir.hsm_outcome("moved")?.is_some());
        Ok(())
    }

    #[test]
    fn new_reader_invalidates_a_prepared_release_without_losing_its_lease() -> CommonResult<()> {
        let (_root, fs, id) = fixture()?;
        let mut dir = fs.fs_dir.write();
        let mut release = transaction(&dir, id, "release-before-reader")?;
        release.record.as_mut().unwrap().state.residency = Residency::Offline;
        let base = dir.hsm_record(id)?.unwrap();
        let mut active = base.clone();
        active.version += 1;
        active.state.revision += 1;
        active.readers.insert("a".repeat(32), i64::MAX);
        let mut batch = dir.get_rocks_store().new_batch();
        ordinary::write_access(&mut batch, &base, &active, 1000)?;
        batch.commit()?;
        let before = dir.get_rocks_store().db.get_db().latest_sequence_number();
        assert_eq!(apply(&mut dir, &release)?["data"]["code"], "busy");
        assert_eq!(
            dir.get_rocks_store().db.get_db().latest_sequence_number(),
            before
        );
        let current = dir.hsm_record(id)?.unwrap();
        assert_eq!(current.state.residency, Residency::Online);
        assert_eq!(current.readers.get(&"a".repeat(32)), Some(&i64::MAX));
        assert!(current.garbage.is_empty() && current.native_garbage.is_empty());
        assert!(dir.hsm_binding("release-before-reader")?.is_none());
        assert!(dir.hsm_outcome("release-before-reader")?.is_none());
        Ok(())
    }

    #[test]
    fn structural_controls_wait_for_local_read_observations() -> CommonResult<()> {
        let (_root, fs, _id) = fixture()?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let query = json!({"op":"inspect"});
            let first =
                turn::AuthorityTurn::acquire(&fs, Class::request(&query), shared_request(&query))
                    .await;
            assert!(first.shared());
            for op in [
                "release",
                "prepare_recall",
                "claim_controller",
                "seal_begin",
                "intent_claim",
                "policy_put",
            ] {
                let mutation = json!({"op":op});
                assert!(!shared_request(&mutation));
                let mut waiting = Box::pin(turn::AuthorityTurn::acquire(
                    &fs,
                    Class::request(&mutation),
                    shared_request(&mutation),
                ));
                assert!(futures::poll!(&mut waiting).is_pending());
                drop(waiting);
            }
            drop(first);
            let release = json!({"op":"release"});
            let exclusive = turn::AuthorityTurn::acquire(
                &fs,
                Class::request(&release),
                shared_request(&release),
            )
            .await;
            assert!(!exclusive.shared());
            Ok(())
        })
    }
}
