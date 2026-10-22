// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Bounded lifecycle read sets, checked before any committed publication.
//! Snapshot filtering is advisory; only the admission observations authorize work.
use super::*;
use serde::{Deserialize, Serialize};

pub(super) const MAGIC: &[u8] = b"hsm-lifecycle-conditional-v1\n";
const MAX_FILES: usize = 16;
const MAX_COMMON: usize = 3;
const MAX_CONTROLS: usize = 34;
const MAX_BYTES: usize = 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub(super) struct Command {
    pub(super) tx: Transaction,
    pub(super) checks: Checks,
}
#[derive(Default, Serialize, Deserialize)]
pub(super) struct Checks {
    common: Vec<CommonCheck>,
    files: Vec<FileCheck>,
    // Persist the decision's telemetry sample, never re-read a replica's local
    // WorkerManager during apply. This is NOT physical capacity reservation.
    capacity: Option<(u64, u64)>,
}
#[derive(Serialize, Deserialize)]
struct CommonCheck {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}
#[derive(Serialize, Deserialize)]
struct FileCheck {
    inode: i64,
    version: Option<u64>,
    path: Option<String>,
    copies: u64,
}

pub(super) fn copies(fs: &FsDir, inode: i64) -> CommonResult<u64> {
    Ok(fs
        .store
        .get_inode(inode, None)?
        .and_then(|inode| {
            inode
                .as_file_ref()
                .ok()
                .map(|file| file.replicas.max(1) as u64)
        })
        .unwrap_or(1))
}
impl Checks {
    pub(super) fn new(fs: &FsDir) -> HsmResult<Self> {
        let mut checks = Self::default();
        checks.common(fs, NS)?;
        Ok(checks)
    }
    pub(super) fn common(&mut self, fs: &FsDir, key: &[u8]) -> HsmResult<Option<Vec<u8>>> {
        if let Some(check) = self.common.iter().find(|check| check.key == key) {
            return Ok(check.value.clone());
        }
        if self.common.len() >= MAX_COMMON {
            return Err(HsmError::Invalid("lifecycle common read-set limit".into()));
        }
        let value = fs.get_rocks_store().hsm_get(key).map_err(io)?;
        self.common.push(CommonCheck {
            key: key.to_vec(),
            value: value.clone(),
        });
        Ok(value)
    }
    pub(super) fn file(
        &mut self,
        inode: i64,
        record: Option<&Record>,
        path: Option<String>,
        copies: u64,
    ) -> HsmResult<()> {
        if self.files.iter().any(|check| check.inode == inode) || self.files.len() >= MAX_FILES {
            return Err(HsmError::Invalid(
                "duplicate or excessive lifecycle file observation".into(),
            ));
        }
        self.files.push(FileCheck {
            inode,
            version: record.map(|record| record.version),
            path,
            copies,
        });
        Ok(())
    }
    /// Returns the current path as well as the merged base/access state. Claim
    /// eligibility must not use the base Record's potentially cached old path.
    pub(super) fn record(&mut self, fs: &FsDir, inode: i64) -> HsmResult<Option<Record>> {
        let mut record = fs.hsm_record(inode).map_err(io)?;
        let path = if let Some(record) = &mut record {
            record.path = fs.get_inode_path(inode).map_err(io)?;
            Some(record.path.clone())
        } else {
            None
        };
        self.file(inode, record.as_ref(), path, copies(fs, inode).map_err(io)?)?;
        Ok(record)
    }
    pub(super) fn capacity(&mut self, capacity: u64, available: u64) {
        self.capacity = Some((capacity, available));
    }
    pub(super) fn stale(&self, fs: &FsDir, tx: &Transaction) -> CommonResult<Option<Value>> {
        for check in &self.common {
            if fs.get_rocks_store().hsm_get(&check.key)? != check.value {
                return Ok(Some(if check.key == NS {
                    failure("fenced", "lifecycle namespace qualification changed")
                } else {
                    failure(
                        "busy",
                        "lifecycle result or capacity budget changed; retry the same identity",
                    )
                }));
            }
        }
        for check in &self.files {
            if fs
                .hsm_record(check.inode)?
                .as_ref()
                .map(|record| record.version)
                != check.version
                || copies(fs, check.inode)? != check.copies
            {
                return Ok(Some(failure(
                    "busy",
                    "lifecycle candidate changed; retry the same identity",
                )));
            }
            if let Some(path) = &check.path {
                if fs.get_inode_path(check.inode)? != *path {
                    return Ok(Some(failure(
                        "busy",
                        "lifecycle candidate moved; retry the same identity",
                    )));
                }
            }
        }
        for change in &tx.controls {
            if fs.get_rocks_store().hsm_get(&change.key)? != change.expected {
                return Ok(Some(failure(
                    "busy",
                    "lifecycle control changed; retry the same identity",
                )));
            }
        }
        Ok(None)
    }
}
pub(super) fn decode_command(bytes: &[u8]) -> CommonResult<Command> {
    if bytes.len() > MAX_BYTES {
        return err_box!("lifecycle command exceeds byte limit");
    }
    let command: Command = decode(bytes)?;
    command.validate()?;
    Ok(command)
}
impl Command {
    pub(super) fn validate(&self) -> CommonResult<()> {
        let tx = &self.tx;
        let checks = &self.checks;
        if tx.inode.is_some()
            || tx.record.is_some()
            || tx.read_state.is_some()
            || tx.namespace.is_some()
            || tx.expected_version.is_some()
            || tx.expected_epoch.is_none()
            || tx.create
            || tx.data.is_some()
            || tx.action.is_some()
            || tx.controls.is_empty()
            || tx.controls.len() > MAX_CONTROLS
            || checks.common.is_empty()
            || checks.common.len() > MAX_COMMON
            || checks.files.len() > MAX_FILES
            || tx.record_checks.len() > MAX_FILES
            || tx.response["kind"]
                .as_str()
                .is_none_or(|kind| !matches!(kind, "policy" | "intent" | "scan"))
        {
            return err_box!("invalid conditional lifecycle transaction");
        }
        let mut common = std::collections::HashSet::new();
        for check in &checks.common {
            if !common.insert(&check.key)
                || !(check.key == NS
                    || check.key == lifecycle::RESERVATION_TOTAL
                    || check.key.starts_with(b"\x03hsm:outcome:"))
            {
                return err_box!("invalid lifecycle common observation");
            }
        }
        if !checks
            .common
            .iter()
            .any(|check| check.key == NS && check.value.is_some())
        {
            return err_box!("lifecycle command lacks namespace observation");
        }
        let mut files = std::collections::HashSet::new();
        for check in &checks.files {
            if !files.insert(check.inode)
                || check.version.is_some() != check.path.is_some()
                || check.copies == 0
            {
                return err_box!("invalid lifecycle file observation");
            }
        }
        for (id, version) in &tx.record_checks {
            if !checks
                .files
                .iter()
                .any(|check| check.inode == *id && check.version == Some(*version))
            {
                return err_box!("lifecycle candidate lacks admission observation");
            }
        }
        let mut controls = std::collections::HashSet::new();
        for change in &tx.controls {
            if !controls.insert(&change.key)
                || ![
                    b"\x03hsm:policy:".as_slice(),
                    b"\x03hsm:scan:",
                    b"\x03hsm:intent:",
                    b"\x03hsm:active:",
                ]
                .iter()
                .any(|prefix| change.key.starts_with(prefix))
            {
                return err_box!("invalid lifecycle write set");
            }
        }
        lifecycle::validate_scan_progress(tx)?;
        Ok(())
    }
}
impl MasterFilesystem {
    pub(super) async fn hsm_submit_lifecycle(
        &self,
        command: Command,
        term: u64,
    ) -> HsmResult<Value> {
        command.validate().map_err(io)?;
        let bytes = pack(&command).map_err(io)?;
        if bytes.len() > MAX_BYTES {
            return Err(HsmError::Invalid(
                "lifecycle command exceeds byte limit".into(),
            ));
        }
        #[cfg(feature = "hsm-scale-test")]
        {
            let pause = {
                let dir = self.fs_dir.read();
                command.tx.workflow.as_ref().and_then(|workflow| {
                    dir.lifecycle_test_pauses.as_ref().and_then(|pauses| {
                        pauses
                            .remove(&workflow.operation_id)
                            .map(|(_, signals)| signals)
                    })
                })
            };
            if let Some((started, resume)) = pause {
                started.notify_one();
                resume.notified().await;
            }
        }
        Ok(self
            .hsm_submit_payload([MAGIC, bytes.as_slice()].concat(), 1, Some(term))
            .await?
            .remove(0))
    }
}

#[cfg(test)]
#[path = "lifecycle_commit_tests.rs"]
mod tests;
