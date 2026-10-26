// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! One public batch is one retry identity and one atomic committed prefix.
//! Preserve ordered stop-on-error for create/allocation and per-item close
//! results. Only successful items enter the private projection.
use super::{commands::PreparedCommand, view::PreparedEdits, NativeInput};
use crate::master::{fs::MasterFilesystem, MountManager};
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_proto::*;
use prost::Message;
use std::collections::BTreeMap;

impl PreparedEdits {
    pub(super) fn append(&mut self, next: Self) {
        // Later commands observe the private result of earlier commands. CAS
        // must retain each key's FIRST observation of the committed prefix.
        for (key, value) in next.inode_checks {
            self.inode_checks.entry(key).or_insert(value);
        }
        for (key, value) in next.edge_checks {
            self.edge_checks.entry(key).or_insert(value);
        }
        for (key, value) in next.hsm_checks {
            self.hsm_checks.entry(key).or_insert(value);
        }
        self.inodes.extend(next.inodes);
        self.edges.extend(next.edges);
        self.hsm_records.extend(next.hsm_records);
        self.locations.extend(next.locations);
        for (key, delta) in next.child_deltas {
            *self.child_deltas.entry(key).or_default() += delta;
        }
        self.last_inode = self.last_inode.max(next.last_inode);
        self.controls.extend(next.controls);
    }

    pub(super) fn coalesce_versions(&mut self) -> CommonResult<()> {
        // Build this map once after all items, rather than rebuilding every
        // successful prefix (quadratic for overwrites with many retirements).
        let mut controls = BTreeMap::new();
        for control in self.controls.drain(..) {
            controls
                .entry(control.key.clone())
                .and_modify(|old: &mut curvine_model::hsm::native::ControlChange| {
                    old.value = control.value.clone()
                })
                .or_insert(control);
        }
        self.controls = controls.into_values().collect();
        for (id, bytes) in &mut self.hsm_records {
            let mut record: curvine_model::hsm::native::Record = serde_json::from_slice(bytes)?;
            // All item effects publish together. The public batch advances
            // the authority record once; generation/state retain all item
            // transitions. No intermediate version is externally observable.
            record.version = self.hsm_checks[id]
                .unwrap_or(0)
                .checked_add(1)
                .ok_or("HSM version exhausted")?;
            *bytes = serde_json::to_vec(&record)?;
        }
        Ok(())
    }
}

pub(super) fn prepare(
    input: NativeInput,
    fs: &MasterFilesystem,
    mounts: &MountManager,
    view: &mut super::view::BatchView<'_>,
    time: i64,
) -> FsResult<PreparedCommand> {
    let (code, inputs) = match input {
        NativeInput::CreateBatch(request) => (
            curvine_fs_api::RpcCode::CreateFilesBatch,
            request
                .requests
                .into_iter()
                .map(NativeInput::Create)
                .collect::<Vec<_>>(),
        ),
        NativeInput::AddBatch(request) => (
            curvine_fs_api::RpcCode::AddBlocksBatch,
            request
                .requests
                .into_iter()
                .map(NativeInput::AddBlock)
                .collect(),
        ),
        NativeInput::CompleteBatch(request) => (
            curvine_fs_api::RpcCode::CompleteFilesBatch,
            request
                .requests
                .into_iter()
                .map(|mut item| {
                    item.return_file_blocks = Some(false);
                    NativeInput::Complete(item)
                })
                .collect(),
        ),
        _ => unreachable!("batch preparation requires a batch input"),
    };
    // Never leak a partial private projection into following coordinator
    // commands if aggregation itself fails. Its size shares the existing
    // bounded preparation budget; it contains no live directory ownership.
    let mut staged = view.clone();
    let mut edits = PreparedEdits::default();
    let mut allocations = vec![];
    let mut statuses = vec![];
    let mut blocks = vec![];
    let mut results = vec![];
    let mut failure = None;
    for input in inputs {
        match input.prepare(fs, mounts, &mut staged, time) {
            Ok(prepared) => {
                match code {
                    curvine_fs_api::RpcCode::CreateFilesBatch => statuses.push(
                        CreateFileResponse::decode(prepared.response.as_slice())
                            .map_err(|error| curvine_error::FsError::common(error.to_string()))?
                            .file_status,
                    ),
                    curvine_fs_api::RpcCode::AddBlocksBatch => blocks.push(
                        curvine_proto::LocatedBlockProto::decode(prepared.response.as_slice())
                            .map_err(|error| curvine_error::FsError::common(error.to_string()))?,
                    ),
                    _ => results.push(true),
                }
                edits.append(prepared.edits);
                if let Some(allocation) = prepared.allocation {
                    allocations.push(allocation);
                }
            }
            Err(error) => {
                if code == curvine_fs_api::RpcCode::CompleteFilesBatch {
                    results.push(false);
                } else {
                    failure = Some(error);
                    break;
                }
            }
        }
    }
    edits.coalesce_versions()?;
    staged.rebase_records(&edits)?;
    let response = match code {
        curvine_fs_api::RpcCode::CreateFilesBatch => CreateFilesBatchResponse {
            file_statuses: statuses,
        }
        .encode_to_vec(),
        curvine_fs_api::RpcCode::AddBlocksBatch => {
            AddBlocksBatchResponse { blocks }.encode_to_vec()
        }
        _ => CompleteFilesBatchResponse { results }.encode_to_vec(),
    };
    *view = staged;
    Ok(PreparedCommand {
        edits,
        response,
        allocation: super::commands::Allocation::combine(allocations),
        failure,
    })
}

#[cfg(test)]
#[path = "batch_tests.rs"]
mod tests;
