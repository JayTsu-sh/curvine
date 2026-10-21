// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Frozen v1 journal DTOs. Bincode field order is part of the replay contract.
use super::{
    authority::{NativeCommand, NativeReply, RequestIdentity},
    transport::NativeBatch,
    view::PreparedEdits,
};
use crate::master::meta::inode::InodeView;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
pub(super) struct NativeBatchV1 {
    pub(super) version: u32,
    pub(super) commands: Vec<NativeCommandV1>,
}
#[derive(Serialize, Deserialize)]
pub(super) struct NativeCommandV1 {
    pub(super) identity: RequestIdentity,
    pub(super) fingerprint: [u8; 32],
    pub(super) time: i64,
    pub(super) edits: PreparedEditsV1,
    pub(super) reply: NativeReply,
}
#[derive(Default, Serialize, Deserialize)]
pub(super) struct PreparedEditsV1 {
    pub(super) inodes: BTreeMap<i64, Option<InodeView>>,
    pub(super) edges: BTreeMap<(i64, String), Option<i64>>,
    pub(super) inode_checks: BTreeMap<i64, Option<[u8; 32]>>,
    pub(super) edge_checks: BTreeMap<(i64, String), Option<i64>>,
    pub(super) child_deltas: BTreeMap<i64, i64>,
    pub(super) last_inode: i64,
}
impl From<NativeBatchV1> for NativeBatch {
    fn from(old: NativeBatchV1) -> Self {
        Self {
            version: old.version,
            commands: old
                .commands
                .into_iter()
                .map(|c| NativeCommand {
                    identity: c.identity,
                    fingerprint: c.fingerprint,
                    time: c.time,
                    reply: c.reply,
                    edits: PreparedEdits {
                        inodes: c.edits.inodes,
                        edges: c.edits.edges,
                        inode_checks: c.edits.inode_checks,
                        edge_checks: c.edits.edge_checks,
                        child_deltas: c.edits.child_deltas,
                        last_inode: c.edits.last_inode,
                        ..Default::default()
                    },
                })
                .collect(),
        }
    }
}
