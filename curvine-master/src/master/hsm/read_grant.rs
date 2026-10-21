// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Large read results retain the original grant and logical layout identity,
//! not a second durable copy of every replica address. Active protection pins
//! the data; retries may refresh placement without extending the grant.
use super::*;
use curvine_model::{FileBlocks, LocatedBlock};
use serde::Deserialize;

const INLINE_BLOCKS: usize = 32;
const KIND: &str = "read_grant_v1";

#[derive(Serialize, Deserialize)]
pub(super) struct CompactGrant {
    handle: ReadHandle,
    blocks: usize,
    layout: [u8; 32],
}

fn layout_hash(blocks: &[LocatedBlock]) -> HsmResult<[u8; 32]> {
    let logical: Vec<_> = blocks.iter().map(|block| &block.block).collect();
    let mut value = serde_json::to_value(logical).map_err(io)?;
    value.sort_all_objects();
    Ok(*blake3::hash(&pack(value).map_err(io)?).as_bytes())
}

pub(super) fn prepare(mut handle: ReadHandle) -> HsmResult<(Value, Option<Value>)> {
    let public = reply("read", &handle);
    if handle.blocks.block_locs.len() <= INLINE_BLOCKS {
        return Ok((public, None));
    }
    let blocks = handle.blocks.block_locs.len();
    let layout = layout_hash(&handle.blocks.block_locs)?;
    handle.blocks.block_locs.clear();
    Ok((
        reply(
            KIND,
            CompactGrant {
                handle,
                blocks,
                layout,
            },
        ),
        Some(public),
    ))
}

pub(super) fn is_grant(response: &Value) -> bool {
    response["kind"] == "read" || response["kind"] == KIND
}

pub(super) fn handle(response: &Value) -> HsmResult<ReadHandle> {
    if response["kind"] == KIND {
        let grant: CompactGrant = field(response, "data")?;
        if !grant.handle.blocks.block_locs.is_empty() || grant.blocks <= INLINE_BLOCKS {
            return Err(HsmError::Integrity("invalid compact read grant".into()));
        }
        Ok(grant.handle)
    } else {
        field(response, "data")
    }
}

fn expand(response: Value, blocks: FileBlocks) -> HsmResult<Value> {
    let mut grant: CompactGrant = field(&response, "data")?;
    if grant.blocks != blocks.block_locs.len()
        || grant.layout != layout_hash(&blocks.block_locs)?
        || grant.handle.blocks.status.id != blocks.status.id
    {
        return Err(HsmError::Fenced);
    }
    // Keep the original status, data, credentials and grant deadline. Only
    // replica placement comes from the current committed native namespace.
    grant.handle.blocks.block_locs = blocks.block_locs;
    Ok(reply("read", grant.handle))
}

impl MasterFilesystem {
    pub(super) fn hsm_expand_read_grant(&self, ns: &Namespace, value: Value) -> HsmResult<Value> {
        if value["kind"] != KIND {
            return Ok(value);
        }
        let handle = handle(&value)?;
        let id = entity_id(&handle.data.entity, ns)?;
        let fs = self.fs_dir.read();
        let path = fs.get_inode_path(id).map_err(io)?;
        let input = InodePath::resolve(fs.root_ptr(), &path, &fs.store).map_err(io)?;
        let blocks = self.get_file_blocks(&path, &fs, &input).map_err(io)?;
        expand(value, blocks)
    }
}

/// Private prepared bytes are exposed only if this exact grant applied. A CAS
/// failure or a cached different result can never publish the prepared layout.
pub(super) fn publish(expected: &Value, applied: Value, public: Option<&Value>) -> Value {
    if &applied == expected {
        public.cloned().unwrap_or(applied)
    } else {
        applied
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{ExtendedBlock, FileStatus, WorkerAddress};

    fn grant() -> ReadHandle {
        ReadHandle {
            blocks: FileBlocks::new(
                FileStatus {
                    id: 42,
                    ..Default::default()
                },
                (0..1024)
                    .map(|id| {
                        LocatedBlock::new(
                            ExtendedBlock {
                                id,
                                len: 4096,
                                ..Default::default()
                            },
                            vec![WorkerAddress::default(); 3],
                        )
                    })
                    .collect(),
            ),
            data: DataKey {
                entity: EntityKey {
                    storage: "s".into(),
                    namespace: "n".into(),
                    entity: "42".into(),
                },
                generation: 1,
            },
            reader: "a".repeat(32),
            endpoint: None,
            granted_ms: 100,
            expires_ms: 200,
        }
    }

    #[test]
    fn compact_result_preserves_original_grant_and_rejects_changed_layout() -> HsmResult<()> {
        let full = grant();
        let (durable, public) = prepare(full.clone())?;
        assert!(pack(&durable).map_err(io)?.len() * 10 < pack(&public).map_err(io)?.len());
        let mut placements = full.blocks.clone();
        placements.block_locs[0].locs.clear();
        let refreshed = expand(durable.clone(), placements.clone())?;
        let refreshed: ReadHandle = field(&refreshed, "data")?;
        assert_eq!(refreshed.granted_ms, full.granted_ms);
        assert_eq!(refreshed.expires_ms, full.expires_ms);
        assert_eq!(refreshed.data, full.data);
        assert!(refreshed.blocks.block_locs[0].locs.is_empty());
        assert_eq!(
            publish(&durable, durable.clone(), public.as_ref()),
            public.unwrap()
        );
        let conflict = failure("conflict", "changed");
        assert_eq!(
            publish(&durable, conflict.clone(), Some(&Value::Null)),
            conflict
        );
        placements.block_locs[0].block.len += 1;
        assert!(matches!(
            expand(durable.clone(), placements.clone()),
            Err(HsmError::Fenced)
        ));
        placements.block_locs.pop();
        assert!(matches!(expand(durable, placements), Err(HsmError::Fenced)));
        Ok(())
    }
}
