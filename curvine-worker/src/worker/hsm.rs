// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Persistent, file-backed HSM allocations. Never exposed through ordinary
//! block IDs, short-circuit paths, replication, batch-write or delete commands.
use curvine_model::hsm::native::*;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub struct HsmStore {
    root: PathBuf,
    _owner: File,
    gates: Vec<parking_lot::Mutex<()>>,
}
#[derive(Serialize, Deserialize)]
struct Allocation {
    // Existing JSON layouts deserialize as Some. A deleted-only reservation
    // has null here: old readers fail closed rather than recreate it.
    layout: Option<Layout>,
    sealed: bool,
    deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deleted_admin: Option<String>,
}
impl Allocation {
    fn live_layout(&self) -> HsmResult<&Layout> {
        if self.deleted {
            return Err(HsmError::Fenced);
        }
        self.layout
            .as_ref()
            .ok_or_else(|| HsmError::Integrity("live allocation has no layout".into()))
    }
    fn admin(&self) -> HsmResult<&str> {
        self.layout
            .as_ref()
            .map(|layout| layout.admin.as_str())
            .or(self.deleted_admin.as_deref())
            .ok_or_else(|| HsmError::Integrity("allocation has no owner".into()))
    }
}
impl HsmStore {
    pub fn open(root: PathBuf) -> HsmResult<Self> {
        fs::create_dir_all(&root)?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        owner
            .try_lock_exclusive()
            .map_err(|_| HsmError::Busy("HSM allocation root already owned".into()))?;
        Ok(Self {
            root,
            _owner: owner,
            gates: (0..256).map(|_| parking_lot::Mutex::new(())).collect(),
        })
    }
    fn dir(&self, id: &str) -> HsmResult<PathBuf> {
        if id.len() != 32 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(HsmError::Invalid("invalid allocation ID".into()));
        }
        Ok(self.root.join(id))
    }
    fn load(dir: &Path) -> HsmResult<Allocation> {
        let bytes = fs::read(dir.join("state.json")).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                HsmError::NotFound("HSM allocation".into())
            } else {
                e.into()
            }
        })?;
        serde_json::from_slice(&bytes).map_err(|e| HsmError::Integrity(e.to_string()))
    }
    fn save(dir: &Path, state: &Allocation) -> HsmResult<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dir.join("state.next"))?;
        f.write_all(&serde_json::to_vec(state).map_err(|e| HsmError::Invalid(e.to_string()))?)?;
        f.sync_all()?;
        fs::rename(dir.join("state.next"), dir.join("state.json"))?;
        File::open(dir)?.sync_all()?;
        Ok(())
    }
    pub fn execute(&self, request: WorkerRequest) -> WorkerReply {
        let result = self.execute0(request);
        match result {
            Ok(value) => value,
            Err(e) => {
                let v = error_value(e);
                WorkerReply::Error {
                    code: v["data"]["code"].as_str().unwrap().into(),
                    message: v["data"]["message"].as_str().unwrap().into(),
                }
            }
        }
    }
    fn execute0(&self, request: WorkerRequest) -> HsmResult<WorkerReply> {
        let id = match &request {
            WorkerRequest::Allocate { layout } => &layout.id,
            WorkerRequest::Read { endpoint, .. }
            | WorkerRequest::Write { endpoint, .. }
            | WorkerRequest::Verify { endpoint } => &endpoint.allocation,
            WorkerRequest::Seal { allocation, .. } | WorkerRequest::Delete { allocation, .. } => {
                allocation
            }
        };
        let _guard = self.gates[blake3::hash(id.as_bytes()).as_bytes()[0] as usize].lock();
        match request {
            WorkerRequest::Allocate { layout } => {
                let dir = self.dir(&layout.id)?;
                if layout.length > i64::MAX as u64
                    || layout.digest.algorithm != "blake3"
                    || layout.digest.value.len() != 32
                    || [&layout.admin, &layout.write, &layout.read]
                        .iter()
                        .any(|t| t.len() != 32)
                    || layout.admin == layout.write
                    || layout.admin == layout.read
                    || layout.read == layout.write
                {
                    return Err(HsmError::Invalid("invalid HSM layout".into()));
                }
                if dir.join("state.json").exists() {
                    let state = Self::load(&dir)?;
                    if serde_json::to_vec(state.live_layout()?).unwrap()
                        != serde_json::to_vec(&layout).unwrap()
                    {
                        return Err(HsmError::Conflict(
                            "allocation identity already bound".into(),
                        ));
                    }
                } else {
                    fs::create_dir_all(&dir)?;
                    File::open(&self.root)?.sync_all()?;
                    // No endpoint is returned before this durable owner record.
                    Self::save(
                        &dir,
                        &Allocation {
                            layout: Some(layout),
                            sealed: false,
                            deleted: false,
                            deleted_admin: None,
                        },
                    )?;
                }
                Ok(WorkerReply::Done)
            }
            WorkerRequest::Write {
                endpoint,
                offset,
                bytes,
            } => {
                let dir = self.dir(&endpoint.allocation)?;
                let state = Self::load(&dir)?;
                let layout = state.live_layout()?;
                if state.sealed || endpoint.token != layout.write {
                    return Err(HsmError::Fenced);
                }
                if bytes.len() > CHUNK_BYTES
                    || offset
                        .checked_add(bytes.len() as u64)
                        .is_none_or(|end| end > layout.length)
                {
                    return Err(HsmError::Invalid("HSM write range".into()));
                }
                let mut f = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(dir.join("data"))?;
                let len = f.metadata()?.len();
                if offset > len {
                    return Err(HsmError::Conflict("nonsequential HSM write".into()));
                }
                let overlap = (len - offset).min(bytes.len() as u64) as usize;
                f.seek(SeekFrom::Start(offset))?;
                let mut prior = vec![0; overlap];
                f.read_exact(&mut prior)?;
                if prior != bytes[..overlap] {
                    return Err(HsmError::Conflict("retry changed bytes".into()));
                }
                f.write_all(&bytes[overlap..])?;
                Ok(WorkerReply::Done)
            }
            WorkerRequest::Read {
                endpoint,
                offset,
                length,
            } => {
                let dir = self.dir(&endpoint.allocation)?;
                let state = Self::load(&dir)?;
                let layout = state.live_layout()?;
                if !state.sealed || endpoint.token != layout.read {
                    return Err(HsmError::Fenced);
                }
                if length > CHUNK_BYTES || offset > layout.length {
                    return Err(HsmError::Invalid("HSM read range".into()));
                }
                let mut f = File::open(dir.join("data"))?;
                f.seek(SeekFrom::Start(offset))?;
                let mut bytes = vec![0; length.min((layout.length - offset) as usize)];
                f.read_exact(&mut bytes)?;
                Ok(WorkerReply::Bytes(bytes))
            }
            WorkerRequest::Verify { endpoint } => {
                let dir = self.dir(&endpoint.allocation)?;
                let state = Self::load(&dir)?;
                let layout = state.live_layout()?;
                if endpoint.token != layout.write {
                    return Err(HsmError::Fenced);
                }
                let mut f = File::open(dir.join("data"))?;
                let mut hash = blake3::Hasher::new();
                let mut buffer = vec![0; CHUNK_BYTES];
                let mut length = 0;
                loop {
                    let n = f.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    hash.update(&buffer[..n]);
                    length += n as u64;
                }
                let digest = ContentDigest {
                    algorithm: "blake3".into(),
                    value: hash.finalize().as_bytes().to_vec(),
                };
                if length != layout.length || digest != layout.digest {
                    return Err(HsmError::Integrity(
                        "private layout verification failed".into(),
                    ));
                }
                Ok(WorkerReply::Verified(TransferEvidence { length, digest }))
            }
            WorkerRequest::Seal { allocation, admin } => {
                let dir = self.dir(&allocation)?;
                let mut state = Self::load(&dir)?;
                let layout = state.live_layout()?;
                if admin != layout.admin {
                    return Err(HsmError::Fenced);
                }
                let mut f = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(dir.join("data"))?;
                f.sync_all()?;
                let mut hash = blake3::Hasher::new();
                let mut buf = vec![0; CHUNK_BYTES];
                let mut length = 0;
                loop {
                    let n = f.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hash.update(&buf[..n]);
                    length += n as u64;
                }
                let digest = ContentDigest {
                    algorithm: "blake3".into(),
                    value: hash.finalize().as_bytes().to_vec(),
                };
                if length != layout.length || digest != layout.digest {
                    return Err(HsmError::Integrity(
                        "unsealed HSM content differs from expected generation".into(),
                    ));
                }
                state.sealed = true;
                Self::save(&dir, &state)?;
                Ok(WorkerReply::Sealed(TransferEvidence { length, digest }))
            }
            WorkerRequest::Delete { allocation, admin } => {
                let dir = self.dir(&allocation)?;
                let mut state = match Self::load(&dir) {
                    Ok(state) => state,
                    Err(HsmError::NotFound(_)) => {
                        if admin.len() != 32 {
                            return Err(HsmError::Invalid("invalid allocation owner".into()));
                        }
                        fs::create_dir_all(&dir)?;
                        File::open(&self.root)?.sync_all()?;
                        Allocation {
                            layout: None,
                            sealed: false,
                            deleted: true,
                            deleted_admin: Some(admin.clone()),
                        }
                    }
                    Err(error) => return Err(error),
                };
                if admin != state.admin()? {
                    return Err(HsmError::Fenced);
                }
                // Durable tombstone precedes unlink or acknowledgement, even
                // if the matching Allocate has not reached this Worker yet.
                state.deleted = true;
                Self::save(&dir, &state)?;
                match fs::remove_file(dir.join("data")) {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e.into()),
                }
                File::open(&dir)?.sync_all()?;
                Ok(WorkerReply::Done)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(root: &Path, bytes: &[u8]) -> (HsmStore, Layout) {
        let store = HsmStore::open(root.to_path_buf()).unwrap();
        let layout = Layout::new(
            Default::default(),
            bytes.len() as u64,
            ContentDigest {
                algorithm: "blake3".into(),
                value: blake3::hash(bytes).as_bytes().to_vec(),
            },
        );
        store
            .execute(WorkerRequest::Allocate {
                layout: layout.clone(),
            })
            .into_result()
            .unwrap();
        (store, layout)
    }
    #[test]
    fn deletion_before_allocation_is_durable_and_fences_a_late_allocator() {
        let root = tempfile::tempdir().unwrap();
        let store = HsmStore::open(root.path().to_path_buf()).unwrap();
        let layout = Layout::new(
            Default::default(),
            0,
            ContentDigest {
                algorithm: "blake3".into(),
                value: blake3::hash(b"").as_bytes().to_vec(),
            },
        );
        let delete = WorkerRequest::Delete {
            allocation: layout.id.clone(),
            admin: layout.admin.clone(),
        };
        store
            .execute(delete.clone())
            .into_result()
            .expect("an abandoned reservation must be retireable before Allocate arrives");
        drop(store);
        let store = HsmStore::open(root.path().to_path_buf()).unwrap();
        store.execute(delete).into_result().unwrap();
        assert!(matches!(
            store
                .execute(WorkerRequest::Delete {
                    allocation: layout.id.clone(),
                    admin: layout.write.clone()
                })
                .into_result(),
            Err(HsmError::Fenced)
        ));
        assert!(
            matches!(
                store
                    .execute(WorkerRequest::Allocate {
                        layout: layout.clone()
                    })
                    .into_result(),
                Err(HsmError::Fenced)
            ),
            "late Allocate resurrected a retired reservation"
        );
        let endpoint = serde_json::from_slice(&layout.endpoint(true).capability).unwrap();
        assert!(matches!(
            store
                .execute(WorkerRequest::Verify { endpoint })
                .into_result(),
            Err(HsmError::Fenced)
        ));
        assert!(!root.path().join(&layout.id).join("data").exists());
    }

    #[test]
    fn sealed_and_deleted_capabilities_stay_fenced_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let (store, layout) = fixture(root.path(), b"content");
        let write: Endpoint = serde_json::from_slice(&layout.endpoint(true).capability).unwrap();
        let req = WorkerRequest::Write {
            endpoint: write.clone(),
            offset: 0,
            bytes: b"content".to_vec(),
        };
        store.execute(req.clone()).into_result().unwrap();
        store.execute(req.clone()).into_result().unwrap();
        store
            .execute(WorkerRequest::Seal {
                allocation: layout.id.clone(),
                admin: layout.admin.clone(),
            })
            .into_result()
            .unwrap();
        drop(store);
        let store = HsmStore::open(root.path().to_path_buf()).unwrap();
        assert!(matches!(
            store.execute(req.clone()).into_result(),
            Err(HsmError::Fenced)
        ));
        assert!(matches!(
            store
                .execute(WorkerRequest::Delete {
                    allocation: layout.id.clone(),
                    admin: write.token
                })
                .into_result(),
            Err(HsmError::Fenced)
        ));
        store
            .execute(WorkerRequest::Delete {
                allocation: layout.id.clone(),
                admin: layout.admin.clone(),
            })
            .into_result()
            .unwrap();
        drop(store);
        let store = HsmStore::open(root.path().to_path_buf()).unwrap();
        assert!(matches!(
            store
                .execute(WorkerRequest::Allocate { layout })
                .into_result(),
            Err(HsmError::Fenced)
        ));
        assert!(matches!(
            store.execute(req).into_result(),
            Err(HsmError::Fenced)
        ));
    }
    #[test]
    fn incomplete_or_corrupted_data_never_seals() {
        let root = tempfile::tempdir().unwrap();
        let (store, layout) = fixture(root.path(), b"content");
        let ep = serde_json::from_slice(&layout.endpoint(true).capability).unwrap();
        store
            .execute(WorkerRequest::Write {
                endpoint: ep,
                offset: 0,
                bytes: b"garbage".to_vec(),
            })
            .into_result()
            .unwrap();
        assert!(matches!(
            store
                .execute(WorkerRequest::Seal {
                    allocation: layout.id,
                    admin: layout.admin
                })
                .into_result(),
            Err(HsmError::Integrity(_))
        ));
    }
}
