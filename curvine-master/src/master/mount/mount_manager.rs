// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused)]
use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::{InodePath, InodePtr, InodeView};
use crate::master::mount::MountTable;
use crate::master::{self, SyncFsDir};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{self, CurvineURI, Path};
use curvine_model::{MkdirOpts, MountInfo, MountOptions};
use curvine_ufs_api::S3Conf;
use log::{info, warn};

pub struct MountManager {
    master_fs: MasterFilesystem,
    mount_table: MountTable,
}

impl MountManager {
    pub fn new(master_fs: MasterFilesystem) -> Self {
        let fs_dir = master_fs.fs_dir.clone();
        MountManager {
            master_fs,
            mount_table: MountTable::new(fs_dir),
        }
    }

    /// recovery mount points from store
    pub fn restore(&self) -> FsResult<()> {
        self.mount_table.restore()?;
        self.get_mount_table().map(|_| ())
    }

    pub fn restore_best_effort(&self) {
        if let Err(error) = self.restore() {
            warn!("failed to restore mount table: {}", error);
        }
    }

    // Unlike mutation guards, mounting must reject even an idle ordinary HSM
    // generation: mounted readers construct unprotected cache/UFS readers.
    // Keep this check on lookup too, so historical replay and best-effort
    // snapshot reload cannot publish an incompatible persisted mount.
    fn ensure_hsm_compatible(&self, mount_path: &str) -> FsResult<()> {
        let dir = self.master_fs.fs_dir.read();
        if !dir.hsm_enabled {
            return Ok(());
        }
        let path = Path::from_str(mount_path)?;
        if Path::has_prefix(curvine_model::hsm::native::HSM_ROOT, path.path())
            || Path::has_prefix(path.path(), curvine_model::hsm::native::HSM_ROOT)
        {
            return err_box!("UFS mounts cannot cover the reserved HSM namespace");
        }
        let namespace_root = dir.hsm_namespace()?.map(|namespace| namespace.root_inode);
        let input = InodePath::resolve(dir.root_ptr(), mount_path, &dir.store)?;
        let Some(inode) = input.task_last() else {
            return Ok(());
        };
        let mut pending = vec![inode];
        while let Some(inode) = pending.pop() {
            if namespace_root == Some(inode.id()) || dir.hsm_record(inode.id())?.is_some() {
                return err_box!(
                    "UFS mount {} overlaps managed HSM data; unmount it or choose an unmanaged subtree",
                    mount_path
                );
            }
            if let InodeView::Dir(directory) = inode.as_ref() {
                pending.extend(directory.children_iter().map(InodePtr::from_ref));
            }
        }
        Ok(())
    }

    fn create_mount_point(&self, mount_path: &str) -> FsResult<bool> {
        let exist = self.master_fs.exists(mount_path)?;
        if exist {
            return Ok(true);
        }

        let opts = MkdirOpts::with_create(true);
        self.master_fs.mkdir_with_opts(mount_path, opts)?;
        Ok(true)
    }

    fn normalize_mount_config(mount: &mut MountInfo) -> FsResult<()> {
        if mount.write_cache && !mount.write_cache_enabled() {
            return err_box!(
                "write_cache requires cache_mode with read_write access_mode for mount {}",
                mount.cv_path
            );
        }

        let path = Path::from_str(&mount.ufs_path)?;
        if !matches!(path.scheme(), Some("s3" | "s3a")) {
            return Ok(());
        }

        let properties = std::mem::take(&mut mount.properties);
        mount.properties = S3Conf::canonicalize_properties(properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })?;
        S3Conf::validate(&mount.properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    fn add_mount(
        &self,
        mnt_id: Option<u32>,
        mount_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        self.ensure_hsm_compatible(mount_path)?;
        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.mount_table.assign_mount_id()?,
        };
        let mut mount = mnt_opt.clone().to_info(assign_id, mount_path, ufs_path);
        Self::normalize_mount_config(&mut mount)?;
        let _ = self.create_mount_point(mount_path)?;

        let mut normalized_options = mnt_opt.clone();
        normalized_options.add_properties = mount.properties;
        self.mount_table
            .add_mount(assign_id, mount_path, ufs_path, &normalized_options)
    }

    fn update_mount(&self, cv_path: &str, mnt_opt: &MountOptions) -> FsResult<()> {
        self.ensure_hsm_compatible(cv_path)?;
        let path = Path::from_str(cv_path)?;
        let Some(existing) = self.get_mount_info(&path)? else {
            return err_box!("mount point {} not found for update", cv_path);
        };
        let mut merged = existing.merge_with(mnt_opt.clone());
        Self::normalize_mount_config(&mut merged)?;

        self.mount_table.update_mount(merged)
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    pub fn mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        if mnt_opt.update {
            return self.update_mount(cv_path, mnt_opt);
        }

        self.add_mount(mnt_id, cv_path, ufs_path, mnt_opt)
    }

    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        self.mount_table.unprotected_add_mount(info)
    }

    pub fn umount(&self, cv_path: &str) -> FsResult<()> {
        self.mount_table.umount(cv_path)
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        let info = self.mount_table.get_mount_info_by_id(id)?;
        self.umount(&info.cv_path)
    }

    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        self.mount_table.unprotected_umount_by_id(id)
    }

    pub fn has_mounted(&self, id: u32) -> FsResult<bool> {
        self.mount_table.has_mounted(id)
    }

    /**
     * use ufs_uri to find mount entry
     */
    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.mount_table.get_mount_info(path)
    }

    /// Validate before publishing mount routing to SDK/UFS readers. Internal
    /// native preparation uses get_mount_info while already holding fs_dir.
    pub(crate) fn get_read_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        let mount = self.mount_table.get_mount_info(path)?;
        if let Some(mount) = &mount {
            self.ensure_hsm_compatible(&mount.cv_path)?;
        }
        Ok(mount)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let table = self.mount_table.get_mount_table()?;

        let mut entries = Vec::new();
        for entry in table {
            self.ensure_hsm_compatible(&entry.cv_path)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::MountManager;
    use crate::master::fs::MasterFilesystem;
    use crate::master::journal::JournalSystem;
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_error::FsResult;
    use curvine_fs_api::Path;
    use curvine_model::hsm::native::Namespace;
    use curvine_model::{AccessMode, MountInfo, MountOptions, WriteType};

    fn managed_fixture() -> FsResult<(tempfile::TempDir, MasterFilesystem)> {
        Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let file = fs.create("/managed/deep/data", true)?;
        fs.mkdir("/plain", false)?;
        {
            // Seed one committed managed generation, then exercise the real
            // MountManager paths without requiring a Raft cluster for this test.
            let mut dir = fs.fs_dir.write();
            let inode = dir.store.get_inode(file.id, None)?.unwrap();
            let record = crate::master::hsm::native_record(
                &Namespace {
                    ordinary_format: 2,
                    root_inode: 9999,
                    storage: "s".into(),
                    incarnation: "n".into(),
                    epoch: 1,
                },
                "/managed/deep/data",
                inode.as_file_ref()?,
                None,
            )?
            .unwrap();
            let mut batch = dir.get_rocks_store().new_batch();
            batch.hsm_put_record(file.id, &record)?;
            batch.commit()?;
            dir.hsm_enabled = true;
        }
        Ok((root, fs))
    }

    #[test]
    fn hsm_mount_rejects_managed_subtrees_and_allows_unmanaged_siblings() -> FsResult<()> {
        let (_root, fs) = managed_fixture()?;
        let manager = MountManager::new(fs.clone());
        for path in ["/managed", "/managed/deep/data"] {
            let error = manager
                .mount(
                    Some(1),
                    path,
                    "file:///managed-ufs",
                    &MountOptions::builder().build(),
                )
                .unwrap_err();
            assert!(error.to_string().contains("overlaps managed HSM data"));
        }
        assert!(fs.fs_dir.read().get_mount_table()?.is_empty());
        manager.mount(
            Some(2),
            "/plain",
            "file:///plain-ufs",
            &MountOptions::builder().build(),
        )?;
        assert_eq!(manager.get_mount_table()?.len(), 1);
        assert_eq!(
            manager
                .get_read_mount_info(&Path::from_str("/plain/child")?)?
                .unwrap()
                .mount_id,
            2
        );
        Ok(())
    }

    #[test]
    fn hsm_mount_cannot_cover_reserved_namespace_before_it_exists() -> FsResult<()> {
        let (_root, fs) = managed_fixture()?;
        assert!(fs.fs_dir.read().hsm_namespace()?.is_none());
        let manager = MountManager::new(fs);
        for path in ["/", "/.hsm", "/.hsm/not-yet-created"] {
            let error = manager
                .mount(
                    Some(1),
                    path,
                    "file:///reserved-ufs",
                    &MountOptions::builder().build(),
                )
                .unwrap_err();
            assert!(error.to_string().contains("reserved HSM namespace"));
        }
        Ok(())
    }

    #[test]
    fn hsm_disabled_mount_keeps_existing_behavior() -> FsResult<()> {
        let (_root, fs) = managed_fixture()?;
        fs.fs_dir.write().hsm_enabled = false;
        let manager = MountManager::new(fs);
        manager.mount(
            Some(1),
            "/managed",
            "file:///managed-ufs",
            &MountOptions::builder().build(),
        )?;
        manager.restore()?;
        assert_eq!(manager.get_mount_table()?.len(), 1);
        Ok(())
    }

    #[test]
    fn persisted_and_replayed_incompatible_mounts_fail_closed_on_lookup() -> FsResult<()> {
        let (_root, fs) = managed_fixture()?;
        let options = MountOptions::builder().build();
        let bad = options
            .clone()
            .to_info(1, "/managed", "file:///managed-ufs");
        let good = options.to_info(2, "/plain", "file:///plain-ufs");
        {
            let mut dir = fs.fs_dir.write();
            dir.unprotected_store_mount(bad.clone())?;
            dir.unprotected_store_mount(good)?;
        }
        let manager = MountManager::new(fs);
        manager.unprotected_add_mount(bad)?;
        let path = Path::from_str("/managed/deep/data")?;
        assert!(manager.get_read_mount_info(&path).is_err());
        assert!(manager.restore().is_err());
        assert!(manager.get_read_mount_info(&path).is_err());
        assert!(manager.get_mount_table().is_err());
        assert!(manager
            .get_read_mount_info(&Path::from_str("/plain/child")?)?
            .is_some());
        let update = MountOptions::builder().update(true).build();
        assert!(manager
            .mount(None, "/managed", "file:///managed-ufs", &update)
            .is_err());
        manager.restore_best_effort();
        assert!(manager.get_read_mount_info(&path).is_err());
        assert!(manager.get_mount_table().is_err());
        // Operators can remove the old mount without first exposing it to readers.
        manager.umount("/managed")?;
        assert_eq!(manager.get_mount_table()?.len(), 1);
        Ok(())
    }

    fn mount_info(write_type: WriteType, access_mode: AccessMode, write_cache: bool) -> MountInfo {
        MountOptions::builder()
            .write_type(write_type)
            .access_mode(access_mode)
            .write_cache(write_cache)
            .build()
            .to_info(1, "/mnt", "file:///tmp/curvine-mount")
    }

    #[test]
    fn normalize_mount_config_accepts_write_cache_for_cache_mode_read_write() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadWrite, true);

        MountManager::normalize_mount_config(&mut info).unwrap();
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_read_only_cache_mode() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadOnly, true);

        let err = MountManager::normalize_mount_config(&mut info).unwrap_err();
        assert!(err
            .to_string()
            .contains("write_cache requires cache_mode with read_write"));
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_fs_mode() {
        let mut info = mount_info(WriteType::FsMode, AccessMode::ReadWrite, true);

        let err = MountManager::normalize_mount_config(&mut info).unwrap_err();
        assert!(err
            .to_string()
            .contains("write_cache requires cache_mode with read_write"));
    }

    #[test]
    fn normalize_mount_config_accepts_disabled_write_cache() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadOnly, false);

        MountManager::normalize_mount_config(&mut info).unwrap();
    }
}
