// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Committed namespace preparation. Transport and activation follow only once
//! all native mutation families have deterministic preparation/publication.
mod authority;
mod commands;
mod session;
mod view;

#[cfg(test)]
mod tests {
    use super::view::*;
    use crate::master::{
        journal::JournalSystem,
        meta::inode::{InodeDir, InodeFile, InodeView, ROOT_INODE_ID},
    };
    use curvine_core_error::CommonResult;

    #[test]
    fn owned_preparation_resolves_pending_moves_without_publishing_or_copying_subtrees(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let dir = fs.mkdir("/data", false)?;
        let file = fs.create("/data/existing", false)?;
        let authority = fs.fs_dir.read();
        let sequence = authority
            .get_rocks_store()
            .db
            .get_db()
            .latest_sequence_number();
        let mut batch = BatchView::new(&authority, 1024 * 1024);
        let mut first = batch.command();
        let new_dir = first.allocate_inode()?;
        let new_file = first.allocate_inode()?;
        let mut inode = InodeDir::new(new_dir, 1000);
        inode.parent_id = ROOT_INODE_ID;
        first.put_inode(InodeView::new_dir("private".into(), inode))?;
        let mut inode = InodeFile::new(new_file, 1000);
        inode.parent_id = new_dir;
        first.put_inode(InodeView::new_file("child".into(), inode))?;
        first.set_edge(ROOT_INODE_ID, "private", Some(new_dir))?;
        first.set_edge(new_dir, "child", Some(new_file))?;
        assert_eq!(
            first.resolve("/private/child")?,
            vec![ROOT_INODE_ID, new_dir, new_file]
        );
        assert_eq!(first.status("/private", new_dir)?.children_num, 1);
        let first = first.finish()?;
        assert_eq!(first.inodes.len(), 2);
        let mut second = batch.command();
        let mut inode = second.inode(dir.id)?.unwrap();
        inode.change_name("moved".into());
        second.put_inode(inode)?;
        second.set_edge(ROOT_INODE_ID, "data", None)?;
        second.set_edge(ROOT_INODE_ID, "moved", Some(dir.id))?;
        assert_eq!(
            second.resolve("/moved/existing")?,
            vec![ROOT_INODE_ID, dir.id, file.id]
        );
        assert_eq!(second.status("/moved", dir.id)?.children_num, 1);
        second.finish()?;
        let mut third = batch.command();
        assert_eq!(third.resolve("/data/existing")?, vec![ROOT_INODE_ID]);
        assert_eq!(
            third.resolve("/moved/existing")?,
            vec![ROOT_INODE_ID, dir.id, file.id]
        );
        third.set_edge(ROOT_INODE_ID, "abandoned", Some(new_dir))?;
        third.delete_inode(new_file)?;
        drop(third);
        let mut later = batch.command();
        assert_eq!(later.resolve("/abandoned")?, vec![ROOT_INODE_ID]);
        assert_eq!(
            later.resolve("/private/child")?,
            vec![ROOT_INODE_ID, new_dir, new_file]
        );
        drop(later);
        drop(batch);
        assert_eq!(
            authority
                .get_rocks_store()
                .db
                .get_db()
                .latest_sequence_number(),
            sequence
        );
        assert_eq!(authority.root_dir.get_child("data").unwrap().id(), dir.id);
        assert!(authority.root_dir.get_child("moved").is_none());
        assert!(authority.root_dir.get_child("private").is_none());
        Ok(())
    }

    #[test]
    fn failed_budget_admission_does_not_consume_ids_or_leave_pending_edges() -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = JournalSystem::fs_only_for_test(&conf)?;
        let authority = fs.fs_dir.read();
        let next = authority.last_inode_id() + 1;
        let mut batch = BatchView::new(&authority, 1024);
        let mut command = batch.command();
        assert_eq!(command.allocate_inode()?, next);
        let mut dir = InodeDir::new(next, 1000);
        dir.parent_id = ROOT_INODE_ID;
        command.put_inode(InodeView::new_dir("never".into(), dir))?;
        let mut rejected = false;
        for index in 0..64 {
            if command
                .set_edge(ROOT_INODE_ID, &format!("never-{index}"), Some(next))
                .is_err()
            {
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        assert!(command.finish().is_err());
        let mut next_command = batch.command();
        assert_eq!(next_command.allocate_inode()?, next);
        assert_eq!(next_command.resolve("/never-0")?, vec![ROOT_INODE_ID]);
        assert!(authority.root_dir.get_child("never").is_none());
        Ok(())
    }
}

#[cfg(test)]
mod command_tests {
    use super::{commands, view::BatchView};
    use curvine_core_error::CommonResult;
    use curvine_model::MkdirOpts;
    use curvine_proto::MkdirResponse;
    use prost::Message;

    #[test]
    fn mkdir_prepares_recursive_edges_and_original_response_without_live_changes(
    ) -> CommonResult<()> {
        crate::master::Master::init_test_metrics();
        let root = tempfile::tempdir()?;
        let mut conf = curvine_config::ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = root.path().join("meta").to_string_lossy().into_owned();
        conf.journal.journal_dir = root.path().join("journal").to_string_lossy().into_owned();
        let fs = crate::master::journal::JournalSystem::fs_only_for_test(&conf)?;
        let authority = fs.fs_dir.read();
        let mut batch = BatchView::new(&authority, 1024 * 1024);
        let result = commands::mkdir(&mut batch, "/a/b", MkdirOpts::with_create(true), 777)?;
        let header = MkdirResponse::decode(result.response.as_slice())?;
        assert_eq!(header.status.path, "/a/b");
        assert_eq!(header.status.mtime, 777);
        assert_eq!(result.edits.edges.len(), 2);
        assert!(authority.root_dir.get_child("a").is_none());
        assert!(commands::mkdir(&mut batch, "/a/b", MkdirOpts::with_create(false), 888).is_err());
        let repeated = commands::mkdir(&mut batch, "/a/b", MkdirOpts::with_create(true), 999)?;
        assert_eq!(result.response, repeated.response);
        assert!(repeated.edits.edges.is_empty());
        Ok(())
    }
}
