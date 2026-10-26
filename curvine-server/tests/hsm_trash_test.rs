#![cfg(feature = "hsm-scale-test")]

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, Reader, RpcCode, Writer};
use curvine_master::master::fs::{MasterFilesystem, TrashExpunger};
use curvine_model::{ProtoUtils, SetAttrOptsBuilder};
use curvine_proto::{
    DeleteRequest, DeleteResponse, HsmControlRequest, NativeMutationRequest,
    NativeMutationResponse, NativeMutationStatusProto, RestoreTrashRequest, RestoreTrashResponse,
};
use curvine_raft::{
    conf::JournalConfExt,
    proto::raft::{RaftRequest, RaftResponse},
    raft::{LibRaftMessage, RaftClient, RaftCode},
};
use curvine_rpc::message::Builder;
use curvine_runtime::{
    common::{LocalTime, Utils},
    runtime::RpcRuntime,
};
use curvine_server::test::MiniCluster;
use prost::Message;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn identity() -> String {
    format!(
        "{:016x}{}",
        LocalTime::mills(),
        uuid::Uuid::new_v4().simple()
    )
}

async fn control(fs: &CurvineFileSystem, request: Value) -> Result<Value> {
    let response = fs
        .hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: serde_json::to_vec(&request)?,
        })
        .await?;
    Ok(serde_json::from_slice(&response.payload)?)
}

async fn write_file(fs: &CurvineFileSystem, path: &Path, data: &[u8]) -> Result {
    let mut writer = fs.create(path, false).await?;
    writer.write(data).await?;
    writer.complete().await?;
    Ok(())
}

async fn move_leadership(cluster: &MiniCluster) -> Result<MasterFilesystem> {
    let old = cluster.get_active_master_fs();
    let conf = cluster
        .master_conf
        .iter()
        .find(|conf| conf.master.meta_dir == old.conf.meta_dir)
        .ok_or("missing leader configuration")?;
    let old_id = conf.journal.node_id()?;
    let target = conf
        .journal
        .journal_addrs
        .iter()
        .find(|peer| peer.id != old_id)
        .ok_or("missing successor")?
        .id;
    let client = RaftClient::from_conf(cluster.clone_client_rt(), &conf.journal);
    let mut message = LibRaftMessage {
        from: target,
        to: old_id,
        ..Default::default()
    };
    message.set_msg_type(raft::eraftpb::MessageType::MsgTransferLeader);
    let packet = Builder::new_rpc(RaftCode::Raft)
        .proto_header(RaftRequest {
            message,
            sender_session: None,
            receiver_session: None,
            leader_commit: None,
        })
        .build();
    let _: RaftResponse = client
        .timeout_rpc(old_id, packet)
        .await
        .map_err(|(_, error)| error)?;
    Ok(tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(current) = cluster.hsm_test_find_active_master_fs() {
                if current.conf.meta_dir != old.conf.meta_dir {
                    return current;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?)
}

#[test]
fn committed_trash_preserves_data_retries_and_failover_protection() -> Result {
    let root = Utils::test_sub_dir(format!("hsm-trash-{}", Utils::rand_str(8)));
    let mut conf = ClusterConf::format();
    conf.testing = false;
    conf.master.hsm_enabled = true;
    conf.master.trash_enabled = true;
    conf.master.trash_retention = "24h".into();
    conf.master.trash_check_interval = "1h".into();
    conf.master.meta_dir = format!("{root}/meta");
    conf.worker.data_dir = vec![format!("{root}/worker")];
    conf.journal.enable = true;
    conf.journal.writer_channel_size = 0;
    conf.journal.leader_create_snapshot = Some(false);
    conf.journal.journal_dir = format!("{root}/journal");
    conf.journal.snapshot_entries = u64::MAX;
    conf.journal.snapshot_interval = "24h".into();
    conf.journal.raft_tick_interval_ms = 100;
    conf.journal.raft_election_tick = 100;
    conf.journal.raft_min_election_ticks = 100;
    conf.journal.raft_max_election_ticks = 150;
    conf.client.master_conn_pool_size = 1;
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.log.level = "WARN".into();
    conf.log.log_dir = "stderr".into();
    conf.master.log = conf.log.clone();
    conf.worker.log = conf.log.clone();
    let cluster = MiniCluster::with_num(&conf, 3, 1);
    cluster.start_cluster();
    let fs = cluster.new_fs();
    cluster.clone_client_rt().block_on(async {
        let source = Path::from_str("/original")?;
        let payload = b"retained-through-trash-and-transfer";
        write_file(&fs, &source, payload).await?;
        fs.set_attr(&source, SetAttrOptsBuilder::new()
            .add_x_attr("user.alpha", b"one".to_vec())
            .add_x_attr("user.beta", b"two".to_vec())
            .add_x_attr("user.gamma", b"three".to_vec())
            .build()).await?;
        let layout = fs.get_block_locations(&source).await?;
        let inode = layout.status.id;

        let mut reader = fs.open(&source).await?;
        assert!(fs.delete(&source, false).await.is_err(), "trash ignored a live reader");
        assert!(fs.exists(&source).await?);
        reader.complete().await?;
        fs.flush_hsm_read_closes().await?;

        let namespace = control(&fs, json!({"op":"namespace"})).await?;
        let delete = NativeMutationRequest {
            version: 1,
            incarnation: namespace["data"]["incarnation"].as_str().ok_or("missing namespace")?.into(),
            session: identity(), sequence: 1, acknowledged: 0,
            code: RpcCode::Delete as u32,
            header: DeleteRequest {
                path: source.encode(), recursive: false, skip_trash: Some(false),
            }.encode_to_vec(),
        };
        let deleted: NativeMutationResponse = fs.fs_client().rpc(RpcCode::NativeMutation, delete.clone()).await?;
        assert_eq!(deleted.status, NativeMutationStatusProto::Committed as i32);
        assert!(deleted.success, "{}", String::from_utf8_lossy(&deleted.body));
        assert!(DeleteResponse::decode(deleted.header.as_slice())?.trashed());
        assert!(!fs.exists(&source).await?);
        let entries = fs.list_trash().await?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].origin, source.path());
        assert_eq!(entries[0].status.id, inode);
        let trash_path = Path::from_str(&entries[0].path)?;
        let trash_root_id = cluster.get_active_master_fs().file_status("/.Trash")?.id;
        let replicas = cluster.hsm_test_master_filesystems();
        assert_eq!(replicas.len(), 3);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if replicas.iter().all(|master| {
                    master.file_status("/.Trash").is_ok_and(|status| status.id == trash_root_id)
                        && master.file_status(trash_path.path()).is_ok_and(|status| status.id == inode)
                }) { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await?;
        let in_trash = fs.get_block_locations(&trash_path).await?;
        assert_eq!(
            in_trash.block_locs.iter().map(|block| (block.block.id, block.block.len)).collect::<Vec<_>>(),
            layout.block_locs.iter().map(|block| (block.block.id, block.block.len)).collect::<Vec<_>>(),
            "soft delete changed block identities",
        );
        assert_eq!(cluster.get_active_master_fs().fs_dir().read().hsm_record(inode)?.unwrap().path, trash_path.path());

        let mut retained_reader = fs.open(&trash_path).await?;
        assert!(fs.empty_trash().await.is_err(), "purge ignored a live reader");
        assert_eq!(fs.list_trash().await?.len(), 1);
        retained_reader.complete().await?;
        fs.flush_hsm_read_closes().await?;

        let restored = Path::from_str("/restored")?;
        write_file(&fs, &restored, b"replaced destination").await?;
        let restore = NativeMutationRequest {
            version: 1,
            incarnation: delete.incarnation.clone(),
            session: identity(), sequence: 1, acknowledged: 0,
            code: RpcCode::RestoreTrash as u32,
            header: RestoreTrashRequest {
                path: trash_path.encode(), dst: Some(restored.encode()), overwrite: Some(true),
            }.encode_to_vec(),
        };
        let restored_reply: NativeMutationResponse = fs.fs_client().rpc(RpcCode::NativeMutation, restore.clone()).await?;
        assert_eq!(restored_reply.status, NativeMutationStatusProto::Committed as i32);
        assert!(restored_reply.success, "{}", String::from_utf8_lossy(&restored_reply.body));
        let result = ProtoUtils::restore_trash_res_from_pb(RestoreTrashResponse::decode(restored_reply.header.as_slice())?);
        assert_eq!(result.dst.as_deref(), Some(restored.path()));
        assert_eq!(result.status.ok_or("missing restored status")?.id, inode);
        assert!(fs.list_trash().await?.is_empty());
        let mut reader = fs.open(&restored).await?;
        let mut bytes = vec![0; payload.len()];
        assert_eq!(reader.read(&mut bytes).await?, payload.len());
        assert_eq!(bytes, payload);
        reader.complete().await?;
        fs.flush_hsm_read_closes().await?;
        assert!(fs.delete(&restored, false).await?.trashed);
        let entry = fs.list_trash().await?.remove(0);
        let trash_path = Path::from_str(&entry.path)?;

        // A retry of the first deletion must not delete a replacement name.
        write_file(&fs, &source, b"new-generation-at-original-name").await?;
        let repeated: NativeMutationResponse = fs.fs_client().rpc(RpcCode::NativeMutation, delete.clone()).await?;
        assert_eq!(repeated, deleted);
        assert!(fs.exists(&source).await?);

        let state = control(&fs, json!({"op":"resolve","path":trash_path.path()})).await?;
        let release = control(&fs, json!({"op":"release","expected":state["data"],
            "context":{"operation_id":identity(),"controller_epoch":namespace["data"]["epoch"],"attempt_epoch":1}
        })).await?;
        assert_eq!(release["kind"], "error");
        assert!(release.to_string().contains("trash retention"), "{release}");

        let old = cluster.get_active_master_fs();
        old.hsm_test_pause_access_checkpoints(true);
        let mut retained_reader = fs.open(&trash_path).await?;
        let mut byte = [0];
        assert_eq!(retained_reader.read(&mut byte).await?, 1);
        assert!(old.fs_dir().read().hsm_record(inode)?.unwrap().readers.is_empty());
        let promoted = move_leadership(&cluster).await?;
        assert_eq!(fs.list_trash().await?[0].status.id, inode);
        let replay: NativeMutationResponse = fs.fs_client().rpc(RpcCode::NativeMutation, delete.clone()).await?;
        assert_eq!(replay, deleted, "new leader did not retain the original receipt");
        let restore_replay: NativeMutationResponse = fs.fs_client().rpc(RpcCode::NativeMutation, restore.clone()).await?;
        assert_eq!(restore_replay, restored_reply, "new leader changed the multi-xattr restore receipt");
        assert!(fs.empty_trash().await.is_err(), "new leader purged a volatile protected grant");
        assert!(fs.exists(&source).await?);
        let future = entry.delete_ms + promoted.conf.trash_retention_ms() as i64 + 1;
        let expunger = TrashExpunger::new(promoted.clone(), promoted.master_monitor.clone());
        let protected = tokio::task::spawn_blocking(move || expunger.expunge_expired(future)).await??;
        assert_eq!(protected.entries, 0, "retention expiry bypassed the recovery horizon");
        // The monotonic protection clock starts lazily on the first guarded
        // operation in this term, after transport redirection has completed.
        let recovery_started = Instant::now();
        assert_eq!(retained_reader.read(&mut byte).await?, 1);

        eprintln!("TRASH_TRANSFER_GRACE_STARTED: waiting for the real 120-second protection horizon");
        tokio::time::sleep_until(tokio::time::Instant::from_std(recovery_started + Duration::from_secs(123))).await;
        assert!(retained_reader.read(&mut byte).await.is_err(), "expired grant exposed cached trash bytes");
        retained_reader.complete().await?;
        fs.flush_hsm_read_closes().await?;

        let active = cluster.get_active_master_fs();
        let expunger = TrashExpunger::new(active.clone(), active.master_monitor.clone());
        let expired = tokio::task::spawn_blocking(move || expunger.expunge_expired(future)).await??;
        assert_eq!(expired.entries, 1);
        assert_eq!(expired.inodes, 1);
        assert!(fs.list_trash().await?.is_empty());
        assert!(fs.exists(&source).await?);
        let deadline = Instant::now() + Duration::from_secs(30);
        let fs_dir = active.fs_dir();
        loop {
            // MiniCluster has no external HSM controller. Drive its normal
            // durable GC interface, as the production controller does.
            assert_eq!(control(&fs, json!({"op":"gc_batch"})).await?["kind"], "gc");
            let retired = {
                let dir = fs_dir.read();
                let locations_retired = layout.block_locs.iter().all(|block| dir.get_block_locations(block.block.id).is_ok_and(|locations| locations.is_empty()));
                locations_retired && dir.get_rocks_store().hsm_scan(b"\x03hsm:native-gc:v1:", None, 1)?.is_empty()
            };
            if retired { break; }
            assert!(Instant::now() < deadline, "purged blocks did not complete committed GC");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    })
}
