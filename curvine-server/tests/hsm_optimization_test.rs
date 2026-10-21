#![cfg(feature = "hsm-scale-test")]
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_proto::HsmControlRequest;
use curvine_runtime::{common::Utils, runtime::RpcRuntime};
use curvine_server::test::MiniCluster;
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn request_id() -> String {
    format!(
        "{:016x}{}",
        curvine_runtime::common::LocalTime::mills(),
        uuid::Uuid::new_v4().simple()
    )
}

async fn control(
    fs: &curvine_client_core::file::CurvineFileSystem,
    request: &Value,
) -> Result<Value> {
    let response = fs
        .hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: serde_json::to_vec(request)?,
        })
        .await?;
    Ok(serde_json::from_slice(&response.payload)?)
}

#[test]
fn read_batches_keep_all_hot_inode_readers_until_committed_close() -> Result {
    let root = Utils::test_sub_dir(format!("hsm-read-batch-{}", Utils::rand_str(8)));
    let mut conf = ClusterConf::format();
    conf.testing = false;
    conf.master.hsm_enabled = true;
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
    conf.client.master_conn_pool_size = 32;
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
        fs.hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: br#"{"op":"namespace"}"#.to_vec(),
        })
        .await?;
        let path = Path::from_str("/hot")?;
        let mut writer = fs.create(&path, false).await?;
        writer.write(b"protected").await?;
        writer.complete().await?;
        let metrics = curvine_master::master::Master::get_metrics()?;
        let before = metrics.hsm_test_commit_stats();
        let readers: Vec<_> = stream::iter(0..32)
            .map(|_| fs.open(&path))
            .buffer_unordered(32)
            .try_collect()
            .await?;
        let after = metrics.hsm_test_commit_stats();
        let barriers =
            after["hsm_barriers"].as_u64().unwrap() - before["hsm_barriers"].as_u64().unwrap();
        let flushes = after["flushes"].as_u64().unwrap() - before["flushes"].as_u64().unwrap();
        assert_eq!(barriers, 0, "read qualification appended empty logs");
        let qualifications =
            after["read_indexes"].as_u64().unwrap() - before["read_indexes"].as_u64().unwrap();
        assert!(
            (1..32).contains(&qualifications),
            "read qualifications were not grouped: {qualifications}"
        );
        assert!(
            flushes < 32,
            "read opens were not grouped: {flushes} proposals"
        );
        assert!(
            fs.create(&path, true).await.is_err(),
            "active readers must protect overwrite"
        );
        stream::iter(readers)
            .map(|mut reader| async move { reader.complete().await })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await?;
        let mut writer = fs.create(&path, true).await?;
        writer.write(b"replacement").await?;
        writer.complete().await?;
        let mut reader = fs.open(&path).await?;
        let mut bytes = vec![0; 11];
        assert_eq!(reader.read(&mut bytes).await?, 11);
        assert_eq!(bytes, b"replacement");
        reader.complete().await?;
        let reader = uuid::Uuid::new_v4().simple().to_string();
        let original =
            json!({"op":"open_path","path":"/hot","reader":reader,"request_id":request_id()});
        let grant = control(&fs, &original).await?;
        assert_eq!(grant["kind"], "read");
        let repeated = control(&fs, &original).await?;
        assert!(
            grant == repeated,
            "a retry must return its original grant without extending it"
        );
        let close = json!({"op":"read_close", "entity":grant["data"]["data"]["entity"],
            "data":grant["data"]["data"],"reader":reader,"request_id":request_id()});
        assert_eq!(control(&fs, &close).await?["kind"], "done");
        assert_eq!(control(&fs, &original).await?["data"]["code"], "fenced");
        let mut rebound = original.clone();
        rebound["path"] = json!("/different");
        assert_eq!(control(&fs, &rebound).await?["data"]["code"], "conflict");
        // A duplicate inside a batch must not advance the private projection
        // and cause the following independent reader to fail its CAS.
        let a = uuid::Uuid::new_v4().simple().to_string();
        let b = uuid::Uuid::new_v4().simple().to_string();
        let a_open = json!({"op":"open_path","path":"/hot","reader":a,"request_id":request_id()});
        let b_open = json!({"op":"open_path","path":"/hot","reader":b,"request_id":request_id()});
        let (a_grant, a_retry, b_grant) = futures::try_join!(
            control(&fs, &a_open),
            control(&fs, &a_open),
            control(&fs, &b_open)
        )?;
        assert_eq!(a_grant["kind"], "read");
        assert!(a_grant == a_retry);
        assert_eq!(b_grant["kind"], "read");
        for (reader, grant) in [(a, a_grant), (b, b_grant)] {
            assert_eq!(
                control(
                    &fs,
                    &json!({"op":"read_close", "entity":grant["data"]["data"]["entity"],
                "data":grant["data"]["data"], "reader":reader,"request_id":request_id()})
                )
                .await?["kind"],
                "done"
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
}
