#![cfg(feature = "hsm-scale-test")]
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, Reader, RpcCode, Writer};
use curvine_model::{CreateFileOpts, MkdirOpts, OpenFlags, ProtoUtils};
use curvine_proto::{
    CreateFileRequest, HsmControlRequest, MkdirRequest, NativeMutationRequest,
    NativeMutationResponse, NativeMutationStatusProto as NativeStatus,
};
use curvine_runtime::{common::Utils, runtime::RpcRuntime};
use curvine_server::test::MiniCluster;
use futures::{stream, StreamExt, TryStreamExt};
use prost::Message;
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
fn disabled_native_mkdir_preserves_legacy_confirmation_and_rejects_bound_namespace() -> Result {
    let root = Utils::test_sub_dir(format!("native-disabled-{}", Utils::rand_str(8)));
    let mut conf = ClusterConf::format();
    conf.testing = false;
    conf.master.meta_dir = format!("{root}/meta");
    conf.journal.enable = true;
    conf.journal.journal_dir = format!("{root}/journal");
    conf.journal.leader_create_snapshot = Some(false);
    conf.journal.raft_tick_interval_ms = 100;
    conf.journal.raft_election_tick = 100;
    conf.journal.raft_min_election_ticks = 100;
    conf.journal.raft_max_election_ticks = 150;
    conf.log.level = "WARN".into();
    conf.log.log_dir = "stderr".into();
    conf.master.log = conf.log.clone();
    let cluster = MiniCluster::with_num(&conf, 1, 0);
    cluster.start_cluster();
    let fs = cluster.new_fs();
    cluster.clone_client_rt().block_on(async {
        let mut request = NativeMutationRequest {
            version: 1,
            incarnation: String::new(),
            session: request_id(),
            sequence: 1,
            acknowledged: 0,
            code: RpcCode::Mkdir as u32,
            header: MkdirRequest {
                path: "/off".into(),
                opts: ProtoUtils::mkdir_opts_to_pb(MkdirOpts::with_create(false)),
            }
            .encode_to_vec(),
        };
        let response: NativeMutationResponse = fs
            .fs_client()
            .rpc(RpcCode::NativeMutation, request.clone())
            .await?;
        assert!(response.success);
        assert_eq!(response.status, NativeStatus::LegacyResult as i32);
        assert!(response.incarnation.is_empty());
        assert!(fs.exists(&Path::from_str("/off")?).await?);
        fs.mkdir(&Path::from_str("/sdk-off")?, false).await?;
        let changed = fs
            .set_attr(
                &Path::from_str("/sdk-off")?,
                curvine_model::SetAttrOpts {
                    mode: Some(0o700),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(changed.mode, 0o700);
        fs.delete(&Path::from_str("/sdk-off")?, false).await?;
        assert!(!fs.exists(&Path::from_str("/sdk-off")?).await?);

        request.incarnation = "bound-volume".into();
        request.header = MkdirRequest {
            path: "/must-not-create".into(),
            opts: ProtoUtils::mkdir_opts_to_pb(MkdirOpts::with_create(false)),
        }
        .encode_to_vec();
        let rejected: NativeMutationResponse =
            fs.fs_client().rpc(RpcCode::NativeMutation, request).await?;
        assert_eq!(rejected.status, NativeStatus::Fenced as i32);
        assert!(!fs.exists(&Path::from_str("/must-not-create")?).await?);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
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
    conf.client.block_size_str = "4KB".into();
    conf.master.min_block_size = 4096;
    conf.log.level = "WARN".into();
    conf.log.log_dir = "stderr".into();
    conf.master.log = conf.log.clone();
    conf.worker.log = conf.log.clone();
    let cluster = MiniCluster::with_num(&conf, 3, 1);
    cluster.start_cluster();
    let fs = cluster.new_fs();
    cluster.clone_client_rt().block_on(async {
        let bootstrap_read = control(&fs, &json!({"op":"open_path","path":"/missing-before-namespace",
            "context":{"operation_id":request_id(),"controller_epoch":1,"attempt_epoch":1}})).await?;
        assert_eq!(bootstrap_read["kind"], "error");
        assert!(fs.exists(&Path::from_str("/.hsm")?).await?, "first read did not initialize the namespace");
        fs.hsm_control(HsmControlRequest {
            protocol_version: 1,
            payload: br#"{"op":"namespace"}"#.to_vec(),
        })
        .await?;
        let before_native = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        stream::iter(0..32)
            .map(|i| {
                let fs = &fs;
                async move {
                    fs.mkdir(&Path::from_str(format!("/native-batch-{i}"))?, false)
                        .await
                }
            })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await?;
        let after_native = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let bundles = after_native["native_bundles"].as_u64().unwrap()
            - before_native["native_bundles"].as_u64().unwrap();
        let commands = after_native["native_commands"].as_f64().unwrap()
            - before_native["native_commands"].as_f64().unwrap();
        assert!(bundles > 0 && bundles < 32 && commands >= 32.0);
        // Logical metadata queries share a committed read qualification and
        // never append workflow outcomes or empty journal barriers.
        let before_queries = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let exists: Vec<_> = stream::iter(0..32).map(|i| {
            let fs = &fs;
            async move { fs.exists(&Path::from_str(format!("/native-batch-{i}"))?).await }
        }).buffer_unordered(32).try_collect().await?;
        assert!(exists.into_iter().all(|exists| exists));
        let after_queries = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let batches = after_queries["metadata_read_batches"].as_u64().unwrap()
            - before_queries["metadata_read_batches"].as_u64().unwrap();
        let reads = after_queries["metadata_read_queries"].as_f64().unwrap()
            - before_queries["metadata_read_queries"].as_f64().unwrap();
        assert!(batches > 0 && batches < 32 && reads >= 32.0,
            "metadata query qualifications were not batched");
        assert_eq!(after_queries["hsm_barriers"], before_queries["hsm_barriers"]);
        assert_eq!(after_queries["outcome_bytes"], before_queries["outcome_bytes"]);
        // Cancel a query while projection coordination is paused. No handler
        // runs over speculative metadata and no vanished ticket pins the gate.
        let gate = cluster.get_active_master_fs().fs_dir().read().hsm_test_mutation_gate();
        let hold = gate.write().await;
        let cancelled_path = Path::from_str("/native-batch-0")?;
        let mut cancelled = Box::pin(fs.exists(&cancelled_path));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        drop(hold);
        tokio::time::timeout(std::time::Duration::from_secs(5),
            fs.mkdir(&Path::from_str("/after-cancelled-metadata-query")?, false)).await??;
        let native = fs.fs_client();
        let make_header = |path: &str, parents| {
            MkdirRequest {
                path: path.into(),
                opts: ProtoUtils::mkdir_opts_to_pb(MkdirOpts::with_create(parents)),
            }
            .encode_to_vec()
        };
        let mut request = NativeMutationRequest {
            version: 1,
            incarnation: String::new(),
            session: request_id(),
            sequence: 1,
            acknowledged: 0,
            code: RpcCode::Mkdir as u32,
            header: make_header("/native/original", true),
        };
        let bootstrap: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, request.clone()).await?;
        assert_eq!(bootstrap.status, NativeStatus::NeedsNamespace as i32);
        assert!(!fs.exists(&Path::from_str("/native")?).await?);
        request.incarnation = bootstrap.incarnation;
        let first: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, request.clone()).await?;
        assert!(first.success && first.status == NativeStatus::Committed as i32);
        fs.rename(
            &Path::from_str("/native")?,
            &Path::from_str("/native-renamed")?,
        )
        .await?;
        let repeated: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, request.clone()).await?;
        assert_eq!(first, repeated);
        assert!(!fs.exists(&Path::from_str("/native")?).await?);
        let mut rebound = request.clone();
        rebound.header = make_header("/rebound", true);
        let rejected: NativeMutationResponse = native.rpc(RpcCode::NativeMutation, rebound).await?;
        assert!(!rejected.success);
        assert!(!fs.exists(&Path::from_str("/rebound")?).await?);
        let mut next = request.clone();
        next.sequence = 2;
        next.acknowledged = 1;
        next.header = make_header("/after-ack", false);
        let second: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, next.clone()).await?;
        assert!(second.success);
        let retired: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, request.clone()).await?;
        assert_eq!(retired.status, NativeStatus::Fenced as i32);
        next.sequence = 3;
        next.acknowledged = 2;
        next.header = make_header("/missing/child", false);
        let failed: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, next.clone()).await?;
        assert!(!failed.success && failed.status == NativeStatus::Committed as i32);
        fs.mkdir(&Path::from_str("/missing")?, false).await?;
        let repeat_failure: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, next.clone()).await?;
        assert_eq!(failed, repeat_failure);
        assert!(!fs.exists(&Path::from_str("/missing/child")?).await?);
        next.incarnation = "another-volume".into();
        let fenced: NativeMutationResponse = native.rpc(RpcCode::NativeMutation, next).await?;
        assert_eq!(fenced.status, NativeStatus::Fenced as i32);
        let mut create = request.clone();
        create.session = request_id();
        create.sequence = 1;
        create.acknowledged = 0;
        create.code = RpcCode::CreateFile as u32;
        create.header = CreateFileRequest {
            path: "/native-created-file".into(),
            opts: ProtoUtils::create_opts_to_pb(CreateFileOpts::with_create(false), "manual"),
            flags: OpenFlags::new_create().set_overwrite(false).value(),
        }
        .encode_to_vec();
        let created: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, create.clone()).await?;
        assert!(created.success && created.status == NativeStatus::Committed as i32);
        fs.rename(
            &Path::from_str("/native-created-file")?,
            &Path::from_str("/native-renamed-file")?,
        )
        .await?;
        let retry_create: NativeMutationResponse =
            native.rpc(RpcCode::NativeMutation, create).await?;
        assert_eq!(created, retry_create);
        assert!(!fs.exists(&Path::from_str("/native-created-file")?).await?);
        let before_files = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let writers: Vec<_> = stream::iter(0..32)
            .map(|i| {
                let fs = &fs;
                async move {
                    fs.create(&Path::from_str(format!("/native-file-{i}"))?, false)
                        .await
                }
            })
            .buffer_unordered(32)
            .try_collect()
            .await?;
        let after_files = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let file_bundles = after_files["native_bundles"].as_u64().unwrap()
            - before_files["native_bundles"].as_u64().unwrap();
        let file_commands = after_files["native_commands"].as_f64().unwrap()
            - before_files["native_commands"].as_f64().unwrap();
        assert!(file_bundles > 0 && file_bundles < 32 && file_commands >= 32.0);
        let before_close = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        stream::iter(writers)
            .map(|mut writer| async move { writer.complete().await })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await?;
        let after_close = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let bundles = after_close["native_bundles"].as_u64().unwrap()
            - before_close["native_bundles"].as_u64().unwrap();
        let commands = after_close["native_commands"].as_f64().unwrap()
            - before_close["native_commands"].as_f64().unwrap();
        assert!(bundles > 0 && bundles < 32 && commands >= 32.0);
        let before_attrs = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        stream::iter(0..32)
            .map(|i| {
                let fs = &fs;
                async move {
                    fs.set_attr(&Path::from_str(format!("/native-file-{i}"))?,
                        curvine_model::SetAttrOpts { owner: Some("policy-owner".into()),
                            ..Default::default() }).await
                }
            })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await?;
        let after_attrs = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let bundles = after_attrs["native_bundles"].as_u64().unwrap()
            - before_attrs["native_bundles"].as_u64().unwrap();
        let commands = after_attrs["native_commands"].as_f64().unwrap()
            - before_attrs["native_commands"].as_f64().unwrap();
        assert!(bundles > 0 && bundles < 32 && commands >= 32.0,
            "nonrecursive setattr did not use native committed batches");
        assert_eq!(fs.get_status(&Path::from_str("/native-file-0")?).await?.owner, "policy-owner");
        // Real cross-family load touches independent inodes. Both pipelines
        // must preserve their results when ready entries share a proposal.
        let writes = stream::iter(0..64).map(|i| {
            let fs = &fs;
            async move { fs.mkdir(&Path::from_str(format!("/mixed-owned-{i}"))?, false).await }
        }).buffer_unordered(16).try_collect::<Vec<_>>();
        let opens = stream::iter(0..64).map(|_| {
            let fs = &fs;
            async move { fs.open(&Path::from_str("/native-file-0")?).await }
        }).buffer_unordered(16).try_collect::<Vec<_>>();
        let (_, concurrent_readers) = tokio::try_join!(writes, opens)?;
        stream::iter(concurrent_readers).map(|mut reader| async move { reader.complete().await })
            .buffer_unordered(16).try_collect::<Vec<_>>().await?;
        // A shared projection boundary represents an in-flight owned query
        // or committed command. Other owned families progress; legacy
        // speculative namespace handlers remain excluded until it finishes.
        let gate = cluster.get_active_master_fs().fs_dir().read().hsm_test_mutation_gate();
        let shared = gate.read().await;
        let online_path = Path::from_str("/native-file-0")?;
        let mut concurrent_reader = tokio::time::timeout(std::time::Duration::from_secs(2), fs.open(&online_path)).await??;
        concurrent_reader.complete().await?;
        tokio::time::timeout(std::time::Duration::from_secs(2),
            fs.mkdir(&Path::from_str("/owned-during-shared-query")?, false)).await??;
        tokio::time::timeout(std::time::Duration::from_secs(2), fs.get_status(&online_path)).await??;
        let source_dir = Path::from_str("/native-batch-1")?;
        let moved_dir = Path::from_str("/legacy-after-shared-query")?;
        let mut legacy = Box::pin(fs.rename(&source_dir, &moved_dir));
        assert!(tokio::time::timeout(std::time::Duration::from_millis(25), &mut legacy).await.is_err(),
            "legacy speculative rename entered a shared projection boundary");
        drop(shared);
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut legacy).await??;
        assert!(fs.exists(&moved_dir).await?);
        let before_delete = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        stream::iter(0..32).map(|i| {
            let fs = &fs;
            async move { fs.delete(&Path::from_str(format!("/native-file-{i}"))?, false).await }
        }).buffer_unordered(32).try_collect::<Vec<_>>().await?;
        let after_delete = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let bundles = after_delete["native_bundles"].as_u64().unwrap()
            - before_delete["native_bundles"].as_u64().unwrap();
        let commands = after_delete["native_commands"].as_f64().unwrap()
            - before_delete["native_commands"].as_f64().unwrap();
        assert!(bundles > 0 && bundles < 32 && commands >= 32.0,
            "nonrecursive delete did not use native committed batches");
        assert!(!fs.exists(&Path::from_str("/native-file-0")?).await?);
        let removed_dir = fs.delete(&Path::from_str("/native-batch-0")?, false).await?;
        assert_eq!(removed_dir.inodes, 0);
        assert!(!fs.exists(&Path::from_str("/native-batch-0")?).await?);

        // Native TTL still uses its original operation during migration, but
        // joins the same gate and confirms its journal prefix on an OS thread.
        let mut ttl_options = MkdirOpts::with_create(false);
        ttl_options.storage_policy.ttl_ms = 1;
        ttl_options.storage_policy.ttl_action = curvine_model::TtlAction::Delete;
        let expired = fs
            .mkdir_with_opts(&Path::from_str("/expired-native-dir")?, ttl_options)
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let active = cluster.get_active_master_fs();
        let cleaned = tokio::task::spawn_blocking(move || {
            curvine_master::master::meta::inode::ttl::InodeTtlExecutor::with_managers(active)
                .execute_by_id(expired.id)
        })
        .await??;
        assert!(cleaned.0);
        assert!(!fs.exists(&Path::from_str("/expired-native-dir")?).await?);
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
        let apply_commits = after["read_apply_commits"].as_u64().unwrap()
            - before["read_apply_commits"].as_u64().unwrap();
        let apply_commands = after["read_apply_commands"].as_f64().unwrap()
            - before["read_apply_commands"].as_f64().unwrap();
        assert!(apply_commits > 0 && apply_commands >= 32.0);
        assert!(
            apply_commands > apply_commits as f64,
            "Raft batching did not coalesce RocksDB writes"
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
        let before_append = metrics.hsm_test_commit_stats();
        let mut append = fs.append(&path).await?;
        append.write(b"++").await?;
        append.complete().await?;
        let after_append = metrics.hsm_test_commit_stats();
        assert!(
            after_append["native_commands"].as_f64().unwrap()
                - before_append["native_commands"].as_f64().unwrap()
                >= 2.0
        );
        let mut reader = fs.open(&path).await?;
        let mut appended = vec![0; 13];
        assert_eq!(reader.read(&mut appended).await?, 13);
        assert_eq!(appended, b"replacement++");
        reader.complete().await?;
        let via_open = Path::from_str("/created-through-open")?;
        let mut writer = fs.open_for_write(&via_open, false).await?;
        writer.write(b"native open create").await?;
        writer.complete().await?;
        let mut reader = fs.open(&via_open).await?;
        let mut bytes = vec![0; 18];
        assert_eq!(reader.read(&mut bytes).await?, 18);
        assert_eq!(bytes, b"native open create");
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
        // A large native layout must not be copied into the durable result
        // for every open. Test fresh replies, retries after ancestor rename,
        // real block reads and retirement through the production RPC path.
        fs.mkdir(&Path::from_str("/before")?, false).await?;
        let big_path = Path::from_str("/before/many-blocks")?;
        let bytes: Vec<u8> = (0..40 * 4096).map(|i| (i % 251) as u8).collect();
        let mut writer = fs.create(&big_path, false).await?;
        writer.write(&bytes).await?;
        writer.complete().await?;
        fs.rename(&Path::from_str("/before")?, &Path::from_str("/after")?)
            .await?;
        let reader = uuid::Uuid::new_v4().simple().to_string();
        let original = json!({"op":"open_path","path":"/after/many-blocks",
            "reader":reader,"request_id":request_id()});
        let outcomes_before = metrics.hsm_test_commit_stats()["outcome_bytes"]
            .as_u64()
            .unwrap();
        let grant = control(&fs, &original).await?;
        assert_eq!(grant["kind"], "read");
        assert_eq!(
            grant["data"]["blocks"]["block_locs"]
                .as_array()
                .unwrap()
                .len(),
            40
        );
        let outcome_bytes = metrics.hsm_test_commit_stats()["outcome_bytes"]
            .as_u64()
            .unwrap()
            - outcomes_before;
        assert!(outcome_bytes > 0);
        assert!(
            // Metrics are shared by all three Masters in this process. Even
            // their combined result bytes must fit below one inline reply.
            (outcome_bytes as usize) < serde_json::to_vec(&grant)?.len(),
            "large read result still persists all block placements"
        );
        assert_eq!(control(&fs, &original).await?, grant);
        let mut data_reader = fs.open(&Path::from_str("/after/many-blocks")?).await?;
        let mut received = vec![0; bytes.len()];
        let mut offset = 0;
        while offset < received.len() {
            let count = data_reader.read(&mut received[offset..]).await?;
            assert!(count > 0);
            offset += count;
        }
        assert_eq!(received, bytes);
        data_reader.complete().await?;
        assert_eq!(
            control(
                &fs,
                &json!({"op":"read_close",
            "entity":grant["data"]["data"]["entity"],"data":grant["data"]["data"],
            "reader":reader,"request_id":request_id()})
            )
            .await?["kind"],
            "done"
        );
        assert_eq!(control(&fs, &original).await?["data"]["code"], "fenced");
        // Retire more than one GC page. Physical deletion is driven solely by
        // the committed outbox and actual Worker heartbeat acknowledgements.
        let current_path = Path::from_str("/after/many-blocks")?;
        let old_layout = fs.fs_client().get_block_locations(&current_path).await?;
        let mut writer = fs.open_for_write(&current_path, true).await?;
        writer.write(b"new layout").await?;
        writer.complete().await?;
        let active = cluster.get_active_master_fs();
        assert!(
            active
                .fs_dir()
                .read()
                .get_rocks_store()
                .hsm_scan(b"\x03hsm:native-gc:v1:", None, 100)?
                .len()
                >= 40
        );
        let outcomes = metrics.hsm_test_commit_stats()["outcome_bytes"]
            .as_u64()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            assert_eq!(control(&fs, &json!({"op":"gc_batch"})).await?["kind"], "gc");
            let pending = active.fs_dir().read().get_rocks_store().hsm_scan(
                b"\x03hsm:native-gc:v1:",
                None,
                1,
            )?;
            if pending.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "native GC did not observe Worker acknowledgements"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            metrics.hsm_test_commit_stats()["outcome_bytes"]
                .as_u64()
                .unwrap(),
            outcomes,
            "GC progress manufactured permanent workflow outcomes"
        );
        {
            let dir = active.fs_dir();
            let guard = dir.read();
            for old in &old_layout.block_locs {
                assert!(guard.get_block_locations(old.block.id)?.is_empty());
            }
        }
        let new_layout = fs.fs_client().get_block_locations(&current_path).await?;
        assert_eq!(new_layout.block_locs.len(), 1);
        assert!(old_layout
            .block_locs
            .iter()
            .all(|old| old.block.id != new_layout.block_locs[0].block.id));
        let mut reader = fs.open(&current_path).await?;
        let mut bytes = vec![0; 10];
        assert_eq!(reader.read(&mut bytes).await?, 10);
        assert_eq!(bytes, b"new layout");
        reader.complete().await?;
        // Removing the inode must leave independently replayable physical GC.
        let deleted = fs.delete(&current_path, false).await?;
        assert_eq!(deleted.inodes, 1);
        assert!(!fs.exists(&current_path).await?);
        assert!(active.fs_dir().read().hsm_record(new_layout.status.id)?.is_none());
        assert!(!active.fs_dir().read().get_rocks_store().hsm_scan(
            b"\x03hsm:native-gc:v1:", None, 1)?.is_empty());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            assert_eq!(control(&fs, &json!({"op":"gc_batch"})).await?["kind"], "gc");
            if active.fs_dir().read().get_rocks_store().hsm_scan(
                b"\x03hsm:native-gc:v1:", None, 1)?.is_empty() { break; }
            assert!(std::time::Instant::now() < deadline, "deleted inode's GC did not finish");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // Proxy one Worker endpoint and pause a real Seal response. This
        // tests the complete operation, not a synthetic lock-only callback.
        use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let proxy_port = listener.local_addr()?.port();
        let upstream = new_layout.block_locs[0].locs[0].inet_addr().to_string();
        let pause_reply = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let proxy = {
            let pause_reply = pause_reply.clone(); let entered = entered.clone(); let resume = resume.clone();
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let upstream = upstream.clone(); let pause_reply = pause_reply.clone();
                    let entered = entered.clone(); let resume = resume.clone();
                    tokio::spawn(async move {
                        let server = tokio::net::TcpStream::connect(upstream).await?;
                        let (mut from_client, mut to_client) = client.into_split();
                        let (mut from_worker, mut to_worker) = server.into_split();
                        let send = tokio::spawn(async move { tokio::io::copy(&mut from_client, &mut to_worker).await });
                        let mut buffer = vec![0; 65536];
                        loop {
                            let n = from_worker.read(&mut buffer).await?;
                            if n == 0 { break; }
                            if pause_reply.swap(false, Ordering::AcqRel) {
                                entered.notify_one(); resume.notified().await;
                            }
                            to_client.write_all(&buffer[..n]).await?;
                        }
                        send.abort();
                        Ok::<(), std::io::Error>(())
                    });
                }
            })
        };
        let proxy_worker = curvine_model::WorkerAddress { worker_id: u32::MAX - 1, hostname: "localhost".into(),
            ip_addr: "127.0.0.1".into(), rpc_port: proxy_port as u32, ..Default::default() };
        let actual_worker = new_layout.block_locs[0].locs[0].worker_id;
        let original_worker = active.worker_manager.read().get_worker(actual_worker).unwrap().clone();
        let mut retired_for_placement = original_worker.clone();
        retired_for_placement.status = curvine_model::WorkerStatus::Decommission;
        active.worker_manager.write().add_test_worker(retired_for_placement);
        active.worker_manager.write().add_test_worker(curvine_model::WorkerInfo {
            address: proxy_worker, last_update: curvine_runtime::common::LocalTime::mills(), ..Default::default()
        });
        let digest = json!({"algorithm":"blake3", "value":blake3::hash(b"").as_bytes().to_vec()});
        let namespace = control(&fs, &json!({"op":"namespace"})).await?;
        let epoch = namespace["data"]["epoch"].as_u64().unwrap();
        let mut selected = None;
        for attempt in 0..2 {
            let context = json!({"operation_id":format!("io-turn-{}-{attempt}", request_id()), "controller_epoch":epoch,"attempt_epoch":1});
            let prepared = control(&fs, &json!({"op":"prepare_import", "context":context,
                "path":format!("/.hsm/io-turn-{attempt}"),"length":0,"digest":digest})).await?;
            assert_eq!(prepared["kind"], "recall");
            let cap: Vec<u8> = serde_json::from_value(prepared["data"]["destination"]["capability"].clone())?;
            let endpoint: curvine_model::hsm::native::Endpoint = serde_json::from_slice(&cap)?;
            if endpoint.worker.rpc_port == proxy_port as u32 {
                selected = Some((context, prepared["data"].clone())); break;
            }
        }
        let (context, target) = selected.expect("placement did not select the sole live test endpoint");
        let commit_request = json!({"op":"commit_import", "context":context,"target":target,
            "evidence":{"length":0,"digest":digest}});
        pause_reply.store(true, Ordering::Release);
        let commit = {
            let fs = fs.clone(); let request = commit_request.clone();
            tokio::spawn(async move { control(&fs, &request).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified()).await?;
        let independent = tokio::time::timeout(std::time::Duration::from_millis(500),
            fs.mkdir(&Path::from_str("/while-worker-seal-waits")?, false)).await;
        // Take over while the original controller is waiting for the Worker.
        // Requalification must fence that publication without caching a
        // permanent failure that would prevent the new owner from resuming.
        let takeover = if independent.is_ok() {
            let claim = json!({"op":"claim_controller", "expected_epoch":epoch,
                "context":{"operation_id":format!("takeover-{}",request_id()),"controller_epoch":epoch,"attempt_epoch":1}});
            Some(tokio::time::timeout(std::time::Duration::from_secs(5), control(&fs, &claim)).await)
        } else { None };
        resume.notify_one();
        let interrupted = commit.await??;
        assert!(independent.is_ok(), "Worker Seal reply wait blocked unrelated committed metadata");
        independent.unwrap()?;
        let takeover = takeover.expect("missing takeover result")??;
        assert_eq!(takeover["kind"], "epoch");
        assert_eq!(takeover["data"], epoch + 1);
        assert_eq!(interrupted["kind"], "error");
        assert_eq!(interrupted["data"]["code"], "fenced");
        let mut resumed = commit_request;
        resumed["context"]["controller_epoch"] = json!(epoch + 1);
        let published = control(&fs, &resumed).await?;
        assert_eq!(published["kind"], "entity", "old I/O completion poisoned the new controller's logical step");
        active.worker_manager.write().add_test_worker(original_worker);
        proxy.abort();

        // Once polled/admitted, a controller operation outlives its caller.
        // Pausing the real authority gate makes cancellation deterministic.
        let namespace = control(&fs, &json!({"op":"namespace"})).await?;
        let epoch = namespace["data"]["epoch"].as_u64().unwrap();
        let gate = active.fs_dir().read().hsm_test_mutation_gate();
        let pause = gate.write().await;
        let claim = json!({"op":"claim_controller", "expected_epoch":epoch,
            "context":{"operation_id":format!("cancelled-{}", request_id()), "controller_epoch":epoch,"attempt_epoch":1}});
        let mut caller = Box::pin(active.hsm_control(HsmControlRequest { protocol_version: 1, payload: serde_json::to_vec(&claim)? }));
        assert!(futures::poll!(&mut caller).is_pending());
        drop(caller);
        drop(pause);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let namespace = control(&fs, &json!({"op":"namespace"})).await?;
            if namespace["data"]["epoch"].as_u64() == Some(epoch + 1) { break; }
            assert!(std::time::Instant::now() < deadline, "accepted HSM operation disappeared when its caller was cancelled");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let replay = control(&fs, &claim).await?;
        assert_eq!(replay["kind"], "epoch");
        assert_eq!(replay["data"], epoch + 1);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
}
