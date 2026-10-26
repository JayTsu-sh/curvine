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

async fn legacy_block_locations(
    fs: &curvine_client_core::file::CurvineFileSystem,
    path: &Path,
) -> curvine_error::FsResult<curvine_proto::GetBlockLocationsResponse> {
    fs.fs_client()
        .rpc(
            RpcCode::GetBlockLocations,
            curvine_proto::GetBlockLocationsRequest {
                path: path.encode(),
                metadata_only: None,
            },
        )
        .await
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
    // All concurrent commands must share one TCP connection: Master RPC
    // dispatch must admit them together instead of relying on a large pool.
    conf.client.master_conn_pool_size = 1;
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
        // A lost Worker's synthetic derived inventory spans three pages.
        // Pause the actual cleanup between pages, then commit and read real
        // foreground data through TCP/Raft while cleanup remains unfinished.
        {
            let master = cluster.get_active_master_fs();
            let worker_id = u32::MAX - 10;
            master.add_test_worker(curvine_model::WorkerInfo {
                address: curvine_model::WorkerAddress { worker_id, ..Default::default() },
                ..Default::default()
            });
            {
                let directory = master.fs_dir();
                let directory = directory.read();
                let mut batch = directory.get_rocks_store().new_batch();
                for id in 1..=2050 {
                    batch.add_location(i64::MAX - id, &curvine_model::BlockLocation::with_id(worker_id))?;
                }
                batch.commit()?;
            }
            let started = std::sync::Arc::new(tokio::sync::Notify::new());
            let paused = started.clone();
            let (resume, wait) = std::sync::mpsc::channel();
            struct CleanupResumeOnDrop(Option<std::sync::mpsc::Sender<()>>);
            impl Drop for CleanupResumeOnDrop {
                fn drop(&mut self) {
                    if let Some(sender) = self.0.take() { let _ = sender.send(()); }
                }
            }
            let resume = CleanupResumeOnDrop(Some(resume));
            let cleanup_master = master.clone();
            let cleanup = tokio::task::spawn_blocking(move || {
                let mut pages = Vec::new();
                let count = cleanup_master.hsm_test_delete_lost_worker_locations(worker_id, |rows| {
                    pages.push(rows);
                    if pages.len() == 1 {
                        paused.notify_one();
                        wait.recv_timeout(std::time::Duration::from_secs(20)).unwrap();
                    }
                })?;
                Ok::<_, curvine_error::FsError>((count, pages))
            });
            tokio::time::timeout(std::time::Duration::from_secs(15), started.notified()).await?;
            assert_eq!(master.fs_dir().read().get_worker_block_ids(worker_id)?.len(), 1026);
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                let path = Path::from_str("/foreground-during-worker-cleanup")?;
                let mut writer = fs.create(&path, false).await?;
                writer.write(b"foreground during cleanup").await?;
                writer.complete().await?;
                let mut reader = fs.open(&path).await?;
                let mut bytes = vec![0; 25];
                assert_eq!(reader.read(&mut bytes).await?, 25);
                assert_eq!(bytes, b"foreground during cleanup");
                reader.complete().await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }).await??;
            // The cleanup really is still paused at the first page.
            assert!(!cleanup.is_finished());
            drop(resume);
            let (count, pages) = tokio::time::timeout(std::time::Duration::from_secs(15), cleanup).await???;
            assert_eq!(count, 2050);
            assert_eq!(pages, [1024, 1024, 2]);
            assert!(master.fs_dir().read().get_worker_block_ids(worker_id)?.is_empty());
        }
        // Parallel imports and native creation share one inode allocator,
        // including Worker allocation and requalification.

        let ids = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            stream::iter(0..16).map(|index| {
                let fs = &fs;
                async move {
                    let context = json!({"operation_id":request_id(),"controller_epoch":1,"attempt_epoch":1});
                    let prepare = json!({"op":"prepare_import", "context":context,
                        "path":format!("/.hsm/parallel-import-{index}"), "length":0,
                        "digest":{"algorithm":"blake3","value":blake3::hash(b"").as_bytes().to_vec()}});
                    let path = Path::from_str(format!("/parallel-native-{index}"))?;
                    let (import, native) = tokio::join!(control(fs, &prepare), fs.mkdir(&path, false));
                    native?;
                    let import = import?;
                    assert_eq!(import["kind"], "recall", "{import}");
                    let private = import["data"]["data"]["entity"]["entity"].as_str().ok_or("missing import ID")?.parse::<i64>()?;
                    let native = fs.get_status(&path).await?.id;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>([private, native])
                }
            }).buffer_unordered(8).try_collect::<Vec<_>>().await
        }).await??;

        let unique: std::collections::BTreeSet<_> = ids.into_iter().flatten().collect();
        assert_eq!(unique.len(), 32, "parallel HSM/native inode identity collision");
        // Lifecycle metadata, including the off-gate scan phase, retains its
        // committed results under exclusive observation coordination.
        let lifecycle_path = Path::from_str("/shared-lifecycle-file")?;
        fs.create(&lifecycle_path, false).await?.complete().await?;
        let lifecycle_id = fs.get_status(&lifecycle_path).await?.id;
        let data = cluster.get_active_master_fs().fs_dir().read().hsm_record(lifecycle_id)?.unwrap().state.data;

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let token = uuid::Uuid::new_v4().simple().to_string();
            let begun = control(&fs, &json!({"op":"seal_begin", "entity":data.entity, "data":data, "token":token})).await?;
            assert_eq!(begun["kind"], "blocks", "{begun}");
            let sealed = control(&fs, &json!({"op":"seal_commit", "entity":data.entity, "data":data, "token":token,
                "evidence":{"length":0,"digest":{"algorithm":"blake3","value":blake3::hash(b"").as_bytes().to_vec()}}})).await?;
            assert_eq!(sealed["kind"], "entity", "{sealed}");
            let context = || json!({"operation_id":request_id(),"controller_epoch":1,"attempt_epoch":1});
            let policy = json!({"id":"shared-lifecycle", "revision":1,"enabled":true,"prefix":"/shared-lifecycle-file",
                "min_size":0,"max_size":null,"min_residency_ms":0,"action":"archive","backend":"local"});
            let put = control(&fs, &json!({"op":"policy_put", "context":context(), "policy":policy,"expected_revision":null})).await?;
            assert_eq!(put["kind"], "policy", "{put}");
            let scanned = control(&fs, &json!({"op":"policy_scan","context":context(),"id":"shared-lifecycle","limit":256})).await?;
            assert_eq!(scanned["kind"], "scan", "{scanned}");
            let task = scanned["data"]["admitted"][0].as_str().ok_or("missing admitted lifecycle task")?;
            let claimed = control(&fs, &json!({"op":"intent_claim", "context":context(), "id":task,"owner":"shared-test"})).await?;
            assert_eq!(claimed["data"]["status"], "running", "{claimed}");
            let finished = control(&fs, &json!({"op":"intent_finish","context":context(),"id":task,
                "version":claimed["data"]["version"],"claim":claimed["data"]["claim"],
                "report":{"status":"retry","message":"shared publication test"}})).await?;
            assert_eq!(finished["data"]["status"], "ready", "{finished}");
            // Pause the actual Source claim after preparation, let a real
            // ordinary reader commit, then deliver the stale claim to Raft.
            // The SAME request can subsequently resolve to Superseded.
            let claim_context = context();
            let operation = claim_context["operation_id"].as_str().unwrap();
            let (started, resume) = cluster.get_active_master_fs().fs_dir().write().hsm_test_pause_lifecycle(operation);
            struct ResumeOnDrop(std::sync::Arc<tokio::sync::Notify>);
            impl Drop for ResumeOnDrop { fn drop(&mut self) { self.0.notify_one(); } }
            let wake = ResumeOnDrop(resume.clone());
            let request = json!({"op":"intent_claim", "context":claim_context,"id":task,"owner":"racing-reader"});
            let client = fs.clone();
            let input = request.clone();
            let pending = tokio::spawn(async move { control(&client, &input).await });
            tokio::time::timeout(std::time::Duration::from_secs(3), started.notified()).await?;
            let mut opening = Box::pin(fs.open(&lifecycle_path));
            assert!(tokio::time::timeout(std::time::Duration::from_millis(25), &mut opening).await.is_err(), "read crossed an exclusive lifecycle preparation");
            resume.notify_one();
            let claimed = pending.await??;
            assert_eq!(claimed["data"]["status"], "running", "{claimed}");
            let mut reader = opening.await?;
            reader.complete().await?;
            fs.flush_hsm_read_closes().await?;
            drop(wake);
            assert_eq!(control(&fs, &json!({"op":"gc_batch"})).await?["kind"], "gc");
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }).await??;

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
        // Public batch methods retain one durable identity per batch. The
        // three stages use the owned coordinator, including per-item close
        // failure and ordering for repeated paths.
        let batch_client = fs.fs_client();
        let before_batches = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        let batch_files = batch_client.create_files_batch((0..32).map(|i| {
            let mut opts = CreateFileOpts::with_create(true);
            opts.block_size = 4096;
            opts.replicas = 1;
            (format!("/public-batch/file-{i}"), opts, OpenFlags::new_create().set_overwrite(false))
        }).collect()).await?;
        assert_eq!(batch_files.len(), 32);
        let locations = batch_client.add_blocks_batch((0..32)
            .map(|i| format!("/public-batch/file-{i}")).collect()).await?;
        assert_eq!(locations.len(), 32);
        assert!(locations.iter().all(|block| !block.locs.is_empty()));
        let name = batch_client.context().clone_client_name();
        let mut closes: Vec<_> = (0..32).map(|i|
            (format!("/public-batch/file-{i}"), 0, vec![], name.clone(), false)).collect();
        closes.insert(1, ("/public-batch/missing".into(), 0, vec![], name, false));
        let closed = batch_client.complete_files_batch(closes).await?;
        assert_eq!(closed.len(), 33);
        assert!(!closed[1]);
        assert!(closed.iter().enumerate().all(|(i, ok)| i == 1 || *ok));
        let after_batches = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        assert!(after_batches["native_commands"].as_f64().unwrap()
            - before_batches["native_commands"].as_f64().unwrap() >= 3.0,
            "public batches bypassed owned native publication");
        let managed_batch = control(&fs, &json!({"op":"resolve","path":"/public-batch/file-0"})).await?;
        assert_eq!(managed_batch["data"]["residency"], "Online");
        fs.delete(&Path::from_str("/public-batch")?, true).await?;
        // Inject a lost receipt after a real committed successful prefix.
        // Uncertain is not an instruction to retry under a new identity.
        let mut uncertain_batch = request.clone();
        uncertain_batch.session = request_id();
        uncertain_batch.sequence = 1;
        uncertain_batch.acknowledged = 0;
        uncertain_batch.code = RpcCode::CreateFilesBatch as u32;
        let item = |path: &str| CreateFileRequest {
            path: path.into(), opts: ProtoUtils::create_opts_to_pb(CreateFileOpts::with_create(true), "lost-receipt"),
            flags: OpenFlags::new_create().set_overwrite(false).value(),
        };
        uncertain_batch.header = curvine_proto::CreateFilesBatchRequest {
            requests: vec![item("/uncertain-batch"), item("/uncertain-batch"), item("/uncertain-never")],
        }.encode_to_vec();
        cluster.get_active_master_fs().fs_dir().write()
            .hsm_test_drop_native_receipt(&uncertain_batch.session);
        let lost: NativeMutationResponse = native.rpc(RpcCode::NativeMutation, uncertain_batch.clone()).await?;
        assert_eq!(lost.status, NativeStatus::Uncertain as i32);
        assert!(fs.exists(&Path::from_str("/uncertain-batch")?).await?);
        assert!(!fs.exists(&Path::from_str("/uncertain-never")?).await?);
        fs.rename(&Path::from_str("/uncertain-batch")?, &Path::from_str("/uncertain-moved")?).await?;
        let original: NativeMutationResponse = native.rpc(RpcCode::NativeMutation, uncertain_batch.clone()).await?;
        assert_eq!(original.status, NativeStatus::Committed as i32);
        assert!(!original.success);
        let repeated: NativeMutationResponse = native.rpc(RpcCode::NativeMutation, uncertain_batch).await?;
        assert_eq!(original, repeated);
        assert!(!fs.exists(&Path::from_str("/uncertain-batch")?).await?);
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
        tokio::time::timeout(std::time::Duration::from_secs(2), fs.get_status(&online_path)).await??;
        let pending_path = Path::from_str("/owned-during-shared-query")?;
        let mut pending_mkdir = Box::pin(fs.mkdir(&pending_path, false));
        assert!(tokio::time::timeout(std::time::Duration::from_millis(25), &mut pending_mkdir).await.is_err(), "structural writer crossed an active shared turn");
        drop(shared);
        pending_mkdir.await?;
        let source_dir = Path::from_str("/native-batch-1")?;
        let owned_dir = Path::from_str("/owned-after-shared-query")?;
        fs.rename(&source_dir, &owned_dir).await?;
        let shared = gate.read().await;
        let moved_dir = Path::from_str("/legacy-after-shared-query")?;
        // rpc_bytes intentionally bypasses SDK Native routing: this really
        // exercises the speculative compatibility handler, unlike fs.rename.
        let legacy_client = fs.fs_client();
        let mut legacy = Box::pin(legacy_client.rpc_bytes(RpcCode::Rename,
            curvine_proto::RenameRequest {
                src: owned_dir.encode(), dst: moved_dir.encode(), flags: 0,
            }));
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
            qualifications < 32,
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
        assert_eq!(apply_commits, 0, "fast opens appended legacy per-reader commands");
        assert_eq!(apply_commands, 0.0);
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
        // Fast observations remain visible across a stable writer's cache
        // invalidation. Duplicate opens/renewals cannot extend an old receipt,
        // and a repeated batched close cannot revive or recount a reader.
        let mut fast = json!({"op":"open_path","path":"/hot","read_mode":"fast_v1",
            "reader":uuid::Uuid::new_v4().simple().to_string(),"request_id":request_id()});
        let binding = control(&fs,&fast).await?;
        assert_eq!(binding["kind"],"read_fast_bind");
        fast["issuer"] = binding["data"].clone();
        let grant = control(&fs,&fast).await?;
        assert_eq!(grant["kind"],"read_fast");
        let revision = control(&fs,&json!({"op":"resolve","path":"/hot"})).await?["data"]["revision"].as_u64().unwrap();
        let duplicate = control(&fs,&fast).await?;
        assert_eq!(duplicate["data"]["expires_ms"],grant["data"]["expires_ms"]);
        assert_eq!(control(&fs,&json!({"op":"resolve","path":"/hot"})).await?["data"]["revision"].as_u64(),Some(revision));
        let mut rebound = fast.clone();rebound["request_id"] = json!(request_id());
        assert_eq!(control(&fs,&rebound).await?["data"]["code"],"fenced");
        let renew = json!({"op":"read_renew","read_mode":"fast_v1","data":grant["data"]["data"],
            "entity":grant["data"]["data"]["entity"],"reader":grant["data"]["reader"],"request_id":request_id()});
        let renewed = control(&fs,&renew).await?;
        assert_eq!(renewed["kind"],"done");
        assert_eq!(control(&fs,&renew).await?["data"]["expires_ms"],renewed["data"]["expires_ms"]);
        assert_eq!(control(&fs,&json!({"op":"resolve","path":"/hot"})).await?["data"]["revision"].as_u64(),Some(revision+1));
        assert!(fs.create(&path,true).await.is_err(),"uncheckpointed fast grant did not protect overwrite");
        let close = json!({"op":"read_close_batch","read_mode":"fast_v1","closes":[{
            "data":grant["data"]["data"],"reader":grant["data"]["reader"]}]});
        assert_eq!(control(&fs,&close).await?["kind"],"done");
        assert_eq!(control(&fs,&close).await?["kind"],"done");
        assert_eq!(control(&fs,&fast).await?["data"]["code"],"fenced");
        assert_eq!(control(&fs,&renew).await?["data"]["code"],"fenced");
        fs.mkdir(&Path::from_str("/after-fast-close-checkpoint")?,false).await?;
        assert_eq!(control(&fs,&json!({"op":"resolve","path":"/hot"})).await?["data"]["revision"].as_u64(),Some(revision+2));
        // Idle retirement replaces closed tombstones with the timestamp floor.
        // An older, still-active open must retain its original receipt even
        // after that floor has advanced past its request timestamp.
        let mut retained = fast.clone();
        retained["reader"] = json!(uuid::Uuid::new_v4().simple().to_string());
        retained["request_id"] = json!(request_id());
        let retained_grant = control(&fs, &retained).await?;
        assert_eq!(retained_grant["kind"], "read_fast");
        let mut retired = retained.clone();
        retired["reader"] = json!(uuid::Uuid::new_v4().simple().to_string());
        retired["request_id"] = json!(request_id());
        let retired_grant = control(&fs, &retired).await?;
        assert_eq!(retired_grant["kind"], "read_fast");
        assert_eq!(control(&fs, &json!({"op":"read_close_batch","read_mode":"fast_v1","closes":[{
            "data":retired_grant["data"]["data"],"reader":retired_grant["data"]["reader"]}]})).await?["kind"], "done");
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert_eq!(control(&fs, &retired).await?["data"]["code"], "fenced");
        let retained_duplicate = control(&fs, &retained).await?;
        assert_eq!(retained_duplicate["kind"], "read_fast");
        assert_eq!(retained_duplicate["data"]["expires_ms"], retained_grant["data"]["expires_ms"]);
        assert_eq!(control(&fs, &json!({"op":"read_close_batch","read_mode":"fast_v1","closes":[{
            "data":retained_grant["data"]["data"],"reader":retained_grant["data"]["reader"]}]})).await?["kind"], "done");
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
        assert!(legacy_block_locations(&fs,&current_path).await.is_err(), "raw managed open bypassed read protection");
        assert!(matches!(
            fs.get_unmanaged_read_blocks(&current_path).await,
            Err(curvine_error::FsError::Unsupported(_))
        ));
        // Exercise qualified snapshot traversal through the real RPC/Raft path.
        let snapshot = fs.get_cv_metadata_snapshot_page(None, Some(10_000)).await?;
        assert!(snapshot.entries.iter().any(|entry| entry.status.path == current_path.path()));
        let old_layout = fs.get_block_locations(&current_path).await?;
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
        // Compact namespace inputs: normal SDK writes, inherited policy
        // metadata, protected subtree rejection and physical Worker cleanup.
        let tree = Path::from_str("/compact-tree/sub")?;
        fs.mkdir(&tree, true).await?;
        for name in ["a", "b"] {
            let path = Path::from_str(format!("/compact-tree/sub/{name}"))?;
            let mut writer = fs.create(&path, false).await?;
            writer.write(b"namespace payload").await?;
            writer.complete().await?;
        }
        let tree_a = Path::from_str("/compact-tree/sub/a")?;
        let file_id = fs.get_status(&tree_a).await?.id;
        let tree_b = Path::from_str("/compact-tree/sub/b")?;
        let old_target = fs.get_status(&tree_b).await?.id;
        let before_namespace = metrics.hsm_test_commit_stats()["namespace_commands"].as_f64().unwrap();
        fs.rename(&Path::from_str("/compact-tree")?, &Path::from_str("/compact-moved")?).await?;
        let moved_a = Path::from_str("/compact-moved/sub/a")?;
        let moved_b = Path::from_str("/compact-moved/sub/b")?;
        assert_eq!(fs.get_status(&moved_a).await?.id, file_id);
        assert_eq!(active.fs_dir().read().hsm_record(file_id)?.unwrap().path, "/compact-moved/sub/a");
        fs.set_attr(&Path::from_str("/compact-moved")?, curvine_model::SetAttrOpts {
            recursive: true, owner: Some("tree-policy-owner".into()), mode: Some(0o755),
            ..Default::default()
        }).await?;
        assert_eq!(active.fs_dir().read().hsm_record(file_id)?.unwrap().owner, "tree-policy-owner");
        // Replacing an allocated file persists its cleanup before the source
        // is published at that edge.
        fs.rename(&moved_a, &moved_b).await?;
        assert_eq!(fs.get_status(&moved_b).await?.id, file_id);
        assert!(active.fs_dir().read().hsm_record(old_target)?.is_none());
        let mut held = fs.open(&moved_b).await?;
        let mut data = vec![0; 17];
        assert_eq!(held.read(&mut data).await?, 17);
        assert_eq!(data, b"namespace payload");
        assert!(fs.delete(&Path::from_str("/compact-moved")?, true).await.is_err());
        assert_eq!(fs.get_status(&moved_b).await?.id, file_id);
        held.complete().await?;
        let deleted = fs.delete(&Path::from_str("/compact-moved")?, true).await?;
        assert_eq!(deleted.inodes, 1);
        assert!(!fs.exists(&Path::from_str("/compact-moved")?).await?);
        assert!(active.fs_dir().read().hsm_record(file_id)?.is_none());
        assert!(metrics.hsm_test_commit_stats()["namespace_commands"].as_f64().unwrap() >= before_namespace + 5.0);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !active.fs_dir().read().get_rocks_store().hsm_scan(b"\x03hsm:native-gc:v1:", None, 1)?.is_empty() {
            assert!(std::time::Instant::now() < deadline, "compact namespace GC did not finish");
            assert_eq!(control(&fs, &json!({"op":"gc_batch"})).await?["kind"], "gc");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // An OS TTL worker joins the same native queue. Namespace init was
        // already completed by ordinary SDK traffic; no nested runtime is used.
        let ttl_dir = Path::from_str("/native-ttl-empty")?;
        fs.mkdir(&ttl_dir, false).await?;
        let ttl_status = fs.get_status(&ttl_dir).await?;
        fs.set_attr(&ttl_dir, curvine_model::SetAttrOpts { mtime: Some(1), ttl_ms: Some(1),
            ttl_action: Some(curvine_model::TtlAction::Delete), ..Default::default() }).await?;
        let ttl = curvine_master::master::meta::inode::ttl::InodeTtlExecutor::with_managers(active.clone());
        let outcome = tokio::task::spawn_blocking(move || ttl.execute_by_id(ttl_status.id)).await??;
        assert!(outcome.0);
        assert!(!fs.exists(&ttl_dir).await?);
        // Native resize/assignment: partial truncation, whole-block retirement
        // and extending an empty layout through real Worker allocation.
        let resize_path = Path::from_str("/native-resize")?;
        let resize_bytes: Vec<u8> = (0..13000).map(|i| (i % 251) as u8).collect();
        let mut writer = fs.create(&resize_path, false).await?;
        writer.write(&resize_bytes).await?; writer.complete().await?;
        let original_resize_layout = fs.fs_client().get_block_locations(&resize_path).await?;
        fs.resize(&resize_path, curvine_model::FileAllocOpts::with_truncate(4103)).await?;
        let mut reader = fs.open(&resize_path).await?;
        let mut truncated = vec![];
        while reader.has_remaining() { truncated.extend_from_slice(reader.async_read(None).await?.as_slice()); }
        reader.complete().await?;
        assert_eq!(truncated, resize_bytes[..4103]);
        assert_eq!(fs.get_status(&resize_path).await?.len, 4103);
        fs.resize(&resize_path, curvine_model::FileAllocOpts::with_truncate(0)).await?;
        assert_eq!(fs.get_status(&resize_path).await?.len, 0);
        fs.resize(&resize_path, curvine_model::FileAllocOpts::with_truncate(8193)).await?;
        let expanded = fs.fs_client().get_block_locations(&resize_path).await?;
        assert!(expanded.block_locs.iter().all(|new| original_resize_layout.block_locs.iter().all(|old| old.block.id != new.block.id)));
        let mut reader = fs.open(&resize_path).await?;
        let mut zeros = vec![];
        while reader.has_remaining() { zeros.extend_from_slice(reader.async_read(None).await?.as_slice()); }
        reader.complete().await?;
        assert_eq!(zeros, vec![0; 8193]);
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
        let private_bytes = b"private-proof";
        let digest = json!({"algorithm":"blake3", "value":blake3::hash(private_bytes).as_bytes().to_vec()});
        let namespace = control(&fs, &json!({"op":"namespace"})).await?;
        let epoch = namespace["data"]["epoch"].as_u64().unwrap();
        let mut selected = None;
        for attempt in 0..2 {
            let context = json!({"operation_id":format!("io-turn-{}-{attempt}", request_id()), "controller_epoch":epoch,"attempt_epoch":1});
            let prepared = control(&fs, &json!({"op":"prepare_import", "context":context,
                "path":format!("/.hsm/io-turn-{attempt}"),"length":private_bytes.len(),"digest":digest})).await?;
            assert_eq!(prepared["kind"], "recall");
            let cap: Vec<u8> = serde_json::from_value(prepared["data"]["destination"]["capability"].clone())?;
            let endpoint: curvine_model::hsm::native::Endpoint = serde_json::from_slice(&cap)?;
            if endpoint.worker.rpc_port == proxy_port as u32 {
                selected = Some((context, prepared["data"].clone())); break;
            }
        }
        let (context, target) = selected.expect("placement did not select the sole live test endpoint");
        let cap:Vec<u8> = serde_json::from_value(target["destination"]["capability"].clone())?;
        let endpoint:curvine_model::hsm::native::Endpoint = serde_json::from_slice(&cap)?;
        let written = curvine_client_core::file::hsm_io::hsm_worker_rpc(fs.rpc_conf(),&endpoint.worker,
            &curvine_model::hsm::native::WorkerRequest::Write {endpoint:endpoint.clone(),offset:0,bytes:private_bytes.to_vec()}).await?;
        assert!(matches!(written,curvine_model::hsm::native::WorkerReply::Done));
        let commit_request = json!({"op":"commit_import", "context":context,"target":target,
            "evidence":{"length":private_bytes.len(),"digest":digest}});
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
        let id=published["data"]["data"]["entity"]["entity"].as_str().ok_or("missing imported inode")?.parse::<i64>()?;
        let imported_path=active.fs_dir().read().get_inode_path(id)?;
        let mut imported=fs.open(&Path::from_str(imported_path)?).await?;
        let mut actual=vec![0;private_bytes.len()];
        assert_eq!(imported.read(&mut actual).await?,private_bytes.len());
        assert_eq!(actual,private_bytes);
        imported.complete().await?;fs.flush_hsm_read_closes().await?;
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

#[test]
fn fast_grants_survive_leader_transfer_without_a_durable_reader_row() -> Result {
    use curvine_raft::{
        conf::JournalConfExt,
        proto::raft::{RaftRequest, RaftResponse},
        raft::{LibRaftMessage, RaftClient, RaftCode},
    };
    use curvine_rpc::message::Builder;
    let root = Utils::test_sub_dir(format!("hsm-fast-transfer-{}", Utils::rand_str(8)));
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
        let path = Path::from_str("/uncheckpointed-reader")?;
        let mut writer = fs.create(&path, false).await?;
        writer.write(b"epoch-data").await?;
        writer.complete().await?;
        let old = cluster.get_active_master_fs();
        old.hsm_test_pause_access_checkpoints(true);
        let mut request = json!({
            "op": "open_path", "path": path.path(),
            "reader": uuid::Uuid::new_v4().simple().to_string(),
            "request_id": request_id(), "read_mode": "fast_v1",
        });
        let binding = control(&fs, &request).await?;
        assert_eq!(binding["kind"], "read_fast_bind");
        request["issuer"] = binding["data"].clone();
        let grant = control(&fs, &request).await?;
        assert_eq!(grant["kind"], "read_fast");
        let repeated = control(&fs, &request).await?;
        assert_eq!(grant["data"]["reader"], repeated["data"]["reader"]);
        assert_eq!(grant["data"]["expires_ms"], repeated["data"]["expires_ms"]);
        assert!(repeated["data"]["granted_ms"].as_i64() >= grant["data"]["granted_ms"].as_i64());
        let mut sdk_reader = fs.open(&path).await?;
        let inode = grant["data"]["blocks"]["status"]["id"]
            .as_i64()
            .ok_or("missing inode")?;
        let observed = control(&fs, &json!({"op": "resolve", "path": path.path()})).await?;
        let durable = old.fs_dir().read().hsm_record(inode)?.unwrap();
        assert!(
            durable.readers.is_empty(),
            "test failed to retain an uncheckpointed grant"
        );
        assert!(observed["data"]["revision"].as_u64().unwrap() > durable.state.revision);
        assert!(
            legacy_block_locations(&fs, &path).await.is_err(),
            "raw managed open bypassed protection"
        );
        assert_eq!(fs.get_block_locations(&path).await?.status.id, inode);

        let old_conf = cluster
            .master_conf
            .iter()
            .find(|conf| conf.master.meta_dir == old.conf.meta_dir)
            .ok_or("missing leader config")?;
        let old_id = old_conf.journal.node_id()?;
        let target = old_conf
            .journal
            .journal_addrs
            .iter()
            .find(|peer| peer.id != old_id)
            .ok_or("missing successor")?
            .id;
        let client = RaftClient::from_conf(cluster.clone_client_rt(), &old_conf.journal);
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
        let promoted = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(current) = cluster.hsm_test_find_active_master_fs() {
                    if current.conf.meta_dir != old.conf.meta_dir {
                        return current;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await?;
        assert!(promoted
            .fs_dir()
            .read()
            .hsm_record(inode)?
            .unwrap()
            .readers
            .is_empty());
        assert!(
            fs.delete(&path, false).await.is_err(),
            "new leader deleted data protected only by an old volatile grant"
        );
        let recovery_started = std::time::Instant::now();
        let rebound = control(&fs, &request).await?;
        assert_eq!(
            rebound["kind"], "read_fast_bind",
            "old open packet created a new grant"
        );
        let renew = json!({
            "op": "read_renew", "read_mode": "fast_v1",
            "entity": grant["data"]["data"]["entity"],
            "data": grant["data"]["data"], "reader": grant["data"]["reader"],
        });
        assert_eq!(control(&fs, &renew).await?["data"]["code"], "fenced");
        let mut byte = [0];
        assert_eq!(sdk_reader.read(&mut byte).await?, 1);
        assert_eq!(byte[0], b'e');
        eprintln!(
            "FAST_TRANSFER_GRACE_STARTED: waiting for the real 120-second protection horizon"
        );
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            recovery_started + std::time::Duration::from_secs(123),
        ))
        .await;
        assert!(
            sdk_reader.read(&mut byte).await.is_err(),
            "expired SDK grant exposed already-buffered data"
        );
        sdk_reader.complete().await?;
        fs.flush_hsm_read_closes().await?;
        // The first Raft RPC after the real idle interval may find a closed
        // transport. Reconcile an uncertain delete against this unchanged
        // path; no competing writer can recreate it in this test.
        let cleanup_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if matches!(fs.exists(&path).await, Ok(false)) {
                break;
            }
            if let Err(error) = fs.delete(&path, false).await {
                eprintln!("post-grace cleanup will reconcile transient outcome: {error}");
            }
            assert!(
                std::time::Instant::now() < cleanup_deadline,
                "post-grace cleanup did not confirm deletion"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
}
