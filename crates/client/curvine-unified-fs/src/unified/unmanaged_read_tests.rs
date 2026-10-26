// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.

use super::UnifiedFileSystem;
use crate::{FallbackFsReader, UfsFileSystem};
use curvine_client_core::file::{FsContext, FsReader};
use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, Path, Reader, RpcCode};
use curvine_io::DataSlice;
use curvine_model::hsm::native::{DataKey, EntityKey, ReadHandle};
use curvine_model::{FileBlocks, FileStatus, MountInfo, MountOptions, ProtoUtils, WriteType};
use curvine_net::net::{ConnState, InetAddr, NetUtils};
use curvine_proto::{
    GetBlockLocationsRequest, GetBlockLocationsResponse, GetFilesystemInfoResponse,
    GetMountTableResponse,
};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

fn runtime() -> Arc<Runtime> {
    static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Arc::new(Runtime::new("unmanaged-read-test", 2, 4)))
        .clone()
}

struct State {
    mount: MountInfo,
    mounted: bool,
    managed: bool,
    table_reads: usize,
    layouts: Vec<bool>,
}

struct Handler(Arc<Mutex<State>>);

impl MessageHandler for Handler {
    type Error = FsError;

    fn handle(&self, request: &Message) -> FsResult<Message> {
        let mut state = self.0.lock().unwrap();
        match RpcCode::from(request.code()) {
            RpcCode::GetFilesystemInfo => Ok(Builder::success_with_header(
                request,
                GetFilesystemInfoResponse::default(),
            )
            .build()),
            RpcCode::GetMountTable => {
                state.table_reads += 1;
                let mount_table = if state.mounted {
                    vec![ProtoUtils::mount_info_to_pb(state.mount.clone())]
                } else {
                    vec![]
                };
                Ok(
                    Builder::success_with_header(request, GetMountTableResponse { mount_table })
                        .build(),
                )
            }
            RpcCode::GetBlockLocations => {
                let header: GetBlockLocationsRequest = request.parse_header()?;
                let metadata_only = header.metadata_only.unwrap_or(false);
                state.layouts.push(metadata_only);
                if state.managed && !metadata_only {
                    return Err(FsError::unsupported(
                        "managed reads require an HSM-aware SDK and a read grant",
                    ));
                }
                let mut status = FileStatus {
                    id: 42,
                    path: header.path,
                    is_complete: true,
                    ..Default::default()
                };
                status.storage_policy.save_ufs(1);
                Ok(Builder::success_with_header(
                    request,
                    GetBlockLocationsResponse {
                        blocks: ProtoUtils::file_blocks_to_pb(FileBlocks::new(status, vec![])),
                    },
                )
                .build())
            }
            code => Err(FsError::common(format!("unexpected test RPC {code:?}"))),
        }
    }
}

struct Service(Arc<Mutex<State>>);

impl HandlerService for Service {
    type Item = Handler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Handler {
        Handler(self.0.clone())
    }
}

#[test]
fn stale_mount_route_cannot_turn_managed_read_denial_into_ufs_fallback() -> FsResult<()> {
    let rt = runtime();
    let root = std::env::temp_dir().join(format!("curvine-stale-mount-{}", Utils::rand_str(12)));
    std::fs::create_dir_all(&root)?;
    // If the denial is swallowed, UnifiedFS can successfully open these old bytes.
    std::fs::write(root.join("new"), b"obsolete-ufs-generation")?;
    let mount = MountOptions::builder()
        .write_type(WriteType::FsMode)
        .build()
        .to_info(1, "/ufs", &format!("file://{}", root.display()));
    let state = Arc::new(Mutex::new(State {
        mount,
        mounted: true,
        managed: false,
        table_reads: 0,
        layouts: vec![],
    }));
    let server = RpcServer::with_rt(
        rt.clone(),
        ServerConf::with_hostname("127.0.0.1", NetUtils::hold_available_port()),
        Service(state.clone()),
    );
    let address = server.bind_addr().clone();
    let mut ready = server.new_state_listener();
    let accept = rt.spawn(async move { server.run().await });
    rt.block_on(ready.wait_startup())?;
    let mut conf = ClusterConf::default();
    conf.client.master_addrs = vec![address];
    conf.client.enable_unified_fs = true;
    conf.client.enable_rust_read_ufs = true;
    conf.client.mount_update_ttl_ms = 60_000;
    conf.client.master_conn_pool_size = 1;
    conf.client.init()?;
    let fs = UnifiedFileSystem::with_rt(conf, rt.clone())?;
    let result = rt.block_on(async {
        let mut reader = fs.open(&Path::from_str("/ufs/old")?).await?;
        assert_eq!(reader.path_type(), "fallback");
        reader.complete().await?;
        assert_eq!(
            state.lock().unwrap().layouts,
            [false],
            "normal cache open added layout RPCs or requested inspection"
        );
        {
            let mut state = state.lock().unwrap();
            // Another client unmounts /ufs, then creates a managed generation.
            state.mounted = false;
            state.managed = true;
        }
        let path = Path::from_str("/ufs/new")?;
        let denied = fs.open(&path).await;
        assert!(
            matches!(denied, Err(FsError::Unsupported(_))),
            "stale mount swallowed managed-read denial"
        );
        assert_eq!(
            state.lock().unwrap().table_reads,
            1,
            "test did not retain the cached mount route"
        );
        assert_eq!(fs.cv.get_block_locations(&path).await?.status.id, 42);
        assert_eq!(state.lock().unwrap().layouts, [false, false, true]);
        Ok(())
    });
    accept.abort();
    std::fs::remove_dir_all(root)?;
    result
}

#[test]
fn fallback_refuses_explicit_managed_reader_even_with_cached_bytes() -> FsResult<()> {
    let rt = runtime();
    let mut conf = ClusterConf::default();
    conf.client.master_addrs = vec![InetAddr::new("127.0.0.1", 0)];
    conf.client.init()?;
    let context = Arc::new(FsContext::with_rt(conf, rt.clone())?);
    let mut reader = FsReader::new_managed(
        Path::from_str("/managed")?,
        context,
        ReadHandle {
            blocks: FileBlocks::new(
                FileStatus {
                    len: 6,
                    ..Default::default()
                },
                vec![],
            ),
            data: DataKey {
                entity: EntityKey {
                    storage: "s".into(),
                    namespace: "n".into(),
                    entity: "42".into(),
                },
                generation: 1,
            },
            reader: "reader".into(),
            endpoint: None,
            granted_ms: 1,
            expires_ms: 2,
        },
        Instant::now() - Duration::from_secs(1),
    )?;
    *reader.chunk_mut() = DataSlice::bytes(bytes::Bytes::from_static(b"cached"));
    let path = Path::from_str("file:///unused-managed-fallback")?;
    let ufs = UfsFileSystem::new(&path, Default::default(), None)?;
    let mut fallback = FallbackFsReader::new(reader, path, ufs, true);
    rt.block_on(async {
        assert!(matches!(
            fallback.read_chunk(Some(1)).await,
            Err(FsError::Unsupported(_))
        ));
        assert_eq!(fallback.chunk_mut().len(), 6);
        assert!(matches!(
            fallback.read_chunk0().await,
            Err(FsError::Unsupported(_))
        ));
        // Dispose through normal complete; bound cleanup because this synthetic
        // handle has no server grant (the configured port cannot host a peer).
        let close = fallback.complete();
        tokio::pin!(close);
        let _ = tokio::time::timeout(Duration::from_millis(1), close).await;
    });
    Ok(())
}
