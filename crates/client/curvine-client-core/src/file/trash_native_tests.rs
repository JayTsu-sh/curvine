// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.

use super::*;
use crate::file::CurvineFileSystem;
use curvine_model::hsm::native::{DataKey, EntityKey};
use curvine_net::net::{ConnState, NetUtils};
use curvine_rpc::handler::{HandlerService, LimitConf, MessageHandler};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::Runtime;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct State {
    close_acknowledged: bool,
    requests: Vec<NativeMutationRequest>,
    attempts: HashMap<(String, u64), usize>,
    lists: usize,
}

struct Probe {
    state: Mutex<State>,
    close_entered: Semaphore,
    close_resume: Semaphore,
}

struct Handler(Arc<Probe>);

impl MessageHandler for Handler {
    type Error = FsError;

    fn is_sync(&self, request: &Message) -> bool {
        RpcCode::from(request.code()) != RpcCode::HsmControl
    }

    fn handle(&self, request: &Message) -> FsResult<Message> {
        match RpcCode::from(request.code()) {
            RpcCode::GetFilesystemInfo => Ok(Builder::success_with_header(
                request,
                GetFilesystemInfoResponse::default(),
            )
            .build()),
            RpcCode::ListTrash => {
                self.0.state.lock().unwrap().lists += 1;
                Ok(Builder::success_with_header(request, ListTrashResponse::default()).build())
            }
            RpcCode::NativeMutation => {
                use curvine_proto::NativeMutationStatusProto as Status;
                let input: NativeMutationRequest = request.parse_header()?;
                let mut state = self.0.state.lock().unwrap();
                assert!(
                    state.close_acknowledged,
                    "mutation crossed an unacknowledged close"
                );
                state.requests.push(input.clone());
                let mut reply = NativeMutationResponse {
                    version: input.version,
                    incarnation: "namespace".into(),
                    session: input.session.clone(),
                    sequence: input.sequence,
                    code: input.code,
                    status: Status::NeedsNamespace as i32,
                    success: true,
                    header: vec![],
                    body: vec![],
                };
                if !input.incarnation.is_empty() {
                    let attempt = state
                        .attempts
                        .entry((input.session.clone(), input.sequence))
                        .or_default();
                    *attempt += 1;
                    reply.status = if *attempt == 1 {
                        Status::Uncertain
                    } else {
                        Status::Committed
                    } as i32;
                    if *attempt > 1 {
                        reply.header = match RpcCode::from(input.code as i8) {
                            RpcCode::RestoreTrash => {
                                let header = RestoreTrashRequest::decode(input.header.as_slice())
                                    .map_err(|error| FsError::common(error.to_string()))?;
                                assert_eq!(header.path, "/.Trash/entry");
                                assert_eq!(header.dst.as_deref(), Some("/restored"));
                                assert!(header.overwrite());
                                RestoreTrashResponse {
                                    dst: header.dst,
                                    status: None,
                                }
                                .encode_to_vec()
                            }
                            RpcCode::EmptyTrash => EmptyTrashResponse {
                                res: FreeResultProto {
                                    inodes: 7,
                                    bytes: 9,
                                },
                            }
                            .encode_to_vec(),
                            RpcCode::Delete => {
                                let header = DeleteRequest::decode(input.header.as_slice())
                                    .map_err(|error| FsError::common(error.to_string()))?;
                                DeleteResponse {
                                    res: Some(FreeResultProto::default()),
                                    trashed: Some(!header.skip_trash()),
                                }
                                .encode_to_vec()
                            }
                            code => {
                                return Err(FsError::common(format!(
                                    "unexpected native RPC {code:?}"
                                )))
                            }
                        };
                    }
                }
                Ok(Builder::success_with_header(request, reply).build())
            }
            code => Err(FsError::common(format!(
                "mutating RPC escaped NativeMutation: {code:?}"
            ))),
        }
    }

    async fn async_handle(&self, request: Message) -> FsResult<Message> {
        let header: HsmControlRequest = request.parse_header()?;
        let close: serde_json::Value = serde_json::from_slice(&header.payload)
            .map_err(|error| FsError::common(error.to_string()))?;
        assert_eq!(close["op"], "read_close_batch");
        self.0.close_entered.add_permits(1);
        self.0.close_resume.acquire().await.unwrap().forget();
        self.0.state.lock().unwrap().close_acknowledged = true;
        Ok(Builder::success_with_header(
            &request,
            HsmControlResponse {
                protocol_version: 1,
                payload: br#"{"kind":"done"}"#.to_vec(),
            },
        )
        .build())
    }
}

struct Service(Arc<Probe>, LimitConf);

impl HandlerService for Service {
    type Item = Handler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Handler {
        Handler(self.0.clone())
    }

    fn is_stream(&self) -> bool {
        false
    }

    fn get_limit(&self) -> LimitConf {
        self.1.clone()
    }
}

#[test]
fn trash_mutations_drain_closes_and_preserve_identity_and_delete_options() -> FsResult<()> {
    static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();
    let rt = RUNTIME
        .get_or_init(|| Arc::new(Runtime::new("trash-native-test", 2, 4)))
        .clone();
    let probe = Arc::new(Probe {
        state: Mutex::new(State::default()),
        close_entered: Semaphore::new(0),
        close_resume: Semaphore::new(0),
    });
    let server = RpcServer::with_rt(
        rt.clone(),
        ServerConf::with_hostname("127.0.0.1", NetUtils::hold_available_port()),
        Service(probe.clone(), LimitConf::new(8, 8)),
    );
    let address = server.bind_addr().clone();
    let mut ready = server.new_state_listener();
    let accept = rt.spawn(async move { server.run().await });
    rt.block_on(ready.wait_startup())?;
    let mut conf = ClusterConf::default();
    conf.client.master_addrs = vec![address];
    conf.client.master_conn_pool_size = 1;
    conf.client.init()?;
    let fs = CurvineFileSystem::with_rt(conf, rt.clone())?;
    let result = rt.block_on(async {
        for (code, skip_trash) in [
            (RpcCode::RestoreTrash, false),
            (RpcCode::EmptyTrash, false),
            (RpcCode::Delete, false),
            (RpcCode::Delete, true),
        ] {
            let before = {
                let mut state = probe.state.lock().unwrap();
                state.close_acknowledged = false;
                state.requests.len()
            };
            fs.fs_context()
                .enqueue_hsm_read_close(
                    DataKey {
                        entity: EntityKey {
                            storage: "s".into(),
                            namespace: "n".into(),
                            entity: "42".into(),
                        },
                        generation: 1,
                    },
                    "reader".into(),
                )
                .await?;
            let fs = fs.clone();
            let mut operation = rt.spawn(async move {
                match code {
                    RpcCode::RestoreTrash => assert_eq!(
                        fs.restore_trash(
                            &Path::from_str("/.Trash/entry")?,
                            Some(&Path::from_str("/restored")?),
                            true
                        )
                        .await?
                        .dst
                        .as_deref(),
                        Some("/restored")
                    ),
                    RpcCode::EmptyTrash => {
                        let result = fs.empty_trash().await?;
                        assert_eq!((result.inodes, result.bytes), (7, 9));
                    }
                    RpcCode::Delete => assert_eq!(
                        fs.delete_with_opts(&Path::from_str("/delete")?, false, skip_trash)
                            .await?
                            .trashed,
                        !skip_trash
                    ),
                    _ => unreachable!(),
                }
                Ok::<_, FsError>(())
            });
            timeout(DEADLINE, probe.close_entered.acquire())
                .await?
                .unwrap()
                .forget();
            assert!(
                timeout(Duration::from_millis(50), &mut operation)
                    .await
                    .is_err(),
                "mutation completed before close acknowledgement"
            );
            assert_eq!(probe.state.lock().unwrap().requests.len(), before);
            probe.close_resume.add_permits(1);
            timeout(DEADLINE, operation).await???;
            let state = probe.state.lock().unwrap();
            let attempts = &state.requests[before..];
            assert!(attempts.len() >= 2);
            for attempt in attempts {
                assert_eq!(attempt.code, code as u32);
                assert_eq!(attempt.session, attempts[0].session);
                assert_eq!(attempt.sequence, attempts[0].sequence);
                assert_eq!(attempt.acknowledged, attempts[0].acknowledged);
                assert_eq!(attempt.header, attempts[0].header);
            }
        }
        assert!(fs.list_trash().await?.is_empty());
        assert_eq!(probe.state.lock().unwrap().lists, 1);
        Ok(())
    });
    probe.close_resume.add_permits(8);
    accept.abort();
    result
}
