// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.

use curvine_core_error::{CommonErrorExt, CommonResult, CommonResultExt};
use curvine_io::DataSlice;
use curvine_net::net::{ConnState, InetAddr, NetUtils};
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::handler::{Frame, HandlerService, LimitConf, MessageHandler, RpcFrame};
use curvine_rpc::message::{Builder, Message};
use curvine_rpc::server::{RpcServer, ServerConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const HELD: i8 = 1;
const SYNC: i8 = 2;
const ASYNC_ERROR: i8 = 3;
const SYNC_ERROR: i8 = 4;
const PANIC: i8 = 5;
const LARGE: i8 = 6;
const LIVENESS: i8 = 7;
const HELD_LIVENESS: i8 = 8;
const INVENTORY: i8 = 9;
const HELD_INVENTORY: i8 = 10;
const REPLICATION_RESULT: i8 = 11;
const HELD_REPLICATION_RESULT: i8 = 12;
const DEADLINE: Duration = Duration::from_secs(5);

struct Probe {
    entered: Semaphore,
    resume: Semaphore,
    completed: Semaphore,
    active: AtomicUsize,
    maximum: AtomicUsize,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            resume: Semaphore::new(0),
            completed: Semaphore::new(0),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        })
    }

    async fn wait_entered(&self, count: u32) {
        timeout(DEADLINE, self.entered.acquire_many(count))
            .await
            .expect("requests must reach the handler")
            .unwrap()
            .forget();
    }
}

struct Handler(Arc<Probe>);

impl MessageHandler for Handler {
    type Error = CommonErrorExt;

    fn is_liveness(&self, msg: &Message) -> bool {
        matches!(
            msg.code(),
            LIVENESS
                | HELD_LIVENESS
                | INVENTORY
                | HELD_INVENTORY
                | REPLICATION_RESULT
                | HELD_REPLICATION_RESULT
        )
    }

    fn is_sync(&self, msg: &Message) -> bool {
        matches!(
            msg.code(),
            SYNC | SYNC_ERROR | LARGE | LIVENESS | INVENTORY | REPLICATION_RESULT
        )
    }

    fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
        assert!(
            msg.time.is_some(),
            "concurrent decode must stamp receipt time"
        );
        if msg.code() == SYNC_ERROR {
            curvine_core_error::err_box!("sync probe failure")
        } else if msg.code() == LARGE {
            self.0.entered.add_permits(1);
            Ok(msg.success_with_data(
                None,
                DataSlice::Buffer(bytes::BytesMut::from(&vec![7; 8 * 1024 * 1024][..])),
            ))
        } else {
            Ok(msg.success_with_data(msg.header.clone(), DataSlice::from_str("sync")))
        }
    }

    async fn async_handle(&self, msg: Message) -> CommonResultExt<Message> {
        assert!(msg.time.is_some());
        match msg.code() {
            HELD | HELD_LIVENESS | HELD_INVENTORY | HELD_REPLICATION_RESULT => {
                let active = self.0.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.0.maximum.fetch_max(active, Ordering::SeqCst);
                self.0.entered.add_permits(1);
                self.0.resume.acquire().await.unwrap().forget();
                self.0.active.fetch_sub(1, Ordering::SeqCst);
                self.0.completed.add_permits(1);
                Ok(msg.success_with_data(msg.header.clone(), DataSlice::from_str("async")))
            }
            PANIC => panic!("probe panic"),
            _ => curvine_core_error::err_box!("async probe failure"),
        }
    }
}

struct Service {
    probe: Arc<Probe>,
    stream: bool,
    limit: LimitConf,
}

impl HandlerService for Service {
    type Item = Handler;

    fn has_conn_state(&self) -> bool {
        true
    }

    fn get_message_handler(&self, state: Option<ConnState>) -> Self::Item {
        assert!(
            state.is_some(),
            "dispatch must retain the peer connection state"
        );
        Handler(self.probe.clone())
    }

    fn is_stream(&self) -> bool {
        self.stream
    }

    fn get_limit(&self) -> LimitConf {
        self.limit.clone()
    }
}

struct Server {
    rt: Arc<Runtime>,
    address: InetAddr,
    accept: JoinHandle<CommonResult<()>>,
    probe: Arc<Probe>,
    limit: LimitConf,
}

impl Server {
    fn start(stream: bool, connection: usize, global: usize) -> Self {
        Self::start_with_timeout(stream, connection, global, 120_000)
    }

    fn start_with_timeout(stream: bool, connection: usize, global: usize, timeout_ms: u64) -> Self {
        static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();
        let rt = RUNTIME
            .get_or_init(|| Arc::new(Runtime::new("rpc-dispatch-test", 2, 4)))
            .clone();
        let probe = Probe::new();
        let limit = LimitConf::new(connection, global);
        let service = Service {
            probe: probe.clone(),
            stream,
            limit: limit.clone(),
        };
        let mut conf = ServerConf::with_hostname("127.0.0.1", NetUtils::hold_available_port());
        conf.timeout_ms = timeout_ms;
        let server = RpcServer::with_rt(rt.clone(), conf, service);
        let address = server.bind_addr().clone();
        let mut ready = server.new_state_listener();
        let accept = rt.spawn(async move { server.run().await });
        rt.block_on(ready.wait_startup()).unwrap();
        Self {
            rt,
            address,
            accept,
            probe,
            limit,
        }
    }

    async fn client(&self) -> RpcClient {
        RpcClient::new(true, self.rt.clone(), &self.address, &ClientConf::default())
            .await
            .unwrap()
    }

    fn held(&self, client: &RpcClient) -> JoinHandle<Message> {
        self.held_code(client, HELD)
    }

    fn held_code(&self, client: &RpcClient, code: i8) -> JoinHandle<Message> {
        let client = client.clone();
        self.rt.spawn(async move {
            client
                .timeout_rpc(DEADLINE, Builder::new_rpc(code).build())
                .await
                .unwrap()
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Also unblock accepted requests if an assertion unwinds the test.
        self.probe.resume.add_permits(64);
        self.accept.abort();
    }
}

#[test]
fn one_connection_dispatches_sync_and_async_calls_and_routes_out_of_order_replies() {
    let server = Server::start(false, 2, 8);
    server.rt.block_on(async {
        let client = server.client().await;
        let held = server.held(&client);
        server.probe.wait_entered(1).await;
        let request = Builder::new_rpc(SYNC).build();
        let id = request.req_id();
        let response = client.timeout_rpc(DEADLINE, request).await.unwrap();
        assert_eq!(response.req_id(), id);
        assert_eq!(response.data_string(), "sync");
        assert!(
            !held.is_finished(),
            "the slow request must still be pending"
        );
        server.probe.resume.add_permits(1);
        assert_eq!(
            timeout(DEADLINE, held)
                .await
                .unwrap()
                .unwrap()
                .data_string(),
            "async"
        );

        for code in [ASYNC_ERROR, SYNC_ERROR] {
            let response = client
                .timeout_rpc(DEADLINE, Builder::new_rpc(code).build())
                .await
                .unwrap();
            assert!(!response.is_success());
            assert!(response.to_error_msg().contains("probe failure"));
        }
    });
}

#[test]
fn stream_service_keeps_requests_in_connection_order() {
    let server = Server::start(true, 2, 8);
    server.rt.block_on(async {
        let client = server.client().await;
        let first = server.held(&client);
        server.probe.wait_entered(1).await;
        let second = server.held(&client);
        assert!(
            timeout(Duration::from_millis(50), server.probe.entered.acquire())
                .await
                .is_err()
        );
        server.probe.resume.add_permits(2);
        timeout(DEADLINE, first).await.unwrap().unwrap();
        timeout(DEADLINE, second).await.unwrap().unwrap();
        assert_eq!(server.probe.maximum.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn connection_limit_bounds_in_flight_calls() {
    let server = Server::start(false, 2, 8);
    server.rt.block_on(async {
        let client = server.client().await;
        let calls = (0..3).map(|_| server.held(&client)).collect::<Vec<_>>();
        server.probe.wait_entered(2).await;
        assert!(
            timeout(Duration::from_millis(50), server.probe.entered.acquire())
                .await
                .is_err()
        );
        server.probe.resume.add_permits(3);
        for call in calls {
            timeout(DEADLINE, call).await.unwrap().unwrap();
        }
        assert_eq!(server.probe.maximum.load(Ordering::SeqCst), 2);
    });
}

#[test]
fn global_limit_is_shared_across_connections() {
    let server = Server::start(false, 2, 3);
    server.rt.block_on(async {
        let a = server.client().await;
        let b = server.client().await;
        let calls = [
            server.held(&a),
            server.held(&a),
            server.held(&b),
            server.held(&b),
        ];
        server.probe.wait_entered(3).await;
        assert!(
            timeout(Duration::from_millis(50), server.probe.entered.acquire())
                .await
                .is_err()
        );
        server.probe.resume.add_permits(4);
        for call in calls {
            timeout(DEADLINE, call).await.unwrap().unwrap();
        }
        assert_eq!(server.probe.maximum.load(Ordering::SeqCst), 3);
    });
}

#[test]
fn accepted_request_survives_disconnect_and_releases_admission() {
    let server = Server::start(false, 1, 1);
    server.rt.block_on(async {
        let stream = TcpStream::connect(server.address.as_pair()).await.unwrap();
        let mut frame = RpcFrame::with_client(stream, 4096);
        frame.send(Builder::new_rpc(HELD).build()).await.unwrap();
        server.probe.wait_entered(1).await;
        drop(frame);
        server.probe.resume.add_permits(1);
        timeout(DEADLINE, server.probe.completed.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        timeout(DEADLINE, async {
            while server.limit.global_limit.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(server.probe.active.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn slow_reader_keeps_response_memory_under_the_admission_limit() {
    let server = Server::start(false, 1, 1);
    server.rt.block_on(async {
        let stream = TcpStream::connect(server.address.as_pair()).await.unwrap();
        socket2::SockRef::from(&stream)
            .set_recv_buffer_size(4096)
            .unwrap();
        let mut frame = RpcFrame::with_client(stream, 4096);
        frame.send(Builder::new_rpc(LARGE).build()).await.unwrap();
        frame.send(Builder::new_rpc(LARGE).build()).await.unwrap();
        server.probe.wait_entered(1).await;
        assert!(
            timeout(Duration::from_millis(50), server.probe.entered.acquire())
                .await
                .is_err()
        );
        assert_eq!(
            server.limit.global_limit.available_permits(),
            0,
            "a completed reply must retain its permit until it is sent"
        );
        drop(frame);
        timeout(DEADLINE, async {
            while server.limit.global_limit.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn liveness_admission_survives_a_slow_ordinary_reader_and_remains_bounded() {
    let server = Server::start(false, 1, 1);
    server.rt.block_on(async {
        let stream = TcpStream::connect(server.address.as_pair()).await.unwrap();
        socket2::SockRef::from(&stream)
            .set_recv_buffer_size(4096)
            .unwrap();
        let mut frame = RpcFrame::with_client(stream, 4096);
        frame.send(Builder::new_rpc(LARGE).build()).await.unwrap();
        server.probe.wait_entered(1).await;
        assert_eq!(server.limit.global_limit.available_permits(), 0);

        // A worker's separate connection carries heartbeats, inventory and
        // replication results. Every step must progress before the following
        // heartbeat even though an ordinary reply cannot finish sending.
        let client = server.client().await;
        for code in [LIVENESS, INVENTORY, REPLICATION_RESULT, LIVENESS] {
            let response = client
                .timeout_rpc(DEADLINE, Builder::new_rpc(code).build())
                .await
                .unwrap();
            assert_eq!(response.data_string(), "sync");
            assert_eq!(server.limit.global_limit.available_permits(), 0);
        }

        // The independent reserve has eight slots across all connections;
        // it must not become an unbounded bypass around global admission.
        let mut calls = Vec::new();
        for index in 0..9 {
            let client = server.client().await;
            let code = match index % 3 {
                0 => HELD_LIVENESS,
                1 => HELD_INVENTORY,
                _ => HELD_REPLICATION_RESULT,
            };
            calls.push(server.held_code(&client, code));
        }
        server.probe.wait_entered(8).await;
        assert!(
            timeout(Duration::from_millis(50), server.probe.entered.acquire())
                .await
                .is_err()
        );
        assert_eq!(server.probe.maximum.load(Ordering::SeqCst), 8);
        server.probe.resume.add_permits(9);
        for call in calls {
            timeout(DEADLINE, call).await.unwrap().unwrap();
        }
        assert_eq!(server.limit.global_limit.available_permits(), 0);
        drop(frame);
        timeout(DEADLINE, async {
            while server.limit.global_limit.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn half_closed_connection_drains_accepted_replies() {
    let server = Server::start(false, 1, 1);
    server.rt.block_on(async {
        let stream = TcpStream::connect(server.address.as_pair()).await.unwrap();
        let mut frame = RpcFrame::with_client(stream, 4096);
        let request = Builder::new_rpc(HELD).build();
        let id = request.req_id();
        frame.send(request).await.unwrap();
        server.probe.wait_entered(1).await;
        socket2::SockRef::from(frame.io())
            .shutdown(std::net::Shutdown::Write)
            .unwrap();
        server.probe.resume.add_permits(1);
        let response = timeout(DEADLINE, frame.receive()).await.unwrap().unwrap();
        assert_eq!(response.req_id(), id);
        assert_eq!(response.data_string(), "async");
    });
}

#[test]
fn idle_timeout_does_not_close_a_connection_with_an_active_handler() {
    let server = Server::start_with_timeout(false, 1, 1, 100);
    server.rt.block_on(async {
        let client = server.client().await;
        let held = server.held(&client);
        server.probe.wait_entered(1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        server.probe.resume.add_permits(1);
        assert!(timeout(DEADLINE, held).await.unwrap().unwrap().is_success());
    });
}

#[test]
fn panicking_handler_closes_the_connection_instead_of_leaving_a_waiter() {
    let server = Server::start(false, 1, 1);
    server.rt.block_on(async {
        let client = server.client().await;
        let response = timeout(DEADLINE, client.rpc(Builder::new_rpc(PANIC).build()))
            .await
            .unwrap();
        assert!(response.is_err());
        assert_eq!(server.limit.global_limit.available_permits(), 1);
    });
}
