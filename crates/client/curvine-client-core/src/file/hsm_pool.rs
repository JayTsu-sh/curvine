// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Exclusive raw-connection leases. A canceled or failed RPC never returns a
//! socket with an unread frame to another transfer. No background runtime or
//! per-target threads are needed; idle connections are globally bounded.
use curvine_model::hsm::native::{HsmError, HsmResult};
use curvine_net::net::InetAddr;
use curvine_rpc::client::{ClientConf, RpcClient};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAX_IDLE: usize = 128;
const MAX_IDLE_PER_TARGET: usize = 4;
const IDLE_TTL: Duration = Duration::from_secs(30);

struct Idle {
    conf: ClientConf,
    addr: InetAddr,
    client: RpcClient,
    since: Instant,
}

fn pool() -> &'static Mutex<Vec<Idle>> {
    static POOL: OnceLock<Mutex<Vec<Idle>>> = OnceLock::new();
    POOL.get_or_init(Default::default)
}

pub(super) struct WorkerConnection {
    conf: ClientConf,
    addr: InetAddr,
    client: Option<RpcClient>,
    reusable: bool,
}

impl WorkerConnection {
    pub(super) async fn acquire(conf: &ClientConf, addr: InetAddr) -> HsmResult<Self> {
        let client = {
            let mut pool = pool().lock().unwrap();
            pool.retain(|idle| idle.since.elapsed() < IDLE_TTL && idle.client.is_active());
            pool.iter()
                .position(|idle| idle.conf == *conf && idle.addr == addr)
                .map(|index| pool.swap_remove(index).client)
        };
        let client = match client {
            Some(client) => client,
            None => RpcClient::with_raw(&addr, conf)
                .await
                .map_err(|error| HsmError::Unknown(error.to_string()))?,
        };
        Ok(Self {
            conf: conf.clone(),
            addr,
            client: Some(client),
            reusable: false,
        })
    }

    pub(super) fn client(&self) -> &RpcClient {
        self.client.as_ref().unwrap()
    }

    /// Only called after the complete response frame has been validated.
    pub(super) fn reusable(&mut self) {
        self.reusable = true;
    }
}

impl Drop for WorkerConnection {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        if !self.reusable || !client.is_active() {
            client.set_closed();
            return;
        }
        let mut pool = pool().lock().unwrap();
        pool.retain(|idle| idle.since.elapsed() < IDLE_TTL && idle.client.is_active());
        if pool.len() < MAX_IDLE
            && pool
                .iter()
                .filter(|idle| idle.conf == self.conf && idle.addr == self.addr)
                .count()
                < MAX_IDLE_PER_TARGET
        {
            pool.push(Idle {
                conf: self.conf.clone(),
                addr: self.addr.clone(),
                client,
                since: Instant::now(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn complete_lease_is_reused_but_unfinished_lease_is_discarded() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: InetAddr = listener.local_addr().unwrap().into();
        let conf = ClientConf::default();
        let (lease, peer) = tokio::join!(
            WorkerConnection::acquire(&conf, addr.clone()),
            listener.accept()
        );
        let _peer = peer.unwrap();
        let mut lease = lease.unwrap();
        let first = lease.client().local_addr().clone();
        lease.reusable();
        drop(lease);
        let lease = WorkerConnection::acquire(&conf, addr.clone())
            .await
            .unwrap();
        assert_eq!(lease.client().local_addr(), &first);
        // A canceled operation drops an unfinished lease: no unread response
        // can leak into the next transfer on this target.
        drop(lease);
        let (lease, peer) = tokio::join!(
            WorkerConnection::acquire(&conf, addr.clone()),
            listener.accept()
        );
        let _peer2 = peer.unwrap();
        assert_ne!(lease.unwrap().client().local_addr(), &first);
    }

    #[tokio::test]
    async fn idle_pool_is_bounded_per_endpoint_and_configuration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: InetAddr = listener.local_addr().unwrap().into();
        let conf = ClientConf::default();
        let mut leases = vec![];
        let mut peers = vec![];
        for _ in 0..8 {
            let (lease, peer) = tokio::join!(
                WorkerConnection::acquire(&conf, addr.clone()),
                listener.accept()
            );
            peers.push(peer.unwrap());
            let mut lease = lease.unwrap();
            lease.reusable();
            leases.push(lease);
        }
        drop(leases);
        assert_eq!(
            pool()
                .lock()
                .unwrap()
                .iter()
                .filter(|idle| idle.addr == addr && idle.conf == conf)
                .count(),
            MAX_IDLE_PER_TARGET
        );
        let mut changed = conf.clone();
        changed.rpc_timeout_ms += 1;
        let (lease, peer) =
            tokio::join!(WorkerConnection::acquire(&changed, addr), listener.accept());
        assert!(lease.is_ok());
        assert!(peer.is_ok());
    }
}
