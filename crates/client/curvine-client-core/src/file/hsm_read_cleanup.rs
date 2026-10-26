// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Bounded, ordered advisory close delivery. A failed delivery makes the
//! manager fail closed: mutation flushes report the failure, while grants
//! remain protected by their original server-side expiry.
use super::{FsClient, FsContext};
use curvine_error::{FsError, FsResult};
use curvine_model::hsm::native::DataKey;
use curvine_proto::HsmControlRequest;
use curvine_runtime::runtime::RpcRuntime;
use serde::Serialize;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const QUEUE: usize = 4096;
// Each close contains namespace credentials; 128 rows fit the 64 KiB control envelope.
const BATCH: usize = 128;

#[derive(Serialize)]
pub(super) struct CloseKey {
    pub data: DataKey,
    pub reader: String,
}

#[derive(Serialize)]
struct CloseBatch<'a> {
    op: &'static str,
    read_mode: &'static str,
    closes: &'a [CloseKey],
}

enum Command {
    Close(CloseKey),
    Flush(oneshot::Sender<FsResult<()>>),
}

pub(super) struct ReadCleanup {
    sender: mpsc::Sender<Command>,
    pending: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<String>>>,
}

impl ReadCleanup {
    pub(super) fn new(context: &Arc<FsContext>) -> Self {
        let (sender, receiver) = mpsc::channel(QUEUE);
        let pending = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(Mutex::new(None));
        let weak = Arc::downgrade(context);
        context.clone_runtime().spawn(run(
            receiver,
            pending.clone(),
            failure.clone(),
            move |closes| {
                let weak = weak.clone();
                async move {
                    let context = weak
                        .upgrade()
                        .ok_or_else(|| FsError::common("read close context stopped"))?;
                    let request = HsmControlRequest {
                        protocol_version: 1,
                        payload: serde_json::to_vec(&CloseBatch {
                            op: "read_close_batch",
                            read_mode: "fast_v1",
                            closes: &closes,
                        })
                        .map_err(|e| FsError::common(e.to_string()))?,
                    };
                    let client = FsClient::new(context);
                    let mut error = String::new();
                    for attempt in 0..3 {
                        let result =
                            client
                                .hsm_control(request.clone())
                                .await
                                .and_then(|response| {
                                    let value: serde_json::Value =
                                        serde_json::from_slice(&response.payload)
                                            .map_err(|e| FsError::common(e.to_string()))?;
                                    if value["kind"] == "done" {
                                        Ok(())
                                    } else {
                                        Err(FsError::common(format!(
                                            "HSM read close batch: {value}"
                                        )))
                                    }
                                });
                        match result {
                            Ok(()) => return Ok(()),
                            Err(e) => error = e.to_string(),
                        }
                        if attempt < 2 {
                            tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
                        }
                    }
                    Err(FsError::common(error))
                }
            },
        ));
        Self {
            sender,
            pending,
            failure,
        }
    }

    fn error(&self) -> FsError {
        FsError::common(
            self.failure
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "HSM read close manager stopped".into()),
        )
    }

    pub(super) async fn enqueue(&self, key: CloseKey) -> FsResult<()> {
        let permit = self.sender.reserve().await.map_err(|_| self.error())?;
        self.pending.fetch_add(1, Ordering::Relaxed);
        permit.send(Command::Close(key));
        Ok(())
    }

    pub(super) fn try_enqueue(&self, key: CloseKey) {
        if let Ok(permit) = self.sender.try_reserve() {
            self.pending.fetch_add(1, Ordering::Relaxed);
            permit.send(Command::Close(key));
        }
    }

    pub(super) async fn flush(&self) -> FsResult<()> {
        if self.pending.load(Ordering::Relaxed) == 0 {
            let failed = self.failure.lock().unwrap().is_some();
            return if failed { Err(self.error()) } else { Ok(()) };
        }
        let (send, receive) = oneshot::channel();
        self.sender
            .send(Command::Flush(send))
            .await
            .map_err(|_| self.error())?;
        receive.await.map_err(|_| self.error())?
    }

    pub(super) fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}

async fn run<F, Fut>(
    mut receiver: mpsc::Receiver<Command>,
    pending: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<String>>>,
    mut deliver: F,
) where
    F: FnMut(Vec<CloseKey>) -> Fut,
    Fut: Future<Output = FsResult<()>>,
{
    let mut batch = Vec::with_capacity(BATCH);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let command = tokio::select! {
            command = receiver.recv() => command,
            _ = tick.tick(), if !batch.is_empty() => None,
        };
        let mut flush = None;
        match command {
            Some(Command::Close(key)) => {
                batch.push(key);
                if batch.len() < BATCH {
                    continue;
                }
            }
            Some(Command::Flush(send)) => flush = Some(send),
            None => {}
        }
        let count = batch.len();
        let result = if count == 0 {
            Ok(())
        } else {
            deliver(std::mem::replace(&mut batch, Vec::with_capacity(BATCH))).await
        };
        if let Err(error) = result {
            let message = error.to_string();
            *failure.lock().unwrap() = Some(message.clone());
            pending.fetch_sub(count, Ordering::Relaxed);
            receiver.close();
            if let Some(send) = flush {
                let _ = send.send(Err(error));
            }
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Close(_) => {
                        pending.fetch_sub(1, Ordering::Relaxed);
                    }
                    Command::Flush(send) => {
                        let _ = send.send(Err(FsError::common(message.clone())));
                    }
                }
            }
            return;
        }
        pending.fetch_sub(count, Ordering::Relaxed);
        if let Some(send) = flush {
            let _ = send.send(Ok(()));
        }
        if receiver.is_closed() && receiver.is_empty() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(reader: &str) -> CloseKey {
        CloseKey {
            data: serde_json::from_value(serde_json::json!({
                "entity":{"storage":"s","namespace":"n","entity":"1"},"generation":1,
            }))
            .unwrap(),
            reader: reader.into(),
        }
    }

    #[tokio::test]
    async fn flush_waits_for_preceding_close_acknowledgements() {
        let (sender, receiver) = mpsc::channel(8);
        let pending = Arc::new(AtomicUsize::new(2));
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let seen = delivered.clone();
        let (ack, wait_ack) = oneshot::channel();
        let mut wait_ack = Some(wait_ack);
        let task = tokio::spawn(run(
            receiver,
            pending.clone(),
            Arc::new(Mutex::new(None)),
            move |batch| {
                let seen = seen.clone();
                let wait = wait_ack.take();
                async move {
                    if let Some(wait) = wait {
                        wait.await.unwrap();
                    }
                    seen.lock()
                        .unwrap()
                        .extend(batch.into_iter().map(|key| key.reader));
                    Ok(())
                }
            },
        ));
        sender.send(Command::Close(key("first"))).await.unwrap();
        sender.send(Command::Close(key("second"))).await.unwrap();
        let (send, mut flushed) = oneshot::channel();
        sender.send(Command::Flush(send)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(
            flushed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        ack.send(()).unwrap();
        flushed.await.unwrap().unwrap();
        assert_eq!(*delivered.lock().unwrap(), ["first", "second"]);
        assert_eq!(pending.load(Ordering::Relaxed), 0);
        drop(sender);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn failed_delivery_rejects_flush_and_further_enqueues() {
        let (sender, receiver) = mpsc::channel(8);
        let failure = Arc::new(Mutex::new(None));
        let pending = Arc::new(AtomicUsize::new(1));
        let task = tokio::spawn(run(receiver, pending.clone(), failure.clone(), |_| async {
            Err(FsError::common("injected close failure"))
        }));
        sender.send(Command::Close(key("first"))).await.unwrap();
        let (send, flushed) = oneshot::channel();
        sender.send(Command::Flush(send)).await.unwrap();
        assert!(flushed.await.unwrap().is_err());
        task.await.unwrap();
        assert!(sender.is_closed());
        assert!(failure
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains("injected"));
        assert_eq!(pending.load(Ordering::Relaxed), 0);
    }
}
