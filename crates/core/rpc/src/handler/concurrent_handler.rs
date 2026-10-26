// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.

use super::message_handler::dispatch;
use super::{Frame, LimitConf, MessageHandler, ReadFrame, WriteFrame};
use crate::message::Message;
use crate::ServerConf;
use curvine_io::IOResult;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

// Keep admission permits until the entire reply is sent, bounding completed
// responses as well as executing handlers when a client stops reading.
struct Response {
    message: IOResult<Message>,
    _connection: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

// Poll handlers within the connection instead of adding a runtime task for
// every small RPC. If admission stops, hand off accepted work before dropping
// the connection future so durable mutations can still finish.
struct Accepted {
    rt: Arc<Runtime>,
    pending: FuturesUnordered<BoxFuture<'static, ()>>,
}

impl Drop for Accepted {
    fn drop(&mut self) {
        let mut pending = std::mem::take(&mut self.pending);
        if !pending.is_empty() {
            self.rt
                .spawn(async move { while pending.next().await.is_some() {} });
        }
    }
}

/// Multiplex independent RPCs on one connection. Stateful data streams must
/// continue using StreamHandler, selected by HandlerService::is_stream().
pub(crate) struct ConcurrentHandler<M> {
    rt: Arc<Runtime>,
    reader: ReadFrame,
    writer: WriteFrame,
    handler: Arc<M>,
    limit: LimitConf,
    liveness_limit: Arc<Semaphore>,
    close_idle: bool,
    timeout: Duration,
}

impl<M: MessageHandler> ConcurrentHandler<M> {
    pub(crate) fn new<F: Frame>(
        rt: Arc<Runtime>,
        frame: F,
        handler: M,
        limit: LimitConf,
        conf: &ServerConf,
        liveness_limit: Arc<Semaphore>,
    ) -> Self {
        let (reader, writer) = frame.split();
        Self {
            rt,
            reader,
            writer,
            handler: Arc::new(handler),
            limit,
            liveness_limit,
            close_idle: conf.close_idle,
            timeout: Duration::from_millis(conf.timeout_ms),
        }
    }

    pub(crate) async fn run(self) -> IOResult<()> {
        let connection = Arc::new(Semaphore::new(self.limit.conn_limit));
        let (responses, receiver) = mpsc::channel(self.limit.conn_limit);
        let receive = Self::receive(
            self.reader,
            self.rt,
            self.handler,
            self.limit,
            self.liveness_limit,
            connection,
            responses,
            self.close_idle,
            self.timeout,
        );
        let send = Self::send(self.writer, receiver, self.timeout);
        tokio::pin!(receive, send);
        tokio::select! {
            // EOF stops admission, but drain replies for already accepted
            // requests (including a client that only half-closes its socket).
            result = &mut receive => result.and(send.await),
            // A failed writer stops admission. Accepted hands off remaining
            // handlers: closing a socket cannot cancel a durable mutation.
            result = &mut send => result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn receive(
        mut reader: ReadFrame,
        rt: Arc<Runtime>,
        handler: Arc<M>,
        limit: LimitConf,
        liveness_limit: Arc<Semaphore>,
        connection: Arc<Semaphore>,
        responses: mpsc::Sender<Response>,
        close_idle: bool,
        duration: Duration,
    ) -> IOResult<()> {
        let mut accepted = Accepted {
            rt: rt.clone(),
            pending: FuturesUnordered::new(),
        };
        loop {
            let admission = async {
                let request = reader.receive();
                tokio::pin!(request);
                let request = loop {
                    match timeout(duration, &mut request).await {
                        Ok(result) => break result?,
                        Err(_)
                            if close_idle && connection.available_permits() == limit.conn_limit =>
                        {
                            return Ok(None);
                        }
                        // Preserve a partially decoded frame while waiting
                        // for accepted handlers to finish.
                        Err(_) => continue,
                    }
                };
                if request.is_empty() {
                    return Ok(None);
                }
                let local = connection
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|error| error.to_string())?;
                let admission_limit = if handler.is_liveness(&request) {
                    &liveness_limit
                } else {
                    &limit.global_limit
                };
                let global = admission_limit
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok::<_, curvine_io::IOError>(Some((request, local, global)))
            };
            tokio::pin!(admission);
            let admitted = loop {
                tokio::select! {
                    request = &mut admission => break request?,
                    Some(()) = accepted.pending.next(), if !accepted.pending.is_empty() => {},
                }
            };
            let Some((request, local, global)) = admitted else {
                return Ok(());
            };
            let handler = handler.clone();
            let runtime = rt.clone();
            let responses = responses.clone();
            accepted.pending.push(Box::pin(async move {
                let message = match AssertUnwindSafe(dispatch(&runtime, handler, request))
                    .catch_unwind()
                    .await
                {
                    Ok(result) => result,
                    Err(_) => err_box!("RPC handler panicked"),
                };
                let _ = responses
                    .send(Response {
                        message,
                        _connection: local,
                        _global: global,
                    })
                    .await;
            }));
        }
    }

    async fn send(
        mut writer: WriteFrame,
        mut responses: mpsc::Receiver<Response>,
        duration: Duration,
    ) -> IOResult<()> {
        while let Some(response) = responses.recv().await {
            let message = response.message?;
            if message.not_empty() {
                timeout(duration, writer.send(&message)).await??;
            }
            // Both permits drop after send, including empty replies/errors.
        }
        Ok(())
    }
}
