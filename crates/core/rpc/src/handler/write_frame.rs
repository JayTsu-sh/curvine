// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

use crate::handler::FrameBuf;
use crate::message::Message;
use curvine_io::IOResult;
use std::io::{self, IoSlice};

pub struct WriteFrame {
    io: WriteHalf<TcpStream>,
    buf: FrameBuf,
}

impl WriteFrame {
    pub(crate) fn new(io: WriteHalf<TcpStream>, buf: FrameBuf) -> Self {
        Self { io, buf }
    }

    pub async fn send(&mut self, msg: &Message) -> IOResult<()> {
        msg.encode_protocol(&mut self.buf);
        let protocol = self.buf.split();
        let mut parts = [
            IoSlice::new(&protocol),
            IoSlice::new(msg.header.as_deref().unwrap_or_default()),
            IoSlice::new(msg.data.as_slice()),
        ];
        let mut remaining = &mut parts[..];
        // Metadata frames are small: send their protocol, header and body in
        // one writev where possible instead of separate TCP_NODELAY writes.
        // Advance across part boundaries after short writes on busy sockets.
        while !remaining.is_empty() {
            let written = self.io.write_vectored(remaining).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "RPC frame write returned zero bytes",
                )
                .into());
            }
            IoSlice::advance_slices(&mut remaining, written);
        }
        self.io.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{Frame, RpcFrame};
    use crate::message::Builder;
    use bytes::BytesMut;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    #[tokio::test]
    async fn vectored_frame_preserves_all_parts_across_short_writes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        stream.set_nodelay(true).unwrap();
        socket2::SockRef::from(&stream)
            .set_send_buffer_size(4096)
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let header = BytesMut::from(&vec![0xa5; 64 * 1024][..]);
        let body = BytesMut::from(&vec![0x5a; 1024 * 1024][..]);
        let request = Builder::new_rpc(7i8)
            .header(header.clone())
            .data(curvine_io::DataSlice::Buffer(body.clone()))
            .build();
        let id = request.req_id();
        let (_reader, mut writer) = RpcFrame::with_client(stream, 4096).split();
        let receive = async move {
            let mut frame = RpcFrame::with_client(peer, 4096);
            frame.receive().await.unwrap()
        };
        let (sent, received) = timeout(Duration::from_secs(5), async {
            tokio::join!(writer.send(&request), receive)
        })
        .await
        .unwrap();
        sent.unwrap();
        assert_eq!(received.req_id(), id);
        assert_eq!(received.header.as_ref().unwrap(), &header);
        assert_eq!(received.data.as_slice(), &body[..]);
    }
}
