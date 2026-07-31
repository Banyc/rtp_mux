use crate::{SocketAddrPair, migrating_write_half::MigratingWriteHalf};
use mux::{MigratingStreamWriter, SplicedReader, StreamReader};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
enum ReaderState {
    #[cfg_attr(not(test), allow(dead_code))]
    Pending {
        rx: tokio::sync::oneshot::Receiver<StreamReader>,
    },
    PendingSpliced {
        rx: tokio::sync::oneshot::Receiver<SplicedReader>,
    },
    Ready {
        reader: StreamReader,
    },
    ReadySpliced {
        reader: SplicedReader,
    },
    Failed,
}
pub struct ClientStream {
    write: MigratingWriteHalf,
    reader_state: ReaderState,
    addr: SocketAddrPair,
    name: mux::StreamName,
    _session: Option<std::sync::Arc<crate::connector::StreamRebind>>,
}
impl ClientStream {
    #[cfg(test)]
    pub(crate) fn new(
        writer: MigratingStreamWriter,
        reader: tokio::sync::oneshot::Receiver<StreamReader>,
        addr: SocketAddrPair,
    ) -> Self {
        let (write, _rebind_tx) = MigratingWriteHalf::new_with_rebind(writer);
        let name = write.name_handle();
        Self {
            write,
            reader_state: ReaderState::Pending { rx: reader },
            addr,
            name,
            _session: None,
        }
    }
    pub(crate) fn new_duplex(
        writer: MigratingStreamWriter,
        reader: tokio::sync::oneshot::Receiver<StreamReader>,
        addr: SocketAddrPair,
        logical_id: u64,
        router: mux::ResponseRouterHandle,
        session: crate::connector::SessionGuard,
    ) -> Self {
        let (write, rebind_tx) = MigratingWriteHalf::new_with_rebind(writer);
        let session = crate::connector::StreamRebind::track(rebind_tx.downgrade(), session);
        let name = write.name_handle();
        let (spliced_tx, spliced_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok(gen0_reader) = reader.await else {
                return;
            };
            let rx = router.expect_response(logical_id, gen0_reader);
            if let Ok(spliced) = rx.await {
                let _ = spliced_tx.send(spliced);
            }
        });
        Self {
            write,
            reader_state: ReaderState::PendingSpliced { rx: spliced_rx },
            addr,
            name,
            _session: Some(session),
        }
    }
    pub fn addr(&self) -> SocketAddrPair {
        self.addr
    }
    pub fn set_name(&self, name: &str) {
        self.name.set(name);
    }
}
impl fmt::Debug for ClientStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientStream")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}
impl AsyncRead for ClientStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.reader_state {
                ReaderState::Pending { rx } => match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(reader)) => self.reader_state = ReaderState::Ready { reader },
                    Poll::Ready(Err(_)) => {
                        self.reader_state = ReaderState::Failed;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "gen-0 reader channel closed before write",
                        )));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ReaderState::PendingSpliced { rx } => match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(reader)) => {
                        self.reader_state = ReaderState::ReadySpliced { reader }
                    }
                    Poll::Ready(Err(_)) => {
                        self.reader_state = ReaderState::Failed;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "response splice channel closed before write",
                        )));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ReaderState::Ready { reader } => return Pin::new(reader).poll_read(cx, buf),
                ReaderState::ReadySpliced { reader } => return Pin::new(reader).poll_read(cx, buf),
                ReaderState::Failed => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "gen-0 reader channel closed before write",
                    )));
                }
            }
        }
    }
}
impl AsyncWrite for ClientStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write
            .poll_write_vectored_inner(cx, &[io::IoSlice::new(buf)])
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.write.poll_write_vectored_inner(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write.poll_flush_inner(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write.poll_shutdown_inner(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::migrating_write_half::{WRITE_MAX_CHUNK, WRITE_QUEUE_CAPACITY};
    use mux::{Initiation, MuxConfig, MuxError};
    use tokio::{io::AsyncWriteExt, task::JoinSet};

    use super::*;

    async fn make_dual_pair() -> (
        mux::DualStreamOpener,
        mux::DualStreamAccepter,
        JoinSet<MuxError>,
        JoinSet<MuxError>,
    ) {
        fn config(initiation: Initiation) -> MuxConfig {
            MuxConfig {
                initiation,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: true,
            }
        }
        fn lane(
            client_config: MuxConfig,
            server_config: MuxConfig,
        ) -> (
            mux::StreamOpener,
            mux::StreamAccepter,
            JoinSet<MuxError>,
            mux::StreamOpener,
            mux::StreamAccepter,
            JoinSet<MuxError>,
        ) {
            let (client_io, server_io) = tokio::io::duplex(64 * 1024);
            let (client_read, client_write) = tokio::io::split(client_io);
            let (server_read, server_write) = tokio::io::split(server_io);
            let mut client_tasks = JoinSet::new();
            let (client_opener, client_accepter) = mux::spawn_mux_no_reconnection(
                client_read,
                client_write,
                client_config,
                &mut client_tasks,
            );
            let mut server_tasks = JoinSet::new();
            let (server_opener, server_accepter) = mux::spawn_mux_no_reconnection(
                server_read,
                server_write,
                server_config,
                &mut server_tasks,
            );
            (
                client_opener,
                client_accepter,
                client_tasks,
                server_opener,
                server_accepter,
                server_tasks,
            )
        }
        let (ci_o, ci_a, ci_t, si_o, si_a, si_t) =
            lane(config(Initiation::Client), config(Initiation::Server));
        let (cb_o, cb_a, cb_t, sb_o, sb_a, sb_t) =
            lane(config(Initiation::Client), config(Initiation::Server));
        let mut client_supervisor = JoinSet::new();
        let (client_opener, _client_accepter) = mux::spawn_dual_mux_paired_supervised(
            ci_o,
            ci_a,
            ci_t,
            cb_o,
            cb_a,
            cb_t,
            &mut client_supervisor,
        );
        let mut server_supervisor = JoinSet::new();
        let (_server_opener, server_accepter) = mux::spawn_dual_mux_paired_supervised(
            si_o,
            si_a,
            si_t,
            sb_o,
            sb_a,
            sb_t,
            &mut server_supervisor,
        );
        (
            client_opener,
            server_accepter,
            client_supervisor,
            server_supervisor,
        )
    }

    #[tokio::test]
    async fn write_is_cancellation_safe_and_shutdown_delivers_final() {
        use tokio::io::{AsyncReadExt, AsyncWrite};
        let (opener, accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let addr = SocketAddrPair {
            local_addr: "127.0.0.1:10000".parse().unwrap(),
            peer_addr: "127.0.0.1:10001".parse().unwrap(),
        };
        let (writer, reader) = opener.open_migrating_with_reader(42, mux::LaneClass::Interactive);
        let mut stream = ClientStream::new(writer, reader, addr);
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let accepter_task = tokio::spawn(async move {
            let mut accepter = accepter.into_migrating_only();
            let mut accepted_tx = Some(accepted_tx);
            while let Ok(accepted) = accepter.accept().await {
                match accepted {
                    mux::AcceptedStream::Migrating { reader, writer, .. } => {
                        if let Some(accepted_tx) = accepted_tx.take() {
                            let _ = accepted_tx.send((reader, writer));
                        }
                    }
                    _ => panic!("expected migrating stream"),
                }
            }
        });
        let big = vec![0xA5; 256 * 1024];
        let mut accepted_big_bytes = 0usize;
        let mut saw_pending = false;
        for _ in 0..WRITE_QUEUE_CAPACITY + 2 {
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            match Pin::new(&mut stream).poll_write(&mut cx, &big) {
                Poll::Ready(Ok(n)) => {
                    assert!(n <= WRITE_MAX_CHUNK);
                    accepted_big_bytes += n;
                }
                Poll::Pending => {
                    saw_pending = true;
                    break;
                }
                Poll::Ready(Err(error)) => panic!("unexpected write error: {error}"),
            }
        }
        assert!(
            saw_pending,
            "bounded writer queue never applied backpressure"
        );
        let small = b"small-after-cancel";
        let n = stream.write(small).await.unwrap();
        assert_eq!(n, small.len());
        let (reader, writer) = tokio::time::timeout(Duration::from_secs(2), accepted_rx)
            .await
            .unwrap()
            .unwrap();
        let receive = tokio::spawn(async move {
            let mut reader = reader;
            let _writer = writer;
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        stream.shutdown().await.unwrap();
        let bytes = tokio::time::timeout(Duration::from_secs(3), receive)
            .await
            .expect("FINAL was not delivered")
            .unwrap();
        assert_eq!(bytes.len(), accepted_big_bytes + small.len());
        assert_eq!(&bytes[bytes.len() - small.len()..], small);
        accepter_task.abort();
    }

    #[tokio::test]
    async fn vectored_data_precedes_fin_in_both_directions() {
        use std::io::IoSlice;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (opener, accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let addr = SocketAddrPair {
            local_addr: "127.0.0.1:10000".parse().unwrap(),
            peer_addr: "127.0.0.1:10001".parse().unwrap(),
        };
        let (writer, reader) = opener.open_migrating_with_reader(43, mux::LaneClass::Interactive);
        let mut client = ClientStream::new(writer, reader, addr);
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let accepter_task = tokio::spawn(async move {
            let mut accepter = accepter.into_migrating_only();
            let mut accepted_tx = Some(accepted_tx);
            while let Ok(accepted) = accepter.accept().await {
                match accepted {
                    mux::AcceptedStream::Migrating {
                        reader,
                        writer,
                        source_lane,
                        ..
                    } => {
                        if let Some(accepted_tx) = accepted_tx.take() {
                            let _ = accepted_tx.send((reader, writer, source_lane));
                        }
                    }
                    _ => panic!("expected migrating stream"),
                }
            }
        });
        assert!(client.is_write_vectored());
        let request = [
            IoSlice::new(b"client-"),
            IoSlice::new(b"vectored-"),
            IoSlice::new(b"request"),
        ];
        let request_len = request.iter().map(|buf| buf.len()).sum::<usize>();
        assert_eq!(client.write_vectored(&request).await.unwrap(), request_len);
        client.shutdown().await.unwrap();
        let (reader, writer, source_lane) =
            tokio::time::timeout(Duration::from_secs(2), accepted_rx)
                .await
                .unwrap()
                .unwrap();
        let mut server = crate::ServerStream::Migrating {
            reader,
            writer,
            addr,
            source_lane,
        };
        assert!(server.is_write_vectored());
        let mut received_request = Vec::new();
        server.read_to_end(&mut received_request).await.unwrap();
        assert_eq!(received_request, b"client-vectored-request");
        let response = [
            IoSlice::new(b"server-"),
            IoSlice::new(b"vectored-"),
            IoSlice::new(b"response"),
        ];
        let response_len = response.iter().map(|buf| buf.len()).sum::<usize>();
        assert_eq!(
            server.write_vectored(&response).await.unwrap(),
            response_len
        );
        server.shutdown().await.unwrap();
        let mut received_response = Vec::new();
        client.read_to_end(&mut received_response).await.unwrap();
        assert_eq!(received_response, b"server-vectored-response");
        accepter_task.abort();
    }
}
