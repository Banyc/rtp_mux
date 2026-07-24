use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use mux::{MigratingStreamWriter, StreamReader};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::SocketAddrPair;

const WRITE_QUEUE_CAPACITY: usize = 8;
const WRITE_MAX_CHUNK: usize = 64 * 1024;

enum WriteCommand {
    Data(Vec<u8>),
    Flush(tokio::sync::oneshot::Sender<Result<(), BackgroundWriteError>>),
    Shutdown(tokio::sync::oneshot::Sender<Result<(), BackgroundWriteError>>),
}

#[derive(Debug, Clone)]
struct BackgroundWriteError {
    message: String,
}

impl fmt::Display for BackgroundWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BackgroundWriteError {}

impl BackgroundWriteError {
    fn from_debug(error: impl fmt::Debug) -> Self {
        Self {
            message: format!("{error:?}"),
        }
    }

    fn to_io(&self) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, self.message.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlKind {
    Flush,
    Shutdown,
}

struct PendingControl {
    kind: ControlKind,
    reply: tokio::sync::oneshot::Receiver<Result<(), BackgroundWriteError>>,
}

enum ReaderState {
    Pending {
        rx: tokio::sync::oneshot::Receiver<StreamReader>,
    },
    Ready {
        reader: StreamReader,
    },
    Failed,
}

pub struct ClientStream {
    write_tx: tokio_util::sync::PollSender<WriteCommand>,
    pending_control: Option<PendingControl>,
    background_error: Arc<Mutex<Option<BackgroundWriteError>>>,
    shutdown_started: bool,
    shutdown_complete: bool,
    reader_state: ReaderState,
    addr: SocketAddrPair,
    _background_writer: tokio::task::JoinHandle<()>,
}

impl ClientStream {
    pub(crate) fn new(
        mut writer: MigratingStreamWriter,
        reader: tokio::sync::oneshot::Receiver<StreamReader>,
        addr: SocketAddrPair,
    ) -> Self {
        let (write_tx, mut write_rx) =
            tokio::sync::mpsc::channel::<WriteCommand>(WRITE_QUEUE_CAPACITY);
        let background_error = Arc::new(Mutex::new(None::<BackgroundWriteError>));
        let background_error_clone = Arc::clone(&background_error);
        let background_writer = tokio::spawn(async move {
            while let Some(command) = write_rx.recv().await {
                match command {
                    WriteCommand::Data(buf) => {
                        if let Err(error) = writer.write_all(&buf).await {
                            *background_error_clone.lock().unwrap() =
                                Some(BackgroundWriteError::from_debug(error));
                            return;
                        }
                    }
                    WriteCommand::Flush(reply) => {
                        let result = writer
                            .flush()
                            .await
                            .map_err(BackgroundWriteError::from_debug);
                        if let Err(error) = &result {
                            *background_error_clone.lock().unwrap() = Some(error.clone());
                        }
                        let failed = result.is_err();
                        let _ = reply.send(result);
                        if failed {
                            return;
                        }
                    }
                    WriteCommand::Shutdown(reply) => {
                        let result = writer
                            .finalize()
                            .await
                            .map_err(BackgroundWriteError::from_debug);
                        if let Err(error) = &result {
                            *background_error_clone.lock().unwrap() = Some(error.clone());
                        }
                        let _ = reply.send(result);
                        return;
                    }
                }
            }
            let _ = writer.finalize().await;
        });
        Self {
            write_tx: tokio_util::sync::PollSender::new(write_tx),
            pending_control: None,
            background_error,
            shutdown_started: false,
            shutdown_complete: false,
            reader_state: ReaderState::Pending { rx: reader },
            addr,
            _background_writer: background_writer,
        }
    }

    pub fn addr(&self) -> SocketAddrPair {
        self.addr
    }

    fn poll_pending_control(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<ControlKind>>> {
        let Some(pending) = &mut self.pending_control else {
            return Poll::Ready(Ok(None));
        };
        let kind = pending.kind;
        let result = match Pin::new(&mut pending.reply).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.pending_control = None;
        if kind == ControlKind::Shutdown {
            self.shutdown_complete = true;
            self.write_tx.close();
        }
        match result {
            Ok(Ok(())) => Poll::Ready(Ok(Some(kind))),
            Ok(Err(error)) => Poll::Ready(Err(error.to_io())),
            Err(_) => Poll::Ready(Err(
                self.background_io_error("RTP mux background writer stopped")
            )),
        }
    }

    fn background_io_error(&self, message: &str) -> io::Error {
        if let Some(error) = &*self.background_error.lock().unwrap() {
            io::Error::new(io::ErrorKind::BrokenPipe, error.message.clone())
        } else {
            io::Error::new(io::ErrorKind::BrokenPipe, message)
        }
    }

    fn poll_write_vectored_inner(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKind::Shutdown))) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stream shut down",
                )));
            }
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        if self.shutdown_complete || self.shutdown_started {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream shut down",
            )));
        }
        if let Some(error) = &*self.background_error.lock().unwrap() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                error.to_string(),
            )));
        }
        let chunk = bufs
            .iter()
            .fold(0usize, |total, buf| total.saturating_add(buf.len()))
            .min(WRITE_MAX_CHUNK);
        match self.write_tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) if chunk > 0 => {
                let mut data = Vec::with_capacity(chunk);
                for buf in bufs {
                    let remaining = chunk - data.len();
                    if remaining == 0 {
                        break;
                    }
                    data.extend_from_slice(&buf[..buf.len().min(remaining)]);
                }
                let _ = self.write_tx.send_item(WriteCommand::Data(data));
                Poll::Ready(Ok(chunk))
            }
            Poll::Ready(Ok(())) => Poll::Ready(Ok(0)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write channel closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
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
                ReaderState::Ready { reader } => return Pin::new(reader).poll_read(cx, buf),
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
        self.poll_write_vectored_inner(cx, &[io::IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_inner(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKind::Shutdown))) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stream shut down",
                )));
            }
            Poll::Ready(Ok(Some(ControlKind::Flush))) => return Poll::Ready(Ok(())),
            Poll::Ready(Ok(None)) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        if self.shutdown_complete || self.shutdown_started {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream shut down",
            )));
        }
        if let Some(error) = &*self.background_error.lock().unwrap() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                error.to_string(),
            )));
        }
        let (reply, response) = tokio::sync::oneshot::channel();
        match self.write_tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let _ = self.write_tx.send_item(WriteCommand::Flush(reply));
                self.pending_control = Some(PendingControl {
                    kind: ControlKind::Flush,
                    reply: response,
                });
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write channel closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKind::Shutdown))) => return Poll::Ready(Ok(())),
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        if self.shutdown_complete {
            return Poll::Ready(Ok(()));
        }
        if self.background_error.lock().unwrap().is_some() {
            self.shutdown_complete = true;
            return Poll::Ready(Err(self.background_io_error("background write error")));
        }
        self.shutdown_started = true;
        let (reply, response) = tokio::sync::oneshot::channel();
        match self.write_tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let _ = self.write_tx.send_item(WriteCommand::Shutdown(reply));
                self.pending_control = Some(PendingControl {
                    kind: ControlKind::Shutdown,
                    reply: response,
                });
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Err(_)) => {
                self.shutdown_complete = true;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "write channel closed during shutdown",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
                    mux::AcceptedStream::Plain { .. } => panic!("expected migrating stream"),
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
                    mux::AcceptedStream::Plain { .. } => panic!("expected migrating stream"),
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
