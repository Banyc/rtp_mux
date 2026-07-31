use mux::MigratingStreamWriter;
use std::{
    fmt, io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
pub(crate) const WRITE_QUEUE_CAPACITY: usize = 8;
pub(crate) const WRITE_MAX_CHUNK: usize = 64 * 1024;
pub(crate) enum WriteCommand {
    Data(Vec<u8>),
    Flush(tokio::sync::oneshot::Sender<Result<(), BackgroundWriteError>>),
    Shutdown(tokio::sync::oneshot::Sender<Result<(), BackgroundWriteError>>),
}
#[derive(Debug, Clone)]
pub(crate) struct BackgroundWriteError {
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
            message: format!("{:?}", error),
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
pub struct MigratingWriteHalf {
    write_tx: tokio_util::sync::PollSender<WriteCommand>,
    pending_control: Option<PendingControl>,
    background_error: Arc<Mutex<Option<BackgroundWriteError>>>,
    shutdown_started: bool,
    shutdown_complete: bool,
    name: mux::StreamName,
    _rebind_tx: tokio::sync::mpsc::Sender<mux::DualStreamOpener>,
    _background_writer: tokio::task::JoinHandle<()>,
}
impl fmt::Debug for MigratingWriteHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigratingWriteHalf").finish_non_exhaustive()
    }
}
impl MigratingWriteHalf {
    pub(crate) fn new_with_rebind(
        mut writer: MigratingStreamWriter,
    ) -> (Self, tokio::sync::mpsc::Sender<mux::DualStreamOpener>) {
        let name = writer.name_handle();
        let (write_tx, mut write_rx) =
            tokio::sync::mpsc::channel::<WriteCommand>(WRITE_QUEUE_CAPACITY);
        let (rebind_tx, mut rebind_rx) = tokio::sync::mpsc::channel::<mux::DualStreamOpener>(1);
        let background_error: Arc<Mutex<Option<BackgroundWriteError>>> = Arc::new(Mutex::new(None));
        let background_error_clone = Arc::clone(&background_error);
        let background_writer = tokio::spawn(async move {
            let mut rebind_open = true;
            loop {
                let command = tokio::select! {
                    biased;
                    opener = rebind_rx.recv(), if rebind_open => {
                        match opener {
                            Some(opener) => {
                                let _ = writer.rebind(opener).await;
                            }
                            None => rebind_open = false,
                        }
                        continue;
                    }
                    command = write_rx.recv() => match command {
                        Some(command) => command,
                        None => break,
                    },
                };
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
        let half = Self {
            write_tx: tokio_util::sync::PollSender::new(write_tx),
            pending_control: None,
            background_error,
            shutdown_started: false,
            shutdown_complete: false,
            name,
            _rebind_tx: rebind_tx.clone(),
            _background_writer: background_writer,
        };
        (half, rebind_tx)
    }
    pub fn name_handle(&self) -> mux::StreamName {
        self.name.clone()
    }
    fn poll_pending_control(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<ControlKindPublic>>> {
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
        let kind = match kind {
            ControlKind::Flush => ControlKindPublic::Flush,
            ControlKind::Shutdown => ControlKindPublic::Shutdown,
        };
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
    pub(crate) fn poll_write_vectored_inner(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKindPublic::Shutdown))) => {
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
    pub(crate) fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKindPublic::Shutdown))) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stream shut down",
                )));
            }
            Poll::Ready(Ok(Some(ControlKindPublic::Flush))) => return Poll::Ready(Ok(())),
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
    pub(crate) fn poll_shutdown_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKindPublic::Shutdown))) => return Poll::Ready(Ok(())),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlKindPublic {
    Flush,
    Shutdown,
}
impl tokio::io::AsyncWrite for MigratingWriteHalf {
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
        self.poll_flush_inner(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_shutdown_inner(cx)
    }
}
