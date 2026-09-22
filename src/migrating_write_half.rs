use mux::MigratingStreamWriter;
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
pub(crate) const WRITE_QUEUE_CAPACITY: usize = 8;
pub(crate) const WRITE_MAX_CHUNK: usize = 64 * 1024;
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(5);
/// The finalize step the background writer runs at shutdown.
///
/// Production uses [`RealFinalize`], which finalizes the migrating stream;
/// the shutdown-timeout arm is only reachable when a finalize outlives
/// [`FINALIZE_TIMEOUT`], so the shutdown tests substitute a finalize that can
/// stall or overrun a shrunken budget.
pub(crate) trait ShutdownFinalize: Send + 'static {
    fn finalize(
        &mut self,
        writer: &mut MigratingStreamWriter,
    ) -> impl Future<Output = Result<(), mux::MigratingStreamError>> + Send;
}
pub(crate) struct RealFinalize;
impl ShutdownFinalize for RealFinalize {
    fn finalize(
        &mut self,
        writer: &mut MigratingStreamWriter,
    ) -> impl Future<Output = Result<(), mux::MigratingStreamError>> + Send {
        writer.finalize()
    }
}
pub(crate) struct RebindSlot {
    latest: Arc<Mutex<Option<mux::DualStreamOpener>>>,
    wake: tokio::sync::mpsc::Sender<()>,
}
#[derive(Clone)]
pub(crate) struct RebindHandle {
    latest: Arc<Mutex<Option<mux::DualStreamOpener>>>,
    wake: tokio::sync::mpsc::WeakSender<()>,
}
impl RebindSlot {
    #[cfg(test)]
    pub(crate) fn detached() -> (Self, tokio::sync::mpsc::Receiver<()>) {
        let (wake, wake_rx) = tokio::sync::mpsc::channel(1);
        let slot = Self {
            latest: Arc::new(Mutex::new(None)),
            wake,
        };
        (slot, wake_rx)
    }
    #[cfg(test)]
    pub(crate) fn take(&self) -> Option<mux::DualStreamOpener> {
        self.latest.lock().unwrap().take()
    }
    pub(crate) fn handle(&self) -> RebindHandle {
        RebindHandle {
            latest: Arc::clone(&self.latest),
            wake: self.wake.downgrade(),
        }
    }
}
impl RebindHandle {
    pub(crate) fn rebind(&self, opener: mux::DualStreamOpener) -> bool {
        let Some(wake) = self.wake.upgrade() else {
            return false;
        };
        *self.latest.lock().unwrap() = Some(opener);
        !matches!(
            wake.try_send(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(()))
        )
    }
    pub(crate) fn is_alive(&self) -> bool {
        self.wake.upgrade().is_some()
    }
}
impl fmt::Debug for RebindHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RebindHandle")
    }
}
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
    shutdown_result: Option<io::Result<()>>,
    name: crate::StreamName,
    /// Keeps the rebind wake channel's sender alive for the lifetime of the
    /// write half; never read directly.
    #[allow(dead_code)]
    rebind_guard: RebindSlot,
    background_writer: tokio::task::JoinSet<()>,
}
impl fmt::Debug for MigratingWriteHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigratingWriteHalf").finish_non_exhaustive()
    }
}
impl Drop for MigratingWriteHalf {
    fn drop(&mut self) {
        self.background_writer.abort_all();
    }
}
impl MigratingWriteHalf {
    pub(crate) fn new_with_rebind(writer: MigratingStreamWriter) -> (Self, RebindHandle) {
        Self::new_with_finalize(writer, RealFinalize)
    }
    /// [`Self::new_with_rebind`] with the finalize step supplied by the
    /// caller: production passes [`RealFinalize`].
    pub(crate) fn new_with_finalize(
        mut writer: MigratingStreamWriter,
        mut finalize: impl ShutdownFinalize,
    ) -> (Self, RebindHandle) {
        let name = writer.name();
        let (write_tx, mut write_rx) =
            tokio::sync::mpsc::channel::<WriteCommand>(WRITE_QUEUE_CAPACITY);
        let (wake_tx, mut wake_rx) = tokio::sync::mpsc::channel::<()>(1);
        let rebind = RebindSlot {
            latest: Arc::new(Mutex::new(None)),
            wake: wake_tx,
        };
        let handle = rebind.handle();
        let latest = Arc::clone(&rebind.latest);
        let background_error: Arc<Mutex<Option<BackgroundWriteError>>> = Arc::new(Mutex::new(None));
        let background_error_clone = Arc::clone(&background_error);
        let mut background_writer = tokio::task::JoinSet::new();
        background_writer.spawn(async move {
            let mut rebind_open = true;
            loop {
                let command = tokio::select! {
                    biased;
                    wake = wake_rx.recv(), if rebind_open => {
                        match wake {
                            Some(()) => {
                                let opener = latest.lock().unwrap().take();
                                if let Some(opener) = opener
                                    && let Err(error) = writer.rebind(opener).await {
                                        *background_error_clone.lock().unwrap() =
                                            Some(BackgroundWriteError::from_debug(error));
                                        break;
                                    }
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
                        let result = match tokio::time::timeout(
                            FINALIZE_TIMEOUT,
                            finalize.finalize(&mut writer),
                        )
                        .await
                        {
                            Ok(result) => result.map_err(BackgroundWriteError::from_debug),
                            Err(_) => Err(BackgroundWriteError::from_debug(
                                "shutdown finalize timed out",
                            )),
                        };
                        if let Err(error) = &result {
                            *background_error_clone.lock().unwrap() = Some(error.clone());
                        }
                        let _ = reply.send(result);
                        return;
                    }
                }
            }
            let _ = tokio::time::timeout(FINALIZE_TIMEOUT, finalize.finalize(&mut writer)).await;
        });
        let half = Self {
            write_tx: tokio_util::sync::PollSender::new(write_tx),
            pending_control: None,
            background_error,
            shutdown_started: false,
            shutdown_complete: false,
            shutdown_result: None,
            name,
            rebind_guard: rebind,
            background_writer,
        };
        (half, handle)
    }
    pub fn name_handle(&self) -> crate::StreamName {
        self.name.clone()
    }
    fn reap_background_writer(&mut self) {
        while let Some(result) = self.background_writer.try_join_next() {
            result.unwrap();
        }
    }
    fn poll_pending_control(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<ControlKind>>> {
        self.reap_background_writer();
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
            self.write_tx.close();
            self.shutdown_result = Some(match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(error.to_io()),
                Err(_) => Err(self.background_io_error("RTP mux background writer stopped")),
            });
            return Poll::Ready(Ok(Some(kind)));
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
    pub(crate) fn poll_write_vectored_inner(
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
    pub(crate) fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
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
    pub(crate) fn poll_shutdown_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shutdown_complete {
            return Poll::Ready(Ok(()));
        }
        if !self.shutdown_started {
            if self.background_error.lock().unwrap().is_some() {
                self.write_tx.close();
                self.shutdown_result =
                    Some(Err(self.background_io_error("background write error")));
                return self.poll_shutdown_epilog(cx);
            }
            let (reply, response) = tokio::sync::oneshot::channel();
            match self.write_tx.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    // Send Shutdown once: only count the shutdown as started
                    // once the command is actually on the channel, so a
                    // pending reserve is retried on the next poll.
                    match self.write_tx.send_item(WriteCommand::Shutdown(reply)) {
                        Ok(()) => {
                            self.shutdown_started = true;
                            self.pending_control = Some(PendingControl {
                                kind: ControlKind::Shutdown,
                                reply: response,
                            });
                            cx.waker().wake_by_ref();
                        }
                        Err(_) => {
                            self.write_tx.close();
                            self.shutdown_result = Some(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "write channel closed during shutdown",
                            )));
                            return self.poll_shutdown_epilog(cx);
                        }
                    }
                }
                Poll::Ready(Err(_)) => {
                    self.write_tx.close();
                    self.shutdown_result = Some(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "write channel closed during shutdown",
                    )));
                    return self.poll_shutdown_epilog(cx);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        match self.poll_pending_control(cx) {
            Poll::Ready(Ok(Some(ControlKind::Shutdown))) => {}
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(error)) => {
                self.write_tx.close();
                self.shutdown_result = Some(Err(error));
                return self.poll_shutdown_epilog(cx);
            }
            Poll::Pending => return Poll::Pending,
        }
        // The finalize reply is in (stored in `shutdown_result`); return only
        // once the background writer task itself has fully ended.
        self.poll_shutdown_epilog(cx)
    }

    /// Join the background writer: `AsyncWrite::poll_shutdown` must not
    /// report completion while the writer task is still unwinding after it
    /// sent its finalize reply.  Returns the stored finalize result only
    /// after the `JoinSet` is empty.
    fn poll_shutdown_epilog(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match Pin::new(&mut self.background_writer).poll_join_next(cx) {
                Poll::Ready(Some(result)) => result.unwrap(),
                Poll::Ready(None) => {
                    self.shutdown_complete = true;
                    return Poll::Ready(
                        self.shutdown_result
                            .take()
                            .expect("shutdown epilog polled without a result"),
                    );
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlKind {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mux::{Initiation, MuxConfig, MuxError};
    use tokio::task::JoinSet;

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
    async fn dropping_the_write_half_aborts_its_background_writer() {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        let (opener, accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(50, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        assert_eq!(
            half.background_writer.len(),
            1,
            "the writer task must be owned by the object while it lives",
        );
        // The migrating stream is opened lazily by the first write; without
        // it the peer never sees an announced stream to accept.
        tokio::time::timeout(Duration::from_secs(2), half.write_all(b"x"))
            .await
            .expect("the first write must open the migrating stream")
            .unwrap();
        let mut accepter = accepter.into_migrating_only();
        let accepted = tokio::time::timeout(Duration::from_secs(2), accepter.accept())
            .await
            .expect("the stream must be accepted while the writer is alive")
            .unwrap();
        let mut reader = match accepted {
            mux::AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected a migrating stream"),
        };
        let mut buf = [0u8; 4];
        let n = tokio::time::timeout(Duration::from_secs(2), reader.read(&mut buf))
            .await
            .expect("the data written before the drop must reach the peer")
            .unwrap();
        assert_eq!(&buf[..n], b"x");
        // Dropping the write half aborts its background writer (the JoinSet
        // drop is the abort backstop): the peer must not receive any further
        // data once the writer task is gone.
        drop(half);
        tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf))
            .await
            .expect_err("the aborted background writer must not deliver further data");
    }

    #[tokio::test]
    async fn shutdown_drains_the_background_writer() {
        use tokio::io::AsyncWriteExt;
        let (opener, _accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(51, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        assert_eq!(
            half.background_writer.len(),
            1,
            "the writer task must be owned by the object while it lives",
        );
        tokio::time::timeout(Duration::from_secs(2), half.shutdown())
            .await
            .expect("a normal shutdown must complete")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !half.background_writer.is_empty() {
                half.reap_background_writer();
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the background writer must drain after a normal shutdown");
        assert!(half.background_writer.is_empty());
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        use tokio::io::AsyncWriteExt;
        let (opener, _accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(52, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        tokio::time::timeout(Duration::from_secs(2), half.shutdown())
            .await
            .expect("the first shutdown must complete")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), half.shutdown())
            .await
            .expect("the second shutdown must complete")
            .expect("a second shutdown of an already-closed half must succeed");
    }

    #[tokio::test]
    async fn a_flush_does_not_close_the_write_half() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (opener, accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(53, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        half.write_all(b"first").await.unwrap();
        half.flush().await.unwrap();
        // A flush must not finalize the stream: data written afterwards still
        // reaches the peer.
        half.write_all(b"second").await.unwrap();
        half.shutdown().await.unwrap();
        let mut accepter = accepter.into_migrating_only();
        let accepted = tokio::time::timeout(Duration::from_secs(2), accepter.accept())
            .await
            .expect("the stream must be accepted")
            .unwrap();
        let mut reader = match accepted {
            mux::AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected a migrating stream"),
        };
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), reader.read_to_end(&mut bytes))
            .await
            .expect("the peer must see the stream end")
            .unwrap();
        assert_eq!(
            bytes, b"firstsecond",
            "a flush ended the stream, so the write after it was lost",
        );
    }

    /// One `poll_write` stages exactly one `WRITE_MAX_CHUNK`-capped command.
    /// The cap is the mux layer's 64 KiB frame/reassembly budget, so accepting
    /// a larger chunk would hand the mux layer one stream write that it cannot
    /// carry in a single frame.
    #[tokio::test(flavor = "current_thread")]
    async fn a_single_poll_write_accepts_exactly_the_max_chunk() {
        use std::task::Waker;
        use tokio::io::AsyncWrite;
        let (opener, _accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(78, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        assert_eq!(
            WRITE_MAX_CHUNK,
            64 * 1024,
            "the chunk cap must stay the mux layer's 64 KiB frame budget",
        );
        let payload = vec![0x5Au8; 3 * WRITE_MAX_CHUNK];
        let mut cx = Context::from_waker(Waker::noop());
        let mut accepted = 0;
        for staged in 0..3 {
            let remaining = payload.len() - accepted;
            let n = match std::pin::Pin::new(&mut half).poll_write(&mut cx, &payload[accepted..]) {
                Poll::Ready(Ok(n)) => n,
                other => panic!("poll_write of {remaining} bytes staged: {other:?}"),
            };
            assert_eq!(
                n,
                64 * 1024,
                "command {staged} accepted {n} bytes; one poll_write may stage at most one chunk",
            );
            accepted += n;
        }
        assert_eq!(accepted, payload.len());
    }

    /// A payload larger than the chunk cap crosses a chunk boundary and still
    /// arrives byte for byte: the chunking loop must not drop, duplicate, or
    /// reorder the tail of a capped command.
    #[tokio::test]
    async fn a_write_larger_than_the_chunk_cap_reaches_the_peer_byte_for_byte() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (opener, accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(79, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        let mut accepter = accepter.into_migrating_only();
        let payload: Vec<u8> = (0..(2 * WRITE_MAX_CHUNK + 7))
            .map(|index| (index % 251) as u8)
            .collect();
        half.write_all(&payload)
            .await
            .expect("the payload must reach the peer");
        half.shutdown().await.expect("the stream must end cleanly");
        let accepted = tokio::time::timeout(Duration::from_secs(2), accepter.accept())
            .await
            .expect("the stream must be accepted")
            .expect("accept must succeed");
        let mut reader = match accepted {
            mux::AcceptedStream::Migrating { reader, .. } => reader,
            _ => panic!("expected a migrating stream"),
        };
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), reader.read_to_end(&mut received))
            .await
            .expect("the peer must see the stream end")
            .expect("the read must succeed");
        if received != payload {
            let first_difference = received
                .iter()
                .zip(payload.iter())
                .position(|(got, want)| got != want);
            panic!(
                "the peer received {} of {} bytes; the first difference is at {first_difference:?}, \
                 so the write path lost, duplicated, or reordered bytes across the chunk boundary",
                received.len(),
                payload.len(),
            );
        }
    }

    /// The write half stages at most `WRITE_QUEUE_CAPACITY` commands ahead of
    /// the background writer, and a zero-byte write consumes no slot: the
    /// queue depth is the in-flight byte budget (8 x 64 KiB), and a free
    /// zero-byte write is what lets a caller poll an empty slice without
    /// spending that budget.
    #[tokio::test(flavor = "current_thread")]
    async fn the_write_queue_stages_exactly_its_depth_and_zero_byte_writes_are_free() {
        use std::task::Waker;
        use tokio::io::AsyncWrite;
        let (opener, _accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(80, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            WRITE_QUEUE_CAPACITY, 8,
            "the in-flight budget must stay eight 64 KiB commands",
        );
        // Nothing is awaited between polls, so the background writer cannot
        // drain and the bounded command channel's depth is exactly observable.
        for empty in 0..(2 * WRITE_QUEUE_CAPACITY) {
            let polled = std::pin::Pin::new(&mut half).poll_write(&mut cx, &[]);
            assert!(
                matches!(polled, Poll::Ready(Ok(0))),
                "zero-byte write {empty} returned {polled:?} instead of Ready(Ok(0)), so an empty \
                 write consumed an in-flight slot",
            );
        }
        let chunk = vec![0x11u8; WRITE_MAX_CHUNK];
        for staged in 0..WRITE_QUEUE_CAPACITY {
            let polled = std::pin::Pin::new(&mut half).poll_write(&mut cx, &chunk);
            assert!(
                matches!(polled, Poll::Ready(Ok(n)) if n == WRITE_MAX_CHUNK),
                "queued command {staged} returned {polled:?} instead of Ready(Ok({WRITE_MAX_CHUNK}))",
            );
        }
        let polled = std::pin::Pin::new(&mut half).poll_write(&mut cx, &chunk);
        assert!(
            matches!(polled, Poll::Pending),
            "the command after a full queue returned {polled:?} instead of Pending, so the in-flight \
             budget is larger than {WRITE_QUEUE_CAPACITY} commands",
        );
    }

    /// A single `poll_write` builds the background-writer command from exactly
    /// one capped chunk buffer: `Vec::with_capacity(chunk)` is the only
    /// allocation the call may make. Regression guard for the per-write
    /// staging cost — adding any second allocation (a scratch buffer, a
    /// per-slice copy, a formatted string) fails the count. Runs on the
    /// current thread so the thread-local allocation counter measures this
    /// test's poll only; the background writer's consumption happens on a
    /// later yield, outside the measured window.
    #[tokio::test(flavor = "current_thread")]
    async fn a_write_builds_exactly_one_capped_chunk_buffer() {
        use std::task::Waker;
        use tokio::io::AsyncWrite;
        let (opener, _accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) = opener.open_migrating_with_reader(77, mux::LaneClass::Interactive);
        let (mut half, _rebind) = MigratingWriteHalf::new_with_rebind(writer);
        let chunk = vec![0xABu8; 4096];
        let mut cx = Context::from_waker(Waker::noop());

        // Warm one write and yield so the background writer consumes it and
        // the command channel stays writable for the measured writes.
        let n = match std::pin::Pin::new(&mut half).poll_write(&mut cx, &chunk) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("first poll_write should be Ready(Ok): {other:?}"),
        };
        assert_eq!(n, 4096);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let before = crate::test_alloc::thread_alloc_count();
        let n = match std::pin::Pin::new(&mut half).poll_write(&mut cx, &chunk) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("a warm poll_write should be Ready(Ok): {other:?}"),
        };
        let allocated = crate::test_alloc::thread_alloc_count() - before;
        assert_eq!(n, 4096);
        assert_eq!(
            allocated, 1,
            "a single poll_write allocated {allocated} times; expected exactly one \
             (the capped chunk command buffer). The write path must stage the \
             caller's bytes into one buffer per call, with no extra scratch \
             allocations",
        );
    }

    /// A substitute finalize for the shutdown path: records that it ran, then
    /// either stalls forever, sleeps for `delay`, or returns `LaneDead`.
    struct SubstitutedFinalize {
        delay: Duration,
        stall: bool,
        fail: bool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl ShutdownFinalize for SubstitutedFinalize {
        fn finalize(
            &mut self,
            _writer: &mut MigratingStreamWriter,
        ) -> impl Future<Output = Result<(), mux::MigratingStreamError>> + Send {
            let delay = self.delay;
            let stall = self.stall;
            let fail = self.fail;
            let calls = Arc::clone(&self.calls);
            async move {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if stall {
                    std::future::pending::<()>().await;
                } else {
                    tokio::time::sleep(delay).await;
                }
                if fail {
                    Err(mux::MigratingStreamError::LaneDead)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// A write half whose shutdown finalize is the caller's substitution. The
    /// pair's mux tasks are dropped before this returns, so only the
    /// substituted finalize and the shutdown timeout arm the clock.
    async fn write_half_with_finalize(
        logical_id: u64,
        stall: bool,
        delay: Duration,
        fail: bool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> MigratingWriteHalf {
        let (opener, _accepter, _client_tasks, _server_tasks) = make_dual_pair().await;
        let (writer, _gen0) =
            opener.open_migrating_with_reader(logical_id, mux::LaneClass::Interactive);
        let (half, _rebind) = MigratingWriteHalf::new_with_finalize(
            writer,
            SubstitutedFinalize {
                delay,
                stall,
                fail,
                calls,
            },
        );
        half
    }

    /// A finalize stuck past the budget must make shutdown report the timeout
    /// error; reporting success would hide a stream that never closed.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_finalize_makes_shutdown_report_the_timeout_error() {
        use tokio::io::AsyncWriteExt;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut half =
            write_half_with_finalize(90, true, Duration::ZERO, false, Arc::clone(&calls)).await;
        let started = tokio::time::Instant::now();
        let result = half.shutdown().await;
        let elapsed = started.elapsed();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the shutdown must run the finalize exactly once",
        );
        let error = result.expect_err(
            "a finalize stuck past FINALIZE_TIMEOUT must make shutdown report an error, not success",
        );
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(
            error.to_string().contains("shutdown finalize timed out"),
            "the shutdown error must name the finalize timeout, got: {error}",
        );
        assert!(
            elapsed <= Duration::from_secs(10),
            "a stuck finalize must be abandoned within a small multiple of the {FINALIZE_TIMEOUT:?} \
             budget, but shutdown waited {elapsed:?}",
        );
    }

    /// A finalize that returns inside the budget must let shutdown succeed:
    /// shrinking the budget below the finalize's duration turns it into a
    /// timeout error.
    #[tokio::test(start_paused = true)]
    async fn a_finalize_that_returns_inside_the_budget_reports_success() {
        use tokio::io::AsyncWriteExt;
        let one_second = Duration::from_secs(1);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut half =
            write_half_with_finalize(91, false, one_second, false, Arc::clone(&calls)).await;
        let result = half.shutdown().await;
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the shutdown must run the finalize exactly once",
        );
        assert!(
            result.is_ok(),
            "a finalize that returns after {one_second:?} must complete inside the \
             {FINALIZE_TIMEOUT:?} budget, but shutdown reported {result:?}",
        );
    }

    /// A finalize that fails must surface its own error, not success and not
    /// the timeout error.
    #[tokio::test(start_paused = true)]
    async fn a_failed_finalize_makes_shutdown_report_the_finalize_error() {
        use tokio::io::AsyncWriteExt;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut half =
            write_half_with_finalize(92, false, Duration::ZERO, true, Arc::clone(&calls)).await;
        let result = half.shutdown().await;
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the shutdown must run the finalize exactly once",
        );
        let error = result.expect_err("a finalize that fails must make shutdown report an error");
        assert!(
            error.to_string().contains("LaneDead"),
            "shutdown must surface the finalize's own error, got: {error}",
        );
        assert!(
            !error.to_string().contains("timed out"),
            "a failing finalize is not a timeout, got: {error}",
        );
    }
}
