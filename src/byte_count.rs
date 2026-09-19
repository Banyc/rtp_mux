use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug, Default)]
pub(crate) struct SessionByteCounters {
    tx_bytes: AtomicU64,
    rx_bytes: AtomicU64,
}

impl SessionByteCounters {
    pub(crate) fn count_read<R>(self: &Arc<Self>, inner: R) -> Counted<R> {
        Counted {
            inner,
            counter: CounterTarget {
                traffic: Arc::clone(self),
                rx: true,
            },
        }
    }

    pub(crate) fn count_write<W>(self: &Arc<Self>, inner: W) -> Counted<W> {
        Counted {
            inner,
            counter: CounterTarget {
                traffic: Arc::clone(self),
                rx: false,
            },
        }
    }

    pub(crate) fn tx_bytes(&self) -> u64 {
        self.tx_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn rx_bytes(&self) -> u64 {
        self.rx_bytes.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct CounterTarget {
    traffic: Arc<SessionByteCounters>,
    rx: bool,
}

impl CounterTarget {
    fn add(&self, bytes: usize) {
        let counter = match self.rx {
            true => &self.traffic.rx_bytes,
            false => &self.traffic.tx_bytes,
        };
        counter.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct Counted<T> {
    inner: T,
    counter: CounterTarget,
}

impl<R: AsyncRead + Unpin> AsyncRead for Counted<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if polled.is_ready() {
            self.counter.add(buf.filled().len().saturating_sub(before));
        }
        polled
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Counted<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(written)) = &polled {
            self.counter.add(*written);
        }
        polled
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(written)) = &polled {
            self.counter.add(*written);
        }
        polled
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionStats {
    pub live_streams: u64,
    pub opened_streams: u64,
    pub tx_bytes: u64,
    pub rx_bytes: u64,
    pub uptime: Duration,
}

fn rate(bytes: u64, uptime: Duration) -> f64 {
    let secs = uptime.as_secs_f64();
    match secs > 0.0 {
        true => bytes as f64 / secs,
        false => 0.0,
    }
}

fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut scaled = bytes;
    for unit in UNITS {
        if scaled < 1024.0 || unit == *UNITS.last().expect("UNITS is not empty") {
            return match unit == "B" {
                true => format!("{scaled:.0}{unit}"),
                false => format!("{scaled:.1}{unit}"),
            };
        }
        scaled /= 1024.0;
    }
    unreachable!()
}

impl SessionStats {
    pub fn tx_bytes_per_sec(&self) -> f64 {
        rate(self.tx_bytes, self.uptime)
    }

    pub fn rx_bytes_per_sec(&self) -> f64 {
        rate(self.rx_bytes, self.uptime)
    }
}

impl std::fmt::Display for SessionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "streams={} live/{} opened, tx={} ({}/s), rx={} ({}/s), up={:.1}s",
            self.live_streams,
            self.opened_streams,
            human_bytes(self.tx_bytes as f64),
            human_bytes(self.tx_bytes_per_sec()),
            human_bytes(self.rx_bytes as f64),
            human_bytes(self.rx_bytes_per_sec()),
            self.uptime.as_secs_f64()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_counted_half_tallies_only_the_bytes_it_moved() {
        let traffic = Arc::new(SessionByteCounters::default());
        let (client, mut server) = tokio::io::duplex(64);
        let (reader, writer) = tokio::io::split(client);
        let mut reader = traffic.count_read(reader);
        let mut writer = traffic.count_write(writer);
        writer.write_all(b"hello").await.unwrap();
        assert_eq!(traffic.tx_bytes(), 5);
        assert_eq!(traffic.rx_bytes(), 0, "a write must not count as received");
        server.write_all(b"hi").await.unwrap();
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(traffic.rx_bytes(), 2);
        assert_eq!(traffic.tx_bytes(), 5, "a read must not count as sent");
    }

    #[derive(Debug, Default)]
    struct VectoredOnly {
        written: usize,
    }

    impl AsyncWrite for VectoredOnly {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::Unsupported)))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let written = bufs.iter().map(|buf| buf.len()).sum();
            self.written += written;
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn counting_a_vectored_writer_keeps_it_vectored() {
        let traffic = Arc::new(SessionByteCounters::default());
        let mut writer = traffic.count_write(VectoredOnly::default());
        assert!(writer.is_write_vectored(), "the wrapper hid the capability");
        let bufs = [
            std::io::IoSlice::new(b"header"),
            std::io::IoSlice::new(b"payload"),
        ];
        let written = writer.write_vectored(&bufs).await.unwrap();
        assert_eq!(written, 13);
        assert_eq!(traffic.tx_bytes(), 13);
    }

    /// A writer that only ever accepts `budget` bytes in total, so a
    /// single `poll_write`/`poll_write_vectored` can genuinely return fewer
    /// bytes than it was handed.  It advertises the default
    /// `is_write_vectored() == false` so the wrapper's capability forwarding
    /// can be observed.
    #[derive(Debug, Default)]
    struct LimitedWriter {
        budget: usize,
    }

    impl AsyncWrite for LimitedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let written = buf.len().min(self.budget);
            self.budget -= written;
            Poll::Ready(Ok(written))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let mut written = 0;
            for buf in bufs {
                let n = buf.len().min(self.budget - written);
                written += n;
                if written == self.budget {
                    break;
                }
            }
            self.budget -= written;
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A reader that fills every unfilled byte it is offered, so counting the
    /// total filled length is distinguishable from counting only the delta.
    #[derive(Debug)]
    struct FillsAll;

    impl AsyncRead for FillsAll {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let n = buf.remaining();
            for byte in buf.initialize_unfilled().iter_mut() {
                *byte = 0xAB;
            }
            buf.advance(n);
            Poll::Ready(Ok(()))
        }
    }

    fn noop_cx() -> Context<'static> {
        Context::from_waker(std::task::Waker::noop())
    }

    #[test]
    fn a_partial_write_counts_only_the_bytes_the_writer_accepted() {
        let traffic = Arc::new(SessionByteCounters::default());
        let mut writer = traffic.count_write(LimitedWriter { budget: 3 });
        let payload = [0u8; 8];
        let polled = Pin::new(&mut writer).poll_write(&mut noop_cx(), &payload);
        assert!(
            matches!(polled, Poll::Ready(Ok(3))),
            "the writer accepted 3 of 8 bytes"
        );
        assert_eq!(
            traffic.tx_bytes(),
            3,
            "a partial write counted the bytes offered instead of the bytes accepted"
        );
    }

    #[test]
    fn a_partial_vectored_write_counts_only_the_bytes_the_writer_accepted() {
        let traffic = Arc::new(SessionByteCounters::default());
        let mut writer = traffic.count_write(LimitedWriter { budget: 5 });
        let bufs = [
            std::io::IoSlice::new(b"aaa"),
            std::io::IoSlice::new(b"bbb"),
            std::io::IoSlice::new(b"ccc"),
        ];
        let polled = Pin::new(&mut writer).poll_write_vectored(&mut noop_cx(), &bufs);
        assert!(
            matches!(polled, Poll::Ready(Ok(5))),
            "the writer accepted 5 of 9 vectored bytes"
        );
        assert_eq!(
            traffic.tx_bytes(),
            5,
            "a partial vectored write counted the bytes offered instead of the bytes accepted"
        );
    }

    #[test]
    fn a_non_vectored_writer_does_not_claim_the_capability_through_the_wrapper() {
        let traffic = Arc::new(SessionByteCounters::default());
        let writer = traffic.count_write(LimitedWriter::default());
        assert!(
            !writer.is_write_vectored(),
            "the wrapper invented a vectored-write capability the inner writer never had"
        );
    }

    #[test]
    fn a_read_counts_only_the_bytes_the_read_filled_not_the_whole_buffer() {
        let traffic = Arc::new(SessionByteCounters::default());
        let mut reader = traffic.count_read(FillsAll);
        let mut storage = [0u8; 8];
        let mut buf = ReadBuf::new(&mut storage);
        buf.advance(3);
        let polled = Pin::new(&mut reader).poll_read(&mut noop_cx(), &mut buf);
        assert!(matches!(polled, Poll::Ready(Ok(()))), "the read failed");
        assert_eq!(buf.filled().len(), 8);
        assert_eq!(
            traffic.rx_bytes(),
            5,
            "the delta of a partially filled read buffer was counted as the whole buffer"
        );
    }

    #[test]
    fn a_rate_over_no_time_is_zero_rather_than_infinite() {
        let stats = SessionStats {
            live_streams: 1,
            opened_streams: 2,
            tx_bytes: 1024,
            rx_bytes: 512,
            uptime: Duration::ZERO,
        };
        assert_eq!(stats.tx_bytes_per_sec(), 0.0);
        assert_eq!(stats.rx_bytes_per_sec(), 0.0);
        let running = SessionStats {
            uptime: Duration::from_secs(2),
            ..stats
        };
        assert_eq!(running.tx_bytes_per_sec(), 512.0);
        assert_eq!(running.rx_bytes_per_sec(), 256.0);
    }

    #[test]
    fn stats_render_both_stream_counts_and_a_scaled_rate() {
        let stats = SessionStats {
            live_streams: 3,
            opened_streams: 9,
            tx_bytes: 2 * 1024 * 1024,
            rx_bytes: 512,
            uptime: Duration::from_secs(4),
        };
        assert_eq!(
            stats.to_string(),
            "streams=3 live/9 opened, tx=2.0MiB (512.0KiB/s), rx=512B (128B/s), up=4.0s"
        );
    }
}
