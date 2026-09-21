//! The cooperation crate's transport-mediated `mux`-over-`rtp` testing kit:
//! the rtp listener/accept plumbing, the per-stream echo/connect/sink servers,
//! the timestamped-message latency sinks, the transient connects, the payload
//! verifier those sinks share, and the loopback bulk-goodput probe constants
//! and floors.
//!
//! This is the one place that sees `mux` and `rtp` together. `mux` must not
//! know the transport and the transport must not know `mux`, so every scenario
//! that needs both layers drives it from here. The transport-free mux half
//! lives in the `mux` layer kit (`mux::testkit`, behind mux's `testing`
//! feature), the rtp echo/connect/sink/frame/perf-trace scaffolding in the
//! `rtp` layer kit (`rtp::testkit`, behind rtp's `testing` feature), and the
//! generic scenario helpers and impairment instrument in the `netem-test`
//! harness kit (`netem_test::kit`, behind its `test-kit` feature). Imports
//! only ever go downward (rtp_mux kit → mux kit / rtp kit / harness kit), so
//! no layer depends on a sibling's kit and `netem-test` stays a leaf.
//!
//! The mux-side measurement helpers the `mux`-over-`rtp` scenarios drive
//! (open a stream, echo a round trip, send a payload) live here too. They need
//! nothing from `rtp`, but every consumer is a `mux`-over-`rtp` scenario, so
//! keeping them with those consumers avoids leaving transport-free dead weight
//! in the `mux` kit.
//!
//! The operator's product constitution is stated in `GATE.md`
//! ("Performance"): rtp_mux owns the production dual-lane topology, so all
//! three mandates are asserted by rtp_mux's own scenario gates.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;

use mux::testkit::stats::{MuxSessionProgress, SinkProgress, SinkReadOutcome};
use netem_test::kit::{
    LATENCY_SAMPLE_CAPACITY, TestScope, TestTask, TestTaskSubmitter, submit_test_task,
    try_send_observation,
};
use rtp::FecTuning;
use rtp::FrameMode;

const PAYLOAD_PATTERN_PERIOD: usize = 251;
const PAYLOAD_VERIFY_CHUNK_BYTES: usize = 64 * 1024;
static PAYLOAD_VERIFY_WINDOW: [u8; PAYLOAD_VERIFY_CHUNK_BYTES + PAYLOAD_PATTERN_PERIOD - 1] = {
    let mut pattern = [0; PAYLOAD_VERIFY_CHUNK_BYTES + PAYLOAD_PATTERN_PERIOD - 1];
    let mut i = 0;
    while i < pattern.len() {
        pattern[i] = (i % PAYLOAD_PATTERN_PERIOD) as u8;
        i += 1;
    }
    pattern
};

/// Verifies consecutive chunks of the deterministic perf payload, advancing
/// its stream offset only after a complete chunk matches.
#[derive(Default)]
struct PayloadPatternVerifier {
    phase: usize,
}

impl PayloadPatternVerifier {
    fn verify(&mut self, actual: &[u8]) -> bool {
        let mut compared = 0;
        while compared < actual.len() {
            let chunk_len = (actual.len() - compared).min(PAYLOAD_VERIFY_CHUNK_BYTES);
            let pattern_start =
                (self.phase + compared % PAYLOAD_PATTERN_PERIOD) % PAYLOAD_PATTERN_PERIOD;
            if actual[compared..compared + chunk_len]
                != PAYLOAD_VERIFY_WINDOW[pattern_start..pattern_start + chunk_len]
            {
                return false;
            }
            compared += chunk_len;
        }
        self.phase = (self.phase + actual.len() % PAYLOAD_PATTERN_PERIOD) % PAYLOAD_PATTERN_PERIOD;
        true
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod payload_pattern_tests {
    use super::{PAYLOAD_PATTERN_PERIOD, PayloadPatternVerifier};

    fn payload(phase: usize, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| ((phase + i) % PAYLOAD_PATTERN_PERIOD) as u8)
            .collect()
    }

    #[test]
    fn verifier_handles_boundaries_and_does_not_advance_on_corruption() {
        let mut verifier = PayloadPatternVerifier::default();
        assert!(verifier.verify(&[]));

        for len in [1, 249, 251, 252, 64 * 1024 + 257] {
            let expected = payload(verifier.phase, len);
            assert!(verifier.verify(&expected));
        }

        let phase_before_corruption = verifier.phase;
        let mut corrupt = payload(phase_before_corruption, 64 * 1024 + 1);
        for index in [0, corrupt.len() / 2, corrupt.len() - 1] {
            corrupt[index] ^= 1;
            assert!(!verifier.verify(&corrupt));
            assert_eq!(verifier.phase, phase_before_corruption);
            corrupt[index] ^= 1;
        }

        assert!(verifier.verify(&corrupt));
    }
}

/// Shared core for [`spawn_mux_over_rtp_server_with_mss`] and its `_via`
/// variant: binds the listener and hands the accept-loop future to `spawn`
/// (either a [`TestScope`] spawn or the bounded reaper submission).
async fn spawn_mux_over_rtp_server_core<F, Fut>(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    fec_tuning: Option<rtp::FecTuning>,
    mss: usize,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
    mux_session: Option<Arc<MuxSessionProgress>>,
    handle_stream: F,
) -> std::io::Result<std::net::SocketAddr>
where
    F: Fn(mux::StreamReader, mux::StreamWriter) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener =
        rtp::udp::Listener::bind("127.0.0.1:0", rtp::udp::ListenerConfig::default()).await?;
    let addr = listener.local_addr();
    let listener = Arc::new(listener);
    spawn(Box::pin({
        let listener = Arc::clone(&listener);
        async move {
            // Apply the caller's explicit per-connection tuning when given;
            // `None` preserves the process-env default from `AcceptConfig`.
            let with_tuning = |mut config: rtp::udp::AcceptConfig| {
                if let Some(tuning) = fec_tuning {
                    config.fec_tuning = tuning;
                }
                config
            };
            // First (and only) rtp connection.
            // An accept failure is a scenario failure; panic so the root
            // JoinError unwrap crashes the test.
            let accepted = listener
                .accept_without_handshake_with(with_tuning(rtp::udp::AcceptConfig {
                    fec,
                    mss: rtp::udp::MssConfig::Custom(mss),
                    metrics_observer,
                    ..rtp::udp::AcceptConfig::default()
                }))
                .await
                .unwrap();
            // The extra-accept drainer loop keeps driving `udp_listener`'s
            // dispatcher for the server's lifetime: `accept()` both
            // establishes new connections and dispatches packets to existing
            // ones. Without a background accept-loop, the dispatcher stops
            // after the first connection and subsequent datagrams are never
            // forwarded to it, so the reliable layer stalls. The drainer is
            // pinned and selected alongside the session supervisor and
            // handlers below and only ends by panicking on an accept error.
            let drainer = {
                let listener = Arc::clone(&listener);
                let drainer_config = with_tuning(rtp::udp::AcceptConfig {
                    fec,
                    mss: rtp::udp::MssConfig::Custom(mss),
                    // The drainer rejects extra logical sessions;
                    // only the first accepted peer is observed.
                    ..rtp::udp::AcceptConfig::default()
                });
                async move {
                    loop {
                        listener
                            .accept_without_handshake_with(drainer_config.clone())
                            .await
                            .unwrap();
                    }
                }
            };
            tokio::pin!(drainer);

            let read = accepted.read.into_async_read();
            let write = accepted.write.into_async_write();
            // The accepted lane's rtp session supervisor owns the session
            // drivers; poll it from the select loop below so a panicked
            // driver terminates the server instead of being silently dropped.
            let supervisor = accepted.supervisor;
            tokio::pin!(supervisor);

            let config = mux::MuxConfig {
                initiation: mux::Initiation::Server,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: false,
            };
            let mut spawner = tokio::task::JoinSet::new();
            let (_opener, mut accepter) =
                mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);

            // Per-stream handlers owned by this accept loop's scope. The loop
            // below polls acceptance, per-stream handler completion, and mux
            // session completion together, so a panicked handler or session
            // surfaces immediately instead of only once the accept loop ends;
            // any still-running handlers are aborted when the local JoinSet
            // drops at scope end.
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    () = &mut supervisor => { break; } // rtp session drivers exited; stop accepting
                    () = &mut drainer => {
                        // The required drainer ended early; panic instead of
                        // silently ending the server.
                        panic!("accept drainer finished before the server scenario completed");
                    }
                    accepted = accepter.accept() => {
                        match accepted {
                            Ok((stream_read, stream_write)) => {
                                let handle_stream = &handle_stream;
                                handlers.spawn(handle_stream(stream_read, stream_write));
                            }
                            Err(_) => break, // peer closed; stop accepting
                        }
                    }
                    Some(joined) = handlers.join_next(), if !handlers.is_empty() => {
                        // A per-stream handler ended: unwrap so a panic
                        // surfaces now; a normal completion just ends it.
                        joined.unwrap();
                    }
                    Some(joined) = spawner.join_next() => {
                        // The mux session ended: unwrap (re-raising a panic)
                        // and stop accepting; record the terminal error for
                        // the diagnostic latch when one is attached.
                        let error = joined.unwrap();
                        if let Some(progress) = &mux_session {
                            progress.record_error(&error);
                        }
                        break;
                    }
                }
            }
            // Drain any remaining handler/supervision joins so panics surface.
            while let Some(result) = handlers.join_next().await {
                result.unwrap();
            }
            // Drain the mux supervision tasks, unwrapping so panics surface.
            while let Some(result) = spawner.join_next().await {
                result.unwrap();
            }
        }
    }));
    Ok(addr)
}

/// Spawn an `rtp` server that accepts one connection and runs a `mux` server
/// on top of the resulting reliable byte stream. Each accepted mux stream is
/// handed to `handle_stream`, which owns its read/write halves. Returns the
/// rtp server's listening address.
///
/// `mss` is passed to the RTP accept helpers; use [`rtp::udp::NO_FEC_MSS`] for
/// the default size.
///
/// The listener is wrapped in an [`Arc`] so a background `accept()`-loop can
/// keep driving `udp_listener`'s dispatcher for the server's lifetime:
/// `accept()` both establishes new connections *and* dispatches packets to
/// existing ones (via `try_send` to their per-conn channels). Without a
/// background accept-loop, the dispatcher stops after the first connection
/// and subsequent datagrams are never forwarded to it, so the reliable
/// layer stalls. This is required by `udp_listener`'s docs ("You still need
/// to put `accept()` in a loop to drive the packet dispatch among the
/// sub-connections").
pub async fn spawn_mux_over_rtp_server_with_mss<F, Fut>(
    tasks: &mut TestScope,
    fec: bool,
    mss: usize,
    handle_stream: F,
) -> std::io::Result<std::net::SocketAddr>
where
    F: Fn(mux::StreamReader, mux::StreamWriter) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    spawn_mux_over_rtp_server_core(
        |fut| tasks.spawn(fut),
        fec,
        None,
        mss,
        None,
        None,
        handle_stream,
    )
    .await
}

/// [`spawn_mux_over_rtp_server_with_mss`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_over_rtp_server_with_mss_via<F, Fut>(
    tx: &TestTaskSubmitter,
    fec: bool,
    mss: usize,
    handle_stream: F,
) -> std::io::Result<std::net::SocketAddr>
where
    F: Fn(mux::StreamReader, mux::StreamWriter) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    spawn_mux_over_rtp_server_core(
        |fut| submit_test_task(tx, fut),
        fec,
        None,
        mss,
        None,
        None,
        handle_stream,
    )
    .await
}

/// Shared core for [`spawn_mux_over_rtp_echo_server_with_mss`] and its `_via`
/// variant: spawns the mux-over-RTP server with an echo handler via `spawn`
/// (either a [`TestScope`] spawn or the bounded reaper submission).
async fn spawn_mux_over_rtp_echo_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    mss: usize,
) -> std::io::Result<std::net::SocketAddr> {
    spawn_mux_over_rtp_server_core(
        spawn,
        fec,
        None,
        mss,
        None,
        None,
        |mut stream_read, mut stream_write| async move {
            let mut buf = vec![0u8; 8 * 1024];
            loop {
                match stream_read.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if stream_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = stream_write.shutdown();
        },
    )
    .await
}

/// Spawn an `rtp` server that accepts one connection and runs a `mux` server
/// on top of the resulting reliable byte stream. Each accepted mux stream is
/// echoed back. Returns the rtp server's listening address.
pub async fn spawn_mux_over_rtp_echo_server_with_mss(
    tasks: &mut TestScope,
    fec: bool,
    mss: usize,
) -> std::io::Result<std::net::SocketAddr> {
    spawn_mux_over_rtp_echo_server_core(|fut| tasks.spawn(fut), fec, mss).await
}

/// [`spawn_mux_over_rtp_echo_server_with_mss`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_over_rtp_echo_server_with_mss_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    mss: usize,
) -> std::io::Result<std::net::SocketAddr> {
    spawn_mux_over_rtp_echo_server_core(|fut| submit_test_task(tx, fut), fec, mss).await
}

/// Spawn a mux-over-RTP echo server using the default MSS.
pub async fn spawn_mux_over_rtp_echo_server(
    tasks: &mut TestScope,
    fec: bool,
) -> std::io::Result<std::net::SocketAddr> {
    spawn_mux_over_rtp_echo_server_with_mss(tasks, fec, rtp::udp::NO_FEC_MSS).await
}

/// [`spawn_mux_over_rtp_echo_server`] through the bounded task-submission
/// handle, for use inside [`TestScope::run`] bodies where `&mut TestScope`
/// is unavailable.
pub async fn spawn_mux_over_rtp_echo_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
) -> std::io::Result<std::net::SocketAddr> {
    spawn_mux_over_rtp_echo_server_with_mss_via(tx, fec, rtp::udp::NO_FEC_MSS).await
}

/// Shared core for [`spawn_mux_over_rtp_sink_server_with_mss`] and its `_via`
/// variant: spawns the mux-over-RTP server with a sink handler via `spawn`
/// (either a [`TestScope`] spawn or the bounded reaper submission).
async fn spawn_mux_over_rtp_sink_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<Vec<u8>>)> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let addr = spawn_mux_over_rtp_server_core(
        spawn,
        fec,
        None,
        mss,
        None,
        None,
        move |mut stream_read, mut stream_write| {
            let tx = tx.clone();
            async move {
                let mut buf = Vec::new();
                let read_ok = stream_read.read_to_end(&mut buf).await.is_ok();
                if read_ok || !buf.is_empty() {
                    let _ = tx.send(buf).await;
                }
                let _ = stream_write.shutdown();
            }
        },
    )
    .await?;
    Ok((addr, rx))
}

/// Spawn an `rtp` server that accepts one connection and runs a `mux` server
/// on top of the resulting reliable byte stream. Each accepted mux stream is
/// read to EOF into a `Vec<u8>` and sent on the returned channel (capacity
/// 16) if the read succeeded or the buffer is non-empty, then the write half
/// is shut down. Returns the rtp server's listening address and the receiver
/// for completed payloads.
pub async fn spawn_mux_over_rtp_sink_server_with_mss(
    tasks: &mut TestScope,
    fec: bool,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<Vec<u8>>)> {
    spawn_mux_over_rtp_sink_server_core(|fut| tasks.spawn(fut), fec, mss).await
}

/// [`spawn_mux_over_rtp_sink_server_with_mss`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_over_rtp_sink_server_with_mss_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<Vec<u8>>)> {
    spawn_mux_over_rtp_sink_server_core(|fut| submit_test_task(tx, fut), fec, mss).await
}

/// Spawn a mux-over-RTP sink server using the default MSS.
pub async fn spawn_mux_over_rtp_sink_server(
    tasks: &mut TestScope,
    fec: bool,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<Vec<u8>>)> {
    spawn_mux_over_rtp_sink_server_with_mss(tasks, fec, rtp::udp::NO_FEC_MSS).await
}

/// [`spawn_mux_over_rtp_sink_server`] through the bounded task-submission
/// handle, for use inside [`TestScope::run`] bodies where `&mut TestScope`
/// is unavailable.
pub async fn spawn_mux_over_rtp_sink_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<Vec<u8>>)> {
    spawn_mux_over_rtp_sink_server_with_mss_via(tx, fec, rtp::udp::NO_FEC_MSS).await
}

/// Spawn a mux-over-RTP server that accepts one connection and parses a simple
/// per-message framing on each accepted mux stream: each record carries a
/// 4-byte little-endian total frame length (including the 4-byte length itself
/// and the 8-byte timestamp trailer), followed by the payload, followed by an
/// 8-byte little-endian send timestamp in microseconds since `base`.
///
/// The server records the one-way latency of every received message as
/// `now_us.saturating_sub(sent_us) / 1000.0` milliseconds and pushes the sample
/// into the returned channel. The read loop continues until the peer closes.
///
/// Using `mux` over RTP is important for sparse-message streams: `mux` emits
/// periodic heartbeat frames that keep the underlying RTP connection alive and
/// acknowledged, avoiding the RTP layer's proactive broken-pipe heuristic that
/// fires on quiet unidirectional streams.
pub async fn spawn_mux_msg_latency_sink(
    tasks: &mut TestScope,
    fec: bool,
    base: Instant,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<f64>)> {
    spawn_mux_msg_latency_sink_with_mss(tasks, fec, base, rtp::udp::NO_FEC_MSS).await
}

/// [`spawn_mux_msg_latency_sink`] through the bounded task-submission handle,
/// for use inside [`TestScope::run`] bodies where `&mut TestScope` is
/// unavailable.
pub async fn spawn_mux_msg_latency_sink_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<f64>)> {
    spawn_mux_msg_latency_sink_with_mss_via(tx, fec, base, rtp::udp::NO_FEC_MSS).await
}

/// Shared core for [`spawn_mux_msg_latency_sink_with_mss`] and its `_via`
/// variant: spawns the mux-over-RTP server with a latency-sink handler via
/// `spawn` (either a [`TestScope`] spawn or the bounded reaper submission).
async fn spawn_mux_msg_latency_sink_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    base: Instant,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<f64>)> {
    let (tx, rx) = tokio::sync::mpsc::channel(LATENCY_SAMPLE_CAPACITY);
    let addr = spawn_mux_over_rtp_server_core(
        spawn,
        fec,
        None,
        mss,
        None,
        None,
        move |mut stream_read, mut stream_write| {
            let tx = tx.clone();
            async move {
                let mut buf = vec![0u8; 64 * 1024];
                let mut offset = 0usize;
                while let Ok(n) = stream_read.read(&mut buf[offset..]).await {
                    if n == 0 {
                        break;
                    }
                    offset += n;
                    // Parse complete frames from the accumulated buffer.
                    loop {
                        if offset < 4 {
                            break;
                        }
                        let frame_len =
                            u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                        if frame_len < 12 {
                            // Invalid frame; drop the whole connection.
                            break;
                        }
                        if offset < frame_len {
                            break;
                        }
                        let payload_end = frame_len - 8;
                        let sent_us = u64::from_le_bytes([
                            buf[payload_end],
                            buf[payload_end + 1],
                            buf[payload_end + 2],
                            buf[payload_end + 3],
                            buf[payload_end + 4],
                            buf[payload_end + 5],
                            buf[payload_end + 6],
                            buf[payload_end + 7],
                        ]);
                        let now_us = base.elapsed().as_micros() as u64;
                        let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
                        if !try_send_observation(&tx, latency_ms, "latency sample") {
                            break;
                        }
                        buf.copy_within(frame_len..offset, 0);
                        offset -= frame_len;
                    }
                }
                let _ = stream_write.shutdown();
            }
        },
    )
    .await?;
    Ok((addr, rx))
}

/// [`spawn_mux_msg_latency_sink`] with a custom RTP MSS.
pub async fn spawn_mux_msg_latency_sink_with_mss(
    tasks: &mut TestScope,
    fec: bool,
    base: Instant,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<f64>)> {
    spawn_mux_msg_latency_sink_core(|fut| tasks.spawn(fut), fec, base, mss).await
}

/// [`spawn_mux_msg_latency_sink_with_mss`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_msg_latency_sink_with_mss_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, tokio::sync::mpsc::Receiver<f64>)> {
    spawn_mux_msg_latency_sink_core(|fut| submit_test_task(tx, fut), fec, base, mss).await
}

/// Send timestamped messages to a mux-stream latency sink.
///
/// The per-message framing (`[4-byte LE total frame length][payload][8-byte
/// LE send timestamp in micros since `base`]`) has exactly **one authority**:
/// the rtp layer kit's encoder
/// ([`rtp::testkit::rtp::send_timestamped_messages`]), whose sink decoders
/// (both this kit's `spawn_mux_msg_latency_sink*` and the rtp kit's
/// `spawn_rtp_msg_latency_sink*`) read the same layout. This module
/// re-exports that encoder — the function is generic over any byte stream, so
/// the single authority also drives the mux stream-framed sparse pings — and
/// the mux layer is a view of the protocol, never a second copy that could
/// drift.
pub use rtp::testkit::rtp::send_timestamped_messages;
/// Open a mux stream, write `payload`, shut the stream down, and read the
/// full echo back until EOF.
///
/// The write and the read run concurrently with `tokio::join!` instead of
/// write-then-read: once the payload exceeds the mux flow-control window a
/// sequential write blocks forever waiting for the peer to drain (which it
/// can only do by echoing), deadlocking the stream.
pub async fn mux_echo_round_trip(opener: &mux::StreamOpener, payload: &[u8]) -> Vec<u8> {
    let (mut stream_read, mut stream_write) = opener.open().await.unwrap();
    let write_fut = async {
        stream_write.write_all(payload).await.unwrap();
        stream_write.shutdown().unwrap();
    };
    let read_fut = async {
        let mut got = Vec::new();
        let mut buf = vec![0u8; 8 * 1024];
        loop {
            match stream_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(e) => panic!("mux stream read failed: {e:?}"),
            }
        }
        got
    };
    let (_, got) = tokio::join!(write_fut, read_fut);
    got
}

/// Open a mux stream, write `payload`, shut the stream down, read the full
/// echo back, and return `(received, elapsed)` where `elapsed` is measured
/// from just before the write to the completion of the read. Used by perf
/// and latency tests to print throughput with `--nocapture`.
///
/// The write and the read run concurrently with `tokio::join!` instead of
/// write-then-read: once the payload exceeds the mux flow-control window a
/// sequential write blocks forever waiting for the peer to drain (which it
/// can only do by echoing), deadlocking the stream.
pub async fn mux_timed_echo_round_trip(
    opener: &mux::StreamOpener,
    payload: &[u8],
) -> (Vec<u8>, Duration) {
    let (mut stream_read, mut stream_write) = opener.open().await.unwrap();
    let start = Instant::now();
    let write_fut = async {
        stream_write.write_all(payload).await.unwrap();
        stream_write.shutdown().unwrap();
    };
    let read_fut = async {
        let mut got = Vec::new();
        let mut buf = vec![0u8; 8 * 1024];
        loop {
            match stream_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(e) => panic!("mux stream read failed: {e:?}"),
            }
        }
        got
    };
    let (_, got) = tokio::join!(write_fut, read_fut);
    (got, start.elapsed())
}

/// Open a mux stream, write `payload`, shut the write half down, then read
/// to EOF to wait for the peer to finish draining. Returns the elapsed time
/// measured from just before the write to the completion of the peer-EOF
/// read — the wall-clock time the peer needed to receive the whole payload.
///
/// `ErrorKind::BrokenPipe` during the peer-EOF wait is tolerated: `rtp`'s
/// broken-pipe heuristic can fire after a completed upload (the ACK path
/// stalls once the peer has no more data to send), and delivery is already
/// verified by the sink-side equality assert. In that case the returned
/// duration may undercount delivery, so a warning is printed. Any other
/// read error panics.
pub async fn mux_send_payload(opener: &mux::StreamOpener, payload: &[u8]) -> Duration {
    mux_send_repeated(opener, payload, 1).await
}

pub async fn mux_send_repeated(
    opener: &mux::StreamOpener,
    chunk: &[u8],
    repeat: usize,
) -> Duration {
    let (mut stream_read, mut stream_write) = opener.open().await.unwrap();
    let start = Instant::now();
    for _ in 0..repeat {
        stream_write.write_all(chunk).await.unwrap();
    }
    stream_write.shutdown().unwrap();
    let mut sink = Vec::new();
    match stream_read.read_to_end(&mut sink).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
            eprintln!(
                "[mux_send_payload] BrokenPipe during peer-EOF wait; \
                 duration may undercount delivery"
            );
        }
        Err(e) => panic!("mux_send_payload peer-EOF read failed: {e:?}"),
    }
    start.elapsed()
}

/// Shared core for [spawn_mux_over_rtp_counting_sink_server] and its _via
/// variant: spawns the mux-over-RTP server with a counting-sink handler via
/// spawn (either a [TestScope] spawn or the bounded reaper submission).
async fn spawn_mux_over_rtp_counting_sink_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    mss: usize,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
) -> std::io::Result<(std::net::SocketAddr, Arc<SinkProgress>)> {
    let progress = Arc::new(SinkProgress::new());
    let addr = spawn_mux_over_rtp_server_core(
        spawn,
        fec,
        None,
        mss,
        metrics_observer,
        Some(progress.mux_session()),
        {
            let progress = Arc::clone(&progress);
            move |mut stream_read, mut stream_write| {
                let progress = Arc::clone(&progress);
                async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut verifier = PayloadPatternVerifier::default();
                    let outcome = loop {
                        match stream_read.read(&mut buf).await {
                            Ok(0) => break SinkReadOutcome::CleanEof,
                            Ok(n) => {
                                if progress.is_corrupt() {
                                    continue;
                                }
                                if !verifier.verify(&buf[..n]) {
                                    progress.corrupt.store(true, Ordering::Relaxed);
                                } else {
                                    progress.delivered.fetch_add(n as u64, Ordering::Relaxed);
                                }
                            }
                            Err(error) => break SinkReadOutcome::ReadError(error.kind()),
                        }
                    };
                    progress.record_read_outcome(outcome);
                    let _ = stream_write.shutdown();
                }
            }
        },
    )
    .await?;
    Ok((addr, progress))
}

/// Spawn an `rtp` server that accepts one connection and runs a `mux` server
/// on top of it. Each accepted mux stream is read chunk-by-chunk into a 64 KiB
/// buffer and verified against the deterministic payload pattern. Verified
/// bytes are atomically added to the returned [`SinkProgress::delivered`];
/// a mismatch sets [`SinkProgress::corrupt`] and stops counting that stream.
///
/// This sink is intentionally kept mid-flight: it does *not* buffer the full
/// payload or read to EOF, so a snapshot of `delivered_bytes()` taken while
/// the transfer is still alive reflects true goodput without an inflated
/// delivery snapshot.
pub async fn spawn_mux_over_rtp_counting_sink_server(
    tasks: &mut TestScope,
    fec: bool,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, Arc<SinkProgress>)> {
    spawn_mux_over_rtp_counting_sink_server_core(|fut| tasks.spawn(fut), fec, mss, None).await
}

/// [`spawn_mux_over_rtp_counting_sink_server`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_over_rtp_counting_sink_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    mss: usize,
) -> std::io::Result<(std::net::SocketAddr, Arc<SinkProgress>)> {
    spawn_mux_over_rtp_counting_sink_server_core(|fut| submit_test_task(tx, fut), fec, mss, None)
        .await
}

/// [`spawn_mux_over_rtp_counting_sink_server_via`] with an optional typed RTP
/// transport observer: the observed counting sink is the only core caller
/// that passes a non-`None` observer.
pub async fn spawn_mux_over_rtp_counting_sink_server_observed_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    mss: usize,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
) -> std::io::Result<(std::net::SocketAddr, Arc<SinkProgress>)> {
    spawn_mux_over_rtp_counting_sink_server_core(
        |fut| submit_test_task(tx, fut),
        fec,
        mss,
        metrics_observer,
    )
    .await
}

/// Convenience wrapper using the default RTP MSS.
pub async fn spawn_mux_over_rtp_counting_sink_server_default(
    tasks: &mut TestScope,
    fec: bool,
) -> std::io::Result<(std::net::SocketAddr, Arc<SinkProgress>)> {
    spawn_mux_over_rtp_counting_sink_server(tasks, fec, rtp::udp::NO_FEC_MSS).await
}

/// [`spawn_mux_over_rtp_counting_sink_server_default`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_over_rtp_counting_sink_server_default_via(
    tx: &TestTaskSubmitter,
    fec: bool,
) -> std::io::Result<(std::net::SocketAddr, Arc<SinkProgress>)> {
    spawn_mux_over_rtp_counting_sink_server_via(tx, fec, rtp::udp::NO_FEC_MSS).await
}

/// Shared core for [spawn_mux_latency_bulk_server] and its _via
/// variant: spawns the mux-over-RTP server with a tag-classifying
/// handler via spawn (either a [TestScope] spawn or the bounded reaper submission).
async fn spawn_mux_latency_bulk_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_latency_bulk_server_core_with_fec_tuning(spawn, fec, None, base).await
}

/// [`spawn_mux_latency_bulk_server_core`] with an explicit per-connection
/// [`rtp::FecTuning`] threaded to the accept side (`None` keeps the
/// process-env default), so a scenario can A/B the stock vs prompt-parity
/// tuning on both peers.
async fn spawn_mux_latency_bulk_server_core_with_fec_tuning(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    fec_tuning: Option<rtp::FecTuning>,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(LATENCY_SAMPLE_CAPACITY);
    let bulk_delivered = Arc::new(AtomicU64::new(0));
    let addr =
        spawn_mux_over_rtp_server_core(spawn, fec, fec_tuning, rtp::udp::NO_FEC_MSS, None, None, {
            let tx = tx.clone();
            let bulk_delivered = Arc::clone(&bulk_delivered);
            move |mut stream_read, mut stream_write| {
                let tx = tx.clone();
                let bulk_delivered = Arc::clone(&bulk_delivered);
                async move {
                    let mut tag = [0u8; 1];
                    let n = match stream_read.read(&mut tag).await {
                        Ok(n) => n,
                        Err(_) => {
                            let _ = stream_write.shutdown();
                            return;
                        }
                    };
                    if n == 0 {
                        let _ = stream_write.shutdown();
                        return;
                    }
                    // Timestamped latency frame parser.
                    if tag[0] == b'L' {
                        let mut buf = vec![0u8; 64 * 1024];
                        let mut offset = 0usize;
                        while let Ok(n) = stream_read.read(&mut buf[offset..]).await {
                            if n == 0 {
                                break;
                            }
                            offset += n;
                            loop {
                                if offset < 4 {
                                    break;
                                }
                                let frame_len =
                                    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                                if frame_len < 12 {
                                    break;
                                }
                                if offset < frame_len {
                                    break;
                                }
                                let payload_end = frame_len - 8;
                                let sent_us = u64::from_le_bytes([
                                    buf[payload_end],
                                    buf[payload_end + 1],
                                    buf[payload_end + 2],
                                    buf[payload_end + 3],
                                    buf[payload_end + 4],
                                    buf[payload_end + 5],
                                    buf[payload_end + 6],
                                    buf[payload_end + 7],
                                ]);
                                let now_us = base.elapsed().as_micros() as u64;
                                let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
                                if !try_send_observation(&tx, latency_ms, "Latency sample") {
                                    break;
                                }
                                buf.copy_within(frame_len..offset, 0);
                                offset -= frame_len;
                            }
                        }
                    } else {
                        // Bulk byte sink: verify the deterministic pattern and
                        // count verified bytes. The tag byte itself is excluded.
                        let mut buf = vec![0u8; 64 * 1024];
                        let mut verifier = PayloadPatternVerifier::default();
                        while let Ok(n) = stream_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            if verifier.verify(&buf[..n]) {
                                bulk_delivered.fetch_add(n as u64, Ordering::Relaxed);
                            }
                        }
                    }
                    let _ = stream_write.shutdown();
                }
            }
        })
        .await?;
    Ok((addr, rx, bulk_delivered))
}

/// Spawn a mux-over-RTP server that accepts one connection and classifies each
/// accepted mux stream by its first byte.
///
/// * `b'L'`: timestamped latency frames (`[4 LE total len][payload][8 LE
///   micros since base]`). One-way latency in milliseconds is pushed into the
///   returned unbounded channel.
/// * any other byte: deterministic bulk byte sink. Bytes after the tag are
///   verified against the `(offset % 251)` pattern and counted in the returned
///   [`AtomicU64`]; they are then discarded.
///
/// This combined server lets HOL and contested-latency scenarios open an
/// interactive ping stream and a competing bulk sink stream on the same mux
/// connection while using a single server address.
pub async fn spawn_mux_latency_bulk_server(
    tasks: &mut TestScope,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_latency_bulk_server_core(|fut| tasks.spawn(fut), fec, base).await
}

/// [`spawn_mux_latency_bulk_server`] through the bounded task-submission
/// handle, for use inside [`TestScope::run`] bodies where `&mut TestScope`
/// is unavailable.
pub async fn spawn_mux_latency_bulk_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_latency_bulk_server_core(|fut| submit_test_task(tx, fut), fec, base).await
}

/// [`spawn_mux_latency_bulk_server_via`] with an explicit per-connection
/// [`rtp::FecTuning`] applied on the accept side. The connecting peer must use
/// the same `fec` flag and tuning (no in-band negotiation), so this pairs with
/// `rtp_connect_with_mss_and_fec_tuning_via`.
pub async fn spawn_mux_latency_bulk_server_with_fec_tuning_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
    fec_tuning: rtp::FecTuning,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_latency_bulk_server_core_with_fec_tuning(
        |fut| submit_test_task(tx, fut),
        fec,
        Some(fec_tuning),
        base,
    )
    .await
}

/// Shared core for [spawn_mux_sized_latency_bulk_server] and its _via
/// variant: spawns the mux-over-RTP server with a tag-classifying
/// handler via spawn (either a [TestScope] spawn or the bounded reaper submission).
async fn spawn_mux_sized_latency_bulk_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    base: Instant,
    mss: usize,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(LATENCY_SAMPLE_CAPACITY);
    let bulk_delivered = Arc::new(AtomicU64::new(0));
    let addr = spawn_mux_over_rtp_server_core(spawn, fec, None, mss, None, None, {
        let tx = tx.clone();
        let bulk_delivered = Arc::clone(&bulk_delivered);
        move |mut stream_read, mut stream_write| {
            let tx = tx.clone();
            let bulk_delivered = Arc::clone(&bulk_delivered);
            async move {
                let mut tag = [0u8; 1];
                let n = match stream_read.read(&mut tag).await {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = stream_write.shutdown();
                        return;
                    }
                };
                if n == 0 {
                    let _ = stream_write.shutdown();
                    return;
                }
                // Timestamped latency frame parser.
                if tag[0] == b'L' {
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut offset = 0usize;
                    while let Ok(n) = stream_read.read(&mut buf[offset..]).await {
                        if n == 0 {
                            break;
                        }
                        offset += n;
                        loop {
                            if offset < 4 {
                                break;
                            }
                            let frame_len =
                                u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                            if frame_len < 12 {
                                break;
                            }
                            if offset < frame_len {
                                break;
                            }
                            let payload_end = frame_len - 8;
                            let sent_us = u64::from_le_bytes([
                                buf[payload_end],
                                buf[payload_end + 1],
                                buf[payload_end + 2],
                                buf[payload_end + 3],
                                buf[payload_end + 4],
                                buf[payload_end + 5],
                                buf[payload_end + 6],
                                buf[payload_end + 7],
                            ]);
                            let now_us = base.elapsed().as_micros() as u64;
                            let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
                            if !try_send_observation(&tx, latency_ms, "Latency sample") {
                                break;
                            }
                            buf.copy_within(frame_len..offset, 0);
                            offset -= frame_len;
                        }
                    }
                } else {
                    // Bulk byte sink: verify the deterministic pattern and
                    // count verified bytes. The tag byte itself is excluded.
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut verifier = PayloadPatternVerifier::default();
                    while let Ok(n) = stream_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        if verifier.verify(&buf[..n]) {
                            bulk_delivered.fetch_add(n as u64, Ordering::Relaxed);
                        }
                    }
                }
                let _ = stream_write.shutdown();
            }
        }
    })
    .await?;
    Ok((addr, rx, bulk_delivered))
}

/// Like [`spawn_mux_latency_bulk_server`] but with a custom RTP MSS.
pub async fn spawn_mux_sized_latency_bulk_server(
    tasks: &mut TestScope,
    fec: bool,
    base: Instant,
    mss: usize,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_sized_latency_bulk_server_core(|fut| tasks.spawn(fut), fec, base, mss).await
}

/// [`spawn_mux_sized_latency_bulk_server`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_sized_latency_bulk_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
    mss: usize,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_sized_latency_bulk_server_core(|fut| submit_test_task(tx, fut), fec, base, mss).await
}

/// Shared core for [spawn_mux_gaming_latency_bulk_server] and its _via
/// variant: spawns the mux-over-RTP server with a gaming tag-classifying
/// handler via spawn (either a [TestScope] spawn or the bounded reaper
/// submission).
async fn spawn_mux_gaming_latency_bulk_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(LATENCY_SAMPLE_CAPACITY);
    let bulk_delivered = Arc::new(AtomicU64::new(0));
    let addr =
        spawn_mux_over_rtp_server_core(spawn, fec, None, rtp::udp::NO_FEC_MSS, None, None, {
            let tx = tx.clone();
            let bulk_delivered = Arc::clone(&bulk_delivered);
            move |mut stream_read, mut stream_write| {
                let tx = tx.clone();
                let bulk_delivered = Arc::clone(&bulk_delivered);
                async move {
                    let mut tag = [0u8; 1];
                    let n = match stream_read.read(&mut tag).await {
                        Ok(n) => n,
                        Err(_) => {
                            let _ = stream_write.shutdown();
                            return;
                        }
                    };
                    if n == 0 {
                        let _ = stream_write.shutdown();
                        return;
                    }
                    if tag[0] == b'G' {
                        const SYNC_BYTES: usize = 8 * 1024;
                        let mut remaining = SYNC_BYTES;
                        let mut buf = vec![0u8; 64 * 1024];
                        while remaining > 0 {
                            let to_read = remaining.min(buf.len());
                            match stream_read.read(&mut buf[..to_read]).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => remaining -= n,
                            }
                        }
                        let mut offset = 0usize;
                        while let Ok(n) = stream_read.read(&mut buf[offset..]).await {
                            if n == 0 {
                                break;
                            }
                            offset += n;
                            loop {
                                if offset < 4 {
                                    break;
                                }
                                let frame_len =
                                    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                                if frame_len < 12 || offset < frame_len {
                                    break;
                                }
                                let payload_end = frame_len - 8;
                                let sent_us = u64::from_le_bytes([
                                    buf[payload_end],
                                    buf[payload_end + 1],
                                    buf[payload_end + 2],
                                    buf[payload_end + 3],
                                    buf[payload_end + 4],
                                    buf[payload_end + 5],
                                    buf[payload_end + 6],
                                    buf[payload_end + 7],
                                ]);
                                let now_us = base.elapsed().as_micros() as u64;
                                let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
                                if !try_send_observation(&tx, latency_ms, "Latency sample") {
                                    break;
                                }
                                buf.copy_within(frame_len..offset, 0);
                                offset -= frame_len;
                            }
                        }
                    } else {
                        // Bulk byte sink: verify the deterministic pattern and
                        // count verified bytes. The tag byte itself is excluded.
                        let mut buf = vec![0u8; 64 * 1024];
                        let mut verifier = PayloadPatternVerifier::default();
                        while let Ok(n) = stream_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            if verifier.verify(&buf[..n]) {
                                bulk_delivered.fetch_add(n as u64, Ordering::Relaxed);
                            }
                        }
                    }
                    let _ = stream_write.shutdown();
                }
            }
        })
        .await?;
    Ok((addr, rx, bulk_delivered))
}

/// Single‑mux gaming server: the first stream tagged `b'G'` is the game
/// stream (3 MiB state-sync followed by 200 B delta frames); all other
/// streams are bulk.
pub async fn spawn_mux_gaming_latency_bulk_server(
    tasks: &mut TestScope,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_gaming_latency_bulk_server_core(|fut| tasks.spawn(fut), fec, base).await
}

/// [`spawn_mux_gaming_latency_bulk_server`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_gaming_latency_bulk_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    Arc<AtomicU64>,
)> {
    spawn_mux_gaming_latency_bulk_server_core(|fut| submit_test_task(tx, fut), fec, base).await
}

/// Shared core for [spawn_mux_frame_delivery_latency_bulk_server] and its _via
/// variant: binds the listener and hands the accept-loop future to
/// spawn (either a [TestScope] spawn or the bounded reaper submission).
async fn spawn_mux_frame_delivery_latency_bulk_server_core(
    spawn: impl FnOnce(TestTask),
    fec: bool,
    base: Instant,
    frame_mode: FrameMode,
    fec_tuning: FecTuning,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(LATENCY_SAMPLE_CAPACITY);
    let bulk_delivered = Arc::new(AtomicU64::new(0));
    let listener = Arc::new(
        rtp::udp::Listener::bind("127.0.0.1:0", rtp::udp::ListenerConfig::default()).await?,
    );
    let addr = listener.local_addr();

    let fd = frame_mode;
    let listener_accept = Arc::clone(&listener);
    let bulk_delivered_for_server = Arc::clone(&bulk_delivered);
    spawn(Box::pin(async move {
        // An accept failure is a scenario failure; panic so the root
        // JoinError unwrap crashes the test.
        let accepted = listener_accept
            .accept_without_handshake_with(rtp::udp::AcceptConfig {
                fec,
                mss: rtp::udp::MssConfig::Custom(rtp::udp::NO_FEC_MSS),
                fec_tuning,
                frame_delivery: fd,
                ..rtp::udp::AcceptConfig::default()
            })
            .await
            .unwrap();
        // The extra-accept drainer loop keeps driving 'udp_listener's
        // dispatcher for the server's lifetime: accept() both establishes
        // new connections and dispatches packets to existing ones. With a
        // background accept-loop, the dispatcher stops after the first
        // connection and subsequent datagrams are never forwarded to it, so
        // the reliable layer stalls. The drainer is pinned and selected
        // alongside the session supervisor and handlers below and only ends
        // by panicking on an accept error.
        let drainer = {
            let listener = Arc::clone(&listener);
            async move {
                loop {
                    listener
                        .accept_without_handshake_with(rtp::udp::AcceptConfig {
                            fec,
                            mss: rtp::udp::MssConfig::Custom(rtp::udp::NO_FEC_MSS),
                            fec_tuning,
                            frame_delivery: fd,
                            ..rtp::udp::AcceptConfig::default()
                        })
                        .await
                        .unwrap();
                }
            }
        };
        tokio::pin!(drainer);

        let read = accepted.read.into_async_read();
        let write = accepted.write.into_async_write();

        // The accepted lane's rtp session supervisor owns the session
        // drivers; poll it from the select loop below so a panicked driver
        // terminates the server instead of being silently dropped.
        let supervisor = accepted.supervisor;
        tokio::pin!(supervisor);

        let config = mux::MuxConfig {
            heartbeat_interval: Duration::from_secs(5),
            initiation: mux::Initiation::Server,
            frame_reassembly: true,
        };
        let mut spawner = JoinSet::new();
        let (_opener, mut accepter) =
            mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);

        // Per-stream sink tasks owned by this accepter. The select
        // below polls acceptance, per-stream handler completion, and mux
        // session completion together, so a panicked handler or session
        // surfaces immediately instead of only once the accept loop ends;
        // any still-running handlers are aborted when this JoinSet drops at
        // scope end.
        let mut handlers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                () = &mut supervisor => { break; } // rtp session drivers exited; stop accepting
                () = &mut drainer => {
                    // The required drainer ended early; panic instead of
                    // silently ending the server.
                    panic!("accept drainer finished before the server scenario completed");
                }
                accepted = accepter.accept() => {
                    match accepted {
                        Ok((mut reader, mut writer)) => {
                            let tx = tx.clone();
                            let bulk = Arc::clone(&bulk_delivered_for_server);
                            handlers.spawn(async move {
                                let mut tag = [0u8; 1];
                                if reader.read_exact(&mut tag).await.is_err() {
                                    let _ = writer.shutdown();
                                    return;
                                }
                                if tag[0] != b'B' {
                                    let mut buf = vec![0u8; 64 * 1024];
                                    let mut offset = 0usize;
                                    while let Ok(n) = reader.read(&mut buf[offset..]).await {
                                        if n == 0 {
                                            break;
                                        }
                                        offset += n;
                                        loop {
                                            if offset < 4 {
                                                break;
                                            }
                                            let frame_len =
                                                u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                                            if frame_len < 12 {
                                                break;
                                            }
                                            if offset < frame_len {
                                                break;
                                            }
                                            let payload_end = frame_len - 8;
                                            let sent_us = u64::from_le_bytes([
                                                buf[payload_end],
                                                buf[payload_end + 1],
                                                buf[payload_end + 2],
                                                buf[payload_end + 3],
                                                buf[payload_end + 4],
                                                buf[payload_end + 5],
                                                buf[payload_end + 6],
                                                buf[payload_end + 7],
                                            ]);
                                            let now_us = base.elapsed().as_micros() as u64;
                                            let latency_ms =
                                                now_us.saturating_sub(sent_us) as f64 / 1000.0;
                                            if !try_send_observation(&tx, (tag[0], latency_ms), "Latency sample") {
                                                break;
                                            }
                                            buf.copy_within(frame_len..offset, 0);
                                            offset -= frame_len;
                                        }
                                    }
                                } else {
                                    // Bulk sink: count every delivered byte.
                                    // The byte-order-strict
                                    // `PayloadPatternVerifier` desyncs once RTP
                                    // frame-reorder fast-forwards a bulk frame
                                    // past a hole, which made the frame-reorder
                                    // arms look artificially light. The
                                    // delivered-byte total is order-independent
                                    // and keeps the offered load matched across
                                    // the strict and reorder arms.
                                    let mut buf = vec![0u8; 64 * 1024];
                                    loop {
                                        match reader.read(&mut buf).await {
                                            Ok(0) | Err(_) => break,
                                            Ok(n) => {
                                                bulk.fetch_add(n as u64, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                }
                                let _ = writer.shutdown();
                            });
                        }
                        Err(_) => break, // peer closed; stop accepting
                    }
                }
                Some(joined) = handlers.join_next(), if !handlers.is_empty() => {
                    // A per-stream handler ended: unwrap so a panic surfaces now.
                    joined.unwrap();
                }
                Some(joined) = spawner.join_next() => {
                    // Session ended: unwrap (re-raising a panic) and
                    // stop accepting.
                    joined.unwrap();
                    break;
                }
            }
        }
        // Drain any remaining handler/supervision joins so panics surface.
        while let Some(result) = handlers.join_next().await {
            result.unwrap();
        }
        // Drain the mux supervision tasks, unwrapping so panics surface.
        while let Some(result) = spawner.join_next().await {
            result.unwrap();
        }
    }));

    Ok((addr, rx, bulk_delivered))
}

/// Spawn a frame-delivery RTP server that accepts one connection, wraps it
/// in a frame-reassembly mux server, and handles latency/bulk streams.
/// Returns `(addr, lat_rx, bulk_counter)` like [`spawn_mux_latency_bulk_server`]
/// but the server uses `frame_reassembly: true` and each RTP connection is
/// accepted in frame-delivery mode.
pub async fn spawn_mux_frame_delivery_latency_bulk_server(
    tasks: &mut TestScope,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
)> {
    spawn_mux_frame_delivery_latency_bulk_server_core(
        |fut| tasks.spawn(fut),
        fec,
        base,
        FrameMode::enabled(),
        FecTuning::default(),
    )
    .await
}

/// [`spawn_mux_frame_delivery_latency_bulk_server`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn spawn_mux_frame_delivery_latency_bulk_server_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
)> {
    spawn_mux_frame_delivery_latency_bulk_server_core(
        |fut| submit_test_task(tx, fut),
        fec,
        base,
        FrameMode::enabled(),
        FecTuning::default(),
    )
    .await
}

/// [`spawn_mux_frame_delivery_latency_bulk_server_via`] with receiver-side
/// fast-forward enabled: every accepted RTP connection uses
/// [`FrameMode::enabled_reordering`], matching the client's
/// [`rtp::testkit::frame::rtp_frame_delivery_connect_reorder_via`]. This is
/// the deployment's interactive-lane frame mode; both peers must select it.
pub async fn spawn_mux_frame_delivery_latency_bulk_server_reorder_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
)> {
    spawn_mux_frame_delivery_latency_bulk_server_core(
        |fut| submit_test_task(tx, fut),
        fec,
        base,
        FrameMode::enabled_reordering(),
        FecTuning::default(),
    )
    .await
}

/// [`spawn_mux_frame_delivery_latency_bulk_server_via`] with an explicit
/// per-connection [`rtp::FecTuning`], so a frame-delivery scenario can run the
/// deployment's frame-mode-plus-FEC path (both peers must set the same
/// tuning; there is no in-band negotiation).
pub async fn spawn_mux_frame_delivery_latency_bulk_server_with_fec_tuning_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
    fec_tuning: rtp::FecTuning,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
)> {
    spawn_mux_frame_delivery_latency_bulk_server_core(
        |fut| submit_test_task(tx, fut),
        fec,
        base,
        FrameMode::enabled(),
        fec_tuning,
    )
    .await
}

/// [`spawn_mux_frame_delivery_latency_bulk_server_reorder_via`] with an
/// explicit per-connection [`rtp::FecTuning`]: the deployment's interactive
/// lane (frame fast-forward **and** FEC) with both peers on the same tuning.
pub async fn spawn_mux_frame_delivery_latency_bulk_server_reorder_with_fec_tuning_via(
    tx: &TestTaskSubmitter,
    fec: bool,
    base: Instant,
    fec_tuning: rtp::FecTuning,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
)> {
    spawn_mux_frame_delivery_latency_bulk_server_core(
        |fut| submit_test_task(tx, fut),
        fec,
        base,
        FrameMode::enabled_reordering(),
        fec_tuning,
    )
    .await
}

// ─────────────────────────── loopback ceiling probes ────────────────────
//
// The one-shot loopback perf-ceiling probe plumbing: transient connects
// (sessions torn down mid-body by design), the shared probe constants, the
// hostile-link goodput floor, and the release-only median bulk-goodput
// floors. Owned here because every one of them is a mux-over-rtp fact; the
// harness's `tests` package reaches the same single authority through its
/// Connect an `rtp` client whose session is intentionally torn down
/// mid-body: each one-shot probe stops its pair (cutting the link) right
/// after measuring, so the supervisor keepalive must be transient (ordinary
/// spawn, ending when the connection closes) rather than `spawn_required`
/// (which would panic when the session ends before the body completes).
pub async fn rtp_connect_transient(
    task_tx: &netem_test::kit::TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    mss: usize,
) -> (
    impl tokio::io::AsyncRead + Unpin + Send + use<>,
    impl tokio::io::AsyncWrite + Unpin + Send + use<>,
) {
    rtp_connect_transient_observed(task_tx, proxy_client_addr, fec, mss, None).await
}

/// [`rtp_connect_transient`] with an optional typed transport observer; the
/// tracing harness passes its RTP observer through here so the capture covers
/// the client endpoint as well.
pub async fn rtp_connect_transient_observed(
    task_tx: &netem_test::kit::TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    mss: usize,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
) -> (
    impl tokio::io::AsyncRead + Unpin + Send + use<>,
    impl tokio::io::AsyncWrite + Unpin + Send + use<>,
) {
    let connected = rtp::udp::connect_with(
        "0.0.0.0:0",
        &proxy_client_addr.to_string(),
        rtp::udp::ConnectConfig {
            handshake: false,
            metrics_observer,
            fec,
            mss: rtp::udp::MssConfig::Custom(mss),
            // The probe measures the proxy's bulk data path, which runs over a
            // dedicated pipe with no competing traffic.  Declare that intent
            // explicitly instead of relying on the delivery-mode bit.
            congestion_lane: rtp::CongestionLane::Dedicated,
            ..rtp::udp::ConnectConfig::default()
        },
    )
    .await
    .unwrap();
    let read = connected.read.into_async_read();
    let write = connected.write.into_async_write();
    // The supervisor owns the session drivers; an ordinary submit keeps it
    // alive only until the session ends (the expected teardown here).
    // Routing it through the already-active bounded outer submitter means a
    // panicked supervisor fails the test immediately instead of being
    // stored in an unpolled body-local scope until it is dropped.
    netem_test::kit::submit_test_task(
        task_tx,
        Box::pin(async move {
            let _ = connected.supervisor.await;
        }),
    );
    (read, write)
}

/// Number of iterations for the ceiling probes. Reporting the median (and
/// worst) of several runs smooths out occasional warm-up / tail-visibility
/// episodes on the loopback path.
pub const PROBE_ITERS: usize = 5;

/// Bulk payload size: 4 MiB, a multiple of the staging payload that is small
/// enough to stay under `rtp`'s broken-pipe heuristic for raw RTP.
pub const BULK: usize = 4 * 1024 * 1024;

/// Loopback MSS for probes that need one: 8192 bytes, a whole multiple of the
/// staging payload and comfortably under macOS `net.inet.udp.maxdgram` ≈ 9216.
pub const LOOPBACK_MSS: usize = 8192;

/// Number of equal sub-windows the hostile measurement window is split into.
/// The guarded quantity is the **median** sub-window goodput, so a single
/// load-spiked sub-window cannot trip the floor; the whole-window rate is still
/// reported and traced.
pub const HOSTILE_GUARD_SUBWINDOWS: usize = 3;

/// Minimum acceptable **median** goodput for the hostile-link probe.
///
/// Calibrated to the gap between the healthy band and the pre-fix collapse
/// rather than to a single run: healthy 30 s windows measure 0.40-0.53 MiB/s
/// at host load 3.7-4.8 (isolated minimum observed 0.356 MiB/s at load ~5),
/// while the pre-fix collapse was ~0.015 MiB/s. 0.075 is the geometric
/// midpoint of 0.356 and 0.015 -- ~4.7x below the slowest healthy sample and
/// ~5x above the collapse -- so ordinary load noise cannot trip it while a
/// genuine collapse still does.
///
/// A ratio against an in-run clean-lane reference was evaluated and rejected:
/// the clean `mux`-over-`rtp` loopback ceiling is CPU-saturated (~1.4 GiB/s,
/// stable to ~1% across host load) rather than load-limited, so the ratio
/// inherits essentially all of the hostile lane's variance and adds no
/// robustness over an absolute floor.
pub const HOSTILE_GOODPUT_FLOOR_MIB_S: f64 = 0.075;

/// Release-only bulk-goodput floors (MiB/s, median of [`PROBE_ITERS`]) for
/// the loopback ceiling probes. Calibrated against the measured release
/// medians — ~191 MiB/s (mux sink 4 MiB 8 KiB-MSS) and ~117 MiB/s (mux sink
/// 4 MiB direct), each with worst-of-5 no lower than ~140 / ~82 MiB/s — with
/// the floor at ~0.5× the median band, so a merge that halves loopback
/// throughput fails the gate while ordinary host-load noise (the measured
/// median held through load ~6) never trips it. Wall-clock, so the floors are
/// compiled out of debug builds entirely and only bite under `--release`,
/// where loopback throughput is representative.
#[cfg(not(debug_assertions))]
pub const MUX_SINK_MSS8K_FLOOR_MIB_S: f64 = 96.0;
#[cfg(not(debug_assertions))]
pub const MUX_SINK_DIRECT_FLOOR_MIB_S: f64 = 58.0;

/// Assert the median-of-[`PROBE_ITERS`] MiB/s against a release-only floor.
/// Only the median is gated — a single load-spiked iteration cannot trip the
/// floor — mirroring the hostile probe's median-sub-window guard.
#[cfg(not(debug_assertions))]
pub fn assert_median_bulk_floor(label: &str, bytes: usize, samples: &[Duration], floor_mib_s: f64) {
    assert!(!samples.is_empty(), "{label}: no samples");
    let mut rates: Vec<f64> = samples
        .iter()
        .map(|elapsed| bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64())
        .collect();
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = rates[rates.len() / 2];
    assert!(
        median >= floor_mib_s,
        "bulk goodput floor: {label} median {median:.2} MiB/s < {floor_mib_s} MiB/s (per-sample {rates:?})"
    );
}
