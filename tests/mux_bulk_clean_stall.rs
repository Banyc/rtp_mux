//! Clean-link `mux`-over-`rtp` bulk stall: reproduction + stall watchdog.
//!
//! `hol_verify4::v4_clean_muxbulk` writes a bulk stream for a fixed window and
//! only completes when the writer drains; on a *clean* 50 ms link it sometimes
//! blocked for tens of seconds (a mux stream `write_all` that neither completed
//! nor errored).
//!
//! **The reproduction must NOT attach a metrics observer.** A snapshot-taking
//! observer's hot-path work changes the interleaving enough to mask the stall
//! entirely (it is a timing-sensitive lost window: the sender's congestion
//! window grew to 8x the BDP, the whole window was burst at once, the peer's
//! bounded receive window overran, and the resulting holes could not be
//! repaired). A no-op observer does not mask it. So
//! [`clean_link_mux_bulk_completes_within_timeout`] connects without an
//! observer, while [`induced_stall_fires_the_watchdog`] attaches one purely to
//! validate the watchdog and its transport dump.
//!
//! The rtp snapshot is captured through the public [`MetricsObserver`] hook:
//! `stall_reason`, congestion window / in-flight / pending-send bytes, the
//! pacer token count, the app write-waiter count, and the last send-driver
//! wake.  Together they say whether the sender is blocked by pacing, by the
//! congestion window, by a full send stage, or by an underlay that stopped
//! accepting — and whether the peer stopped acknowledging.
//!
//! A blocked `write_all` is *by itself* only backpressure, not a stall: when
//! the link keeps delivering (the server sink keeps advancing), a slow write is
//! exactly what a full pipe looks like.  The
//! detector therefore declares a stall only when the write blocks for the
//! watchdog window **and** no end-to-end progress is observed in that window,
//! which is the property the scenario claims to measure.  A hard-wall
//! deadline on a detached thread bounds the run even if a wedged transport
//! starves the tokio timer wheel (the failure mode that used to leave the
//! suite hanging for minutes).
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test mux_bulk_clean_stall -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mux::testkit::mux::mux_client_connect_via;
use netem_test::kit::payload::cyclic_payload;
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{NetemConfig, NetemPair};
use rtp::metrics::{
    MetricsEvent, MetricsInterest, MetricsObserver, MetricsSendDriverWake, MetricsSnapshot,
};
use rtp::testkit::rtp::{rtp_connect_with_mss_and_observer_via, rtp_connect_with_mss_via};
use rtp_mux::testkit::mux_over_rtp::spawn_mux_over_rtp_server_with_mss_via;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

const CHUNK: usize = 262_044;
const WRITE_WINDOW: Duration = Duration::from_secs(20);
const STALL_TIMEOUT: Duration = Duration::from_secs(60);
/// A single 256 KiB mux write completes in well under a second even on a slow
/// link; a multi-second block is the stall.  This is the deterministic
/// detector: it fires on the blocking write instead of waiting out the whole
/// window.
const WRITE_WATCHDOG: Duration = Duration::from_secs(5);

/// A clean (no-loss) 50 ms RTT link, matching the `hol_verify4` preset that
/// exposes the stall: the mux stream's flow-control window fills before the
/// sender drains, and the writer wedges.
fn clean_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(50),
        seed,
        ..NetemConfig::default()
    }
}

/// A deliberately slow link for the watchdog validation: at 200 Kbps a 256 KiB
/// frame takes ~10 s, so the write blocks while the session stays healthy
/// (ACKs still flow, just slowly).
fn slow_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(50),
        rate: 200_000,
        seed,
        ..NetemConfig::default()
    }
}

/// The latest transport state seen through the metrics observer.
#[derive(Clone, Copy, Default)]
struct LatestState {
    snapshot: Option<MetricsSnapshot>,
    last_wake: Option<MetricsSendDriverWake>,
    last_event: Option<(MetricsEvent, Duration)>,
    event_index: u64,
}

/// A metrics observer that keeps the newest transport snapshot (plus the last
/// send-driver wake) in a shared slot the watchdog can read when a write
/// blocks.  Per-packet attempts are the highest-rate event, so they only take
/// a fresh snapshot every 256th call; raw RTT samples are skipped.
fn diagnostic_observer() -> (MetricsObserver, Arc<Mutex<LatestState>>) {
    let latest = Arc::new(Mutex::new(LatestState::default()));
    let attempts = Arc::new(AtomicU64::new(0));
    let observer = MetricsObserver::selective(
        {
            let attempts = Arc::clone(&attempts);
            move |event, _elapsed| match event {
                MetricsEvent::SendDataPacketAttempt => {
                    if attempts.fetch_add(1, Ordering::Relaxed).is_multiple_of(256) {
                        MetricsInterest::Snapshot
                    } else {
                        MetricsInterest::EventOnly
                    }
                }
                MetricsEvent::RttSample => MetricsInterest::Skip,
                // Resume requests now fire once per application write: keep
                // them cheap, a full snapshot is only needed on rare events.
                MetricsEvent::SendDriverResumeRequest(_) => MetricsInterest::EventOnly,
                _ => MetricsInterest::Snapshot,
            }
        },
        {
            let latest = Arc::clone(&latest);
            move |observation| {
                let mut state = latest.lock().unwrap();
                state.event_index = observation.event_index;
                state.last_event = Some((observation.event, observation.elapsed));
                if let Some(snapshot) = observation.snapshot {
                    state.snapshot = Some(snapshot);
                }
                if let MetricsEvent::SendDriverWake(wake) = observation.event {
                    state.last_wake = Some(wake);
                }
            }
        },
    );
    (observer, latest)
}

/// Print everything the watchdog knows about the parked connection.
fn dump_stall(
    label: &str,
    latest: &Arc<Mutex<LatestState>>,
    writes: u64,
    delivered: u64,
    elapsed: Duration,
) {
    let state = *latest.lock().unwrap();
    eprintln!("===== mux bulk stall: {label} =====");
    eprintln!("  writes={writes} delivered={delivered}B elapsed={elapsed:?}");
    eprintln!("  rtp events seen: {}", state.event_index);
    eprintln!("  last rtp event: {:?}", state.last_event);
    eprintln!("  last send-driver wake: {:?}", state.last_wake);
    let Some(s) = state.snapshot else {
        eprintln!("  (no rtp snapshot captured)");
        return;
    };
    eprintln!(
        "  cwnd={}pkt in_flight={} pipe={} pending_send_bytes={} accepts_new_packet={}",
        s.congestion_window_packets,
        s.in_flight_packets,
        s.packets_in_pipe,
        s.pending_send_bytes,
        s.accepts_new_packet,
    );
    eprintln!(
        "  send_rate={:.0}pkt/s pacer_tokens={:.2} app_write_waiters={} slow_start={} outage_recovery={}",
        s.send_rate_packets_per_second,
        s.pacer_tokens_packets,
        s.application_write_waiters,
        s.slow_start,
        s.outage_recovery,
    );
    eprintln!(
        "  next_send_seq={} next_recv_seq={:?} received={} delivery_rate={:?} app_limited={:?}",
        s.next_send_sequence,
        s.next_receive_sequence,
        s.received_packets,
        s.delivery_rate_packets_per_second,
        s.delivery_sample_app_limited,
    );
    eprintln!(
        "  no_progress_for={:?} no_response_for={:?} stall_reason={:?}",
        s.no_progress_for, s.no_response_for, s.stall_reason,
    );
    eprintln!(
        "  retransmit_ready={} retransmit_active={} retransmitted={} min_rtt={:?} srtt={:?} rto={:?}",
        s.retransmission_ready_packets,
        s.retransmission_active_packets,
        s.retransmitted_packets,
        s.minimum_rtt,
        s.smoothed_rtt,
        s.retransmission_timeout,
    );
}

/// What one bounded write window did.
enum WriteOutcome {
    Completed { writes: u64, elapsed: Duration },
    Stalled { writes: u64, elapsed: Duration },
}

/// A hard, wall-clock deadline on a detached OS thread.
///
/// Every tokio timer in the test is useless once a wedged transport starves
/// the timer wheel: the per-write watchdog and the outer `STALL_TIMEOUT` both
/// stop firing and the binary hangs for minutes. This guard runs off-runtime,
/// so on expiry it prints a diagnostic and aborts the test binary (a fast,
/// diagnosable failure) instead of poisoning the suite. It is disarmed on
/// drop, so a normal run pays nothing.
struct HardDeadline {
    done: Arc<AtomicBool>,
}

impl HardDeadline {
    fn arm(label: &'static str, limit: Duration, latest: Arc<Mutex<LatestState>>) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let done_for_thread = Arc::clone(&done);
        std::thread::spawn(move || {
            let start = Instant::now();
            while !done_for_thread.load(Ordering::Relaxed) {
                if start.elapsed() >= limit {
                    eprintln!("===== mux bulk stall: HARD DEADLINE {limit:?} exceeded =====");
                    eprintln!(
                        "  the transport wedged hard enough that tokio timers no longer fire;\n  aborting the test binary so a stalled run cannot hang the suite ({label})"
                    );
                    dump_stall("hard deadline", &latest, 0, 0, limit);
                    std::process::abort();
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        });
        Self { done }
    }
}

impl Drop for HardDeadline {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
    }
}

/// Upper bound on the implicit runtime teardown.  A driver that never yields
/// cannot park `Runtime::drop` past this, and [`HardDeadline`] aborts the
/// binary if even this teardown overruns.
const RUNTIME_SHUTDOWN: Duration = Duration::from_secs(15);

/// Run `body` on an explicit multi-thread runtime under [`HardDeadline`].
///
/// The deadline must outlive the runtime teardown: `#[tokio::test]` drops its
/// runtime *after* the async body returns, so a guard living inside the body
/// is already disarmed when it waits on a driver that never yields — the
/// residual 48-minute hang — and `Runtime::drop` has no timeout of its own.
/// Building the runtime here keeps the guard in the caller's frame (it is
/// also dropped last on unwind, so it still covers a panicking body), and
/// `shutdown_timeout` bounds the drop instead of parking forever.
fn run_bounded<T>(
    label: &'static str,
    latest: Arc<Mutex<LatestState>>,
    body: impl std::future::Future<Output = T>,
) -> T {
    let _hard = HardDeadline::arm(label, STALL_TIMEOUT, Arc::clone(&latest));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("multi-thread runtime");
    let outcome = runtime.block_on(body);
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN);
    outcome
}

/// Write `CHUNK`-sized frames until [`WRITE_WINDOW`] elapses.
///
/// A blocked `write_all` is only a stall when **no end-to-end progress** is
/// observed for [`WRITE_WATCHDOG`]. `progress()` reports the server sink's
/// advanced byte count; a slow-but-live link (or a momentarily starved tokio
/// scheduler whose sink still drains) is backpressure, not a stall. Only a
/// write that blocks while the sink stops advancing is reported as
/// [`WriteOutcome::Stalled`], with the transport dump. The window is
/// self-bounded (`WRITE_WINDOW` plus at most one watchdog), so the run always
/// returns and teardown never depends on the outer timeout cancelling a live
/// future.
async fn drive_writes<W: AsyncWrite + Unpin>(
    stream_write: &mut W,
    latest: &Arc<Mutex<LatestState>>,
    delivered: &AtomicU64,
    progress: &dyn Fn() -> u64,
) -> WriteOutcome {
    let payload = cyclic_payload(CHUNK);
    let start = Instant::now();
    let window_end = start + WRITE_WINDOW;
    let hard_end = window_end + WRITE_WATCHDOG;
    let mut writes = 0u64;
    while Instant::now() < window_end {
        let write = stream_write.write_all(&payload[..CHUNK]);
        tokio::pin!(write);
        loop {
            let before = progress();
            match tokio::time::timeout(WRITE_WATCHDOG, &mut write).await {
                Ok(result) => {
                    result.unwrap();
                    break;
                }
                Err(_) => {
                    if progress() > before {
                        // The path is still advancing: upstream backpressure,
                        // not a stall. Wait for the same write again, unless
                        // the window has fully elapsed (then the writer is
                        // merely slow-but-live and the run finishes cleanly).
                        if Instant::now() >= hard_end {
                            return WriteOutcome::Completed {
                                writes,
                                elapsed: start.elapsed(),
                            };
                        }
                        continue;
                    }
                    let elapsed = start.elapsed();
                    dump_stall(
                        "write_all watchdog (end-to-end progress stopped)",
                        latest,
                        writes,
                        delivered.load(Ordering::Relaxed),
                        elapsed,
                    );
                    return WriteOutcome::Stalled { writes, elapsed };
                }
            }
        }
        writes += 1;
    }
    WriteOutcome::Completed {
        writes,
        elapsed: start.elapsed(),
    }
}

/// A clean-link mux bulk stream must make end-to-end progress and complete
/// its write window.  A write that blocks while the whole path keeps advancing
/// is backpressure and is not a stall; only a write that blocks while the sink
/// stops advancing trips [`WRITE_WATCHDOG`], dumping the
/// transport state. [`HardDeadline`] bounds the run even if the transport
/// wedges hard enough to starve the tokio timer wheel.
#[test]
fn clean_link_mux_bulk_completes_within_timeout() {
    // No metrics observer: attaching the snapshot observer masks the stall.
    let latest = Arc::new(Mutex::new(LatestState::default()));
    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_for_run = Arc::clone(&delivered);
    let latest_for_run = Arc::clone(&latest);

    let outcome = run_bounded(
        "clean_link_mux_bulk_completes_within_timeout",
        Arc::clone(&latest),
        async move {
            let mut tasks = TestScope::new();
            let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
            let run = tasks.run(async move {
                let delivered_for_server = Arc::clone(&delivered_for_run);
                let server_addr = spawn_mux_over_rtp_server_with_mss_via(
                    &task_tx,
                    false,
                    rtp::udp::NO_FEC_MSS,
                    move |mut stream_read, mut stream_write| {
                        let delivered = Arc::clone(&delivered_for_server);
                        async move {
                            let mut buf = vec![0u8; 64 * 1024];
                            while let Ok(n) = stream_read.read(&mut buf).await {
                                if n == 0 {
                                    break;
                                }
                                delivered.fetch_add(n as u64, Ordering::Relaxed);
                            }
                            let _ = stream_write.shutdown();
                        }
                    },
                )
                .await
                .unwrap();

                let pair = NetemPair::spawn(server_addr, clean_link(11), clean_link(22)).unwrap();
                let (connected_read, connected_write) = rtp_connect_with_mss_via(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                )
                .await;
                let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);
                let (mut stream_read, mut stream_write) = opener.open().await.unwrap();

                // Drain the read half so flow-control ACKs keep moving.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = stream_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );

                let progress = || delivered_for_run.load(Ordering::Relaxed);
                let outcome = drive_writes(
                    &mut stream_write,
                    &latest_for_run,
                    &delivered_for_run,
                    &progress,
                )
                .await;
                let _ = stream_write.shutdown();
                drop(opener);
                pair.stop();
                outcome
            });

            tokio::time::timeout(STALL_TIMEOUT, run).await
        },
    );

    match outcome {
        Ok(WriteOutcome::Completed { writes, elapsed }) => {
            let bytes = delivered.load(Ordering::Relaxed);
            eprintln!(
                "[mux-clean-stall] completed: writes={writes} elapsed={elapsed:?} delivered={bytes}B"
            );
            assert!(writes > 0, "the clean-link mux writer made no progress");
            assert!(bytes > 0, "the clean-link mux sink delivered nothing");
        }
        Ok(WriteOutcome::Stalled { writes, elapsed }) => panic!(
            "clean-link mux bulk stalled after {writes} writes ({elapsed:?}); transport dump printed above"
        ),
        Err(_) => {
            dump_stall(
                "outer timeout",
                &latest,
                0,
                delivered.load(Ordering::Relaxed),
                Duration::ZERO,
            );
            panic!("clean-link mux bulk did not complete within {STALL_TIMEOUT:?}");
        }
    }
}

/// Deterministic validation of the watchdog: on a 200 Kbps link the client's
/// `write_all` blocks for longer than [`WRITE_WATCHDOG`] on every frame while
/// the stream stays open, and the progress signal is deliberately frozen (the
/// injected stalled state).  The watchdog must fire and dump the transport
/// state — this is the vacuity check for the detector, proving it is capable
/// of failing.  ([`slow_live_link_is_backpressure_not_a_stall`] is the paired
/// negative control: the same slow writes with a *live* progress signal must
/// not be misreported as a stall.)
#[test]
#[ignore = "watchdog validation via an induced stall; run with --ignored --nocapture --test-threads=1"]
fn induced_stall_fires_the_watchdog() {
    let (observer, latest) = diagnostic_observer();

    let outcome = run_bounded(
        "induced_stall_fires_the_watchdog",
        Arc::clone(&latest),
        async move {
            let mut tasks = TestScope::new();
            let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
            let run = tasks.run(async move {
                let server_addr = spawn_mux_over_rtp_server_with_mss_via(
                    &task_tx,
                    false,
                    rtp::udp::NO_FEC_MSS,
                    move |mut stream_read, mut _stream_write| {
                        async move {
                            let mut buf = vec![0u8; 64 * 1024];
                            // Read far slower than the sender writes: the mux window
                            // fills and the peer's write_all blocks, while the stream
                            // stays open (the read half keeps being polled).
                            loop {
                                match stream_read.read(&mut buf).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(_) => {}
                                }
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                        }
                    },
                )
                .await
                .unwrap();

                let pair = NetemPair::spawn(server_addr, slow_link(11), slow_link(22)).unwrap();
                let (connected_read, connected_write) = rtp_connect_with_mss_and_observer_via(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                    observer,
                )
                .await;
                let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);
                let (mut stream_read, mut stream_write) = opener.open().await.unwrap();

                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = stream_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );

                let no_delivery = AtomicU64::new(0);
                // Deliberately frozen progress: the injected stalled state the
                // watchdog must flag even though the writer is only backpressured.
                let progress = || no_delivery.load(Ordering::Relaxed);
                let outcome =
                    drive_writes(&mut stream_write, &latest, &no_delivery, &progress).await;
                let _ = stream_write.shutdown();
                drop(opener);
                pair.stop();
                outcome
            });

            tokio::time::timeout(STALL_TIMEOUT, run).await
        },
    );

    match outcome {
        Ok(WriteOutcome::Stalled { writes, .. }) => {
            eprintln!(
                "[induced-stall] watchdog fired after {writes} writes — instrumentation validated"
            );
        }
        Ok(WriteOutcome::Completed { writes, elapsed }) => panic!(
            "the watchdog did not fire on an induced stall (writes={writes} elapsed={elapsed:?})"
        ),
        Err(_) => panic!("the induced-stall run did not finish within {STALL_TIMEOUT:?}"),
    }
}

/// The watchdog's negative control: on a very slow but *live* link the client's
/// `write_all` blocks for longer than [`WRITE_WATCHDOG`] on every frame, yet the
/// sink keeps receiving (just slowly). That is backpressure, not a stall, so the
/// run must finish [`WriteOutcome::Completed`] — this is the guard against the
/// false positive that made the clean-link scenario flaky under load.
#[test]
#[ignore = "watchdog negative control: slow-but-live backpressure must not trip it; run with --ignored --nocapture --test-threads=1"]
fn slow_live_link_is_backpressure_not_a_stall() {
    let latest = Arc::new(Mutex::new(LatestState::default()));
    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_for_run = Arc::clone(&delivered);
    let latest_for_run = Arc::clone(&latest);

    let outcome = run_bounded(
        "slow_live_link_is_backpressure_not_a_stall",
        Arc::clone(&latest),
        async move {
            let mut tasks = TestScope::new();
            let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
            let run = tasks.run(async move {
                let delivered_for_server = Arc::clone(&delivered_for_run);
                let server_addr = spawn_mux_over_rtp_server_with_mss_via(
                    &task_tx,
                    false,
                    rtp::udp::NO_FEC_MSS,
                    move |mut stream_read, mut stream_write| {
                        let delivered = Arc::clone(&delivered_for_server);
                        async move {
                            let mut buf = vec![0u8; 64 * 1024];
                            while let Ok(n) = stream_read.read(&mut buf).await {
                                if n == 0 {
                                    break;
                                }
                                delivered.fetch_add(n as u64, Ordering::Relaxed);
                            }
                            let _ = stream_write.shutdown();
                        }
                    },
                )
                .await
                .unwrap();

                let pair = NetemPair::spawn(server_addr, slow_link(11), slow_link(22)).unwrap();
                let (connected_read, connected_write) = rtp_connect_with_mss_via(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                )
                .await;
                let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);
                let (mut stream_read, mut stream_write) = opener.open().await.unwrap();

                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = stream_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );

                let progress = || delivered_for_run.load(Ordering::Relaxed);
                let outcome = drive_writes(
                    &mut stream_write,
                    &latest_for_run,
                    &delivered_for_run,
                    &progress,
                )
                .await;
                let _ = stream_write.shutdown();
                drop(opener);
                pair.stop();
                outcome
            });

            tokio::time::timeout(STALL_TIMEOUT, run).await
        },
    );

    match outcome {
        Ok(WriteOutcome::Completed { writes, elapsed }) => {
            let bytes = delivered.load(Ordering::Relaxed);
            eprintln!(
                "[slow-live] backpressure not a stall: writes={writes} elapsed={elapsed:?} delivered={bytes}B"
            );
            assert!(writes > 0, "the slow-but-live writer made no progress");
            assert!(bytes > 0, "the slow-but-live sink delivered nothing");
        }
        Ok(WriteOutcome::Stalled { writes, elapsed }) => panic!(
            "a slow-but-live link was misreported as a stall after {writes} writes ({elapsed:?})"
        ),
        Err(_) => panic!("the slow-but-live run did not finish within {STALL_TIMEOUT:?}"),
    }
}

/// Vacuity check for [`RUNTIME_SHUTDOWN`]: a blocking task that never returns
/// must not park the runtime teardown.  `Runtime::drop` waits forever for such
/// a task (the residual hang); `shutdown_timeout` is the bound that prevents
/// it.  Remove the bound and this test parks instead of finishing.
#[test]
fn bounded_teardown_does_not_park_on_a_stuck_blocking_task() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("multi-thread runtime");
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    runtime.spawn_blocking(move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    });
    started_rx.recv().expect("blocking task started");

    let bound = Duration::from_millis(500);
    let start = Instant::now();
    runtime.shutdown_timeout(bound);
    let elapsed = start.elapsed();
    drop(release_tx);
    assert!(
        elapsed >= bound && elapsed < Duration::from_secs(5),
        "shutdown_timeout({bound:?}) returned in {elapsed:?}; it must bound (and not skip) a stuck blocking task"
    );
}
