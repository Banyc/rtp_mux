//! Mux-level stream fairness over a single `rtp` connection.
//!
//! `rtp`'s congestion-control fairness governs how *separate* rtp flows share
//! a bottleneck.  Within one rtp flow every mux logical stream is opaque bytes
//! to the transport, so any per-stream bandwidth skew must come from the `mux`
//! layer's own egress scheduler.  This scenario builds the measurement that
//! isolates it: one `mux` session over one `rtp` connection through a
//! fixed-rate, seeded `NetemPair`, N bulk logical streams, and per-stream
//! delivered bytes counted by the peer.
//!
//! Each stream writes an 8-byte little-endian tag before its payload, so the
//! per-stream counters are keyed by stream identity rather than accept order.
//! A warmup window is discarded and the steady window's per-stream byte deltas
//! feed a Jain index.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p rtp_mux --test mux_stream_fairness -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use mux::testkit::mux::mux_client_connect_via;
use netem_test::kit::task_scope::submit_test_task;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::rtp_connect_via;
use rtp_mux::testkit::mux_over_rtp::spawn_mux_over_rtp_server_with_mss_via;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Fixed offered aggregate load is far above the shaped link rate, so every
/// non-throttled stream is backlogged for the whole steady window and the split
/// is decided by the mux scheduler, not by a producer running dry.
const LINK_RATE_BPS: u64 = 4_000_000;
const LINK_LATENCY_MS: u64 = 20;
const WARMUP: Duration = Duration::from_secs(4);
const STEADY: Duration = Duration::from_secs(8);

/// Read an environment knob, defaulting when unset or unparseable.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// What a measured arm must satisfy.
#[derive(Clone, Copy)]
enum Floor {
    /// Equal-size chunks must split the link evenly and stably.
    HomogeneousJain,
    /// Different-size bulk chunks must split the link by bytes too, not by
    /// messages: byte-fair deficit round robin keeps a small-message stream's
    /// share near its peers', so the Jain index stays high.
    ByteFairJain,
    /// Report-only: a genuinely latency-sensitive small-message stream is
    /// allowed to dominate, and a throttled stream is expected to see an
    /// unequal (offered-rate-limited) share.
    ReportOnly,
}

/// One measurement arm.
struct Arm {
    label: &'static str,
    /// Per-stream write chunk size; the stream count is its length.
    chunks: Vec<usize>,
    /// Delay inserted between opening successive streams.
    skew: Duration,
    /// When set, stream 0 offers at most this many bytes/s instead of running
    /// backlogged, so the arm exercises a per-stream rate limit.
    throttled_bps: Option<u64>,
    floor: Floor,
}

/// Jain floor for equal-size arms.
const HOMOGENEOUS_JAIN_FLOOR: f64 = 0.98;
/// Jain floor for heterogeneous arms once the round is byte-fair. The
/// pre-byte-fair scheduler measured ~0.75 on `hetero_4k_64k_64k`, so this
/// floor is a real regression guard, not a tautology.
const BYTE_FAIR_JAIN_FLOOR: f64 = 0.98;
/// Starvation floor: every arm must keep every stream above this share.
const HETERO_MIN_SHARE: f64 = 0.02;

/// Jain fairness index over per-stream delivered bytes.
fn jain(values: &[u64]) -> f64 {
    let n = values.len() as f64;
    let sum: f64 = values.iter().map(|&v| v as f64).sum();
    let sq: f64 = values.iter().map(|&v| (v as f64) * (v as f64)).sum();
    if sum <= 0.0 || sq <= 0.0 {
        return 0.0;
    }
    sum * sum / (n * sq)
}

/// Run one arm once and return the per-stream delivered bytes over the steady
/// window, indexed by the tag each client stream writes first.
async fn measure_arm(arm: &Arm, seed: u64) -> Vec<u64> {
    let (windows, _) = measure_arm_windows(arm, seed, WARMUP, STEADY, STEADY).await;
    let mut totals = vec![0u64; arm.chunks.len()];
    for window in &windows {
        for (slot, &delta) in totals.iter_mut().zip(window.iter()) {
            *slot += delta;
        }
    }
    totals
}

/// Run one arm and return per-stream delivered bytes for each sampling window
/// across the steady phase, so a caller can report fairness over time instead
/// of one aggregate. The client streams identify themselves with an 8-byte
/// little-endian tag before their payload.
async fn measure_arm_windows(
    arm: &Arm,
    seed: u64,
    warmup: Duration,
    steady: Duration,
    window_len: Duration,
) -> (Vec<Vec<u64>>, Duration) {
    let n = arm.chunks.len();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let slots: Arc<Vec<AtomicU64>> = Arc::new((0..n).map(|_| AtomicU64::new(0)).collect());
    let server_slots = Arc::clone(&slots);

    let windows = tasks
        .run(async move {
            let server_addr = spawn_mux_over_rtp_server_with_mss_via(
                &task_tx,
                false,
                rtp::udp::NO_FEC_MSS,
                move |mut read, mut write| {
                    let slots = Arc::clone(&server_slots);
                    async move {
                        // First 8 bytes identify the logical stream.
                        let mut tag = [0u8; 8];
                        if read.read_exact(&mut tag).await.is_err() {
                            return;
                        }
                        let idx = u64::from_le_bytes(tag) as usize;
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            match read.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(got) => {
                                    if let Some(slot) = slots.get(idx) {
                                        slot.fetch_add(got as u64, Ordering::Relaxed);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        let _ = write.shutdown();
                    }
                },
            )
            .await
            .unwrap();

            let cfg = NetemConfig {
                latency: Duration::from_millis(LINK_LATENCY_MS),
                rate: LINK_RATE_BPS,
                seed,
                ..NetemConfig::default()
            };
            let pair = NetemPair::spawn(server_addr, cfg.clone(), cfg).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);

            let stop = Arc::new(AtomicBool::new(false));
            for (i, &chunk_len) in arm.chunks.iter().enumerate() {
                let (stream_read, mut stream_write) = opener.open().await.unwrap();
                drop(stream_read);
                stream_write
                    .write_all(&(i as u64).to_le_bytes())
                    .await
                    .unwrap();
                let stop = Arc::clone(&stop);
                let chunk = vec![0xABu8; chunk_len];
                let throttle = if i == 0 { arm.throttled_bps } else { None };
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut stream_write = stream_write;
                        while !stop.load(Ordering::Relaxed) {
                            if stream_write.write_all(&chunk).await.is_err() {
                                break;
                            }
                            if let Some(bps) = throttle {
                                let secs = chunk.len() as f64 / bps as f64;
                                tokio::time::sleep(Duration::from_secs_f64(secs)).await;
                            }
                        }
                        let _ = stream_write.shutdown();
                    }),
                );
                if !arm.skew.is_zero() && i + 1 < n {
                    tokio::time::sleep(arm.skew).await;
                }
            }

            tokio::time::sleep(warmup).await;
            let mut last: Vec<u64> = slots.iter().map(|s| s.load(Ordering::Relaxed)).collect();
            let start = tokio::time::Instant::now();
            let mut windows = Vec::new();
            while start.elapsed() < steady {
                tokio::time::sleep(window_len).await;
                let now: Vec<u64> = slots.iter().map(|s| s.load(Ordering::Relaxed)).collect();
                let delta: Vec<u64> = now
                    .iter()
                    .zip(last.iter())
                    .map(|(h, w)| h.saturating_sub(*w))
                    .collect();
                last = now;
                windows.push(delta);
            }
            stop.store(true, Ordering::Relaxed);
            pair.stop();
            windows
        })
        .await;
    (windows, window_len)
}

fn arms() -> Vec<Arm> {
    let bulk = |label, n: usize| Arm {
        label,
        chunks: vec![64 * 1024; n],
        skew: Duration::ZERO,
        throttled_bps: None,
        floor: Floor::HomogeneousJain,
    };
    vec![
        bulk("bulk2", 2),
        bulk("bulk3", 3),
        bulk("bulk4", 4),
        Arm {
            label: "bulk3_skew",
            chunks: vec![64 * 1024; 3],
            skew: Duration::from_millis(1500),
            throttled_bps: None,
            floor: Floor::HomogeneousJain,
        },
        Arm {
            label: "bulk3_low_rate_stream0",
            chunks: vec![64 * 1024; 3],
            skew: Duration::ZERO,
            throttled_bps: Some(200_000),
            floor: Floor::ReportOnly,
        },
        Arm {
            label: "hetero_1k_64k_64k",
            chunks: vec![1024, 64 * 1024, 64 * 1024],
            skew: Duration::ZERO,
            throttled_bps: None,
            floor: Floor::ByteFairJain,
        },
        Arm {
            label: "hetero_4k_64k_64k",
            chunks: vec![4096, 64 * 1024, 64 * 1024],
            skew: Duration::ZERO,
            throttled_bps: None,
            floor: Floor::ByteFairJain,
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network fairness sweep; run with --ignored --nocapture --test-threads=1"]
async fn mux_stream_fairness_sweep() {
    for (arm_idx, arm) in arms().iter().enumerate() {
        for rep in 0..3u64 {
            let seed = 100 + rep * 7 + arm_idx as u64;
            let deltas = measure_arm(arm, seed).await;
            let total: u64 = deltas.iter().sum();
            let shares: Vec<f64> = deltas
                .iter()
                .map(|&v| {
                    if total == 0 {
                        0.0
                    } else {
                        v as f64 / total as f64
                    }
                })
                .collect();
            let j = jain(&deltas);
            eprintln!(
                "[mux-fair] {} seed={seed} bytes={deltas:?} shares={shares:?} jain={j:.4}",
                arm.label,
            );
            let min_share = shares.iter().cloned().fold(f64::INFINITY, f64::min);
            // The starvation guard applies to every arm: no stream may be
            // pinned near zero, whatever its message size or offered rate.
            assert!(
                deltas.iter().all(|&v| v > 0),
                "{} seed={seed}: a stream was starved to zero: {deltas:?}",
                arm.label,
            );
            assert!(
                min_share >= HETERO_MIN_SHARE,
                "{} seed={seed}: smallest share {min_share:.4} below {HETERO_MIN_SHARE}",
                arm.label,
            );
            match arm.floor {
                Floor::HomogeneousJain => {
                    assert!(
                        j >= HOMOGENEOUS_JAIN_FLOOR,
                        "{} seed={seed}: homogeneous Jain {j:.3} below {HOMOGENEOUS_JAIN_FLOOR}",
                        arm.label,
                    );
                }
                Floor::ByteFairJain => {
                    assert!(
                        j >= BYTE_FAIR_JAIN_FLOOR,
                        "{} seed={seed}: byte-fair Jain {j:.3} below {BYTE_FAIR_JAIN_FLOOR}",
                        arm.label,
                    );
                }
                Floor::ReportOnly => {}
            }
        }
    }
}

/// Long-run, many-stream byte-fairness: runs the mixed-chunk substrate over a
/// multi-minute window with a per-window byte-share series, so a long-tail
/// starvation or a slow scheduler drift is visible even when the whole-run
/// Jain looks healthy. Report-only: the short sweep owns the floors.
///
/// Knobs: `MUX_FAIR_STREAMS` (default 8), `MUX_FAIR_CHUNKS` (comma list),
/// `MUX_FAIR_STEADY_SECS` (default 240), `MUX_FAIR_WARMUP_SECS` (default 10),
/// `MUX_FAIR_WINDOW_SECS` (default 10), `MUX_FAIR_REPS` (default 1).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-run fairness measurement (multi-minute, real time); run with --ignored --nocapture --test-threads=1"]
async fn mux_stream_fairness_longrun() {
    let warmup = Duration::from_secs(env_u64("MUX_FAIR_WARMUP_SECS", 10));
    let steady = Duration::from_secs(env_u64("MUX_FAIR_STEADY_SECS", 240));
    let window = Duration::from_secs(env_u64("MUX_FAIR_WINDOW_SECS", 10).max(1));
    for arm in longrun_arms() {
        for rep in 0..env_u64("MUX_FAIR_REPS", 1) {
            let seed = 500 + rep * 7;
            let (windows, _) = measure_arm_windows(&arm, seed, warmup, steady, window).await;
            let n = arm.chunks.len();
            let mut worst_jain = f64::INFINITY;
            let mut worst_jain_at = 0usize;
            let mut worst_min_share = f64::INFINITY;
            for (wi, w) in windows.iter().enumerate() {
                let j = jain(w);
                let total: u64 = w.iter().sum();
                let shares: Vec<f64> = w
                    .iter()
                    .map(|&v| {
                        if total > 0 {
                            v as f64 / total as f64
                        } else {
                            0.0
                        }
                    })
                    .collect();
                let min_share = shares.iter().cloned().fold(f64::INFINITY, f64::min);
                eprintln!(
                    "[mux-fair-longrun] {} rep={rep} window={wi} bytes={w:?} \
                     shares={shares:?} jain={j:.4} min_share={min_share:.4}",
                    arm.label,
                );
                if j < worst_jain {
                    worst_jain = j;
                    worst_jain_at = wi;
                }
                worst_min_share = worst_min_share.min(min_share);
            }
            let mut totals = vec![0u64; n];
            for w in &windows {
                for (i, &v) in w.iter().enumerate() {
                    totals[i] += v;
                }
            }
            let total: u64 = totals.iter().sum();
            let shares: Vec<f64> = totals
                .iter()
                .map(|&v| {
                    if total > 0 {
                        v as f64 / total as f64
                    } else {
                        0.0
                    }
                })
                .collect();
            eprintln!(
                "[mux-fair-longrun-summary] {} rep={rep} streams={n} windows={} totals={totals:?} \
                 shares={shares:?} jain={:.4} worst_window_jain={worst_jain:.4} \
                 worst_window={worst_jain_at} worst_min_share={worst_min_share:.4}",
                arm.label,
                windows.len(),
                jain(&totals),
            );
            assert!(
                totals.iter().all(|&v| v > 0),
                "{} rep={rep}: a stream was starved to zero over the long run: {totals:?}",
                arm.label,
            );
        }
    }
}

/// The long-run arms: a homogeneous N-stream arm and a mixed-chunk N-stream
/// arm. `MUX_FAIR_STREAMS` sets the count (default 8); `MUX_FAIR_CHUNKS`
/// overrides the mixed arm's per-stream chunk sizes.
fn longrun_arms() -> Vec<Arm> {
    let streams = env_u64("MUX_FAIR_STREAMS", 8).clamp(1, 16) as usize;
    let mixed: Vec<usize> = match std::env::var("MUX_FAIR_CHUNKS") {
        Ok(v) => v.split(',').filter_map(|s| s.trim().parse().ok()).collect(),
        Err(_) => {
            const SIZES: [usize; 8] =
                [512, 1024, 1500, 4096, 8192, 16 * 1024, 32 * 1024, 64 * 1024];
            (0..streams).map(|i| SIZES[i % SIZES.len()]).collect()
        }
    };
    vec![
        Arm {
            label: "longrun_bulk",
            chunks: vec![64 * 1024; streams],
            skew: Duration::ZERO,
            throttled_bps: None,
            floor: Floor::ReportOnly,
        },
        Arm {
            label: "longrun_mixed",
            chunks: mixed,
            skew: Duration::ZERO,
            throttled_bps: None,
            floor: Floor::ReportOnly,
        },
    ]
}
