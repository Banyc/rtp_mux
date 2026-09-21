//! Head-of-line-blocking probe: sparse interactive messages competing with a
//! bulk flow through the same bottleneck.
//!
//! These tests are `#[ignore]`-d by default so they do not slow normal builds
//! and because they depend on the in-flight `rtp`/`mux` path dependencies.
//! Run them with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test hol_probe -- --ignored --nocapture --test-threads=1
//! ```
//!
//! # Hostile-profile caveat: multi-minute interactive tail is BY DESIGN
//!
//! On the hostile profile (`hol_hostile_solo` / `hol_hostile_shared` /
//! `hol_hostile_split`, and the equivalent `contested_latency.rs::contested_hostile`),
//! a bulk stream deliberately starves the interactive lane's SEND path, so a
//! multi-minute interactive tail is acceptable BY DESIGN. In particular
//! `hol_hostile_shared` asserts only `delivery_pct >= 0.80`; the robust signal
//! is the send count (37 pings sent where ~82 are due) and the delivery ratio,
//! not the p99 — at n ≈ 37 the percentiles are single observations. A green
//! run means the delivery gate holds, not that the interactive lane is
//! latency-bounded. Interactive p99 under contention tracks how hard the bulk
//! stream pushes: a faster host deepens the queue the interactive lane waits
//! behind.
//!
//! # Saturating bulk + median p99 under contention
//!
//! The competing bulk flows in this battery are SATURATING by default: they
//! write back-to-back as fast as the reliable transport accepts bytes,
//! restoring the measured saturation load the probes quantify (unpaced bulk
//! goodput varied 0.20-2.95 MiB/s run to run — 0.48-1.54 MiB/s measured on
//! the rtt100 GE5 shared frame-delivery row). Pacing to
//! [`BULK_PACE_BYTES_PER_SEC`] is used ONLY by the dedicated three-run
//! deterministic regression test (`hol_paced_bulk_median_p99_regression`),
//! which bounds the queue behind the interactive lane and gates the MEDIAN
//! of the three p99s. The rows that gate p99 under contention
//! (`hol_cap400_solo`) run the probe THREE times and gate the MEDIAN of the
//! three p99s, because the residual tail is dominated by loss-stall recovery
//! whose timing against the ping cadence still varies with host scheduling.
//! `delivery_pct` and `p50` stay per-run correctness gates. The p99 limits
//! are the historical ones — they are never raised.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use mux::testkit::mux::mux_client_connect_via;
use netem_test::kit::payload::{cyclic_payload, run_bounded, with_timeout};
use netem_test::kit::presets::gilbert_elliott_loss;
use netem_test::kit::stats::{HolSummary, combined_stats, summarize};
use netem_test::kit::submit_test_task;
use netem_test::{BottleneckShaper, NetemConfig, NetemPair};
use rtp::testkit::frame::rtp_frame_delivery_connect_via;
use rtp::testkit::rtp::{
    rtp_connect_with_mss_via, spawn_rtp_bulk_upload_via, spawn_rtp_byte_sink_server_via,
};
use rtp_mux::testkit::dual::{
    dual_mux_client_connect_with_lane_modes_via,
    spawn_dual_mux_latency_bulk_server_two_listeners_via,
};
use rtp_mux::testkit::mux_over_rtp::{
    send_timestamped_messages, spawn_mux_frame_delivery_latency_bulk_server_via,
    spawn_mux_latency_bulk_server_via,
};
use rtp_mux::testkit::rtp_mux::{
    LaneFecEvidence, RtpMuxFecCapture, rtp_mux_connector_observed_via,
    spawn_rtp_mux_latency_bulk_server_observed_via,
};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// One-way delay for the rtt100 rows.
const OWD_100MS: Duration = Duration::from_millis(50);

/// Default interactive message size.
const DEFAULT_MSG_BYTES: usize = 256;
/// Default cadence between interactive messages.
const DEFAULT_CADENCE: Duration = Duration::from_millis(25);
/// Default interactive run time: 1.5 s ramp + 15 s steady-state ping window.
const DEFAULT_RUN_FOR: Duration = Duration::from_millis(16_500);
/// Default grace period for stragglers.
const DEFAULT_GRACE: Duration = Duration::from_secs(3);
/// Bulk ramp: interactive runs solo for this long before the bulk flow starts.
const BULK_RAMP: Duration = Duration::from_millis(1500);

/// Paced bulk arrival rate (bytes/sec) — used ONLY by the dedicated three-run
/// deterministic regression test (`hol_paced_bulk_median_p99_regression`).
///
/// All other competing bulk flows are saturating by default (see the module
/// header). 256 KiB/s sits below the slowest observed unpaced goodput floor
/// (the rtt100 GE5 shared row delivered 0.48-1.54 MiB/s), so with pacing the
/// send queue drains faster than the bulk arrives and stays bounded and
/// reproducible. On links whose transport sustains less than the target,
/// write backpressure paces the bulk anyway — the target only caps the
/// arrival rate, it never forces bytes in.
const BULK_PACE_BYTES_PER_SEC: u64 = 256 * 1024;

/// Write chunk for the paced bulk flows: 8 KiB keeps the arrival pattern
/// fine-grained (one chunk every ~31 ms at [`BULK_PACE_BYTES_PER_SEC`]) so the
/// interleaving with the 25 ms interactive cadence is deterministic.
const BULK_PACE_CHUNK_BYTES: usize = 8 * 1024;

#[test]
fn fec_gaming_treatment_has_bad_path_and_large_capacity_headroom() {
    let link = netem_test::kit::presets::fec_gaming_fat_pipe();
    let offered_bits_per_second = DEFAULT_MSG_BYTES as f64 * 8.0 / DEFAULT_CADENCE.as_secs_f64();
    assert!(
        offered_bits_per_second < link.rate as f64 / 100.0,
        "interactive offered load must stay below 1% of shaped capacity"
    );
    assert_eq!(link.loss_model, netem_test::LossModel::Random);
    assert_eq!(link.loss, u32::MAX / 20);
    assert_eq!(link.latency, Duration::from_millis(300));
}

#[test]
fn fec_saturated_pair_keys_loss_to_the_same_rtp_sequence() {
    let off = netem_test::kit::presets::fec_paired_saturated_bottleneck(false);
    let on = netem_test::kit::presets::fec_paired_saturated_bottleneck(true);
    assert_eq!(
        off.loss_model,
        netem_test::LossModel::PacketKeyed { key_offset: 1 }
    );
    assert_eq!(
        on.loss_model,
        netem_test::LossModel::PacketKeyed { key_offset: 11 }
    );
    assert_eq!(off.loss, on.loss);
    assert_eq!(off.rate, on.rate);
    assert_eq!(off.latency, on.latency);
    assert_eq!(off.queue_limit_pkts, on.queue_limit_pkts);
}

/// How a competing bulk flow shares the bottleneck with the interactive stream.
#[derive(Clone, Debug)]
pub enum BulkMode {
    /// No competing bulk flow.
    None,
    /// Second mux stream on the same RTP connection / NetemPair.
    Shared,
    /// Separate independent NetemPair for the bulk flow.
    Split(Box<(NetemConfig, NetemConfig)>),
    /// Two NetemPairs sharing one [`BottleneckShaper`] per direction.
    SplitSharedBneck(BottleneckShaper, BottleneckShaper),
}

/// How a competing bulk flow offers bytes to the transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkLoad {
    /// Write back-to-back as fast as the transport accepts bytes: the
    /// measured saturation load.
    Saturating,
    /// Pace the offered load at [`BULK_PACE_BYTES_PER_SEC`].
    Paced,
}

#[derive(Clone, Copy, Debug)]
struct TrafficConfig {
    msg_bytes: usize,
    cadence: Duration,
    run_for: Duration,
    grace: Duration,
}

#[derive(Clone, Debug)]
struct HolProbeConfig {
    bulk: BulkMode,
    bulk_load: BulkLoad,
    fec: bool,
    traffic: TrafficConfig,
}

#[derive(Clone, Copy, Debug)]
struct DualLaneProbeConfig {
    interactive_frame: bool,
    bulk_frame: bool,
    traffic: TrafficConfig,
}

#[derive(Clone, Copy, Debug)]
struct FrameDeliveryProbeConfig {
    fec: bool,
    traffic: TrafficConfig,
}

/// Run one head-of-line-blocking probe.
///
/// Spawns a mux-over-RTP server that accepts one RTP connection. The server
/// classifies each mux stream by its first byte: `b'L'` streams push
/// timestamped one-way latencies into a channel, and any other tag is treated
/// as a deterministic bulk byte sink. An interactive `b'L'` stream sends
/// `msg_bytes`-sized timestamped messages every `cadence` for `run_for`,
/// optionally contested by a bulk flow depending on `bulk`. A `BULK_RAMP`
/// interval at the start lets the solo baseline establish before the bulk
/// flow begins.
async fn run_hol_probe(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    config: HolProbeConfig,
) -> HolSummary {
    let HolProbeConfig {
        bulk,
        bulk_load,
        fec,
        traffic:
            TrafficConfig {
                msg_bytes,
                cadence,
                run_for,
                grace,
            },
    } = config;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            // The whole scenario — server startup, pairs, connection setup,
            // measurement, teardown — runs inside the actively-reaped body.
            let (server_addr, mut latencies, mux_bulk_counter) =
                spawn_mux_latency_bulk_server_via(&task_tx, fec, base)
                    .await
                    .unwrap();

            // Set up the interactive NetemPair and, for split modes, a bulk pair.
            let (interactive_pair, bulk_pair_opt, bulk_counter) = match &bulk {
                BulkMode::None | BulkMode::Shared => {
                    let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
                    (pair, None, Arc::clone(&mux_bulk_counter))
                }
                BulkMode::Split(box_config) => {
                    let (c2s_bulk, s2c_bulk) = box_config.as_ref();
                    let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
                    let (sink_addr, counter) =
                        spawn_rtp_byte_sink_server_via(&task_tx, fec).await.unwrap();
                    let bulk_pair =
                        NetemPair::spawn(sink_addr, c2s_bulk.clone(), s2c_bulk.clone()).unwrap();
                    (pair, Some(bulk_pair), counter)
                }
                BulkMode::SplitSharedBneck(shaper_c2s, shaper_s2c) => {
                    let c2s_bulk = c2s.clone();
                    let s2c_bulk = s2c.clone();
                    let pair = NetemPair::spawn_shared(
                        server_addr,
                        c2s,
                        s2c,
                        Some(shaper_c2s.clone()),
                        Some(shaper_s2c.clone()),
                    )
                    .unwrap();
                    let (sink_addr, counter) =
                        spawn_rtp_byte_sink_server_via(&task_tx, fec).await.unwrap();
                    let bulk_pair = NetemPair::spawn_shared(
                        sink_addr,
                        c2s_bulk,
                        s2c_bulk,
                        Some(shaper_c2s.clone()),
                        Some(shaper_s2c.clone()),
                    )
                    .unwrap();
                    (pair, Some(bulk_pair), counter)
                }
            };

            // Connect the interactive mux client.
            let (connected_read, connected_write) = rtp_connect_with_mss_via(
                &task_tx,
                interactive_pair.client_addr(),
                fec,
                rtp::udp::NO_FEC_MSS,
            )
            .await;
            let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);

            // Open the interactive `b'L'` stream and keep its read half alive.
            let (mut rr_read, mut rr_write) = opener.open().await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = rr_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            // For Shared mode, open a second mux stream for the bulk flow.
            let mut shared_bulk_write = None;
            if matches!(bulk, BulkMode::Shared) {
                let (mut bulk_read, bulk_write) = opener.open().await.unwrap();
                // Parked until the stream closes; the owning JoinSet aborts it at scope end.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = bulk_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                shared_bulk_write = Some(bulk_write);
            }

            let active_for = run_for - BULK_RAMP;
            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));

            // Run the interactive sender and the bulk sender concurrently.
            // For split modes, spawn the bulk flow on its own pair BEFORE the
            // join so it runs concurrently with the interactive sender.
            let mut split_bulk_tasks: tokio::task::JoinSet<u64> = match &bulk {
                BulkMode::Split(_) | BulkMode::SplitSharedBneck(_, _) => {
                    let mut set = tokio::task::JoinSet::new();
                    let client_addr = bulk_pair_opt.as_ref().unwrap().client_addr();
                    set.spawn(run_rtp_bulk_flow(
                        task_tx.clone(),
                        client_addr,
                        fec,
                        Arc::clone(&payload),
                        BULK_RAMP,
                        active_for,
                        bulk_load,
                    ));
                    set
                }
                _ => tokio::task::JoinSet::new(),
            };

            let rr_fut =
                run_mux_interactive_stream(&mut rr_write, base, msg_bytes, cadence, run_for);
            let bulk_fut = async {
                if let Some(mut w) = shared_bulk_write {
                    tokio::time::sleep(BULK_RAMP).await;
                    run_mux_bulk_stream(&mut w, Arc::clone(&payload), active_for, bulk_load).await
                } else {
                    0u64
                }
            };
            let split_fut = async {
                while let Some(result) = split_bulk_tasks.join_next().await {
                    result.unwrap();
                }
                0u64
            };
            let (sent, _shared_bulk_written, _) = tokio::join!(rr_fut, bulk_fut, split_fut);

            // Allow stragglers to arrive before draining the latency channel.
            tokio::time::sleep(grace).await;
            let mut samples = Vec::new();
            while let Ok(lat) = latencies.try_recv() {
                samples.push(lat);
            }

            let received = samples.len() as u64;
            let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
            let bulk_secs = (run_for - BULK_RAMP).as_secs_f64();
            let summary = summarize(samples, sent, received, bulk_bytes, bulk_secs);

            print_hol_summary(label, &summary);
            eprintln!(
                "[hol {label}] pair stats = {:?}",
                combined_stats(&interactive_pair)
            );

            interactive_pair.stop();
            if let Some(pair) = bulk_pair_opt {
                pair.stop();
            }
            summary
        })
        .await
}

/// Send timestamped `b'L'` frames through a byte-stream write half.
async fn run_mux_interactive_stream(
    write: &mut (impl AsyncWrite + Unpin),
    base: Instant,
    msg_bytes: usize,
    cadence: Duration,
    run_for: Duration,
) -> u64 {
    if write.write_all(b"L").await.is_err() {
        return 0;
    }
    send_timestamped_messages(write, base, msg_bytes, cadence, run_for).await
}

/// Shared no-op stop flag for bulk flows that run a fixed window to completion.
static BULK_NO_STOP: AtomicBool = AtomicBool::new(false);

/// Write the cyclic payload through `write` for at most `active_for`, paced so
/// the arrival rate at the transport is bounded at [`BULK_PACE_BYTES_PER_SEC`].
///
/// One [`BULK_PACE_CHUNK_BYTES`] chunk is offered per budget window, then the
/// loop sleeps until the pacing budget permits the next chunk. If the
/// transport itself is slower than the target the `write` blocks on
/// backpressure and no extra sleep is due — pacing never exceeds what the
/// transport can accept, it only bounds the queue when the transport is fast.
/// Returns the number of payload bytes written.
async fn paced_bulk_write(
    write: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    active_for: Duration,
    stop: &AtomicBool,
) -> u64 {
    let start = Instant::now();
    let chunk = BULK_PACE_CHUNK_BYTES.min(payload.len());
    let mut offset = 0usize;
    let mut written = 0u64;
    while start.elapsed() < active_for && !stop.load(Ordering::Relaxed) {
        let mut remaining = chunk;
        while remaining > 0 {
            if start.elapsed() >= active_for || stop.load(Ordering::Relaxed) {
                return written;
            }
            let avail = payload.len() - offset;
            let take = remaining.min(avail);
            match write.write(&payload[offset..offset + take]).await {
                Ok(0) => return written,
                Ok(n) => {
                    offset = (offset + n) % payload.len();
                    remaining -= n;
                    written += n as u64;
                }
                Err(_) => return written,
            }
        }
        // Pace: wait until the budget allows the next chunk. When the
        // transport is slower than the target, `now` already exceeds the
        // budget and no sleep is due.
        let budget = written as f64 / BULK_PACE_BYTES_PER_SEC as f64;
        let now = start.elapsed().as_secs_f64();
        if budget > now {
            tokio::time::sleep(Duration::from_secs_f64(budget - now)).await;
        }
    }
    written
}

/// Write the cyclic payload through `write` for at most `active_for`, as
/// fast as the transport accepts bytes: 64 KiB chunks offered back-to-back
/// with no pacing sleep, so the bulk flow saturates the bottleneck exactly
/// like the probes originally measured. Returns the number of payload bytes
/// written.
async fn saturating_bulk_write(
    write: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    active_for: Duration,
    stop: &AtomicBool,
) -> u64 {
    let start = Instant::now();
    let chunk = (64 * 1024).min(payload.len());
    let mut offset = 0usize;
    let mut written = 0u64;
    while start.elapsed() < active_for && !stop.load(Ordering::Relaxed) {
        let mut remaining = chunk;
        while remaining > 0 {
            if start.elapsed() >= active_for || stop.load(Ordering::Relaxed) {
                return written;
            }
            let avail = payload.len() - offset;
            let take = remaining.min(avail);
            match write.write(&payload[offset..offset + take]).await {
                Ok(0) => return written,
                Ok(n) => {
                    offset = (offset + n) % payload.len();
                    remaining -= n;
                    written += n as u64;
                }
                Err(_) => return written,
            }
        }
    }
    written
}

/// Send a deterministic `b'B'` bulk stream through a byte-stream write half.
///
/// `Saturating` (the default for this battery) writes back-to-back as fast
/// as the transport accepts bytes, restoring the measured saturation load;
/// `Paced` bounds the offered rate at [`BULK_PACE_BYTES_PER_SEC`] and is used
/// only by the dedicated three-run deterministic regression test.
async fn run_mux_bulk_stream(
    write: &mut (impl AsyncWrite + Unpin),
    payload: Arc<Vec<u8>>,
    active_for: Duration,
    load: BulkLoad,
) -> u64 {
    if write.write_all(b"B").await.is_err() {
        return 0;
    }
    match load {
        BulkLoad::Saturating => {
            saturating_bulk_write(write, &payload, active_for, &BULK_NO_STOP).await
        }
        BulkLoad::Paced => paced_bulk_write(write, &payload, active_for, &BULK_NO_STOP).await,
    }
}

/// Delayed, stop-able `b'B'` bulk stream used by the dual-lane probes;
/// dispatches to the saturating or paced writer like [`run_mux_bulk_stream`].
async fn run_delayed_mux_bulk_stream(
    write: &mut (impl AsyncWrite + Unpin),
    payload: Arc<Vec<u8>>,
    delay: Duration,
    active_for: Duration,
    stop: &AtomicBool,
    load: BulkLoad,
) -> u64 {
    tokio::time::sleep(delay).await;
    if stop.load(Ordering::Relaxed) || write.write_all(b"B").await.is_err() {
        return 0;
    }
    match load {
        BulkLoad::Saturating => saturating_bulk_write(write, &payload, active_for, stop).await,
        BulkLoad::Paced => paced_bulk_write(write, &payload, active_for, stop).await,
    }
}

/// Pump a plain-RTP bulk flow through a separate NetemPair, saturating or
/// paced per `load` (see [`BULK_PACE_BYTES_PER_SEC`]).
async fn run_rtp_bulk_flow(
    task_tx: netem_test::kit::TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    payload: Arc<Vec<u8>>,
    ramp: Duration,
    active_for: Duration,
    load: BulkLoad,
) -> u64 {
    // The upload's read-keepalive is submitted through the already-active
    // bounded outer submitter (this helper runs inside the run-racing scope
    // that owns it), so a panic in the upload's supervisor driver cascades
    // into the caller immediately instead of being stored until a nested
    // scope is dropped. Connection setup therefore happens inside the
    // caller's actively-reaped body — never before supervision starts. The
    // keepalive exits when the session closes (normal teardown) and is
    // drained silently; dropping the write half at return closes the
    // session, and the outer scope aborts anything still running at teardown.
    let Ok(mut writer) = spawn_rtp_bulk_upload_via(&task_tx, proxy_client_addr, fec).await else {
        return 0;
    };
    tokio::time::sleep(ramp).await;
    match load {
        BulkLoad::Saturating => {
            saturating_bulk_write(&mut writer, &payload, active_for, &BULK_NO_STOP).await
        }
        BulkLoad::Paced => paced_bulk_write(&mut writer, &payload, active_for, &BULK_NO_STOP).await,
    }
}

fn print_hol_summary(label: &str, s: &HolSummary) {
    eprintln!(
        "[hol {label}] sent={sent} recv={recv} delivery={del:.3} p50={p50:.1} p90={p90:.1} p99={p99:.1} max={max:.1} over250={o25:.3} over1000={o1k:.3} episodes={ep} max_run={mr} bulk={bulk:.3} MiB/s",
        sent = s.sent,
        recv = s.received,
        del = s.delivery_pct,
        p50 = s.p50,
        p90 = s.p90,
        p99 = s.p99,
        max = s.max,
        o25 = s.over250_pct,
        o1k = s.over1000_pct,
        ep = s.episodes,
        mr = s.max_run,
        bulk = s.bulk_mibps,
    );
}

/// Gate the correctness signals of a triple-run contention probe.
///
/// `delivery_pct` and `p50` are per-run correctness gates and must hold on
/// EVERY run. `p99` is gated on the MEDIAN of the three runs: even with the
/// bulk flow paced (see [`BULK_PACE_BYTES_PER_SEC`]) the tail is dominated by
/// loss-stall recovery, whose timing against the ping cadence varies with
/// host scheduling, so a single run's p99 is a noisy observation. The p99
/// limit is the historical threshold — it is never raised.
fn assert_triple_run_gates(label: &str, runs: &[HolSummary], p50_ms: f64, p99_ms: f64) {
    assert_eq!(runs.len(), 3, "{label}: probe must produce exactly 3 runs");
    for (i, s) in runs.iter().enumerate() {
        let run = i + 1;
        assert!(
            s.delivery_pct >= 0.95,
            "[{label} run {run}] delivery {:.3} < 0.95",
            s.delivery_pct
        );
        assert!(
            s.p50 <= p50_ms,
            "[{label} run {run}] p50 {:.1} ms > {:.0} ms",
            s.p50,
            p50_ms
        );
    }
    let mut p99s: Vec<f64> = runs.iter().map(|s| s.p99).collect();
    p99s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = p99s[1];
    assert!(
        median <= p99_ms,
        "[{label}] median p99 {:.1} ms > {:.0} ms (per-run p99: {:.1}, {:.1}, {:.1})",
        median,
        p99_ms,
        p99s[0],
        p99s[1],
        p99s[2]
    );
}

// ────────────────────────────── config builders ─────────────────────────────

fn rtt100_clean(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD_100MS,
        seed,
        ..NetemConfig::default()
    }
}

fn rtt100_ge5(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD_100MS,
        loss_model: gilbert_elliott_loss(5.0, 3.0),
        seed,
        ..NetemConfig::default()
    }
}

fn rtt100_ge1_loss1(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD_100MS,
        loss_model: gilbert_elliott_loss(1.0, 3.0),
        loss: u32::MAX / 100,
        seed,
        ..NetemConfig::default()
    }
}

fn cap400(seed: u64) -> NetemConfig {
    NetemConfig {
        rate: 400 * 1024 * 8,
        loss: u32::MAX / 100,
        latency: Duration::from_millis(10),
        jitter: Duration::from_millis(2),
        seed,
        ..NetemConfig::default()
    }
}

fn rtt40_ge1(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(20),
        loss_model: gilbert_elliott_loss(1.0, 3.0),
        seed,
        ..NetemConfig::default()
    }
}

fn rtt40_ge1_loss1(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(20),
        loss_model: gilbert_elliott_loss(1.0, 3.0),
        loss: u32::MAX / 100,
        seed,
        ..NetemConfig::default()
    }
}

fn hostile_real_link_seeded(seed: u64) -> NetemConfig {
    let mut c = netem_test::kit::presets::hostile_real_link();
    c.seed = seed;
    c
}

/// [`netem_test::kit::presets::fec_gaming_fat_pipe`] with the iid loss raised to 20 %
/// and an explicit seed, for the default-on-FEC recovery probe.
///
/// The interactive lane's parity is *reactive*: the loss gate flushes a group
/// only after recovery evidence, and the flush protects the group the sender
/// is currently carrying.  At the preset's 5 % loss the contended sparse
/// stream opens the gate for only a handful of single-symbol groups in the
/// measurement window and none of them loses its data symbol, so the decoder
/// reconstructs nothing even though parity is emitted.  20 % keeps the same
/// bandwidth-delay product, queue limit, and seed discipline while making at
/// least one flushed group lose a data symbol, so the reconstruction path is
/// actually exercised (verified across seeds 171 and 181).
const FEC_RECOVERY_LOSS_FRACTION: u32 = 5;

fn fec_recovery_fat_pipe_seeded(seed: u64) -> NetemConfig {
    let mut c = netem_test::kit::presets::fec_gaming_fat_pipe();
    c.loss = u32::MAX / FEC_RECOVERY_LOSS_FRACTION;
    c.seed = seed;
    c
}

/// [`netem_test::kit::presets::controller_fat_pipe`] with an explicit seed, so the
/// bulk controller-retention lane of a paired default-on-FEC HOL run stays
/// reproducible.
fn controller_fat_pipe_seeded(seed: u64) -> NetemConfig {
    let mut c = netem_test::kit::presets::controller_fat_pipe();
    c.seed = seed;
    c
}

// ══════════════════════════════ reference matrix ══════════════════════════════
// ┌────────────────┬─────────┬───────────┬───────────┬─────────────┬────────────┬──────────┐
// │ link           │  rtt    │ loss      │ jitter    │ rate        │ queue      │ modes    │
// ├────────────────┼─────────┼───────────┼───────────┼─────────────┼────────────┼──────────┤
// │ rtt100 clean   │  100 ms │    0      │    0      │ unbounded   │ unbounded  │ SOL SHR  │
// │ rtt100 GE5     │  100 ms │ 5% GI bus │    0      │ unbounded   │ unbounded  │ SOL SHR  │
// │ rtt100 GE5 v2  │  100 ms │ 5% GI bus │    0      │ unbounded   │ unbounded  │ SOL SHR  │
// │ rtt100 GE5 v3  │  100 ms │ 5% GI bus │    0      │ unbounded   │ unbounded  │ SOL SHR SPL│
// │ rtt100 GE1+loss│  100 ms │ 1% GI bus │    0      │ unbounded   │ unbounded  │ SOL SHR SPL│
// │ cap400         │   20 ms │ 1% indep  │   2 ms    │ 400 kbps    │  4096 B    │ SOL SHR  │
// │ cap400 ShrShp  │   20 ms │ 1% indep  │   2 ms    │ 400 kbps shp│     0      │ SSHB RPT  │
// │ rtt40  GE1     │   40 ms │ 1% GI bus │    0      │ unbounded   │ unbounded  │ SOL SHR SPL│
// │ rtt40  GE1+loss│   40 ms │ 1% GI bus │    0      │ unbounded   │ unbounded  │ SOL SHR SPL│
// │ FEC cap400     │   20 ms │ 1% indep  │   2 ms    │ 400 kbps    │  4096 B    │ SOL (FEC) │
// │ hostile        │  varied │ burst+indp│  varied   │ varied      │  4096 B    │ SOL SHR SPL│
// └────────────────┴─────────┴───────────┴───────────┴─────────────┴────────────┴──────────┘
// ────────────────────────────── scenario macro ──────────────────────────────

macro_rules! hol_test {
    (
        $name:ident,
        $label:expr,
        $c2s:expr,
        $s2c:expr,
        $bulk:expr,
        $msg_bytes:expr,
        $cadence:expr,
        $run_for:expr,
        $grace:expr,
        $timeout:expr,
        $gates:expr
    ) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
        async fn $name() {
            let summary = with_timeout(
                $timeout,
                $label,
                run_hol_probe(
                    $label,
                    $c2s,
                    $s2c,
                    HolProbeConfig {
                        bulk: $bulk,
                        bulk_load: BulkLoad::Saturating,
                        fec: false,
                        traffic: TrafficConfig {
                            msg_bytes: $msg_bytes,
                            cadence: $cadence,
                            run_for: $run_for,
                            grace: $grace,
                        },
                    },
                ),
            )
            .await;
            let check: fn(&HolSummary) = $gates;
            check(&summary);
        }
    };
}

// ────────────────────────────── rtt100 clean row ────────────────────────────

hol_test!(
    hol_rtt100_clean_solo,
    "rtt100 clean solo",
    rtt100_clean(11),
    rtt100_clean(12),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
        assert!(summary.p50 <= 250.0, "p50 {:.1} ms > 250 ms", summary.p50);
        assert!(summary.p99 <= 800.0, "p99 {:.1} ms > 800 ms", summary.p99);
    }
);

hol_test!(
    hol_rtt100_clean_shared,
    "rtt100 clean shared",
    rtt100_clean(21),
    rtt100_clean(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
        assert!(summary.p50 <= 250.0, "p50 {:.1} ms > 250 ms", summary.p50);
        assert!(summary.p99 <= 800.0, "p99 {:.1} ms > 800 ms", summary.p99);
    }
);

hol_test!(
    hol_rtt100_clean_split,
    "rtt100 clean split",
    rtt100_clean(31),
    rtt100_clean(32),
    BulkMode::Split(Box::new((rtt100_clean(33), rtt100_clean(34)))),
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
        assert!(summary.p50 <= 250.0, "p50 {:.1} ms > 250 ms", summary.p50);
        assert!(summary.p99 <= 800.0, "p99 {:.1} ms > 800 ms", summary.p99);
    }
);

// ────────────────────────────── rtt100 GE5 row ──────────────────────────────

hol_test!(
    hol_rtt100_ge5_solo,
    "rtt100 GE5 solo",
    rtt100_ge5(11),
    rtt100_ge5(12),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge5_shared,
    "rtt100 GE5 shared",
    rtt100_ge5(21),
    rtt100_ge5(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge5_split,
    "rtt100 GE5 split",
    rtt100_ge5(31),
    rtt100_ge5(32),
    BulkMode::Split(Box::new((rtt100_ge5(33), rtt100_ge5(34)))),
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

// ─────────────────────── rtt100 GE5 seed-variant rows ────────────────────────

hol_test!(
    hol_rtt100_ge5_v2_solo,
    "rtt100 GE5 v2 solo",
    rtt100_ge5(41),
    rtt100_ge5(42),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge5_v2_shared,
    "rtt100 GE5 v2 shared",
    rtt100_ge5(51),
    rtt100_ge5(52),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge5_v3_solo,
    "rtt100 GE5 v3 solo",
    rtt100_ge5(61),
    rtt100_ge5(62),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge5_v3_shared,
    "rtt100 GE5 v3 shared",
    rtt100_ge5(71),
    rtt100_ge5(72),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge5_v3_split,
    "rtt100 GE5 v3 split",
    rtt100_ge5(81),
    rtt100_ge5(82),
    BulkMode::Split(Box::new((rtt100_ge5(83), rtt100_ge5(84)))),
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

// ────────────────────────────── rtt100 GE1+loss1 row ─────────────────────────

hol_test!(
    hol_rtt100_ge1_loss1_solo,
    "rtt100 GE1+loss1 solo",
    rtt100_ge1_loss1(11),
    rtt100_ge1_loss1(12),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge1_loss1_shared,
    "rtt100 GE1+loss1 shared",
    rtt100_ge1_loss1(21),
    rtt100_ge1_loss1(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt100_ge1_loss1_split,
    "rtt100 GE1+loss1 split",
    rtt100_ge1_loss1(31),
    rtt100_ge1_loss1(32),
    BulkMode::Split(Box::new((rtt100_ge1_loss1(33), rtt100_ge1_loss1(34)))),
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

// ────────────────────────────── cap400 row ──────────────────────────────────

// hol_cap400_solo is written out (not via the `hol_test!` macro) because its
// p99 gate is the MEDIAN of THREE probe runs: on the rate-limited cap400 link
// the tail tracks how many retransmissions are in flight when a ping is
// enqueued, which varies with host speed even though there is no bulk flow.
// delivery/p50 stay per-run gates; the p99 limit is unchanged.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_cap400_solo() {
    let label = "cap400 solo";
    let mut runs = Vec::with_capacity(3);
    for i in 0..3 {
        let run_label = format!("{label} run{}", i + 1);
        let summary = with_timeout(
            Duration::from_secs(180),
            &run_label,
            run_hol_probe(
                &run_label,
                cap400(11),
                cap400(12),
                HolProbeConfig {
                    bulk: BulkMode::None,
                    bulk_load: BulkLoad::Saturating,
                    fec: false,
                    traffic: TrafficConfig {
                        msg_bytes: DEFAULT_MSG_BYTES,
                        cadence: DEFAULT_CADENCE,
                        run_for: DEFAULT_RUN_FOR,
                        grace: DEFAULT_GRACE,
                    },
                },
            ),
        )
        .await;
        runs.push(summary);
    }
    assert_triple_run_gates(label, &runs, 100.0, 400.0);
}

hol_test!(
    hol_cap400_shared,
    "cap400 shared",
    cap400(21),
    cap400(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
        assert!(summary.p50 <= 500.0, "p50 {:.1} ms > 500 ms", summary.p50);
        assert!(summary.p99 <= 1200.0, "p99 {:.1} ms > 1200 ms", summary.p99);
    }
);

// ────────────────────────────── cap400 shared-bottleneck report-only ──────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_cap400_loss1_split_shared() {
    let label = "cap400 loss1 split-shared";
    let rate_bps = 400 * 1024 * 8;
    let c2s_shaper = BottleneckShaper::new(rate_bps, 0);
    let s2c_shaper = BottleneckShaper::new(rate_bps, 0);
    let mut c2s = cap400(41);
    c2s.rate = 0;
    c2s.loss = u32::MAX / 100;
    c2s.latency = Duration::from_millis(10);
    c2s.jitter = Duration::from_millis(2);
    let mut s2c = cap400(42);
    s2c.rate = 0;
    s2c.loss = u32::MAX / 100;
    s2c.latency = Duration::from_millis(10);
    s2c.jitter = Duration::from_millis(2);

    let _summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe(
            label,
            c2s,
            s2c,
            HolProbeConfig {
                bulk: BulkMode::SplitSharedBneck(c2s_shaper, s2c_shaper),
                bulk_load: BulkLoad::Saturating,
                fec: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
}

// ────────────────────────────── rtt40 GE1 rows ────────────────────────────────

hol_test!(
    hol_rtt40_ge1_solo,
    "rtt40 GE1 solo",
    rtt40_ge1(11),
    rtt40_ge1(12),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt40_ge1_loss1_solo,
    "rtt40 GE1+loss1 solo",
    rtt40_ge1_loss1(11),
    rtt40_ge1_loss1(12),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt40_ge1_shared,
    "rtt40 GE1 shared",
    rtt40_ge1(21),
    rtt40_ge1(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt40_ge1_split,
    "rtt40 GE1 split",
    rtt40_ge1(31),
    rtt40_ge1(32),
    BulkMode::Split(Box::new((rtt40_ge1(33), rtt40_ge1(34)))),
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt40_ge1_loss1_shared,
    "rtt40 GE1+loss1 shared",
    rtt40_ge1_loss1(21),
    rtt40_ge1_loss1(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_rtt40_ge1_loss1_split,
    "rtt40 GE1+loss1 split",
    rtt40_ge1_loss1(31),
    rtt40_ge1_loss1(32),
    BulkMode::Split(Box::new((rtt40_ge1_loss1(33), rtt40_ge1_loss1(34)))),
    DEFAULT_MSG_BYTES,
    DEFAULT_CADENCE,
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(120),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.95,
            "delivery {:.3} < 0.95",
            summary.delivery_pct
        );
    }
);

// ────────────────────────────── FEC mitigation row ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_cap400_fec_solo() {
    let label = "cap400 FEC solo";
    let _summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe(
            label,
            cap400(11),
            cap400(12),
            HolProbeConfig {
                bulk: BulkMode::None,
                bulk_load: BulkLoad::Saturating,
                fec: true,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
}

/// Production default-on interactive FEC policy probe: the `rtp_mux`
/// composition must enable `FecTuning::max_diversity()` plus in-stream group
/// FEC on the interactive RTP lane with no caller toggle, while the bulk
/// lane stays FEC-free. Every lane endpoint asserts its typed
/// `MetricsFecCounters` independently, the interactive lanes must emit parity
/// on the lossy gaming fat pipe, and — on the [`fec_recovery_fat_pipe_seeded`]
/// arm whose loss forces a flushed group to lose a data symbol — the receiver
/// must reconstruct it.  The stock 5 % gaming preset emits parity sporadically
/// and never makes a flushed group lose its symbol in this contended sparse
/// stream, so it cannot carry the recovery assertion.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow default-on FEC policy probe; run in-release mode with --ignored --nocapture --test-threads=1"]
async fn hol_rtp_mux_fec_default_on_recovery() {
    let traffic = TrafficConfig {
        msg_bytes: DEFAULT_MSG_BYTES,
        cadence: DEFAULT_CADENCE,
        run_for: DEFAULT_RUN_FOR,
        grace: DEFAULT_GRACE,
    };
    // The parity gate is reactive and each flush protects the group the sender
    // is currently carrying, so whether a *flushed* group also loses a data
    // symbol is a property of the seeded loss realization, not of the FEC
    // policy: a single arm can legitimately flush parity and reconstruct
    // nothing (observed at both the preset's 5 % and a forced 20 %).  The
    // reconstruction path is therefore gated on the aggregate over the arms —
    // at the forced loss rate every arm flushes parity and the decoder
    // reconstructs (measured 3-6 symbols per arm) — while the per-arm parity
    // gate keeps each arm honest.  With FEC off both totals are zero, so the
    // aggregate assertion is the vacuity check for the whole composition.
    let mut parity_total = 0u64;
    let mut recovered_total = 0u64;
    for (label, seed) in [
        ("rtp_mux default FEC A", 171),
        ("rtp_mux default FEC B", 181),
    ] {
        let result = with_timeout(
            Duration::from_secs(180),
            label,
            run_hol_probe_rtp_mux(
                label,
                fec_recovery_fat_pipe_seeded(seed),
                fec_recovery_fat_pipe_seeded(seed + 1),
                controller_fat_pipe_seeded(seed + 2),
                controller_fat_pipe_seeded(seed + 3),
                traffic,
            ),
        )
        .await;
        result.assert_lane_observability();
        assert!(
            result.summary.delivery_pct > 0.0,
            "{label} delivered no messages"
        );
        assert!(
            result.summary.bulk_mibps > 0.0,
            "{label} bulk lane was not active"
        );
        let sent = result.client_interactive_fec.counters.unwrap();
        let received = result.server_interactive_fec.counters.unwrap();
        assert!(
            sent.parity_sent > 0,
            "{label} did not activate default FEC after path-recovery evidence: {sent:?}"
        );
        parity_total += sent.parity_sent;
        recovered_total += received.recovered_symbols;
        eprintln!("[hol {label}] default FEC evidence: sender={sent:?} receiver={received:?}");
    }
    assert!(
        parity_total > 0,
        "default-on interactive FEC never emitted parity across the arms"
    );
    assert!(
        recovered_total > 0,
        "default-on interactive FEC emitted {parity_total} parity symbols across the arms but the \
         receiver reconstructed none; the reconstruction path is dead"
    );
}

// ────────────────────────────── hostile rows ──────────────────────────────────

hol_test!(
    hol_hostile_solo,
    "hostile solo",
    hostile_real_link_seeded(11),
    hostile_real_link_seeded(12),
    BulkMode::None,
    DEFAULT_MSG_BYTES,
    Duration::from_millis(200),
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(300),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.80,
            "delivery {:.3} < 0.80",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_hostile_shared,
    "hostile shared",
    hostile_real_link_seeded(21),
    hostile_real_link_seeded(22),
    BulkMode::Shared,
    DEFAULT_MSG_BYTES,
    Duration::from_millis(200),
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(300),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.80,
            "delivery {:.3} < 0.80",
            summary.delivery_pct
        );
    }
);

hol_test!(
    hol_hostile_split,
    "hostile split",
    hostile_real_link_seeded(31),
    hostile_real_link_seeded(32),
    BulkMode::Split(Box::new((
        hostile_real_link_seeded(33),
        hostile_real_link_seeded(34)
    ))),
    DEFAULT_MSG_BYTES,
    Duration::from_millis(200),
    DEFAULT_RUN_FOR,
    DEFAULT_GRACE,
    Duration::from_secs(300),
    |summary: &HolSummary| {
        assert!(
            summary.delivery_pct >= 0.80,
            "delivery {:.3} < 0.80",
            summary.delivery_pct
        );
    }
);

// ═══════════════════════════════════════════════════════════════════════════════
// Frame‑delivery & dual‑lane runners
// ═══════════════════════════════════════════════════════════════════════════════

/// Run an HOL probe on a single frame-delivery RTP connection with a
/// reassembly mux on top.  Interactive and bulk streams share one RTP
/// connection whose frames are preserved end-to-end — every mux frame maps
/// to exactly one RTP frame.  Returns a [`HolSummary`].
async fn run_hol_probe_frame_delivery_shared(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    fec: bool,
    load: BulkLoad,
    traffic: TrafficConfig,
) -> HolSummary {
    let TrafficConfig {
        msg_bytes,
        cadence,
        run_for,
        grace,
    } = traffic;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, mut latencies, mux_bulk_counter) =
                spawn_mux_frame_delivery_latency_bulk_server_via(&task_tx, fec, base)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let (reader, writer) =
                rtp_frame_delivery_connect_via(&task_tx, pair.client_addr(), fec).await;
            let config = mux::MuxConfig {
                initiation: mux::Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: true,
            };
            let mut spawner = tokio::task::JoinSet::new();
            let (opener, _accepter) =
                mux::spawn_mux_no_reconnection(reader, writer, config, &mut spawner);
            // The mux supervision is drained by a non-required background
            // task: these echo lanes let the session tear down normally (FIN
            // exchanged) before the measurement body finishes, so a required
            // task would panic on that normal early completion. A panicked
            // supervision task still surfaces at scope end, and the JoinSet
            // is dropped when the task completes, aborting any stragglers.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    if let Some(Err(err)) = spawner.join_next().await {
                        panic!("mux client session supervision failed: {err:?}");
                    }
                }),
            );
            let (mut rr_read, mut rr_write) = opener.open().await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = rr_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let (mut bulk_read, bulk_write) = opener.open().await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let active_for = run_for - BULK_RAMP;
            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let rr_fut =
                run_mux_interactive_stream(&mut rr_write, base, msg_bytes, cadence, run_for);
            let bulk_fut = async {
                let mut w = bulk_write;
                tokio::time::sleep(BULK_RAMP).await;
                run_mux_bulk_stream(&mut w, Arc::clone(&payload), active_for, load).await
            };
            let (sent, _bulk_written) = tokio::join!(rr_fut, bulk_fut);
            tokio::time::sleep(grace).await;
            let mut samples = Vec::new();
            while let Ok((_tag, lat)) = latencies.try_recv() {
                samples.push(lat);
            }
            let received = samples.len() as u64;
            let bulk_bytes = mux_bulk_counter.load(Ordering::Relaxed);
            let bulk_secs = active_for.as_secs_f64();
            let summary = summarize(samples, sent, received, bulk_bytes, bulk_secs);
            print_hol_summary(label, &summary);
            eprintln!("[hol {}] pair stats = {:?}", label, combined_stats(&pair));
            pair.stop();
            summary
        })
        .await
}

/// Head-of-line summary plus per-lane-endpoint typed FEC evidence from a
/// production `rtp_mux` HOL probe.
#[derive(Clone, Debug)]
struct RtpMuxHolResult {
    summary: HolSummary,
    client_interactive_fec: LaneFecEvidence,
    client_bulk_fec: LaneFecEvidence,
    server_interactive_fec: LaneFecEvidence,
    server_bulk_fec: LaneFecEvidence,
}

impl RtpMuxHolResult {
    /// Gate the lane observability contract: every lane endpoint must have
    /// reported RTP metrics, the interactive lanes must have enabled FEC by
    /// default (typed counters present), and the bulk lanes must have stayed
    /// FEC-free (typed counters absent — never a fabricated zero).
    fn assert_lane_observability(&self) {
        for (name, evidence) in [
            ("client interactive", self.client_interactive_fec),
            ("client bulk", self.client_bulk_fec),
            ("server interactive", self.server_interactive_fec),
            ("server bulk", self.server_bulk_fec),
        ] {
            assert!(evidence.observed, "{name} RTP metrics were not observed");
        }
        assert!(
            self.client_interactive_fec.counters.is_some(),
            "client interactive lane did not enable FEC by default"
        );
        assert!(
            self.server_interactive_fec.counters.is_some(),
            "server interactive lane did not enable FEC by default"
        );
        assert!(
            self.client_bulk_fec.counters.is_none(),
            "client bulk lane unexpectedly activated FEC: {:?}",
            self.client_bulk_fec.counters
        );
        assert!(
            self.server_bulk_fec.counters.is_none(),
            "server bulk lane unexpectedly activated FEC: {:?}",
            self.server_bulk_fec.counters
        );
    }
}

/// Run an HOL probe through production `rtp_mux`.  Both lanes use
/// frame‑delivery RTP; the connector routes the interactive stream through
/// the interactive lane and the bulk stream through the bulk lane.  The
/// probe attaches independent [`RtpMuxFecCapture`]s to the server and client
/// lanes, requires the bulk pump to stay live until the interactive
/// measurement finishes, and joins the pump before the straggler grace so
/// the bulk byte snapshot is stable.
async fn run_hol_probe_rtp_mux(
    label: &str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk_c2s: NetemConfig,
    bulk_s2c: NetemConfig,
    traffic: TrafficConfig,
) -> RtpMuxHolResult {
    let TrafficConfig {
        msg_bytes,
        cadence,
        run_for,
        grace,
    } = traffic;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let server_metrics = RtpMuxFecCapture::default();
            let client_metrics = RtpMuxFecCapture::default();
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_rtp_mux_latency_bulk_server_observed_via(
                    &task_tx,
                    base,
                    server_metrics.observers(),
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            let bulk_pair = NetemPair::spawn(bulk_addr, bulk_c2s, bulk_s2c).unwrap();
            let connector = Arc::new(rtp_mux_connector_observed_via(
                &task_tx,
                bulk_pair.client_addr(),
                client_metrics.observers(),
            ));
            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let active_for = run_for - BULK_RAMP;
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            // `pump_failed_tx` records that the pump ended on its own (open or
            // write failure) rather than on the watch stop signal; only an
            // own-end while the measurement is running is a premature pump end.
            let (pump_failed_tx, pump_failed_rx) = tokio::sync::watch::channel(false);
            let mut bulk_tasks = tokio::task::JoinSet::new();
            {
                let connector = Arc::clone(&connector);
                let payload = Arc::clone(&payload);
                let int_proxy_addr = int_pair.client_addr();
                bulk_tasks.spawn(async move {
                    let mut stream = match connector
                        .connect_stream_with_lane(int_proxy_addr, rtp_mux::LaneClass::Bulk)
                        .await
                    {
                        Ok(stream) => stream,
                        Err(_) => {
                            let _ = pump_failed_tx.send(true);
                            return;
                        }
                    };
                    // The pump runs until the watch signals shutdown at the end of
                    // the interactive measurement (or a write failure ends it).
                    // `BULK_NO_STOP` keeps the internal stop flag inert and the long
                    // window is only a backstop so the pump never ends on its own
                    // while the measurement is still running.
                    tokio::select! {
                        _ = bulk_stop_rx.changed() => {}
                        _ = run_delayed_mux_bulk_stream(
                            &mut stream,
                            payload,
                            BULK_RAMP,
                            Duration::from_secs(3600),
                            &BULK_NO_STOP,
                            BulkLoad::Saturating,
                        ) => {
                            // The writer returned short of its 3600s backstop and
                            // before the stop signal: the pump ended on a write
                            // failure while the measurement may still be running.
                            let _ = pump_failed_tx.send(true);
                        }
                    }
                    let _ = stream.shutdown().await;
                });
            }
            let body = async {
                let mut stream = connector
                    .connect_stream_with_lane(
                        int_pair.client_addr(),
                        rtp_mux::LaneClass::Interactive,
                    )
                    .await
                    .unwrap();
                let sent =
                    run_mux_interactive_stream(&mut stream, base, msg_bytes, cadence, run_for)
                        .await;
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                // Signal the pump to stop before the straggler drain.  The
                // pump may already have exited on a write failure, in which
                // case its receiver is gone and the send is a no-op; the
                // `pump_failed` watch below is what reports a premature end.
                let _ = bulk_stop_tx.send(true);
                // Drain until every sent message is observed or the sink stops
                // advancing for `grace`.  The write half stays OPEN across the
                // drain: a delivery measurement must keep the sender alive
                // until the receiver has everything, otherwise a graceful
                // close races the last in-flight frames.
                //
                // A fixed grace conflates "the transport lost a message" with
                // "the final buffered messages did not finish draining within
                // the window": the saturating bulk lane can starve the
                // interactive lane's write path for many seconds, so the last
                // writes are accepted just as the window closes and their
                // delivery completes only once the contention stops.  Waiting
                // while the sink keeps advancing measures delivery (a genuinely
                // lost message leaves no progress and trips the `grace`
                // no-progress bound), not the drain rate.
                let mut samples = Vec::new();
                let mut idle_since = Instant::now();
                loop {
                    let before = samples.len() as u64;
                    while let Ok((_tag, latency)) = latencies.try_recv() {
                        samples.push(latency);
                    }
                    if samples.len() as u64 >= sent {
                        break;
                    }
                    if samples.len() as u64 > before {
                        idle_since = Instant::now();
                    } else if idle_since.elapsed() >= grace {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                let _ = stream.shutdown().await;
                let received = samples.len() as u64;
                let bulk_secs = active_for.as_secs_f64();
                let summary = summarize(samples, sent, received, bulk_bytes, bulk_secs);
                print_hol_summary(label, &summary);
                eprintln!(
                    "[hol {label}] int pair stats = {:?}  bulk pair stats = {:?}",
                    combined_stats(&int_pair),
                    combined_stats(&bulk_pair)
                );
                int_pair.stop();
                bulk_pair.stop();
                RtpMuxHolResult {
                    summary,
                    client_interactive_fec: client_metrics.interactive(),
                    client_bulk_fec: client_metrics.bulk(),
                    server_interactive_fec: server_metrics.interactive(),
                    server_bulk_fec: server_metrics.bulk(),
                }
            };
            // The bulk pump must stay live (contending) for the whole
            // interactive measurement: run it to completion, then fail if the
            // pump ended on its own (open or write failure) instead of on the
            // watch stop signal.
            let result = body.await;
            if *pump_failed_rx.borrow() {
                panic!("bulk pump ended before the interactive measurement completed");
            }
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

/// Run an HOL probe on a dual-lane setup: two independent RTP connections
/// (one per lane), each with its own [`NetemPair`].  `interactive_frame` /
/// `bulk_frame` control per-lane frame‑delivery.
///
/// The interactive stream rides the interactive lane; the bulk stream rides
/// the bulk lane.  When both lanes request frame‑delivery the probe routes
/// through the production [`rtp_mux`] composition via
/// [`run_hol_probe_rtp_mux`]; asymmetric lane‑mode diagnostics continue
/// through the lower‑level dual‑mux helpers.
async fn run_hol_probe_dual_lane(
    label: &str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk_c2s: NetemConfig,
    bulk_s2c: NetemConfig,
    config: DualLaneProbeConfig,
) -> HolSummary {
    if config.interactive_frame && config.bulk_frame {
        return run_hol_probe_rtp_mux(label, int_c2s, int_s2c, bulk_c2s, bulk_s2c, config.traffic)
            .await
            .summary;
    }
    let DualLaneProbeConfig {
        interactive_frame,
        bulk_frame,
        traffic,
    } = config;
    let TrafficConfig {
        msg_bytes,
        cadence,
        run_for,
        grace,
    } = traffic;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_via(
                    &task_tx,
                    false,
                    base,
                    interactive_frame,
                    bulk_frame,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            let bulk_pair = NetemPair::spawn(bulk_addr, bulk_c2s, bulk_s2c).unwrap();
            let (opener, _accepter) = dual_mux_client_connect_with_lane_modes_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
                interactive_frame,
                bulk_frame,
            )
            .await
            .unwrap();
            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let active_for = run_for - BULK_RAMP;
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            // `pump_failed_tx` records that the pump ended on its own (open or
            // write failure) rather than on the watch stop signal; only an
            // own-end while the measurement is running is a premature pump end.
            let (pump_failed_tx, pump_failed_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            let mut bulk_tasks = tokio::task::JoinSet::new();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(mux::LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = pump_failed_tx.send(true);
                            return;
                        }
                    };
                    // The pump runs until the watch signals shutdown at the end of
                    // the interactive measurement (or a write failure ends it).
                    // `BULK_NO_STOP` keeps the internal stop flag inert and the long
                    // window is only a backstop so the pump never ends on its own
                    // while the measurement is still running.
                    tokio::select! {
                        _ = bulk_stop_rx.changed() => {}
                        _ = run_delayed_mux_bulk_stream(
                            &mut w,
                            payload,
                            BULK_RAMP,
                            Duration::from_secs(3600),
                            &BULK_NO_STOP,
                            BulkLoad::Saturating,
                        ) => {
                            // The writer returned short of its 3600s backstop and
                            // before the stop signal: the pump ended on a write
                            // failure while the measurement may still be running.
                            let _ = pump_failed_tx.send(true);
                        }
                    }
                    let _ = w.shutdown();
                });
            }
            let (mut rr_read, mut rr_write) =
                opener.open(mux::LaneClass::Interactive).await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = rr_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let body = async {
                let sent =
                    run_mux_interactive_stream(&mut rr_write, base, msg_bytes, cadence, run_for)
                        .await;
                let _ = rr_write.shutdown();
                // Signal the pump to stop before the straggler grace; the
                // epilog join happens after the raced body.
                bulk_stop_tx.send(true).unwrap();
                tokio::time::sleep(grace).await;
                let mut samples = Vec::new();
                while let Ok((_tag, lat)) = latencies.try_recv() {
                    samples.push(lat);
                }
                let received = samples.len() as u64;
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let bulk_secs = active_for.as_secs_f64();
                let summary = summarize(samples, sent, received, bulk_bytes, bulk_secs);
                print_hol_summary(label, &summary);
                eprintln!(
                    "[hol {label}] int pair stats = {:?}  bulk pair stats = {:?}",
                    combined_stats(&int_pair),
                    combined_stats(&bulk_pair)
                );
                int_pair.stop();
                bulk_pair.stop();
                summary
            };
            // The bulk pump must stay live (contending) for the whole
            // interactive measurement: run it to completion, then fail if the
            // pump ended on its own (open or write failure) instead of on the
            // watch stop signal.
            let summary = body.await;
            if *pump_failed_rx.borrow() {
                panic!("bulk pump ended before the interactive measurement completed");
            }
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            summary
        })
        .await
}

/// Dual‑lane HOL probe with TWO interactive streams on the interactive
/// lane (tagged `b'A'` / `b'B'`) plus one bulk stream on the bulk lane.
/// Returns `(summary_a, summary_b, ` combined summary across both, `bulk_mibps)`.
async fn run_hol_probe_dual_lane_two_interactive(
    label: &str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk_c2s: NetemConfig,
    bulk_s2c: NetemConfig,
    config: DualLaneProbeConfig,
) -> (HolSummary, HolSummary, HolSummary, f64) {
    let DualLaneProbeConfig {
        interactive_frame,
        bulk_frame,
        traffic:
            TrafficConfig {
                msg_bytes,
                cadence,
                run_for,
                grace,
            },
    } = config;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies_all, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_via(
                    &task_tx,
                    false,
                    base,
                    interactive_frame,
                    bulk_frame,
                )
                .await
                .unwrap();

            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            let bulk_pair = NetemPair::spawn(bulk_addr, bulk_c2s, bulk_s2c).unwrap();

            let (opener, _accepter) = dual_mux_client_connect_with_lane_modes_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
                interactive_frame,
                bulk_frame,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let active_for = run_for - BULK_RAMP;
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            // `pump_failed_tx` records that the pump ended on its own (open or
            // write failure) rather than on the watch stop signal; only an
            // own-end while the measurement is running is a premature pump end.
            let (pump_failed_tx, pump_failed_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            let mut bulk_tasks = tokio::task::JoinSet::new();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(mux::LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = pump_failed_tx.send(true);
                            return;
                        }
                    };
                    // The pump runs until the watch signals shutdown at the end of
                    // the interactive measurement (or a write failure ends it).
                    // `BULK_NO_STOP` keeps the internal stop flag inert and the long
                    // window is only a backstop so the pump never ends on its own
                    // while the measurement is still running.
                    tokio::select! {
                        _ = bulk_stop_rx.changed() => {}
                        _ = run_delayed_mux_bulk_stream(
                            &mut w,
                            payload,
                            BULK_RAMP,
                            Duration::from_secs(3600),
                            &BULK_NO_STOP,
                            BulkLoad::Saturating,
                        ) => {
                            // The writer returned short of its 3600s backstop and
                            // before the stop signal: the pump ended on a write
                            // failure while the measurement may still be running.
                            let _ = pump_failed_tx.send(true);
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            let (mut read_a, mut write_a) = opener.open_auto();
            let (mut read_b, mut write_b) = opener.open_auto();
            // Parked until the streams close; the owning JoinSet aborts them at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = read_a.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = read_b.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            let body = async {
                if write_a.write_all(b"A").await.is_err() {
                    let _ = write_a.shutdown();
                    drop(write_b);
                    return (
                        HolSummary::default(),
                        HolSummary::default(),
                        HolSummary::default(),
                        0.0,
                    );
                }
                if write_b.write_all(b"B").await.is_err() {
                    let _ = write_b.shutdown();
                    let _ = write_a.shutdown();
                    return (
                        HolSummary::default(),
                        HolSummary::default(),
                        HolSummary::default(),
                        0.0,
                    );
                }
                let fut_a =
                    send_timestamped_messages(&mut write_a, base, msg_bytes, cadence, run_for);
                let fut_b =
                    send_timestamped_messages(&mut write_b, base, msg_bytes, cadence, run_for);
                let (sent_a, sent_b) = tokio::join!(fut_a, fut_b);
                let _ = write_a.shutdown();
                let _ = write_b.shutdown();
                // Signal the pump to stop before the straggler grace; the
                // epilog join happens after the raced body.
                bulk_stop_tx.send(true).unwrap();

                tokio::time::sleep(grace).await;
                let mut samples = Vec::new();
                let mut samples_a = Vec::new();
                let mut samples_b = Vec::new();
                while let Ok((tag, lat)) = latencies_all.try_recv() {
                    samples.push(lat);
                    if tag == b'A' {
                        samples_a.push(lat);
                    } else if tag == b'B' {
                        samples_b.push(lat);
                    }
                }

                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let bulk_secs = active_for.as_secs_f64();
                let bulk_mibps = if bulk_secs > 0.0 {
                    bulk_bytes as f64 / (1024.0 * 1024.0) / bulk_secs
                } else {
                    0.0
                };

                let n_all = samples.len() as u64;
                let combined = summarize(samples, sent_a + sent_b, n_all, bulk_bytes, bulk_secs);
                let summary_a =
                    summarize(samples_a.clone(), sent_a, samples_a.len() as u64, 0, 0.0);
                let summary_b =
                    summarize(samples_b.clone(), sent_b, samples_b.len() as u64, 0, 0.0);

                eprintln!(
                    "[hol {label} A] p50={p50_a:.1} p99={p99_a:.1} max={max_a:.1}",
                    p50_a = summary_a.p50,
                    p99_a = summary_a.p99,
                    max_a = summary_a.max,
                );
                eprintln!(
                    "[hol {label} B] p50={p50_b:.1} p99={p99_b:.1} max={max_b:.1}",
                    p50_b = summary_b.p50,
                    p99_b = summary_b.p99,
                    max_b = summary_b.max,
                );
                print_hol_summary(&format!("{label}_combined"), &combined);
                eprintln!(
                    "[hol {label}] int pair stats = {:?}  bulk pair stats = {:?}",
                    combined_stats(&int_pair),
                    combined_stats(&bulk_pair),
                );

                int_pair.stop();
                bulk_pair.stop();
                (summary_a, summary_b, combined, bulk_mibps)
            };
            // The bulk pump must stay live (contending) for the whole
            // interactive measurement: run it to completion, then fail if the
            // pump ended on its own (open or write failure) instead of on the
            // watch stop signal.
            let result = body.await;
            if *pump_failed_rx.borrow() {
                panic!("bulk pump ended before the interactive measurement completed");
            }
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

/// Two‑interactive baseline on a single frame‑delivery RTP connection:
/// two tagged streams (b'A'/b'B'), no bulk contender.
async fn run_frame_delivery_two_interactive(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    config: FrameDeliveryProbeConfig,
) -> (HolSummary, HolSummary, HolSummary) {
    let FrameDeliveryProbeConfig {
        fec,
        traffic:
            TrafficConfig {
                msg_bytes,
                cadence,
                run_for,
                grace,
            },
    } = config;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, mut latencies, _bulk_counter) =
                spawn_mux_frame_delivery_latency_bulk_server_via(&task_tx, fec, base)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let (reader, writer) =
                rtp_frame_delivery_connect_via(&task_tx, pair.client_addr(), fec).await;
            let config = mux::MuxConfig {
                initiation: mux::Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: true,
            };
            let mut spawner = tokio::task::JoinSet::new();
            let (opener, _accepter) =
                mux::spawn_mux_no_reconnection(reader, writer, config, &mut spawner);
            // The mux supervision is drained by a non-required background
            // task: these echo lanes let the session tear down normally (FIN
            // exchanged) before the measurement body finishes, so a required
            // task would panic on that normal early completion. A panicked
            // supervision task still surfaces at scope end, and the JoinSet
            // is dropped when the task completes, aborting any stragglers.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    if let Some(Err(err)) = spawner.join_next().await {
                        panic!("mux client session supervision failed: {err:?}");
                    }
                }),
            );
            let (mut read_a, mut write_a) = opener.open().await.unwrap();
            let (mut read_b, mut write_b) = opener.open().await.unwrap();
            // Parked until the streams close; the owning JoinSet aborts them at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = read_a.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = read_b.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let _ = write_a.write_all(b"A").await;
            let _ = write_b.write_all(b"L").await;
            let fut_a = send_timestamped_messages(&mut write_a, base, msg_bytes, cadence, run_for);
            let fut_b = send_timestamped_messages(&mut write_b, base, msg_bytes, cadence, run_for);
            let (sent_a, sent_b) = tokio::join!(fut_a, fut_b);
            let _ = write_a.shutdown();
            let _ = write_b.shutdown();
            tokio::time::sleep(grace).await;
            let mut samples = Vec::new();
            let mut samples_a = Vec::new();
            let mut samples_b = Vec::new();
            while let Ok((tag, lat)) = latencies.try_recv() {
                samples.push(lat);
                if tag == b'A' {
                    samples_a.push(lat);
                } else if tag == b'L' {
                    samples_b.push(lat);
                }
            }
            let combined = summarize(
                samples.clone(),
                sent_a + sent_b,
                samples.len() as u64,
                0,
                0.0,
            );
            let summary_a = summarize(samples_a.clone(), sent_a, samples_a.len() as u64, 0, 0.0);
            let summary_b = summarize(samples_b.clone(), sent_b, samples_b.len() as u64, 0, 0.0);
            eprintln!(
                "[hol {} A] p50={:.1} p99={:.1} max={:.1}",
                label, summary_a.p50, summary_a.p99, summary_a.max
            );
            eprintln!(
                "[hol {} B] p50={:.1} p99={:.1} max={:.1}",
                label, summary_b.p50, summary_b.p99, summary_b.max
            );
            print_hol_summary(&format!("{}_combined", label), &combined);
            pair.stop();
            (summary_a, summary_b, combined)
        })
        .await
}

/// The per-flow tag byte for multi-flow frame-delivery probes. The server
/// routes every stream whose first byte is not `b'B'` through its latency
/// parser (`b'B'` is the reserved bulk-sink tag), so the two-interactive
/// battery's second flow already uses `b'L'`; every later flow gets a fresh
/// distinct letter (`b'C'`, `b'D', ...) so each flow's samples are
/// attributable and no flow silently lands in another flow's bucket.
fn flow_tag(flow: usize) -> u8 {
    match flow {
        0 => b'A',
        1 => b'L',
        _ => b'A' + flow as u8,
    }
}

/// Run a frame-delivery probe with `flows` interactive streams on ONE mux
/// connection over ONE frame-delivery RTP connection: the two-interactive
/// pattern generalized. Every stream tags its first message with a distinct
/// [`flow_tag`] byte so the server routes it through the latency parser and
/// its samples are bucketed per flow; the returned vector is indexed by flow
/// with the combined summary last. The single connection is the point of the
/// arm — four interactive flows sharing one frame path — and the link is
/// exactly the two-interactive battery's GE5 seed pair so the per-flow
/// percentiles are comparable.
async fn run_frame_delivery_multi_interactive(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    flows: usize,
    config: FrameDeliveryProbeConfig,
) -> (Vec<HolSummary>, HolSummary) {
    assert!(
        (1..=7).contains(&flows),
        "{label}: flows {flows} out of the A..H (b'B' reserved) tag range"
    );
    let FrameDeliveryProbeConfig {
        fec,
        traffic:
            TrafficConfig {
                msg_bytes,
                cadence,
                run_for,
                grace,
            },
    } = config;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, mut latencies, _bulk_counter) =
                spawn_mux_frame_delivery_latency_bulk_server_via(&task_tx, fec, base)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let (reader, writer) =
                rtp_frame_delivery_connect_via(&task_tx, pair.client_addr(), fec).await;
            let config = mux::MuxConfig {
                initiation: mux::Initiation::Client,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: true,
            };
            let mut spawner = tokio::task::JoinSet::new();
            let (opener, _accepter) =
                mux::spawn_mux_no_reconnection(reader, writer, config, &mut spawner);
            // The mux supervision is drained by a non-required background
            // task: these echo lanes let the session tear down normally (FIN
            // exchanged) before the measurement body finishes, so a required
            // task would panic on that normal early completion. A panicked
            // supervision task still surfaces at scope end, and the JoinSet
            // is dropped when the task completes, aborting any stragglers.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    if let Some(Err(err)) = spawner.join_next().await {
                        panic!("mux client session supervision failed: {err:?}");
                    }
                }),
            );
            let mut streams = Vec::with_capacity(flows);
            for flow in 0..flows {
                let (mut read, write) = opener.open().await.unwrap();
                // Parked until the streams close; the owning JoinSet aborts
                // them at scope end.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                streams.push((flow_tag(flow), write));
            }

            let mut sent_per_flow = vec![0u64; flows];
            let mut writes = Vec::with_capacity(flows);
            for (tag, write) in streams.iter_mut() {
                let _ = write.write_all(&[*tag]).await;
                writes.push(&mut *write);
            }

            // All flows offered concurrently, joined before shutdown.
            let mut futs = Vec::with_capacity(flows);
            for write in writes {
                futs.push(send_timestamped_messages(
                    write, base, msg_bytes, cadence, run_for,
                ));
            }
            for (i, fut) in futs.into_iter().enumerate() {
                sent_per_flow[i] = fut.await;
            }
            for (_, write) in streams.iter_mut() {
                let _ = write.shutdown();
            }

            tokio::time::sleep(grace).await;
            let mut samples: Vec<f64> = Vec::new();
            let mut per_flow: Vec<Vec<f64>> = vec![Vec::new(); flows];
            while let Ok((tag, lat)) = latencies.try_recv() {
                samples.push(lat);
                if let Some(flow) = (0..flows).find(|&i| flow_tag(i) == tag) {
                    per_flow[flow].push(lat);
                }
            }
            let total_sent: u64 = sent_per_flow.iter().sum();
            let combined = summarize(samples.clone(), total_sent, samples.len() as u64, 0, 0.0);
            let mut summaries = Vec::with_capacity(flows);
            for flow in 0..flows {
                let summary = summarize(
                    per_flow[flow].clone(),
                    sent_per_flow[flow],
                    per_flow[flow].len() as u64,
                    0,
                    0.0,
                );
                eprintln!(
                    "[hol {} flow {}] p50={:.1} p99={:.1} max={:.1}",
                    label, flow, summary.p50, summary.p99, summary.max
                );
                summaries.push(summary);
            }
            print_hol_summary(&format!("{}_combined", label), &combined);
            pair.stop();
            (summaries, combined)
        })
        .await
}

// ═══════════════════════════════════════════════════════════════════════════════
// Frame‑delivery & dual‑lane scenarios (all #[ignore])
// ═══════════════════════════════════════════════════════════════════════════════

// ───── single‑connection frame‑delivery ─────

/// rtt100 GE5 shared frame-delivery: the bulk stream shares the connection
/// and is SATURATING (see the module header) — a single run restoring the
/// measured saturation load. The p99 gate was dropped with pacing; under the
/// saturating load the delivery gate is the robust correctness signal,
/// matching the other GE5 shared rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_shared_frame_delivery() {
    let label = "rtt100 GE5 shared frame-delivery";
    let summary = with_timeout(
        Duration::from_secs(180),
        label,
        run_hol_probe_frame_delivery_shared(
            label,
            rtt100_ge5(21),
            rtt100_ge5(22),
            false,
            BulkLoad::Saturating,
            TrafficConfig {
                msg_bytes: DEFAULT_MSG_BYTES,
                cadence: DEFAULT_CADENCE,
                run_for: DEFAULT_RUN_FOR,
                grace: DEFAULT_GRACE,
            },
        ),
    )
    .await;
    assert!(
        summary.delivery_pct >= 0.95,
        "delivery {:.3} < 0.95",
        summary.delivery_pct
    );
}

/// Deterministic regression for the PACED bulk mode: three runs of the rtt100
/// GE5 shared frame-delivery probe with [`BulkLoad::Paced`] — the only
/// scenario that still paces its bulk flow (see the module header) — must
/// hold delivery/p50 on every run and the MEDIAN p99 gate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_paced_bulk_median_p99_regression() {
    let label = "rtt100 GE5 shared frame-delivery paced-bulk regression";
    let mut runs = Vec::with_capacity(3);
    for i in 0..3 {
        let run_label = format!("{label} run{}", i + 1);
        let summary = with_timeout(
            Duration::from_secs(180),
            &run_label,
            run_hol_probe_frame_delivery_shared(
                &run_label,
                rtt100_ge5(21),
                rtt100_ge5(22),
                false,
                BulkLoad::Paced,
                TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            ),
        )
        .await;
        runs.push(summary);
    }
    assert_triple_run_gates(label, &runs, 100.0, 400.0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_two_interactive_frame_delivery() {
    let label = "rtt100 GE5 two-interactive frame-delivery";
    let (summary_a, summary_b, combined) = with_timeout(
        Duration::from_secs(120),
        label,
        run_frame_delivery_two_interactive(
            label,
            rtt100_ge5(31),
            rtt100_ge5(32),
            FrameDeliveryProbeConfig {
                fec: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(combined.delivery_pct >= 0.90, "combined delivery too low");
    let solo_ref = 50.0;
    assert!(
        summary_a.p50 <= solo_ref * 2.5,
        "stream A p50 {:.1} > {:.0}",
        summary_a.p50,
        solo_ref * 2.5,
    );
    assert!(
        summary_b.p50 <= solo_ref * 2.5,
        "stream B p50 {:.1} > {:.0}",
        summary_b.p50,
        solo_ref * 2.5,
    );
}

/// Multi-flow interactive scaling beyond the two-interactive battery: four
/// interactive streams sharing ONE frame-delivery connection at the same GE5
/// link as the two-interactive arm, asserting the outcome triad per flow —
/// delivery >= 0.90 and p50 <= 125 ms (the solo reference x2.5) — plus the
/// combined delivery >= 0.90. The single-flow HOL battery and the
/// two-interactive arm cover one and two flows; the inventory called
/// multi-flow (4/8) scaling never-measured, and this arm closes the 4-flow
/// rung.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_four_interactive_frame_delivery() {
    const FLOWS: usize = 4;
    let label = "rtt100 GE5 four-interactive frame-delivery";
    let (summaries, combined) = with_timeout(
        Duration::from_secs(180),
        label,
        run_frame_delivery_multi_interactive(
            label,
            rtt100_ge5(31),
            rtt100_ge5(32),
            FLOWS,
            FrameDeliveryProbeConfig {
                fec: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(
        combined.delivery_pct >= 0.90,
        "combined delivery {:.3} < 0.90",
        combined.delivery_pct
    );
    let solo_ref = 50.0;
    for (flow, summary) in summaries.iter().enumerate() {
        let name = flow_tag(flow) as char;
        assert!(
            summary.delivery_pct >= 0.90,
            "flow {name} delivery {:.3} < 0.90",
            summary.delivery_pct
        );
        assert!(
            summary.p50 <= solo_ref * 2.5,
            "flow {name} p50 {:.1} > {:.0} ms",
            summary.p50,
            solo_ref * 2.5,
        );
    }
}

// ───── diagnostics: frame‑delivery shared on various link profiles ─────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_clean_shared_frame_delivery_diag() {
    let label = "rtt100 clean shared frame-delivery diag";
    let summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_frame_delivery_shared(
            label,
            rtt100_clean(41),
            rtt100_clean(42),
            false,
            BulkLoad::Saturating,
            TrafficConfig {
                msg_bytes: DEFAULT_MSG_BYTES,
                cadence: DEFAULT_CADENCE,
                run_for: DEFAULT_RUN_FOR,
                grace: DEFAULT_GRACE,
            },
        ),
    )
    .await;
    assert!(summary.delivery_pct > 0.0, "no delivery");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge1_shared_frame_delivery_diag() {
    let label = "rtt100 GE1 shared frame-delivery diag";
    let summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_frame_delivery_shared(
            label,
            rtt100_ge1_loss1(51),
            rtt100_ge1_loss1(52),
            false,
            BulkLoad::Saturating,
            TrafficConfig {
                msg_bytes: DEFAULT_MSG_BYTES,
                cadence: DEFAULT_CADENCE,
                run_for: DEFAULT_RUN_FOR,
                grace: DEFAULT_GRACE,
            },
        ),
    )
    .await;
    assert!(summary.delivery_pct > 0.0, "no delivery");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_hostile_shared_frame_delivery_diag() {
    let label = "hostile shared frame-delivery diag";
    let summary = with_timeout(
        Duration::from_secs(300),
        label,
        run_hol_probe_frame_delivery_shared(
            label,
            hostile_real_link_seeded(61),
            hostile_real_link_seeded(62),
            false,
            BulkLoad::Saturating,
            TrafficConfig {
                msg_bytes: DEFAULT_MSG_BYTES,
                cadence: Duration::from_millis(200),
                run_for: DEFAULT_RUN_FOR,
                grace: DEFAULT_GRACE,
            },
        ),
    )
    .await;
    assert!(summary.delivery_pct > 0.0, "no delivery");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_cap400_shared_frame_delivery_diag() {
    let label = "cap400 shared frame-delivery diag";
    let summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_frame_delivery_shared(
            label,
            cap400(71),
            cap400(72),
            false,
            BulkLoad::Saturating,
            TrafficConfig {
                msg_bytes: DEFAULT_MSG_BYTES,
                cadence: DEFAULT_CADENCE,
                run_for: DEFAULT_RUN_FOR,
                grace: DEFAULT_GRACE,
            },
        ),
    )
    .await;
    assert!(summary.delivery_pct > 0.0, "no delivery");
}

// ───── dual‑lane: stock lanes ─────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_shared_dual_lane() {
    let label = "rtt100 GE5 shared dual-lane";
    let summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_dual_lane(
            label,
            rtt100_ge5(91),
            rtt100_ge5(92),
            rtt100_ge5(93),
            rtt100_ge5(94),
            DualLaneProbeConfig {
                interactive_frame: false,
                bulk_frame: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(
        summary.delivery_pct >= 0.95,
        "delivery {:.3} < 0.95",
        summary.delivery_pct
    );
}

// ───── dual‑lane: both lanes frame‑delivery ─────

#[test]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
fn hol_rtt100_ge5_shared_dual_lane_frame_delivery() {
    let label = "rtt100 GE5 shared dual-lane frame-delivery";
    // `run_bounded` (rather than `#[tokio::test]`) keeps the wall-clock
    // deadline alive across the implicit runtime teardown: this row leaves the
    // shared bulk lane's rtp session alive with a large in-flight set, and
    // `Runtime::drop` waits unboundedly on its non-yielding write driver.
    let summary = run_bounded(label, Duration::from_secs(180), async {
        with_timeout(
            Duration::from_secs(120),
            label,
            run_hol_probe_dual_lane(
                label,
                rtt100_ge5(101),
                rtt100_ge5(102),
                rtt100_ge5(103),
                rtt100_ge5(104),
                DualLaneProbeConfig {
                    interactive_frame: true,
                    bulk_frame: true,
                    traffic: TrafficConfig {
                        msg_bytes: DEFAULT_MSG_BYTES,
                        cadence: DEFAULT_CADENCE,
                        run_for: DEFAULT_RUN_FOR,
                        grace: DEFAULT_GRACE,
                    },
                },
            ),
        )
        .await
    });
    assert!(
        summary.delivery_pct >= 0.999,
        "delivery {:.3} < 0.999",
        summary.delivery_pct
    );
}

// ───── asymmetric: interactive frame, bulk stock ─────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_shared_dual_lane_asym_frame_diag() {
    let label = "rtt100 GE5 shared dual-lane asym frame diag";
    let summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_dual_lane(
            label,
            rtt100_ge5(111),
            rtt100_ge5(112),
            rtt100_ge5(113),
            rtt100_ge5(114),
            DualLaneProbeConfig {
                interactive_frame: true,
                bulk_frame: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(summary.delivery_pct > 0.0, "no delivery");
}

// ───── two‑interactive intra‑lane isolation ─────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_dual_lane_two_interactive_stock_diag() {
    let label = "rtt100 GE5 dual-lane two-interactive stock";
    let (summary_a, summary_b, _combined, _bulk) = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_dual_lane_two_interactive(
            label,
            rtt100_ge5(121),
            rtt100_ge5(122),
            rtt100_ge5(123),
            rtt100_ge5(124),
            DualLaneProbeConfig {
                interactive_frame: false,
                bulk_frame: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(summary_a.delivery_pct > 0.0, "stream A no delivery");
    assert!(summary_b.delivery_pct > 0.0, "stream B no delivery");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn hol_rtt100_ge5_dual_lane_two_interactive_frame_diag() {
    let label = "rtt100 GE5 dual-lane two-interactive frame";
    let (summary_a, summary_b, _combined, _bulk) = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_dual_lane_two_interactive(
            label,
            rtt100_ge5(131),
            rtt100_ge5(132),
            rtt100_ge5(133),
            rtt100_ge5(134),
            DualLaneProbeConfig {
                interactive_frame: true,
                bulk_frame: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(summary_a.delivery_pct > 0.0, "stream A no delivery");
    assert!(summary_b.delivery_pct > 0.0, "stream B no delivery");
}

// ───── separate‑listener: asymmetric frame delivery + teardown ─────

/// Run an HOL probe on a dual-lane setup with SEPARATE interactive and bulk
/// listeners.  Each lane's RTP frame‑delivery mode is fixed at accept time
/// on its dedicated listener, so the interactive listener can use
/// frame‑delivery while the bulk listener uses stock byte‑stream — the
/// server never has to guess the lane class before the mux lane‑hello.
async fn run_hol_probe_dual_lane_separate_listeners(
    label: &str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk_c2s: NetemConfig,
    bulk_s2c: NetemConfig,
    config: DualLaneProbeConfig,
) -> HolSummary {
    let DualLaneProbeConfig {
        interactive_frame,
        bulk_frame,
        traffic:
            TrafficConfig {
                msg_bytes,
                cadence,
                run_for,
                grace,
            },
    } = config;
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_via(
                    &task_tx,
                    false,
                    base,
                    interactive_frame,
                    bulk_frame,
                )
                .await
                .unwrap();

            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            let bulk_pair = NetemPair::spawn(bulk_addr, bulk_c2s, bulk_s2c).unwrap();

            let (opener, _accepter) = dual_mux_client_connect_with_lane_modes_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
                interactive_frame,
                bulk_frame,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let active_for = run_for - BULK_RAMP;
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            // `pump_failed_tx` records that the pump ended on its own (open or
            // write failure) rather than on the watch stop signal; only an
            // own-end while the measurement is running is a premature pump end.
            let (pump_failed_tx, pump_failed_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            let mut bulk_tasks = tokio::task::JoinSet::new();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(mux::LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = pump_failed_tx.send(true);
                            return;
                        }
                    };
                    // The pump runs until the watch signals shutdown at the end of
                    // the interactive measurement (or a write failure ends it).
                    // `BULK_NO_STOP` keeps the internal stop flag inert and the long
                    // window is only a backstop so the pump never ends on its own
                    // while the measurement is still running.
                    tokio::select! {
                        _ = bulk_stop_rx.changed() => {}
                        _ = run_delayed_mux_bulk_stream(
                            &mut w,
                            payload,
                            BULK_RAMP,
                            Duration::from_secs(3600),
                            &BULK_NO_STOP,
                            BulkLoad::Saturating,
                        ) => {
                            // The writer returned short of its 3600s backstop and
                            // before the stop signal: the pump ended on a write
                            // failure while the measurement may still be running.
                            let _ = pump_failed_tx.send(true);
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            let (mut rr_read, mut rr_write) =
                opener.open(mux::LaneClass::Interactive).await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = rr_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let body = async {
                let sent =
                    run_mux_interactive_stream(&mut rr_write, base, msg_bytes, cadence, run_for)
                        .await;
                let _ = rr_write.shutdown();

                // Signal the pump to stop before the straggler grace; the
                // epilog join happens after the raced body.
                bulk_stop_tx.send(true).unwrap();

                tokio::time::sleep(grace).await;
                let mut samples = Vec::new();
                while let Ok((_tag, lat)) = latencies.try_recv() {
                    samples.push(lat);
                }

                let received = samples.len() as u64;
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let bulk_secs = active_for.as_secs_f64();
                let summary = summarize(samples, sent, received, bulk_bytes, bulk_secs);

                print_hol_summary(label, &summary);
                eprintln!(
                    "[hol {label}] int pair stats = {:?}  bulk pair stats = {:?}",
                    combined_stats(&int_pair),
                    combined_stats(&bulk_pair),
                );
                int_pair.stop();
                bulk_pair.stop();
                summary
            };
            // The bulk pump must stay live (contending) for the whole
            // interactive measurement: run it to completion, then fail if the
            // pump ended on its own (open or write failure) instead of on the
            // watch stop signal.
            let summary = body.await;
            if *pump_failed_rx.borrow() {
                panic!("bulk pump ended before the interactive measurement completed");
            }
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            summary
        })
        .await
}

/// Asymmetric frame‑delivery dual‑lane test: interactive lane uses
/// frame‑delivery (on its own dedicated listener), bulk lane uses stock
/// byte‑stream (on its own dedicated listener).  Under 5 % Gilbert‑Elliott
/// loss the probe must deliver messages and tear down cleanly without
/// wedging the runtime.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; slow end-to-end probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dual_lane_asym_frame_delivers_and_tears_down() {
    let label = "asym separate-listener frame teardown";
    let summary = with_timeout(
        Duration::from_secs(120),
        label,
        run_hol_probe_dual_lane_separate_listeners(
            label,
            rtt100_ge5(141),
            rtt100_ge5(142),
            rtt100_ge5(143),
            rtt100_ge5(144),
            DualLaneProbeConfig {
                interactive_frame: true,
                bulk_frame: false,
                traffic: TrafficConfig {
                    msg_bytes: DEFAULT_MSG_BYTES,
                    cadence: DEFAULT_CADENCE,
                    run_for: DEFAULT_RUN_FOR,
                    grace: DEFAULT_GRACE,
                },
            },
        ),
    )
    .await;
    assert!(
        summary.delivery_pct >= 0.95,
        "delivery {:.3} < 0.95 — adapter may be wedged",
        summary.delivery_pct
    );
}
