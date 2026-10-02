//! The bulk stand-off's **multiplicative-decrease factor** (`beta`): does a
//! gentler competing decrease take delivered share from a TCP-shaped (AIMD)
//! competitor, and what does the queue that share needs cost in RTT?
//!
//! # Why this arm exists
//!
//! `standoff_burst` and `standoff_nonconvergent` measure *whether the stand-off
//! competes at all*.  They cannot measure whether the competition is **strong
//! enough**: the stand-off's multiplicative decrease and the reference AIMD
//! competitor's are one shared constant (`0.5`), so every recorded split is a
//! comparison of two identical controls.  On TCP's own terms a fair split is
//! the only possible outcome — the arm cannot tell "our competing response
//! works" from "our competing response is a no-op".
//!
//! AIMD throughput goes as `sqrt(alpha * (1 + beta) / (2 * p * (1 - beta))) / RTT`.
//! If our bulk's decrease on a loss is *gentler* than the competitor's, our bulk
//! takes a larger steady-state share of the same bottleneck:
//! `beta 0.5 -> 0.75` predicts `1.53x` the competitor's rate (`share ~0.60`),
//! `0.5 -> 0.9` predicts `~2.5x` (`share ~0.72`), and `beta = 1.0` is the
//! non-elastive end (the rate is held through a loss instead of retreated).
//!
//! The lever is untested because the decrease factor is not reachable per side:
//! [`rtp::cc::CcSignalHub::with_standoff_decrease_factor`] makes it so, scoped
//! to the bulk lane's stand-off (the reference competitor keeps
//! `REFERENCE_AIMD_DECREASE_FACTOR`).
//!
//! # The arm
//!
//! One window per (rep, beta).  The four beta values are run **interleaved
//! within each rep**, so a slow drift (thermal, allocator, cache) is common
//! mode across the four cells instead of being attributed to beta:
//!
//! * our bulk lane: a `Dedicated` rtp flow carrying the same cross-lane CC link
//!   the deployment attaches, saturating its sink, with the interactive lane
//!   **idle** for the whole window — the regime the product requirement names
//!   ("out-compete TCP while the interactive lane is not running traffic");
//! * the competitor: the same saturating rtp flow with the test-only AIMD
//!   reference law (`reference_aimd = true`, `Dedicated`);
//! * both cross **one** `BottleneckShaper(8 388 608 bit/s, 128 KiB)` drop-tail
//!   queue — the same buffer the interactive lane has to cross.
//!
//! The interactive lane is the mux harness's (`frame_reordering`, the
//! deployment's interactive config) so its CC presence, offer clock and
//! cross-lane payload are the production machinery; it is opened and then
//! offers nothing, so the stand-off's quiet window is open across the measured
//! part of the run.
//!
//! # Readings (per beta)
//!
//! * **share** — our bulk lane's fraction of the two bulk flows' delivered
//!   bytes, integrated from both cumulative byte counters over the window's
//!   measured tail (after the stand-off window and the AIMD ramps have
//!   settled);
//! * **control RTT** — our bulk connection's own `congestion_control_rtt`,
//!   mean and p95 over the same tail; the RTT inflation is this reading against
//!   the `beta = 0.5` control's;
//! * **bottleneck queue** — the shaper's sampled backlog, mean and max over the
//!   tail, converted to the standing queueing delay it is worth at the shaper
//!   rate (`bytes * 8 / rate`): the queue a larger share is bought with, and
//!   the quantity the interactive lane would have to cross;
//! * **gate** — the stand-off's own input (`offered_quiet_for`,
//!   `CcSignal::is_shared`, the windowed loss sample), so "the gate never
//!   opened" is distinguishable from "the gate opened and the gentler factor
//!   still did not take share".
//!
//! # Assertions
//!
//! Instrument sanity only; the lever's direction is the *reading*, reported with
//! its paired confidence interval — a flat sweep is a result (the lever is
//! dead), not a failure to launder.  Each assertion has a named vacuity
//! demonstration in `GATE.md`:
//!
//! * the gate actually opened in the measured window (a disarmed hub during the
//!   survey produced `NEVER`; see `GATE.md`);
//! * every beta's hub read back the beta it ran under (hardcoding the factor to
//!   the default made all four identical);
//! * both bulk flows delivered bytes and the pair together saturated the
//!   shaper, so the share is against a competitor that contested the link;
//! * the `beta = 0.5` control split near fair, so the topology itself is not
//!   lopsided.
//!
//! Run:
//! ```sh
//! cargo test --release -p rtp_mux --test standoff_beta_sweep -- --ignored --nocapture --test-threads=1
//! ```

use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{BottleneckShaper, NetemConfig, NetemPair};
use rtp::cc::{CcSignalHub, STANDOFF_WINDOW};
use rtp::metrics::{MetricsCongestionAction, MetricsEvent, MetricsObserver};
use rtp::testkit::rtp::{spawn_rtp_bulk_upload_with_options_via, spawn_rtp_byte_sink_server_via};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via_cc_link,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use rtp_mux::testkit::payload::{BYTE_SINK_BULK_CHUNK_BYTES, byte_sink_payload, saturate};
use rtp_mux::testkit::profile::{JITTER, OWD, SHAPER_LIMIT_BYTES, SHAPER_RATE_BPS};
use rtp_mux::testkit::standoff::SAMPLE_STEP;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// The deployment link profile, the shared shaper and the stand-off sampling
// cadence are declared once in `rtp_mux::testkit`.

/// One window: setup + the stand-off window + the measured tail.
const RUN_FOR: Duration = Duration::from_secs(12);
/// The measured tail starts here.  By four seconds the stand-off's 1500 ms
/// quiet window has been open for ~2.5 s and both AIMD flows have left their
/// initial ramps.
const MEASURE_FROM: Duration = Duration::from_secs(4);
/// Drains stragglers before the counters are read.
const GRACE: Duration = Duration::from_secs(1);
/// Interleaved reps: every rep runs all four betas.  Reported with the paired
/// sd/sem, the 95 % CI and the minimum detectable effect, so a delta inside the
/// instrument's noise is stated as such rather than read as an effect.
const REPS: usize = 8;
/// The sweep.  `0.5` is the control: the stand-off and the reference competitor
/// then share one factor and a fair split is the only possible reading.
const BETAS: [f64; 4] = [0.5, 0.75, 0.9, 1.0];
/// The aggregate the two bulk flows must deliver, as a fraction of the shaper's
/// serialization capacity over the measured tail, for the share to be a
/// comparison against a competitor that contested the link.  Instrument floor,
/// not a product bound (rtp's own loss A/B sets its analogous floor at 0.80).
const SATURATION_FLOOR: f64 = 0.5;
/// The measured tail's control share must sit inside this band: the stand-off
/// and the competitor then run the *same* law, and a lopsided topology (or an
/// unarmed stand-off) would land outside it.  The one-rep-per-cell survey read
/// `0.521`, so this is a sanity band, not a product bound.
const CONTROL_SHARE_BAND: (f64, f64) = (0.35, 0.65);

/// This arm's own fault namespace.  `BETA_SWEEP_FAULT=OFFER_CONTINUOUSLY`
/// makes the interactive lane offer a `300 B` frame every `200 ms` for the
/// whole window, so the stand-off's `STANDOFF_WINDOW` quiet window never
/// elapses: the arm then measures the shipped delay-first policy rather than
/// the competing response, and its gate/quiet-time assertions fail by name.
/// It is the demonstration that those assertions can fail from the measurement
/// path.  Unset (the default) leaves the lane quiet, exactly as the arm ships.
fn beta_sweep_fault(name: &str) -> bool {
    std::env::var("BETA_SWEEP_FAULT").is_ok_and(|value| value.trim() == name)
}

fn link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: JITTER,
        seed,
        ..NetemConfig::default()
    }
}

fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

/// One observation of our bulk controller, read from the public metrics
/// surface: the action it took, the rate it settled on and its own control RTT.
#[derive(Clone, Copy)]
struct BulkSample {
    t: f64,
    rate: f64,
    control_rtt_ms: f64,
    loss_ratio: Option<f64>,
    action: Option<MetricsCongestionAction>,
}

#[derive(Default)]
struct BulkTaps {
    samples: AtomicU64,
    probe: AtomicU64,
    loss: AtomicU64,
    drain: AtomicU64,
    gentle: AtomicU64,
    hold: AtomicU64,
    timeline: Mutex<Vec<BulkSample>>,
}

fn bulk_observer(base: Instant, taps: Arc<BulkTaps>) -> MetricsObserver {
    MetricsObserver::filtered(
        |event, _| event == MetricsEvent::RttSample,
        move |observation| {
            let Some(snapshot) = observation.snapshot else {
                return;
            };
            taps.samples.fetch_add(1, Ordering::Relaxed);
            match snapshot.congestion_action {
                Some(MetricsCongestionAction::BandwidthProbe) => {
                    taps.probe.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::LossBackoff) => {
                    taps.loss.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::DelayDrain) => {
                    taps.drain.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::GentleProbe) => {
                    taps.gentle.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::QueueHold) => {
                    taps.hold.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            taps.timeline.lock().unwrap().push(BulkSample {
                t: base.elapsed().as_secs_f64(),
                rate: snapshot.send_rate_packets_per_second,
                control_rtt_ms: snapshot
                    .congestion_control_rtt
                    .map(|rtt| rtt.as_secs_f64() * 1000.0)
                    .unwrap_or(f64::NAN),
                loss_ratio: snapshot.congestion_loss_ratio,
                action: snapshot.congestion_action,
            });
        },
    )
}

/// The shaper's own queue over the measured tail: the mean and max backlog, in
/// bytes, and the standing queueing delay each is worth at the shaper rate.
#[derive(Clone, Copy, Debug, Default)]
struct QueueRead {
    mean_bytes: f64,
    max_bytes: u64,
    mean_ms: f64,
    max_ms: f64,
}

/// One (rep, beta) window's raw readings.
struct RawRun {
    beta: f64,
    /// `(t, our_delivered_bytes, competitor_delivered_bytes)`.
    counter_samples: Vec<(f64, u64, u64)>,
    /// `(t, offered_quiet_seconds, is_shared)` from the path's CC signal.
    gate_samples: Vec<(f64, Option<f64>, bool)>,
    bulk: Vec<BulkSample>,
    queue: QueueRead,
    shaper_dropped: u64,
    taps: (u64, u64, u64, u64, u64, u64),
    beta_readback: f64,
}

/// One integration of our bulk lane's share of the two flows' delivered bytes,
/// over the intervals a caller's predicate keeps.
#[derive(Clone, Copy, Debug, Default)]
struct ShareRead {
    share: f64,
    our: u64,
    comp: u64,
    /// Wall time the kept intervals cover: the integration's own sample-count
    /// analogue, so "no share" is distinguishable from "no samples".
    secs: f64,
}

/// Integrate the share over the intervals of the measured tail whose midpoint
/// falls in `[from, to)` and for which `keep` (indexed like the sampler's own
/// push order) holds.  The byte counters and the gate are pushed in the same
/// sampler iteration, so their indices align; a tail mismatch is a wiring
/// defect, not a datum.
fn share_over(
    counter_samples: &[(f64, u64, u64)],
    gates: &[(f64, Option<f64>, bool)],
    from: f64,
    to: f64,
    keep: impl Fn(usize) -> bool,
) -> ShareRead {
    assert_eq!(
        counter_samples.len(),
        gates.len(),
        "the byte-counter and CC-gate samplers must be pushed in lockstep"
    );
    let mut our = 0u64;
    let mut comp = 0u64;
    let mut secs = 0.0f64;
    for (i, pair) in counter_samples.windows(2).enumerate() {
        let (t0, o0, c0) = pair[0];
        let (t1, o1, c1) = pair[1];
        let mid = 0.5 * (t0 + t1);
        if mid >= from && mid < to && keep(i) {
            our += o1.saturating_sub(o0);
            comp += c1.saturating_sub(c0);
            secs += t1 - t0;
        }
    }
    let total = our + comp;
    ShareRead {
        share: if total == 0 {
            f64::NAN
        } else {
            our as f64 / total as f64
        },
        our,
        comp,
        secs,
    }
}

/// Control RTT over `[from, to]`: mean and p95, in ms, plus the sample count.
fn rtt_over(bulk: &[BulkSample], from: f64, to: f64) -> (f64, f64, u64) {
    let mut xs: Vec<f64> = bulk
        .iter()
        .filter(|s| s.t >= from && s.t < to && s.control_rtt_ms.is_finite())
        .map(|s| s.control_rtt_ms)
        .collect();
    if xs.is_empty() {
        return (f64::NAN, f64::NAN, 0);
    }
    let n = xs.len() as u64;
    let mean = xs.iter().sum::<f64>() / n as f64;
    xs.sort_by(|a, b| a.total_cmp(b));
    let idx = (((n as f64) * 0.95) as usize).min(xs.len() - 1);
    (mean, xs[idx], n)
}

/// The largest windowed loss ratio our bulk connection sampled over the tail.
/// The stand-off's claim is vetoed when this reaches `CC_DATA_LOSS_RATE` (0.2),
/// so a reading below it means the gate was not vetoed.
fn max_loss_over(bulk: &[BulkSample], from: f64, to: f64) -> f64 {
    bulk.iter()
        .filter(|s| s.t >= from && s.t < to)
        .filter_map(|s| s.loss_ratio)
        .fold(0.0f64, f64::max)
}

/// One window: our bulk vs the AIMD reference competitor on one shared shaper,
/// the interactive lane open but idle, the stand-off's factor `beta`.
async fn run_window(beta: f64) -> RawRun {
    let base = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            // The mux harness supplies the interactive lane and its CC signal;
            // its own bulk lane is left unopened.  The product bulk lane this
            // window measures is a direct `rtp` `Dedicated` connection on the
            // same `(src, dst)` path with the same CC link the deployment
            // attaches, so the stand-off is read without a second variable (the
            // mux stream's own flow control) between the controller and the
            // wire.
            let (int_addr, bulk_addr, _latencies, _bulk_counter, _server_tx) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let (our_addr, our_counter) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let (comp_addr, comp_counter) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let shaper = BottleneckShaper::new(SHAPER_RATE_BPS, SHAPER_LIMIT_BYTES);
            let int_pair =
                NetemPair::spawn_shared(int_addr, link(41), link(42), Some(shaper.clone()), None)
                    .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(bulk_addr, link(43), link(44), Some(shaper.clone()), None)
                    .unwrap();
            let our_pair =
                NetemPair::spawn_shared(our_addr, link(47), link(48), Some(shaper.clone()), None)
                    .unwrap();
            let comp_pair =
                NetemPair::spawn_shared(comp_addr, link(45), link(46), Some(shaper.clone()), None)
                    .unwrap();
            // Our side's stand-off runs `beta`; the competitor is the reference
            // law (its own factor untouched).
            let hub = CcSignalHub::with_standoff_decrease_factor(beta);
            let product_cc = Some(rtp::cc::CcLink::new(hub.clone(), rtp::cc::CcRole::Bulk));
            let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
            let gate = hub.group(loopback, loopback).bulk();
            let beta_readback = gate.standoff_decrease_factor();
            let taps = Arc::new(BulkTaps::default());
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via_cc_link(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
                Some(hub),
            )
            .await
            .unwrap();
            // Open the interactive lane so it registers on the path and
            // publishes its (idle) payload, then write nothing on it: this is
            // the regime the requirement names.  Its queue in the latency sink
            // is never read; the lane exists only to be idle.
            let offer_continuously = beta_sweep_fault("OFFER_CONTINUOUSLY");
            let (mut int_read, mut int_write) =
                opener.open(mux::LaneClass::Interactive).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    if offer_continuously {
                        let frame = vec![0u8; 300];
                        while int_write.write_all(&frame).await.is_ok() {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        return;
                    }
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = int_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let mut our_write = spawn_rtp_bulk_upload_with_options_via(
                &task_tx,
                our_pair.client_addr(),
                false,
                rtp::CongestionLane::Dedicated,
                rtp::FrameMode::default(),
                false,
                Some(bulk_observer(base, Arc::clone(&taps))),
                product_cc,
            )
            .await
            .unwrap();
            let mut comp_write = spawn_rtp_bulk_upload_with_options_via(
                &task_tx,
                comp_pair.client_addr(),
                false,
                rtp::CongestionLane::Dedicated,
                rtp::FrameMode::default(),
                true,
                None,
                None,
            )
            .await
            .unwrap();

            // Samplers: both cumulative byte counters, the shared buffer's own
            // backlog (over the measured tail only), and the path's CC gate.
            let counters = Arc::new(Mutex::new(Vec::<(f64, u64, u64)>::new()));
            let gate_samples = Arc::new(Mutex::new(Vec::<(f64, Option<f64>, bool)>::new()));
            let backlog_sum = Arc::new(AtomicU64::new(0));
            let backlog_max = Arc::new(AtomicU64::new(0));
            let backlog_count = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            {
                let our = Arc::clone(&our_counter);
                let comp = Arc::clone(&comp_counter);
                let sink = Arc::clone(&counters);
                let gate_sink = Arc::clone(&gate_samples);
                let gate = gate.clone();
                let stop = Arc::clone(&stop);
                let sum = Arc::clone(&backlog_sum);
                let max = Arc::clone(&backlog_max);
                let count = Arc::clone(&backlog_count);
                let shaper = shaper.clone();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        while !stop.load(Ordering::Relaxed) {
                            let t = base.elapsed().as_secs_f64();
                            sink.lock().unwrap().push((
                                t,
                                our.load(Ordering::Relaxed),
                                comp.load(Ordering::Relaxed),
                            ));
                            gate_sink.lock().unwrap().push((
                                t,
                                gate.offered_quiet_for().map(|d| d.as_secs_f64()),
                                gate.is_shared(),
                            ));
                            if t >= MEASURE_FROM.as_secs_f64() && t < RUN_FOR.as_secs_f64() {
                                let backlog = shaper.backlog_bytes(Instant::now());
                                sum.fetch_add(backlog, Ordering::Relaxed);
                                max.fetch_max(backlog, Ordering::Relaxed);
                                count.fetch_add(1, Ordering::Relaxed);
                            }
                            tokio::time::sleep(SAMPLE_STEP).await;
                        }
                    }),
                );
            }

            let payload = byte_sink_payload(BYTE_SINK_BULK_CHUNK_BYTES);
            let our_payload = payload.clone();
            let comp_payload = payload.clone();
            // No leading tag byte on either flow: `spawn_rtp_byte_sink_server_via`
            // verifies `(offset + j) % 251` from offset 0 with no tag skip, so a
            // one-byte prefix desynchronises the sink's phase and its delivered
            // counter freezes (silently -- the read does not error) until a read
            // happens to realign.  Both sinks here are plain `rtp` byte sinks, so
            // both writers start their period-aligned payload at byte 0.
            let our_fut = async move {
                saturate(&mut our_write, &our_payload, RUN_FOR).await;
            };
            let comp_fut = async move {
                saturate(&mut comp_write, &comp_payload, RUN_FOR).await;
            };
            let ((), ()) = tokio::join!(our_fut, comp_fut);

            tokio::time::sleep(GRACE).await;
            stop.store(true, Ordering::Relaxed);
            let counter_samples = counters.lock().unwrap().clone();
            let gate_samples = gate_samples.lock().unwrap().clone();
            let backlog_samples = backlog_count.load(Ordering::Relaxed);
            let backlog_mean = if backlog_samples == 0 {
                0.0
            } else {
                backlog_sum.load(Ordering::Relaxed) as f64 / backlog_samples as f64
            };
            let backlog_max = backlog_max.load(Ordering::Relaxed);
            let shaper_dropped = shaper.dropped();
            let bulk = taps.timeline.lock().unwrap().clone();
            let taps_out = (
                taps.samples.load(Ordering::Relaxed),
                taps.probe.load(Ordering::Relaxed),
                taps.loss.load(Ordering::Relaxed),
                taps.drain.load(Ordering::Relaxed),
                taps.gentle.load(Ordering::Relaxed),
                taps.hold.load(Ordering::Relaxed),
            );
            int_pair.stop();
            bulk_pair.stop();
            our_pair.stop();
            comp_pair.stop();
            let ms_per_byte = 8.0 * 1000.0 / SHAPER_RATE_BPS as f64;
            RawRun {
                beta,
                counter_samples,
                gate_samples,
                bulk,
                queue: QueueRead {
                    mean_bytes: backlog_mean,
                    max_bytes: backlog_max,
                    mean_ms: backlog_mean * ms_per_byte,
                    max_ms: backlog_max as f64 * ms_per_byte,
                },
                shaper_dropped,
                taps: taps_out,
                beta_readback,
            }
        })
        .await
}

/// One (rep, beta) cell's derived readings.
struct Cell {
    beta: f64,
    /// The primary reading: the share integrated over the stand-off's
    /// quiet-elapsed intervals in the measured tail.
    share: f64,
    our_bytes: u64,
    comp_bytes: u64,
    /// The whole measured tail, including the mux keepalive's post-offer yield
    /// window (a `beta`-independent instrument artifact).
    share_whole: f64,
    /// The share integrated only over intervals where the claim was also clear
    /// of R1's loss veto -- the closed-loop "while competing" reading.
    share_armed: f64,
    quiet_secs: f64,
    armed_secs: f64,
    rtt_mean_ms: f64,
    rtt_p95_ms: f64,
    max_loss: f64,
    queue: QueueRead,
    gate_open_at: Option<f64>,
    gate_open_fraction: f64,
    claim_armed_fraction: f64,
    taps: (u64, u64, u64, u64, u64, u64),
    beta_readback: f64,
}

fn derive(raw: &RawRun) -> Cell {
    let from = MEASURE_FROM.as_secs_f64();
    let to = RUN_FOR.as_secs_f64();
    let (rtt_mean, rtt_p95, _n) = rtt_over(&raw.bulk, from, to);
    // The stand-off's claim is armed when the quiet window has elapsed AND R1's
    // loss veto has not fired.  The veto reads the windowed loss sample this
    // bulk's own controller saw, so the nearest preceding `RttSample` snapshot
    // is the honest pairing: a sample with no loss reading has not vetoed.  This
    // is a *closed-loop* state of our own flow (a higher `beta` fills the queue
    // and so vetoes more often), reported as a duty cycle rather than used to
    // condition the primary reading.
    let loss_blocks = |t: f64| -> bool {
        raw.bulk
            .iter()
            .rev()
            .find(|s| s.t <= t)
            .and_then(|s| s.loss_ratio)
            .is_some_and(|loss| loss >= 0.2)
    };
    let quiet_elapsed = |q: Option<f64>| q.is_some_and(|q| q >= STANDOFF_WINDOW.as_secs_f64());
    // The primary reading integrates only over the intervals the stand-off's
    // quiet window had elapsed for.  The interactive lane carries the mux
    // harness's own keepalive (one frame about every five seconds), and an
    // offer re-opens the yield window for `STANDOFF_WINDOW`; that artifact is
    // independent of `beta` and common to every cell, so excluding its
    // intervals leaves the cells one dimension apart.  The whole-tail reading
    // is reported beside it.
    let quiet =
        |i: usize| quiet_elapsed(raw.gate_samples[i].1) && quiet_elapsed(raw.gate_samples[i + 1].1);
    let armed = |i: usize| {
        quiet(i) && !loss_blocks(raw.gate_samples[i].0) && !loss_blocks(raw.gate_samples[i + 1].0)
    };
    let whole = share_over(&raw.counter_samples, &raw.gate_samples, from, to, |_| true);
    let quiet_read = share_over(&raw.counter_samples, &raw.gate_samples, from, to, quiet);
    let armed_read = share_over(&raw.counter_samples, &raw.gate_samples, from, to, armed);
    let window: Vec<&(f64, Option<f64>, bool)> = raw
        .gate_samples
        .iter()
        .filter(|(t, _, _)| *t >= from && *t < to)
        .collect();
    let open = window
        .iter()
        .filter(|(_, quiet, _)| quiet.is_some_and(|q| q >= STANDOFF_WINDOW.as_secs_f64()))
        .count();
    let gate_open_fraction = if window.is_empty() {
        f64::NAN
    } else {
        open as f64 / window.len() as f64
    };
    let gate_open_at = raw
        .gate_samples
        .iter()
        .find(|(_, quiet, _)| quiet.is_some_and(|q| q >= STANDOFF_WINDOW.as_secs_f64()))
        .map(|(t, _, _)| *t);
    let claim_armed = window
        .iter()
        .filter(|(t, quiet, _)| quiet_elapsed(*quiet) && !loss_blocks(*t))
        .count();
    let claim_armed_fraction = if window.is_empty() {
        f64::NAN
    } else {
        claim_armed as f64 / window.len() as f64
    };
    Cell {
        beta: raw.beta,
        share: quiet_read.share,
        our_bytes: quiet_read.our,
        comp_bytes: quiet_read.comp,
        share_whole: whole.share,
        share_armed: armed_read.share,
        quiet_secs: quiet_read.secs,
        armed_secs: armed_read.secs,
        rtt_mean_ms: rtt_mean,
        rtt_p95_ms: rtt_p95,
        max_loss: max_loss_over(&raw.bulk, from, to),
        queue: raw.queue,
        gate_open_at,
        gate_open_fraction,
        claim_armed_fraction,
        taps: raw.taps,
        beta_readback: raw.beta_readback,
    }
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn sd(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return f64::NAN;
    }
    let m = mean(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns threads and binds ephemeral ports; 8 interleaved 12 s windows per beta; run with --ignored --nocapture --test-threads=1"]
async fn bulk_standoff_beta_controls_the_share_against_an_aimd_competitor() {
    let dir = "target/standoff-beta-sweep";
    std::fs::create_dir_all(dir).unwrap();
    let mut cells: Vec<(usize, Cell)> = Vec::new();
    let mut cells_csv = String::from(
        "rep,beta,share,share_whole,share_armed,quiet_secs,armed_secs,our_bytes,comp_bytes,\
         rtt_mean_ms,rtt_p95_ms,max_loss,queue_mean_b,queue_max_b,queue_mean_ms,queue_max_ms,\
         gate_open_fraction,claim_armed_fraction,gate_open_at,probes,losses,drains,gentles,\
         holds,samples,beta_readback\n",
    );
    let mut counters_csv = String::from("rep,beta,t,our_bytes,comp_bytes\n");
    let mut gate_csv = String::from("rep,beta,t,offered_quiet_s,shared\n");
    let mut rtt_csv = String::from("rep,beta,t,control_rtt_ms,rate_pps,action\n");

    for rep in 0..REPS {
        // Interleave the four betas inside the rep and alternate the order by
        // rep, so a slow drift is common mode across the cells rather than
        // attributed to beta.
        let mut order: Vec<f64> = BETAS.to_vec();
        if rep % 2 == 1 {
            order.reverse();
        }
        for beta in order {
            let raw = run_window(beta).await;
            let cell = derive(&raw);
            eprintln!(
                "[beta] rep{rep} beta {beta:.2}  share {:6.4} (whole {:6.4} armed {:6.4})  \
                 (our {} / comp {} B over {:.2} s)  rtt mean {:6.2} p95 {:6.2} ms  \
                 queue mean {:7.1} max {:7.1} B ({:5.2}/{:6.2} ms)  gate open {:5.1}%  \
                 armed {:5.1}%  maxloss {:.3}  actions probe {} loss {} drain {} gentle {} \
                 hold {} of {}  shaper dropped {}  beta-readback {:.2}",
                cell.share,
                cell.share_whole,
                cell.share_armed,
                cell.our_bytes,
                cell.comp_bytes,
                cell.quiet_secs,
                cell.rtt_mean_ms,
                cell.rtt_p95_ms,
                cell.queue.mean_bytes,
                cell.queue.max_bytes,
                cell.queue.mean_ms,
                cell.queue.max_ms,
                cell.gate_open_fraction * 100.0,
                cell.claim_armed_fraction * 100.0,
                cell.max_loss,
                cell.taps.1,
                cell.taps.2,
                cell.taps.3,
                cell.taps.4,
                cell.taps.5,
                cell.taps.0,
                raw.shaper_dropped,
                cell.beta_readback,
            );
            cells_csv.push_str(&format!(
                "{rep},{beta:.2},{:.6},{:.6},{:.6},{:.3},{:.3},{},{},{:.3},{:.3},{:.4},{:.1},{},\
                 {:.3},{:.3},{:.3},{:.3},{},{},{},{},{},{},{},{:.2}\n",
                cell.share,
                cell.share_whole,
                cell.share_armed,
                cell.quiet_secs,
                cell.armed_secs,
                cell.our_bytes,
                cell.comp_bytes,
                cell.rtt_mean_ms,
                cell.rtt_p95_ms,
                cell.max_loss,
                cell.queue.mean_bytes,
                cell.queue.max_bytes,
                cell.queue.mean_ms,
                cell.queue.max_ms,
                cell.gate_open_fraction,
                cell.claim_armed_fraction,
                cell.gate_open_at.unwrap_or(f64::NAN),
                cell.taps.1,
                cell.taps.2,
                cell.taps.3,
                cell.taps.4,
                cell.taps.5,
                cell.taps.0,
                cell.beta_readback,
            ));
            for (t, our, comp) in &raw.counter_samples {
                counters_csv.push_str(&format!("{rep},{beta:.2},{t:.3},{our},{comp}\n"));
            }
            for (t, quiet, shared) in &raw.gate_samples {
                gate_csv.push_str(&format!(
                    "{rep},{beta:.2},{t:.3},{},{shared}\n",
                    quiet.map(|q| format!("{q:.4}")).unwrap_or_default(),
                ));
            }
            for s in &raw.bulk {
                rtt_csv.push_str(&format!(
                    "{rep},{beta:.2},{:.3},{:.3},{:.1},{:?}\n",
                    s.t, s.control_rtt_ms, s.rate, s.action,
                ));
            }
            cells.push((rep, cell));
        }
    }
    std::fs::write(format!("{dir}/cells.csv"), &cells_csv).unwrap();
    std::fs::write(format!("{dir}/counters.csv"), &counters_csv).unwrap();
    std::fs::write(format!("{dir}/gate.csv"), &gate_csv).unwrap();
    std::fs::write(format!("{dir}/rtt.csv"), &rtt_csv).unwrap();

    // ---- per-beta aggregates -------------------------------------------------
    let cap_bytes = (SHAPER_RATE_BPS as f64 / 8.0) * (RUN_FOR - MEASURE_FROM).as_secs_f64();
    let control: Vec<f64> = cells
        .iter()
        .filter(|(_, c)| c.beta == BETAS[0])
        .map(|(_, c)| c.share)
        .collect();
    let control_mean = mean(&control);
    eprintln!(
        "[beta] shaper capacity over the measured tail {:8.0} B ({} s at {} bit/s)",
        cap_bytes,
        (RUN_FOR - MEASURE_FROM).as_secs_f32(),
        SHAPER_RATE_BPS,
    );
    eprintln!(
        "[beta] beta  n  share_mean  share_sd  paired_delta  ci95_lo  ci95_hi  mde80  \
         rtt_mean_ms  rtt_inflation  queue_mean_ms  queue_max_ms  aggregate_frac  \
         beta_gain_pred  beta_gain_obs  gain_over_inflation  (whole / armed)"
    );
    let mut summary = String::from(
        "beta,n,share_mean,share_sd,paired_delta_mean,paired_delta_sd,ci95_lo,ci95_hi,mde80,\
         rtt_mean_ms,rtt_inflation,queue_mean_ms,queue_max_ms,aggregate_fraction,\
         beta_gain_pred,beta_gain_obs,gain_over_inflation,share_whole,share_armed\n",
    );
    let mut verdicts: Vec<(f64, f64, f64, f64, f64, f64, f64)> = Vec::new();
    for beta in BETAS {
        let sh: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c.share)
            .collect();
        let sh_whole: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c.share_whole)
            .collect();
        let sh_armed: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c.share_armed)
            .collect();
        let rtt: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c.rtt_mean_ms)
            .collect();
        let qmean: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c.queue.mean_ms)
            .collect();
        let qmax: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c.queue.max_ms)
            .collect();
        // The pair's aggregate as a fraction of the shaper's capacity *over the
        // intervals the reading covers* (`quiet_secs`), so a cell that
        // integrated only a little of the tail is scored against the link it
        // actually crossed.
        let agg: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| {
                let cap = (SHAPER_RATE_BPS as f64 / 8.0) * c.quiet_secs;
                if cap > 0.0 {
                    (c.our_bytes + c.comp_bytes) as f64 / cap
                } else {
                    f64::NAN
                }
            })
            .collect();
        let paired: Vec<f64> = (0..REPS)
            .filter_map(|rep| {
                let c0 = cells
                    .iter()
                    .find(|(r, c)| *r == rep && c.beta == BETAS[0])
                    .map(|(_, c)| c.share)?;
                let cb = cells
                    .iter()
                    .find(|(r, c)| *r == rep && c.beta == beta)
                    .map(|(_, c)| c.share)?;
                Some(cb - c0)
            })
            .collect();
        let d_mean = mean(&paired);
        let d_sd = sd(&paired);
        let sem = d_sd / (paired.len() as f64).sqrt();
        let (ci_lo, ci_hi) = if sem.is_finite() {
            (d_mean - 1.96 * sem, d_mean + 1.96 * sem)
        } else {
            (f64::NAN, f64::NAN)
        };
        let mde = 2.802 * sem;
        let rtt_mean = mean(&rtt);
        let inflation = rtt_mean
            / mean(
                &cells
                    .iter()
                    .filter(|(_, c)| c.beta == BETAS[0])
                    .map(|(_, c)| c.rtt_mean_ms)
                    .collect::<Vec<f64>>(),
            );
        let share_mean = mean(&sh);
        // The formula's predicted rate ratio against the 0.5 competitor:
        // sqrt((1+beta)/(1-beta)) / sqrt(1.5/0.5).
        let gain_pred = ((1.0 + beta) / (1.0 - beta)).sqrt() / (1.5f64 / 0.5).sqrt();
        let gain_obs = share_mean / (1.0 - share_mean);
        eprintln!(
            "[beta] {beta:.2}  {}  {:10.4}  {:8.4}  {:+11.4}  {:+7.4}  {:+7.4}  {:6.4}  \
             {:11.2}  {:12.3}  {:13.2}  {:12.2}  {:14.3}  {:14.3}  {:13.3}  {:19.3}  \
             (whole {:10.4} armed {:10.4})",
            sh.len(),
            share_mean,
            sd(&sh),
            d_mean,
            ci_lo,
            ci_hi,
            mde,
            rtt_mean,
            inflation,
            mean(&qmean),
            mean(&qmax),
            mean(&agg),
            gain_pred,
            gain_obs,
            gain_obs / inflation,
            mean(&sh_whole),
            mean(&sh_armed),
        );
        summary.push_str(&format!(
            "{beta:.2},{},{share_mean:.6},{:.6},{d_mean:.6},{d_sd:.6},{ci_lo:.6},{ci_hi:.6},\
             {mde:.6},{rtt_mean:.4},{inflation:.4},{:.4},{:.4},{:.6},{gain_pred:.4},\
             {gain_obs:.4},{:.4},{whole:.6},{armed:.6}\n",
            sh.len(),
            sd(&sh),
            mean(&qmean),
            mean(&qmax),
            mean(&agg),
            gain_obs / inflation,
            whole = mean(&sh_whole),
            armed = mean(&sh_armed),
        ));
        verdicts.push((
            beta,
            share_mean,
            d_mean,
            ci_lo,
            ci_hi,
            gain_obs,
            gain_obs / inflation,
        ));
    }
    std::fs::write(format!("{dir}/summary.csv"), &summary).unwrap();

    // ---- the control's own reading ------------------------------------------
    eprintln!(
        "[beta] control (beta 0.5) mean share {control_mean:.4} \
         (band {:?}); the control runs the SAME law as the competitor",
        CONTROL_SHARE_BAND
    );

    // ---- the queue-vs-gain verdict, computed from this run's own numbers -----
    for (beta, share, d_mean, ci_lo, ci_hi, gain, product) in &verdicts {
        let verdict = if *beta == BETAS[0] {
            "control".to_string()
        } else if *ci_lo > 0.0 {
            format!("gain resolved (+{d_mean:.3}, CI [{ci_lo:+.3},{ci_hi:+.3}])")
        } else {
            format!("gain NOT resolved (+{d_mean:.3}, CI [{ci_lo:+.3},{ci_hi:+.3}])")
        };
        eprintln!(
            "[beta] verdict beta {beta:.2}: share {share:.4}, gain_obs {gain:.3}, \
             (gain_obs)/(RTT inflation) {product:.3} -> {verdict}"
        );
    }

    // ---- assertions: instrument sanity only ---------------------------------
    // Ordered so the *policy-under-measurement* checks report first: a fault
    // that closes the stand-off's gate must name the gate, not a downstream
    // "no bytes integrated" symptom.
    for (rep, cell) in &cells {
        assert!(
            cell.gate_open_fraction > 0.75,
            "[beta] rep{rep} beta {:.2}: the stand-off's quiet window had elapsed for only {:.1}% \
             of the measured tail (first elapsed at {:?} s) — the interactive lane was offering \
             through the window, so the arm measured the shipped delay-first policy rather than \
             the competing response",
            cell.beta,
            cell.gate_open_fraction * 100.0,
            cell.gate_open_at,
        );
        assert!(
            cell.beta_readback == cell.beta,
            "[beta] rep{rep} beta {:.2}: the hub read back {:.2} — the sweep did not apply the \
             factor it labelled the cell with",
            cell.beta,
            cell.beta_readback,
        );
        assert!(
            cell.our_bytes > 0 && cell.comp_bytes > 0,
            "[beta] rep{rep} beta {:.2}: our bulk delivered {} B and the competitor {} B over the \
             quiet-elapsed intervals — one bulk flow was absent, so the share is undefined",
            cell.beta,
            cell.our_bytes,
            cell.comp_bytes,
        );
        assert!(
            cell.share.is_finite(),
            "[beta] rep{rep} beta {:.2}: no delivered bytes landed in the measured tail",
            cell.beta,
        );
        // The primary reading's own sufficiency: the quiet-elapsed intervals it
        // integrated must cover enough of the tail to be a measurement.  The
        // mux keepalive re-opens the yield window once per keepalive period, so
        // the shortfall from a whole tail is expected; a lane that never stops
        // offering (the arm this one is a corrected counterpart of) integrates
        // nothing and fails here by name.
        assert!(
            cell.quiet_secs >= 5.0,
            "[beta] rep{rep} beta {:.2}: the share integrated only {:.2} s of quiet-elapsed \
             intervals in the {:.0} s measured tail",
            cell.beta,
            cell.quiet_secs,
            RUN_FOR.as_secs_f32() - MEASURE_FROM.as_secs_f32(),
        );
        // R1's loss veto is a closed-loop state of our own flow, so its duty is
        // reported above; the arm only needs the competing branch to have run
        // long enough to read it.
        assert!(
            cell.armed_secs >= 1.0,
            "[beta] rep{rep} beta {:.2}: the stand-off's claim was armed (quiet window elapsed \
             and R1's loss veto clear, max windowed loss {:.3}) for only {:.2} s of the \
             quiet-elapsed intervals: the competing branch was not the policy under measurement",
            cell.beta,
            cell.max_loss,
            cell.armed_secs,
        );
    }
    let control_ok = control_mean >= CONTROL_SHARE_BAND.0 && control_mean <= CONTROL_SHARE_BAND.1;
    assert!(
        control_ok,
        "[beta] the beta 0.5 control's mean share {control_mean:.4} sits outside {:?}: the \
         stand-off and the competitor run the same law there, so the topology (or the stand-off \
         arming) is lopsided and the sweep's deltas would be read against a biased control",
        CONTROL_SHARE_BAND,
    );
    for beta in BETAS {
        let agg: Vec<f64> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| (c.our_bytes + c.comp_bytes) as f64 / cap_bytes)
            .collect();
        let agg_mean = mean(&agg);
        assert!(
            agg_mean >= SATURATION_FLOOR,
            "[beta] beta {beta:.2}: the two bulk flows together delivered {agg_mean:.3} of the \
             shaper's capacity over the measured tail, below the {SATURATION_FLOOR:.2} \
             saturation floor — the competitor was not contesting the link, so the share is not \
             a comparison",
        );
    }
    eprintln!("[beta] data: {dir}/cells.csv, counters.csv, gate.csv, rtt.csv, summary.csv");
}
