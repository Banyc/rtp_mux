//! The bulk lane's **interactive stand-off**: does it reclaim the link during
//! the interactive lane's idle gaps without spiking the interactive tail when
//! the lane resumes?
//!
//! # The mechanism
//!
//! `rtp`'s bulk (`Dedicated`) lane carries a cross-lane CC link.  While our own
//! interactive lane is active on the same egress path the bulk lane keeps the
//! shipped delay-first policy (yield/drain).  Once the interactive lane has
//! been quiet for longer than `rtp::cc::STANDOFF_WINDOW` the bulk lane competes
//! against an external loss-based flow on TCP's terms (an absolute additive
//! increase, a 0.5 multiplicative decrease per loss event), and when the
//! interactive lane resumes it drains below its competing rate for
//! `STANDOFF_HOLD` so the queue it filled is cleared before the first
//! interactive packets traverse it.
//!
//! # The arm
//!
//! Two arms, interleaved rep by rep, one dimension apart:
//!
//! * `standoff` — the CC hub attached, the stand-off armed (production).
//! * `yield` — the same hub attached but disarmed
//!   (`CcSignalHub::without_standoff`), so the path signal still suppresses the
//!   loss gate exactly as it does today and the *only* difference is the
//!   stand-off.
//!
//! Both arms share **one bottleneck shaper** (1 MiB/s, 128 KiB drop-tail) with
//! a saturating rtp AIMD reference, so the two bulk flows genuinely contend for
//! the queue the interactive lane crosses.  The interactive lane is **bursty**:
//! a short burst every few seconds, with idle gaps longer than the stand-off
//! window.
//!
//! # Readings
//!
//! * **idle-gap bulk share** — the product bulk lane's fraction of the two bulk
//!   flows' bytes delivered in the late part of each gap (after the stand-off
//!   window has elapsed), sampled from both cumulative byte counters.  The
//!   mechanism must raise it materially toward the fair split.
//! * **resume tail** — the interactive one-way p99 and max over the window
//!   around each resume.  Filling the buffer during the gap is only acceptable
//!   if the hold keeps the transition tail near the yield arm's.
//!
//! Report-only in the tier sense (the printed table and the CSVs are the
//! deliverable); the assertions are instrument sanity and the two product
//! properties, and every one is vacuity-checked by the mechanism's own probes
//! (see `GATE.md`).
//!
//! Run:
//! ```sh
//! cargo test --release -p rtp_mux --test standoff_burst -- --ignored --nocapture --test-threads=1
//! ```

use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::stats::summarize;
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{BottleneckShaper, NetemConfig, NetemPair};
use rtp::cc::CcSignalHub;
use rtp::metrics::{MetricsCongestionAction, MetricsEvent, MetricsObserver};
use rtp::testkit::rtp::{spawn_rtp_bulk_upload_with_options_via, spawn_rtp_byte_sink_server_via};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via_cc_link,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use rtp_mux::testkit::payload::{BYTE_SINK_BULK_CHUNK_BYTES, byte_sink_payload, saturate};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// One-way delay, matching the mandate arms.
const OWD: Duration = Duration::from_millis(25);
const JITTER: Duration = Duration::from_millis(5);
/// The interactive message size (a typical game ping).
const MSG_BYTES: usize = 256;
/// Cadence within a burst.
const CADENCE: Duration = Duration::from_millis(5);
/// The burst shape: a short active stretch, a long idle gap (several times
/// `STANDOFF_WINDOW`, so the stand-off genuinely arms inside each gap).
const BURST_ON: Duration = Duration::from_millis(250);
const BURST_OFF: Duration = Duration::from_secs(4);
/// Delay before the first burst, so the lane handshakes settle first.
const FIRST_BURST: Duration = Duration::from_millis(500);
const BURSTS: usize = 4;
/// The whole window: setup + bursts + a closing drain.
const RUN_FOR: Duration = Duration::from_secs(14);
/// Drains stragglers before the summary is read.
const GRACE: Duration = Duration::from_secs(1);
/// Interleaved reps per arm.  The run-to-run spread is reported beside the
/// effect; an effect smaller than the spread is not an effect.  Sixteen reps
/// put the sign test's 5 % threshold at 12/16 and make the paired delta's 95 %
/// CI tight enough to resolve the effect the predecessors could not: on the
/// eight-rep arm the paired sd was ~0.23, so sixteen reps carry a ~0.11
/// half-width and an ~0.14 minimum detectable effect at 80 % power.
const REPS: usize = 16;
/// A rep whose bulk or competitor connection delivered no bytes is not a
/// sample of the mechanism -- the flow was absent.  Re-run it (a bounded
/// number of times) rather than folding a degenerate ratio into the spread.
const MAX_REP_ATTEMPTS: usize = 4;
/// The shared bottleneck both bulk flows and the interactive lane cross.
const SHAPER_RATE_BPS: u64 = 8_388_608; // 1 MiB/s
const SHAPER_LIMIT_BYTES: u64 = 128 * 1024;
/// How often both bulk byte counters are sampled.  The share is integrated
/// over these samples, so the cadence is the share's time resolution.
const SAMPLE_STEP: Duration = Duration::from_millis(5);
/// The part of each gap the share is read over: from the moment the stand-off
/// window has elapsed (plus slack for the interactive staleness horizon) to the
/// next burst.  A gap shorter than this contributes no samples.
const GAP_MEASURE_TAIL: Duration = Duration::from_millis(1600);
/// The resume window: the burst plus one hold and a scheduling margin.
const RESUME_SPAN: Duration = Duration::from_millis(750);
/// How many interactive samples after each resume the hold's first packets are
/// read from.  The whole-window p99 is repair-dominated (a ~357 ms plateau), so
/// the hold's queue contribution is only visible in the first few packets of
/// each resume, before a repair has had time to fire.
const RESUME_FIRST_N: usize = 8;

/// The two arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    /// The stand-off armed (production).
    Standoff,
    /// The same CC link, stand-off disarmed: the shipped delay-first policy.
    Yield,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Standoff => "standoff",
            Arm::Yield => "yield",
        }
    }
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

/// One sample of the bulk lane's CC gate on the shared path: how long the
/// interactive lane has gone without an application *offer*, and whether the
/// path currently reads as shared.  This is the stand-off's own input gate,
/// read from the same `CcSignal` the bulk controller consumes.
#[derive(Clone, Copy)]
struct CcSample {
    t: f64,
    offered_quiet: Option<f64>,
    shared: bool,
}

/// One observation of the bulk controller, read from the public metrics
/// surface: the send rate it had settled on and the action it took.  Sampled
/// beside the gate, it shows whether the lane responded to the gate opening.
#[derive(Clone, Copy)]
struct ActionSample {
    t: f64,
    rate: f64,
    action: Option<MetricsCongestionAction>,
}

/// One arm/rep's raw readings.
struct RawRun {
    counter_samples: Vec<(f64, u64, u64)>,
    samples: Vec<(f64, f64)>,
    burst_starts: Vec<f64>,
    bulk_bytes: u64,
    comp_bytes: u64,
    actions: BulkActions,
    cc_samples: Vec<CcSample>,
    action_timeline: Vec<ActionSample>,
    shaper_dropped: u64,
    shaper_backlog_max: u64,
}

/// The mux bulk lane's controller actions over one rep, read from the public
/// metrics surface: the instrument sanity that the stand-off actually decided
/// rather than being starved before it could.
#[derive(Default, Clone, Copy)]
struct BulkActions {
    samples: u64,
    drain: u64,
    loss: u64,
    probe: u64,
    gentle: u64,
    hold: u64,
}

struct ActionTaps {
    base: Instant,
    samples: AtomicU64,
    drain: AtomicU64,
    loss: AtomicU64,
    probe: AtomicU64,
    gentle: AtomicU64,
    hold: AtomicU64,
    timeline: Mutex<Vec<ActionSample>>,
}

impl ActionTaps {
    fn new(base: Instant) -> Self {
        Self {
            base,
            samples: AtomicU64::new(0),
            drain: AtomicU64::new(0),
            loss: AtomicU64::new(0),
            probe: AtomicU64::new(0),
            gentle: AtomicU64::new(0),
            hold: AtomicU64::new(0),
            timeline: Mutex::new(Vec::new()),
        }
    }
}

fn action_observer(taps: Arc<ActionTaps>) -> MetricsObserver {
    MetricsObserver::filtered(
        |event, _| event == MetricsEvent::RttSample,
        move |observation| {
            let Some(snapshot) = observation.snapshot else {
                return;
            };
            taps.samples.fetch_add(1, Ordering::Relaxed);
            match snapshot.congestion_action {
                Some(MetricsCongestionAction::DelayDrain) => {
                    taps.drain.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::LossBackoff) => {
                    taps.loss.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::BandwidthProbe) => {
                    taps.probe.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::GentleProbe) => {
                    taps.gentle.fetch_add(1, Ordering::Relaxed);
                }
                Some(MetricsCongestionAction::QueueHold) => {
                    taps.hold.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            taps.timeline.lock().unwrap().push(ActionSample {
                t: taps.base.elapsed().as_secs_f64(),
                rate: snapshot.send_rate_packets_per_second,
                action: snapshot.congestion_action,
            });
        },
    )
}

/// One arm/rep's derived readings.
struct Run {
    /// Late-gap share of the two bulk flows' delivered bytes.
    gap_share: f64,
    bulk_bytes: u64,
    comp_bytes: u64,
    /// Interactive one-way p99 / max over each resume window, pooled.
    resume_p99: f64,
    resume_max: f64,
    /// Max latency over the first [`RESUME_FIRST_N`] interactive samples after
    /// each resume, pooled: the hold's own queue contribution, before any
    /// repair can mask it.
    resume_first_max: f64,
    /// All interactive samples, for the pooled summary.
    samples: Vec<f64>,
}

/// Send `bursts` bursts of `msg_bytes` at `cadence`, `BURST_OFF` apart, over a
/// byte stream the latency sink decodes.  Returns the number sent and the
/// `base`-clock stamp of each burst start.
async fn send_bursts(
    write: &mut (impl AsyncWrite + Unpin),
    base: Instant,
    bursts: usize,
) -> (u64, Vec<f64>) {
    let payload: Vec<u8> = (0..(MSG_BYTES - 12)).map(|i| (i % 251) as u8).collect();
    let mut sent = 0u64;
    let mut starts = Vec::with_capacity(bursts);
    let start = Instant::now();
    let period = BURST_ON + BURST_OFF;
    for i in 0..bursts {
        let at = start + FIRST_BURST + period * i as u32;
        tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
        starts.push(base.elapsed().as_secs_f64());
        let end = Instant::now() + BURST_ON;
        while Instant::now() < end {
            let sent_us = base.elapsed().as_micros() as u64;
            let mut frame = Vec::with_capacity(MSG_BYTES);
            frame.extend_from_slice(&((MSG_BYTES as u32).to_le_bytes()));
            frame.extend_from_slice(&payload);
            frame.extend_from_slice(&sent_us.to_le_bytes());
            if write.write_all(&frame).await.is_err() {
                return (sent, starts);
            }
            sent += 1;
            tokio::time::sleep(CADENCE).await;
        }
    }
    (sent, starts)
}

/// One arm/rep.
async fn run_arm(arm: Arm) -> RawRun {
    let base = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            // The mux harness supplies the interactive lane and its CC signal;
            // its bulk lane is left unopened.  The product bulk lane this arm
            // measures is a direct `rtp` `Dedicated` connection on the same
            // `(src, dst)` path, carrying the same CC link the deployment
            // attaches, so the stand-off is read without a second variable (the
            // mux stream's own flow control) between the controller and the
            // wire.
            let (int_addr, bulk_addr, mut latencies, _bulk_counter, _server_tx) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let (prod_addr, bulk_counter) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let (comp_addr, comp_delivered) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let shaper = BottleneckShaper::new(SHAPER_RATE_BPS, SHAPER_LIMIT_BYTES);
            let int_pair =
                NetemPair::spawn_shared(int_addr, link(41), link(42), Some(shaper.clone()), None)
                    .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(bulk_addr, link(43), link(44), Some(shaper.clone()), None)
                    .unwrap();
            let prod_pair =
                NetemPair::spawn_shared(prod_addr, link(47), link(48), Some(shaper.clone()), None)
                    .unwrap();
            let comp_pair =
                NetemPair::spawn_shared(comp_addr, link(45), link(46), Some(shaper.clone()), None)
                    .unwrap();
            let hub = match arm {
                Arm::Standoff => Some(CcSignalHub::new()),
                // The same CC link, stand-off disarmed: the loss gate and
                // shared-path suppression behave exactly as production, and
                // only the stand-off is off.
                Arm::Yield => Some(CcSignalHub::without_standoff()),
            };
            let product_cc = hub
                .as_ref()
                .map(|hub| rtp::cc::CcLink::new(hub.clone(), rtp::cc::CcRole::Bulk));
            // The stand-off's own input gate: the shared path's CC signal, read
            // from the same `(src, dst)` group the bulk controller consumes.
            // Sampling `offered_quiet_for` beside the controller's rate timeline
            // is what tells "the gate never opened" apart from "the gate opened
            // and the lane still could not claim share".
            let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
            let cc_gate = hub.as_ref().map(|hub| hub.group(loopback, loopback).bulk());
            let taps = Arc::new(ActionTaps::new(base));
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via_cc_link(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
                hub,
            )
            .await
            .unwrap();

            // Latency collector: stamp each sample's arrival on the base clock
            // so it can be attributed to a burst.
            let collected = Arc::new(Mutex::new(Vec::<(f64, f64)>::new()));
            {
                let collected = Arc::clone(&collected);
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        while let Some((_tag, latency)) = latencies.recv().await {
                            collected
                                .lock()
                                .unwrap()
                                .push((base.elapsed().as_secs_f64(), latency));
                        }
                    }),
                );
            }
            // Bulk-counter sampler: both cumulative byte counters every
            // `SAMPLE_STEP`, so the share can be integrated over the gaps.
            let counters = Arc::new(Mutex::new(Vec::<(f64, u64, u64)>::new()));
            let sampler_stop = Arc::new(AtomicBool::new(false));
            let backlog_max = Arc::new(AtomicU64::new(0));
            {
                let bulk = Arc::clone(&bulk_counter);
                let comp = Arc::clone(&comp_delivered);
                let sink = Arc::clone(&counters);
                let stop = Arc::clone(&sampler_stop);
                let bmax = Arc::clone(&backlog_max);
                let shaper_for_sampler = shaper.clone();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        while !stop.load(Ordering::Relaxed) {
                            bmax.fetch_max(
                                shaper_for_sampler.backlog_bytes(Instant::now()),
                                Ordering::Relaxed,
                            );
                            sink.lock().unwrap().push((
                                base.elapsed().as_secs_f64(),
                                bulk.load(Ordering::Relaxed),
                                comp.load(Ordering::Relaxed),
                            ));
                            tokio::time::sleep(SAMPLE_STEP).await;
                        }
                    }),
                );
            }
            // CC-gate sampler: the stand-off's input clock and sharedness on
            // the same cadence as the byte counters, so gate-open can be
            // located inside each gap to the sample step.
            let cc_samples = Arc::new(Mutex::new(Vec::<CcSample>::new()));
            {
                let gate = cc_gate.clone();
                let sink = Arc::clone(&cc_samples);
                let stop = Arc::clone(&sampler_stop);
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        while !stop.load(Ordering::Relaxed) {
                            if let Some(gate) = gate.as_ref() {
                                sink.lock().unwrap().push(CcSample {
                                    t: base.elapsed().as_secs_f64(),
                                    offered_quiet: gate
                                        .offered_quiet_for()
                                        .map(|d| d.as_secs_f64()),
                                    shared: gate.is_shared(),
                                });
                            }
                            tokio::time::sleep(SAMPLE_STEP).await;
                        }
                    }),
                );
            }

            let mut prod_write = spawn_rtp_bulk_upload_with_options_via(
                &task_tx,
                prod_pair.client_addr(),
                false,
                rtp::CongestionLane::Dedicated,
                rtp::FrameMode::default(),
                false,
                Some(action_observer(Arc::clone(&taps))),
                product_cc,
            )
            .await
            .unwrap();
            let (mut int_read, mut int_write) =
                opener.open(mux::LaneClass::Interactive).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = int_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
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

            let payload = byte_sink_payload(BYTE_SINK_BULK_CHUNK_BYTES);
            let comp_payload = payload.clone();
            let bulk_payload = payload.clone();
            // Tag byte so the latency sink attributes the samples, then the
            // bursty offer, both concurrent with the two saturating bulk flows.
            let interactive = async move {
                let _ = int_write.write_all(&[1u8]).await;
                send_bursts(&mut int_write, base, BURSTS).await
            };
            let bulk_fut = async move {
                let _ = prod_write.write_all(b"B").await;
                saturate(&mut prod_write, &bulk_payload, RUN_FOR).await;
            };
            let comp_fut = async move {
                saturate(&mut comp_write, &comp_payload, RUN_FOR).await;
            };
            let ((_sent, burst_starts), (), ()) = tokio::join!(interactive, bulk_fut, comp_fut);

            tokio::time::sleep(GRACE).await;
            sampler_stop.store(true, Ordering::Relaxed);
            let counter_samples = counters.lock().unwrap().clone();
            let samples = collected.lock().unwrap().clone();
            let cc_samples = cc_samples.lock().unwrap().clone();
            let action_timeline = taps.timeline.lock().unwrap().clone();
            let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
            let comp_bytes = comp_delivered.load(Ordering::Relaxed);
            int_pair.stop();
            bulk_pair.stop();
            prod_pair.stop();
            comp_pair.stop();
            RawRun {
                counter_samples,
                samples,
                burst_starts,
                bulk_bytes,
                comp_bytes,
                actions: BulkActions {
                    samples: taps.samples.load(Ordering::Relaxed),
                    drain: taps.drain.load(Ordering::Relaxed),
                    loss: taps.loss.load(Ordering::Relaxed),
                    probe: taps.probe.load(Ordering::Relaxed),
                    gentle: taps.gentle.load(Ordering::Relaxed),
                    hold: taps.hold.load(Ordering::Relaxed),
                },
                cc_samples,
                action_timeline,
                shaper_dropped: shaper.dropped(),
                shaper_backlog_max: backlog_max.load(Ordering::Relaxed),
            }
        })
        .await
}

/// Integrate the bulk share over each between-burst gap's late part.
fn late_gap_share(counter_samples: &[(f64, u64, u64)], burst_starts: &[f64]) -> (f64, u64, u64) {
    let mut bulk = 0u64;
    let mut comp = 0u64;
    let tail = GAP_MEASURE_TAIL.as_secs_f64();
    for pair in counter_samples.windows(2) {
        let (t0, b0, c0) = pair[0];
        let (t1, b1, c1) = pair[1];
        let mid = 0.5 * (t0 + t1);
        // A gap runs from the end of one burst to the start of the next; its
        // measured part is the tail end, after the stand-off window elapsed.
        let in_late_gap = burst_starts.windows(2).any(|w| {
            let gap_start = w[0] + BURST_ON.as_secs_f64();
            let gap_end = w[1];
            mid >= gap_end - (BURST_OFF.as_secs_f64() - tail) && mid < gap_end && mid > gap_start
        });
        if in_late_gap {
            bulk += b1.saturating_sub(b0);
            comp += c1.saturating_sub(c0);
        }
    }
    let total = bulk + comp;
    let share = if total == 0 {
        f64::NAN
    } else {
        bulk as f64 / total as f64
    };
    (share, bulk, comp)
}

/// Pool the interactive samples whose arrival falls in a resume window.
fn resume_tail(samples: &[(f64, f64)], burst_starts: &[f64]) -> (f64, f64) {
    let mut window: Vec<f64> = samples
        .iter()
        .filter(|(t, _)| {
            burst_starts
                .iter()
                .any(|s| *t >= *s && *t < *s + RESUME_SPAN.as_secs_f64())
        })
        .map(|(_, latency)| *latency)
        .collect();
    if window.is_empty() {
        return (f64::NAN, f64::NAN);
    }
    window.sort_by(|a, b| a.total_cmp(b));
    let idx = ((window.len() as f64 * 0.99) as usize).min(window.len() - 1);
    (window[idx], *window.last().unwrap())
}

/// Max interactive latency over the first `n` samples after each resume.  \
/// Samples are in arrival order, so `take(n)` is the first `n` packets of the \
/// burst.  This is the measurement that can see the hold: the whole-window p99 \
/// is repair-dominated, but the first packets after a resume are not.
fn resume_first_max(samples: &[(f64, f64)], burst_starts: &[f64], n: usize) -> f64 {
    let mut max = f64::NAN;
    for s in burst_starts {
        let first: Vec<f64> = samples
            .iter()
            .filter(|(t, _)| *t >= *s && *t < *s + RESUME_SPAN.as_secs_f64())
            .map(|(_, latency)| *latency)
            .take(n)
            .collect();
        for latency in first {
            if max.is_nan() || latency > max {
                max = latency;
            }
        }
    }
    max
}

/// One idle gap's stand-off state, read from the bulk lane's own gate and rate
/// timeline: when the gate opened (the interactive lane's offer clock crossed
/// `STANDOFF_WINDOW`), the competing rate it entered at, how far the rate
/// ramped before the gap ended, and how many competing actions fired.
struct GapState {
    index: usize,
    gap_start: f64,
    gate_open: Option<f64>,
    rate_at_open: Option<f64>,
    rate_at_end: Option<f64>,
    competing_actions: u64,
}

/// Locate each gap's gate-open time and rate response.  The gate is the
/// stand-off's own input: `offered_quiet_for` on the path's `CcSignal`, which
/// is the same value the bulk controller reads.  The rate response comes from
/// the bulk connection's public metrics timeline.
fn gap_diagnosis(
    cc_samples: &[CcSample],
    action_timeline: &[ActionSample],
    burst_starts: &[f64],
) -> Vec<GapState> {
    let window = rtp::cc::STANDOFF_WINDOW.as_secs_f64();
    let mut out = Vec::new();
    for (index, w) in burst_starts.windows(2).enumerate() {
        let gap_start = w[0] + BURST_ON.as_secs_f64();
        let gap_end = w[1];
        let gate_open = cc_samples
            .iter()
            .find(|s| {
                s.t >= gap_start && s.t < gap_end && s.offered_quiet.is_some_and(|q| q >= window)
            })
            .map(|s| s.t);
        let rate_at = |t: f64| {
            action_timeline
                .iter()
                .find(|a| a.t >= t && a.t < gap_end)
                .map(|a| a.rate)
        };
        let rate_at_open = gate_open.and_then(rate_at);
        let rate_at_end = action_timeline
            .iter()
            .rev()
            .find(|a| a.t >= gap_start && a.t < gap_end)
            .map(|a| a.rate);
        let competing_actions = gate_open.map_or(0, |open| {
            action_timeline
                .iter()
                .filter(|a| {
                    a.t >= open
                        && a.t < gap_end
                        && matches!(
                            a.action,
                            Some(MetricsCongestionAction::BandwidthProbe)
                                | Some(MetricsCongestionAction::LossBackoff)
                        )
                })
                .count() as u64
        });
        out.push(GapState {
            index,
            gap_start,
            gate_open,
            rate_at_open,
            rate_at_end,
            competing_actions,
        });
    }
    out
}

/// Decimated per-sample timeline of one gap (every 100 ms), so the transition
/// can be read rather than inferred from the summary.
fn print_gap_timeline(
    label: &str,
    cc: &[CcSample],
    actions: &[ActionSample],
    gap_start: f64,
    gap_end: f64,
) {
    eprintln!(
        "[standoff] {label} gap timeline from +0.00s (every 100 ms): quiet_ms shared rate_pps action"
    );
    let mut next = gap_start;
    for s in cc.iter().filter(|s| s.t >= gap_start && s.t < gap_end) {
        if s.t < next {
            continue;
        }
        next = s.t + 0.1;
        let act = actions.iter().find(|a| a.t >= s.t);
        eprintln!(
            "[standoff]   +{:5.2}s  quiet {:>7}  shared {:5}  rate {:>8.1}  {:?}",
            s.t - gap_start,
            s.offered_quiet
                .map(|q| format!("{:.0}", q * 1000.0))
                .unwrap_or_else(|| "None".to_string()),
            s.shared,
            act.map(|a| a.rate).unwrap_or(f64::NAN),
            act.and_then(|a| a.action),
        );
    }
}

/// Smallest number of positive signs whose one-sided tail probability under
/// the no-effect null (`p = 0.5`) is at most 5 %: the sign-test threshold for
/// `n` paired reps.  At `n = 16` it is 12 (`P(X >= 12) = 0.038`), at `n = 8`
/// it is 7 (`P = 0.035`).
fn sign_test_threshold(n: usize) -> usize {
    let mut row = vec![1u128];
    for _ in 0..n {
        let mut next = vec![1u128; row.len() + 1];
        for i in 1..row.len() {
            next[i] = row[i - 1] + row[i];
        }
        row = next;
    }
    let total: u128 = row.iter().sum();
    let mut tail = 0u128;
    let mut threshold = 0;
    for k in (0..=n).rev() {
        tail += row[k];
        if tail * 20 <= total {
            threshold = k;
        } else {
            break;
        }
    }
    threshold
}

fn median(xs: &mut [f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    xs[xs.len() / 2]
}

fn span(xs: &mut [f64]) -> (f64, f64) {
    if xs.is_empty() {
        return (f64::NAN, f64::NAN);
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    (xs[0], *xs.last().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns threads and binds ephemeral ports; 2 arms x 16 interleaved ~15 s runs; run with --ignored --nocapture --test-threads=1"]
async fn bulk_standoff_reclaims_idle_gaps_without_spiking_the_resume_tail() {
    let dir = "target/standoff-burst";
    std::fs::create_dir_all(dir).unwrap();
    let mut runs: Vec<(Arm, usize, Run)> = Vec::new();
    let mut reps_csv = String::from(
        "arm,rep,gap_share,bulk_bytes,comp_bytes,resume_p99,resume_max,resume_first_max\n",
    );
    let mut gaps_csv = String::from(
        "arm,rep,gap,gap_start,gate_open,compete_s,rate_at_open,rate_at_end,competing_actions\n",
    );
    let mut counters_csv = String::from("arm,rep,t,bulk_bytes,comp_bytes\n");
    let mut interactive_csv = String::from("arm,rep,t,latency_ms\n");
    let mut attempts_total = 0u64;
    let mut retried = 0u64;
    for rep in 0..REPS {
        // Alternate which arm runs first, so a systematic order bias (thermal,
        // allocator, cache warming) is common-mode across pairs instead of
        // being added to the effect the pair is meant to isolate.
        let order = if rep % 2 == 0 {
            [Arm::Yield, Arm::Standoff]
        } else {
            [Arm::Standoff, Arm::Yield]
        };
        for arm in order {
            let mut attempts = 1u32;
            let raw = loop {
                let raw = run_arm(arm).await;
                if raw.bulk_bytes > 0 && raw.comp_bytes > 0 && !raw.samples.is_empty() {
                    break raw;
                }
                if attempts as usize >= MAX_REP_ATTEMPTS {
                    break raw;
                }
                attempts += 1;
                retried += 1;
                eprintln!(
                    "[standoff] {} rep{rep} attempt {} was degenerate (bulk {} / comp {} bytes, \
                     {} samples): re-running the rep",
                    arm.name(),
                    attempts - 1,
                    raw.bulk_bytes,
                    raw.comp_bytes,
                    raw.samples.len(),
                );
            };
            attempts_total += attempts as u64;
            let (gap_share, _gap_bulk, _gap_comp) =
                late_gap_share(&raw.counter_samples, &raw.burst_starts);
            let (resume_p99, resume_max) = resume_tail(&raw.samples, &raw.burst_starts);
            let resume_first_max =
                resume_first_max(&raw.samples, &raw.burst_starts, RESUME_FIRST_N);
            let all: Vec<f64> = raw.samples.iter().map(|(_, l)| *l).collect();
            let summary = summarize(all.clone(), all.len() as u64, all.len() as u64, 0, 0.0);
            let run = Run {
                gap_share,
                bulk_bytes: raw.bulk_bytes,
                comp_bytes: raw.comp_bytes,
                resume_p99,
                resume_max,
                resume_first_max,
                samples: all,
            };
            eprintln!(
                "[standoff] {:>8} rep{rep}  gap_share {:5.3} (bulk {} / comp {} B)  \
                 resume p99 {:6.1} max {:7.1} first{} max {:7.1} ms  samples {}  all p50 {:5.1} p99 {:6.1} max {:7.1}  \
                 bulk-actions drain {} loss {} probe {} gentle {} hold {} of {}  \
                 shaper dropped {} backlog_max {} B",
                arm.name(),
                run.gap_share,
                run.bulk_bytes,
                run.comp_bytes,
                run.resume_p99,
                run.resume_max,
                RESUME_FIRST_N,
                run.resume_first_max,
                run.samples.len(),
                summary.p50,
                summary.p99,
                summary.max,
                raw.actions.drain,
                raw.actions.loss,
                raw.actions.probe,
                raw.actions.gentle,
                raw.actions.hold,
                raw.actions.samples,
                raw.shaper_dropped,
                raw.shaper_backlog_max,
            );
            // The gap-state diagnosis: does the gate open, when, and does the
            // rate respond before the gap closes?
            let gaps = gap_diagnosis(&raw.cc_samples, &raw.action_timeline, &raw.burst_starts);
            for g in &gaps {
                eprintln!(
                    "[standoff] {:>8} rep{rep} gap{}  +{:.2}s of {:.2}s  gate_open {}  \
                     rate {:.1} -> {:.1} pps  competing-actions {}",
                    arm.name(),
                    g.index,
                    g.gate_open.map(|t| t - g.gap_start).unwrap_or(f64::NAN),
                    GAP_MEASURE_TAIL.as_secs_f64(),
                    g.gate_open
                        .map(|t| format!("+{:.2}s", t - g.gap_start))
                        .unwrap_or_else(|| "NEVER".to_string()),
                    g.rate_at_open.unwrap_or(f64::NAN),
                    g.rate_at_end.unwrap_or(f64::NAN),
                    g.competing_actions,
                );
                gaps_csv.push_str(&format!(
                    "{},{rep},{},{:.3},{},{:.3},{:.3},{:.3},{}\n",
                    arm.name(),
                    g.index,
                    g.gap_start,
                    g.gate_open.unwrap_or(f64::NAN),
                    g.gate_open.map(|t| t - g.gap_start).unwrap_or(f64::NAN),
                    g.rate_at_open.unwrap_or(f64::NAN),
                    g.rate_at_end.unwrap_or(f64::NAN),
                    g.competing_actions,
                ));
            }
            if rep == 0
                && let Some(g) = gaps.first()
            {
                print_gap_timeline(
                    arm.name(),
                    &raw.cc_samples,
                    &raw.action_timeline,
                    g.gap_start,
                    g.gap_start + BURST_OFF.as_secs_f64(),
                );
            }
            reps_csv.push_str(&format!(
                "{},{rep},{:.4},{},{},{:.3},{:.3},{:.3}\n",
                arm.name(),
                run.gap_share,
                run.bulk_bytes,
                run.comp_bytes,
                run.resume_p99,
                run.resume_max,
                run.resume_first_max,
            ));
            for (t, b, c) in &raw.counter_samples {
                counters_csv.push_str(&format!("{},{rep},{:.3},{},{}\n", arm.name(), t, b, c));
            }
            for (t, latency) in &raw.samples {
                interactive_csv.push_str(&format!(
                    "{},{rep},{:.3},{:.3}\n",
                    arm.name(),
                    t,
                    latency
                ));
            }
            runs.push((arm, rep, run));
        }
    }
    std::fs::write(format!("{dir}/reps.csv"), &reps_csv).unwrap();
    std::fs::write(format!("{dir}/gaps.csv"), &gaps_csv).unwrap();
    std::fs::write(format!("{dir}/counters.csv"), &counters_csv).unwrap();
    std::fs::write(format!("{dir}/interactive.csv"), &interactive_csv).unwrap();
    eprintln!(
        "[standoff] rep attempts {attempts_total} for {} measured reps ({retried} re-runs)",
        REPS * 2
    );

    let stat = |arm: Arm, f: fn(&Run) -> f64| -> (f64, f64, f64) {
        let mut xs: Vec<f64> = runs
            .iter()
            .filter(|(a, _, _)| *a == arm)
            .map(|(_, _, r)| f(r))
            .collect();
        let med = median(&mut xs.clone());
        let (lo, hi) = span(&mut xs);
        (med, lo, hi)
    };
    let (ys, ys_lo, ys_hi) = stat(Arm::Yield, |r| r.gap_share);
    let (ss, ss_lo, ss_hi) = stat(Arm::Standoff, |r| r.gap_share);
    let (yp99, _, _) = stat(Arm::Yield, |r| r.resume_p99);
    let (sp99, _, _) = stat(Arm::Standoff, |r| r.resume_p99);
    let (ymax, _, _) = stat(Arm::Yield, |r| r.resume_max);
    let (smax, _, _) = stat(Arm::Standoff, |r| r.resume_max);
    let (yfirst, _, _) = stat(Arm::Yield, |r| r.resume_first_max);
    let (sfirst, _, _) = stat(Arm::Standoff, |r| r.resume_first_max);
    // The paired statistic: the arms are interleaved rep by rep, so the per-rep
    // difference removes the slow drift a min/max span cannot.  A sign count
    // and the median paired delta are reported beside the span.
    let paired: Vec<f64> = (0..REPS)
        .filter_map(|rep| {
            let y = runs
                .iter()
                .find(|(a, r, _)| *a == Arm::Yield && *r == rep)
                .map(|(_, _, run)| run.gap_share)?;
            let s = runs
                .iter()
                .find(|(a, r, _)| *a == Arm::Standoff && *r == rep)
                .map(|(_, _, run)| run.gap_share)?;
            Some(s - y)
        })
        .collect();
    let positive = paired.iter().filter(|d| **d > 0.0).count();
    let mut paired_sorted = paired.clone();
    let paired_median = median(&mut paired_sorted);
    let paired_mean = if paired.is_empty() {
        f64::NAN
    } else {
        paired.iter().sum::<f64>() / paired.len() as f64
    };
    eprintln!(
        "[standoff] late-gap bulk share   yield {ys:.3} [{ys_lo:.3},{ys_hi:.3}]   \
         standoff {ss:.3} [{ss_lo:.3},{ss_hi:.3}]   delta {:+.3}",
        ss - ys
    );
    eprintln!(
        "[standoff] paired (standoff-yield) gap-share delta: median {paired_median:+.3} \
         mean {paired_mean:+.3}, {positive}/{} reps positive",
        paired.len()
    );
    // The power of the paired design: the per-rep spread, the standard error of
    // the mean delta, its 95 % CI, and the smallest effect the rep count can
    // resolve at 80 % power.  A CI that straddles zero is an inconclusive
    // result, whatever the medians do.
    let paired_sd = if paired.len() > 1 {
        (paired
            .iter()
            .map(|d| (d - paired_mean).powi(2))
            .sum::<f64>()
            / (paired.len() - 1) as f64)
            .sqrt()
    } else {
        f64::NAN
    };
    let paired_sem = paired_sd / (paired.len() as f64).sqrt();
    let ci_lo = paired_mean - 1.96 * paired_sem;
    let ci_hi = paired_mean + 1.96 * paired_sem;
    let mde = 2.802 * paired_sem; // (z_{0.975} + z_{0.8}) * sem
    eprintln!(
        "[standoff] paired power: mean {paired_mean:+.3}  sd {paired_sd:.3}  sem {paired_sem:.3}  \
         95 % CI [{ci_lo:+.3},{ci_hi:+.3}]  MDE(80 %) {mde:.3}  n {}",
        paired.len()
    );
    eprintln!(
        "[standoff] resume p99  yield {yp99:.1}  standoff {sp99:.1} ms   \
         resume max  yield {ymax:.1}  standoff {smax:.1} ms   \
         first-{RESUME_FIRST_N} max  yield {yfirst:.1}  standoff {sfirst:.1} ms"
    );
    eprintln!("[standoff] data: {dir}/reps.csv");

    for (arm, rep, run) in &runs {
        assert!(
            !run.samples.is_empty(),
            "[standoff] {} rep{rep} measured no interactive sample: the latency sink was not wired",
            arm.name()
        );
        assert!(
            run.bulk_bytes > 0 && run.comp_bytes > 0,
            "[standoff] {} rep{rep} delivered bulk {} / comp {} bytes: one bulk flow was absent, \
             so the share is undefined",
            arm.name(),
            run.bulk_bytes,
            run.comp_bytes
        );
        assert!(
            run.gap_share.is_finite(),
            "[standoff] {} rep{rep} integrated no late-gap samples: the gap did not overlap the window",
            arm.name()
        );
    }
    // The product property: the stand-off must materially raise the late-gap
    // share.  The arms are interleaved rep by rep and the order is alternated,
    // so the paired per-rep delta is the noise-correct statistic.  The raw
    // min/max span is stated but is not a usable threshold: a single rep whose
    // two bulk byte totals in the measured tail are near zero pins one arm's
    // share at 0 or 1, and no mechanism could clear the resulting span.
    //
    // The decision is a paired test at a stated confidence, not a sign count:
    // the 95 % CI of the mean paired delta must exclude zero, and the sign test
    // must reject the no-effect null.  At 16 reps the 5 % one-sided sign-test
    // threshold is 12/16 (P(X>=12 | p=0.5) = 0.038); at 8 reps the
    // predecessor's 7/8 was the same test (P=0.035).  This is that test powered
    // up, not a weaker one.
    let effect = ss - ys;
    let spread = (ys_hi - ys_lo).max(ss_hi - ss_lo);
    eprintln!(
        "[standoff] effect (median - median) {effect:+.3}, worst-arm share span {spread:.3} \
         (stated, not thresholded)"
    );
    let sign_threshold = sign_test_threshold(paired.len());
    assert!(
        paired.len() >= REPS,
        "[standoff] only {} paired reps were collected",
        paired.len()
    );
    assert!(
        positive >= sign_threshold,
        "[standoff] the stand-off's late-gap share gain is not consistent: {positive}/{} paired \
         reps positive (need {sign_threshold} for the 5 % sign test)",
        paired.len()
    );
    assert!(
        ci_lo > 0.0,
        "[standoff] the stand-off's paired late-gap share gain is not resolved at 95 % confidence: \
         mean {paired_mean:+.3}, 95 % CI [{ci_lo:+.3},{ci_hi:+.3}] (sd {paired_sd:.3}, n {})",
        paired.len()
    );
    assert!(
        paired_median > 0.0,
        "[standoff] the stand-off's median paired late-gap share gain {paired_median:+.3} is not \
         positive"
    );
    assert!(
        ss > ys,
        "[standoff] the stand-off did not raise the late-gap share ({ss:.3} vs {ys:.3})"
    );
}
