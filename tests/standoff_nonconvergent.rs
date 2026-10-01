//! The bulk lane's interactive stand-off, decided against a **non-converging**
//! competitor.
//!
//! # Why this arm exists
//!
//! `standoff_burst` measures the stand-off against a saturating rtp **AIMD**
//! competitor.  That competitor converges both bulk flows to fair sharing, so
//! its presence erases whatever share the stand-off claims: the delivered-byte
//! share returns to the same value whether the mechanism competes or yields, so
//! the share metric cannot tell a working mechanism from a broken one.  The
//! measured effect was inside the instrument's noise for exactly this reason.
//!
//! This arm replaces the competitor with a **fixed-rate flow that does not
//! yield**: a raw UDP source paced at [`COMPETITOR_RATE_BPS`] (half the
//! bottleneck), whose offered rate is constant regardless of loss.  It never
//! converges to fair sharing, so the delivered split between it and our bulk is
//! a free variable rather than a quantity both arms converge to.  Paired with
//! the **reclaim-latency** reading (how long the interactive lane takes to
//! return to its floor after a resume) this lets the arm answer the product
//! question the share ratio alone cannot: does competing in the gaps buy enough
//! delivered share to justify the queue it leaves for the resuming lane?
//!
//! # The mechanism
//!
//! `rtp`'s bulk (`Dedicated`) lane carries a cross-lane CC link.  While our own
//! interactive lane is offering data the bulk keeps the shipped delay-first
//! policy; once it has not offered for `rtp::cc::STANDOFF_WINDOW` the bulk
//! competes against the external flow on TCP's terms (an absolute additive
//! increase, a `0.5` multiplicative decrease per loss event), and when the
//! interactive lane resumes it drains below its competing rate for
//! `STANDOFF_HOLD` so the queue it filled is cleared.
//!
//! # The arms
//!
//! Two phases, all against the same fixed-rate competitor and the same shared
//! bottleneck shaper (`1 MiB/s`, `128 KiB` drop-tail):
//!
//! * **solo sanity** — no interactive lane at all.  `standoff` competes
//!   permanently (no offer witness ever opens the yield window, so the lane is
//!   always in its competing episode); `yield` never competes.  The competing
//!   arm must out-claim the standing-off one, or the share metric has no
//!   direction to read.
//! * **gap arms** — the bursty interactive lane, as in `standoff_burst`: a
//!   `250 ms` burst every `4 s`, so each gap exceeds the stand-off window.  The
//!   arms are interleaved rep by rep with the order alternated.
//!
//! # Readings
//!
//! * **per-episode bulk share** — the product bulk lane's fraction of the two
//!   flows' bytes delivered over one gap's late part.
//! * **reclaim latency** — per episode, the wall time from the resume's first
//!   interactive sample until the interactive one-way latency first falls back
//!   to the rep's own floor.  This is the product question the share ratio
//!   cannot express: how long the client feels the bulk's aggression after it
//!   starts talking again.  It is also the instrument's sensitivity check: a
//!   competing bulk must slow reclaim measurably, or the share reading is not
//!   evidence about the mechanism.
//! * **resume tail** — the interactive p99 and max over each resume window, and
//!   the max over the first [`RESUME_FIRST_N`] samples after the resume (before
//!   a repair masks the hold's own queue).
//!
//! Asserting in the `full` tier.  The hard assertions are the two halves of the
//! mandate -- the late-gap share gain must resolve, and the interactive lane's
//! reclaim cost must **not** -- plus non-degeneracy.  The absolute bound the
//! cost is held to (a full bottleneck buffer's drain time) is printed with the
//! CI.  The mechanism's liveness is proven by the vacuity probes recorded in
//! `GATE.md`: R1 forced busy collapses the share gain, and R2's arming forced
//! always-true restores the resolved cost.  Run:
//! ```sh
//! cargo test --release -p rtp_mux --test standoff_nonconvergent -- --ignored --nocapture --test-threads=1
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
use rtp::testkit::rtp::{
    spawn_rtp_bulk_upload_with_options_via, spawn_rtp_byte_sink_server_tagged_via,
};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via_cc_link,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use rtp_mux::testkit::payload::{BYTE_SINK_BULK_CHUNK_BYTES, byte_sink_payload, saturate};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UdpSocket;

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
/// The fixed-rate competitor's offered rate: a constant `0.6 x` the shared
/// bottleneck.  It is **not reduced on loss** -- the offered rate is the same
/// whether our bulk competes or yields -- so unlike an AIMD reference it cannot
/// converge the difference away.  It sits above the `0.5 x` fair share it would
/// have to accept against a competing flow, so it never yields toward fair
/// sharing; and it sits below the link rate, so on its own it leaves the
/// interactive floor clean and only the *combination* with our bulk saturates.
const COMPETITOR_RATE_BPS: u64 = SHAPER_RATE_BPS / 2;
/// The competitor's datagram payload.  Fixed, so the offered packet rate is
/// fixed with it.
const COMPETITOR_PAYLOAD: usize = 1200;
/// How long a solo sanity arm runs, and the tail of it the share is read over
/// once the bulk controller has settled.  The window is long (the whole run
/// after start-up) so a momentary AIMD collapse inside one gap is averaged
/// rather than read as a zero share.
const SANITY_RUN_FOR: Duration = Duration::from_secs(6);
const SANITY_MEASURE: Duration = Duration::from_millis(5000);
/// Interleaved reps per solo sanity arm.
const SANITY_REPS: usize = 8;
/// How far above a rep's own floor a sample may sit and still read as "at the
/// floor": the injected jitter plus a scheduling margin.
const RECLAIM_TOL_MS: f64 = 12.0;
/// The bulk payload length: the shared byte-sink helper's period-aligned
/// chunk, the single authority for a seamless `saturate` wrap (`64 KiB` is
/// not a whole multiple of the sink's `% 251` period and freezes the count).
const BULK_CHUNK: usize = BYTE_SINK_BULK_CHUNK_BYTES;
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
    comp_offered_bytes: u64,
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
    /// Late-gap share of the two bulk flows' delivered bytes, pooled.
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
    /// Per-episode reclaim latency in milliseconds: from the resume's first
    /// interactive sample until the one-way latency first returns to the rep's
    /// own floor.
    reclaim_ms: Vec<f64>,
    /// All interactive samples, for the pooled summary.
    samples: Vec<f64>,
}

/// Offer `rate_bps` to `sock` for `run_for`, as a **fixed-rate** flow: the send
/// schedule is advanced by wall time and packets are offered whether or not the
/// shaper is dropping them.  There is no congestion response anywhere in this
/// function, so its offered rate is the same under loss as under a clean link --
/// which is what makes it a competitor that cannot converge the split away.
async fn blast_fixed_rate(
    sock: &UdpSocket,
    rate_bps: u64,
    payload_len: usize,
    run_for: Duration,
) -> u64 {
    let payload = vec![0xA5u8; payload_len];
    let start = Instant::now();
    let mut next = 0u64;
    while start.elapsed() < run_for {
        let elapsed = start.elapsed().as_secs_f64();
        let target = (elapsed * rate_bps as f64 / 8.0 / payload_len as f64) as u64;
        while next < target {
            // A refused send is a packet the OS dropped before the shaper; the
            // schedule still advances so the offered rate does not fall.
            let _ = sock.send(&payload).await;
            next += 1;
        }
        tokio::time::sleep(Duration::from_micros(500)).await;
    }
    next * payload_len as u64
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
async fn run_arm(arm: Arm, interactive: bool, run_for: Duration) -> RawRun {
    let base = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let shaper = BottleneckShaper::new(SHAPER_RATE_BPS, SHAPER_LIMIT_BYTES);
            // The product bulk writes the `b"B"` tag ahead of its payload (see
            // `bulk_fut` below), so its sink declares that tag; the plain
            // sink's phase is anchored at payload offset 0 and an undeclared
            // tag freezes its delivered counter.
            let (prod_addr, bulk_counter) =
                spawn_rtp_byte_sink_server_tagged_via(&task_tx, false, b'B')
                    .await
                    .unwrap();
            let prod_pair =
                NetemPair::spawn_shared(prod_addr, link(47), link(48), Some(shaper.clone()), None)
                    .unwrap();
            // The competitor is a plain UDP sink behind the same shared shaper,
            // fed by a client socket paced at a fixed rate.  It has no
            // congestion response at all, so its offered rate is identical
            // whether our bulk competes or yields.
            let comp_sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let comp_sink_addr = comp_sink.local_addr().unwrap();
            let comp_pair = NetemPair::spawn_shared(
                comp_sink_addr,
                link(45),
                link(46),
                Some(shaper.clone()),
                None,
            )
            .unwrap();
            let comp_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            comp_client.connect(comp_pair.client_addr()).await.unwrap();

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

            // The gap arms carry the bursty interactive lane on the same path;
            // the solo sanity arms carry none, so the path never reads shared
            // and no offer ever shuts the stand-off's competing episode.
            let (int_write, latencies, int_pair, bulk_pair) = if interactive {
                let (int_addr, bulk_addr, latencies, _bulk_counter, _server_tx) =
                    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                        &task_tx, base, int_rtp, bulk_rtp,
                    )
                    .await
                    .unwrap();
                let int_pair = NetemPair::spawn_shared(
                    int_addr,
                    link(41),
                    link(42),
                    Some(shaper.clone()),
                    None,
                )
                .unwrap();
                let bulk_pair = NetemPair::spawn_shared(
                    bulk_addr,
                    link(43),
                    link(44),
                    Some(shaper.clone()),
                    None,
                )
                .unwrap();
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
                let (mut int_read, int_write) =
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
                (
                    Some(int_write),
                    Some(latencies),
                    Some(int_pair),
                    Some(bulk_pair),
                )
            } else {
                // A solo bulk keeps the CC link attached; only the shared-path
                // witness is absent, so the stand-off competes permanently.
                drop(hub);
                (None, None, None, None)
            };

            // Latency collector: stamp each sample's arrival on the base clock
            // so it can be attributed to a burst.
            let collected = Arc::new(Mutex::new(Vec::<(f64, f64)>::new()));
            if let Some(mut latencies) = latencies {
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
            let sampler_stop = Arc::new(AtomicBool::new(false));
            // Competitor-delivered counter: the raw sink receives whatever the
            // shaper let through, so its byte total is the competitor's
            // *delivered* share of the bottleneck.
            let comp_delivered = Arc::new(AtomicU64::new(0));
            {
                let cnt = Arc::clone(&comp_delivered);
                let stop = Arc::clone(&sampler_stop);
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = [0u8; 2048];
                        while let Ok((n, _)) = comp_sink.recv_from(&mut buf).await {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            cnt.fetch_add(n as u64, Ordering::Relaxed);
                        }
                    }),
                );
            }
            // Bulk-counter sampler: both cumulative byte counters every
            // `SAMPLE_STEP`, so the share can be integrated over the gaps.
            let counters = Arc::new(Mutex::new(Vec::<(f64, u64, u64)>::new()));
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

            let payload = byte_sink_payload(BULK_CHUNK);
            let bulk_payload = payload.clone();
            let bulk_fut = async move {
                let _ = prod_write.write_all(b"B").await;
                saturate(&mut prod_write, &bulk_payload, run_for).await;
            };
            let comp_fut = async move {
                blast_fixed_rate(
                    &comp_client,
                    COMPETITOR_RATE_BPS,
                    COMPETITOR_PAYLOAD,
                    run_for,
                )
                .await
            };
            // The interactive lane offers a burst every `BURST_OFF`, so each gap
            // exceeds the stand-off window; a solo arm has no lane and returns an
            // empty burst list.
            let interactive_fut = async move {
                let Some(mut write) = int_write else {
                    return (0u64, Vec::new());
                };
                let _ = write.write_all(&[1u8]).await;
                send_bursts(&mut write, base, BURSTS).await
            };
            let ((_sent, burst_starts), (), comp_offered_bytes) =
                tokio::join!(interactive_fut, bulk_fut, comp_fut);

            tokio::time::sleep(GRACE).await;
            sampler_stop.store(true, Ordering::Relaxed);
            let counter_samples = counters.lock().unwrap().clone();
            let samples = collected.lock().unwrap().clone();
            let cc_samples = cc_samples.lock().unwrap().clone();
            let action_timeline = taps.timeline.lock().unwrap().clone();
            let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
            let comp_bytes = comp_delivered.load(Ordering::Relaxed);
            if let Some(pair) = int_pair {
                pair.stop();
            }
            if let Some(pair) = bulk_pair {
                pair.stop();
            }
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
                comp_offered_bytes,
            }
        })
        .await
}

/// The product bulk lane's fraction of the two flows' delivered bytes over the
/// run's tail window `[from_t, end]` (the solo arms read steady state, not a
/// gap).
fn tail_share(counter_samples: &[(f64, u64, u64)], from_t: f64) -> (f64, u64, u64) {
    let mut bulk = 0u64;
    let mut comp = 0u64;
    for pair in counter_samples.windows(2) {
        let (t0, b0, c0) = pair[0];
        let (t1, b1, c1) = pair[1];
        if 0.5 * (t0 + t1) >= from_t {
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

/// One late-gap share per between-burst gap, so a single degenerate gap cannot
/// swamp the arm's reading.
fn episode_shares(counter_samples: &[(f64, u64, u64)], burst_starts: &[f64]) -> Vec<f64> {
    let tail = GAP_MEASURE_TAIL.as_secs_f64();
    let mut out = Vec::new();
    for w in burst_starts.windows(2) {
        let gap_start = w[0] + BURST_ON.as_secs_f64();
        let gap_end = w[1];
        let from = gap_end - (BURST_OFF.as_secs_f64() - tail);
        let mut bulk = 0u64;
        let mut comp = 0u64;
        for pair in counter_samples.windows(2) {
            let (t0, b0, c0) = pair[0];
            let (t1, b1, c1) = pair[1];
            let mid = 0.5 * (t0 + t1);
            if mid >= from && mid < gap_end && mid > gap_start {
                bulk += b1.saturating_sub(b0);
                comp += c1.saturating_sub(c0);
            }
        }
        let total = bulk + comp;
        out.push(if total == 0 {
            f64::NAN
        } else {
            bulk as f64 / total as f64
        });
    }
    out
}

/// One reclaim latency per resume episode (milliseconds): the wall time from
/// the resume's first interactive sample until the one-way latency first falls
/// back to the rep's own floor.  The floor is the rep's 1st percentile plus
/// [`RECLAIM_TOL_MS`], so a rep with a higher intrinsic floor is not scored as
/// never reclaiming.  A resume that never returns inside [`RESUME_SPAN`] is
/// reported as `NaN`.
fn reclaim_latencies(samples: &[(f64, f64)], burst_starts: &[f64]) -> Vec<f64> {
    if samples.is_empty() || burst_starts.len() < 2 {
        return Vec::new();
    }
    let mut all: Vec<f64> = samples.iter().map(|(_, latency)| *latency).collect();
    all.sort_by(|a, b| a.total_cmp(b));
    let idx = ((all.len() as f64 * 0.01) as usize).min(all.len() - 1);
    let floor = all[idx] + RECLAIM_TOL_MS;
    let mut out = Vec::new();
    for start in &burst_starts[1..] {
        let first_t = samples.iter().find(|(t, _)| *t >= *start).map(|(t, _)| *t);
        let Some(first_t) = first_t else {
            out.push(f64::NAN);
            continue;
        };
        let hit = samples.iter().find(|(t, latency)| {
            *t >= *start && *t < *start + RESUME_SPAN.as_secs_f64() && *latency <= floor
        });
        out.push(match hit {
            Some((t, _)) => (t - first_t) * 1000.0,
            None => f64::NAN,
        });
    }
    out
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
#[ignore = "spawns threads and binds ephemeral ports; a solo sanity pair plus 2 arms x 16 interleaved ~15 s runs; run with --ignored --nocapture --test-threads=1"]
async fn bulk_standoff_holds_share_against_a_non_converging_competitor() {
    let dir = "target/standoff-nonconvergent";
    std::fs::create_dir_all(dir).unwrap();

    // --- Phase 1: the instrument sanity check -----------------------------
    // No interactive lane.  `standoff` competes permanently (nothing ever
    // opens its yield window), `yield` never competes.  If the share metric
    // cannot separate these two extremes by more than the within-pair spread,
    // it has no teeth and the gap arms must not be read as evidence.
    let mut sanity: Vec<(Arm, usize, f64)> = Vec::new();
    let mut sanity_csv = String::from("arm,rep,share,bulk_bytes,comp_bytes\n");
    for rep in 0..SANITY_REPS {
        let order = if rep % 2 == 0 {
            [Arm::Yield, Arm::Standoff]
        } else {
            [Arm::Standoff, Arm::Yield]
        };
        for arm in order {
            let raw = run_arm(arm, false, SANITY_RUN_FOR).await;
            let t_end = raw
                .counter_samples
                .last()
                .map(|(t, _, _)| *t)
                .unwrap_or(f64::NAN);
            let from = t_end - SANITY_MEASURE.as_secs_f64();
            let (share, win_bulk, win_comp) = tail_share(&raw.counter_samples, from);
            let last_rate = raw
                .action_timeline
                .last()
                .map(|a| a.rate)
                .unwrap_or(f64::NAN);
            let max_rate = raw
                .action_timeline
                .iter()
                .map(|a| a.rate)
                .fold(f64::NAN, f64::max);
            eprintln!(
                "[nonconv] sanity {:>8} rep{rep}  share {:5.3}  bulk {} / comp {} B  \
                 offered {} B  window {:.2}s (bulk {} / comp {} B)  \
                 dropped {} backlog_max {} B  rate last {:.0} max {:.0} pps  actions {} drain {} loss {} probe {}",
                arm.name(),
                share,
                raw.bulk_bytes,
                raw.comp_bytes,
                raw.comp_offered_bytes,
                t_end - from,
                win_bulk,
                win_comp,
                raw.shaper_dropped,
                raw.shaper_backlog_max,
                last_rate,
                max_rate,
                raw.actions.samples,
                raw.actions.drain,
                raw.actions.loss,
                raw.actions.probe,
            );
            sanity_csv.push_str(&format!(
                "{},{rep},{:.4},{},{}\n",
                arm.name(),
                share,
                raw.bulk_bytes,
                raw.comp_bytes
            ));
            assert!(
                raw.bulk_bytes > 0 && raw.comp_bytes > 0,
                "[nonconv] sanity {} rep{rep} delivered bulk {} / comp {} bytes: a flow was absent",
                arm.name(),
                raw.bulk_bytes,
                raw.comp_bytes
            );
            sanity.push((arm, rep, share));
        }
    }
    std::fs::write(format!("{dir}/sanity.csv"), &sanity_csv).unwrap();
    let sanity_share = |arm: Arm| -> Vec<f64> {
        (0..SANITY_REPS)
            .filter_map(|rep| {
                sanity
                    .iter()
                    .find(|(a, r, _)| *a == arm && *r == rep)
                    .map(|(_, _, s)| *s)
            })
            .collect()
    };
    // `standoff` with no interactive lane competes permanently; `yield` never
    // competes.  These are the known-good / known-bad extremes.
    let known_good = sanity_share(Arm::Standoff);
    let known_bad = sanity_share(Arm::Yield);
    let sanity_paired: Vec<f64> = known_good
        .iter()
        .zip(known_bad.iter())
        .map(|(good, bad)| good - bad)
        .collect();
    let sanity_mean = sanity_paired.iter().sum::<f64>() / sanity_paired.len() as f64;
    let sanity_sd = if sanity_paired.len() > 1 {
        (sanity_paired
            .iter()
            .map(|d| (d - sanity_mean).powi(2))
            .sum::<f64>()
            / (sanity_paired.len() - 1) as f64)
            .sqrt()
    } else {
        f64::NAN
    };
    let mut good_sorted = known_good.clone();
    let mut bad_sorted = known_bad.clone();
    let good_med = median(&mut good_sorted);
    let bad_med = median(&mut bad_sorted);
    eprintln!(
        "[nonconv] sanity (no interactive lane): compete share {good_med:.3} vs stand-off \
         {bad_med:.3}  paired mean {sanity_mean:+.3} sd {sanity_sd:.3} n {}",
        sanity_paired.len()
    );
    // The solo phase is a diagnostic, not the load-bearing check: its paired
    // separation is inside its own spread (see GATE.md), so asserting a
    // magnitude here would assert on noise.  The instrument's sensitivity check
    // is the reclaim separation asserted after the gap arms.
    assert!(
        sanity_paired.len() == SANITY_REPS && sanity_mean.is_finite() && sanity_sd.is_finite(),
        "[nonconv] the solo sanity pair did not produce {SANITY_REPS} finite reps: \
         compete {known_good:?} vs stand-off {known_bad:?}"
    );

    // --- Phase 2: the gap arms --------------------------------------------
    let mut runs: Vec<(Arm, usize, Run)> = Vec::new();
    let mut reps_csv = String::from(
        "arm,rep,gap_share,bulk_bytes,comp_bytes,resume_p99,resume_max,resume_first_max\n",
    );
    let mut episodes_csv = String::from("arm,rep,episode,gap_share,reclaim_ms,resume_first_max\n");
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
                let raw = run_arm(arm, true, RUN_FOR).await;
                if raw.bulk_bytes > 0 && raw.comp_bytes > 0 && !raw.samples.is_empty() {
                    break raw;
                }
                if attempts as usize >= MAX_REP_ATTEMPTS {
                    break raw;
                }
                attempts += 1;
                retried += 1;
                eprintln!(
                    "[nonconv] {} rep{rep} attempt {} was degenerate (bulk {} / comp {} bytes, \
                     {} samples): re-running the rep",
                    arm.name(),
                    attempts - 1,
                    raw.bulk_bytes,
                    raw.comp_bytes,
                    raw.samples.len(),
                );
            };
            attempts_total += attempts as u64;
            let eps_shares = episode_shares(&raw.counter_samples, &raw.burst_starts);
            let valid: Vec<f64> = eps_shares
                .iter()
                .copied()
                .filter(|s| s.is_finite())
                .collect();
            let gap_share = if valid.is_empty() {
                f64::NAN
            } else {
                valid.iter().sum::<f64>() / valid.len() as f64
            };
            let (resume_p99, resume_max) = resume_tail(&raw.samples, &raw.burst_starts);
            let resume_first_max =
                resume_first_max(&raw.samples, &raw.burst_starts, RESUME_FIRST_N);
            let reclaim_ms = reclaim_latencies(&raw.samples, &raw.burst_starts);
            let all: Vec<f64> = raw.samples.iter().map(|(_, l)| *l).collect();
            let summary = summarize(all.clone(), all.len() as u64, all.len() as u64, 0, 0.0);
            let run = Run {
                gap_share,
                bulk_bytes: raw.bulk_bytes,
                comp_bytes: raw.comp_bytes,
                resume_p99,
                resume_max,
                resume_first_max,
                reclaim_ms: reclaim_ms.clone(),
                samples: all,
            };
            let mut reclaim_for_median = reclaim_ms
                .iter()
                .copied()
                .filter(|x| x.is_finite())
                .collect::<Vec<f64>>();
            let reclaim_med = median(&mut reclaim_for_median);
            eprintln!(
                "[nonconv] {:>8} rep{rep}  gap_share {:5.3} (bulk {} / comp {} B)  \
                 reclaim median {:6.1} ms  resume p99 {:6.1} max {:7.1} first{} max {:7.1} ms  \
                 samples {}  all p50 {:5.1} p99 {:6.1} max {:7.1}  \
                 bulk-actions drain {} loss {} probe {} gentle {} hold {} of {}  \
                 shaper dropped {} backlog_max {} B",
                arm.name(),
                run.gap_share,
                run.bulk_bytes,
                run.comp_bytes,
                reclaim_med,
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
            for (episode, start) in raw.burst_starts.iter().enumerate() {
                let share = eps_shares.get(episode).copied().unwrap_or(f64::NAN);
                let reclaim = reclaim_ms.get(episode).copied().unwrap_or(f64::NAN);
                let first_max = raw
                    .samples
                    .iter()
                    .filter(|(t, _)| *t >= *start && *t < *start + RESUME_SPAN.as_secs_f64())
                    .map(|(_, latency)| *latency)
                    .take(RESUME_FIRST_N)
                    .fold(f64::NAN, f64::max);
                eprintln!(
                    "[nonconv] {:>8} rep{rep} episode{episode}  share {:5.3}  reclaim {:6.1} ms  \
                     first{} max {:7.1} ms",
                    arm.name(),
                    share,
                    reclaim,
                    RESUME_FIRST_N,
                    first_max,
                );
                episodes_csv.push_str(&format!(
                    "{},{rep},{episode},{:.4},{:.3},{:.3}\n",
                    arm.name(),
                    share,
                    reclaim,
                    first_max,
                ));
            }
            // The gap-state diagnosis: does the gate open, when, and does the
            // rate respond before the gap closes?
            let gaps = gap_diagnosis(&raw.cc_samples, &raw.action_timeline, &raw.burst_starts);
            for g in &gaps {
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
    std::fs::write(format!("{dir}/episodes.csv"), &episodes_csv).unwrap();
    std::fs::write(format!("{dir}/gaps.csv"), &gaps_csv).unwrap();
    std::fs::write(format!("{dir}/counters.csv"), &counters_csv).unwrap();
    std::fs::write(format!("{dir}/interactive.csv"), &interactive_csv).unwrap();
    eprintln!(
        "[nonconv] rep attempts {attempts_total} for {} measured reps ({retried} re-runs)",
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

    let find = |arm: Arm, rep: usize| -> Option<&Run> {
        runs.iter()
            .find(|(a, r, _)| *a == arm && *r == rep)
            .map(|(_, _, run)| run)
    };
    // Per-rep reclaim median over the rep's episodes: a rep whose first resume
    // recovered and whose later one did not is not lost in a pooled percentile.
    let rep_reclaim = |arm: Arm, rep: usize| -> f64 {
        let Some(run) = find(arm, rep) else {
            return f64::NAN;
        };
        let mut xs: Vec<f64> = run
            .reclaim_ms
            .iter()
            .copied()
            .filter(|x| x.is_finite())
            .collect();
        median(&mut xs)
    };

    let paired: Vec<f64> = (0..REPS)
        .filter_map(|rep| {
            let s = find(Arm::Standoff, rep)?.gap_share;
            let y = find(Arm::Yield, rep)?.gap_share;
            (s.is_finite() && y.is_finite()).then_some(s - y)
        })
        .collect();
    let paired_reclaim: Vec<f64> = (0..REPS)
        .filter_map(|rep| {
            let s = rep_reclaim(Arm::Standoff, rep);
            let y = rep_reclaim(Arm::Yield, rep);
            (s.is_finite() && y.is_finite()).then_some(s - y)
        })
        .collect();

    let stats = |xs: &[f64]| -> (f64, f64, f64, f64, f64, f64) {
        let mean = xs.iter().sum::<f64>() / xs.len() as f64;
        let sd = if xs.len() > 1 {
            (xs.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt()
        } else {
            f64::NAN
        };
        let sem = sd / (xs.len() as f64).sqrt();
        let lo = mean - 1.96 * sem;
        let hi = mean + 1.96 * sem;
        let mde = 2.802 * sem;
        (mean, sd, sem, lo, hi, mde)
    };
    let (share_mean, share_sd, share_sem, share_lo, share_hi, share_mde) = stats(&paired);
    let (rec_mean, rec_sd, rec_sem, rec_lo, rec_hi, rec_mde) = stats(&paired_reclaim);
    let positive = paired.iter().filter(|d| **d > 0.0).count();

    // The per-episode reclaim distribution, pooled across the arm's episodes.
    let pooled = |arm: Arm| -> Vec<f64> {
        runs.iter()
            .filter(|(a, _, _)| *a == arm)
            .flat_map(|(_, _, run)| run.reclaim_ms.iter().copied())
            .filter(|x| x.is_finite())
            .collect()
    };
    let mut y_reclaim = pooled(Arm::Yield);
    let mut s_reclaim = pooled(Arm::Standoff);
    y_reclaim.sort_by(|a, b| a.total_cmp(b));
    s_reclaim.sort_by(|a, b| a.total_cmp(b));
    let q = |xs: &[f64], p: f64| -> f64 {
        if xs.is_empty() {
            return f64::NAN;
        }
        xs[((xs.len() as f64 * p) as usize).min(xs.len() - 1)]
    };

    eprintln!(
        "[nonconv] late-gap bulk share   yield {ys:.3} [{ys_lo:.3},{ys_hi:.3}]   \
         standoff {ss:.3} [{ss_lo:.3},{ss_hi:.3}]   delta {:+.3}",
        ss - ys
    );
    eprintln!(
        "[nonconv] paired (standoff-yield) gap-share delta: median {:+.3} mean {share_mean:+.3} \
         sd {share_sd:.3} sem {share_sem:.3} 95 % CI [{share_lo:+.3},{share_hi:+.3}] \
         MDE(80 %) {share_mde:.3}  {positive}/{} positive",
        median(&mut paired.clone()),
        paired.len()
    );
    eprintln!(
        "[nonconv] resume p99  yield {yp99:.1}  standoff {sp99:.1} ms   \
         resume max  yield {ymax:.1}  standoff {smax:.1} ms   \
         first-{RESUME_FIRST_N} max  yield {yfirst:.1}  standoff {sfirst:.1} ms"
    );
    eprintln!(
        "[nonconv] reclaim (per episode, ms): yield p50 {:.1} p95 {:.1} max {:.1} n {} | \
         standoff p50 {:.1} p95 {:.1} max {:.1} n {}",
        q(&y_reclaim, 0.50),
        q(&y_reclaim, 0.95),
        q(&y_reclaim, 1.0),
        y_reclaim.len(),
        q(&s_reclaim, 0.50),
        q(&s_reclaim, 0.95),
        q(&s_reclaim, 1.0),
        s_reclaim.len(),
    );
    eprintln!(
        "[nonconv] paired reclaim delta (standoff-yield, per-rep median of episodes): \
         mean {rec_mean:+.1} ms  sd {rec_sd:.1}  sem {rec_sem:.1}  95 % CI \
         [{rec_lo:+.1},{rec_hi:+.1}]  MDE(80 %) {rec_mde:.1}  n {}",
        paired_reclaim.len()
    );
    eprintln!("[nonconv] data: {dir}/reps.csv, {dir}/episodes.csv");

    for (arm, rep, run) in &runs {
        assert!(
            !run.samples.is_empty(),
            "[nonconv] {} rep{rep} measured no interactive sample: the latency sink was not wired",
            arm.name()
        );
        assert!(
            run.bulk_bytes > 0 && run.comp_bytes > 0,
            "[nonconv] {} rep{rep} delivered bulk {} / comp {} bytes: one flow was absent, \
             so the share is undefined",
            arm.name(),
            run.bulk_bytes,
            run.comp_bytes
        );
        assert!(
            run.gap_share.is_finite(),
            "[nonconv] {} rep{rep} integrated no late-gap samples: the gap did not overlap the window",
            arm.name()
        );
        assert!(
            run.reclaim_ms.iter().any(|x| x.is_finite()),
            "[nonconv] {} rep{rep} recorded no reclaim latency: every resume stayed above its floor",
            arm.name()
        );
    }
    // The mandate the arm decides, both halves, from the same 16 interleaved
    // reps.  The **benefit**: the stand-off's late-gap share gain must have a
    // paired 95 % CI that excludes zero, or the mechanism has no direction to
    // read.  The **cost**: the interactive lane's reclaim must *no longer*
    // resolve harmful -- the paired delta's 95 % CI must not lie entirely above
    // zero -- and the absolute bound it is held to is the full bottleneck
    // buffer's drain time, printed with the CI.
    //
    // Liveness is proven by the vacuity probes recorded in `GATE.md`: with R1
    // forced busy the share gain collapses (the share assertion reddens), and
    // with R2's arming forced always-true the pre-fix reclaim cost returns (the
    // reclaim assertion reddens).  A pair of mirror-image assertions, each of
    // which a named mutation reddens, is the instrument-sensitivity check.
    assert!(
        paired.len() >= REPS,
        "[nonconv] only {} paired share reps were collected",
        paired.len()
    );
    assert!(
        paired_reclaim.len() >= REPS / 2,
        "[nonconv] only {} paired reclaim reps were collected: the interactive lane's reclaim \
         could not be measured",
        paired_reclaim.len()
    );
    // A full drop-tail buffer is the longest any flow can queue a resume, so a
    // reclaim cost at that scale is the physical ceiling the mechanism must
    // stay under; it is stated against the standing-off arm's own p95.
    let drain_bound_ms = 1000.0 * SHAPER_LIMIT_BYTES as f64 / (SHAPER_RATE_BPS as f64 / 8.0);
    eprintln!(
        "[nonconv] reclaim bound: standing-off p95 {:.1} ms; a full {SHAPER_LIMIT_BYTES} B \
         drop-tail buffer drains in {drain_bound_ms:.1} ms at {SHAPER_RATE_BPS} bit/s; stand-off \
         paired mean {rec_mean:+.1} ms 95 % CI [{rec_lo:+.1},{rec_hi:+.1}] MDE(80 %) {rec_mde:.1}",
        q(&y_reclaim, 0.95),
    );
    assert!(
        share_lo > 0.0,
        "[nonconv] the stand-off's late-gap share gain is not resolved: paired mean \
         {share_mean:+.3} 95 % CI [{share_lo:+.3},{share_hi:+.3}] MDE(80 %) {share_mde:.3} \
         (n {})",
        paired.len()
    );
    assert!(
        rec_lo <= 0.0,
        "[nonconv] the stand-off still resolves a reclaim cost on the interactive lane: paired \
         mean {rec_mean:+.1} ms 95 % CI [{rec_lo:+.1},{rec_hi:+.1}] MDE(80 %) {rec_mde:.1} \
         against the standing-off p95 {:.1} ms and the {drain_bound_ms:.1} ms full-buffer bound",
        q(&y_reclaim, 0.95),
    );
    eprintln!(
        "[nonconv] share direction: {positive}/{} paired reps positive (5 % sign threshold {})",
        paired.len(),
        sign_test_threshold(paired.len())
    );
    // The verdict -- accept: the share gain resolves and the reclaim cost no
    // longer does -- is recorded in GATE.md.
}
