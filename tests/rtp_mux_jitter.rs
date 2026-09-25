//! Interactive-message latency-jitter probe for the game-relay topology:
//! `game client -TCP-> access-server -rtp_mux(rtp+mux)-> proxy-server -TCP->
//! game server`.
//!
//! A game client's ping graph shows a steady one-way latency with regular
//! sudden spikes. Three mechanisms are suspected:
//!
//! (a) global head-of-line blocking at the RTP frame layer when a packet is
//!     lost/reordered (frame-mode delivery withholds every frame behind an
//!     unrepaired front hole; each mux frame maps 1:1 to an RTP frame);
//! (b) congestion-controller bufferbloat oscillation (a `+50%` probe vs a
//!     `-10%` drain);
//! (c) bursty sending (a 64-packet pacer burst floor; retransmits bypass the
//!     token bucket).
//!
//! These scenarios REPRODUCE and QUANTIFY the latency jitter so fixes can be
//! validated. With the exception of the two constitution gates below, the
//! arms are deliberately report-only measurement harnesses: the assertions
//! are loose sanity guards, and the `HolSummary` printed with `--nocapture`
//! is the deliverable, not a pass/fail gate.
//!
//! # The tri-mandate constitution (asserted here and in `dual_lane_mandates.rs`)
//!
//! The operator's product constitution is three mandates, jointly the
//! acceptance criterion for every change to the interactive path, and rtp_mux
//! owns the dual-lane topology that constrains them. Two are asserted in this
//! file, each with the derived bound stated beside its gate:
//!
//! * **M1 — low latency of the interactive lane**: the interactive lane's
//!   tail latency (p99, plus a spike bound) stays at its floor on the
//!   production dual-lane topology. Asserted by
//!   [`jitter_duallane_constitution_gate_p99`] (`full` tier): the bound is
//!   the topology's one-way delay floor (25 ms) plus a documented margin — the
//!   `250 ms` spike ceiling (~8× the measured ~29 ms p99, and the README's
//!   "zero >250 ms spikes" criterion) — gated on the **median of three**
//!   seeded runs, with a zero `>250 ms` spike count on every run.
//! * **M2 — reasonable goodput of the interactive lane**: the interactive
//!   lane delivers what it is offered (`delivery == 1.000`) **without
//!   inflating its own wire** to get there. Asserted by
//!   [`jitter_duallane_constitution_gate`] (**default tier** — runs on every
//!   `cargo test -p rtp_mux`; both quantities are deterministic counts, and
//!   counts belong in the always-run gate): the offered payload
//!   is the deterministic sent-message byte count, and the lane's *aggregate*
//!   client→server wire forwarded by the impairment proxy must stay within a
//!   fixed budget of it (6×, measured ~3.6× on the seeded `both` arm — ~1.6×
//!   headroom, so the aggregate wire may grow by ~+64 % before it trips).
//!
//! M3 — **high goodput of the bulk lane** (goodput ≥ a derived fraction of
//! the configured link rate, median-of-N) — is asserted in
//! `dual_lane_mandates.rs`, which states the full constitution module-level.
//!
//! Redundancy monotonicity is NOT a mandate: FEC recovery parity may
//! legitimately grow with loss; what must not happen is the interactive
//! lane's extra/armor packets inflating its own delivered wire.
//! The harness (`netem_test`) does not restate any of this: it keeps the
//! impairment instrument and points at the owning crates.
//!
//! Passes:
//! * [`jitter_decomposition`] — runs `solo`, `loss-only`, `bulk-only`, and
//!   `bulk+loss` on the same seeded impairment and prints the loss-vs-queueing
//!   decomposition (`loss_delta`, `queue_delta`, `combined_delta`, and whether
//!   combined is additive or super-additive) for p50/p90/p99/max and for the
//!   episode/spike counts. This is the separation of the loss tail from the
//!   queue tail. The same four arms are then repeated in RTP frame-delivery
//!   mode (`solo_frame`, `loss_frame`, `bulk_frame`, `bulk_and_loss_frame`) to
//!   measure the deployment's `frame_reassembly` path alongside byte-stream,
//!   and once more with receiver-side fast-forward
//!   ([`jitter_frame_reorder_decomposition`]: `solo_frame_reorder`,
//!   `loss_frame_reorder`, `bulk_frame_reorder`, `bulk_and_loss_frame_reorder`)
//!   to measure the deployment's interactive-lane `allow_reorder` mode against
//!   the strict frame-delivery table.
//! * [`jitter_frame_reorder_fec_arms`] — the deployment's real interactive-lane
//!   configuration: frame fast-forward **and** FEC (prompt tuning, permissive
//!   loss gate) at 2% loss, printed beside the strict-frame + FEC and the
//!   reorder FEC-off arms, with the FEC counters and the c2s wire bulk load so
//!   the offered load can be checked matched across all three.
//! * [`jitter_fec_arms_2pct`] / [`jitter_fec_arms_6pct`] — the interactive
//!   lane with FEC `off` / stock (`default`) / prompt parity
//!   (`instream_flush=true, small_group_parity_count=1`) at 2% and 6% loss,
//!   plus `bulk_and_loss_fec_prompt`, with the RTP FEC counters (parity sent,
//!   loss-gate skips) captured so the 5% gate's effect is visible directly.
//!
//! The remaining single-scenario tests keep the original arms for continuity.
//!
//! * [`jitter_duallane_arms`] — the deployment topology at MATCHED bulk load:
//!   the interactive lane (frame mode + FEC, prompt tuning) and the bulk lane
//!   (a second, independent, strict byte-stream, FEC-free RTP connection) run
//!   on separate `NetemPair` links. The `both` arm prints the interactive
//!   decomposition plus the bulk lane's wire goodput, so receiver-side
//!   fast-forward (`bulk_and_loss_duallane_reorder_fec`) can be compared with
//!   strict frame delivery (`bulk_and_loss_duallane_strict_fec`) at the SAME
//!   bulk load instead of the confounded single-connection arms.
//!
//! These tests are `#[ignore]`-d by default so they do not slow normal builds.
//! Run them with (release is expected; multi-threaded scenarios):
//!
//! ```sh
//! cargo test --release -p rtp_mux --test rtp_mux_jitter -- \
//!     --ignored --nocapture --test-threads=1
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mux::testkit::mux::{mux_client_connect_frame_delivery_via, mux_client_connect_via};
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::presets::gilbert_elliott_loss;
use netem_test::kit::stats::{HolSummary, combined_stats, summarize};
use netem_test::kit::{TestScope, submit_test_task};
use netem_test::{BottleneckShaper, Counters, LossModel, NetemConfig, NetemPair};
use rtp::testkit::frame::{
    rtp_frame_delivery_connect_reorder_with_fec_tuning_via,
    rtp_frame_delivery_connect_with_fec_tuning_via,
};
use rtp::testkit::rtp::{
    rtp_connect_with_mss_fec_tuning_and_observer_via, spawn_rtp_byte_sink_server_via,
};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use rtp_mux::testkit::mux_over_rtp::{
    send_timestamped_messages,
    spawn_mux_frame_delivery_latency_bulk_server_reorder_with_fec_tuning_via,
    spawn_mux_frame_delivery_latency_bulk_server_with_fec_tuning_via,
    spawn_mux_latency_bulk_server_with_fec_tuning_via,
};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::MissedTickBehavior;

/// One-way delay applied to every packet in both directions.
const OWD: Duration = Duration::from_millis(25);
/// Uniform jitter around [`OWD`].
const JITTER: Duration = Duration::from_millis(5);
/// `u32` loss threshold equal to `pct` percent per packet.
const fn loss_pct(pct: u32) -> u32 {
    (u32::MAX / 100) * pct
}
/// The original interactive-loss probe's 2% independent per-packet loss.
const LOSS_2: u32 = loss_pct(2);
/// A loss level above FEC's 5% enable threshold (see
/// `rtp/src/traffic_shaping/redundancy/fec_gate.rs`).
const LOSS_6: u32 = loss_pct(6);
/// Bottleneck rate for the bulk cases (1 MiB/s): chosen so a 2 MiB burst takes
/// ~2 s to drain, producing several hundred ms of queueing without the queue
/// growing without bound across periods.
const BULK_RATE_BPS: u64 = 1024 * 1024 * 8;
/// Bytes offered per bulk burst.
const BULK_BURST_BYTES: usize = 2 * 1024 * 1024;
/// Interval between bulk bursts.
const BULK_PERIOD: Duration = Duration::from_secs(3);
/// Let the ping stream establish a solo floor before the first burst.
const BULK_RAMP: Duration = Duration::from_millis(1500);
/// Interactive message size and cadence (a typical game ping).
const MSG_BYTES: usize = 256;
const CADENCE: Duration = Duration::from_millis(25);
/// Interactive run time — long enough to observe several bulk bursts.
const RUN_FOR: Duration = Duration::from_secs(30);
/// Drain stragglers before reading the latency channel.
const GRACE: Duration = Duration::from_secs(3);
/// Bounded queue for test-owned tasks (mirrors the sibling scenarios).
const TASK_QUEUE_BOUND: usize = netem_test::kit::TEST_TASK_QUEUE_BOUND;

/// Build one impairment direction: fixed delay + jitter, an independent-loss
/// threshold (`0` = none), and an optional rate cap.
fn link(seed: u64, loss: u32, rate_bps: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: JITTER,
        rate: rate_bps,
        loss,
        seed,
        ..NetemConfig::default()
    }
}

/// Build one impairment direction with every non-loss knob exposed: fixed
/// delay, jitter, iid loss, duplication, and sch_netem reordering (once
/// `reorder_gap_pkts` packets have accumulated, a packet that draws under the
/// reorder threshold jumps ahead of the delayed queue).
#[allow(clippy::too_many_arguments)]
fn link_custom(
    seed: u64,
    latency: Duration,
    jitter: Duration,
    loss: u32,
    duplicate: u32,
    reorder: u32,
    reorder_gap_pkts: u32,
    rate_bps: u64,
) -> NetemConfig {
    NetemConfig {
        latency,
        jitter,
        rate: rate_bps,
        loss,
        duplicate,
        reorder,
        reorder_gap_pkts,
        seed,
        ..NetemConfig::default()
    }
}

/// A periodic bulk burst: `burst_bytes` offered every `period`, as fast as the
/// transport accepts, for the duration of the run.
#[derive(Clone, Copy, Debug)]
struct BulkSpec {
    burst_bytes: usize,
    period: Duration,
}

/// The periodic 2 MiB / 3 s bulk burst used by every bulk arm.
const BULK: BulkSpec = BulkSpec {
    burst_bytes: BULK_BURST_BYTES,
    period: BULK_PERIOD,
};

/// Everything one jitter scenario needs.
#[derive(Clone, Debug)]
struct JitterScenario {
    label: String,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
    /// FEC enabled on both ends of the RTP connection.
    fec: bool,
    /// Per-connection FEC tuning; both ends must set the same value.
    fec_tuning: rtp::FecTuning,
}

/// Build a scenario with both FEC settings off (the original measurement arms).
fn scen(label: &str, c2s: NetemConfig, s2c: NetemConfig, bulk: Option<BulkSpec>) -> JitterScenario {
    JitterScenario {
        label: label.to_owned(),
        c2s,
        s2c,
        bulk,
        fec: false,
        fec_tuning: rtp::FecTuning::default(),
    }
}

/// The prompt-parity preset: force-flush each interactive data burst's open
/// FEC group at the burst tail, with a single parity symbol.
fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

/// Build a scenario with FEC enabled on both ends and the given per-connection
/// tuning (the deployment's interactive-lane configuration).
fn scen_fec(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
    fec_tuning: rtp::FecTuning,
) -> JitterScenario {
    JitterScenario {
        label: label.to_owned(),
        c2s,
        s2c,
        bulk,
        fec: true,
        fec_tuning,
    }
}

/// One measured arm: latency summary, wire counters, and the FEC counters the
/// observer last captured (`None` means FEC is disabled on the connection).
struct JitterRun {
    summary: HolSummary,
    counters: Counters,
    fec: Option<rtp::metrics::MetricsFecCounters>,
    rtx: Option<rtp::metrics::MetricsRetransmissionCounters>,
    /// Congestion-controller timeline captured at 50 ms cadence: the action,
    /// send rate, RTT floor/tolerance, and the drain/backoff counters. Empty
    /// for the legacy arms whose observer did not record it.
    timeline: Vec<CongestionRow>,
}

/// One sampled congestion-controller state row.
#[derive(Clone, Debug)]
struct CongestionRow {
    at: Duration,
    action: Option<rtp::metrics::MetricsCongestionAction>,
    send_rate: f64,
    cwnd: usize,
    in_flight: usize,
    smooth_rtt: Duration,
    /// The live retransmission timeout: `max(srtt + 4*rttvar, 1 s)`.
    rto: Duration,
    /// The lifetime minimum RTT, the second premise of the queue-premised
    /// fast-loss arming rule.
    min_rtt: Option<Duration>,
    floor: Option<Duration>,
    tolerance: Option<Duration>,
    persistent_for: Option<Duration>,
    delivery_peak: Option<f64>,
    delivery_rate: Option<f64>,
    /// Whether the latest rate sample was application-limited.  A shared lane
    /// that is app-limited has less queued than the pipe can carry, so it must
    /// not attribute cross-traffic delay or loss to its own send rate; the
    /// burst-loss arm uses this column to show the loss-backoff collapse that
    /// serializes the interactive repair.
    app_limited: Option<bool>,
    drain_target: Option<f64>,
    loss_backoff_target: Option<f64>,
    delay_drains: u64,
    loss_backoffs: u64,
    bandwidth_probes: u64,
    write_waiters: usize,
    queue_building: bool,
    gentle_mode: bool,
}

/// The metrics observer plus the FEC-counter, full-snapshot, congestion-
/// timeline, and raw-RTT-sample cells it fills, so a run can read the FEC
/// counters, the retransmission counters, the congestion state, and the RTT
/// sample stream after it completes.
///
/// The raw sample stream is what makes the *repair deadlines* recoverable: the
/// transport snapshot publishes `smoothed_rtt` and `retransmission_timeout`
/// but neither `rttvar`, the reorder window, nor the fast-loss arming bit, and
/// those three decide which repair path a lost interactive tail takes (see
/// [`replay_rto`]).
type ObserverBundle = (
    rtp::metrics::MetricsObserver,
    Arc<Mutex<Option<rtp::metrics::MetricsFecCounters>>>,
    Arc<Mutex<Option<rtp::metrics::MetricsSnapshot>>>,
    Arc<Mutex<Vec<CongestionRow>>>,
    Arc<Mutex<Vec<Duration>>>,
);

/// A lightweight metrics observer that retains the latest FEC and
/// retransmission counter snapshots, appends a congestion-controller timeline
/// row at a 50 ms cadence, and records every accepted raw RTT sample. The
/// cadence is coarse enough that the state snapshots do not perturb the
/// measured traffic; the raw RTT samples are captured event-only (no state
/// scan), because they are the estimator's own input and the only way to
/// recover `rttvar` from outside the transport.
fn fec_observer() -> ObserverBundle {
    let cell = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&cell);
    let snapshot_cell = Arc::new(Mutex::new(None));
    let snapshot_sink = Arc::clone(&snapshot_cell);
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let timeline_sink = Arc::clone(&timeline);
    let rtt_samples = Arc::new(Mutex::new(Vec::new()));
    let rtt_sink = Arc::clone(&rtt_samples);
    let last_ms = Arc::new(AtomicU64::new(0));
    let observer = rtp::metrics::MetricsObserver::selective(
        move |event, elapsed| {
            if event == rtp::metrics::MetricsEvent::RttSample {
                return rtp::metrics::MetricsInterest::EventOnly;
            }
            let now = elapsed.as_millis() as u64;
            let previous = last_ms.load(Ordering::Relaxed);
            if now >= previous.saturating_add(50) {
                last_ms.store(now, Ordering::Relaxed);
                rtp::metrics::MetricsInterest::Snapshot
            } else {
                rtp::metrics::MetricsInterest::Skip
            }
        },
        move |observation| {
            if let Some(sample) = observation.raw_rtt_sample {
                rtt_sink.lock().unwrap().push(sample);
            }
            if let Some(snapshot) = observation.snapshot {
                if let Some(fec) = snapshot.fec_counters {
                    *sink.lock().unwrap() = Some(fec);
                }
                *snapshot_sink.lock().unwrap() = Some(snapshot);
                timeline_sink.lock().unwrap().push(CongestionRow {
                    at: observation.elapsed,
                    action: snapshot.congestion_action,
                    send_rate: snapshot.send_rate_packets_per_second,
                    cwnd: snapshot.congestion_window_packets,
                    in_flight: snapshot.in_flight_packets,
                    smooth_rtt: snapshot.smoothed_rtt,
                    rto: snapshot.retransmission_timeout,
                    min_rtt: snapshot.minimum_rtt,
                    floor: snapshot.congestion_rtt_floor,
                    tolerance: snapshot.congestion_queue_tolerance,
                    persistent_for: snapshot.congestion_persistent_queue_for,
                    delivery_peak: snapshot.congestion_delivery_peak_packets_per_second,
                    delivery_rate: snapshot.delivery_rate_packets_per_second,
                    app_limited: snapshot.delivery_sample_app_limited,
                    drain_target: snapshot.congestion_drain_target_packets_per_second,
                    loss_backoff_target: snapshot.congestion_loss_backoff_target_packets_per_second,
                    delay_drains: snapshot.congestion_delay_drains,
                    loss_backoffs: snapshot.congestion_loss_backoffs,
                    bandwidth_probes: snapshot.congestion_bandwidth_probe_increases,
                    write_waiters: snapshot.application_write_waiters,
                    queue_building: snapshot.queue_building,
                    gentle_mode: snapshot.gentle_mode,
                });
            }
        },
    );
    (observer, cell, snapshot_cell, timeline, rtt_samples)
}

/// The RTO estimator's derived repair deadlines, recomputed in the test by
/// replaying the raw RTT samples through the same recursion as the shipped
/// `RtxTimer` (`BETA = 1/4`, `ALPHA = 1/8`, `K = 4`, `MIN_RTO = 1 s`).
///
/// The transport publishes `smoothed_rtt` and `retransmission_timeout` but not
/// `rttvar`, the reorder window, or the fast-loss arming bit — and those three
/// are exactly what decides which repair path a lost interactive tail takes:
/// an evidence-gated fast-loss declaration, the reorder-window ARQ
/// fall-through, the tail-loss probe, or the `MIN_RTO` floor.
#[derive(Clone, Copy, Debug)]
struct RepairDeadlines {
    srtt: Duration,
    rttvar: Duration,
    rto: Duration,
    /// `min(srtt + max(4*rttvar, srtt/4), rto)` — the stock reorder window,
    /// used to schedule an already-armed retransmit when `RTP_JITTER_CAP` is
    /// off.
    stock_reorder_window: Duration,
    /// `min(srtt + max(rttvar, srtt/4), rto)` — the window the shipped default
    /// (`RTP_JITTER_CAP` ON) actually schedules an out-of-order-passed
    /// retransmit with.
    fast_reorder_window: Duration,
    /// `4*rttvar < srtt/4`: the srtt-relative half of the composite fast-loss
    /// arming decision. `false` means a SACK gap is treated as reordering
    /// evidence and the reorder window owns the repair.
    fast_loss_armed: bool,
}

impl RepairDeadlines {
    fn at(srtt: Duration, rttvar: Duration) -> Self {
        let srtt_f = srtt.as_secs_f64();
        let rttvar_f = rttvar.as_secs_f64();
        let rto = Duration::from_secs_f64(srtt_f + 4. * rttvar_f).max(Duration::from_secs(1));
        let stock = Duration::from_secs_f64(srtt_f + (4. * rttvar_f).max(srtt_f / 4.));
        let fast = Duration::from_secs_f64(srtt_f + rttvar_f.max(srtt_f / 4.));
        Self {
            srtt,
            rttvar,
            rto,
            stock_reorder_window: stock.min(rto),
            fast_reorder_window: fast.min(rto),
            fast_loss_armed: 4. * rttvar_f < srtt_f / 4.,
        }
    }
}

/// The estimator trajectory over one arm: the last replayed state plus the
/// extremes, so an arm reports the *worst* deadline its tail could have been
/// waiting on rather than only the final one.
#[derive(Clone, Copy, Debug, Default)]
struct RepairTrajectory {
    samples: usize,
    last: Option<RepairDeadlines>,
    srtt_max: Duration,
    rttvar_max: Duration,
    rto_max: Duration,
    stock_reorder_window_max: Duration,
    fast_reorder_window_max: Duration,
    /// Whether the srtt-relative fast-loss gate was ever armed during the arm.
    fast_loss_armed_any: bool,
}

/// Replay the raw RTT samples through the shipped estimator recursion and
/// report the extremes of the derived deadlines.
fn replay_rto(samples: &[Duration]) -> RepairTrajectory {
    let mut trajectory = RepairTrajectory::default();
    let Some((&first, rest)) = samples.split_first() else {
        return trajectory;
    };
    let mut srtt = first.as_secs_f64();
    let mut rttvar = first.as_secs_f64() / 2.;
    let record = |srtt: f64, rttvar: f64, trajectory: &mut RepairTrajectory| {
        let deadlines = RepairDeadlines::at(
            Duration::from_secs_f64(srtt),
            Duration::from_secs_f64(rttvar),
        );
        trajectory.last = Some(deadlines);
        trajectory.srtt_max = trajectory.srtt_max.max(deadlines.srtt);
        trajectory.rttvar_max = trajectory.rttvar_max.max(deadlines.rttvar);
        trajectory.rto_max = trajectory.rto_max.max(deadlines.rto);
        trajectory.stock_reorder_window_max = trajectory
            .stock_reorder_window_max
            .max(deadlines.stock_reorder_window);
        trajectory.fast_reorder_window_max = trajectory
            .fast_reorder_window_max
            .max(deadlines.fast_reorder_window);
        trajectory.fast_loss_armed_any |= deadlines.fast_loss_armed;
        trajectory.samples += 1;
    };
    record(srtt, rttvar, &mut trajectory);
    for rtt in rest {
        let sample = rtt.as_secs_f64();
        rttvar = 0.75 * rttvar + 0.25 * (srtt - sample).abs();
        srtt = 0.875 * srtt + 0.125 * sample;
        record(srtt, rttvar, &mut trajectory);
    }
    trajectory
}

/// Print the congestion-controller timeline for one arm, one line per action
/// transition (plus the first and last sample) so a multi-second stall's
/// controller evolution is readable without dumping every 50 ms row.
fn print_congestion_timeline(label: &str, rows: &[CongestionRow]) {
    if rows.is_empty() {
        return;
    }
    eprintln!("[ctrl {label}] congestion timeline (action transitions + first/last):");
    let mut previous: Option<CongestionRow> = None;
    for (index, row) in rows.iter().enumerate() {
        let last = index + 1 == rows.len();
        let changed = previous
            .as_ref()
            .is_none_or(|p| p.action != row.action || p.queue_building != row.queue_building);
        if changed || last {
            let marker = if last { " last" } else { "" };
            let action = row.action.map(|a| a.as_str()).unwrap_or("none");
            eprintln!(
                "[ctrl {label}]{marker} t={:>7.1}ms action={action:<14} rate={:>8.1} cwnd={:>4} \
                 inflight={:>4} srtt={:>7.1}ms rto={:>7.1}ms min_rtt={:>7.1}ms floor={:>7.1}ms \
                 tol={:>7.1}ms queue={} gentle={} \
                 app_lim={:?} q_for={:?} peak={:?} d={:?} drains={} backoffs={} probes={} waiters={} \
                 drain_tgt={:?} bkoff_tgt={:?}",
                row.at.as_secs_f64() * 1000.0,
                row.send_rate,
                row.cwnd,
                row.in_flight,
                row.smooth_rtt.as_secs_f64() * 1000.0,
                row.rto.as_secs_f64() * 1000.0,
                row.min_rtt.map(|m| m.as_secs_f64() * 1000.0).unwrap_or(0.0),
                row.floor.map(|f| f.as_secs_f64() * 1000.0).unwrap_or(0.0),
                row.tolerance
                    .map(|t| t.as_secs_f64() * 1000.0)
                    .unwrap_or(0.0),
                row.queue_building,
                row.gentle_mode,
                row.app_limited,
                row.persistent_for,
                row.delivery_peak,
                row.delivery_rate,
                row.delay_drains,
                row.loss_backoffs,
                row.bandwidth_probes,
                row.write_waiters,
                row.drain_target,
                row.loss_backoff_target,
            );
        }
        previous = Some(row.clone());
    }
}

/// Run one interactive-vs-bulk/loss jitter scenario and return its measurements.
///
/// Spawns the combined mux-over-RTP server (it classifies each mux stream by
/// its first byte: `b'L'` = timestamped latency frames, any other byte = bulk
/// sink), one [`NetemPair`], and one mux connection carrying both streams. The
/// interactive `b'L'` stream sends [`MSG_BYTES`] messages every [`CADENCE`] for
/// [`RUN_FOR`], optionally contested by a periodic `b'B'` bulk burst. Both the
/// client and server RTP connections use the scenario's `fec`/`fec_tuning`.
async fn run_jitter(scenario: JitterScenario) -> JitterRun {
    let JitterScenario {
        label,
        c2s,
        s2c,
        bulk,
        fec,
        fec_tuning,
    } = scenario;
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, mut latencies, bulk_counter) =
                spawn_mux_latency_bulk_server_with_fec_tuning_via(&task_tx, fec, base, fec_tuning)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();

            let (observer, fec_cell, snapshot_cell, timeline_cell, _rtt_samples) = fec_observer();
            let (connected_read, connected_write) =
                rtp_connect_with_mss_fec_tuning_and_observer_via(
                    &task_tx,
                    pair.client_addr(),
                    fec,
                    rtp::udp::NO_FEC_MSS,
                    fec_tuning,
                    observer,
                )
                .await;
            let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);

            // Interactive latency stream (`b'L'`). Its read half stays parked
            // until the stream closes; the owning scope aborts it at teardown.
            let (mut lat_read, mut lat_write) = opener.open().await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = lat_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            // Optional bulk stream (`b'B'`) on the same mux connection.
            let bulk_write = if bulk.is_some() {
                let (mut bulk_read, bulk_write) = opener.open().await.unwrap();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = bulk_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                Some(bulk_write)
            } else {
                None
            };

            let interactive = async {
                if lat_write.write_all(b"L").await.is_err() {
                    return 0;
                }
                send_timestamped_messages(&mut lat_write, base, MSG_BYTES, CADENCE, RUN_FOR).await
            };
            let bulk_fut = async {
                let Some(mut write) = bulk_write else {
                    return 0;
                };
                if write.write_all(b"B").await.is_err() {
                    return 0;
                }
                let spec = bulk.expect("bulk_write is Some iff bulk is Some");
                let payload = cyclic_payload(spec.burst_bytes);
                periodic_burst(
                    &mut write,
                    &payload,
                    spec.burst_bytes,
                    spec.period,
                    BULK_RAMP,
                    RUN_FOR,
                )
                .await
            };
            let (sent, _bulk_written) = tokio::join!(interactive, bulk_fut);

            // Let stragglers arrive before draining the latency channel.
            tokio::time::sleep(GRACE).await;
            let counters = combined_stats(&pair);
            let mut samples = Vec::new();
            while let Ok(lat) = latencies.try_recv() {
                samples.push(lat);
            }
            let received = samples.len() as u64;
            let summary = summarize(
                samples,
                sent,
                received,
                bulk_counter.load(Ordering::Relaxed),
                RUN_FOR.as_secs_f64(),
            );
            let fec = *fec_cell.lock().unwrap();
            let snapshot = *snapshot_cell.lock().unwrap();
            let rtx = snapshot.map(|s| s.retransmission_counters);
            let timeline = std::mem::take(&mut *timeline_cell.lock().unwrap());

            print_summary(&label, &summary);
            eprintln!("[jitter {label}] pair stats = {counters:?}");
            if let Some(fec) = fec {
                eprintln!(
                    "[jitter {label}] fec parity_sent={} groups_flushed={} \
                     loss_gate_skips={} no_spare_capacity_skips={} burst_end_skips={} \
                     recovered={}",
                    fec.parity_sent,
                    fec.groups_flushed,
                    fec.groups_skipped_loss_gate,
                    fec.groups_skipped_no_spare_capacity,
                    fec.groups_skipped_burst_end,
                    fec.recovered_symbols,
                );
            }

            pair.stop();
            JitterRun {
                summary,
                counters,
                fec,
                rtx,
                timeline,
            }
        })
        .await
}

/// Wrap a single-decision [`JitterScenario`] in the standard timeout and run it.
async fn run_one(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
) -> JitterRun {
    with_timeout(
        Duration::from_secs(120),
        label,
        run_jitter(scen(label, c2s, s2c, bulk)),
    )
    .await
}

/// Run one interactive-vs-bulk/loss jitter scenario in RTP frame-delivery mode.
/// Mirrors [`run_jitter`] but connects through the frame-delivery accept/connect
/// helpers and a `frame_reassembly` mux client, matching the deployment path
/// (`rtp_mux` sets `frame_reassembly: true`). The scenario's `fec`/`fec_tuning`
/// are threaded to both peers (the FEC-off [`scen`] defaults leave them off), so
/// the deployment's frame-mode-plus-FEC path is measurable.
async fn run_jitter_frame(scenario: JitterScenario) -> JitterRun {
    run_jitter_frame_mode(scenario, false).await
}

/// [`run_jitter_frame`] with receiver-side fast-forward: both the frame-delivery
/// server and the frame-delivery client use
/// [`rtp::FrameMode::enabled_reordering`](rtp::FrameMode::enabled_reordering),
/// so a complete frame starting past an unrepaired in-order hole is delivered
/// immediately. Mirrors [`run_jitter_frame`] in every other respect.
async fn run_jitter_frame_reorder(scenario: JitterScenario) -> JitterRun {
    run_jitter_frame_mode(scenario, true).await
}

/// Shared body for [`run_jitter_frame`] (strict) and [`run_jitter_frame_reorder`]
/// (fast-forward): `reorder` selects the matching server/client frame-mode
/// helpers; both peers flip together because the mode is not negotiated.
async fn run_jitter_frame_mode(scenario: JitterScenario, reorder: bool) -> JitterRun {
    let JitterScenario {
        label,
        c2s,
        s2c,
        bulk,
        fec,
        fec_tuning,
    } = scenario;
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, mut latencies, bulk_counter) = if reorder {
                spawn_mux_frame_delivery_latency_bulk_server_reorder_with_fec_tuning_via(
                    &task_tx, fec, base, fec_tuning,
                )
                .await
                .unwrap()
            } else {
                spawn_mux_frame_delivery_latency_bulk_server_with_fec_tuning_via(
                    &task_tx, fec, base, fec_tuning,
                )
                .await
                .unwrap()
            };
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();

            let (observer, fec_cell, snapshot_cell, timeline_cell, _rtt_samples) = fec_observer();
            let (reader, writer) = if reorder {
                rtp_frame_delivery_connect_reorder_with_fec_tuning_via(
                    &task_tx,
                    pair.client_addr(),
                    fec,
                    fec_tuning,
                    Some(observer),
                )
                .await
            } else {
                rtp_frame_delivery_connect_with_fec_tuning_via(
                    &task_tx,
                    pair.client_addr(),
                    fec,
                    fec_tuning,
                    Some(observer),
                )
                .await
            };
            let opener = mux_client_connect_frame_delivery_via(&task_tx, reader, writer);

            // Interactive latency stream (`b'L'`). Its read half stays parked
            // until the stream closes; the owning scope aborts it at teardown.
            let (mut lat_read, mut lat_write) = opener.open().await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = lat_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            // Optional bulk stream (`b'B'`) on the same mux connection.
            let bulk_write = if bulk.is_some() {
                let (mut bulk_read, bulk_write) = opener.open().await.unwrap();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = bulk_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                Some(bulk_write)
            } else {
                None
            };

            let interactive = async {
                if lat_write.write_all(b"L").await.is_err() {
                    return 0;
                }
                send_timestamped_messages(&mut lat_write, base, MSG_BYTES, CADENCE, RUN_FOR).await
            };
            let bulk_fut = async {
                let Some(mut write) = bulk_write else {
                    return 0;
                };
                if write.write_all(b"B").await.is_err() {
                    return 0;
                }
                let spec = bulk.expect("bulk_write is Some iff bulk is Some");
                let payload = cyclic_payload(spec.burst_bytes);
                periodic_burst(
                    &mut write,
                    &payload,
                    spec.burst_bytes,
                    spec.period,
                    BULK_RAMP,
                    RUN_FOR,
                )
                .await
            };
            let (sent, _bulk_written) = tokio::join!(interactive, bulk_fut);

            // Let stragglers arrive before draining the latency channel.
            tokio::time::sleep(GRACE).await;
            let counters = combined_stats(&pair);
            let c2s = pair.stats_c2s();
            let mut samples = Vec::new();
            while let Ok((tag, lat)) = latencies.try_recv() {
                if tag == b'L' {
                    samples.push(lat);
                }
            }
            let received = samples.len() as u64;
            // The bulk sink's byte stream stalls at its first unrepaired hole
            // under frame fast-forward (the mux per-stream reader is in-order),
            // so the sink count collapses in the reorder arms even while the
            // wire carries the full offered load. Report the client->server
            // wire bytes as the load measure so strict and reorder arms are
            // compared at matched offered load; keep the sink count (now an
            // order-independent raw read total) as a goodput diagnostic.
            let sink_delivered = bulk_counter.load(Ordering::Relaxed);
            let summary = summarize(
                samples,
                sent,
                received,
                c2s.forwarded_bytes,
                RUN_FOR.as_secs_f64(),
            );

            print_summary(&label, &summary);
            eprintln!(
                "[jitter {label}] bulk sink delivered = {sink_delivered} bytes; \
                 wire c2s forwarded = {} bytes / {} pkts",
                c2s.forwarded_bytes, c2s.forwarded
            );
            eprintln!("[jitter {label}] pair stats = {counters:?}");
            let fec = *fec_cell.lock().unwrap();
            let snapshot = *snapshot_cell.lock().unwrap();
            let rtx = snapshot.map(|s| s.retransmission_counters);
            let timeline = std::mem::take(&mut *timeline_cell.lock().unwrap());
            if let Some(rtx) = rtx {
                eprintln!(
                    "[jitter {label}] rtx attempts={} first={} repeat={} rto={} reorder={} fast_loss={} pre_outage={} tail_probes={}",
                    rtx.attempts,
                    rtx.first_attempts,
                    rtx.repeat_attempts,
                    rtx.rto_reason,
                    rtx.reorder_reason,
                    rtx.fast_loss_reason,
                    rtx.pre_outage_reason,
                    rtx.tail_probes,
                );
            }
            if let Some(fec) = fec {
                eprintln!(
                    "[jitter {label}] fec parity_sent={} groups_flushed={} \
                     loss_gate_skips={} no_spare_capacity_skips={} burst_end_skips={} \
                     recovered={}",
                    fec.parity_sent,
                    fec.groups_flushed,
                    fec.groups_skipped_loss_gate,
                    fec.groups_skipped_no_spare_capacity,
                    fec.groups_skipped_burst_end,
                    fec.recovered_symbols,
                );
            }

            pair.stop();
            JitterRun {
                summary,
                counters,
                fec,
                rtx,
                timeline,
            }
        })
        .await
}

/// Wrap a single-decision frame-delivery [`JitterScenario`] in the standard
/// timeout and run it through [`run_jitter_frame`].
async fn run_one_frame(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
) -> JitterRun {
    with_timeout(
        Duration::from_secs(120),
        label,
        run_jitter_frame(scen(label, c2s, s2c, bulk)),
    )
    .await
}

/// Wrap a single-decision fast-forward frame-delivery [`JitterScenario`] in the
/// standard timeout and run it through [`run_jitter_frame_reorder`].
async fn run_one_frame_reorder(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
) -> JitterRun {
    with_timeout(
        Duration::from_secs(120),
        label,
        run_jitter_frame_reorder(scen(label, c2s, s2c, bulk)),
    )
    .await
}

/// Wrap a single-decision strict frame-delivery [`JitterScenario`] with FEC on
/// and the given tuning in the standard timeout, running it through
/// [`run_jitter_frame`].
async fn run_one_frame_fec(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
    fec_tuning: rtp::FecTuning,
) -> JitterRun {
    with_timeout(
        Duration::from_secs(120),
        label,
        run_jitter_frame(scen_fec(label, c2s, s2c, bulk, fec_tuning)),
    )
    .await
}

/// Wrap a single-decision fast-forward frame-delivery [`JitterScenario`] with
/// FEC on and the given tuning in the standard timeout, running it through
/// [`run_jitter_frame_reorder`].
async fn run_one_frame_reorder_fec(
    label: &str,
    c2s: NetemConfig,
    s2c: NetemConfig,
    bulk: Option<BulkSpec>,
    fec_tuning: rtp::FecTuning,
) -> JitterRun {
    with_timeout(
        Duration::from_secs(120),
        label,
        run_jitter_frame_reorder(scen_fec(label, c2s, s2c, bulk, fec_tuning)),
    )
    .await
}

/// Offer `burst_bytes` every `period` through `write`, as fast as the transport
/// accepts, starting after `ramp` and stopping at `run_for`. Returns the number
/// of payload bytes written.
///
/// The burst is offered in back-to-back `write_all` chunks with no pacing: the
/// transport's flow control and the link's rate cap are the only throttle, so
/// the ping stream observes the real queue the burst creates. When a burst
/// takes longer than `period`, the interval's [`MissedTickBehavior::Delay`]
/// coalesces missed ticks instead of firing a catch-up storm.
async fn periodic_burst(
    write: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    burst_bytes: usize,
    period: Duration,
    ramp: Duration,
    run_for: Duration,
) -> u64 {
    let start = Instant::now();
    tokio::time::sleep(ramp).await;
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick is immediate; consume it so bursts start at `ramp`.
    interval.tick().await;

    let mut cursor = 0usize;
    let mut written = 0u64;
    loop {
        if start.elapsed() >= run_for {
            break;
        }
        let mut remaining = burst_bytes;
        while remaining > 0 {
            if start.elapsed() >= run_for {
                return written;
            }
            let avail = payload.len() - cursor;
            let take = remaining.min(avail);
            if write
                .write_all(&payload[cursor..cursor + take])
                .await
                .is_err()
            {
                return written;
            }
            cursor = (cursor + take) % payload.len();
            remaining -= take;
            written += take as u64;
        }
        interval.tick().await;
    }
    written
}

/// Print the measurement summary so the numbers are visible with
/// `--nocapture`.
fn print_summary(label: &str, s: &HolSummary) {
    eprintln!(
        "[jitter {label}] sent={sent} recv={recv} delivery={del:.3} \
         p50={p50:.1} p90={p90:.1} p99={p99:.1} max={max:.1} \
         over250={o25:.3} over1000={o1k:.3} episodes={ep} max_run={mr} \
         bulk={bulk:.3} MiB/s",
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

/// Loose sanity guard: the scenario must have sent and received a sane number
/// of messages. This is documentation/regression insurance, not a latency
/// bound — the numbers above are the deliverable.
fn assert_sane(label: &str, s: &HolSummary) {
    assert!(s.sent >= 100, "[{label}] only {} sent", s.sent);
    assert!(s.received >= 50, "[{label}] only {} received", s.received);
}

/// Classify the p99 combined/delta-sum ratio as sub-additive, additive, or
/// super-additive.
fn classify_ratio(ratio: f64) -> &'static str {
    if !ratio.is_finite() {
        "n/a"
    } else if ratio > 1.15 {
        "super-additive"
    } else if ratio < 0.85 {
        "sub-additive"
    } else {
        "additive"
    }
}

/// A `HolSummary` latency extractor used by the decomposition table rows.
type SummaryMetric = fn(&HolSummary) -> f64;

/// Print the loss-vs-queueing decomposition table (the deliverable for
/// [`jitter_decomposition`]): the four absolute arms and the derived deltas,
/// for both the latency percentiles and the spike/episode counts. `mode`
/// labels the RTP delivery path (`byte-stream` or `frame-delivery`) so the two
/// tables are directly comparable.
fn print_decomposition(
    mode: &str,
    solo: &JitterRun,
    loss: &JitterRun,
    bulk: &JitterRun,
    both: &JitterRun,
) {
    eprintln!(
        "[decomp {mode}] interactive latency decomposition (2% loss, 1 MiB/s rate, 2 MiB/3 s bulk)"
    );
    eprintln!(
        "[decomp {mode}] metric       solo     loss     bulk     both   loss_d  queue_d   comb_d  comb/(loss+q)"
    );
    let mut p99_ratio = f64::NAN;
    let percentile_rows: [(&str, SummaryMetric); 4] = [
        ("p50", |s| s.p50),
        ("p90", |s| s.p90),
        ("p99", |s| s.p99),
        ("max", |s| s.max),
    ];
    for (name, get) in percentile_rows {
        let s = get(&solo.summary);
        let l = get(&loss.summary);
        let b = get(&bulk.summary);
        let c = get(&both.summary);
        let loss_delta = l - s;
        let queue_delta = b - s;
        let combined_delta = c - s;
        let denominator = loss_delta + queue_delta;
        let ratio = if denominator.abs() > f64::EPSILON {
            combined_delta / denominator
        } else {
            f64::NAN
        };
        if name == "p99" {
            p99_ratio = ratio;
        }
        eprintln!(
            "[decomp {mode}] {name:<8} {s:8.1} {l:8.1} {b:8.1} {c:8.1} \
             {loss_delta:8.1} {queue_delta:8.1} {combined_delta:8.1}     {ratio:6.2}"
        );
    }
    #[derive(Clone, Copy)]
    enum Spike {
        Pct(fn(&HolSummary) -> f64),
        Count(fn(&HolSummary) -> u64),
    }
    let spike_rows: [(&str, Spike); 3] = [
        ("over250", Spike::Pct(|s| s.over250_pct)),
        ("episodes", Spike::Count(|s| s.episodes)),
        ("max_run", Spike::Count(|s| s.max_run)),
    ];
    eprintln!(
        "[decomp {mode}] spikes       solo     loss     bulk     both   loss_d  queue_d   comb_d"
    );
    for (name, kind) in spike_rows {
        let (s, l, b, c) = match kind {
            Spike::Pct(get) => (
                get(&solo.summary),
                get(&loss.summary),
                get(&bulk.summary),
                get(&both.summary),
            ),
            Spike::Count(get) => (
                get(&solo.summary) as f64,
                get(&loss.summary) as f64,
                get(&bulk.summary) as f64,
                get(&both.summary) as f64,
            ),
        };
        eprintln!(
            "[decomp {mode}] {name:<8} {s:8.3} {l:8.3} {b:8.3} {c:8.3} \
             {:8.3} {:8.3} {:8.3}",
            l - s,
            b - s,
            c - s
        );
    }
    eprintln!(
        "[decomp {mode}] p99 combined/(loss_delta+queue_delta) = {p99_ratio:.2} => {}",
        classify_ratio(p99_ratio)
    );
    eprintln!(
        "[decomp {mode}] p99 loss_only_delta={:.1} bqueue_only_delta={:.1} sum={:.1} combined={:.1}",
        loss.summary.p99 - solo.summary.p99,
        bulk.summary.p99 - solo.summary.p99,
        (loss.summary.p99 - solo.summary.p99) + (bulk.summary.p99 - solo.summary.p99),
        both.summary.p99 - solo.summary.p99,
    );
}

/// Print one FEC-arm table: latency percentiles plus the RTP FEC counters that
/// say whether parity was actually emitted or the loss gate skipped it.
fn print_fec_table(level: &str, loss: u32, runs: &[(&str, JitterRun)]) {
    let pct = loss as f64 / u32::MAX as f64 * 100.0;
    eprintln!("[fec] --- interactive lane at {pct:.0}% per-packet loss (seeded link) ---");
    eprintln!(
        "[fec] arm                        p50     p90     p99     max  over250  ep  run  parity  flushed  gate_skip  spare_skip  burst_skip  recovered"
    );
    for (name, run) in runs {
        let s = &run.summary;
        let f = run.fec.unwrap_or_default();
        eprintln!(
            "[fec] {name:<27} {p50:7.1} {p90:7.1} {p99:7.1} {max:7.1} {o25:7.3} {ep:3} {mr:3} \
             {parity:6} {flushed:8} {gate:9} {spare:11} {burst:11} {recovered:9}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            max = s.max,
            o25 = s.over250_pct,
            ep = s.episodes,
            mr = s.max_run,
            parity = f.parity_sent,
            flushed = f.groups_flushed,
            gate = f.groups_skipped_loss_gate,
            spare = f.groups_skipped_no_spare_capacity,
            burst = f.groups_skipped_burst_end,
            recovered = f.recovered_symbols,
        );
    }
    let baseline = runs
        .iter()
        .find(|(name, _)| *name == "loss_fec_off")
        .map(|(_, run)| run)
        .expect("the FEC table always includes the loss_fec_off baseline arm");
    for (name, run) in runs {
        if *name == "solo" || *name == "loss_fec_off" {
            continue;
        }
        let s = &run.summary;
        let off = &baseline.summary;
        eprintln!(
            "[fec] {level} {name} - loss_fec_off: p50={:+.1} p90={:+.1} p99={:+.1} max={:+.1} \
             over250={:+.3} episodes={:+}",
            s.p50 - off.p50,
            s.p90 - off.p90,
            s.p99 - off.p99,
            s.max - off.max,
            s.over250_pct - off.over250_pct,
            s.episodes as i64 - off.episodes as i64,
        );
    }
    eprintln!("[fec] {level} wire forwarded (both directions; FEC parity inflates this):");
    for (name, run) in runs {
        eprintln!(
            "[fec] {level} {name:<27} forwarded_pkts={:>8} forwarded_bytes={:>10}",
            run.counters.forwarded, run.counters.forwarded_bytes,
        );
    }
}

/// Print the frame-delivery + FEC arm table: latency percentiles, the RTP FEC
/// counters (so it is visible whether parity was actually emitted), and the
/// client->server wire bulk load. The `_fec_off` reorder rows are the FEC-off
/// baselines and the `_fec` rows are the deployment's frame-mode-plus-FEC
/// path; the bulk column is the load-match check. It is the c2s wire bytes
/// (not the sink's delivered bytes): the frame fast-forward leaves the bulk
/// sink's byte stream stalled at its first hole, so the sink count collapses
/// in the reorder arms even though the wire carries the full offered load.
fn print_frame_fec_table(runs: &[(&str, JitterRun)]) {
    eprintln!(
        "[framefec] frame-delivery interactive lane, 2% per-packet loss; *_fec rows use prompt \
         tuning (instream_flush, small_group_parity_count=1)"
    );
    eprintln!(
        "[framefec] arm                              p50     p90     p99     max  over250  ep  run  \
         parity  flushed   gate  recovered     bulk"
    );
    for (name, run) in runs {
        let s = &run.summary;
        let f = run.fec.unwrap_or_default();
        eprintln!(
            "[framefec] {name:<32} {p50:7.1} {p90:7.1} {p99:7.1} {max:7.1} {o25:7.3} {ep:3} {mr:3} \
             {parity:7} {flushed:8} {gate:6} {recovered:9} {bulk:8.3}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            max = s.max,
            o25 = s.over250_pct,
            ep = s.episodes,
            mr = s.max_run,
            parity = f.parity_sent,
            flushed = f.groups_flushed,
            gate = f.groups_skipped_loss_gate,
            recovered = f.recovered_symbols,
            bulk = s.bulk_mibps,
        );
    }
    let find = |name: &str| -> Option<&JitterRun> {
        runs.iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, run)| run)
    };
    for (fec_arm, off_arm) in [
        ("loss_frame_reorder_fec", "loss_frame_reorder_fec_off"),
        (
            "bulk_and_loss_frame_reorder_fec",
            "bulk_and_loss_frame_reorder_fec_off",
        ),
    ] {
        let (Some(on), Some(off)) = (find(fec_arm), find(off_arm)) else {
            continue;
        };
        let s = &on.summary;
        let o = &off.summary;
        eprintln!(
            "[framefec] {fec_arm} - {off_arm}: p50={:+.1} p90={:+.1} p99={:+.1} max={:+.1} \
             over250={:+.3} episodes={:+} bulk={:+.3}",
            s.p50 - o.p50,
            s.p90 - o.p90,
            s.p99 - o.p99,
            s.max - o.max,
            s.over250_pct - o.over250_pct,
            s.episodes as i64 - o.episodes as i64,
            s.bulk_mibps - o.bulk_mibps,
        );
    }
}

/// Run the four FEC treatments (plus a loss-free `solo` reference on the same
/// seeds) at one loss level and print the table.
async fn run_fec_level(level: &str, loss: u32, c2s_seed: u64, s2c_seed: u64) {
    let stock = rtp::FecTuning::default();
    let prompt = prompt_tuning();
    let arms: [(&str, bool, rtp::FecTuning, bool, bool); 5] = [
        // name, fec, tuning, bulk, loss-applied
        ("solo", false, stock, false, false),
        ("loss_fec_off", false, stock, false, true),
        ("loss_fec_default", true, stock, false, true),
        ("loss_fec_prompt", true, prompt, false, true),
        ("bulk_and_loss_fec_prompt", true, prompt, true, true),
    ];
    let mut runs: Vec<(&str, JitterRun)> = Vec::new();
    for (name, fec, tuning, with_bulk, with_loss) in arms {
        let label = format!("{level}/{name}");
        let applied = if with_loss { loss } else { 0 };
        let rate = if with_bulk { BULK_RATE_BPS } else { 0 };
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_jitter(JitterScenario {
                label: label.clone(),
                c2s: link(c2s_seed, applied, rate),
                s2c: link(s2c_seed, applied, rate),
                bulk: with_bulk.then_some(BULK),
                fec,
                fec_tuning: tuning,
            }),
        )
        .await;
        assert_sane(&label, &run.summary);
        runs.push((name, run));
    }
    print_fec_table(level, loss, &runs);
}

/// Deliverable 1: the loss-vs-queueing decomposition on the same seeded link,
/// printed as the table that separates the loss tail from the queue tail.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; eight ~35 s arms (byte-stream + frame-delivery); run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_decomposition() {
    let solo = run_one("solo", link(11, 0, 0), link(12, 0, 0), None).await;
    let loss_only = run_one("loss-only", link(21, LOSS_2, 0), link(22, LOSS_2, 0), None).await;
    let bulk_only = run_one(
        "bulk-only",
        link(31, 0, BULK_RATE_BPS),
        link(32, 0, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;
    let bulk_and_loss = run_one(
        "bulk+loss",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;

    for (label, run) in [
        ("solo", &solo),
        ("loss-only", &loss_only),
        ("bulk-only", &bulk_only),
        ("bulk+loss", &bulk_and_loss),
    ] {
        assert_sane(label, &run.summary);
    }
    print_decomposition("byte-stream", &solo, &loss_only, &bulk_only, &bulk_and_loss);

    // The same four arms in the deployment's frame-delivery path.
    let solo_frame = run_one_frame("solo_frame", link(11, 0, 0), link(12, 0, 0), None).await;
    let loss_frame =
        run_one_frame("loss_frame", link(21, LOSS_2, 0), link(22, LOSS_2, 0), None).await;
    let bulk_frame = run_one_frame(
        "bulk_frame",
        link(31, 0, BULK_RATE_BPS),
        link(32, 0, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;
    let bulk_and_loss_frame = run_one_frame(
        "bulk_and_loss_frame",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;

    for (label, run) in [
        ("solo_frame", &solo_frame),
        ("loss_frame", &loss_frame),
        ("bulk_frame", &bulk_frame),
        ("bulk_and_loss_frame", &bulk_and_loss_frame),
    ] {
        assert_sane(label, &run.summary);
    }
    print_decomposition(
        "frame-delivery",
        &solo_frame,
        &loss_frame,
        &bulk_frame,
        &bulk_and_loss_frame,
    );
}

/// Deliverable 1b: the same four frame-delivery arms with receiver-side
/// fast-forward (`FrameMode::enabled_reordering`), the deployment's
/// interactive-lane mode. Both the accept side (`*_reorder_via`) and the
/// connect side (`rtp_frame_delivery_connect_reorder_via`) select
/// `allow_reorder`, so a complete interactive frame is delivered as soon as
/// it arrives instead of waiting behind a bulk hole. The table is printed with
/// the `frame-reorder` label and is directly comparable to the strict
/// `frame-delivery` table from [`jitter_decomposition`].
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; four ~35 s fast-forward frame arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_frame_reorder_decomposition() {
    let solo_frame_reorder =
        run_one_frame_reorder("solo_frame_reorder", link(11, 0, 0), link(12, 0, 0), None).await;
    let loss_frame_reorder = run_one_frame_reorder(
        "loss_frame_reorder",
        link(21, LOSS_2, 0),
        link(22, LOSS_2, 0),
        None,
    )
    .await;
    let bulk_frame_reorder = run_one_frame_reorder(
        "bulk_frame_reorder",
        link(31, 0, BULK_RATE_BPS),
        link(32, 0, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;
    let bulk_and_loss_frame_reorder = run_one_frame_reorder(
        "bulk_and_loss_frame_reorder",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;

    for (label, run) in [
        ("solo_frame_reorder", &solo_frame_reorder),
        ("loss_frame_reorder", &loss_frame_reorder),
        ("bulk_frame_reorder", &bulk_frame_reorder),
        ("bulk_and_loss_frame_reorder", &bulk_and_loss_frame_reorder),
    ] {
        assert_sane(label, &run.summary);
    }
    print_decomposition(
        "frame-reorder",
        &solo_frame_reorder,
        &loss_frame_reorder,
        &bulk_frame_reorder,
        &bulk_and_loss_frame_reorder,
    );
}

/// Deliverable 1c: the deployment's real interactive-lane configuration — RTP
/// frame-delivery with receiver-side fast-forward **and** FEC. Runs the strict
/// frame + FEC and reorder frame + FEC arms beside the reorder FEC-off
/// baselines on the same seeded 2% link, so the three configurations can be
/// read off one table. The `_fec` arms use the interactive prompt tuning
/// (`instream_flush`, `small_group_parity_count = 1`), which selects FEC's
/// permissive loss gate so parity actually opens at 2%.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; six ~35 s frame+FEC arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_frame_reorder_fec_arms() {
    let prompt = prompt_tuning();

    // Strict frame delivery + FEC.
    let loss_frame_fec = run_one_frame_fec(
        "loss_frame_fec",
        link(21, LOSS_2, 0),
        link(22, LOSS_2, 0),
        None,
        prompt,
    )
    .await;
    let bulk_and_loss_frame_fec = run_one_frame_fec(
        "bulk_and_loss_frame_fec",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
        prompt,
    )
    .await;

    // Frame fast-forward, FEC off (the measured baselines).
    let loss_frame_reorder_fec_off = run_one_frame_reorder(
        "loss_frame_reorder_fec_off",
        link(21, LOSS_2, 0),
        link(22, LOSS_2, 0),
        None,
    )
    .await;
    let bulk_and_loss_frame_reorder_fec_off = run_one_frame_reorder(
        "bulk_and_loss_frame_reorder_fec_off",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;

    // Frame fast-forward + FEC (the deployment's real path).
    let loss_frame_reorder_fec = run_one_frame_reorder_fec(
        "loss_frame_reorder_fec",
        link(21, LOSS_2, 0),
        link(22, LOSS_2, 0),
        None,
        prompt,
    )
    .await;
    let bulk_and_loss_frame_reorder_fec = run_one_frame_reorder_fec(
        "bulk_and_loss_frame_reorder_fec",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
        prompt,
    )
    .await;

    let runs = [
        ("loss_frame_fec", loss_frame_fec),
        ("bulk_and_loss_frame_fec", bulk_and_loss_frame_fec),
        ("loss_frame_reorder_fec_off", loss_frame_reorder_fec_off),
        ("loss_frame_reorder_fec", loss_frame_reorder_fec),
        (
            "bulk_and_loss_frame_reorder_fec_off",
            bulk_and_loss_frame_reorder_fec_off,
        ),
        (
            "bulk_and_loss_frame_reorder_fec",
            bulk_and_loss_frame_reorder_fec,
        ),
    ];
    for (label, run) in &runs {
        assert_sane(label, &run.summary);
    }
    print_frame_fec_table(&runs);
}

/// Print the per-arm interactive-latency percentiles, delivery, client->server
/// wire load, and the RTP retransmission counters for the bulk+loss+reorder
/// decomposition. Unlike [`print_nonloss_table`] this includes the bulk load
/// and the loss column, so the isolated reorder arm can be compared with the
/// combined one at matched offered load.
fn print_blr_table(runs: &[(&str, JitterRun)]) {
    eprintln!(
        "[blr] interactive lane: frame fast-forward + prompt FEC, 2% loss, 10% reorder (gap 2); \
         bulk = 2 MiB / 3 s on the same connection except where noted"
    );
    eprintln!(
        "[blr] arm                      p50     p90     p99     max  over250  del  recv  \
         wire_bytes   rtx_first  rtx_rto  rtx_reord  rtx_fast  rtx_pre  rtx_tail  rtx_repeat"
    );
    for (name, run) in runs {
        let s = &run.summary;
        let r = run.rtx.unwrap_or_default();
        eprintln!(
            "[blr] {name:<22} {p50:7.1} {p90:7.1} {p99:7.1} {max:7.1} {o25:7.3} {del:5.3} \
             {recv:5} {wbytes:>11} {first:>11} {rto:8} {reord:10} {fast:9} {pre:8} {tail:9} \
             {repeat:10}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            max = s.max,
            o25 = s.over250_pct,
            del = s.delivery_pct,
            recv = s.received,
            wbytes = run.counters.forwarded_bytes,
            first = r.first_attempts,
            rto = r.rto_reason,
            reord = r.reorder_reason,
            fast = r.fast_loss_reason,
            pre = r.pre_outage_reason,
            tail = r.tail_probes,
            repeat = r.repeat_attempts,
        );
    }
    for (name, run) in runs {
        print_congestion_timeline(name, &run.timeline);
    }
}

/// Deliverable 1d: the missing combination — the deployment's interactive lane
/// (frame fast-forward + prompt FEC) with a bulk burst on the SAME connection
/// plus 2% loss AND 10% reorder. The prior audit saw a p99 stall (1.4-2 s)
/// here while bulk-off / loss-only / reorder-only arms sat at the floor. This
/// is a report-only arm: the printed table is the deliverable, with the RTP
/// retransmission counters (fast_loss, reorder, rto, pre_outage, tail_probes)
/// so the stall's repair mechanism is visible.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; four ~35 s arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_frame_reorder_fec_bulk_loss_reorder() {
    let prompt = prompt_tuning();
    // 10% per-packet reorder with a two-packet gap, matching the non-loss
    // reorder arms, combined with the 2% loss and the bulk rate cap.
    let reorder_link = |seed: u64, loss: u32, bulk: bool| {
        link_custom(
            seed,
            OWD,
            JITTER,
            loss,
            0,
            IMPAIR_10,
            2,
            if bulk { BULK_RATE_BPS } else { 0 },
        )
    };

    let bulk_loss = run_one_frame_reorder_fec(
        "bulk+loss",
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
        prompt,
    )
    .await;
    let loss_reorder = run_one_frame_reorder_fec(
        "loss+reorder",
        reorder_link(51, LOSS_2, false),
        reorder_link(52, LOSS_2, false),
        None,
        prompt,
    )
    .await;
    let bulk_reorder = run_one_frame_reorder_fec(
        "bulk+reorder",
        reorder_link(61, 0, true),
        reorder_link(62, 0, true),
        Some(BULK),
        prompt,
    )
    .await;
    let bulk_loss_reorder = run_one_frame_reorder_fec(
        "bulk+loss+reorder",
        reorder_link(71, LOSS_2, true),
        reorder_link(72, LOSS_2, true),
        Some(BULK),
        prompt,
    )
    .await;

    let runs = [
        ("bulk+loss", bulk_loss),
        ("loss+reorder", loss_reorder),
        ("bulk+reorder", bulk_reorder),
        ("bulk+loss+reorder", bulk_loss_reorder),
    ];
    for (label, run) in &runs {
        assert_sane(label, &run.summary);
    }
    let views: Vec<(&str, JitterRun)> = runs.into_iter().collect();
    print_blr_table(&views);
}

/// Deliverable 2: FEC arms at 2% loss (below FEC's 5% enable gate).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; five ~35 s arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_fec_arms_2pct() {
    run_fec_level("2pct", LOSS_2, 51, 52).await;
}

/// Deliverable 2: FEC arms at 6% loss (above FEC's 5% enable gate).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; five ~35 s arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_fec_arms_6pct() {
    run_fec_level("6pct", LOSS_6, 61, 62).await;
}

/// A 10% per-packet reorder/duplication threshold (`u32` fraction).
const IMPAIR_10: u32 = u32::MAX / 10;

/// Print one non-loss arm's latency percentiles, wire load, and RTP
/// retransmission counters so a repair/reorder artifact is separable from
/// expected propagation jitter.
fn print_nonloss_table(runs: &[(&str, JitterRun)]) {
    eprintln!(
        "[nonloss] deployment interactive lane: frame fast-forward + prompt FEC, no loss, no bulk"
    );
    eprintln!(
        "[nonloss] arm                     p50     p90     p99     max  over250  del  recv  \
         wire_pkts  wire_bytes  rtx_first  rtx_rto  rtx_reord  rtx_fast  rtx_repeat  parity"
    );
    for (name, run) in runs {
        let s = &run.summary;
        let r = run.rtx.unwrap_or_default();
        let parity = run.fec.map(|f| f.parity_sent).unwrap_or(0);
        eprintln!(
            "[nonloss] {name:<24} {p50:7.1} {p90:7.1} {p99:7.1} {max:7.1} {o25:7.3} {del:5.3} \
             {recv:5} {wpkts:>10} {wbytes:>11} {first:>10} {rto:8} {reord:10} {fast:9} \
             {repeat:11} {parity:7}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            max = s.max,
            o25 = s.over250_pct,
            del = s.delivery_pct,
            recv = s.received,
            wpkts = run.counters.forwarded,
            wbytes = run.counters.forwarded_bytes,
            first = r.first_attempts,
            rto = r.rto_reason,
            reord = r.reorder_reason,
            fast = r.fast_loss_reason,
            repeat = r.repeat_attempts,
        );
    }
}

/// Non-loss impairment arm sweep on the deployment's interactive lane: the
/// clean no-jitter floor, jitter alone, a one-slot reorder window, datagram
/// duplication, and reorder+duplication, each with the RTP retransmission
/// counters so a spurious repair is visible. A strict-frame reorder arm is
/// printed beside the fast-forward arms to show whether the already-deployed
/// fast-forward is what keeps the reorder window from costing latency.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; six ~35 s non-loss arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_nonloss_impairments() {
    let prompt = prompt_tuning();
    let clean_a = link_custom(71, OWD, Duration::ZERO, 0, 0, 0, 0, 0);
    let clean_b = link_custom(72, OWD, Duration::ZERO, 0, 0, 0, 0, 0);
    let jitter_a = link_custom(73, OWD, JITTER, 0, 0, 0, 0, 0);
    let jitter_b = link_custom(74, OWD, JITTER, 0, 0, 0, 0, 0);
    let reorder_a = link_custom(75, OWD, Duration::ZERO, 0, 0, IMPAIR_10, 2, 0);
    let reorder_b = link_custom(76, OWD, Duration::ZERO, 0, 0, IMPAIR_10, 2, 0);
    let dup_a = link_custom(77, OWD, Duration::ZERO, 0, IMPAIR_10, 0, 0, 0);
    let dup_b = link_custom(78, OWD, Duration::ZERO, 0, IMPAIR_10, 0, 0, 0);
    let reorder_dup_a = link_custom(79, OWD, Duration::ZERO, 0, IMPAIR_10, IMPAIR_10, 2, 0);
    let reorder_dup_b = link_custom(80, OWD, Duration::ZERO, 0, IMPAIR_10, IMPAIR_10, 2, 0);
    let strict_reorder_a = link_custom(81, OWD, Duration::ZERO, 0, 0, IMPAIR_10, 2, 0);
    let strict_reorder_b = link_custom(82, OWD, Duration::ZERO, 0, 0, IMPAIR_10, 2, 0);

    let mut runs: Vec<(&str, JitterRun)> = Vec::new();
    runs.push((
        "ff_clean_nojitter",
        run_one_frame_reorder_fec("ff_clean_nojitter", clean_a, clean_b, None, prompt).await,
    ));
    runs.push((
        "ff_jitter_only",
        run_one_frame_reorder_fec("ff_jitter_only", jitter_a, jitter_b, None, prompt).await,
    ));
    runs.push((
        "ff_reorder_only",
        run_one_frame_reorder_fec("ff_reorder_only", reorder_a, reorder_b, None, prompt).await,
    ));
    runs.push((
        "ff_dup_only",
        run_one_frame_reorder_fec("ff_dup_only", dup_a, dup_b, None, prompt).await,
    ));
    runs.push((
        "ff_reorder_dup",
        run_one_frame_reorder_fec("ff_reorder_dup", reorder_dup_a, reorder_dup_b, None, prompt)
            .await,
    ));
    runs.push((
        "strict_reorder_only",
        run_one_frame_fec(
            "strict_reorder_only",
            strict_reorder_a,
            strict_reorder_b,
            None,
            prompt,
        )
        .await,
    ));
    for (label, run) in &runs {
        assert_sane(label, &run.summary);
    }
    print_nonloss_table(&runs);
}

/// Reorder-rate curve on the deployment's interactive lane: the fast-forward
/// path at 1%, 3%, and 10% per-packet reorder (gap 2) beside the strict path
/// at 10%, so the p99 cost can be attributed to the reorder depth and to the
/// fast-forward choice.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; four ~35 s reorder arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_reorder_rate_curve() {
    let prompt = prompt_tuning();
    let mut runs: Vec<(&str, JitterRun)> = Vec::new();
    for (label, pct) in [
        ("ff_reorder_1pct", 1u32),
        ("ff_reorder_3pct", 3),
        ("ff_reorder_10pct", 10),
    ] {
        let a = link_custom(
            90 + pct as u64,
            OWD,
            Duration::ZERO,
            0,
            0,
            loss_pct(pct),
            2,
            0,
        );
        let b = link_custom(
            190 + pct as u64,
            OWD,
            Duration::ZERO,
            0,
            0,
            loss_pct(pct),
            2,
            0,
        );
        runs.push((
            label,
            run_one_frame_reorder_fec(label, a, b, None, prompt).await,
        ));
    }
    let a = link_custom(81, OWD, Duration::ZERO, 0, 0, loss_pct(10), 2, 0);
    let b = link_custom(82, OWD, Duration::ZERO, 0, 0, loss_pct(10), 2, 0);
    runs.push((
        "strict_reorder_10pct",
        run_one_frame_fec("strict_reorder_10pct", a, b, None, prompt).await,
    ));
    for (label, run) in &runs {
        assert_sane(label, &run.summary);
    }
    print_nonloss_table(&runs);
}

/// Direction split of the 3% reorder collapse: c2s-only reorder (a data hole)
/// versus s2c-only reorder (an acknowledgement-path reorder), both on the
/// fast-forward interactive lane, so the stall can be attributed to the data
/// or the ACK path.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; two ~35 s reorder arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_reorder_direction() {
    let prompt = prompt_tuning();
    let clean = |seed: u64| link_custom(seed, OWD, Duration::ZERO, 0, 0, 0, 0, 0);
    let reord = |seed: u64| link_custom(seed, OWD, Duration::ZERO, 0, 0, loss_pct(3), 2, 0);
    let c2s =
        run_one_frame_reorder_fec("ff_reorder_c2s_3pct", reord(101), clean(102), None, prompt)
            .await;
    let s2c =
        run_one_frame_reorder_fec("ff_reorder_s2c_3pct", clean(103), reord(104), None, prompt)
            .await;
    assert_sane("ff_reorder_c2s_3pct", &c2s.summary);
    assert_sane("ff_reorder_s2c_3pct", &s2c.summary);
    print_nonloss_table(&[("ff_reorder_c2s_3pct", c2s), ("ff_reorder_s2c_3pct", s2c)]);
}

/// The floor: interactive pings on a delay+jitter link, no loss, no bulk.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; ~35 s measurement; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_interactive_solo() {
    let label = "solo";
    let run = run_one(label, link(11, 0, 0), link(12, 0, 0), None).await;
    assert_sane(label, &run.summary);
}

/// Isolates loss/HOL repair latency: interactive pings on a 2% lossy link,
/// no bulk.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; ~35 s measurement; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_interactive_with_loss() {
    let label = "loss";
    let run = run_one(label, link(21, LOSS_2, 0), link(22, LOSS_2, 0), None).await;
    assert_sane(label, &run.summary);
}

/// Isolates bufferbloat/queueing: interactive pings plus a periodic 2 MiB bulk
/// burst on a 1 MiB/s rate-limited link, no loss.
///
/// Runs the same rate-limited link twice — bulk on, then bulk off — so the
/// added latency can be attributed to the burst's queue. The delta is printed
/// explicitly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; ~65 s measurement (two arms); run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_interactive_with_bulk() {
    let bulk_on = run_one(
        "bulk-on",
        link(31, 0, BULK_RATE_BPS),
        link(32, 0, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;
    let bulk_off = run_one(
        "bulk-off",
        link(31, 0, BULK_RATE_BPS),
        link(32, 0, BULK_RATE_BPS),
        None,
    )
    .await;

    eprintln!(
        "[jitter attribution] bulk-on minus bulk-off: p50={:.1} ms p90={:.1} ms \
         p99={:.1} ms max={:.1} ms over250={:+.3}",
        bulk_on.summary.p50 - bulk_off.summary.p50,
        bulk_on.summary.p90 - bulk_off.summary.p90,
        bulk_on.summary.p99 - bulk_off.summary.p99,
        bulk_on.summary.max - bulk_off.summary.max,
        bulk_on.summary.over250_pct - bulk_off.summary.over250_pct,
    );

    assert_sane("bulk-on", &bulk_on.summary);
    assert_sane("bulk-off", &bulk_off.summary);
}

/// The realistic case: interactive pings plus the periodic bulk burst on a 2%
/// lossy, rate-limited link.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; ~35 s measurement; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_interactive_bulk_and_loss() {
    let label = "bulk+loss";
    let run = run_one(
        label,
        link(41, LOSS_2, BULK_RATE_BPS),
        link(42, LOSS_2, BULK_RATE_BPS),
        Some(BULK),
    )
    .await;
    assert_sane(label, &run.summary);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Dual-lane interactive latency: the deployment topology at matched bulk load
// ═══════════════════════════════════════════════════════════════════════════════

/// Which impairments a dual-lane arm applies. Each lane rides its own
/// [`NetemPair`], so the interactive and bulk impairments are independent: the
/// bulk lane's rate cap and loss never touch the interactive lane's RTP
/// connection.
#[derive(Clone, Copy, Debug)]
enum DualImpairment {
    /// No loss, no bulk: the interactive-lane floor.
    Solo,
    /// 2% loss on both lanes, no bulk.
    Loss,
    /// No loss, bulk burst contesting only the bulk lane.
    Bulk,
    /// 2% loss plus the bulk burst: the deployment case.
    Both,
}

impl DualImpairment {
    fn has_loss(self) -> bool {
        matches!(self, Self::Loss | Self::Both)
    }

    fn has_bulk(self) -> bool {
        matches!(self, Self::Bulk | Self::Both)
    }
}

/// One measured dual-lane arm: the interactive-lane [`JitterRun`] (latency +
/// FEC evidence) plus the bulk lane's wire load and sink goodput.
struct DualRun {
    run: JitterRun,
    /// Interactive-lane client->server wire bytes forwarded by the impairment
    /// proxy: the interactive lane's own offered wire (data + repairs +
    /// control) — the wire-budget evidence for the constitution gate.
    int_c2s_wire_bytes: u64,
    /// Bulk-lane client->server wire bytes forwarded by the impairment proxy:
    /// the offered bulk load, i.e. the matched-load check across the two
    /// interactive frame modes.
    bulk_wire_bytes: u64,
    /// Bulk-lane sink-delivered payload bytes (independent of wire load).
    bulk_sink_bytes: u64,
    /// Combined bulk-pair counters (both directions).
    bulk_counters: Counters,
    /// The interactive lane's echo latencies in delivery order (ms), so an arm
    /// can report *where* its slow echoes sit rather than one opaque maximum:
    /// a cluster at `~1000 ms` is the `MIN_RTO` floor, a cluster at the
    /// reorder window plus a round trip is the ARQ fall-through, and a cluster
    /// at the one-way floor is a same-round-trip parity/armor recovery.
    echo_latencies: Vec<f64>,
    /// The replayed RTO-estimator trajectory: the repair deadlines (reorder
    /// window, RTO) the measured tail was waiting on.
    repair: RepairTrajectory,
}

/// Run one dual-lane arm: the interactive lane on its own RTP connection
/// (frame mode + FEC per `interactive_reorder`) and the bulk lane on a second,
/// independent, strict byte-stream FEC-free RTP connection. The two lanes ride
/// two separate [`NetemPair`]s so the bulk burst cannot head-of-line block the
/// interactive RTP connection, and the bulk lane is byte-for-byte identical in
/// the fast-forward and strict arms.
async fn run_duallane(
    label: &str,
    interactive_reorder: bool,
    impairment: DualImpairment,
) -> DualRun {
    let interactive_loss = if impairment.has_loss() { LOSS_2 } else { 0 };
    let bulk_loss = if impairment.has_loss() { LOSS_2 } else { 0 };
    let bulk_rate = if impairment.has_bulk() {
        BULK_RATE_BPS
    } else {
        0
    };
    // The interactive lane never takes the rate cap (the cap is the bulk
    // lane's own connection); its loss is the interactive `2%`.
    let int_c2s = link(41, interactive_loss, 0);
    let int_s2c = link(42, interactive_loss, 0);
    // The bulk lane is byte-for-byte identical in the fast-forward and strict
    // arms (same seeds, same loss, same rate cap), so the wire bulk goodput is
    // the matched-load evidence.
    let bulk_c2s = link(43, bulk_loss, bulk_rate);
    let bulk_s2c = link(44, bulk_loss, bulk_rate);
    run_duallane_links(
        label,
        interactive_reorder,
        int_c2s,
        int_s2c,
        bulk_c2s,
        bulk_s2c,
        impairment.has_bulk(),
    )
    .await
}

/// [`run_duallane`] with explicit per-lane impairment configs, so a report arm
/// can place a bursty loss model on the interactive lane while keeping the
/// bulk lane byte-for-byte identical to the production `both` arm.
async fn run_duallane_links(
    label: &str,
    interactive_reorder: bool,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk_c2s: NetemConfig,
    bulk_s2c: NetemConfig,
    has_bulk: bool,
) -> DualRun {
    run_duallane_links_shaped(
        label,
        interactive_reorder,
        DualLaneLinks {
            int_c2s,
            int_s2c,
            bulk_c2s,
            bulk_s2c,
        },
        has_bulk,
        None,
    )
    .await
}

/// One dual-lane arm's four per-direction impairment configs, bundled so the
/// shaped runner stays within the argument budget.
struct DualLaneLinks {
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk_c2s: NetemConfig,
    bulk_s2c: NetemConfig,
}

/// [`run_duallane_links`] with an optional shared client->server
/// [`BottleneckShaper`]. When `Some`, both lanes' c2s traffic contends for one
/// serialization clock — the shared low-capacity link — instead of each lane
/// getting its own per-flow `rate` cap; the s2c direction is never shared. The
/// per-lane `c2s` configs must carry `rate = 0` (the shaper owns the rate); a
/// non-zero rate with a shaper would double-shape. Every other detail matches
/// [`run_duallane_links`] so an arm's interactive latency is comparable to the
/// production dual-lane table.
async fn run_duallane_links_shaped(
    label: &str,
    interactive_reorder: bool,
    links: DualLaneLinks,
    has_bulk: bool,
    shared_c2s: Option<BottleneckShaper>,
) -> DualRun {
    let DualLaneLinks {
        int_c2s,
        int_s2c,
        bulk_c2s,
        bulk_s2c,
    } = links;
    let prompt = prompt_tuning();
    let int_rtp = if interactive_reorder {
        LaneRtpConfig::frame_reordering(true, prompt)
    } else {
        LaneRtpConfig::frame_strict_tuned(true, prompt)
    };
    // The bulk co-tenant is the deployment's own lane, not the stock
    // byte-stream default: production maps `LaneClass::Bulk` to a
    // `Dedicated` congestion intent, and the interactive mandate is measured
    // on the production topology.
    let bulk_rtp = LaneRtpConfig::production_bulk();

    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = match shared_c2s.clone() {
                Some(shaper) => {
                    NetemPair::spawn_shared(int_addr, int_c2s, int_s2c, Some(shaper), None).unwrap()
                }
                None => NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap(),
            };
            let bulk_pair = match shared_c2s {
                Some(shaper) => {
                    NetemPair::spawn_shared(bulk_addr, bulk_c2s, bulk_s2c, Some(shaper), None)
                        .unwrap()
                }
                None => NetemPair::spawn(bulk_addr, bulk_c2s, bulk_s2c).unwrap(),
            };

            let (observer, fec_cell, snapshot_cell, timeline_cell, rtt_samples) = fec_observer();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                Some(observer),
                None,
            )
            .await
            .unwrap();

            // Interactive latency stream (`b'L'`) on the interactive lane.
            let (mut lat_read, mut lat_write) =
                opener.open(mux::LaneClass::Interactive).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = lat_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            // Bulk stream (`b'B'`) on the separate bulk lane, only when offered.
            let bulk_write = if has_bulk {
                let (mut bulk_read, bulk_write) = opener.open(mux::LaneClass::Bulk).await.unwrap();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = bulk_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                Some(bulk_write)
            } else {
                None
            };

            let interactive = async {
                if lat_write.write_all(b"L").await.is_err() {
                    return 0;
                }
                send_timestamped_messages(&mut lat_write, base, MSG_BYTES, CADENCE, RUN_FOR).await
            };
            let bulk_fut = async {
                let Some(mut write) = bulk_write else {
                    return 0;
                };
                if write.write_all(b"B").await.is_err() {
                    return 0;
                }
                let payload = cyclic_payload(BULK_BURST_BYTES);
                periodic_burst(
                    &mut write,
                    &payload,
                    BULK_BURST_BYTES,
                    BULK_PERIOD,
                    BULK_RAMP,
                    RUN_FOR,
                )
                .await
            };
            let (sent, _bulk_written) = tokio::join!(interactive, bulk_fut);

            tokio::time::sleep(GRACE).await;
            let int_counters = int_pair.stats();
            let int_c2s_wire_bytes = int_pair.stats_c2s().forwarded_bytes;
            let bulk_counters = bulk_pair.stats();
            let bulk_wire_bytes = bulk_pair.stats_c2s().forwarded_bytes;
            let mut samples = Vec::new();
            while let Ok((tag, lat)) = latencies.try_recv() {
                if tag == b'L' {
                    samples.push(lat);
                }
            }
            let echo_latencies = samples.clone();
            let repair = replay_rto(&rtt_samples.lock().unwrap());
            let received = samples.len() as u64;
            let bulk_active_secs = (RUN_FOR - BULK_RAMP).as_secs_f64();
            let summary = summarize(samples, sent, received, bulk_wire_bytes, bulk_active_secs);
            let fec = *fec_cell.lock().unwrap();
            let snapshot = *snapshot_cell.lock().unwrap();
            let rtx = snapshot.map(|s| s.retransmission_counters);
            let timeline = std::mem::take(&mut *timeline_cell.lock().unwrap());
            let bulk_sink_bytes = bulk_counter.load(Ordering::Relaxed);

            print_summary(label, &summary);
            eprintln!(
                "[duallane {label}] interactive={} bulk wire c2s forwarded = {bulk_wire_bytes} \
                 bytes / {} pkts; sink delivered = {bulk_sink_bytes} bytes",
                if interactive_reorder {
                    "fast-forward"
                } else {
                    "strict"
                },
                bulk_counters.forwarded,
            );
            eprintln!("[duallane {label}] interactive pair = {int_counters:?}");
            eprintln!("[duallane {label}] bulk pair = {bulk_counters:?}");
            if let Some(fec) = fec {
                eprintln!(
                    "[duallane {label}] fec parity_sent={} groups_flushed={} \
                     loss_gate_skips={} no_spare_capacity_skips={} burst_end_skips={} \
                     recovered={}",
                    fec.parity_sent,
                    fec.groups_flushed,
                    fec.groups_skipped_loss_gate,
                    fec.groups_skipped_no_spare_capacity,
                    fec.groups_skipped_burst_end,
                    fec.recovered_symbols,
                );
            }

            int_pair.stop();
            bulk_pair.stop();
            DualRun {
                run: JitterRun {
                    summary,
                    counters: int_counters,
                    fec,
                    rtx,
                    timeline,
                },
                int_c2s_wire_bytes,
                bulk_wire_bytes,
                bulk_sink_bytes,
                bulk_counters,
                echo_latencies,
                repair,
            }
        })
        .await
}

/// Print the dual-lane headline table: the interactive-lane latency
/// percentiles plus the bulk lane's wire goodput (the matched-load check) and
/// the interactive lane's RTP FEC counters.
fn print_duallane_table(runs: &[(&str, &DualRun)]) {
    eprintln!(
        "[duallane] interactive lane at 2% per-packet loss, prompt FEC tuning; bulk lane is a \
         separate strict byte-stream FEC-free RTP connection"
    );
    eprintln!(
        "[duallane] arm                       p50     p90     p99     max  over250  ep  run  \
         bulk_wire_MiB/s  sink_MiB  parity  recovered"
    );
    for (name, r) in runs {
        let s = &r.run.summary;
        let f = r.run.fec.unwrap_or_default();
        let sink_mib = r.bulk_sink_bytes as f64 / (1024.0 * 1024.0);
        eprintln!(
            "[duallane] {name:<27} {p50:7.1} {p90:7.1} {p99:7.1} {max:7.1} {o25:7.3} {ep:3} {mr:3} \
             {bulk:>15.3} {sink:>9.3} {parity:>7} {recovered:>9}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            max = s.max,
            o25 = s.over250_pct,
            ep = s.episodes,
            mr = s.max_run,
            bulk = s.bulk_mibps,
            sink = sink_mib,
            parity = f.parity_sent,
            recovered = f.recovered_symbols,
        );
    }
    let find = |name: &str| runs.iter().find(|(n, _)| *n == name).map(|(_, r)| *r);
    let (Some(reorder), Some(strict)) = (find("both_reorder"), find("both_strict")) else {
        return;
    };
    let (a, b) = (&reorder.run.summary, &strict.run.summary);
    eprintln!(
        "[duallane] both_reorder - both_strict: p50={:+.1} p90={:+.1} p99={:+.1} max={:+.1} \
         over250={:+.3} episodes={:+} bulk_wire_MiB/s={:+.3}",
        a.p50 - b.p50,
        a.p90 - b.p90,
        a.p99 - b.p99,
        a.max - b.max,
        a.over250_pct - b.over250_pct,
        a.episodes as i64 - b.episodes as i64,
        a.bulk_mibps - b.bulk_mibps,
    );
}

/// Print the bulk lane's wire load and sink goodput for each arm of one
/// interactive frame mode — the matched-load evidence for the headline
/// comparison.
fn print_duallane_loads(mode: &str, runs: &[(&str, &DualRun)]) {
    eprintln!("[duallane {mode}] bulk-lane load (matched-load evidence):");
    for (name, r) in runs {
        eprintln!(
            "[duallane {mode}] {name:<6} wire_c2s={:>10} bytes ({:.3} MiB/s over {:?}) \
             pkts={:>7} sink={:>10} bytes",
            r.bulk_wire_bytes,
            r.run.summary.bulk_mibps,
            RUN_FOR - BULK_RAMP,
            r.bulk_counters.forwarded,
            r.bulk_sink_bytes,
        );
    }
}

/// Dual-lane interactive-latency decomposition. The deployment's topology is
/// reproduced at MATCHED bulk load: the interactive lane runs frame mode + FEC
/// on its own RTP connection (receiver-side fast-forward in the `reorder` arm,
/// strict in the `strict` arm) and the bulk lane is a SECOND, independent,
/// strict, FEC-free byte-stream RTP connection. Because the lanes are separate
/// connections the bulk burst cannot head-of-line block the interactive lane,
/// and because the two arms share the identical bulk lane the fast-forward win
/// — if any — is read at the same offered bulk load.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; eight ~35 s dual-lane arms (fast-forward + strict); run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_duallane_arms() {
    let impairments = [
        ("solo", DualImpairment::Solo),
        ("loss", DualImpairment::Loss),
        ("bulk", DualImpairment::Bulk),
        ("both", DualImpairment::Both),
    ];

    let mut reorder: Vec<(&str, DualRun)> = Vec::new();
    for (name, impairment) in impairments {
        let label = format!("duallane_reorder_fec/{name}");
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane(&label, true, impairment),
        )
        .await;
        assert_sane(&label, &run.run.summary);
        reorder.push((name, run));
    }
    let mut strict: Vec<(&str, DualRun)> = Vec::new();
    for (name, impairment) in impairments {
        let label = format!("duallane_strict_fec/{name}");
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane(&label, false, impairment),
        )
        .await;
        assert_sane(&label, &run.run.summary);
        strict.push((name, run));
    }

    // The requested arms, named for the headline comparison.
    eprintln!("[duallane] bulk_and_loss_duallane_reorder_fec = duallane_reorder_fec/both");
    eprintln!("[duallane] bulk_and_loss_duallane_strict_fec = duallane_strict_fec/both");

    let reorder_view: Vec<(&str, &DualRun)> = reorder.iter().map(|(n, r)| (*n, r)).collect();
    let strict_view: Vec<(&str, &DualRun)> = strict.iter().map(|(n, r)| (*n, r)).collect();
    let headline: Vec<(&str, &DualRun)> = vec![
        ("both_reorder", &reorder[3].1),
        ("both_strict", &strict[3].1),
    ];
    print_duallane_table(&headline);

    print_decomposition(
        "duallane-reorder-fec",
        &reorder[0].1.run,
        &reorder[1].1.run,
        &reorder[2].1.run,
        &reorder[3].1.run,
    );
    print_duallane_loads("duallane-reorder-fec", &reorder_view);
    print_decomposition(
        "duallane-strict-fec",
        &strict[0].1.run,
        &strict[1].1.run,
        &strict[2].1.run,
        &strict[3].1.run,
    );
    print_duallane_loads("duallane-strict-fec", &strict_view);
}

/// for the interactive lane's client→server wire
/// versus the offered interactive payload (see
/// [`jitter_duallane_constitution_gate`]). Measured overhead on the seeded
/// `both` arm is ~3.6× the offered payload (RTP/mux framing + control + the
/// repair traffic the 2 % loss needs); 6× leaves ~1.6× headroom, so the
/// lane's aggregate wire must not grow by more than ~+64 %. The budget
/// bounds that aggregate over the run, not one message's redundancy: a
/// fully-armored lone interactive tail is `primary + 5 copies` = six
/// datagrams carrying the same 256 B payload, so it alone is at least the
/// whole budget before framing.
const INTERACTIVE_WIRE_BUDGET_X: u64 = 6;

/// The interactive-lane constitution gate (mandate 2): the deployment
/// topology's outcome criteria — the interactive lane keeps `delivery == 1.000`
/// and its client→server wire stays within a fixed budget of the offered
/// payload — asserted on the production `both` dual-lane arm (frame mode +
/// fast-forward + prompt FEC interactive lane at 2 % loss, separate strict
/// bulk lane at the matched 2 MiB / 3 s load). The offered payload is the
/// deterministic sent-message byte count, so both quantities are counts over
/// the seeded, deterministic impairment link: the gate is deterministic — a
/// redundancy ladder that eats the interactive lane's own goodput (delivery
/// falling below `1.000`) or a wire that inflates without bound fails here
/// instead of being a human-read table row. Delivery is additionally the
/// README constitution's first criterion; the wire budget is the second
/// (redundancy may use *some* wire, never unboundedly). Latency is
/// deliberately not asserted here — the p99 floor is the median-of-3
/// constitution gate (mandate 1) that follows this one; the bulk lane's
/// goodput fraction is mandate 3, asserted in `dual_lane_mandates.rs`.
///
/// Counts belong in the default gate (the constitution's tier rule), so this
/// gate is **not** `#[ignore]`d: it runs on every `cargo test -p rtp_mux`.
#[tokio::test(flavor = "multi_thread")]
async fn jitter_duallane_constitution_gate() {
    let label = "duallane_constitution/both";
    let run = with_timeout(
        Duration::from_secs(120),
        label,
        run_duallane(label, true, DualImpairment::Both),
    )
    .await;
    let summary = &run.run.summary;
    assert_sane(label, summary);
    // The offered interactive payload: `sent` messages of `MSG_BYTES` bytes.
    let offered = summary.sent * MSG_BYTES as u64;
    let wire = run.int_c2s_wire_bytes;
    assert_eq!(
        summary.received, summary.sent,
        "[{label}] interactive delivery must be exactly 1.000: {}/{} messages delivered ({:.3}), the interactive lane ate its own goodput",
        summary.received, summary.sent, summary.delivery_pct,
    );
    assert!(
        wire <= offered * INTERACTIVE_WIRE_BUDGET_X,
        "[{label}] interactive c2s wire {wire} bytes exceeds the {INTERACTIVE_WIRE_BUDGET_X}x offered-payload budget ({} bytes): redundant wire must not inflate unboundedly (measured {:.2}x)",
        offered * INTERACTIVE_WIRE_BUDGET_X,
        wire as f64 / offered as f64,
    );
    eprintln!(
        "[{label}] constitution OK: delivery {:.3}, c2s wire {wire} bytes = {:.2}x offered {offered} bytes, p50 {:.1} p99 {:.1} max {:.1} ms",
        summary.delivery_pct,
        wire as f64 / offered as f64,
        summary.p50,
        summary.p99,
        summary.max,
    );
}

/// The interactive p99 ceiling above the one-way floor: a repair that needs
/// more than this many milliseconds of p99 latency violates the README's
/// "zero >250 ms spikes" criterion. Measured p99 on the seeded `both` arm is
/// ~29 ms (25 ms one-way + jitter + repairs), so the 250 ms ceiling leaves a
/// ~8× margin while still biting on any regression that lets the interactive
/// tail decay (the GE-burst diagnostics that keep delivery at 1.000 push p99
/// well past this ceiling). The ceiling is the mandate-1 bound: the
/// topology's one-way delay floor (25 ms) plus a documented margin.
const INTERACTIVE_P99_CEILING_MS: f64 = 250.0;

/// The median-of-3 interactive-latency constitution gate (mandate 1): the
/// production `both` dual-lane arm (frame + fast-forward + prompt-FEC
/// interactive lane at 2 % loss, separate strict bulk lane) run three times,
/// asserting the interactive outcome triad on every run — `delivery == 1.000`
/// and the client→server wire within the [`INTERACTIVE_WIRE_BUDGET_X`] budget
/// (both deterministic counts, min over the three reps) — and the median-of-3
/// p99 against the [`INTERACTIVE_P99_CEILING_MS`] ceiling, with a zero
/// `>250 ms` spike count on every run. The p99 is a single wall-clock
/// observation and its timing against the seeded loss stream varies with host
/// scheduling, so it is gated on the median of the three runs, exactly as the
/// contested-latency and paced-bulk HOL gates do.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; three ~35 s dual-lane constitution runs; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_duallane_constitution_gate_p99() {
    const REPS: usize = 3;
    let mut p99s = [0.0f64; REPS];
    for (rep, slot) in p99s.iter_mut().enumerate() {
        let label = format!("duallane_constitution_p99/both/rep{}", rep + 1);
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane(&label, true, DualImpairment::Both),
        )
        .await;
        let summary = &run.run.summary;
        assert_sane(&label, summary);
        let offered = summary.sent * MSG_BYTES as u64;
        let wire = run.int_c2s_wire_bytes;
        assert_eq!(
            summary.received, summary.sent,
            "[{label}] interactive delivery must be exactly 1.000: {}/{} messages delivered ({:.3}), the interactive lane ate its own goodput",
            summary.received, summary.sent, summary.delivery_pct,
        );
        assert!(
            wire <= offered * INTERACTIVE_WIRE_BUDGET_X,
            "[{label}] interactive c2s wire {wire} bytes exceeds the {INTERACTIVE_WIRE_BUDGET_X}x offered-payload budget ({} bytes): redundant wire must not inflate unboundedly (measured {:.2}x)",
            offered * INTERACTIVE_WIRE_BUDGET_X,
            wire as f64 / offered as f64,
        );
        assert_eq!(
            summary.over250_pct,
            0.0,
            "[{label}] interactive lane must have zero >250 ms spikes (mandate 1 spike bound), got {:.3}% of samples over the ceiling (p99 {:.1} ms, max {:.1} ms)",
            summary.over250_pct * 100.0,
            summary.p99,
            summary.max,
        );
        *slot = summary.p99;
        eprintln!(
            "[{label}] rep {}: delivery {:.3}, c2s wire {wire} bytes = {:.2}x offered {offered} bytes, p50 {:.1} p99 {:.1} ms",
            rep + 1,
            summary.delivery_pct,
            wire as f64 / offered as f64,
            summary.p50,
            summary.p99,
        );
    }
    let mut sorted = p99s;
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[REPS / 2];
    assert!(
        median <= INTERACTIVE_P99_CEILING_MS,
        "[duallane_constitution_p99/both] median p99 {median:.1} ms > {INTERACTIVE_P99_CEILING_MS} ms ceiling (per-run p99: {p99s:?}): the interactive tail must stay at the one-way floor"
    );
    eprintln!(
        "[duallane_constitution_p99/both] median-of-3 p99 {median:.1} ms <= {INTERACTIVE_P99_CEILING_MS} ms ceiling OK"
    );
}

/// Print one burst-loss arm's interactive-latency percentiles (p99.9 beside
/// p99 so a rare repair spike is not hidden by the floor), delivery, the
/// interactive lane's *own* client->server wire against the payload it
/// offered (mandate 2's budget quantity — not the pair's both-direction
/// total), the RTP repair counters, and then, per arm, the replayed repair
/// deadlines and the slow-echo tail that the `max` was drawn from.
fn print_burst_table(runs: &[(&str, &DualRun)]) {
    eprintln!(
        "[burst] deployment interactive lane (fast-forward + prompt FEC, own RTP connection); \
         interactive loss model as named; bulk lane = production 2 MiB / 3 s at 2% loss"
    );
    eprintln!(
        "[burst] arm                     p50     p90     p99    p999     max  over250  del  recv  \
         offered    wire_c2s  x_off   bulk   rtx_first  rtx_rto  rtx_reord  rtx_fast  rtx_tail  \
         rtx_repeat  parity  recovered"
    );
    for (name, r) in runs {
        let s = &r.run.summary;
        let rtx = r.run.rtx.unwrap_or_default();
        let fec = r.run.fec.unwrap_or_default();
        // The offered interactive payload: `sent` messages of [`MSG_BYTES`]
        // bytes. The wire is the lane's own client->server forwarded bytes.
        let offered = s.sent * MSG_BYTES as u64;
        let wire = r.int_c2s_wire_bytes;
        eprintln!(
            "[burst] {name:<22} {p50:7.1} {p90:7.1} {p99:7.1} {p999:7.1} {max:7.1} {o25:7.3} \
             {del:5.3} {recv:5} {offered:9} {wire:>10} {xoff:6.2} {bulk:5.2} {first:>11} {rto:8} \
             {reord:10} {fast:9} {tail:9} {repeat:11} {parity:7} {recovered:9}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            p999 = s.p999,
            max = s.max,
            o25 = s.over250_pct,
            del = s.delivery_pct,
            recv = s.received,
            offered = offered,
            wire = wire,
            xoff = wire as f64 / offered as f64,
            bulk = s.bulk_mibps,
            first = rtx.first_attempts,
            rto = rtx.rto_reason,
            reord = rtx.reorder_reason,
            fast = rtx.fast_loss_reason,
            tail = rtx.tail_probes,
            repeat = rtx.repeat_attempts,
            parity = fec.parity_sent,
            recovered = fec.recovered_symbols,
        );
    }
    for (name, r) in runs {
        let deadline = &r.repair;
        // All-zero when the observer captured no RTT sample at all.
        let last = deadline.last.unwrap_or(RepairDeadlines {
            srtt: Duration::ZERO,
            rttvar: Duration::ZERO,
            rto: Duration::ZERO,
            stock_reorder_window: Duration::ZERO,
            fast_reorder_window: Duration::ZERO,
            fast_loss_armed: false,
        });
        eprintln!(
            "[burst-deadline] {name:<22} rtt_samples={:5} last_srtt={:7.1}ms last_rttvar={:7.1}ms \
             last_rto={:7.1}ms max_srtt={:7.1}ms max_rttvar={:7.1}ms max_stock_reorder_window={:7.1}ms \
             max_fast_reorder_window={:7.1}ms fast_loss_armed_any={}",
            deadline.samples,
            last.srtt.as_secs_f64() * 1000.0,
            last.rttvar.as_secs_f64() * 1000.0,
            last.rto.as_secs_f64() * 1000.0,
            deadline.srtt_max.as_secs_f64() * 1000.0,
            deadline.rttvar_max.as_secs_f64() * 1000.0,
            deadline.stock_reorder_window_max.as_secs_f64() * 1000.0,
            deadline.fast_reorder_window_max.as_secs_f64() * 1000.0,
            deadline.fast_loss_armed_any,
        );
        // The latency bands that separate the repair paths: an echo at the
        // one-way floor is a same-round-trip recovery, one at the 300 ms
        // tail-loss-probe floor is a TLP, one at the reorder window plus a
        // one-way delay is the ARQ fall-through, and one at the `MIN_RTO`
        // floor (1 s) is the RTO path.
        let band = |lo: f64| r.echo_latencies.iter().filter(|x| **x > lo).count();
        let mut slowest = r.echo_latencies.iter().enumerate().collect::<Vec<_>>();
        slowest.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
        let head: Vec<(usize, String)> = slowest
            .iter()
            .take(10)
            .map(|(i, x)| (*i, format!("{x:.1}")))
            .collect();
        eprintln!(
            "[burst-tail] {name:<22} n={:5} >100ms={:5} >250ms={:5} >500ms={:5} >750ms={:5} \
             >950ms={:5} >1400ms={:5}",
            r.echo_latencies.len(),
            band(100.0),
            band(250.0),
            band(500.0),
            band(750.0),
            band(950.0),
            band(1400.0),
        );
        eprintln!("[burst-tail] {name:<22} slowest (echo ordinal, ms): {head:?}");
    }
    for (name, r) in runs {
        print_congestion_timeline(name, &r.run.timeline);
    }
}

/// Burst-loss / high-loss sweep on the deployment's interactive lane: the
/// production dual-lane arms only exercise independent 2% loss, so this arm
/// adds the two field dimensions the harness was missing — a higher iid rate
/// (6%) and a bursty Gilbert-Elliot model (5% long-term, mean burst 8) — each
/// on the interactive lane with its own RTP connection and the production
/// fast-forward + prompt FEC tuning, once without bulk and once at the
/// production bulk load. p99.9 is printed beside p99 so a rare repair spike
/// stays visible above the floor.
///
/// It also sweeps the *jitter* dimension the clean arms pin at 5 ms, because
/// the repair deadlines are variance-quantized: the reorder window is
/// `srtt + max(4*rttvar, srtt/4)` and the fast-loss gate arms only while
/// `4*rttvar < srtt/4`. The four `jitter_*` arms move only the jitter, so the
/// measured tail is attributable to the deadline that produced it rather than
/// to a path change.
///
/// Report-only: the table is the deliverable. Each row is followed by the
/// replayed estimator deadlines (the window an ARQ fall-through waits on) and
/// the slow-echo tail with its delivery ordinals, so a maximum can be
/// attributed to the `MIN_RTO` floor, the reorder window, the 300 ms
/// tail-loss-probe floor, or a same-round-trip parity/armor recovery instead
/// of being read as one opaque number.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; twelve ~35 s burst-loss dual-lane arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_burst_loss_arms() {
    let int_link = |seed: u64, loss_model: LossModel| NetemConfig {
        latency: OWD,
        jitter: JITTER,
        loss_model,
        seed,
        ..NetemConfig::default()
    };
    let iid_2 = |seed: u64| NetemConfig {
        loss: LOSS_2,
        ..int_link(seed, LossModel::Random)
    };
    let bulk_c2s = link(43, LOSS_2, BULK_RATE_BPS);
    let bulk_s2c = link(44, LOSS_2, BULK_RATE_BPS);
    // iid 6% needs the explicit `loss` threshold on `Random`.
    let iid_6 = |seed: u64| NetemConfig {
        loss: LOSS_6,
        ..int_link(seed, LossModel::Random)
    };
    let burst = |seed: u64| int_link(seed, gilbert_elliott_loss(5.0, 8.0));
    // The field path is not the arms' clean 5 ms-jitter link: a real client
    // behind the deployment sees a ~190 ms RTT floor with excursions of
    // several hundred ms. The repair deadlines are variance-quantized
    // (`4*rttvar` vs `srtt/4` decides the fast-loss arming bit, and the
    // reorder window is `srtt + max(K*rttvar, srtt/4)`), so the high-jitter
    // arms below sweep the same one-way delay as the clean arms while moving
    // only the jitter, which is the knob the deadlines respond to.
    let high_jitter = |seed: u64, jitter_ms: u64, loss: u32| NetemConfig {
        latency: OWD,
        jitter: Duration::from_millis(jitter_ms),
        loss,
        seed,
        ..NetemConfig::default()
    };

    let arms: Vec<(&str, NetemConfig, NetemConfig, bool)> = vec![
        ("iid_2pct", iid_2(41), iid_2(42), false),
        ("iid_6pct", iid_6(41), iid_6(42), false),
        ("iid_6pct_bulk", iid_6(41), iid_6(42), true),
        ("burst_5pct_mean8", burst(41), burst(42), false),
        ("burst_5pct_mean8_bulk", burst(41), burst(42), true),
        (
            "burst_10pct_mean4",
            int_link(41, gilbert_elliott_loss(10.0, 4.0)),
            int_link(42, gilbert_elliott_loss(10.0, 4.0)),
            false,
        ),
        (
            "jitter_50ms_2pct",
            high_jitter(41, 50, LOSS_2),
            high_jitter(42, 50, LOSS_2),
            false,
        ),
        (
            "jitter_50ms_2pct_bulk",
            high_jitter(41, 50, LOSS_2),
            high_jitter(42, 50, LOSS_2),
            true,
        ),
        (
            "jitter_100ms_2pct",
            high_jitter(41, 100, LOSS_2),
            high_jitter(42, 100, LOSS_2),
            false,
        ),
        (
            "jitter_100ms_6pct",
            high_jitter(41, 100, LOSS_6),
            high_jitter(42, 100, LOSS_6),
            false,
        ),
        (
            "jitter_200ms_2pct",
            high_jitter(41, 200, LOSS_2),
            high_jitter(42, 200, LOSS_2),
            false,
        ),
        (
            "jitter_200ms_6pct",
            high_jitter(41, 200, LOSS_6),
            high_jitter(42, 200, LOSS_6),
            false,
        ),
    ];

    let mut runs: Vec<(&str, DualRun)> = Vec::new();
    for (name, int_c2s, int_s2c, with_bulk) in arms {
        let label = format!("burst/{name}");
        eprintln!(
            "[newdim] arm={name} primary_jitter={}ms primary_loss={}% (seed {}) bulk={with_bulk}",
            int_c2s.jitter.as_secs_f64() * 1000.0,
            int_c2s.loss as f64 / (u32::MAX as f64 / 100.0),
            int_c2s.seed,
        );
        let (bulk_c2s, bulk_s2c) = if with_bulk {
            (bulk_c2s.clone(), bulk_s2c.clone())
        } else {
            (NetemConfig::default(), NetemConfig::default())
        };
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane_links(
                &label, true, int_c2s, int_s2c, bulk_c2s, bulk_s2c, with_bulk,
            ),
        )
        .await;
        assert_sane(&label, &run.run.summary);
        runs.push((name, run));
    }
    let views: Vec<(&str, &DualRun)> = runs.iter().map(|(n, r)| (*n, r)).collect();
    print_burst_table(&views);
}

// ══════════════════════════════════════════════════════════════════════
// Unexercised dimensions: low-capacity shared link, bufferbloat / droptail,
// ACK-path (reverse-path) impairment, and cellular-like jitter
// ══════════════════════════════════════════════════════════════════════

/// One reverse-path (ACK-path) impairment config: the interactive-lane
/// server->client direction only. The client->server direction is left clean,
/// so any interactive-latency change is attributable to the ACK path that
/// carries the SACK evidence.
fn ack_path(
    seed: u64,
    extra_delay: Duration,
    loss_points: u32,
    reorder: u32,
    reorder_gap_pkts: u32,
) -> NetemConfig {
    NetemConfig {
        latency: OWD + extra_delay,
        jitter: JITTER,
        loss: if loss_points == 0 {
            0
        } else {
            loss_pct(loss_points)
        },
        reorder,
        reorder_gap_pkts,
        seed,
        ..NetemConfig::default()
    }
}

/// Print one unexercised-dimension arm's interactive-latency percentiles
/// (p99.9 beside p99 so a rare spike stays visible), delivery, offered
/// interactive wire, the RTP repair counters, and the bulk lane's wire
/// goodput, so the bulk-throughput contract is checkable in the same table.
fn print_newdim_table(runs: &[(&str, &DualRun)]) {
    eprintln!(
        "[newdim] deployment interactive lane (fast-forward + prompt FEC, own RTP connection); \
         dimensions: shared low-capacity link, bufferbloat/droptail, ACK-path impairment, \
         cellular jitter"
    );
    eprintln!(
        "[newdim] arm                     p50     p90     p99    p999     max  over250  del  recv  \
         wire_bytes  rtx_first  rtx_rto  rtx_reord  rtx_fast  rtx_tail  parity  recovered  bulk_MiB/s"
    );
    for (name, r) in runs {
        let s = &r.run.summary;
        let rtx = r.run.rtx.unwrap_or_default();
        let fec = r.run.fec.unwrap_or_default();
        eprintln!(
            "[newdim] {name:<22} {p50:7.1} {p90:7.1} {p99:7.1} {p999:7.1} {max:7.1} {o25:7.3} \
             {del:5.3} {recv:5} {wbytes:>11} {first:>11} {rto:8} {reord:10} {fast:9} {tail:9} \
             {parity:7} {recovered:9} {bulk:>10.3}",
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            p999 = s.p999,
            max = s.max,
            o25 = s.over250_pct,
            del = s.delivery_pct,
            recv = s.received,
            wbytes = r.run.counters.forwarded_bytes,
            first = rtx.first_attempts,
            rto = rtx.rto_reason,
            reord = rtx.reorder_reason,
            fast = rtx.fast_loss_reason,
            tail = rtx.tail_probes,
            parity = fec.parity_sent,
            recovered = fec.recovered_symbols,
            bulk = s.bulk_mibps,
        );
    }
}

/// Sweep the interactive-latency dimensions the existing arms do not
/// exercise: (a) a low-capacity client->server link shared by the interactive
/// and bulk lanes, at several shared queue depths (bufferbloat onset/offset
/// and a small droptail buffer); (b) reverse-path (ACK-path) delay, thinning,
/// and reordering, which is where the SACK evidence rides; (c) cellular-like
/// high, time-varying jitter. Report-only and seeded: the table is the
/// deliverable, the assertions are liveness guards.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; eleven ~35 s dimension arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_latency_dimension_arms() {
    /// 400 KiB/s = 3.2768 Mbps shared client->server bottleneck.
    const SHARED_400KIB_BPS: u64 = 400 * 1024 * 8;
    /// 200 KiB/s shared client->server bottleneck.
    const SHARED_200KIB_BPS: u64 = 200 * 1024 * 8;

    let clean = |seed: u64| link_custom(seed, OWD, JITTER, 0, 0, 0, 0, 0);
    // The bulk lane's per-flow config carries no rate when a shared shaper
    // supplies the bottleneck; only the shaper rate shapes it.
    let bulk_c2s = link(43, LOSS_2, 0);
    let bulk_s2c = link(44, LOSS_2, 0);

    let mut runs: Vec<(&str, DualRun)> = Vec::new();

    // (b) Reverse-path impairment on the interactive lane; no bulk, so any
    // latency change is attributable to the ACK path alone.
    let ack_arms: Vec<(&str, NetemConfig)> = vec![
        ("ack_baseline", clean(42)),
        (
            "ack_delay_100ms",
            ack_path(42, Duration::from_millis(100), 0, 0, 0),
        ),
        ("ack_thin_10pct", ack_path(42, Duration::ZERO, 10, 0, 0)),
        (
            "ack_reorder_10pct",
            ack_path(42, Duration::ZERO, 0, IMPAIR_10, 2),
        ),
        (
            "ack_thin10_delay100",
            ack_path(42, Duration::from_millis(100), 10, 0, 0),
        ),
    ];
    for (name, s2c) in ack_arms {
        let label = format!("newdim/{name}");
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane_links(
                &label,
                true,
                clean(41),
                s2c,
                NetemConfig::default(),
                NetemConfig::default(),
                false,
            ),
        )
        .await;
        assert_reportable(&label, &run.run.summary);
        runs.push((name, run));
    }

    // (a) Shared low-capacity link: both lanes contend for one serialization
    // clock. The shared tail-drop buffer is the router queue, so its depth is
    // the bufferbloat / droptail dimension. The interactive lane's offered
    // rate (~10 KiB/s) is far below 400 KiB/s, so the bulk lane is the queue
    // builder and the interactive packets queue behind it.
    let shared_arms: Vec<(&str, u64, u64)> = vec![
        ("shared400k_128kib", SHARED_400KIB_BPS, 128 * 1024),
        ("shared400k_32kib", SHARED_400KIB_BPS, 32 * 1024),
        ("shared200k_64kib", SHARED_200KIB_BPS, 64 * 1024),
        ("shared400k_4kib", SHARED_400KIB_BPS, 4 * 1024),
    ];
    for (name, rate, limit_bytes) in shared_arms {
        let label = format!("newdim/{name}");
        let shaper = BottleneckShaper::new(rate, limit_bytes);
        let run = with_timeout(
            Duration::from_secs(180),
            &label,
            run_duallane_links_shaped(
                &label,
                true,
                DualLaneLinks {
                    int_c2s: clean(41),
                    int_s2c: clean(42),
                    bulk_c2s: bulk_c2s.clone(),
                    bulk_s2c: bulk_s2c.clone(),
                },
                true,
                Some(shaper),
            ),
        )
        .await;
        assert_reportable(&label, &run.run.summary);
        runs.push((name, run));
    }

    // (c) Cellular-like jitter: a high uniform jitter with no loss, which the
    // low-jitter non-loss arms do not reach. Note the report artifact: the
    // server timestamps a message when it *reads* it, so under jitter-driven
    // reordering fast-forward delivers buffered later messages together with
    // the delayed head, and their `now - sent` can collapse toward zero. The
    // p50/p90 therefore understate the true one-way latency here; p99.9/max
    // (the delayed head's own latency) are the meaningful columns.
    let cell_arms: Vec<(&str, u64)> = vec![("cell_jitter_100ms", 100), ("cell_jitter_200ms", 200)];
    for (name, jitter_ms) in cell_arms {
        let label = format!("newdim/{name}");
        let cfg = |seed: u64| NetemConfig {
            latency: OWD,
            jitter: Duration::from_millis(jitter_ms),
            seed,
            ..NetemConfig::default()
        };
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane_links(
                &label,
                true,
                cfg(41),
                cfg(42),
                NetemConfig::default(),
                NetemConfig::default(),
                false,
            ),
        )
        .await;
        assert_reportable(&label, &run.run.summary);
        runs.push((name, run));
    }

    let views: Vec<(&str, &DualRun)> = runs.iter().map(|(n, r)| (*n, r)).collect();
    print_newdim_table(&views);
    if let Some((worst, run)) = runs
        .iter()
        .max_by(|a, b| a.1.run.summary.p999.total_cmp(&b.1.run.summary.p999))
    {
        eprintln!(
            "[newdim] worst p99.9 arm = {worst} ({:.1} ms)",
            run.run.summary.p999
        );
        print_congestion_timeline(worst, &run.run.timeline);
    }
    if let Some((worst, run)) = runs
        .iter()
        .max_by(|a, b| a.1.run.summary.max.total_cmp(&b.1.run.summary.max))
    {
        eprintln!(
            "[newdim] worst max arm = {worst} ({:.1} ms)",
            run.run.summary.max
        );
    }
    // Report-only: the cellular-jitter arms are where the interactive lane is
    // most likely to show a controller reaction hidden by the jitter floor, so
    // print their controller timelines even when they are not the worst arm.
    for (name, run) in &runs {
        if name.starts_with("cell_jitter") {
            print_congestion_timeline(name, &run.run.timeline);
        }
    }
}

/// Report-only: the two cellular-like jitter arms run in isolation with their
/// controller timelines printed, so a controller over-reaction hidden beneath
/// the jitter envelope is visible instead of only the worst p99.9 arm's.  The
/// link is the same 25 ms one-way delay with 100/200 ms uniform jitter used by
/// [`jitter_latency_dimension_arms`]; seeded and report-only besides liveness.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; two ~35 s cellular-jitter arms; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_cellular_timeline_arms() {
    let cfg = |seed: u64, jitter_ms: u64| NetemConfig {
        latency: OWD,
        jitter: Duration::from_millis(jitter_ms),
        seed,
        ..NetemConfig::default()
    };
    for (name, jitter_ms) in [("cell_jitter_100ms", 100u64), ("cell_jitter_200ms", 200u64)] {
        let label = format!("cell/{name}");
        let run = with_timeout(
            Duration::from_secs(120),
            &label,
            run_duallane_links(
                &label,
                true,
                cfg(41, jitter_ms),
                cfg(42, jitter_ms),
                NetemConfig::default(),
                NetemConfig::default(),
                false,
            ),
        )
        .await;
        assert_reportable(&label, &run.run.summary);
        eprintln!(
            "[cell] {name}: p50={:.1} p90={:.1} p99={:.1} p999={:.1} max={:.1} ms",
            run.run.summary.p50,
            run.run.summary.p90,
            run.run.summary.p99,
            run.run.summary.p999,
            run.run.summary.max,
        );
        print_congestion_timeline(name, &run.run.timeline);
    }
}

/// A looser sanity guard than [`assert_sane`] for the report-only dimension
/// arms: the interactive stream must actually have run, but a deep shared
/// queue may leave samples in flight, so the received floor is low.
fn assert_reportable(label: &str, s: &HolSummary) {
    assert!(s.sent >= 100, "[{label}] only {} sent", s.sent);
    assert!(s.received >= 5, "[{label}] only {} received", s.received);
}

/// Report-only: one RTP bulk lane that bursts and then goes fully idle, so the
/// delay controller's persistent-queue timer either survives the idle gap (a
/// stale drain/hold state that punishes the first burst after idle) or
/// restarts fresh.  Each ~2 s burst is offered through a rate-shaped link and
/// followed by a 4 s silence -- several times the one-second gentle-entry
/// stretch -- before the next burst.  The congestion timeline is printed so
/// the post-idle action is visible, and the delivered bytes make a throughput
/// cost visible.  Seeded and report-only besides liveness.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; one ~35 s idle-restart arm; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_bulk_idle_restart_arm() {
    /// 2 Mbit/s: a 512 KiB burst drains in ~2 s.
    const RATE_BPS: u64 = 2_000_000;
    const BURST_BYTES: usize = 512 * 1024;
    const IDLE: Duration = Duration::from_secs(4);
    const RUN_FOR: Duration = Duration::from_secs(34);

    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TASK_QUEUE_BOUND);
    let (timeline, delivered, bursts, send_time) = tasks
        .run(async {
            let (sink_addr, delivered) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let pair = NetemPair::spawn(
                sink_addr,
                NetemConfig {
                    latency: OWD,
                    jitter: JITTER,
                    rate: RATE_BPS,
                    seed: 41,
                    ..NetemConfig::default()
                },
                NetemConfig {
                    latency: OWD,
                    jitter: JITTER,
                    seed: 42,
                    ..NetemConfig::default()
                },
            )
            .unwrap();
            let (observer, _fec_cell, _snapshot_cell, timeline_cell, _rtt_samples) = fec_observer();
            let (mut read, mut write) = with_timeout(
                Duration::from_secs(15),
                "idle-restart connect",
                rtp_connect_with_mss_fec_tuning_and_observer_via(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                    rtp::FecTuning::default(),
                    observer,
                ),
            )
            .await;
            // Keep the read half alive so ACKs are processed while sending.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let payload = cyclic_payload(BURST_BYTES);
            let start = Instant::now();
            let mut first = true;
            let mut send_time = Duration::ZERO;
            let mut bursts = 0u32;
            while start.elapsed() < RUN_FOR {
                if !first {
                    tokio::time::sleep(IDLE).await;
                }
                first = false;
                // One burst; blocks until the staging buffer accepts it, so the
                // writer paces the offered load instead of dumping it.  The
                // accepted-burst time is the controller's throughput cost
                // without the fixed idle gaps.
                let burst_start = Instant::now();
                if write.write_all(&payload).await.is_err() {
                    break;
                }
                send_time += burst_start.elapsed();
                bursts += 1;
            }
            tokio::time::sleep(GRACE).await;
            let timeline = std::mem::take(&mut *timeline_cell.lock().unwrap());
            let delivered = delivered.load(Ordering::Relaxed);
            pair.stop();
            (timeline, delivered, bursts, send_time)
        })
        .await;
    eprintln!(
        "[idlerestart] delivered={delivered} bytes over {} s ({:.0} KiB/s); bursts={bursts} \
         accepted_send_time={:.2} s ({:.0} ms/burst)",
        RUN_FOR.as_secs(),
        delivered as f64 / RUN_FOR.as_secs_f64() / 1024.0,
        send_time.as_secs_f64(),
        send_time.as_secs_f64() * 1000.0 / f64::from(bursts.max(1)),
    );
    print_congestion_timeline("bulk_idle_restart", &timeline);
}

/// Focused shared-bottleneck sweep: runs only the four shared-link queue-depth
/// arms (`shared400k_128kib`, `shared400k_32kib`, `shared200k_64kib`,
/// `shared400k_4kib`), so the queue-depth-versus-interactive-latency trade and
/// the bulk-lane goodput can be reproduced in isolation. Report-only besides
/// liveness guards: the table is the deliverable.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "shared-bottleneck latency sweep; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn jitter_shared_bottleneck_arms() {
    /// 400 KiB/s = 3.2768 Mbps shared client->server bottleneck.
    const SHARED_400KIB_BPS: u64 = 400 * 1024 * 8;
    /// 200 KiB/s shared client->server bottleneck.
    const SHARED_200KIB_BPS: u64 = 200 * 1024 * 8;

    let clean = |seed: u64| link_custom(seed, OWD, JITTER, 0, 0, 0, 0, 0);
    // The bulk lane's per-flow config carries no rate when a shared shaper
    // supplies the bottleneck; only the shaper rate shapes it.
    let bulk_c2s = link(43, LOSS_2, 0);
    let bulk_s2c = link(44, LOSS_2, 0);

    let shared_arms: Vec<(&str, u64, u64)> = vec![
        ("shared400k_128kib", SHARED_400KIB_BPS, 128 * 1024),
        ("shared400k_32kib", SHARED_400KIB_BPS, 32 * 1024),
        ("shared200k_64kib", SHARED_200KIB_BPS, 64 * 1024),
        ("shared400k_4kib", SHARED_400KIB_BPS, 4 * 1024),
    ];
    let mut runs: Vec<(&str, DualRun)> = Vec::new();
    for (name, rate, limit_bytes) in shared_arms {
        let label = format!("shared/{name}");
        let shaper = BottleneckShaper::new(rate, limit_bytes);
        // Sample the shared shaper's serialization backlog every 10 ms for the
        // whole bulk-active window. This is the low-noise mechanism signal:
        // the interactive latency is the queue depth divided by the link rate.
        let backlog = Arc::new(Mutex::new(Vec::<u64>::new()));
        let backlog_sink = Arc::clone(&backlog);
        let backlog_shaper = shaper.clone();
        // Owned sampler task: a JoinSet is kept so the test body aborts and
        // drains it at scope end, matching the crate's task-ownership rule
        // (no detached `tokio::spawn`).
        let mut sampler = tokio::task::JoinSet::new();
        sampler.spawn(async move {
            let start = Instant::now();
            tokio::time::sleep(BULK_RAMP).await;
            while start.elapsed() < RUN_FOR {
                backlog_sink
                    .lock()
                    .unwrap()
                    .push(backlog_shaper.backlog_bytes(Instant::now()));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let run = with_timeout(
            Duration::from_secs(180),
            &label,
            run_duallane_links_shaped(
                &label,
                true,
                DualLaneLinks {
                    int_c2s: clean(41),
                    int_s2c: clean(42),
                    bulk_c2s: bulk_c2s.clone(),
                    bulk_s2c: bulk_s2c.clone(),
                },
                true,
                Some(shaper),
            ),
        )
        .await;
        sampler.abort_all();
        while sampler.join_next().await.is_some() {}
        assert_reportable(&label, &run.run.summary);
        runs.push((name, run));
        let mut samples = backlog.lock().unwrap().clone();
        samples.sort_unstable();
        let percentile = |p: f64| -> u64 {
            if samples.is_empty() {
                return 0;
            }
            let idx = ((samples.len() - 1) as f64 * p).round() as usize;
            samples[idx]
        };
        eprintln!(
            "[backlog] {name}: n={} p50={} p90={} p99={} max={} bytes (link={} B/s)",
            samples.len(),
            percentile(0.50),
            percentile(0.90),
            percentile(0.99),
            samples.last().copied().unwrap_or(0),
            rate / 8,
        );
    }
    let views: Vec<(&str, &DualRun)> = runs.iter().map(|(n, r)| (*n, r)).collect();
    print_newdim_table(&views);
    for (name, run) in &runs {
        let goodput = run.bulk_sink_bytes as f64 / (RUN_FOR - BULK_RAMP).as_secs_f64();
        eprintln!(
            "[shared] {name}: p50={:.1} p99={:.1} p999={:.1} max={:.1} ms | bulk_wire={} \
             sink={} goodput={:.0} B/s | int_pair={:?} | bulk_pair={:?}",
            run.run.summary.p50,
            run.run.summary.p99,
            run.run.summary.p999,
            run.run.summary.max,
            run.bulk_wire_bytes,
            run.bulk_sink_bytes,
            goodput,
            run.run.counters,
            run.bulk_counters,
        );
        print_congestion_timeline(name, &run.run.timeline);
    }
}
