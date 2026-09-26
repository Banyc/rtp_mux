//! The tri-mandate performance smoke set: one short, always-run measurement
//! per mandate, plus the panels a reader checks the numbers against.
//!
//! The operator's product constitution is three mandates — **M1** interactive
//! tail latency, **M2** the interactive lane delivering what it is offered
//! without inflating its own wire, **M3** bulk goodput as a fraction of the
//! link rate — and this target is the one command that measures all three and
//! leaves machine-checkable evidence for each:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test mandate_smoke -- --nocapture
//! ```
//!
//! The one authority for the mandate bounds (their values, their derivation
//! and the arms they are asserted on) is `rtp_mux/GATE.md` ("Performance");
//! the existing opt-in constitution gates
//! (`rtp_mux_jitter::jitter_duallane_constitution_gate_p99`,
//! `rtp_mux_jitter::jitter_duallane_constitution_gate`,
//! `dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction`)
//! remain their owners. This file is a **smoke set alongside** them: it does
//! not retune, re-arm or replace any of them.
//!
//! # The smoke arms
//!
//! All three mandates are measured on the production dual-lane topology (the
//! interactive lane on its own RTP connection in frame fast-forward + prompt
//! FEC, the bulk lane on a second, separate `LaneRtpConfig::production_bulk`
//! connection). M1 and M2 share three arms, in the shape the field sends:
//!
//! | arm | impairment | interactive load | bulk |
//! | --- | --- | --- | --- |
//! | `clean` | 2 % iid, 25 ms one-way, 5 ms jitter | 256 B cadence | 2 MiB / 3 s |
//! | `hostile` | GE `gilbert_elliott_loss(5, 8)`, 25 ms one-way, 100 ms jitter | 256 B cadence | 2 MiB / 3 s |
//! | `lone_tail` | GE `gilbert_elliott_loss(5, 8)`, 25 ms one-way, 100 ms jitter | `RequestResponse { depth: 1 }` (the lone tail) | none |
//!
//! The windows are short by measurement, not by habit: the goodput signal
//! settles within a few seconds of a long window, so every window here is
//! `<= 15 s` and the interactive cadence is `~5 ms` so the tail percentiles
//! have thousands of samples per arm instead of hundreds. `MANDATE_SMOKE_QUICK=1`
//! (set by `tools/mandate-check --quick`) takes the shortest windows while
//! still printing all three verdict lines and writing all six evidence files.
//!
//! # Bounds: mandate assertions and regression guards
//!
//! **M1** asserts the mandate bound — `p99 <= 250 ms` and **zero** samples
//! `> 250 ms` — on the `clean` arm. The `hostile` and `lone_tail` arms carry
//! the product's **known, measured** hostile defect (the 1 s `MIN_RTO` repair
//! floor: measured GE lone-tail p99 1053–1542 ms, `> 250 ms` up to 2.7 %), so
//! they assert a *regression bound* derived from that measurement with
//! documented headroom instead of a bound that is currently false. **M2**
//! asserts `delivery == 1.000` and the `6x` own-wire budget on `clean`, and
//! regression bounds (delivery floor, wire ceiling) on the hostile arms.
//! **M3** asserts the within-run delivered/shaper-forwarded fraction against
//! the `0.35x` floor. The hostile panels draw the mandate ceiling/budget/floor
//! lines regardless, so the breach stays visible even where the assertion is
//! only a guard — the assertion is a tripwire, the panel is the evidence.
//!
//! The regression bounds and their derivation are recorded in `GATE.md`; the
//! constants below carry a one-line pointer rather than restating it.
//!
//! # M4: the interactive lane's split across several flows
//!
//! M1 and M2 measure one interactive flow, so a mandate result obtained by
//! starving one of several flows sharing the interactive lane would pass them.
//! **M4** closes that: `M4_FLOWS` interactive flows are multiplexed on ONE
//! interactive lane (the same production `LaneRtpConfig::frame_reordering`
//! lane as M1/M2's clean arm), each offering the same payload at the same
//! cadence, and the arm asserts the outcome pair the fairness mandate names —
//! **no starvation** (every flow delivers what it is offered) and **fair
//! share** (no flow's share of the lane's delivered bytes departs from the
//! equal share by more than [`M4_IMBALANCE_BOUND`]) — plus a fair-latency bound
//! (no flow's p99 exceeds the best flow's p99 by more than
//! [`M4_LATENCY_SPREAD_BOUND`] on the clean arm, the dimension the share
//! statistic cannot see; the hostile arm keeps M1's absolute guard).
//! The per-flow latencies are reported and the panel draws M1's ceiling, so a
//! fair-but-slow split is visible; M1 remains the authority for the absolute
//! interactive ceiling. The statistic, its derived
//! bound and the arms are stated in `rtp_mux/GATE.md` ("Performance"), one
//! authority with the rest of the mandate bounds; the constants below carry a
//! pointer, not a restatement.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::join_all;
use mux::LaneClass;
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::presets::gilbert_elliott_loss;
use netem_test::kit::stats::{HolSummary, summarize};
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{NetemConfig, NetemPair};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use rtp_mux::testkit::mux_over_rtp::send_timestamped_messages;
use rtp_mux::testkit::rtp_mux::ECHO_TAG;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::MissedTickBehavior;

// ─────────────────────────────── arm constants ───────────────────────────────

/// One-way delay applied to every interactive packet: the deployment profile.
const OWD: Duration = Duration::from_millis(25);
/// Uniform jitter around [`OWD`] on the clean arm (the existing arms' 5 ms).
const JITTER: Duration = Duration::from_millis(5);
/// Uniform jitter on the hostile arms: the field's ~100 ms excursion regime.
const HOSTILE_JITTER: Duration = Duration::from_millis(100);
/// `u32` loss threshold equal to `pct` percent per packet.
const fn loss_pct(pct: u32) -> u32 {
    (u32::MAX / 100) * pct
}
/// The clean/mild arm's independent per-packet loss.
const LOSS_2: u32 = loss_pct(2);
/// The interactive message size, a typical game ping.
const MSG_BYTES: usize = 256;
/// Interactive cadence: ~5 ms, so a short window still buys thousands of
/// samples rather than hundreds.
const CADENCE: Duration = Duration::from_millis(5);
/// Interactive window (full tier).
const WINDOW: Duration = Duration::from_secs(12);
/// Interactive window (quick tier) — the shortest window with a usable tail.
const QUICK_WINDOW: Duration = Duration::from_secs(4);
/// Request/response (lone-tail) window. A lone-tail lane offers one round trip
/// at a time, so it needs a longer window than the cadence arms for a usable
/// sample count; still `<= 15 s`.
const RR_WINDOW: Duration = Duration::from_secs(15);
/// Request/response window (quick tier).
const RR_QUICK_WINDOW: Duration = Duration::from_secs(5);
/// Drains stragglers before the summary is read, so a message offered at the
/// window's edge is not counted as lost.
const GRACE: Duration = Duration::from_secs(2);
/// The bulk burst shape on the M1/M2 arms (the existing production load).
const BULK_RATE_BPS: u64 = 8 * 1024 * 1024;
const BULK_BURST_BYTES: usize = 2 * 1024 * 1024;
const BULK_PERIOD: Duration = Duration::from_secs(3);
const BULK_RAMP: Duration = Duration::from_millis(1500);
/// The bulk lane's configured capacity, the M3 denominator.
const M3_CAPACITY_BPS: u64 = 8 * 1024 * 1024;
/// The saturating bulk window (full / quick tier).
const BULK_WINDOW: Duration = Duration::from_secs(6);
const QUICK_BULK_WINDOW: Duration = Duration::from_secs(2);
const M3_REPS: usize = 3;

// ─────────────────────── mandate bounds (authority: GATE.md) ─────────────────

/// The M1 ceiling: the mandate bound asserted on the clean arm. One authority
/// for its value and derivation: `rtp_mux/GATE.md` ("Performance").
const M1_CEILING_MS: f64 = 250.0;
/// The M2 own-wire budget (`int_c2s_wire_bytes / (sent * MSG_BYTES)`) asserted
/// on the clean arm. One authority: `rtp_mux/GATE.md` ("Performance").
const M2_WIRE_BUDGET_X: f64 = 6.0;
/// The M3 goodput floor as a fraction of the configured link rate. One
/// authority: `rtp_mux/GATE.md` ("Performance").
const M3_CAPACITY_FRACTION: f64 = 0.35;

// ───────────────── hostile regression guards (derivation: GATE.md) ──────────

/// M1 hostile cadence-arm p99 guard. The 12 s GE `5 %`/mean-8 + 100 ms-jitter
/// cadence arm measured p99 212-280 ms across runs; the guard is ~3x that
/// band, so a change that doubles the hostile tail fails while the measured
/// defect (which the arm exists to keep visible against the 250 ms line) does
/// not.
const M1_HOSTILE_P99_GUARD_MS: f64 = 900.0;
/// M1 hostile cadence-arm `> 250 ms` sample-count guard (measured 0-2.75 %).
const M1_HOSTILE_OVER250_GUARD_PCT: f64 = 8.0;
/// M1 lone-tail p99 guard. The field's 60 s GE lone-tail arms measured p99
/// 1053-1542 ms (the 1 s `MIN_RTO` repair floor plus backoff); the guard is
/// ~2x the top of that band, so a change that at least doubles the known
/// lone-tail defect fails. It is a guard, not the 250 ms mandate ceiling.
const M1_LONE_P99_GUARD_MS: f64 = 3200.0;
/// M1 lone-tail p99.9 guard. The smoke arm's 15 s window measured p999
/// 797-1636 ms and the field's slowest 60 s RTO ladder reached 5315 ms; the
/// guard clears the measured ladder with headroom, so a defect that doubles
/// the ladder fails.
const M1_LONE_P999_GUARD_MS: f64 = 8000.0;
/// M1 lone-tail `> 250 ms` sample-count guard. The field measured up to 2.7 %
/// and the smoke arm 0-0.7 %; ~3x the field band.
const M1_LONE_OVER250_GUARD_PCT: f64 = 8.0;
/// One-way delay of the field-RTT lone-tail arm. The smoke arms above run the
/// deployment's 25 ms profile (~50 ms round trip); the deployed client reports
/// a ~190 ms *minimum* round trip, so this arm moves the same request/response
/// shape onto a ~100 ms one-way path and asks whether the tail follows the
/// RTT. It does not: the repair ladder's step is a constant (the 1 s
/// `MIN_RTO` floor, `rtp/src/traffic_shaping/recovery/rto.rs`), not an
/// RTT-derived value, so the arm only gets *smaller* as the RTT grows.
const FIELD_RTT_OWD: Duration = Duration::from_millis(100);
/// M1 field-RTT lone-tail p99 guard. The band spans the two revisions this
/// crate has run: on the pinned `rtp v0.0.94` three 15 s runs measured p99
/// 427-719 ms, and on the landed `rtp` dev (`bdacf5c0`) four runs measured
/// 293-432 ms. The guard clears the top of the *pinned* band at ~2.1x, so a
/// change that doubles the arm's tail fails on either revision. It is a
/// regression tripwire, not the 250 ms mandate ceiling, for the same reason
/// the other impaired arms carry guards: the tail defect is open. The
/// revision-to-revision delta is recorded in `GATE.md` as the arm's reading of
/// what the landed transport bought at the field's RTT.
const M1_FIELD_RTT_P99_GUARD_MS: f64 = 1500.0;
/// M1 field-RTT lone-tail `> 250 ms` guard. The pinned arm measured 3.3-5.6 %
/// of samples over the ceiling and the landed arm 2.6-5.3 %; ~2.7x the worst
/// of the band.
const M1_FIELD_RTT_OVER250_GUARD_PCT: f64 = 15.0;
/// M2 hostile cadence-arm delivery floor (regression guard; measured 1.000).
const M2_HOSTILE_DELIVERY_FLOOR: f64 = 0.995;
/// M2 lone-tail delivery floor (measured 1.000).
const M2_LONE_DELIVERY_FLOOR: f64 = 0.995;
/// M2 hostile cadence-arm wire-multiple guard (measured 4.7-4.8x).
const M2_HOSTILE_WIRE_GUARD_X: f64 = 10.0;
/// M2 lone-tail wire-multiple guard. The field measured lone-tail wire
/// 6.22-7.17x (over the 6x budget) and the smoke arm 6.07-6.41x; ~2x the top.
const M2_LONE_WIRE_GUARD_X: f64 = 14.0;

// ─────────────────────────────── diagnostics ─────────────────────────────────

/// Serialises the three mandate measurements: this target is run with no
/// `--test-threads` flag, and the latency assertions are wall-clock, so the
/// windows must not overlap. A `tokio` mutex (not `std`) so the guard may be
/// held across `.await` without parking a runtime worker.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn quick() -> bool {
    matches!(std::env::var("MANDATE_SMOKE_QUICK").as_deref(), Ok("1"))
}

fn cadence_window() -> Duration {
    if quick() { QUICK_WINDOW } else { WINDOW }
}

fn rr_window() -> Duration {
    if quick() { RR_QUICK_WINDOW } else { RR_WINDOW }
}

fn bulk_window() -> Duration {
    if quick() {
        QUICK_BULK_WINDOW
    } else {
        BULK_WINDOW
    }
}

/// The evidence directory: `$MANDATE_CHECK_DIR` when the runner set it (it
/// always does), else a sane default under `target/` for a plain
/// `cargo test -p rtp_mux`.
fn out_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("MANDATE_CHECK_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    let root = std::env::var("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
    root.join("mandate-smoke")
}

/// The deliberate-fault selector used only by the vacuity demonstrations:
/// `M1_latency`, `M2_wire`, `M2_delivery`, `M3_starve`, `M4_starve` or
/// `M4_drop`. Unset in every real
/// run (the runner never sets it). Faults perturb an arm's *input* — the
/// impairment or the offered payload — never the assertion, so the failure is
/// produced by the measurement path.
fn fault(mandate: &str) -> Option<String> {
    let value = std::env::var("MANDATE_SMOKE_FAULT").ok()?;
    let value = value.trim();
    if value.is_empty() || !value.starts_with(mandate) {
        return None;
    }
    Some(value.to_owned())
}

fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

// ────────────────────────────── arm definitions ──────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Load {
    Cadence,
    RequestResponse { depth: usize },
}

struct ArmSpec {
    name: &'static str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk: bool,
    load: Load,
    window: Duration,
    msg_bytes: usize,
}

/// One impairment direction: fixed delay + jitter, an independent-loss
/// threshold, and an optional per-flow rate cap.
fn link(seed: u64, latency: Duration, jitter: Duration, loss: u32, rate_bps: u64) -> NetemConfig {
    NetemConfig {
        latency,
        jitter,
        rate: rate_bps,
        loss,
        seed,
        ..NetemConfig::default()
    }
}

fn hostile_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: HOSTILE_JITTER,
        loss_model: gilbert_elliott_loss(5.0, 8.0),
        seed,
        ..NetemConfig::default()
    }
}

/// [`hostile_link`] moved onto the field's ~190 ms round trip: the same GE
/// model and jitter with the one-way delay the deployed client reports.
fn field_rtt_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: FIELD_RTT_OWD,
        ..hostile_link(seed)
    }
}

/// The field-RTT lone-tail arm: the M1/M2 `lone_tail` shape (one unacked 256 B
/// message at a time, no bulk lane) at [`FIELD_RTT_OWD`]. The fault selector
/// `MANDATE_SMOKE_FAULT=M1_FIELD_RTT_slow` injects +1000 ms one-way delay on
/// both directions, the arm's own vacuity demonstration: it perturbs the arm's
/// *input*, so the failure it produces comes from the measurement path.
fn field_rtt_arm() -> ArmSpec {
    let slow = fault("M1_FIELD_RTT").is_some();
    let extra = if slow {
        Duration::from_millis(1000)
    } else {
        Duration::ZERO
    };
    let shift = |mut link: NetemConfig| {
        link.latency += extra;
        link
    };
    ArmSpec {
        name: "field_rtt",
        int_c2s: shift(field_rtt_link(41)),
        int_s2c: shift(field_rtt_link(42)),
        bulk: false,
        load: Load::RequestResponse { depth: 1 },
        window: rr_window(),
        msg_bytes: MSG_BYTES,
    }
}

/// The M1/M2 arm set, with the fault for `mandate` applied to the `clean` arm
/// when one is selected. `clean` is the mandate-bound arm; `hostile` and
/// `lone_tail` are the regression-guard arms.
fn mandate_arms(mandate: &str) -> Vec<ArmSpec> {
    let clean_fault = fault(mandate);
    let mut clean_c2s = link(41, OWD, JITTER, LOSS_2, 0);
    let mut clean_s2c = link(42, OWD, JITTER, LOSS_2, 0);
    let mut clean_bulk = true;
    let mut clean_load = Load::Cadence;
    let mut clean_window = cadence_window();
    if let Some(fault) = clean_fault.as_deref() {
        match fault {
            // Blow the latency ceiling: +500 ms one-way on both directions.
            "M1_latency" => {
                clean_c2s.latency = OWD + Duration::from_millis(500);
                clean_s2c.latency = OWD + Duration::from_millis(500);
            }
            // Inflate the own-wire multiple: put the clean arm on the lone-tail
            // request/response shape, the measured over-budget wire regime
            // (the fresh-tail armour of a lone unacked message).
            "M2_wire" => {
                clean_bulk = false;
                clean_load = Load::RequestResponse { depth: 1 };
                clean_window = rr_window();
            }
            // Drop a delivery: starve the interactive lane to 90 % iid loss.
            "M2_delivery" => {
                clean_c2s.loss = loss_pct(90);
                clean_s2c.loss = loss_pct(90);
            }
            _ => {}
        }
    }
    let cadence = cadence_window();
    vec![
        ArmSpec {
            name: "clean",
            int_c2s: clean_c2s,
            int_s2c: clean_s2c,
            bulk: clean_bulk,
            load: clean_load,
            window: clean_window,
            msg_bytes: MSG_BYTES,
        },
        ArmSpec {
            name: "hostile",
            int_c2s: hostile_link(41),
            int_s2c: hostile_link(42),
            bulk: true,
            load: Load::Cadence,
            window: cadence,
            msg_bytes: MSG_BYTES,
        },
        ArmSpec {
            name: "lone_tail",
            int_c2s: hostile_link(41),
            int_s2c: hostile_link(42),
            bulk: false,
            load: Load::RequestResponse { depth: 1 },
            window: rr_window(),
            msg_bytes: MSG_BYTES,
        },
    ]
}

// ───────────────────────────────── the runner ────────────────────────────────

struct ArmRun {
    name: &'static str,
    summary: HolSummary,
    /// The measured latency samples: the server's one-way reading for the
    /// cadence arms, the client's own round trip for the lone-tail arm (the
    /// deadline the application waits on).
    samples: Vec<f64>,
    /// `(elapsed seconds, latency ms)` per sample, in delivery order.
    timeline: Vec<(f64, f64)>,
    int_c2s_wire_bytes: u64,
    offered_bytes: u64,
    wire_x: f64,
    bulk_sink_bytes: u64,
    bulk_wire_bytes: u64,
    window: Duration,
    wall: Duration,
}

/// Offer request/response rounds until `run_for` elapses: write `depth`
/// timestamped [`MSG_BYTES`] frames back-to-back, read their echoes, then
/// offer the next round. Returns the offered round count and one
/// `(elapsed seconds, round-trip ms)` per completed round. At `depth` 1 the
/// tracked tail is *lone* — the only unacked data packet on the connection.
async fn request_response_timed(
    write: &mut (impl AsyncWrite + Unpin),
    read: &mut (impl AsyncRead + Unpin),
    base: Instant,
    tag: u8,
    depth: usize,
    msg_bytes: usize,
    run_for: Duration,
) -> (u64, Vec<(f64, f64)>) {
    let payload_bytes = msg_bytes - 12;
    let payload: Vec<u8> = (0..payload_bytes).map(|i| (i % 251) as u8).collect();
    let mut frame = Vec::with_capacity(msg_bytes);
    let mut echoed = vec![0u8; msg_bytes];
    let mut sent = 0u64;
    let mut rounds = Vec::new();
    if write.write_all(&[tag]).await.is_err() {
        return (sent, rounds);
    }
    let start = Instant::now();
    while start.elapsed() < run_for {
        let sent_us = base.elapsed().as_micros() as u64;
        for _ in 0..depth {
            frame.clear();
            frame.extend_from_slice(&((msg_bytes as u32).to_le_bytes()));
            frame.extend_from_slice(&payload);
            frame.extend_from_slice(&sent_us.to_le_bytes());
            if write.write_all(&frame).await.is_err() {
                return (sent, rounds);
            }
            sent += 1;
        }
        for _ in 0..depth {
            if read.read_exact(&mut echoed).await.is_err() {
                return (sent, rounds);
            }
            let elapsed = base.elapsed().as_secs_f64();
            rounds.push((elapsed, elapsed * 1000.0 - sent_us as f64 / 1000.0));
        }
    }
    (sent, rounds)
}

/// A periodic bulk burst: `burst_bytes` offered every `period`, as fast as the
/// transport accepts, for the duration of the run (the production load shape).
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

/// Run one dual-lane smoke arm and read back everything the three mandates
/// need from it: the latency summary, the timeline (for the panel), the
/// interactive lane's own client->server wire, the offered payload, and the
/// bulk sink/shaper counters.
async fn run_arm(spec: ArmSpec) -> ArmRun {
    let ArmSpec {
        name,
        int_c2s,
        int_s2c,
        bulk,
        load,
        window,
        msg_bytes,
    } = spec;
    let wall = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let bulk_c2s = link(43, OWD, JITTER, LOSS_2, BULK_RATE_BPS);
    let bulk_s2c = link(44, OWD, JITTER, LOSS_2, BULK_RATE_BPS);
    let bulk_off = NetemConfig::default();

    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let outcome = tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            let bulk_pair = NetemPair::spawn(
                bulk_addr,
                if bulk {
                    bulk_c2s.clone()
                } else {
                    bulk_off.clone()
                },
                if bulk {
                    bulk_s2c.clone()
                } else {
                    bulk_off.clone()
                },
            )
            .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
            )
            .await
            .unwrap();

            let (mut lat_read, mut lat_write) = opener.open(LaneClass::Interactive).await.unwrap();
            let bulk_write = if bulk {
                let (mut bulk_read, bulk_write) = opener.open(LaneClass::Bulk).await.unwrap();
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

            // The sink publishes one row per parsed frame into a bounded
            // channel; a collector drains it for the whole arm, so a lane whose
            // sample rate exceeds the channel depth cannot overflow it or leave
            // samples undrained.
            let collector_sink = Arc::new(Mutex::new(Vec::<(f64, f64)>::new()));
            let sink_for_task = Arc::clone(&collector_sink);
            let task_tx_int = task_tx.clone();
            let interactive = async move {
                submit_test_task(
                    &task_tx_int,
                    Box::pin(async move {
                        while let Some((_tag, latency)) = latencies.recv().await {
                            sink_for_task
                                .lock()
                                .unwrap()
                                .push((base.elapsed().as_secs_f64(), latency));
                        }
                    }),
                );
                match load {
                    Load::Cadence => {
                        submit_test_task(
                            &task_tx_int,
                            Box::pin(async move {
                                let mut buf = vec![0u8; 8 * 1024];
                                while let Ok(n) = lat_read.read(&mut buf).await {
                                    if n == 0 {
                                        break;
                                    }
                                }
                            }),
                        );
                        let sent = if lat_write.write_all(b"L").await.is_err() {
                            0
                        } else {
                            send_timestamped_messages(
                                &mut lat_write,
                                base,
                                msg_bytes,
                                CADENCE,
                                window,
                            )
                            .await
                        };
                        (sent, Vec::new())
                    }
                    Load::RequestResponse { depth } => {
                        request_response_timed(
                            &mut lat_write,
                            &mut lat_read,
                            base,
                            ECHO_TAG,
                            depth,
                            msg_bytes,
                            window,
                        )
                        .await
                    }
                }
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
                    window,
                )
                .await
            };
            let ((sent, rtts), _bulk_written) = tokio::join!(interactive, bulk_fut);

            tokio::time::sleep(GRACE).await;
            let int_c2s_wire_bytes = int_pair.stats_c2s().forwarded_bytes;
            let bulk_sink_bytes = bulk_counter.load(Ordering::Relaxed);
            let bulk_wire_bytes = bulk_pair.stats_c2s().forwarded_bytes;
            // The collector's sink: which rows become the arm's measured
            // sample is the load shape's choice — the server's one-way reading
            // for a cadence arm, the client's round trip for a request/response
            // arm.
            let collected = std::mem::take(&mut *collector_sink.lock().unwrap());
            let (samples, timeline) = match load {
                Load::Cadence => {
                    let samples: Vec<f64> = collected.iter().map(|(_, l)| *l).collect();
                    (samples, collected)
                }
                Load::RequestResponse { .. } => {
                    let samples: Vec<f64> = rtts.iter().map(|(_, l)| *l).collect();
                    (samples, rtts)
                }
            };
            int_pair.stop();
            bulk_pair.stop();
            (
                sent,
                samples,
                timeline,
                int_c2s_wire_bytes,
                bulk_sink_bytes,
                bulk_wire_bytes,
            )
        })
        .await;
    let (sent, samples, timeline, int_c2s_wire_bytes, bulk_sink_bytes, bulk_wire_bytes) = outcome;
    let received = samples.len() as u64;
    let bulk_active_secs = if bulk {
        (window.saturating_sub(BULK_RAMP)).as_secs_f64()
    } else {
        0.0
    };
    let offered_bytes = sent.saturating_mul(msg_bytes as u64);
    let summary = summarize(
        samples.clone(),
        sent,
        received,
        bulk_wire_bytes,
        bulk_active_secs,
    );
    ArmRun {
        name,
        summary,
        samples,
        timeline,
        int_c2s_wire_bytes,
        offered_bytes,
        wire_x: if offered_bytes == 0 {
            f64::INFINITY
        } else {
            int_c2s_wire_bytes as f64 / offered_bytes as f64
        },
        bulk_sink_bytes,
        bulk_wire_bytes,
        window,
        wall: wall.elapsed(),
    }
}

// ──────────────────── the M1/M2 shared arm measurement ───────────────────────

/// The arm runs M1 and M2 both read, measured once.
///
/// [`mandate_arms`] returns the **same** three arms for M1 and M2 — same names,
/// impairment, seeds and windows — and the two mandates differ only in which
/// fields of each [`ArmRun`] they assert on ([`m1_rows`] versus [`m2_rows`]), so
/// measuring them twice is a duplicated run rather than extra coverage.
/// Whichever test reaches this cache first measures the arms and stores them;
/// the other reads the very same runs.
///
/// The state is keyed by [`fault`]'s selection, the one input that changes the
/// arms. A fault perturbs an arm's *input* (impairment or offered load), which
/// makes it a different measurement, so a fault run is **never** stored and
/// never served: only the clean `MANDATE_SMOKE_FAULT`-unset run — fault key
/// `""` — is cacheable. At most one mandate matches a given fault value
/// ([`fault`] returns `Some` only for the mandate whose prefix the value
/// carries), so a single slot is a complete cache. That is what keeps the
/// deliberate-fault isolation intact: `MANDATE_SMOKE_FAULT=M2_delivery` moves
/// M2 alone, and a clean M1 can never inherit it.
static ARM_RUNS: std::sync::Mutex<Option<(String, Arc<Vec<ArmRun>>)>> = std::sync::Mutex::new(None);

/// The three [`mandate_arms`] runs, measured unless already cached under
/// `mandate`'s fault key. The arms themselves are untouched: the same specs,
/// seeds, windows, cadence and `GRACE` as before, driven through the same
/// [`run_arm`] and [`with_timeout`]; only the duplicated execution is gone.
/// The cache lock is never held across an `await` — [`SERIAL`] already serialises
/// the callers, so the critical sections are two plain field reads.
async fn mandate_runs(mandate: &str) -> Arc<Vec<ArmRun>> {
    let key = fault(mandate).unwrap_or_default();
    let cached = ARM_RUNS
        .lock()
        .expect("the arm-run cache mutex is never poisoned")
        .clone();
    if let Some((cached_key, runs)) = cached
        && cached_key == key
    {
        eprintln!(
            "[mandate-smoke] {mandate} reads the arm runs already measured under the {cached_key:?} fault key"
        );
        // Both mandates report the arms they assert on: `tools/mandate-check`
        // attributes every arm line to the mandate whose `MANDATE` line follows
        // it and refuses a mandate with no arm line (`M2/clean`, `M2/hostile`
        // and `M2/lone_tail` are declared cells in `tools/mandate-arms.json`).
        // These rows are the measurement the mandate reads, reprinted under
        // the reading mandate's attribution; they are not a second run.
        for run in runs.iter() {
            print_arm(run);
        }
        return runs;
    }
    let runs = Arc::new(measure_arms(mandate).await);
    if key.is_empty() {
        *ARM_RUNS
            .lock()
            .expect("the arm-run cache mutex is never poisoned") = Some((key, Arc::clone(&runs)));
    }
    runs
}

/// Measure the [`mandate_arms`] set in order, printing each arm's row, exactly
/// as the M1 and M2 runner loops used to before the measurement was shared.
async fn measure_arms(mandate: &str) -> Vec<ArmRun> {
    let mut runs = Vec::new();
    for spec in mandate_arms(mandate) {
        let label = format!("{}/{}", mandate.to_lowercase(), spec.name);
        let run = with_timeout(Duration::from_secs(120), &label, run_arm(spec)).await;
        print_arm(&run);
        runs.push(run);
    }
    runs
}

fn over250_count(samples: &[f64]) -> usize {
    samples.iter().filter(|x| **x > M1_CEILING_MS).count()
}

fn over250_pct(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        100.0 * over250_count(samples) as f64 / samples.len() as f64
    }
}

/// A nearest-rank CDF on the mandated 0-100 percentile axis: `(latency ms,
/// percentile)` pairs, so the renderer can plot it without deriving anything.
fn cdf_points(samples: &[f64], points: usize) -> Vec<(f64, f64)> {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if sorted.is_empty() {
        return vec![(0.0, 0.0)];
    }
    let mut out = Vec::with_capacity(points);
    for index in 0..points {
        let pct = 100.0 * index as f64 / (points - 1).max(1) as f64;
        let rank = ((sorted.len() - 1) as f64 * pct / 100.0).round() as usize;
        out.push((sorted[rank.min(sorted.len() - 1)], pct));
    }
    out
}

fn print_arm(run: &ArmRun) {
    let s = &run.summary;
    let row = format!(
        "[mandate-smoke {name:<9}] sent={sent:>5} recv={recv:>5} delivery={del:.3} \
         p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} p999={p999:7.1} max={max:8.1} \
         over250={o25:>4} wire={w:>10}B x={x:.2} bulk_sink={bs:>10}B bulk_wire={bw:>10}B wall={wall:.1}s window={win:?}\n",
        name = run.name,
        sent = s.sent,
        recv = s.received,
        del = s.delivery_pct,
        p50 = s.p50,
        p90 = s.p90,
        p99 = s.p99,
        p999 = s.p999,
        max = s.max,
        o25 = over250_count(&run.samples),
        w = run.int_c2s_wire_bytes,
        x = run.wire_x,
        bs = run.bulk_sink_bytes,
        bw = run.bulk_wire_bytes,
        wall = run.wall.as_secs_f64(),
        win = run.window,
    );
    // One locked `write_all` of a whole row: `eprintln!` issues one write per
    // format segment, and the four smoke tests share one merged stdout/stderr
    // stream, so a row printed while another test finishes can be split
    // mid-field and become unparseable for `tools/mandate-check`'s arm-line
    // reader. A single write under `PIPE_BUF` cannot interleave.
    let mut stderr = std::io::stderr().lock();
    let _ = std::io::Write::write_all(&mut stderr, row.as_bytes());
}

// ───────────────────────────── evidence writing ──────────────────────────────

fn write_evidence(
    dir: &Path,
    mandate: &str,
    declaration: &str,
    rows: &[(String, String, f64, f64)],
) {
    std::fs::create_dir_all(dir)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot create evidence directory {dir:?}: {e}"));
    std::fs::write(dir.join(format!("{mandate}.json")), declaration)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot write declaration: {e}"));
    let mut csv = String::from("panel,series,x,y\n");
    for (panel, series, x, y) in rows {
        csv.push_str(&format!("{panel},{series},{x:.6},{y:.6}\n"));
    }
    std::fs::write(dir.join(format!("{mandate}.csv")), csv)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot write data CSV: {e}"));
    eprintln!("[mandate-smoke] wrote {mandate}.json + {mandate}.csv into {dir:?}");
}

fn verdict(pass: bool) -> &'static str {
    if pass { "PASS" } else { "FAIL" }
}

// ─────────────────────────────── M1: latency ─────────────────────────────────

fn m1_declaration() -> String {
    format!(
        r#"{{"mandate":"M1","title":"M1 interactive tail latency (clean vs hostile GE+jitter vs hostile lone tail)","x_label":"elapsed time (s)","y_label":"latency (ms)","panels":[{{"id":"latency","chart":"line","series":[{{"name":"clean"}},{{"name":"hostile"}},{{"name":"lone_tail"}}],"bounds":[{{"y":{M1_CEILING_MS},"label":"M1 ceiling 250 ms"}}]}},{{"id":"cdf","chart":"cdf","x_label":"latency (ms)","y_label":"percentile (%)","series":[{{"name":"clean"}},{{"name":"hostile"}},{{"name":"lone_tail"}}],"bounds":[]}}]}}"#
    )
}

fn m1_rows(runs: &[ArmRun]) -> Vec<(String, String, f64, f64)> {
    let mut rows = Vec::new();
    for run in runs {
        for (x, y) in &run.timeline {
            rows.push(("latency".to_owned(), run.name.to_owned(), *x, *y));
        }
        for (x, y) in cdf_points(&run.samples, 101) {
            rows.push(("cdf".to_owned(), run.name.to_owned(), x, y));
        }
    }
    rows
}

/// Mandate 1: interactive tail latency. The clean arm asserts the mandate
/// bound (`p99 <= 250 ms`, zero samples `> 250 ms`); the hostile and lone-tail
/// arms assert the documented regression guards and still draw the ceiling.
#[tokio::test(flavor = "multi_thread")]
async fn m1_interactive_tail_latency() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let runs = mandate_runs("M1").await;
    write_evidence(&dir, "M1", &m1_declaration(), &m1_rows(&runs));

    let clean = &runs[0];
    let hostile = &runs[1];
    let lone = &runs[2];
    let pass = clean.summary.p99 <= M1_CEILING_MS
        && over250_count(&clean.samples) == 0
        && hostile.summary.p99 <= M1_HOSTILE_P99_GUARD_MS
        && over250_pct(&hostile.samples) <= M1_HOSTILE_OVER250_GUARD_PCT
        && lone.summary.p99 <= M1_LONE_P99_GUARD_MS
        && lone.summary.p999 <= M1_LONE_P999_GUARD_MS
        && over250_pct(&lone.samples) <= M1_LONE_OVER250_GUARD_PCT;
    println!(
        "MANDATE M1 {} clean_p50={:.1} clean_p90={:.1} clean_p99={:.1} clean_p999={:.1} clean_max={:.1} clean_over250={} hostile_p50={:.1} hostile_p90={:.1} hostile_p99={:.1} hostile_p999={:.1} hostile_max={:.1} hostile_over250={} lone_p50={:.1} lone_p90={:.1} lone_p99={:.1} lone_p999={:.1} lone_max={:.1} lone_over250={} ceiling={:.1} hostile_p99_guard={:.1} hostile_over250_guard={:.1} lone_p99_guard={:.1} lone_p999_guard={:.1} lone_over250_guard={:.1}",
        verdict(pass),
        clean.summary.p50,
        clean.summary.p90,
        clean.summary.p99,
        clean.summary.p999,
        clean.summary.max,
        over250_count(&clean.samples),
        hostile.summary.p50,
        hostile.summary.p90,
        hostile.summary.p99,
        hostile.summary.p999,
        hostile.summary.max,
        over250_count(&hostile.samples),
        lone.summary.p50,
        lone.summary.p90,
        lone.summary.p99,
        lone.summary.p999,
        lone.summary.max,
        over250_count(&lone.samples),
        M1_CEILING_MS,
        M1_HOSTILE_P99_GUARD_MS,
        M1_HOSTILE_OVER250_GUARD_PCT,
        M1_LONE_P99_GUARD_MS,
        M1_LONE_P999_GUARD_MS,
        M1_LONE_OVER250_GUARD_PCT,
    );

    assert!(
        clean.summary.p99 <= M1_CEILING_MS,
        "[M1] clean-arm p99 {:.1} ms exceeds the {M1_CEILING_MS} ms ceiling: the interactive tail must stay at the one-way floor on the mild 2% iid arm",
        clean.summary.p99,
    );
    assert_eq!(
        over250_count(&clean.samples),
        0,
        "[M1] clean arm has {} sample(s) > {M1_CEILING_MS} ms (p99 {:.1} ms, max {:.1} ms): the mandate requires zero spikes over the ceiling",
        over250_count(&clean.samples),
        clean.summary.p99,
        clean.summary.max,
    );
    assert!(
        hostile.summary.p99 <= M1_HOSTILE_P99_GUARD_MS,
        "[M1] hostile (GE+jitter) arm p99 {:.1} ms exceeds its {M1_HOSTILE_P99_GUARD_MS} ms regression guard: the known hostile tail defect has at least doubled",
        hostile.summary.p99,
    );
    assert!(
        over250_pct(&hostile.samples) <= M1_HOSTILE_OVER250_GUARD_PCT,
        "[M1] hostile arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_HOSTILE_OVER250_GUARD_PCT}% regression guard",
        over250_pct(&hostile.samples),
    );
    assert!(
        lone.summary.p99 <= M1_LONE_P99_GUARD_MS,
        "[M1] hostile lone-tail arm p99 {:.1} ms exceeds its {M1_LONE_P99_GUARD_MS} ms regression guard: the known GE lone-tail defect has at least doubled",
        lone.summary.p99,
    );
    assert!(
        lone.summary.p999 <= M1_LONE_P999_GUARD_MS,
        "[M1] hostile lone-tail arm p999 {:.1} ms exceeds its {M1_LONE_P999_GUARD_MS} ms regression guard (max {:.1} ms): the known RTO-ladder defect has at least doubled",
        lone.summary.p999,
        lone.summary.max,
    );
    assert!(
        over250_pct(&lone.samples) <= M1_LONE_OVER250_GUARD_PCT,
        "[M1] lone-tail arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_LONE_OVER250_GUARD_PCT}% regression guard",
        over250_pct(&lone.samples),
    );
}

// ───────────────────── M1 at the field's RTT scale ─────────────────────────

/// Mandate 1 at the deployed client's round-trip scale.
///
/// The M1/M2 arms above run the deployment's 25 ms one-way profile (~50 ms
/// round trip). The deployed client reports a ~190 ms *minimum* round trip, so
/// "the tail holds on the `clean` arm" says nothing about the RTT the field
/// sees: the M1 breach is a repair ladder, and a ladder's step and rung count
/// are not RTT-invariant. This arm re-runs the `lone_tail` shape
/// (request/response, depth 1, one unacked 256 B message) on
/// [`FIELD_RTT_OWD`]'s ~190 ms round trip and asserts a derived regression
/// guard on the same two quantities M1 asserts on the smoke arms.
///
/// It is a **new** arm, not a retuned one: the `clean`, `hostile` and
/// `lone_tail` arms keep their settings, tiers and guards. It is `#[ignore]`d
/// (`full` tier) because it needs its own ~20 s window on top of the smoke
/// set's ~3 minutes, and because it is a measurement of the open tail defect
/// rather than a mandate bound that currently holds. The guard's derivation is
/// in `GATE.md`; the constants above carry a pointer to it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "field-RTT lone-tail arm; ~20 s; run with --ignored --nocapture"]
async fn m1_lone_tail_field_rtt() {
    let _serial = SERIAL.lock().await;
    let run = with_timeout(
        Duration::from_secs(120),
        "m1-field-rtt/lone_tail",
        run_arm(field_rtt_arm()),
    )
    .await;
    print_arm(&run);

    let pass = run.summary.p99 <= M1_FIELD_RTT_P99_GUARD_MS
        && over250_pct(&run.samples) <= M1_FIELD_RTT_OVER250_GUARD_PCT;
    println!(
        "MANDATE M1_FIELD_RTT {} owd_ms={} samples={} p50={:.1} p90={:.1} p99={:.1} p999={:.1} max={:.1} over250={} over250_pct={:.3} p99_guard={:.1} over250_guard={:.1} ceiling={:.1}",
        verdict(pass),
        FIELD_RTT_OWD.as_millis(),
        run.samples.len(),
        run.summary.p50,
        run.summary.p90,
        run.summary.p99,
        run.summary.p999,
        run.summary.max,
        over250_count(&run.samples),
        over250_pct(&run.samples),
        M1_FIELD_RTT_P99_GUARD_MS,
        M1_FIELD_RTT_OVER250_GUARD_PCT,
        M1_CEILING_MS,
    );

    assert!(
        run.summary.p99 <= M1_FIELD_RTT_P99_GUARD_MS,
        "[M1] field-RTT ({FIELD_RTT_OWD:?} one-way) lone-tail arm p99 {:.1} ms exceeds its {M1_FIELD_RTT_P99_GUARD_MS} ms regression guard (max {:.1} ms): the lone-tail defect at the deployed client's ~190 ms round trip has grown by at least 2x",
        run.summary.p99,
        run.summary.max,
    );
    assert!(
        over250_pct(&run.samples) <= M1_FIELD_RTT_OVER250_GUARD_PCT,
        "[M1] field-RTT lone-tail arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_FIELD_RTT_OVER250_GUARD_PCT}% regression guard",
        over250_pct(&run.samples),
    );
}

// ────────────────────────── M2: delivery and wire ────────────────────────────

fn m2_declaration() -> String {
    format!(
        r#"{{"mandate":"M2","title":"M2 interactive delivery and own-wire multiple (1=clean 2=hostile 3=lone_tail)","x_label":"arm (1=clean 2=hostile 3=lone_tail)","y_label":"value","panels":[{{"id":"delivery","chart":"bar","series":[{{"name":"delivery"}}],"bounds":[{{"y":1.0,"label":"M2 delivery floor 1.000"}}]}},{{"id":"wire","chart":"bar","series":[{{"name":"wire_x"}}],"bounds":[{{"y":{M2_WIRE_BUDGET_X},"label":"M2 wire budget 6x"}}]}}]}}"#
    )
}

fn m2_rows(runs: &[ArmRun]) -> Vec<(String, String, f64, f64)> {
    // One series per panel, one bar per arm at its ordinal x, so the 6x
    // budget (and the 1.000 floor) is drawn once against all three arms and a
    // hostile/lone breach is visible as a bar crossing the line.
    let mut rows = Vec::new();
    for (index, run) in runs.iter().enumerate() {
        let x = (index + 1) as f64;
        rows.push((
            "delivery".to_owned(),
            "delivery".to_owned(),
            x,
            run.summary.delivery_pct,
        ));
        rows.push(("wire".to_owned(), "wire_x".to_owned(), x, run.wire_x));
    }
    rows
}

/// Mandate 2: the interactive lane delivers what it is offered without
/// inflating its own wire. The clean arm asserts the mandate bound
/// (`delivery == 1.000`, own-wire `<= 6x`); the hostile arms assert the
/// regression guards.
#[tokio::test(flavor = "multi_thread")]
async fn m2_interactive_delivery_and_wire() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let runs = mandate_runs("M2").await;
    write_evidence(&dir, "M2", &m2_declaration(), &m2_rows(&runs));

    let clean = &runs[0];
    let hostile = &runs[1];
    let lone = &runs[2];
    let pass = clean.summary.received == clean.summary.sent
        && clean.wire_x <= M2_WIRE_BUDGET_X
        && hostile.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR
        && hostile.wire_x <= M2_HOSTILE_WIRE_GUARD_X
        && lone.summary.delivery_pct >= M2_LONE_DELIVERY_FLOOR
        && lone.wire_x <= M2_LONE_WIRE_GUARD_X;
    println!(
        "MANDATE M2 {} clean_delivery={:.3} clean_wire_x={:.2} hostile_delivery={:.3} hostile_wire_x={:.2} lone_delivery={:.3} lone_wire_x={:.2} budget={:.1} hostile_wire_guard={:.1} lone_wire_guard={:.1} delivery_floor={:.3}",
        verdict(pass),
        clean.summary.delivery_pct,
        clean.wire_x,
        hostile.summary.delivery_pct,
        hostile.wire_x,
        lone.summary.delivery_pct,
        lone.wire_x,
        M2_WIRE_BUDGET_X,
        M2_HOSTILE_WIRE_GUARD_X,
        M2_LONE_WIRE_GUARD_X,
        M2_LONE_DELIVERY_FLOOR,
    );

    assert_eq!(
        clean.summary.received, clean.summary.sent,
        "[M2] clean-arm interactive delivery must be exactly 1.000: {}/{} messages delivered ({:.3}) — the interactive lane ate its own goodput",
        clean.summary.received, clean.summary.sent, clean.summary.delivery_pct,
    );
    assert!(
        clean.wire_x <= M2_WIRE_BUDGET_X,
        "[M2] clean-arm interactive c2s wire {} bytes is {:.2}x the offered {} bytes, over the {M2_WIRE_BUDGET_X}x own-wire budget: redundant wire must not inflate unboundedly",
        clean.int_c2s_wire_bytes,
        clean.wire_x,
        clean.offered_bytes,
    );
    assert!(
        hostile.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR,
        "[M2] hostile (GE+jitter) arm delivery {:.3} fell below its {M2_HOSTILE_DELIVERY_FLOOR} regression floor",
        hostile.summary.delivery_pct,
    );
    assert!(
        hostile.wire_x <= M2_HOSTILE_WIRE_GUARD_X,
        "[M2] hostile arm own-wire {:.2}x exceeds its {M2_HOSTILE_WIRE_GUARD_X}x regression guard",
        hostile.wire_x,
    );
    assert!(
        lone.summary.delivery_pct >= M2_LONE_DELIVERY_FLOOR,
        "[M2] hostile lone-tail arm delivery {:.3} fell below its {M2_LONE_DELIVERY_FLOOR} regression floor",
        lone.summary.delivery_pct,
    );
    assert!(
        lone.wire_x <= M2_LONE_WIRE_GUARD_X,
        "[M2] lone-tail own-wire {:.2}x exceeds its {M2_LONE_WIRE_GUARD_X}x regression guard: the known lone-tail wire defect has at least doubled",
        lone.wire_x,
    );
}

// ─────────────────────────────── M3: goodput ─────────────────────────────────

struct BulkRep {
    delivered_mib_s: f64,
    shaper_mib_s: f64,
    capacity_mib_s: f64,
    fraction: f64,
    delivered_bytes: u64,
    shaper_bytes: u64,
    elapsed: Duration,
}

fn m3_declaration() -> String {
    // The fraction floor is the configured-rate fraction; the goodput panel's
    // floor line is that same fraction expressed in MiB/s at the configured
    // capacity, so the reader sees both the raw rates and the ratio.
    let capacity_mib_s = M3_CAPACITY_BPS as f64 / 8.0 / (1024.0 * 1024.0);
    let floor_mib_s = capacity_mib_s * M3_CAPACITY_FRACTION;
    format!(
        r#"{{"mandate":"M3","title":"M3 bulk goodput vs the shaped clock and the configured link rate","x_label":"seed","y_label":"MiB/s","panels":[{{"id":"goodput","chart":"bar","series":[{{"name":"delivered"}},{{"name":"shaper_forwarded"}}],"bounds":[{{"y":{floor_mib_s:.6},"label":"M3 floor {M3_CAPACITY_FRACTION}x link rate"}}]}},{{"id":"fraction","chart":"bar","series":[{{"name":"fraction"}}],"bounds":[{{"y":{M3_CAPACITY_FRACTION},"label":"M3 floor {M3_CAPACITY_FRACTION}x link rate"}}]}}]}}"#
    )
}

fn m3_rows(reps: &[BulkRep]) -> Vec<(String, String, f64, f64)> {
    let mut rows = Vec::new();
    // Every series carries a point at every rep's x so the renderer groups the
    // two goodput bars side by side at each rep (its grouped-bar slot math
    // assumes a full matrix).
    for (index, rep) in reps.iter().enumerate() {
        let x = (index + 1) as f64;
        rows.push((
            "goodput".to_owned(),
            "delivered".to_owned(),
            x,
            rep.delivered_mib_s,
        ));
        rows.push((
            "goodput".to_owned(),
            "shaper_forwarded".to_owned(),
            x,
            rep.shaper_mib_s,
        ));
        rows.push((
            "fraction".to_owned(),
            "fraction".to_owned(),
            x,
            rep.fraction,
        ));
    }
    rows
}

/// One M3 rep: the production dual-lane composition with the bulk lane
/// saturated, sampled at both ends of the offered window while the pump still
/// runs. `delivered` is the sink's delta and `shaper_forwarded` the shaped
/// link's own forwarded delta over the same interval — the in-process
/// reference clock the within-run ratio is taken against.
async fn run_bulk_rep(window: Duration, starve: bool) -> BulkRep {
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    // A starved bulk lane: the shaper reference (the configured capacity) is
    // unchanged, so the achieved fraction collapses.
    let link_rate = if starve {
        M3_CAPACITY_BPS / 10
    } else {
        M3_CAPACITY_BPS
    };
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_sink, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(
                int_addr,
                link(41, OWD, JITTER, 0, 0),
                link(42, OWD, JITTER, 0, 0),
            )
            .unwrap();
            let bulk_pair = NetemPair::spawn(
                bulk_addr,
                link(43, OWD, JITTER, 0, link_rate),
                link(44, OWD, JITTER, 0, link_rate),
            )
            .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
            )
            .await
            .unwrap();

            let (mut lat_read, mut lat_write) = opener.open(LaneClass::Interactive).await.unwrap();
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
            let (mut bulk_read, mut bulk_write) = opener.open(LaneClass::Bulk).await.unwrap();
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
            // The interactive lane's light latency stream runs for the whole
            // rep so the topology is the true dual-lane one; its load is
            // negligible against the shaped bulk lane.
            let interactive = async {
                let _ = lat_write.write_all(b"L").await;
                send_timestamped_messages(
                    &mut lat_write,
                    base,
                    MSG_BYTES,
                    CADENCE,
                    BULK_RAMP + window + GRACE,
                )
                .await
            };
            let mut pump = tokio::task::JoinSet::new();
            let (pump_stop_tx, mut pump_stop_rx) = tokio::sync::watch::channel(false);
            pump.spawn(async move {
                let payload = cyclic_payload(64 * 1024 * 1024);
                let mut offset = 0usize;
                if bulk_write.write_all(b"B").await.is_err() {
                    return;
                }
                loop {
                    tokio::select! {
                        _ = pump_stop_rx.changed() => break,
                        result = bulk_write.write(&payload[offset..]) => match result {
                            Ok(0) => break,
                            Ok(n) => offset = (offset + n) % payload.len(),
                            Err(_) => break,
                        },
                    }
                }
            });

            tokio::time::sleep(BULK_RAMP).await;
            let window_start = Instant::now();
            let delivered_before = bulk_sink.load(Ordering::Relaxed);
            let forwarded_before = bulk_pair.stats_c2s().forwarded_bytes;
            tokio::select! {
                joined = pump.join_next(), if !pump.is_empty() => {
                    joined.expect("bulk pump exists").unwrap();
                    panic!("[M3] bulk pump ended before the measurement window completed");
                }
                _ = tokio::time::sleep(window) => {}
            }
            let elapsed = window_start.elapsed();
            let delivered = bulk_sink
                .load(Ordering::Relaxed)
                .saturating_sub(delivered_before);
            let forwarded = bulk_pair
                .stats_c2s()
                .forwarded_bytes
                .saturating_sub(forwarded_before);
            pump_stop_tx.send(true).unwrap();
            // Drain the window's stragglers before teardown; the drain is not
            // part of the measured interval (the clock runs while the sender
            // pumps).
            let _ = interactive.await;
            tokio::time::sleep(GRACE).await;
            while let Some(result) = pump.join_next().await {
                result.unwrap();
            }
            while latencies.try_recv().is_ok() {}
            let capacity_mib_s = M3_CAPACITY_BPS as f64 / 8.0 / (1024.0 * 1024.0);
            let secs = elapsed.as_secs_f64().max(f64::EPSILON);
            let delivered_mib_s = delivered as f64 / (1024.0 * 1024.0) / secs;
            let shaper_mib_s = forwarded as f64 / (1024.0 * 1024.0) / secs;
            int_pair.stop();
            bulk_pair.stop();
            BulkRep {
                delivered_mib_s,
                shaper_mib_s,
                capacity_mib_s,
                fraction: delivered_mib_s / capacity_mib_s,
                delivered_bytes: delivered,
                shaper_bytes: forwarded,
                elapsed,
            }
        })
        .await
}

/// Mandate 3: bulk goodput on the production dual-lane topology, as a
/// within-run fraction of the shaped clock / configured link rate, median of
/// three seeded reps, `>= 0.35x`.
#[tokio::test(flavor = "multi_thread")]
async fn m3_bulk_goodput_fraction() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let starve = fault("M3").as_deref() == Some("M3_starve");
    let window = bulk_window();
    let mut reps = Vec::new();
    for rep in 1..=M3_REPS {
        let label = format!("m3/rep{rep}");
        let result = with_timeout(
            Duration::from_secs(90),
            &label,
            run_bulk_rep(window, starve),
        )
        .await;
        eprintln!(
            "[mandate-smoke m3/rep{rep}] delivered {:.3} MiB/s over {:?}, shaper forwarded {:.3} MiB/s, capacity {:.3} MiB/s, fraction {:.3} ({} / {} bytes)",
            result.delivered_mib_s,
            result.elapsed,
            result.shaper_mib_s,
            result.capacity_mib_s,
            result.fraction,
            result.delivered_bytes,
            result.shaper_bytes,
        );
        reps.push(result);
    }
    let mut fractions: Vec<f64> = reps.iter().map(|r| r.fraction).collect();
    fractions.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = fractions[fractions.len() / 2];
    let delivered_median = {
        let mut v: Vec<f64> = reps.iter().map(|r| r.delivered_mib_s).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let shaper_median = {
        let mut v: Vec<f64> = reps.iter().map(|r| r.shaper_mib_s).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let wall: f64 = reps.iter().map(|r| r.elapsed.as_secs_f64()).sum::<f64>();
    write_evidence(&dir, "M3", &m3_declaration(), &m3_rows(&reps));

    let pass = median >= M3_CAPACITY_FRACTION;
    println!(
        "MANDATE M3 {} delivered_mib_s={:.3} shaper_mib_s={:.3} capacity_mib_s={:.3} fraction={:.3} floor={:.3} reps={} measured_s={:.1}",
        verdict(pass),
        delivered_median,
        shaper_median,
        reps[0].capacity_mib_s,
        median,
        M3_CAPACITY_FRACTION,
        M3_REPS,
        wall,
    );

    assert!(
        median >= M3_CAPACITY_FRACTION,
        "[M3] median bulk goodput fraction {median:.3} < the {M3_CAPACITY_FRACTION} floor (delivered {delivered_median:.3} MiB/s of the {:.3} MiB/s configured link rate; per-rep fractions {fractions:?}): the bulk lane must keep a high fraction of its link's capacity on the dual-lane topology",
        reps[0].capacity_mib_s,
    );
}

// ───────────────────────── M4: interactive lane fairness ─────────────────────
//
// M1 and M2 measure ONE interactive flow. A mandate result achieved by
// starving one of several flows sharing the interactive lane is not a pass, so
// M4 measures the *split* of the same production interactive lane across
// several flows offering the same payload at the same cadence: every flow must
// deliver what it is offered (no starvation), no flow's share of the lane's
// delivered bytes may depart from the equal share by more than the bound
// derived in `rtp_mux/GATE.md` (fair share), and no flow's p99 may depart from
// its peers' the way the share statistic cannot see. The per-flow latencies
// are also reported and drawn against M1's own ceiling, so a result that is
// fair and slow is visible; M1 stays the authority for that ceiling.
//
// The arm is the M1/M2 `clean` interactive lane with `M4_FLOWS` interactive
// streams on it instead of one, and the second arm is the M1/M2 `hostile`
// impairment with the same multi-flow offer — the same link, the same tagged-
// stream sink (`spawn_tagged_stream_sink` buckets every sample by the flow's
// first-byte tag as it already does for the two-interactive battery), the same
// `send_timestamped_messages` offer. The bulk lane is connected (the topology
// is the production dual-lane one) but carries no stream: M4 isolates the
// interactive lane's own split, which is the quantity the mandate-3 arms do
// not measure.

/// The interactive flows multiplexed on the one interactive lane. Four is the
/// smallest count that makes an unfair split a *share* rather than a binary
/// win/lose, and it is the count the existing 4-flow scaling probe uses, so a
/// skew seen here is comparable with that arm's per-flow floors.
const M4_FLOWS: usize = 4;

/// The per-flow delivery floor. Derived from M4's own measurement (GATE.md):
/// both arms delivered every offered message on every flow across the 29 runs
/// the bound is derived from, so the floor carries the same slack M2's hostile
/// floor uses -- a flow that loses more than ~0.5 % of its own offer is
/// starved, while ordinary tail-repair jitter never trips it.
const M4_DELIVERY_FLOOR: f64 = 0.995;

/// The fair-share imbalance bound: the worst flow's share of the lane's
/// delivered bytes may not depart from the equal share `1/M4_FLOWS` by more
/// than this fraction of the equal share. Derived from M4's own measurement
/// (GATE.md): across the 29 runs the bound is derived from, the worst
/// departure was `0.46 %` (clean arm; hostile `0.43 %`), while one
/// delivered frame is `1 / (4 x 2064) = 0.012 %` of the lane -- `0.048 %` of
/// the equal share -- so the observed skew is a handful of frames of
/// connection ramp at the window edges. The bound is `2.2x` the worst measured
/// departure, so a change that at least doubles the imbalance fails while
/// frame-edge ramp cannot reach it.
const M4_IMBALANCE_BOUND: f64 = 0.01;

/// The fair-latency bound: the worst flow's p99 may not exceed the best flow's
/// p99 by more than this factor, asserted on the **clean** arm. Derived from
/// M4's own measurement (GATE.md): over the 36 clean-arm runs the bound is
/// derived from (both windows) the worst spread was `1.20x`, so the bound is
/// `1.67x` the worst measured spread. It asserts the dimension the share
/// statistic cannot see: with equal offers and per-flow delivery at 1.000, a
/// scheduler that favours one flow's *ordering* rather than its goodput would
/// show up here and not in the shares. It is deliberately **not** asserted on
/// the hostile arm, where the per-flow p99 differences are a GE loss
/// realization rather than a scheduler property: that arm measured a spread of
/// up to `2.93x` across 10 runs, so an asserted spread there would measure
/// which flow caught the burst. The hostile arm keeps M1's absolute guard.
const M4_LATENCY_SPREAD_BOUND: f64 = 2.0;

/// The per-flow tag byte, the same A/L/C/D convention the multi-flow scaling
/// probe uses (`b'B'` is the reserved bulk-sink tag, so it is skipped). The
/// server's tagged sink routes every interactive-lane stream through its
/// latency parser and labels each sample with the stream's tag, which is what
/// makes per-flow attribution possible.
fn m4_flow_tag(flow: usize) -> u8 {
    match flow {
        0 => b'A',
        1 => b'L',
        _ => b'A' + flow as u8,
    }
}

/// One M4 arm: the production interactive lane, `M4_FLOWS` flows offering the
/// same payload at the same cadence, and the impairment the clean/hostile arms
/// already use.
struct FairArmSpec {
    name: &'static str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    window: Duration,
    /// The preferential-service fault injection: every flow but the first
    /// starts offering this long into the window (zero in every real run).
    stagger: Duration,
}

/// One flow's measured outcome: what it offered, what the lane delivered for
/// it, its share of the lane's delivered bytes, and its latency summary.
struct FlowSample {
    tag: u8,
    sent: u64,
    received: u64,
    offered_bytes: u64,
    delivered_bytes: u64,
    share: f64,
    summary: HolSummary,
}

/// One M4 arm's outcome, plus the aggregate statistic the fair-share bound is
/// asserted on.
struct FairRun {
    name: &'static str,
    flows: Vec<FlowSample>,
    ideal_share: f64,
    min_share: f64,
    max_share: f64,
    /// The worst flow's relative departure from the equal share:
    /// `max_i |share_i - 1/N| / (1/N)`. Zero means every flow received exactly
    /// its equal share of the lane's delivered bytes; it is the statistic the
    /// fair-share bound is derived from.
    imbalance: f64,
    window: Duration,
    wall: Duration,
}

/// The fairness window. The share statistic's resolution is one delivered
/// frame: `1 / (M4_FLOWS x frames-per-flow)` of the lane, and the flows are
/// opened in sequence and each runs its own 5 ms interval, so their frame
/// counts differ by a few frames of connection ramp. At the arms' 12 s window
/// that structural skew measured under 0.5 %, but at a 4 s window it reached
/// the 1 % bound on a handful of frames alone, so M4's quick window is longer
/// than the other mandates' (still well under the full 12 s).
const M4_QUICK_WINDOW: Duration = Duration::from_secs(8);

fn fairness_window() -> Duration {
    if quick() { M4_QUICK_WINDOW } else { WINDOW }
}

/// The M4 arm set. `clean` is the mandate arm (M1/M2's clean interactive link)
/// and carries the fault injection when one is selected; `hostile` is the
/// regression-guard arm (M1/M2's GE `5 %`/mean-8 + 100 ms-jitter link).
fn fairness_arms(mandate: &str) -> Vec<FairArmSpec> {
    let clean_fault = fault(mandate);
    let mut clean_c2s = link(41, OWD, JITTER, LOSS_2, 0);
    let mut clean_s2c = link(42, OWD, JITTER, LOSS_2, 0);
    let mut stagger = Duration::ZERO;
    if let Some(fault) = clean_fault.as_deref() {
        match fault {
            // Serve one flow preferentially: flows 1.. offer only the second
            // half of the window, so the lane's delivered bytes concentrate on
            // flow 0 and the fair-share bound must fail while every flow still
            // delivers everything it offers.
            "M4_starve" => stagger = fairness_window() / 2,
            // Collapse every flow's delivery: the per-flow delivery floor must
            // fail naming M4.
            "M4_drop" => {
                clean_c2s.loss = loss_pct(99);
                clean_s2c.loss = loss_pct(99);
            }
            _ => {}
        }
    }
    vec![
        FairArmSpec {
            name: "clean",
            int_c2s: clean_c2s,
            int_s2c: clean_s2c,
            window: fairness_window(),
            stagger,
        },
        FairArmSpec {
            name: "hostile",
            int_c2s: hostile_link(41),
            int_s2c: hostile_link(42),
            window: fairness_window(),
            stagger: Duration::ZERO,
        },
    ]
}

/// Run one fairness arm: `M4_FLOWS` interactive streams on ONE interactive
/// lane, all tagged, all offered the same `MSG_BYTES` payload at `CADENCE` for
/// `window`, all drained by one collector that buckets the tagged sink's
/// samples per flow.
async fn run_fairness_arm(spec: FairArmSpec) -> FairRun {
    let FairArmSpec {
        name,
        int_c2s,
        int_s2c,
        window,
        stagger,
    } = spec;
    let wall = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let outcome = tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, _bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            // The bulk lane is connected (the production topology pairs both
            // lanes at connect) but never opened: M4 measures the interactive
            // lane's own split.
            let bulk_pair =
                NetemPair::spawn(bulk_addr, NetemConfig::default(), NetemConfig::default())
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
            )
            .await
            .unwrap();

            // One collector drains the shared tagged channel for the whole arm,
            // keeping `(tag, elapsed, latency)` so each sample is attributable
            // to its flow: the sink's channel is bounded and a lane carrying N
            // flows produces N times the sample rate.
            let collector = Arc::new(Mutex::new(Vec::<(u8, f64, f64)>::new()));
            let collector_sink = Arc::clone(&collector);
            let task_tx_collector = task_tx.clone();
            submit_test_task(
                &task_tx_collector,
                Box::pin(async move {
                    while let Some((tag, latency)) = latencies.recv().await {
                        collector_sink.lock().unwrap().push((
                            tag,
                            base.elapsed().as_secs_f64(),
                            latency,
                        ));
                    }
                }),
            );

            let mut streams = Vec::with_capacity(M4_FLOWS);
            for flow in 0..M4_FLOWS {
                let (mut read, write) = opener.open(LaneClass::Interactive).await.unwrap();
                // Parked until the streams close; the owning scope aborts them.
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
                streams.push((m4_flow_tag(flow), write));
            }

            // Tag first, then offer every flow *concurrently*: the arm is N
            // flows multiplexed on one lane, not N sequential sweeps. In every
            // real run `delay` is zero and every flow offers for the whole
            // window; the preferential-service fault gives flow 0 the whole
            // window while the rest offer only its second half, so the lane's
            // delivered bytes concentrate on flow 0.
            let mut futs = Vec::with_capacity(M4_FLOWS);
            for (index, (tag, write)) in streams.iter_mut().enumerate() {
                if write.write_all(&[*tag]).await.is_err() {
                    return (vec![0u64; M4_FLOWS], Vec::new());
                }
                let delay = if index == 0 { Duration::ZERO } else { stagger };
                let run_for = window.saturating_sub(delay);
                let write = &mut *write;
                futs.push(async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    send_timestamped_messages(write, base, MSG_BYTES, CADENCE, run_for).await
                });
            }
            let sent_per_flow: Vec<u64> = join_all(futs).await;
            for (_, write) in streams.iter_mut() {
                let _ = write.shutdown();
            }

            tokio::time::sleep(GRACE).await;
            let collected = std::mem::take(&mut *collector.lock().unwrap());
            int_pair.stop();
            bulk_pair.stop();
            (sent_per_flow, collected)
        })
        .await;
    let (sent_per_flow, collected) = outcome;

    let mut per_flow_samples: Vec<Vec<f64>> = vec![Vec::new(); M4_FLOWS];
    for (tag, _elapsed, latency) in collected {
        if let Some(flow) = (0..M4_FLOWS).find(|&i| m4_flow_tag(i) == tag) {
            per_flow_samples[flow].push(latency);
        }
    }
    let delivered: Vec<u64> = per_flow_samples
        .iter()
        .map(|samples| samples.len() as u64 * MSG_BYTES as u64)
        .collect();
    let total_delivered: u64 = delivered.iter().sum();
    let ideal_share = 1.0 / M4_FLOWS as f64;
    let mut flows = Vec::with_capacity(M4_FLOWS);
    for flow in 0..M4_FLOWS {
        let sent = sent_per_flow[flow];
        let received = per_flow_samples[flow].len() as u64;
        let offered_bytes = sent.saturating_mul(MSG_BYTES as u64);
        let share = if total_delivered == 0 {
            0.0
        } else {
            delivered[flow] as f64 / total_delivered as f64
        };
        let summary = summarize(per_flow_samples[flow].clone(), sent, received, 0, 0.0);
        flows.push(FlowSample {
            tag: m4_flow_tag(flow),
            sent,
            received,
            offered_bytes,
            delivered_bytes: delivered[flow],
            share,
            summary,
        });
    }
    let min_share = flows.iter().map(|f| f.share).fold(f64::INFINITY, f64::min);
    let max_share = flows.iter().map(|f| f.share).fold(0.0, f64::max);
    let imbalance = flows
        .iter()
        .map(|f| ((f.share - ideal_share) / ideal_share).abs())
        .fold(0.0, f64::max);
    FairRun {
        name,
        flows,
        ideal_share,
        min_share,
        max_share,
        imbalance,
        window,
        wall: wall.elapsed(),
    }
}

fn print_fair_arm(run: &FairRun) {
    for flow in &run.flows {
        eprintln!(
            "[mandate-smoke m4/{name} flow {tag}] sent={sent:>5} recv={recv:>5} \
             delivery={del:.3} share={share:.4} offered={offered:>8}B delivered={delivered:>8}B \
             p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} max={max:8.1}",
            name = run.name,
            tag = flow.tag as char,
            sent = flow.sent,
            recv = flow.received,
            del = flow.summary.delivery_pct,
            share = flow.share,
            offered = flow.offered_bytes,
            delivered = flow.delivered_bytes,
            p50 = flow.summary.p50,
            p90 = flow.summary.p90,
            p99 = flow.summary.p99,
            max = flow.summary.max,
        );
    }
    eprintln!(
        "[mandate-smoke m4/{name}] ideal_share={ideal:.4} min_share={min:.4} max_share={max:+.4} \
         imbalance={imbalance:.4} window={window:?} wall={wall:.1}s",
        name = run.name,
        ideal = run.ideal_share,
        min = run.min_share,
        max = run.max_share,
        imbalance = run.imbalance,
        window = run.window,
        wall = run.wall.as_secs_f64(),
    );
}

// ────────────────────────── M4: evidence writing ─────────────────────────────

fn m4_declaration() -> String {
    let ideal = 1.0 / M4_FLOWS as f64;
    let ideal_pct = ideal * 100.0;
    let bound_pct = M4_IMBALANCE_BOUND * 100.0;
    let bound = M4_IMBALANCE_BOUND;
    let flows = M4_FLOWS;
    let delivery_floor = M4_DELIVERY_FLOOR;
    let ceiling = M1_CEILING_MS;
    // The fair-share line is drawn on the share panel; the floor is the same
    // line pulled in by the imbalance bound, so drawing both there overprints
    // two labels one percent apart. The floor is drawn instead on the imbalance
    // panel, whose axis is the deviation itself, where the two bounds and every
    // flow's departure are legible.
    format!(
        r#"{{"mandate":"M4","title":"M4 interactive lane fairness: {flows} flows on one interactive lane","x_label":"flow (1..{flows})","y_label":"share of the lane's delivered bytes","panels":[{{"id":"shares","chart":"bar","series":[{{"name":"clean"}},{{"name":"hostile"}}],"bounds":[{{"y":{ideal:.6},"label":"fair share {ideal_pct:.1}%"}}]}},{{"id":"imbalance","chart":"bar","y_label":"departure from the fair share","x_label":"flow (1..{flows})","series":[{{"name":"clean"}},{{"name":"hostile"}}],"bounds":[{{"y":{bound},"label":"fair-share bound \u00b1{bound_pct:.1}%"}}]}},{{"id":"delivery","chart":"bar","y_label":"delivery (received / offered)","series":[{{"name":"clean"}},{{"name":"hostile"}}],"bounds":[{{"y":{delivery_floor},"label":"M4 per-flow delivery floor {delivery_floor}"}}]}},{{"id":"latency","chart":"bar","y_label":"latency (ms)","series":[{{"name":"clean_p50"}},{{"name":"clean_p99"}},{{"name":"hostile_p50"}},{{"name":"hostile_p99"}}],"bounds":[{{"y":{ceiling},"label":"M1 ceiling {ceiling} ms"}}]}}]}}"#
    )
}

fn m4_rows(runs: &[FairRun]) -> Vec<(String, String, f64, f64)> {
    let mut rows = Vec::new();
    for run in runs {
        for (index, flow) in run.flows.iter().enumerate() {
            let x = (index + 1) as f64;
            rows.push(("shares".to_owned(), run.name.to_owned(), x, flow.share));
            rows.push((
                "imbalance".to_owned(),
                run.name.to_owned(),
                x,
                (flow.share - run.ideal_share) / run.ideal_share,
            ));
            rows.push((
                "delivery".to_owned(),
                run.name.to_owned(),
                x,
                flow.summary.delivery_pct,
            ));
            rows.push((
                "latency".to_owned(),
                format!("{}_p50", run.name),
                x,
                flow.summary.p50,
            ));
            rows.push((
                "latency".to_owned(),
                format!("{}_p99", run.name),
                x,
                flow.summary.p99,
            ));
        }
    }
    rows
}

/// Mandate 4: the interactive lane's split across several flows. Every flow
/// must deliver what it is offered (no starvation), no flow's share of the
/// lane's delivered bytes may depart from the equal share by more than
/// [`M4_IMBALANCE_BOUND`] (fair share), the clean arm's worst flow's p99 may
/// not exceed the best flow's p99 by more than [`M4_LATENCY_SPREAD_BOUND`]
/// (fair latency), and no hostile-arm flow's p99 may cross M1's hostile guard. The absolute
/// interactive ceiling is **not** re-asserted per flow here: the 4-flow arm
/// measures p99 179-231 ms, 0.72-0.92 of M1's 250 ms ceiling, so an absolute
/// per-flow assertion would sit within 1.1x of the arm's own measurement and
/// fire on host noise. M1 owns the ceiling, the M4 latency panel draws it, and
/// the `MANDATE M4` line reports `clean_p99_max` -- which is what makes a
/// multi-flow latency regression visible.
#[tokio::test(flavor = "multi_thread")]
async fn m4_interactive_lane_fairness() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let arms = fairness_arms("M4");
    let mut runs = Vec::new();
    for spec in arms {
        let label = format!("m4/{}", spec.name);
        let run = with_timeout(Duration::from_secs(120), &label, run_fairness_arm(spec)).await;
        print_fair_arm(&run);
        runs.push(run);
    }
    write_evidence(&dir, "M4", &m4_declaration(), &m4_rows(&runs));

    let clean = &runs[0];
    let hostile = &runs[1];
    let delivery_floor_of = |run: &FairRun| {
        run.flows
            .iter()
            .map(|f| f.summary.delivery_pct)
            .fold(f64::INFINITY, f64::min)
    };
    let p99_floor_of = |run: &FairRun| {
        run.flows
            .iter()
            .map(|f| f.summary.p99)
            .fold(f64::INFINITY, f64::min)
    };
    let p99_ceiling_of =
        |run: &FairRun| run.flows.iter().map(|f| f.summary.p99).fold(0.0, f64::max);
    let p50_ceiling_of =
        |run: &FairRun| run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max);
    let clean_floor = delivery_floor_of(clean);
    let hostile_floor = delivery_floor_of(hostile);
    // A flow that delivered nothing has no p99, so the spread is undefined
    // (`summarize` yields NaN). Report 0 instead of NaN so the verdict line
    // stays machine-parseable: the per-flow delivery floor is what names such a
    // run, and it fires before this statistic is even reached.
    let spread_of = |run: &FairRun| {
        let floor = p99_floor_of(run);
        let ceiling = p99_ceiling_of(run);
        if floor.is_finite() && floor > 0.0 {
            ceiling / floor
        } else {
            0.0
        }
    };
    let clean_spread = spread_of(clean);
    let hostile_spread = spread_of(hostile);
    let clean_p99_max = p99_ceiling_of(clean);
    let clean_p50_max = p50_ceiling_of(clean);
    let hostile_p99_max = p99_ceiling_of(hostile);
    let wall = clean.wall.as_secs_f64() + hostile.wall.as_secs_f64();
    let pass = clean_floor >= M4_DELIVERY_FLOOR
        && hostile_floor >= M4_DELIVERY_FLOOR
        && clean.imbalance <= M4_IMBALANCE_BOUND
        && hostile.imbalance <= M4_IMBALANCE_BOUND
        && clean_spread <= M4_LATENCY_SPREAD_BOUND
        && hostile_p99_max <= M1_HOSTILE_P99_GUARD_MS;
    println!(
        "MANDATE M4 {} flows={} clean_delivery_min={:.3} hostile_delivery_min={:.3} clean_share_min={:.4} clean_share_max={:.4} hostile_share_min={:.4} hostile_share_max={:.4} clean_imbalance={:.4} hostile_imbalance={:.4} imbalance_bound={:.3} fair_share={:.4} delivery_floor={:.3} clean_p99_spread={:.3} hostile_p99_spread={:.3} spread_bound={:.1} clean_p50_max={:.1} clean_p99_max={:.1} hostile_p99_max={:.1} ceiling={:.1} hostile_p99_guard={:.1} window_s={:.1} wall_s={:.1}",
        verdict(pass),
        M4_FLOWS,
        clean_floor,
        hostile_floor,
        clean.min_share,
        clean.max_share,
        hostile.min_share,
        hostile.max_share,
        clean.imbalance,
        hostile.imbalance,
        M4_IMBALANCE_BOUND,
        clean.ideal_share,
        M4_DELIVERY_FLOOR,
        clean_spread,
        hostile_spread,
        M4_LATENCY_SPREAD_BOUND,
        clean_p50_max,
        clean_p99_max,
        hostile_p99_max,
        M1_CEILING_MS,
        M1_HOSTILE_P99_GUARD_MS,
        clean.window.as_secs_f64(),
        wall,
    );

    for (index, flow) in clean.flows.iter().enumerate() {
        assert!(
            flow.summary.delivery_pct >= M4_DELIVERY_FLOOR,
            "[M4] clean-arm flow {} (tag {}) delivered {}/{} messages ({:.3} < the {M4_DELIVERY_FLOOR} floor): a flow sharing the interactive lane was starved of what it offered",
            index + 1,
            flow.tag as char,
            flow.received,
            flow.sent,
            flow.summary.delivery_pct,
        );
    }
    for (index, flow) in hostile.flows.iter().enumerate() {
        assert!(
            flow.summary.delivery_pct >= M4_DELIVERY_FLOOR,
            "[M4] hostile-arm flow {} (tag {}) delivered {}/{} messages ({:.3} < the {M4_DELIVERY_FLOOR} floor): a flow sharing the interactive lane was starved of what it offered under the hostile impairment",
            index + 1,
            flow.tag as char,
            flow.received,
            flow.sent,
            flow.summary.delivery_pct,
        );
    }
    assert!(
        clean.imbalance <= M4_IMBALANCE_BOUND,
        "[M4] clean-arm fair-share breach: the worst flow's share of the lane's delivered bytes departs {:.4} from the equal share {:.4} (shares {:?}), over the {M4_IMBALANCE_BOUND} bound -- one flow is being served preferentially on the shared interactive lane",
        clean.imbalance,
        clean.ideal_share,
        clean.flows.iter().map(|f| f.share).collect::<Vec<_>>(),
    );
    assert!(
        hostile.imbalance <= M4_IMBALANCE_BOUND,
        "[M4] hostile-arm fair-share breach: the worst flow's share of the lane's delivered bytes departs {:.4} from the equal share {:.4} (shares {:?}), over the {M4_IMBALANCE_BOUND} bound -- one flow is being served preferentially on the shared interactive lane under the hostile impairment",
        hostile.imbalance,
        hostile.ideal_share,
        hostile.flows.iter().map(|f| f.share).collect::<Vec<_>>(),
    );
    for (index, flow) in clean.flows.iter().enumerate() {
        assert!(
            flow.summary.p99 <= p99_floor_of(clean) * M4_LATENCY_SPREAD_BOUND,
            "[M4] clean-arm flow {} (tag {}) p99 {:.1} ms is more than {M4_LATENCY_SPREAD_BOUND}x the best flow's p99 {:.1} ms (p50 {:.1}, max {:.1}), over the fair-latency bound -- one flow's tail is being served preferentially",
            index + 1,
            flow.tag as char,
            flow.summary.p99,
            p99_floor_of(clean),
            flow.summary.p50,
            flow.summary.max,
        );
    }
    for (index, flow) in hostile.flows.iter().enumerate() {
        assert!(
            flow.summary.p99 <= M1_HOSTILE_P99_GUARD_MS,
            "[M4] hostile-arm flow {} (tag {}) p99 {:.1} ms exceeds M1's {M1_HOSTILE_P99_GUARD_MS} ms hostile guard (p50 {:.1}, max {:.1}): the known hostile tail defect has at least doubled with several flows on the lane",
            index + 1,
            flow.tag as char,
            flow.summary.p99,
            flow.summary.p50,
            flow.summary.max,
        );
    }
}
