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

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
/// `M1_latency`, `M2_wire`, `M2_delivery` or `M3_starve`. Unset in every real
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
    eprintln!(
        "[mandate-smoke {name:<9}] sent={sent:>5} recv={recv:>5} delivery={del:.3} \
         p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} p999={p999:7.1} max={max:8.1} \
         over250={o25:>4} wire={w:>10}B x={x:.2} bulk_sink={bs:>10}B bulk_wire={bw:>10}B wall={wall:.1}s window={win:?}",
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
    let arms = mandate_arms("M1");
    let mut runs = Vec::new();
    for spec in arms {
        let label = format!("m1/{}", spec.name);
        let run = with_timeout(Duration::from_secs(120), &label, run_arm(spec)).await;
        print_arm(&run);
        runs.push(run);
    }
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
    let arms = mandate_arms("M2");
    let mut runs = Vec::new();
    for spec in arms {
        let label = format!("m2/{}", spec.name);
        let run = with_timeout(Duration::from_secs(120), &label, run_arm(spec)).await;
        print_arm(&run);
        runs.push(run);
    }
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
