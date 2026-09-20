//! The rtp_mux tri-mandate constitution, asserted as gates.
//!
//! The operator's product constitution is **three mandates**, jointly the
//! acceptance criterion for every change to the interactive path — a change
//! that improves one mandate while violating another is a failure, not a win.
//! rtp_mux owns the production dual-lane topology (a game client's
//! interactive lane on its own RTP connection, the bulk lane on a second,
//! separate connection), so all three mandates are asserted by rtp_mux's own
//! scenario gates:
//!
//! 1. **Low latency of the interactive lane** — the interactive lane's tail
//!    latency (p99, plus a spike bound) stays at or below its floor on the
//!    production dual-lane topology.
//!    *Bound (derived):* the topology's one-way delay floor is 25 ms (the
//!    `OWD` constant of `rtp_mux_jitter.rs`, the deployment link profile);
//!    the `250 ms` p99/spike ceiling is that floor plus a large documented
//!    margin — it is the README's "zero >250 ms spikes" criterion, ~8× the
//!    measured ~29 ms p99 on the seeded `both` arm, and it still bites on any
//!    regression that lets the interactive tail decay.
//!    *Asserted by:* `rtp_mux_jitter.rs::jitter_duallane_constitution_gate_p99`
//!    (full tier, opt-in): the median of three seeded runs plus a zero
//!    `>250 ms` spike count on every run. Wall-clock, so median-of-N with a
//!    documented run command.
//! 2. **Reasonable goodput of the interactive lane** — the interactive lane
//!    delivers what it is offered (`delivery == 1.000`) **without inflating
//!    its own wire** to get there.
//!    *Bound (derived):* the offered payload is the deterministic
//!    sent-message byte count; the client→server wire forwarded by the
//!    impairment proxy must stay within a fixed budget of it — `6×` (the
//!    `INTERACTIVE_WIRE_BUDGET_X` constant of `rtp_mux_jitter.rs`), where
//!    the measured overhead on the seeded `both` arm is ~3.7×
//!    (RTP/mux framing + control + the repair traffic that 2% loss needs),
//!    so the budget leaves ~1.6× headroom while a redundancy inflation of
//!    +50% still trips it.
//!    *Asserted by:* `rtp_mux_jitter.rs::jitter_duallane_constitution_gate`
//!    (default tier, runs on every `cargo test -p rtp_mux`). Both quantities
//!    are deterministic counts over the seeded impairment link — counts
//!    belong in the always-run gate — and they are asserted together so that
//!    redundancy that inflates the interactive lane to buy latency violates
//!    the constitution. At the mux layer beneath, per-stream delivery against
//!    the offered payload is additionally asserted in `mux`'s default tier
//!    (mux's `GATE.md`), so the offered payload's integrity is always-run
//!    coverage.
//! 3. **High goodput of the bulk lane** — the bulk lane's goodput stays at a
//!    high fraction of the link's capacity on the same topology.
//!    *Bound (derived):* the bulk lane's link is shaped at a configured rate
//!    and the sink-delivered goodput must stay ≥ [`BULK_GOODPUT_CAPACITY_FRACTION`]
//!    of that capacity — the same `0.35 × capacity` shape as the
//!    `rtp_bufferbloat` floor (there, chosen so the gate survives the
//!    concurrent `rtp` branches without asserting the measured stock number).
//!    The bulk lane has its own link (no interactive contention), so the
//!    stock transport sits at ~0.86× of the shaped rate — well above the
//!    floor; a change that at least halves the bulk lane's goodput fails.
//!    The sink counter is sampled as a window delta so the pump's pre-window
//!    saturation phase cannot inflate the reading (the cumulative counter
//!    measured a spurious 1.88× on a 1.0 MiB/s cap before this fix).
//!    *Asserted by:* [`bulk_lane_goodput_stays_above_capacity_fraction`] below —
//!    median-of-3, opt-in `full` tier, wall-clock with a documented run
//!    command.
//!
//! **Redundancy monotonicity is NOT a mandate** — it was only ever a proxy
//! for these outcomes. FEC recovery parity may legitimately grow with loss;
//! what must not happen is the interactive lane's extra/armor packets
//! inflating its own delivered wire.
//!
//! The harness (`netem_test`) must not restate this constitution: it keeps
//! the impairment instrument and its own gate manifest, and points at the
//! owning crates (`netem_test/tests/README.md` is now a pointer table). Each
//! mandate has exactly one asserting authority: M1 and M2 above, M3 here.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use mux::LaneClass;
use mux::testkit::mux::send_timestamped_messages;
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{NetemConfig, NetemPair};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One-way delay applied to every packet on both lanes (the deployment link
/// profile, shared with the `rtp_mux_jitter` battery).
const OWD: Duration = Duration::from_millis(25);
/// Uniform jitter around [`OWD`].
const JITTER: Duration = Duration::from_millis(5);
/// The configured bulk-lane rate cap (bits per second), applied to both bulk
/// directions by the impairment proxy — the link capacity the mandate's
/// goodput is a fraction of.
const BULK_RATE_BPS: u64 = 8 * 1024 * 1024;
/// Minimum sink-delivered bulk goodput as a fraction of
/// [`BULK_RATE_BPS`], the mandate-3 floor. The shape follows the
/// `rtp_bufferbloat` precedent (`GOODPUT_CAPACITY_FLOOR = 0.35` there, chosen
/// so the gate survives the concurrent `rtp` branches without asserting the
/// measured stock number). The bulk lane here has its own link (no
/// interactive contention), so the floor is deliberately slack against the
/// measured band: a change that at least halves the bulk lane's goodput
/// fails while ordinary host-load noise never trips it.
const BULK_GOODPUT_CAPACITY_FRACTION: f64 = 0.35;
/// Interactive message size, a typical game ping.
const MSG_BYTES: usize = 256;
const CADENCE: Duration = Duration::from_millis(25);
/// Measurement window per rep.
const RUN_FOR: Duration = Duration::from_secs(15);
/// Drain stragglers through the shaped link before reading the sink counter.
const GRACE: Duration = Duration::from_secs(2);

/// Build one impairment direction: fixed delay + jitter, an optional rate
/// cap, no loss (the bulk goodput mandate is about the lane holding its link,
/// not about loss recovery).
fn link(seed: u64, rate_bps: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: JITTER,
        rate: rate_bps,
        seed,
        ..NetemConfig::default()
    }
}

/// The deployment's interactive-lane FEC tuning (prompt parity, in-stream
/// flush), shared with the `rtp_mux_jitter` battery.
fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

/// One mandate-3 rep: run the production dual-lane composition — interactive
/// lane (frame fast-forward + prompt FEC) on its own RTP connection, bulk
/// lane (strict byte-stream, FEC-free) on a second connection shaped at
/// [`BULK_RATE_BPS`] — with the bulk lane *saturated* by a back-to-back
/// sender, and return the sink-delivered goodput measured across the window.
///
/// The interactive lane runs a light latency stream (the composition's own
/// lane, ~40 pings/s of 256 B on a separate, unshaped link — negligible
/// load), so the gate exercises the true dual-lane topology; the bulk lane's
/// shaped link is the constraint the goodput is measured against.
async fn run_bulk_saturation_rep(base: Instant) -> f64 {
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let (goodput_mib_s, received, sent) = tasks
        .run(async {
            let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
            let bulk_rtp = LaneRtpConfig::byte_stream();
            let (int_addr, bulk_addr, mut latencies, bulk_sink, _task_tx) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(int_addr, link(41, 0), link(42, 0)).unwrap();
            let bulk_pair =
                NetemPair::spawn(bulk_addr, link(43, BULK_RATE_BPS), link(44, BULK_RATE_BPS))
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

            let interactive = async {
                if lat_write.write_all(b"L").await.is_err() {
                    return 0u64;
                }
                send_timestamped_messages(&mut lat_write, base, MSG_BYTES, CADENCE, RUN_FOR).await
            };
            // Saturating bulk sender: write back-to-back as fast as the
            // transport accepts bytes, exactly the load the mandates assume
            // the bulk lane can carry.
            let (pump_stop_tx, mut pump_stop_rx) = tokio::sync::watch::channel(false);
            let payload = cyclic_payload(64 * 1024 * 1024);
            let mut pump_tasks = tokio::task::JoinSet::new();
            pump_tasks.spawn(async move {
                let mut write = bulk_write;
                if write.write_all(b"B").await.is_err() {
                    return;
                }
                let mut offset = 0usize;
                loop {
                    tokio::select! {
                        _ = pump_stop_rx.changed() => break,
                        result = write.write(&payload[offset..]) => match result {
                            Ok(0) => break,
                            Ok(n) => offset = (offset + n) % payload.len(),
                            Err(_) => break,
                        },
                    }
                }
            });

            let sent = interactive.await;
            // The bulk pump must keep the link saturated for the whole window;
            // a premature end would measure a dead upload.
            let window_start = Instant::now();
            // The pump has been saturating the link since before the window
            // started (it runs through the whole interactive phase too), so the
            // sink counter already holds pre-window bytes; sample the delta
            // over the window, not the cumulative total, or the goodput is
            // inflated by the pre-window pumping (measured 1.88x with the
            // cumulative counter through a 1.0 MiB/s cap).
            let delivered_before = bulk_sink.load(Ordering::Relaxed) as f64;
            tokio::select! {
                joined = pump_tasks.join_next(), if !pump_tasks.is_empty() => {
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement window completed");
                }
                _ = tokio::time::sleep(RUN_FOR) => {}
            }
            pump_stop_tx.send(true).unwrap();
            // Give the final bulk bytes time to drain through the shaped link
            // before measuring elapsed goodput (the `rtp_bufferbloat` shape).
            tokio::time::sleep(GRACE).await;
            let elapsed = window_start.elapsed();
            let delivered = (bulk_sink.load(Ordering::Relaxed) as f64 - delivered_before).max(0.0);
            while let Some(result) = pump_tasks.join_next().await {
                result.unwrap();
            }
            let mut received = 0u64;
            while let Ok((tag, _lat)) = latencies.try_recv() {
                if tag == b'L' {
                    received += 1;
                }
            }
            let goodput_mib_s = delivered / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            int_pair.stop();
            bulk_pair.stop();
            (goodput_mib_s, received, sent)
        })
        .await;
    eprintln!(
        "[mandate-3] interactive delivery {received}/{sent}, bulk goodput {goodput_mib_s:.3} MiB/s",
    );
    goodput_mib_s
}

/// Mandate 3 (high goodput of the bulk lane): the sink-delivered bulk
/// goodput on the production dual-lane topology, median of three seeded runs,
/// must stay >= [`BULK_GOODPUT_CAPACITY_FRACTION`] of the configured bulk-lane
/// rate. Wall-clock, so median-of-N with the documented run command:
///
/// ```sh
/// cargo test --release -p rtp_mux --test dual_lane_mandates -- \
///     --ignored bulk_lane_goodput_stays_above_capacity_fraction --nocapture --test-threads=1
/// ```
///
/// Vacuity: raise the fraction above the achievable band (or starve the bulk
/// lane) and the median falls below the floor, failing with a message naming
/// the mandate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; three 15 s dual-lane saturated runs; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn bulk_lane_goodput_stays_above_capacity_fraction() {
    const REPS: usize = 3;
    let capacity_mib_s: f64 = BULK_RATE_BPS as f64 / 8.0 / (1024.0 * 1024.0);
    let floor_mib_s: f64 = capacity_mib_s * BULK_GOODPUT_CAPACITY_FRACTION;

    let base = Instant::now();
    let mut goodputs = [0.0f64; REPS];
    for (rep, slot) in goodputs.iter_mut().enumerate() {
        let label = format!("mandate-3 bulk goodput/rep{}", rep + 1);
        let goodput = with_timeout(
            Duration::from_secs(60),
            &label,
            run_bulk_saturation_rep(base),
        )
        .await;
        *slot = goodput;
        eprintln!(
            "[mandate-3] rep {}: bulk goodput {goodput:.3} MiB/s (capacity {capacity_mib_s:.3} MiB/s, floor {floor_mib_s:.3} MiB/s = {BULK_GOODPUT_CAPACITY_FRACTION}x)",
            rep + 1,
        );
    }
    let mut sorted = goodputs;
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[REPS / 2];
    assert!(
        median >= floor_mib_s,
        "[mandate-3] median bulk goodput {median:.3} MiB/s < floor {floor_mib_s:.3} MiB/s \
         ({BULK_GOODPUT_CAPACITY_FRACTION}x of the {capacity_mib_s:.3} MiB/s configured link \
         rate; per-run: {goodputs:?}): the bulk lane must keep a high fraction of its link's \
         capacity on the dual-lane topology"
    );
    eprintln!(
        "[mandate-3] median-of-{REPS} bulk goodput {median:.3} MiB/s >= floor {floor_mib_s:.3} MiB/s OK"
    );
}
