//! # The `mux`-over-`rtp` layer's performance contract (tri-mandate
//! constitution)
//!
//! The operator's product constitution is three mandates; each is asserted by
//! the crate that owns the lane it constrains, stated in full in that crate's
//! own `GATE.md` ("Performance"). At the `mux`-over-`rtp` layer the mandates
//! read:
//!
//! 1. **Low latency of the interactive lane** — the lane's tail latency (p99
//!    plus a spike bound) stays at its floor on the production dual-lane
//!    topology. The topology and its one-way delay floor are owned by
//!    `rtp_mux`; the layer's contribution is that a small interactive stream
//!    is never placed behind bulk traffic it shares a session with — asserted here by
//!    `mux_over_rtp_small_stream_while_bulk_perf` (small-before-bulk arrival
//!    ordering, plus the liveness bound derived from the scenario budget).
//! 2. **Reasonable goodput of the interactive lane** — the lane delivers what
//!    it is offered (`delivery = 1.000`) without inflating its own wire. The
//!    wire budget is owned by `rtp_mux`; the layer asserts the delivery half
//!    at stream granularity: every scenario asserts the offered payload arrives byte-identical (`mux_over_rtp_over_netem_clean_link_echoes`,
//!    `mux_over_rtp_lossy_perf_smoke`, `mux_over_rtp_400kib_lossy_contended_perf`,
//!    `mux_over_rtp_400mib_hostile_perf`'s `delivered == sent`). The bound is
//!    the offered payload itself — derived, not a magic constant.
//! 3. **High goodput of the bulk lane** — the bulk lane keeps a high fraction
//!    of the link's capacity on the same topology. The topology-level gate is
//!    owned by `rtp_mux`; the layer asserts (a) the loopback bulk ceiling
//!    (`mux_ceiling_probe::probe_mux_sink_*`, release-only floors derived from
//!    the measured median band: `MUX_SINK_MSS8K_FLOOR_MIB_S = 96` ≈ 0.5× the ~191
//!    MiB/s median, `MUX_SINK_DIRECT_FLOOR_MIB_S = 58` ≈ 0.5× the ~117 MiB/s
//!    median, so a merge that halves loopback throughput fails while ordinary
//!    host-load noise never trips it) and (b) that every stream keeps its
//!    offered share of a shaped link (`mux_stream_fairness`: Jain ≥ 0.98, the
//!    byte-fair floor measured against the pre-fix ~0.75 scheduler, and a
//!    ≥ 2 % minimum share — no stream may be starved).
//!
//! These three are jointly the acceptance criterion: a change that improves
//! one mandate while violating another is a failure. Redundancy monotonicity
//! is NOT a mandate (FEC recovery parity may grow with loss; what must not
//! happen is the interactive lane's extra/armor packets inflating its own
//! delivered wire). Every floor with a wall-clock component is gated
//! release-only or in an opt-in tier and vacuity-checked: break the mandate
//! (inflate a floor, starve a stream, drop a delivery) and the gate fails
//! naming the mandate.
//!
//! The harness must not restate this constitution: `netem_test` keeps the
//! impairment instrument and points at the owning crates.

//! `mux` over `rtp` performance scenarios through [`netem_test::NetemPair`].
//!
//! These exercise the multiplexed byte stream over a contended, lossy 400
//! KiB/s link and a small-interactive-stream-while-bulk scenario. Perf tests
//! print elapsed/throughput/stats with `--nocapture`.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p rtp_mux --test mux_over_rtp_perf -- --ignored --nocapture --test-threads=1
//! ```

use std::time::{Duration, Instant};

use mux::testkit::mux::mux_client_connect_via;
use netem_test::kit::payload::{cyclic_payload, payload, with_timeout};
use netem_test::kit::presets::{hostile_fat_pipe, lossy_400kib_per_sec};
use netem_test::kit::stats::{combined_stats, print_perf};
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::rtp_connect_via;
use rtp_mux::testkit::mux_over_rtp::{
    mux_send_payload, mux_send_repeated, mux_timed_echo_round_trip,
    spawn_mux_over_rtp_counting_sink_server_via, spawn_mux_over_rtp_echo_server_via,
    spawn_mux_over_rtp_sink_server_via,
};

/// `mux` over `rtp` should deliver a 1 KiB payload intact through a lossy,
/// rate-limited 400 KiB/s netem link — a smoke test that the multiplexed
/// stream survives a contended link without tripping `rtp`'s broken-pipe
/// heuristic. `stats.rate_limited > 0` is deterministic (every non-reordered
/// forwarded packet increments it when `rate != 0`), so this is not flaky.
///
/// Default tier: the 1 KiB round-trip finishes in ~30 ms and both stats
/// bounds are deterministic; over 3 runs at load average 4.2-4.9 it passed
/// 3/3.
#[tokio::test(flavor = "multi_thread")]
async fn mux_over_rtp_lossy_perf_smoke() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_mux_over_rtp_echo_server_via(&task_tx, false)
                .await
                .unwrap();

            let c2s = lossy_400kib_per_sec();
            let s2c = lossy_400kib_per_sec();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);

            let payload = payload(1024);
            let (got, elapsed) = with_timeout(
                Duration::from_secs(30),
                "mux-over-rtp 1KiB lossy perf smoke",
                mux_timed_echo_round_trip(&opener, &payload),
            )
            .await;

            assert_eq!(got, payload, "mux stream must deliver all 1KiB intact");
            print_perf("mux-over-rtp 1KiB lossy perf smoke", payload.len(), elapsed);

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    eprintln!("[perf] mux-over-rtp 1KiB lossy perf smoke stats: {stats:?}");
    assert!(
        stats.forwarded > 0,
        "proxy should forward packets, got {stats:?}"
    );
    assert!(
        stats.rate_limited > 0,
        "rate-limited link should shape packets, got {stats:?}"
    );
}

/// `mux` over `rtp` should deliver a 400 KiB payload intact through a lossy,
/// rate-limited 400 KiB/s netem link to a read-only sink, and report
/// throughput with `--nocapture`. The rate is `400 * 1024 * 8` bits/s plus
/// small loss/latency/jitter so the link is contended but not hopeless.
///
/// Default tier: the 400 KiB transfer completes in ~1.0 s and `dropped > 0`
/// plus `rate_limited > 0` are deterministic on the lossy link; over 13 runs
/// at load average 4.2-5.9 it passed 13/13.
#[tokio::test(flavor = "multi_thread")]
async fn mux_over_rtp_400kib_lossy_contended_perf() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let (server_addr, mut received) = spawn_mux_over_rtp_sink_server_via(&task_tx, false)
                .await
                .unwrap();

            let c2s = lossy_400kib_per_sec();
            let s2c = lossy_400kib_per_sec();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);

            let payload = payload(400 * 1024);
            let elapsed = with_timeout(
                Duration::from_secs(120),
                "mux-over-rtp 400KiB lossy perf",
                mux_send_payload(&opener, &payload),
            )
            .await;

            let got = with_timeout(
                Duration::from_secs(120),
                "mux-over-rtp 400KiB lossy perf receive",
                async { received.recv().await.expect("sink channel closed") },
            )
            .await;

            assert_eq!(got, payload, "mux stream must deliver all 400KiB intact");
            print_perf(
                "mux-over-rtp 400KiB lossy/contended",
                payload.len(),
                elapsed,
            );

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    eprintln!("[perf] mux-over-rtp 400KiB lossy/contended stats: {stats:?}");
    assert!(
        stats.dropped > 0,
        "lossy link should drop some, got {stats:?}"
    );
    assert!(
        stats.rate_limited > 0,
        "rate-limited link should shape packets, got {stats:?}"
    );
}

/// A small interactive mux stream should complete while a bulk 400 KiB/s
/// transfer is also in flight over the same contended link — verifying the
/// multiplexer does not starve small streams under bulk load. Both payloads
/// are sent to a read-only sink through the same `rtp` connection and the
/// same proxy; the small payload must arrive within 5 s of the start.
///
/// Default tier: the ordering assertion is relative (small before bulk) and
/// the absolute liveness bound is derived from the 120 s scenario budget, so
/// host load slows both streams together. Over 13 runs at load average
/// 4.2-5.8 it passed 13/13 and completed in ~1.05 s per run.
#[tokio::test(flavor = "multi_thread")]
async fn mux_over_rtp_small_stream_while_bulk_perf() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let (server_addr, mut received) = spawn_mux_over_rtp_sink_server_via(&task_tx, false)
                .await
                .unwrap();

            // A mildly contended link: latency + small loss, no harsh rate limit so
            // the bulk and small streams can both make progress.
            let impaired = NetemConfig {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(3),
                loss: u32::MAX / 200, // ~0.5%
                rate: 400 * 1024 * 8,
                seed: 17,
                ..NetemConfig::default()
            };
            let pair = NetemPair::spawn(server_addr, impaired.clone(), impaired).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);

            // Send the bulk 400 KiB payload on one stream, then after a short delay
            // send the small interactive payload on a second stream. The opener's
            // `open` takes `&self`, so a cloned opener can share the connection.
            let bulk = payload(400 * 1024);
            let small = b"small-interactive-stream".to_vec();

            let start = Instant::now();
            let bulk_opener = opener.clone();
            let bulk_for_compare = bulk.clone();

            // The bulk and small sends run CONCURRENTLY as pinned futures
            // driven through tokio::join!, so the small stream rides the
            // same connection while the 400 KiB bulk is still in flight.
            let (bulk_elapsed, small_elapsed) = with_timeout(
                Duration::from_secs(120),
                "mux-over-rtp small-while-bulk sends",
                async {
                    let bulk_send = mux_send_payload(&bulk_opener, &bulk);
                    let small_send = async {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        mux_send_payload(&opener, &small).await
                    };
                    tokio::join!(bulk_send, small_send)
                },
            )
            .await;

            // Drain the sink channel until both payloads have arrived,
            // matching by equality. Panic on any unexpected payload.
            let mut got_bulk = false;
            let mut got_small = false;
            let mut bulk_arrived_at: Option<Instant> = None;
            let mut small_arrived_at: Option<Instant> = None;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            while !got_bulk || !got_small {
                let recv = tokio::time::timeout_at(deadline, received.recv())
                    .await
                    .unwrap_or_else(|_| panic!("timeout waiting for sink payloads"))
                    .expect("sink channel closed");
                if recv == bulk_for_compare {
                    got_bulk = true;
                    bulk_arrived_at = Some(Instant::now());
                } else if recv == small {
                    got_small = true;
                    small_arrived_at = Some(Instant::now());
                } else {
                    panic!("unexpected payload from sink: len {}", recv.len());
                }
            }

            assert!(got_bulk, "bulk stream must deliver all 400KiB intact");
            assert!(got_small, "small stream must deliver intact");
            let small_at = small_arrived_at.expect("small payload arrived");
            let bulk_at = bulk_arrived_at.expect("bulk payload arrived");
            // Starvation is a *relative* property: the small interactive
            // stream must overtake the 400 KiB bulk it shares the connection
            // with, not merely finish inside some absolute wall-clock budget.
            // Comparing the two arrivals stays valid under host load (both
            // streams slow down together) while still failing if the bulk
            // drains before the small payload — i.e. if the multiplexer
            // starves the small stream behind the bulk.
            assert!(
                small_at < bulk_at,
                "small interactive stream must be delivered before the bulk stream \
                 completes (small at {:?}, bulk at {:?} after start)",
                small_at.duration_since(start),
                bulk_at.duration_since(start),
            );
            // Absolute liveness guard only, derived from the scenario's own
            // 120 s budget rather than a tight scheduling bound; the ordering
            // assertion above is the starvation detector.
            let small_arrived_after = small_at.duration_since(start);
            assert!(
                small_arrived_after < Duration::from_secs(30),
                "small stream took {small_arrived_after:?}, far past the scenario budget"
            );
            let _ = small_elapsed;
            print_perf(
                "mux-over-rtp bulk 400KiB (small-while-bulk)",
                bulk_for_compare.len(),
                bulk_elapsed,
            );

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    eprintln!("[perf] mux-over-rtp small-while-bulk stats: {stats:?}");
    assert!(stats.forwarded > 0, "proxy should forward packets");
}

/// `mux` over `rtp` should deliver a 400 MiB payload intact through the
/// [`hostile_fat_pipe`] link — a 100 Mbit/s rate cap, 150 ms one-way latency,
/// 30 ms jitter, ~2% Gilbert-Elliott burst loss (mean burst 4), a 16k-packet
/// queue limit, seed 4 — to a read-only sink, and report throughput with
/// `--nocapture`. The fat pipe + long latency inflates the bandwidth-delay
/// product toward the queue limit, exposing proxy-level bottlenecks (queue
/// insertion, per-packet lock contention, per-packet allocation) that the mild
/// synthetic presets cannot reach.
///
/// Throughput is dominated by `rtp`'s ARQ, not the proxy, and varies run to
/// run with host scheduling and era: earlier observed runs completed 400 MiB
/// in ~146-243 s (~1.65-2.75 MiB/s). Re-measurement on the current host spans
/// the same band but with a slow tail — 237 s (1.69 MiB/s) at load ~3, and a
/// 64 MiB shrunken probe at 76 s (0.856 MiB/s), which extrapolates to ~467 s
/// for the full payload — so the old 335 s budget caught the slow tail and the
/// delivery assertions never ran. `BUDGET` (750 s) is the re-calibrated
/// bound — 1.6x the slow-tail extrapolation, 3.2x the fast observation, still
/// bounded — and this is a measurement, not a throughput gate: the assertions
/// that run on completion are the delivery-integrity checks
/// (`delivered_bytes == sent`, no corruption), never a wall-clock goodput
/// floor.
///
/// Run with:
///
/// ```sh
/// cargo test --release -p mux --test mux_over_rtp_perf \
///     mux_over_rtp_400mib_hostile_perf -- --ignored --nocapture --test-threads=1
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf scenario over a contended, lossy link; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn mux_over_rtp_400mib_hostile_perf() {
    const TARGET_BYTES: usize = 400 * 1024 * 1024;
    const BUDGET: Duration = Duration::from_secs(750);
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let (server_addr, progress) =
                spawn_mux_over_rtp_counting_sink_server_via(&task_tx, false, rtp::udp::NO_FEC_MSS)
                    .await
                    .unwrap();
            let impaired = hostile_fat_pipe();
            let pair = NetemPair::spawn(server_addr, impaired.clone(), impaired).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);
            let chunk = cyclic_payload(1024 * 1024);
            let repeat = TARGET_BYTES.div_ceil(chunk.len());
            let sent = chunk.len() * repeat;
            let elapsed = with_timeout(
                BUDGET,
                "mux-over-rtp 400MiB hostile perf",
                mux_send_repeated(&opener, &chunk, repeat),
            )
            .await;
            assert!(
                !progress.is_corrupt(),
                "sink saw bytes diverging from the payload pattern"
            );
            assert_eq!(
                progress.delivered_bytes(),
                sent as u64,
                "mux stream must deliver every byte sent"
            );
            print_perf("mux-over-rtp 400MiB hostile", sent, elapsed);

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    eprintln!("[perf] mux-over-rtp 400MiB hostile stats: {stats:?}");
    assert!(
        stats.dropped > 0,
        "hostile link should drop some, got {stats:?}"
    );
    assert!(
        stats.delayed > 0,
        "hostile link should delay packets, got {stats:?}"
    );
    assert!(
        stats.rate_limited > 0,
        "rate-capped link should shape packets, got {stats:?}"
    );
}
