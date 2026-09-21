//! Loopback perf-ceiling probes for the `mux`-over-`rtp` ceiling: 4 MiB sink
//! uploads and 1 MiB echo round-trips against a `NetemPair` on a clean
//! loopback link, reporting median/worst throughput (one-way bytes for echo).
//!
//! The `mux`-over-`rtp` layer's contribution to the tri-mandate constitution
//! ("Performance" in `GATE.md`): the release-only median bulk-goodput floors
//! below are the bulk-lane ceiling arm of mandate 3 at the raw-transport
//! level — a merge that halves loopback throughput fails, while ordinary
//! host-load noise (the measured medians held through load ~6) never trips
//! the 0.5x-margin floors. Delivery integrity (mandate 2's per-stream half)
//! is asserted per iteration (`assert_eq!(got, data)`).
//!
//! The target is named `mux_ceiling_probe` because the cooperation crate's
//! `perf_probe` target is the `tools/perf-loop` battery; the two are distinct
//! instruments.
//!
//! The probes are `#[ignore]` by default so they compile without running;
//! execute them with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test mux_ceiling_probe -- --ignored --nocapture --test-threads=1
//! ```

use std::time::Duration;

use netem_test::NetemPair;
use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::presets::clean;
use netem_test::kit::stats::{combined_stats, print_median_worst};
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use rtp_mux::testkit::mux_over_rtp::{
    BULK, LOOPBACK_MSS, PROBE_ITERS, mux_send_payload, mux_timed_echo_round_trip,
    rtp_connect_transient, spawn_mux_over_rtp_echo_server_via,
    spawn_mux_over_rtp_echo_server_with_mss_via, spawn_mux_over_rtp_sink_server_via,
    spawn_mux_over_rtp_sink_server_with_mss_via,
};
// The release-only bulk-goodput floors exist only under --release (they are
// wall-clock, so debug builds compile them out entirely), so the import must
// match their cfg or a debug build fails to resolve them.
#[cfg(not(debug_assertions))]
use rtp_mux::testkit::mux_over_rtp::{
    MUX_SINK_DIRECT_FLOOR_MIB_S, MUX_SINK_MSS8K_FLOOR_MIB_S, assert_median_bulk_floor,
};

/// The sink server echoes nothing; the client writes the full payload and waits
/// for the peer-side EOF. The upload size is reported as payload bytes. A fresh
/// mux server is spawned for each iteration because the mux server only handles
/// its first accepted RTP connection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_mux_sink_4mib_direct() {
    let data = payload(BULK);
    let mut tasks = TestScope::new();

    // Pre-spawn one one-shot mux sink server per iteration; each accepts its
    // first (and only) RTP connection during that iteration and its task
    // completes once the client closes the session afterwards.
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let samples = tasks
        .run(async {
            let mut servers = Vec::new();
            for _ in 0..PROBE_ITERS {
                let (server_addr, received) = spawn_mux_over_rtp_sink_server_via(&task_tx, false)
                    .await
                    .unwrap();
                servers.push((server_addr, received));
            }

            let mut samples = Vec::with_capacity(PROBE_ITERS);
            for (server_addr, mut received) in servers {
                let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
                let (read, write) = rtp_connect_transient(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                )
                .await;
                let config = mux::MuxConfig {
                    initiation: mux::Initiation::Client,
                    heartbeat_interval: Duration::from_secs(5),
                    frame_reassembly: false,
                };
                let mut spawner = tokio::task::JoinSet::new();
                let (opener, _accepter) =
                    mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);
                // Transient supervision drain submitted to the test-owned
                // reaper: the session is intentionally torn down mid-body
                // (pair.stop() cuts the link after each one-shot probe), so
                // the drain must not be required. The reaper unwraps every
                // completion, so a panicked supervision task still fails the
                // test; a MuxError session-end is the expected teardown.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        if let Some(result) = spawner.join_next().await {
                            result.unwrap();
                        }
                    }),
                );

                let elapsed = with_timeout(
                    Duration::from_secs(60),
                    "mux sink 4MiB direct upload",
                    mux_send_payload(&opener, &data),
                )
                .await;

                let got = with_timeout(
                    Duration::from_secs(60),
                    "mux sink 4MiB direct receive",
                    async { received.recv().await.expect("sink channel closed") },
                )
                .await;

                assert_eq!(got, data, "mux sink must deliver all 4MiB intact");
                samples.push(elapsed);

                pair.stop();
                let stats = combined_stats(&pair);
                assert!(
                    stats.forwarded > 0,
                    "proxy should forward packets, got {stats:?}"
                );
            }
            samples
        })
        .await;

    #[cfg(not(debug_assertions))]
    assert_median_bulk_floor(
        "mux sink 4MiB direct",
        BULK,
        &samples,
        MUX_SINK_DIRECT_FLOOR_MIB_S,
    );
    print_median_worst("mux sink 4MiB direct", BULK, samples);
}

/// `mux`-over-`rtp` 4 MiB sink upload using the loopback-sized MSS.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_mux_sink_4mib_mss8k() {
    let data = payload(BULK);
    let mut tasks = TestScope::new();

    // Pre-spawn one one-shot mux sink server per iteration; each accepts its
    // first (and only) RTP connection during that iteration.
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let samples = tasks
        .run(async {
            let mut servers = Vec::new();
            for _ in 0..PROBE_ITERS {
                let (server_addr, received) =
                    spawn_mux_over_rtp_sink_server_with_mss_via(&task_tx, false, LOOPBACK_MSS)
                        .await
                        .unwrap();
                servers.push((server_addr, received));
            }

            let mut samples = Vec::with_capacity(PROBE_ITERS);
            for (server_addr, mut received) in servers {
                let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
                let (read, write) =
                    rtp_connect_transient(&task_tx, pair.client_addr(), false, LOOPBACK_MSS).await;
                let config = mux::MuxConfig {
                    initiation: mux::Initiation::Client,
                    heartbeat_interval: Duration::from_secs(5),
                    frame_reassembly: false,
                };
                let mut spawner = tokio::task::JoinSet::new();
                let (opener, _accepter) =
                    mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);
                // Transient supervision drain submitted to the test-owned
                // reaper: the session is intentionally torn down mid-body
                // (pair.stop() cuts the link after each one-shot probe), so
                // the drain must not be required. The reaper unwraps every
                // completion, so a panicked supervision task still fails the
                // test; a MuxError session-end is the expected teardown.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        if let Some(result) = spawner.join_next().await {
                            result.unwrap();
                        }
                    }),
                );

                let elapsed = with_timeout(
                    Duration::from_secs(60),
                    "mux sink 4MiB 8KiB-MSS upload",
                    mux_send_payload(&opener, &data),
                )
                .await;

                let got = with_timeout(
                    Duration::from_secs(60),
                    "mux sink 4MiB 8KiB-MSS receive",
                    async { received.recv().await.expect("sink channel closed") },
                )
                .await;

                assert_eq!(got, data, "mux sink must deliver all 4MiB intact");
                samples.push(elapsed);

                pair.stop();
                let stats = combined_stats(&pair);
                assert!(
                    stats.forwarded > 0,
                    "proxy should forward packets, got {stats:?}"
                );
            }
            samples
        })
        .await;

    #[cfg(not(debug_assertions))]
    assert_median_bulk_floor(
        "mux sink 4MiB 8KiB-MSS",
        BULK,
        &samples,
        MUX_SINK_MSS8K_FLOOR_MIB_S,
    );
    print_median_worst("mux sink 4MiB 8KiB-MSS", BULK, samples);
}

/// `mux`-over-`rtp` 1 MiB echo round-trip, default MSS.
///
/// Echo moves the payload twice; throughput is reported as one-way bytes.
/// This probe is known to hit `rtp`'s broken-pipe heuristic under bulk write
/// pressure once the mux ACK path stalls, so a failure here documents the
/// upstream `rtp` limitation rather than a defect in this probe.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_mux_echo_1mib_direct() {
    let data = payload(1024 * 1024);
    let mut tasks = TestScope::new();

    // Pre-spawn one one-shot mux echo server per iteration; each accepts its
    // first (and only) RTP connection during that iteration.
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let samples = tasks
        .run(async {
            let mut servers = Vec::new();
            for _ in 0..PROBE_ITERS {
                let server_addr = spawn_mux_over_rtp_echo_server_via(&task_tx, false)
                    .await
                    .unwrap();
                servers.push(server_addr);
            }

            let mut samples = Vec::with_capacity(PROBE_ITERS);
            for server_addr in servers {
                let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
                let (read, write) = rtp_connect_transient(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                )
                .await;
                let config = mux::MuxConfig {
                    initiation: mux::Initiation::Client,
                    heartbeat_interval: Duration::from_secs(5),
                    frame_reassembly: false,
                };
                let mut spawner = tokio::task::JoinSet::new();
                let (opener, _accepter) =
                    mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);
                // Transient supervision drain submitted to the test-owned
                // reaper: the session is intentionally torn down mid-body
                // (pair.stop() cuts the link after each one-shot probe), so
                // the drain must not be required. The reaper unwraps every
                // completion, so a panicked supervision task still fails the
                // test; a MuxError session-end is the expected teardown.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        if let Some(result) = spawner.join_next().await {
                            result.unwrap();
                        }
                    }),
                );

                let (got, elapsed) = with_timeout(
                    Duration::from_secs(60),
                    "mux echo 1MiB direct round-trip",
                    mux_timed_echo_round_trip(&opener, &data),
                )
                .await;

                assert_eq!(got, data, "mux echo must deliver all 1MiB intact");
                samples.push(elapsed);

                pair.stop();
                let stats = combined_stats(&pair);
                assert!(
                    stats.forwarded > 0,
                    "proxy should forward packets, got {stats:?}"
                );
            }
            samples
        })
        .await;

    print_median_worst("mux echo 1MiB direct (one-way bytes)", 1024 * 1024, samples);
}

/// `mux`-over-`rtp` 1 MiB echo round-trip using the loopback-sized MSS.
///
/// Like the direct variant, this probe is a known-failure reproduction for
/// `rtp`'s broken-pipe heuristic once the mux ACK path stalls under bulk
/// echo pressure.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_mux_echo_1mib_mss8k() {
    let data = payload(1024 * 1024);
    let mut tasks = TestScope::new();

    // Pre-spawn one one-shot mux echo server per iteration; each accepts its
    // first (and only) RTP connection during that iteration.
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let samples = tasks
        .run(async {
            let mut servers = Vec::new();
            for _ in 0..PROBE_ITERS {
                let server_addr =
                    spawn_mux_over_rtp_echo_server_with_mss_via(&task_tx, false, LOOPBACK_MSS)
                        .await
                        .unwrap();
                servers.push(server_addr);
            }

            let mut samples = Vec::with_capacity(PROBE_ITERS);
            for server_addr in servers {
                let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
                let (read, write) =
                    rtp_connect_transient(&task_tx, pair.client_addr(), false, LOOPBACK_MSS).await;
                let config = mux::MuxConfig {
                    initiation: mux::Initiation::Client,
                    heartbeat_interval: Duration::from_secs(5),
                    frame_reassembly: false,
                };
                let mut spawner = tokio::task::JoinSet::new();
                let (opener, _accepter) =
                    mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);
                // Transient supervision drain submitted to the test-owned
                // reaper: the session is intentionally torn down mid-body
                // (pair.stop() cuts the link after each one-shot probe), so
                // the drain must not be required. The reaper unwraps every
                // completion, so a panicked supervision task still fails the
                // test; a MuxError session-end is the expected teardown.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        if let Some(result) = spawner.join_next().await {
                            result.unwrap();
                        }
                    }),
                );

                let (got, elapsed) = with_timeout(
                    Duration::from_secs(60),
                    "mux echo 1MiB 8KiB-MSS round-trip",
                    mux_timed_echo_round_trip(&opener, &data),
                )
                .await;

                assert_eq!(got, data, "mux echo must deliver all 1MiB intact");
                samples.push(elapsed);

                pair.stop();
                let stats = combined_stats(&pair);
                assert!(
                    stats.forwarded > 0,
                    "proxy should forward packets, got {stats:?}"
                );
            }
            samples
        })
        .await;

    print_median_worst(
        "mux echo 1MiB 8KiB-MSS (one-way bytes)",
        1024 * 1024,
        samples,
    );
}
