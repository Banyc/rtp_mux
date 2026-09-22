//! Perf probes for the `rtp` + `mux` cooperation: the perf-loop battery lanes
//! (time-boxed `mux`-over-`rtp` goodput on impaired links, sparse-message
//! latency, raw `rtp` 4 MiB echo ceilings) plus the two seeding tests that pin
//! the controller / deterministic-iid-loss link presets those lanes are shaped
//! with.
//!
//! `tools/perf-loop` compiles this target (`cargo test -p rtp_mux --test
//! perf_probe`) from the frozen suite's exported `rtp_mux` component, so a
//! `--component-revision rtp_mux=<commit>` pin selects the probe that runs.
//! The `mux`-only loopback ceiling probes live in `mux/tests/perf_probe.rs`
//! and the `rtp`-only suites in `rtp/tests`; this target is the cooperation
//! probe the paired loop drives.
//!
//! The probes are `#[ignore]` by default so they compile without running;
//! execute them with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test perf_probe -- --ignored --nocapture --test-threads=1
//! ```

use std::time::{Duration, Instant};

use mux::testkit::mux::mux_client_connect_transient;
use netem_test::kit::payload::{cyclic_payload, payload, with_timeout};
use netem_test::kit::presets::clean;
use netem_test::kit::stats::{combined_stats, print_median_worst, print_perf};
use netem_test::{CountersSnapshot, NetemPair};
use rtp::testkit::perf_trace::PerfTrace;
use rtp::testkit::rtp::{
    rtp_echo_payload, spawn_rtp_echo_server_via, spawn_rtp_echo_server_with_mss_via,
};
use rtp_mux::testkit::mux_over_rtp::{
    BULK, HOSTILE_GOODPUT_FLOOR_MIB_S, HOSTILE_GUARD_SUBWINDOWS, LOOPBACK_MSS, PROBE_ITERS,
    rtp_connect_transient, rtp_connect_transient_observed, send_timestamped_messages,
    spawn_mux_over_rtp_counting_sink_server_observed_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The controller-retention lane must be pure fixed shaping: a fixed
/// 100 Mbit/s rate and 150 ms latency with no stochastic loss or jitter.
#[test]
fn controller_fat_pipe_has_only_fixed_shaping() {
    let config = netem_test::kit::presets::controller_fat_pipe();
    assert_eq!(config.rate, 100 * 1000 * 1000);
    assert_eq!(config.latency, Duration::from_millis(150));
    assert_eq!(config.queue_limit_pkts, 16 * 1024);
    assert_eq!(config.loss, 0, "fixed shaping must not include random loss");
    assert_eq!(
        config.jitter,
        Duration::ZERO,
        "fixed shaping must not include jitter"
    );
    assert!(
        matches!(config.loss_model, netem_test::LossModel::Random),
        "fixed shaping must not include a stochastic loss model"
    );
}

/// The deterministic iid-loss lane must be a fixed-seed independent
/// per-packet loss on the same shaped fat pipe as the controller-retention
/// lane, so its results are reproducible from the seed alone.
#[test]
fn deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss() {
    let config = netem_test::kit::presets::deterministic_iid_loss_fat_pipe();
    assert_eq!(config.rate, 100 * 1000 * 1000);
    assert_eq!(config.latency, Duration::from_millis(150));
    assert_eq!(config.queue_limit_pkts, 16 * 1024);
    assert!(config.loss > 0, "the lane must include fixed iid loss");
    assert_eq!(config.loss_corr, 0, "iid loss must not be correlated");
    assert_eq!(
        config.jitter,
        Duration::ZERO,
        "iid loss must not include jitter"
    );
    assert!(
        matches!(config.loss_model, netem_test::LossModel::Random),
        "the lane must use the independent random loss model"
    );
    assert_eq!(
        config.seed, 4,
        "the lane must be reproducible from a fixed seed"
    );
}

/// Raw `rtp` 4 MiB direct echo, default MSS.
///
/// Echo moves the payload twice, so throughput is reported as one-way bytes.
/// The server is reused across the five iterations; each iteration opens a
/// fresh RTP connection through its own `NetemPair` because a standard pair
/// pins its receive transport to the first client tuple, so a single shared
/// pair would black-hole every connection after the first (a fresh connection
/// binds a fresh ephemeral port).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_rtp_echo_4mib_direct() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);

    let data = payload(BULK);
    let (samples, forwarded) = tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_via(&task_tx, false).await.unwrap();

            let mut samples = Vec::with_capacity(PROBE_ITERS);
            let mut forwarded = 0u64;
            for _ in 0..PROBE_ITERS {
                let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
                let (read, write) = rtp_connect_transient(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                )
                .await;
                let start = Instant::now();
                let got = with_timeout(
                    Duration::from_secs(60),
                    "rtp 4MiB direct echo",
                    rtp_echo_payload(read, write, &data),
                )
                .await;
                let elapsed = start.elapsed();
                assert_eq!(got, data);
                samples.push(elapsed);
                pair.stop();
                forwarded += combined_stats(&pair).forwarded;
            }
            (samples, forwarded)
        })
        .await;

    print_median_worst("rtp 4MiB direct echo (one-way bytes)", BULK, samples);

    assert!(
        forwarded > 0,
        "proxy should forward packets, got {forwarded}"
    );
}

/// Raw `rtp` 4 MiB echo through a `NetemPair` using the loopback-sized MSS.
///
/// Each iteration gets its own pair for the same reason as
/// [`probe_rtp_echo_4mib_direct`]: a standard pair pins the first client tuple.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_rtp_echo_4mib_mss8k() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);

    let data = payload(BULK);
    let (samples, forwarded) = tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_with_mss_via(&task_tx, false, LOOPBACK_MSS)
                .await
                .unwrap();

            let mut samples = Vec::with_capacity(PROBE_ITERS);
            let mut forwarded = 0u64;
            for _ in 0..PROBE_ITERS {
                let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
                let (read, write) =
                    rtp_connect_transient(&task_tx, pair.client_addr(), false, LOOPBACK_MSS).await;
                let start = Instant::now();
                let got = with_timeout(
                    Duration::from_secs(60),
                    "rtp 4MiB 8KiB-MSS echo",
                    rtp_echo_payload(read, write, &data),
                )
                .await;
                let elapsed = start.elapsed();
                assert_eq!(got, data);
                samples.push(elapsed);
                pair.stop();
                forwarded += combined_stats(&pair).forwarded;
            }
            (samples, forwarded)
        })
        .await;

    print_median_worst("rtp 4MiB 8KiB-MSS echo (one-way bytes)", BULK, samples);

    assert!(
        forwarded > 0,
        "proxy should forward packets, got {forwarded}"
    );
}

/// Time-boxed `mux`-over-`rtp` goodput probe across the hostile link profile.
///
/// A cyclic payload is written repeatedly for the whole 30 s window, so the
/// measured goodput is receive-limited, not capped by a finite sender-payload.
/// The counting sink verifies every byte in-flight and is snapshotted while the
/// transfer is still mid-flight, so a reversed measurement order cannot inflate
/// goodput.
///
/// The window is split into [`HOSTILE_GUARD_SUBWINDOWS`] equal sub-windows and
/// the guard asserts the **median** sub-window goodput against
/// [`HOSTILE_GOODPUT_FLOOR_MIB_S`], so one load-spiked sub-window cannot trip
/// it; a collapse moves every sub-window and still fails.
///
/// `NETEM_PERF_LINK_PROFILE=direct` bypasses NetemPair entirely: the client
/// connects straight to the server and the trace records zero-valued netem
/// placeholders so artifacts stay schema-compatible.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_hostile_goodput_30s() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut trace = PerfTrace::from_env();
            let window_seconds = std::env::var("NETEM_PERF_WINDOW_SECONDS")
                .map(|value| {
                    let seconds = value
                        .parse::<f64>()
                        .expect("NETEM_PERF_WINDOW_SECONDS must be a number");
                    assert!(seconds.is_finite() && seconds > 0.0);
                    seconds
                })
                .unwrap_or(30.0);
            let warmup_seconds = std::env::var("NETEM_PERF_WARMUP_SECONDS")
                .map(|value| {
                    let seconds = value
                        .parse::<f64>()
                        .expect("NETEM_PERF_WARMUP_SECONDS must be a number");
                    assert!(seconds.is_finite() && seconds >= 0.0);
                    seconds
                })
                .unwrap_or(20.0);
            let link_profile = std::env::var("NETEM_PERF_LINK_PROFILE")
                .unwrap_or_else(|_| "hostile".to_owned());
            assert!(
                matches!(
                    link_profile.as_str(),
                    "hostile"
                        | "hostile-steady"
                        | "hostile-steady-bottleneck"
                        | "hostile-steady-bottleneck-20ms"
                        | "hostile-steady-bottleneck-100ms"
                        | "hostile-periodic-bottleneck"
                        | "lossy-400kib"
                        | "hostile-fat-pipe"
                        | "controller-fat-pipe"
                        | "deterministic-iid-loss-fat-pipe"
                        | "jittery-short-rtt"
                        | "high-rtt-low-rate-bottleneck"
                        | "clean"
                        | "direct"
                        | "hostile-bottleneck-20ms"
                        | "hostile-bottleneck-100ms"
                        | "hostile-bottleneck-300ms"
                        | "fec-recoverable-bottleneck"
                        | "fec-gaming-fat-pipe"
                        | "fec-paired-saturated"
                        | "fec-paired-saturated-bottleneck"
                        | "hostile-periodic-bottleneck-20ms"
                        | "hostile-periodic-bottleneck-100ms"
                        | "hostile-periodic-bottleneck-300ms"
                ),
                "NETEM_PERF_LINK_PROFILE must be one of the hostile/controller/fec/regime calibration profiles, got {link_profile:?}"
            );
            let direct = link_profile == "direct";
            let mss_bytes = std::env::var("NETEM_PERF_MSS_BYTES")
                .map(|value| {
                    let mss = value
                        .parse::<usize>()
                        .expect("NETEM_PERF_MSS_BYTES must be a positive usize");
                    assert!(mss > 0, "NETEM_PERF_MSS_BYTES must be positive");
                    mss
                })
                .unwrap_or(LOOPBACK_MSS);
            // Paired run flags are parsed strictly: only 0|1|false|true are
            // accepted, so a typo cannot silently flip FEC or armor on a
            // timed run.
            let parse_flag_env = |name: &str| -> bool {
                match std::env::var(name).as_deref() {
                    Ok("0") | Ok("false") => false,
                    Ok("1") | Ok("true") => true,
                    Ok(other) => panic!(
                        "{name} must be exactly 0, 1, false, or true, got {other:?}"
                    ),
                    Err(_) => false,
                }
            };
            let fec = parse_flag_env("NETEM_PERF_FEC");
            let retransmission_armor = parse_flag_env("RTP_RTX_DUP");
            let make_link = || match link_profile.as_str() {
                "hostile" => netem_test::kit::presets::hostile_real_link(),
                "hostile-steady" => netem_test::kit::presets::hostile_steady_link(),
                "hostile-steady-bottleneck" => netem_test::kit::presets::hostile_steady_bottleneck(),
                "hostile-steady-bottleneck-20ms" => {
                    netem_test::kit::presets::hostile_steady_bottleneck_20ms()
                }
                "hostile-steady-bottleneck-100ms" => {
                    netem_test::kit::presets::hostile_steady_bottleneck_100ms()
                }
                "lossy-400kib" => netem_test::kit::presets::lossy_400kib_per_sec(),
                "hostile-fat-pipe" => netem_test::kit::presets::hostile_fat_pipe(),
                "controller-fat-pipe" => netem_test::kit::presets::controller_fat_pipe(),
                "deterministic-iid-loss-fat-pipe" => {
                    netem_test::kit::presets::deterministic_iid_loss_fat_pipe()
                }
                // The two regime lanes the battery's shaped/zero-jitter lanes
                // cannot reach: the jitter lane is the only one that reorders
                // (so its `4 * rttvar` can exceed `srtt / 4` and disarm the
                // fast-loss gate), and the thin-link lane's rate and queue can
                // hold a round trip long enough for RFC 6298's `srtt + 4 *
                // rttvar` to reach the tens of seconds.
                "jittery-short-rtt" => netem_test::kit::presets::jittery_short_rtt_link(),
                "high-rtt-low-rate-bottleneck" => {
                    netem_test::kit::presets::high_rtt_low_rate_bottleneck()
                }
                "clean" | "direct" => netem_test::kit::presets::clean(),
                "hostile-bottleneck-20ms" => netem_test::kit::presets::hostile_steady_bottleneck_20ms(),
                "hostile-bottleneck-100ms" => netem_test::kit::presets::hostile_steady_bottleneck_100ms(),
                "hostile-bottleneck-300ms" => netem_test::kit::presets::hostile_steady_bottleneck(),
                "fec-recoverable-bottleneck" => netem_test::kit::presets::fec_recoverable_bottleneck(),
                "fec-gaming-fat-pipe" => netem_test::kit::presets::fec_gaming_fat_pipe(),
                // The runtime FEC flag selects the paired-saturated key
                // offset: the FEC-on arm wraps the data packet in the 10-byte
                // FEC envelope, so the netem key must point past it at the
                // same logical codec sequence the FEC-off arm keys on.
                "fec-paired-saturated" | "fec-paired-saturated-bottleneck" => {
                    netem_test::kit::presets::fec_paired_saturated_bottleneck(fec)
                }
                "hostile-periodic-bottleneck" => netem_test::kit::presets::hostile_periodic_bottleneck(),
                "hostile-periodic-bottleneck-20ms" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck_20ms()
                }
                "hostile-periodic-bottleneck-100ms" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck_100ms()
                }
                "hostile-periodic-bottleneck-300ms" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck_300ms()
                }
                _ => unreachable!("link profile was validated above"),
            };
            let c2s_seed = std::env::var("NETEM_PERF_SEED")
                .map(|value| value.parse::<u64>().expect("NETEM_PERF_SEED must be a u64"))
                .unwrap_or(4);
            let s2c_seed = c2s_seed.wrapping_add(1);
            let mut c2s = make_link();
            c2s.seed = c2s_seed;
            let mut s2c = make_link();
            s2c.seed = s2c_seed;
            let c2s_description = format!("{c2s:?}");
            let s2c_description = format!("{s2c:?}");

            let (server_addr, progress) = spawn_mux_over_rtp_counting_sink_server_observed_via(
                &task_tx,
                fec,
                mss_bytes,
                trace.as_ref().and_then(PerfTrace::rtp_peer_observer),
            )
            .await
            .unwrap();
            let server_mux = progress.mux_session();
            // Direct mode bypasses NetemPair entirely: the client connects
            // straight to the server and the trace records zero-valued netem
            // placeholders so artifacts stay schema-compatible.
            let pair = if direct {
                None
            } else {
                Some(NetemPair::spawn(server_addr, c2s, s2c).unwrap())
            };
            let pair_ref = pair.as_ref();
            let connect_addr = pair_ref.map_or(server_addr, |pair| pair.client_addr());
            let (read, write) = rtp_connect_transient_observed(
                &task_tx,
                connect_addr,
                fec,
                mss_bytes,
                trace.as_ref().and_then(PerfTrace::rtp_observer),
            )
            .await;
            let (opener, client_mux) = mux_client_connect_transient(&task_tx, read, write);

            // Open the stream under a generous timeout before we start the clock.
            let (stream_read, mut stream_write) = with_timeout(
                Duration::from_secs(30),
                "open mux stream for hostile goodput",
                async { opener.open().await.unwrap() },
            )
            .await;

            // A cyclic payload never exhausts: the pump keeps writing until the
            // measurement owner signals it to stop.
            let data = cyclic_payload(1024 * 1024);
            let (pump_stop_tx, mut pump_stop_rx) = tokio::sync::watch::channel(false);
            let mut pump_tasks = tokio::task::JoinSet::new();
            pump_tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = pump_stop_rx.changed() => return Ok::<(), std::io::Error>(()),
                        result = stream_write.write_all(&data) => {
                            result?;
                        }
                    }
                }
            });

            // Unmeasured warmup: let the live transfer reach steady state
            // before the measurement boundary is anchored on the shared trace
            // clock. Warmup delivery is excluded from every progress and
            // final delivered value.
            let mut pump_error = None;
            let warmup = tokio::time::sleep(Duration::from_secs_f64(warmup_seconds));
            tokio::pin!(warmup);
            tokio::select! {
                joined = pump_tasks.join_next(), if !pump_tasks.is_empty() => {
                    let result = joined.expect("bulk pump exists").unwrap();
                    pump_error = Some(match result {
                        Ok(()) => "bulk pump ended before the measurement window".to_owned(),
                        Err(error) => format!("bulk pump failed: {error:?}: {error}"),
                    });
                }
                _ = &mut warmup => {}
            }
            let warmup_delivered = progress.delivered_bytes();
            let start = Instant::now();
            // Anchor the measurement boundary on the shared trace clock so
            // netem/progress samples line up with the RTP endpoint rows.
            if let Some(trace) = trace.as_mut() {
                trace.mark_measurement_start(start);
            }
            let mut netem_tick = tokio::time::interval(Duration::from_millis(50));
            netem_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Split the measurement window into equal sub-windows and record
            // each one's delivered bytes independently. The guard consumes the
            // median rate, so a single load-spiked sub-window cannot trip the
            // floor; the whole-window rate is still reported and traced.
            let subwindow = Duration::from_secs_f64(window_seconds / HOSTILE_GUARD_SUBWINDOWS as f64);
            let mut subwindow_rates_mib_s = Vec::with_capacity(HOSTILE_GUARD_SUBWINDOWS);
            if pump_error.is_none() {
                'subwindows: for _ in 0..HOSTILE_GUARD_SUBWINDOWS {
                    let subwindow_mark = progress.delivered_bytes();
                    let subwindow_start = Instant::now();
                    let deadline = tokio::time::sleep(subwindow);
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            joined = pump_tasks.join_next(), if !pump_tasks.is_empty() => {
                                let result = joined.expect("bulk pump exists").unwrap();
                                pump_error = Some(match result {
                                    Ok(()) => "bulk pump ended before the measurement window".to_owned(),
                                    Err(error) => format!("bulk pump failed: {error:?}: {error}"),
                                });
                                break 'subwindows;
                            }
                            _ = netem_tick.tick(), if trace.is_some() => {
                                trace.as_mut().unwrap().record_netem(
                                    start.elapsed(),
                                    pair_ref.map_or(CountersSnapshot::default(), |pair| pair.snapshot_c2s()),
                                    pair_ref.map_or(CountersSnapshot::default(), |pair| pair.snapshot_s2c()),
                                    progress.delivered_bytes() - warmup_delivered,
                                );
                            }
                            _ = &mut deadline => break,
                        }
                    }
                    subwindow_rates_mib_s.push(
                        (progress.delivered_bytes() - subwindow_mark) as f64
                            / (1024.0 * 1024.0)
                            / subwindow_start.elapsed().as_secs_f64(),
                    );
                }
            }

            let delivered = progress.delivered_bytes() - warmup_delivered;
            let elapsed = start.elapsed();
            let mut sorted_subwindow_rates = subwindow_rates_mib_s.clone();
            sorted_subwindow_rates.sort_by(f64::total_cmp);
            let median_goodput_mib_s = sorted_subwindow_rates
                .get(sorted_subwindow_rates.len() / 2)
                .copied()
                .unwrap_or(0.0);
            assert!(
                delivered > 0,
                "probe must deliver payload bytes, got {delivered}"
            );
            let _ = pump_stop_tx.send(true);
            while let Some(result) = pump_tasks.join_next().await {
                let result = result.unwrap();
                if let Err(error) = result {
                    pump_error.get_or_insert_with(|| {
                        format!("bulk pump failed during epilog: {error:?}: {error}")
                    });
                }
            }
            if pump_error.is_some() {
                let _ =
                    tokio::time::timeout(Duration::from_secs(1), progress.wait_for_read_outcome())
                        .await;
                let _ = tokio::time::timeout(Duration::from_secs(1), client_mux.wait_for_outcome())
                    .await;
                let _ = tokio::time::timeout(Duration::from_secs(1), server_mux.wait_for_outcome())
                    .await;
            }

            if let Some(trace) = trace {
                let revision = std::env::var("NETEM_PERF_REVISION")
                    .unwrap_or_else(|_| "unspecified".to_owned());
                let output_dir = trace
                    .finish(&[
                        (
                            "scenario",
                            format!("mux_over_rtp_{link_profile}_goodput_window"),
                        ),
                        ("link_profile", link_profile.clone()),
                        ("revision", revision),
                        ("window_seconds", window_seconds.to_string()),
                        ("warmup_seconds", warmup_seconds.to_string()),
                        ("warmup_delivered_bytes", warmup_delivered.to_string()),
                        (
                            "measurement_end_reason",
                            if pump_error.is_none() {
                                "timebox_elapsed".to_owned()
                            } else {
                                "pump_ended_early".to_owned()
                            },
                        ),
                        ("mss_bytes", mss_bytes.to_string()),
                        ("fec", fec.to_string()),
                        ("retransmission_armor", retransmission_armor.to_string()),
                        ("rtp_handshake", "false".to_owned()),
                        ("netem_sample_interval_micros", "50000".to_owned()),
                        ("delivered_bytes", delivered.to_string()),
                        ("elapsed_seconds", elapsed.as_secs_f64().to_string()),
                        (
                            "goodput_mib_per_second",
                            (delivered as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64())
                                .to_string(),
                        ),
                        ("netem_c2s_seed", c2s_seed.to_string()),
                        ("netem_s2c_seed", s2c_seed.to_string()),
                        (
                            "probe_outcome",
                            pump_error.as_deref().unwrap_or("completed").to_owned(),
                        ),
                        ("sink_read_outcome", progress.read_outcome().as_label()),
                        ("client_mux_outcome", client_mux.outcome().as_label()),
                        ("server_mux_outcome", server_mux.outcome().as_label()),
                        (
                            "netem_c2s",
                            if direct { "direct".to_owned() } else { c2s_description },
                        ),
                        (
                            "netem_s2c",
                            if direct { "direct".to_owned() } else { s2c_description },
                        ),
                    ])
                    .expect("write perf trace");
                eprintln!("[trace] {}", output_dir.display());
            }

            if let Some(pair) = pair_ref {
                pair.stop();
                let stats = combined_stats(pair);
                eprintln!("[stats] {stats:?}");
                if matches!(
                    link_profile.as_str(),
                    "hostile"
                        | "hostile-steady"
                        | "hostile-steady-bottleneck"
                        | "hostile-steady-bottleneck-20ms"
                        | "hostile-steady-bottleneck-100ms"
                        | "hostile-periodic-bottleneck"
                        | "lossy-400kib"
                        | "hostile-fat-pipe"
                        | "deterministic-iid-loss-fat-pipe"
                        | "hostile-bottleneck-20ms"
                        | "hostile-bottleneck-100ms"
                        | "hostile-bottleneck-300ms"
                        | "fec-recoverable-bottleneck"
                        | "fec-gaming-fat-pipe"
                        | "fec-paired-saturated"
                        | "fec-paired-saturated-bottleneck"
                        | "hostile-periodic-bottleneck-20ms"
                        | "hostile-periodic-bottleneck-100ms"
                        | "hostile-periodic-bottleneck-300ms"
                ) {
                    assert!(
                        stats.dropped > 0 && stats.delayed > 0,
                        "lossy link should drop and delay packets, got {stats:?}"
                    );
                } else if matches!(
                    link_profile.as_str(),
                    "controller-fat-pipe"
                        | "jittery-short-rtt"
                        | "high-rtt-low-rate-bottleneck"
                ) {
                    assert!(
                        stats.forwarded > 0 && stats.dropped == 0 && stats.delayed > 0,
                        "delay-configured link without configured loss should forward and delay packets without loss drops, got {stats:?}"
                    );
                } else {
                    assert!(
                        stats.forwarded > 0 && stats.dropped == 0,
                        "clean link should forward packets without drops, got {stats:?}"
                    );
                }
            }

            assert!(
                !progress.is_corrupt(),
                "sink saw bytes diverging from the payload pattern"
            );
            if let Some(error) = pump_error {
                panic!("{error}; failed after {:.3} s", elapsed.as_secs_f64());
            }

            let goodput_mib_s = delivered as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            eprintln!(
                "[perf] hostile guard: whole-window {goodput_mib_s:.3} MiB/s, sub-window median {median_goodput_mib_s:.3} MiB/s, sub-windows {subwindow_rates_mib_s:?} MiB/s, floor {HOSTILE_GOODPUT_FLOOR_MIB_S} MiB/s"
            );
            let diagnostic_mode = std::env::var("NETEM_PERF_DIAGNOSTIC_MODE")
                .map(|value| value == "1")
                .unwrap_or(false);
            if diagnostic_mode && median_goodput_mib_s < HOSTILE_GOODPUT_FLOOR_MIB_S {
                eprintln!(
                    "[diagnostic] median sub-window goodput {median_goodput_mib_s:.3} MiB/s below floor {HOSTILE_GOODPUT_FLOOR_MIB_S} MiB/s bypassed by NETEM_PERF_DIAGNOSTIC_MODE=1"
                );
            } else {
                assert!(
                    median_goodput_mib_s >= HOSTILE_GOODPUT_FLOOR_MIB_S,
                    "median sub-window goodput {median_goodput_mib_s:.3} MiB/s below floor {HOSTILE_GOODPUT_FLOOR_MIB_S} MiB/s (whole-window {goodput_mib_s:.3} MiB/s, sub-windows {subwindow_rates_mib_s:?})"
                );
            }

            print_perf(
                "mux-over-rtp hostile 30s goodput window",
                delivered as usize,
                elapsed,
            );

            // Keep the stream read half alive until after the delivered
            // snapshot.
            let _ = stream_read;
        })
        .await;
}

/// Sparse-message probe constants: 64-byte timestamped messages every 100 ms.
const MESSAGE_BYTES: usize = 64;
const MESSAGE_CADENCE: Duration = Duration::from_millis(100);
/// Straggler allowance after the measurement window before the latency
/// samples are drained.
const MESSAGE_GRACE: Duration = Duration::from_secs(4);

/// Parse one accepted mux stream as length-prefixed timestamped frames
/// (`u32` LE length, payload, `u64` LE send timestamp in the last 8 bytes)
/// and push the one-way latency of each complete frame into `latency_tx`;
/// verified frame bytes accumulate in `delivered`.
async fn parse_message_latency_stream(
    mut stream_read: mux::StreamReader,
    mut stream_write: mux::StreamWriter,
    latency_tx: tokio::sync::mpsc::Sender<f64>,
    delivered: std::sync::Arc<std::sync::atomic::AtomicU64>,
    base: Instant,
) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut offset = 0usize;
    while let Ok(n) = stream_read.read(&mut buf[offset..]).await {
        if n == 0 {
            break;
        }
        offset += n;
        loop {
            if offset < 4 {
                break;
            }
            let frame_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if frame_len < 12 || offset < frame_len {
                break;
            }
            let payload_end = frame_len - 8;
            let sent_us = u64::from_le_bytes(buf[payload_end..payload_end + 8].try_into().unwrap());
            let now_us = base.elapsed().as_micros() as u64;
            let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
            if !netem_test::kit::try_send_observation(
                &latency_tx,
                latency_ms,
                "message latency sample",
            ) {
                break;
            }
            delivered.fetch_add(frame_len as u64, std::sync::atomic::Ordering::Relaxed);
            buf.copy_within(frame_len..offset, 0);
            offset -= frame_len;
        }
    }
    let _ = stream_write.shutdown();
}

/// Sparse-message latency server: accepts one mux-over-RTP connection with
/// the requested FEC/MSS, runs each accepted stream through
/// [`parse_message_latency_stream`], and reports one-way latencies plus the
/// mux session outcome for the trace evidence.
async fn spawn_message_latency_server_via(
    task_tx: &netem_test::kit::TestTaskSubmitter,
    fec: bool,
    mss: usize,
    base: Instant,
    metrics_observer: Option<rtp::metrics::MetricsObserver>,
) -> std::io::Result<(
    std::net::SocketAddr,
    tokio::sync::mpsc::Receiver<f64>,
    std::sync::Arc<std::sync::atomic::AtomicU64>,
    std::sync::Arc<mux::testkit::stats::MuxSessionProgress>,
)> {
    let (latency_tx, latency_rx) =
        tokio::sync::mpsc::channel(netem_test::kit::LATENCY_SAMPLE_CAPACITY);
    let delivered = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mux_session = std::sync::Arc::new(mux::testkit::stats::MuxSessionProgress::new());
    let listener =
        rtp::udp::Listener::bind("127.0.0.1:0", rtp::udp::ListenerConfig::default()).await?;
    let addr = listener.local_addr();
    let listener = std::sync::Arc::new(listener);
    let delivered_for_handlers = std::sync::Arc::clone(&delivered);
    netem_test::kit::submit_test_task_required(task_tx, "message latency server", {
        let listener = std::sync::Arc::clone(&listener);
        let mux_session = std::sync::Arc::clone(&mux_session);
        Box::pin(async move {
            // First (and only) rtp connection. An accept failure is a
            // scenario failure; panic so the root JoinError unwrap crashes
            // the test.
            let accepted = listener
                .accept_without_handshake_with(rtp::udp::AcceptConfig {
                    fec,
                    mss: rtp::udp::MssConfig::Custom(mss),
                    metrics_observer,
                    ..rtp::udp::AcceptConfig::default()
                })
                .await
                .unwrap();
            // The extra-accept drainer loop keeps driving `udp_listener`'s
            // dispatcher for the server's lifetime; without it the
            // dispatcher stops after the first connection and the reliable
            // layer stalls. It ends only by panicking on an accept error.
            let drainer = {
                let listener = std::sync::Arc::clone(&listener);
                async move {
                    loop {
                        listener
                            .accept_without_handshake_with(rtp::udp::AcceptConfig {
                                fec,
                                mss: rtp::udp::MssConfig::Custom(mss),
                                ..rtp::udp::AcceptConfig::default()
                            })
                            .await
                            .unwrap();
                    }
                }
            };
            tokio::pin!(drainer);
            let read = accepted.read.into_async_read();
            let write = accepted.write.into_async_write();
            // The accepted lane's rtp session supervisor owns the session
            // drivers; poll it from the select loop below so a panicked
            // driver terminates the server instead of being silently dropped.
            let supervisor = accepted.supervisor;
            tokio::pin!(supervisor);
            let config = mux::MuxConfig {
                initiation: mux::Initiation::Server,
                heartbeat_interval: Duration::from_secs(5),
                frame_reassembly: false,
            };
            let mut spawner = tokio::task::JoinSet::new();
            let (_opener, mut accepter) =
                mux::spawn_mux_no_reconnection(read, write, config, &mut spawner);
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    () = &mut supervisor => break,
                    () = &mut drainer => {
                        panic!("accept drainer finished before the message-latency scenario completed");
                    }
                    accepted = accepter.accept() => {
                        match accepted {
                            Ok((stream_read, stream_write)) => {
                                let latency_tx = latency_tx.clone();
                                let delivered = std::sync::Arc::clone(&delivered_for_handlers);
                                handlers.spawn(parse_message_latency_stream(
                                    stream_read,
                                    stream_write,
                                    latency_tx,
                                    delivered,
                                    base,
                                ));
                            }
                            Err(_) => break,
                        }
                    }
                    Some(joined) = handlers.join_next(), if !handlers.is_empty() => {
                        joined.unwrap();
                    }
                    Some(joined) = spawner.join_next() => {
                        let error = joined.unwrap();
                        mux_session.record_error(&error);
                        break;
                    }
                }
            }
            // Drain remaining handler/supervision joins so panics surface.
            while let Some(result) = handlers.join_next().await {
                result.unwrap();
            }
            while let Some(result) = spawner.join_next().await {
                result.unwrap();
            }
        })
    });
    Ok((addr, latency_rx, delivered, mux_session))
}

/// Time-boxed sparse-message latency probe across the periodic hostile
/// bottleneck lanes. 64-byte timestamped messages are sent every 100 ms for
/// `NETEM_PERF_WINDOW_SECONDS` (30 s default) and one-way latency plus
/// delivery are recorded as p50/p95/p99; netem is sampled every 50 ms and a
/// four-second straggler allowance precedes sample collection. Only the
/// periodic 300/20/100 ms profiles are accepted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "loopback perf-ceiling probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn probe_hostile_message_latency() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut trace = PerfTrace::from_env();
            let window_seconds = std::env::var("NETEM_PERF_WINDOW_SECONDS")
                .map(|value| {
                    let seconds = value
                        .parse::<f64>()
                        .expect("NETEM_PERF_WINDOW_SECONDS must be a number");
                    assert!(seconds.is_finite() && seconds > 0.0);
                    seconds
                })
                .unwrap_or(30.0);
            let warmup_seconds = std::env::var("NETEM_PERF_WARMUP_SECONDS")
                .map(|value| {
                    let seconds = value
                        .parse::<f64>()
                        .expect("NETEM_PERF_WARMUP_SECONDS must be a number");
                    assert!(seconds.is_finite() && seconds >= 0.0);
                    seconds
                })
                .unwrap_or(5.0);
            let link_profile = std::env::var("NETEM_PERF_LINK_PROFILE")
                .unwrap_or_else(|_| "hostile-periodic-bottleneck-300ms".to_owned());
            assert!(
                matches!(
                    link_profile.as_str(),
                    "hostile-periodic-bottleneck"
                        | "hostile-periodic-bottleneck-300ms"
                        | "hostile-periodic-bottleneck-100ms"
                        | "hostile-periodic-bottleneck-20ms"
                ),
                "NETEM_PERF_LINK_PROFILE for the message-latency probe must be exactly 'hostile-periodic-bottleneck', 'hostile-periodic-bottleneck-300ms', 'hostile-periodic-bottleneck-100ms', or 'hostile-periodic-bottleneck-20ms', got {link_profile:?}"
            );
            let mss_bytes = std::env::var("NETEM_PERF_MSS_BYTES")
                .map(|value| {
                    let mss = value
                        .parse::<usize>()
                        .expect("NETEM_PERF_MSS_BYTES must be a positive usize");
                    assert!(mss > 0, "NETEM_PERF_MSS_BYTES must be positive");
                    mss
                })
                .unwrap_or(1400);
            // Paired run flags are parsed strictly: only 0|1|false|true are
            // accepted, so a typo cannot silently flip FEC or armor on a
            // timed run.
            let parse_flag_env = |name: &str| -> bool {
                match std::env::var(name).as_deref() {
                    Ok("0") | Ok("false") => false,
                    Ok("1") | Ok("true") => true,
                    Ok(other) => panic!(
                        "{name} must be exactly 0, 1, false, or true, got {other:?}"
                    ),
                    Err(_) => false,
                }
            };
            let fec = parse_flag_env("NETEM_PERF_FEC");
            let retransmission_armor = parse_flag_env("RTP_RTX_DUP");
            let make_link = || match link_profile.as_str() {
                "hostile-periodic-bottleneck" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck()
                }
                "hostile-periodic-bottleneck-300ms" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck_300ms()
                }
                "hostile-periodic-bottleneck-100ms" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck_100ms()
                }
                "hostile-periodic-bottleneck-20ms" => {
                    netem_test::kit::presets::hostile_periodic_bottleneck_20ms()
                }
                _ => unreachable!("link profile was validated above"),
            };
            let c2s_seed = std::env::var("NETEM_PERF_SEED")
                .map(|value| value.parse::<u64>().expect("NETEM_PERF_SEED must be a u64"))
                .unwrap_or(4);
            let s2c_seed = c2s_seed.wrapping_add(1);
            let mut c2s = make_link();
            c2s.seed = c2s_seed;
            let mut s2c = make_link();
            s2c.seed = s2c_seed;
            let c2s_description = format!("{c2s:?}");
            let s2c_description = format!("{s2c:?}");

            let base = Instant::now();
            let (server_addr, mut latencies, server_delivered, server_mux) =
                spawn_message_latency_server_via(
                    &task_tx,
                    fec,
                    mss_bytes,
                    base,
                    trace.as_ref().and_then(PerfTrace::rtp_peer_observer),
                )
                .await
                .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let (read, write) = rtp_connect_transient_observed(
                &task_tx,
                pair.client_addr(),
                fec,
                mss_bytes,
                trace.as_ref().and_then(PerfTrace::rtp_observer),
            )
            .await;
            let (opener, client_mux) = mux_client_connect_transient(&task_tx, read, write);

            // Open the stream under a generous timeout before we start the clock.
            let (mut stream_read, mut stream_write) = with_timeout(
                Duration::from_secs(30),
                "open mux stream for message latency",
                async { opener.open().await.unwrap() },
            )
            .await;
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            netem_test::kit::submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = stream_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            // Unmeasured warmup: let the live session reach steady state
            // before the measurement boundary is anchored on the shared trace
            // clock. Warmup latency samples are drained below and excluded
            // from the measurement window.
            let warmup_sent = send_timestamped_messages(
                &mut stream_write,
                base,
                MESSAGE_BYTES,
                MESSAGE_CADENCE,
                Duration::from_secs_f64(warmup_seconds),
            )
            .await;
            let mut warmup_received = 0u64;
            while let Ok(_latency) = latencies.try_recv() {
                warmup_received += 1;
            }
            let warmup_delivered = server_delivered.load(std::sync::atomic::Ordering::Relaxed);
            let start = Instant::now();
            // Anchor the measurement boundary on the shared trace clock so
            // netem/latency samples line up with the RTP endpoint rows.
            if let Some(trace) = trace.as_mut() {
                trace.mark_measurement_start(start);
            }
            let mut netem_tick = tokio::time::interval(Duration::from_millis(50));
            netem_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut sender: tokio::task::JoinSet<(u64, mux::StreamWriter)> =
                tokio::task::JoinSet::new();
            sender.spawn(async move {
                let sent = send_timestamped_messages(
                    &mut stream_write,
                    base,
                    MESSAGE_BYTES,
                    MESSAGE_CADENCE,
                    Duration::from_secs_f64(window_seconds),
                )
                .await;
                (sent, stream_write)
            });
            let window = tokio::time::sleep(Duration::from_secs_f64(window_seconds));
            tokio::pin!(window);
            let mut sender_error = None;
            let (measurement_sent, mut stream_write) = loop {
                tokio::select! {
                    joined = sender.join_next(), if !sender.is_empty() => {
                        // The sender ended before the window elapsed unless
                        // both ended together at the boundary.
                        let (sent, writer) = joined.expect("message sender exists").unwrap();
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed < window_seconds {
                            sender_error = Some(format!(
                                "message sender ended {elapsed:.3}s into the {window_seconds}s window"
                            ));
                        }
                        break (sent, writer);
                    }
                    _ = netem_tick.tick(), if trace.is_some() => {
                        trace.as_mut().unwrap().record_netem(
                            start.elapsed(),
                            pair.snapshot_c2s(),
                            pair.snapshot_s2c(),
                            server_delivered.load(std::sync::atomic::Ordering::Relaxed)
                                - warmup_delivered,
                        );
                    }
                    _ = &mut window => {
                        // The window elapsed; the sender ends around the same
                        // time — join it for the final count.
                        let (sent, writer) = sender
                            .join_next()
                            .await
                            .expect("message sender exists")
                            .unwrap();
                        break (sent, writer);
                    }
                }
            };
            let _ = stream_write.shutdown();
            // Allow four seconds for stragglers before draining the samples.
            tokio::time::sleep(MESSAGE_GRACE).await;
            let mut samples = Vec::new();
            while let Ok(latency) = latencies.try_recv() {
                samples.push(latency);
            }
            let received = samples.len() as u64;
            let delivered = server_delivered.load(std::sync::atomic::Ordering::Relaxed)
                - warmup_delivered;
            let elapsed = start.elapsed();
            assert!(
                measurement_sent > 0,
                "probe must send messages, got {measurement_sent}"
            );
            assert!(
                received > 0,
                "probe must deliver messages, got {received}"
            );
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p50 = netem_test::kit::stats::percentile(&samples, 0.50);
            let p95 = netem_test::kit::stats::percentile(&samples, 0.95);
            let p99 = netem_test::kit::stats::percentile(&samples, 0.99);
            let delivery_pct = received as f64 / measurement_sent as f64;
            eprintln!(
                "[perf] message latency {link_profile}: sent={measurement_sent} recv={received} delivery={delivery_pct:.3} p50={p50:.1} p95={p95:.1} p99={p99:.1} ms bytes={delivered}"
            );

            if let Some(trace) = trace {
                let revision = std::env::var("NETEM_PERF_REVISION")
                    .unwrap_or_else(|_| "unspecified".to_owned());
                let output_dir = trace
                    .finish(&[
                        (
                            "scenario",
                            format!("mux_over_rtp_{link_profile}_message_latency_window"),
                        ),
                        ("link_profile", link_profile.clone()),
                        ("revision", revision),
                        ("seed", c2s_seed.to_string()),
                        ("window_seconds", window_seconds.to_string()),
                        ("warmup_seconds", warmup_seconds.to_string()),
                        ("warmup_messages_sent", warmup_sent.to_string()),
                        ("warmup_messages_received", warmup_received.to_string()),
                        ("messages_sent", measurement_sent.to_string()),
                        ("messages_received", received.to_string()),
                        ("message_delivery_percent", delivery_pct.to_string()),
                        ("message_latency_p50_ms", p50.to_string()),
                        ("message_latency_p95_ms", p95.to_string()),
                        ("message_latency_p99_ms", p99.to_string()),
                        (
                            "measurement_end_reason",
                            if sender_error.is_none() {
                                "timebox_elapsed".to_owned()
                            } else {
                                "sender_ended_early".to_owned()
                            },
                        ),
                        ("mss_bytes", mss_bytes.to_string()),
                        ("fec", fec.to_string()),
                        ("retransmission_armor", retransmission_armor.to_string()),
                        ("rtp_handshake", "false".to_owned()),
                        ("netem_sample_interval_micros", "50000".to_owned()),
                        ("delivered_bytes", delivered.to_string()),
                        ("elapsed_seconds", elapsed.as_secs_f64().to_string()),
                        ("netem_c2s_seed", c2s_seed.to_string()),
                        ("netem_s2c_seed", s2c_seed.to_string()),
                        (
                            "probe_outcome",
                            sender_error.clone().unwrap_or_else(|| "completed".to_owned()),
                        ),
                        ("client_mux_outcome", client_mux.outcome().as_label()),
                        ("server_mux_outcome", server_mux.outcome().as_label()),
                        ("netem_c2s", c2s_description),
                        ("netem_s2c", s2c_description),
                    ])
                    .expect("write perf trace");
                eprintln!("[trace] {}", output_dir.display());
            }

            pair.stop();
            let stats = combined_stats(&pair);
            eprintln!("[stats] {stats:?}");
            assert!(
                stats.forwarded > 0 && stats.dropped > 0,
                "periodic lossy link should forward and drop packets, got {stats:?}"
            );
            if let Some(error) = sender_error {
                panic!("{error}; failed after {:.3} s", elapsed.as_secs_f64());
            }
        })
        .await;
}
