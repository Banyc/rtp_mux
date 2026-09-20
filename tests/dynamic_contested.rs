//! Dynamic-packet-size latency/bulk battery. Measures how the mux
//! dual-lane facade's static sticky routing misclassifies a realistic
//! mixed-size latency flow (200 B messages with 1-in-16 bursts of
//! 4–64 KiB). Five arms isolate the failure modes.
//!
//! Run with:
//! ```sh
//! cargo test --release -p rtp_mux --test dynamic_contested -- --ignored --nocapture --test-threads=1
//! ```
//!
//! # Traffic model
//!
//! Latency messages every 25 ms, 200 B each, except 1-in-16 drawn
//! uniformly 4..=64 KiB. Bulk chunks drawn 64..=512 KiB continuously.
//! Seeds: `MSG_SEED=0xD15E+rep`, `BULK_SEED=0xB01D+rep`. Record
//! small-message and burst-message latencies separately; percentiles
//! over per-message one-way latencies, median over reps.

use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use mux::testkit::mux::{
    mux_client_connect_via, spawn_mux_gaming_latency_bulk_server_via,
    spawn_mux_latency_bulk_server_via,
};
use mux::{DeliveryMode, DualMessageSender, LaneClass, MigratingStreamWriter};
use netem_test::kit::contested::{DynTrafficResult, dyn_run_secs, summarize};
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::prng::SplitMix64;
use netem_test::kit::stats::percentile;
use netem_test::kit::submit_test_task;
use netem_test::{BottleneckShaper, NetemConfig, NetemPair};
use rtp::testkit::rtp::rtp_connect_with_mss_via;
use rtp_mux::testkit::dual::{
    dual_mux_client_connect_via, spawn_dual_msg_channel_server_via,
    spawn_dual_mux_gaming_latency_bulk_server_via, spawn_dual_mux_latency_bulk_server_via,
    spawn_dual_mux_migrating_latency_bulk_server_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MSG_SEED_BASE: u64 = 0xD15E;
const BULK_RAMP: Duration = Duration::from_millis(1500);
const LATENCY_CADENCE: Duration = Duration::from_millis(25);
const SMALL_MSG_BYTES: usize = 200;
const BURST_RATIO: u64 = 16;
const RATE_BPS: u64 = 400 * 1024 * 8;

fn dyn_reps() -> usize {
    std::env::var("DYN_REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Shared bottleneck config
//
// The rate lives on a BottleneckShaper shared across all lanes; the per-pair
// configs carry only loss/latency/jitter/limit and have rate=0 so that
// spawn_shared does not panic with "double-shape".
// ═══════════════════════════════════════════════════════════════════════════════

fn bottleneck_config(seed: u64, _rate_bps: u64) -> (NetemConfig, NetemConfig) {
    let loss = ((2.0 / 100.0) * u32::MAX as f64).clamp(0.0, u32::MAX as f64) as u32;
    (
        NetemConfig {
            rate: 0,
            loss,
            latency: Duration::from_millis(25),
            jitter: Duration::from_millis(20),
            queue_limit_pkts: 4096,
            seed,
            ..NetemConfig::default()
        },
        NetemConfig {
            rate: 0,
            loss,
            latency: Duration::from_millis(25),
            jitter: Duration::from_millis(20),
            queue_limit_pkts: 4096,
            seed: seed + 1,
            ..NetemConfig::default()
        },
    )
}

/// Budget for one rep of one arm. Generous enough for a clean run, tight
/// enough that a hang surfaces instead of parking the thread forever.
const REP_TIMEOUT: Duration = Duration::from_secs(120);
const GAMING_REP_TIMEOUT: Duration = Duration::from_secs(30);

// ═══════════════════════════════════════════════════════════════════════════════
// Results and helpers
// ═══════════════════════════════════════════════════════════════════════════════

const LATENCY_TAG: &[u8] = b"L";
const BULK_TAG: &[u8] = b"B";

fn make_latency_frame(msg_size: usize, base: Instant) -> Vec<u8> {
    assert!(msg_size >= 12, "msg_size {msg_size} too small for framing");
    let sent_us = base.elapsed().as_micros() as u64;
    let frame_len = msg_size as u32;
    let payload_bytes = msg_size - 12;
    let mut frame = Vec::with_capacity(msg_size);
    frame.extend_from_slice(&frame_len.to_le_bytes());
    frame.resize(4 + payload_bytes, b'X');
    frame.extend_from_slice(&sent_us.to_le_bytes());
    frame
}

async fn run_latency_flow(
    base: Instant,
    seed_base: u64,
    run_for: Duration,
    lat_write: &mut (impl tokio::io::AsyncWrite + Unpin),
    lat_rx: &mut tokio::sync::mpsc::Receiver<f64>,
    tag_written: bool,
) -> (Vec<f64>, Vec<f64>, u64) {
    let mut msg_rng = SplitMix64::new(MSG_SEED_BASE + seed_base);
    let mut small_latencies = Vec::new();
    let mut burst_latencies = Vec::new();
    let mut sent = 0u64;
    let start = Instant::now();
    while start.elapsed() < run_for {
        let msg_size = if msg_rng.next_u64().is_multiple_of(BURST_RATIO) {
            msg_rng.uniform_usize(4 * 1024, 64 * 1024)
        } else {
            SMALL_MSG_BYTES
        };
        let is_burst = msg_size > SMALL_MSG_BYTES;
        let frame = make_latency_frame(msg_size, base);
        let write_buf = if !tag_written && sent == 0 {
            let mut tagged = Vec::with_capacity(LATENCY_TAG.len() + frame.len());
            tagged.extend_from_slice(LATENCY_TAG);
            tagged.extend_from_slice(&frame);
            tagged
        } else {
            frame
        };
        if lat_write.write_all(&write_buf).await.is_err() {
            break;
        }
        sent += 1;
        match lat_rx.recv().await {
            Some(lat) => {
                if is_burst {
                    burst_latencies.push(lat);
                } else {
                    small_latencies.push(lat);
                }
            }
            None => break,
        }
        tokio::time::sleep(LATENCY_CADENCE).await;
    }
    (small_latencies, burst_latencies, sent)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm A: single-mux baseline
// ═══════════════════════════════════════════════════════════════════════════════

async fn dyn_single_mux_rep(seed_base: u64, run_secs: u64) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(100 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_mux_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();

            let (connected_read, connected_write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, rtp::udp::NO_FEC_MSS)
                    .await;
            let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);

            let (mut _lat_read, mut lat_write) = opener.open().await.unwrap();
            let (mut bulk_read, mut bulk_write) = opener.open().await.unwrap();

            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = _lat_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let _ = bulk_write.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = bulk_write.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = bulk_write.shutdown();
                });
            }
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            let body = async {
                let (small, burst, sent) =
                    run_latency_flow(base, seed_base, run_for, &mut lat_write, &mut lat_rx, false)
                        .await;
                let _ = lat_write.shutdown();
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let received = (small.len() + burst.len()) as u64;

                DynTrafficResult {
                    small_latencies: small,
                    burst_latencies: burst,
                    sent,
                    received,
                    bulk_bytes,
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_single_mux() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_single_mux rep",
                dyn_single_mux_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("single_mux (A)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm B: dual-lane, sticky auto, first write small → interactive
// ═══════════════════════════════════════════════════════════════════════════════

async fn dyn_dual_auto_small_first_rep(seed_base: u64, run_secs: u64) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(100 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_mux_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let _ = w.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            let body = async {
                tokio::time::sleep(BULK_RAMP).await;

                let (auto_reader, mut auto_writer) = opener.open_auto();
                let (small, burst, sent) = run_latency_flow(
                    base,
                    seed_base,
                    run_for,
                    &mut auto_writer,
                    &mut lat_rx,
                    false,
                )
                .await;
                let _ = auto_writer.shutdown();
                drop(auto_reader);
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let received = (small.len() + burst.len()) as u64;

                DynTrafficResult {
                    small_latencies: small,
                    burst_latencies: burst,
                    sent,
                    received,
                    bulk_bytes,
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_auto_small_first() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_auto_small_first rep",
                dyn_dual_auto_small_first_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("dual_auto_small_first (B)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm C: dual-lane, sticky auto, first write forced burst → bulk
// ═══════════════════════════════════════════════════════════════════════════════

async fn dyn_dual_auto_big_first_rep(seed_base: u64, run_secs: u64) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(100 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_mux_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let _ = w.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            let body = async {
                tokio::time::sleep(BULK_RAMP).await;

                let (auto_reader, mut auto_writer) = opener.open_auto();

                // FIRST write is forced to be large (> 2 KiB) so auto classifies as Bulk.
                let first_size: usize = 4 * 1024;
                let first_frame = make_latency_frame(first_size, base);
                let mut first_buf = Vec::with_capacity(LATENCY_TAG.len() + first_frame.len());
                first_buf.extend_from_slice(LATENCY_TAG);
                first_buf.extend_from_slice(&first_frame);
                if auto_writer.write_all(&first_buf).await.is_err() {
                    let _ = auto_writer.shutdown();
                    drop(auto_reader);
                    return DynTrafficResult {
                        small_latencies: vec![],
                        burst_latencies: vec![],
                        sent: 0,
                        received: 0,
                        bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                    };
                }
                if let Some(lat) = lat_rx.recv().await {
                    let rest_run = run_for.saturating_sub(base.elapsed());
                    let (small, mut burst, mut sent) = run_latency_flow(
                        base,
                        seed_base,
                        rest_run,
                        &mut auto_writer,
                        &mut lat_rx,
                        true,
                    )
                    .await;
                    burst.insert(0, lat);
                    sent += 1;
                    let _ = auto_writer.shutdown();
                    drop(auto_reader);
                    let received = (small.len() + burst.len()) as u64;
                    return DynTrafficResult {
                        small_latencies: small,
                        burst_latencies: burst,
                        sent,
                        received,
                        bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                    };
                }

                let _ = auto_writer.shutdown();
                drop(auto_reader);
                DynTrafficResult {
                    small_latencies: vec![],
                    burst_latencies: vec![],
                    sent: 1,
                    received: 0,
                    bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_auto_big_first() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_auto_big_first rep",
                dyn_dual_auto_big_first_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("dual_auto_big_first (C)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm D: dual-lane, fresh open_auto stream per message
// ═══════════════════════════════════════════════════════════════════════════════

async fn dyn_dual_auto_per_message_rep(seed_base: u64, run_secs: u64) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(100 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_mux_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let _ = w.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            tokio::time::sleep(BULK_RAMP).await;

            let body = async {
                let mut msg_rng = SplitMix64::new(MSG_SEED_BASE + seed_base);
                let mut small_latencies = Vec::new();
                let mut burst_latencies = Vec::new();
                let mut sent = 0u64;
                let start = Instant::now();

                while start.elapsed() < run_for {
                    sent += 1;
                    let msg_size = if msg_rng.next_u64().is_multiple_of(BURST_RATIO) {
                        msg_rng.uniform_usize(4 * 1024, 64 * 1024)
                    } else {
                        SMALL_MSG_BYTES
                    };
                    let is_burst = msg_size > SMALL_MSG_BYTES;
                    let frame = make_latency_frame(msg_size, base);

                    let (reader, mut writer) = opener.open_auto();
                    let write_buf = [LATENCY_TAG, &frame[..]].concat();
                    if writer.write_all(&write_buf).await.is_err() {
                        break;
                    }
                    let _ = writer.shutdown();
                    drop(reader);

                    match lat_rx.recv().await {
                        Some(lat) => {
                            if is_burst {
                                burst_latencies.push(lat);
                            } else {
                                small_latencies.push(lat);
                            }
                        }
                        None => break,
                    }

                    tokio::time::sleep(LATENCY_CADENCE).await;
                }

                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let received = (small_latencies.len() + burst_latencies.len()) as u64;

                DynTrafficResult {
                    small_latencies,
                    burst_latencies,
                    sent,
                    received,
                    bulk_bytes,
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_auto_per_message() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_auto_per_message rep",
                dyn_dual_auto_per_message_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("dual_auto_per_message (D)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm E: dual-lane, explicit LaneClass::Interactive hint
// ═══════════════════════════════════════════════════════════════════════════════

async fn dyn_dual_hint_static_rep(seed_base: u64, run_secs: u64) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(100 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_mux_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let _ = w.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            tokio::time::sleep(BULK_RAMP).await;

            let body = async {
                let (int_reader, mut int_writer) = opener
                    .open(LaneClass::Interactive)
                    .await
                    .expect("open interactive");
                int_writer
                    .write_all(LATENCY_TAG)
                    .await
                    .expect("write latency tag");

                let mut msg_rng = SplitMix64::new(MSG_SEED_BASE + seed_base);
                let mut small_latencies = Vec::new();
                let mut burst_latencies = Vec::new();
                let mut sent = 0u64;
                let start = Instant::now();

                while start.elapsed() < run_for {
                    sent += 1;
                    let msg_size = if msg_rng.next_u64().is_multiple_of(BURST_RATIO) {
                        msg_rng.uniform_usize(4 * 1024, 64 * 1024)
                    } else {
                        SMALL_MSG_BYTES
                    };
                    let is_burst = msg_size > SMALL_MSG_BYTES;
                    let frame = make_latency_frame(msg_size, base);

                    if is_burst {
                        if let Ok((_, mut bw)) = opener.open(LaneClass::Bulk).await {
                            let _ = bw.write_all(LATENCY_TAG).await;
                            if bw.write_all(&frame).await.is_err() {
                                break;
                            }
                            let _ = bw.shutdown();
                            match lat_rx.recv().await {
                                Some(lat) => burst_latencies.push(lat),
                                None => break,
                            }
                        }
                    } else {
                        if int_writer.write_all(&frame).await.is_err() {
                            break;
                        }
                        match lat_rx.recv().await {
                            Some(lat) => small_latencies.push(lat),
                            None => break,
                        }
                    }

                    tokio::time::sleep(LATENCY_CADENCE).await;
                }

                let _ = int_writer.shutdown();
                drop(int_reader);
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let received = (small_latencies.len() + burst_latencies.len()) as u64;

                DynTrafficResult {
                    small_latencies,
                    burst_latencies,
                    sent,
                    received,
                    bulk_bytes,
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_hint_static() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_hint_static rep",
                dyn_dual_hint_static_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("dual_hint_static (E)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm F: dual_message per-message routing (DualMessageSender / Receiver)
// ═══════════════════════════════════════════════════════════════════════════════

async fn dyn_dual_msg_channel_rep(
    seed_base: u64,
    run_secs: u64,
    mode: DeliveryMode,
) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(100 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_msg_channel_server_via(&task_tx, false, base, mode)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            tokio::time::sleep(BULK_RAMP).await;

            let body = async {
                let sender = DualMessageSender::new(opener, mode);
                let mut msg_rng = SplitMix64::new(MSG_SEED_BASE + seed_base);
                let mut small_latencies = Vec::new();
                let mut burst_latencies = Vec::new();
                let mut sent = 0u64;
                let start = Instant::now();

                while start.elapsed() < run_for {
                    let msg_size = if msg_rng.next_u64().is_multiple_of(BURST_RATIO) {
                        msg_rng.uniform_usize(4 * 1024, 64 * 1024)
                    } else {
                        SMALL_MSG_BYTES
                    };
                    let is_burst = msg_size > SMALL_MSG_BYTES;
                    let frame = make_latency_frame(msg_size, base);

                    if sender.send(&frame).await.is_err() {
                        break;
                    }
                    sent += 1;

                    match lat_rx.recv().await {
                        Some(lat) => {
                            if is_burst {
                                burst_latencies.push(lat);
                            } else {
                                small_latencies.push(lat);
                            }
                        }
                        None => break,
                    }

                    tokio::time::sleep(LATENCY_CADENCE).await;
                }

                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let received = (small_latencies.len() + burst_latencies.len()) as u64;

                DynTrafficResult {
                    small_latencies,
                    burst_latencies,
                    sent,
                    received,
                    bulk_bytes,
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_msg_channel() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_msg_channel rep",
                dyn_dual_msg_channel_rep(rep as u64, dyn_run_secs(), DeliveryMode::Unordered),
            )
            .await,
        );
    }
    summarize("dual_msg_channel (F Unordered)", &results);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_msg_channel_ordered() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_msg_channel_ordered rep",
                dyn_dual_msg_channel_rep(rep as u64, dyn_run_secs(), DeliveryMode::Ordered),
            )
            .await,
        );
    }
    summarize("dual_msg_channel (F Ordered)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm G: gaming-pattern — one stream, 3 MiB state-sync then 200 B deltas
// ═══════════════════════════════════════════════════════════════════════════════

const GAMING_SYNC_BYTES: usize = 8 * 1024;
const GAMING_TAG: &[u8] = b"G";
const GAMING_TRANSITION_SECS: u64 = 3;

struct GamingResult {
    transition_latencies: Vec<f64>,
    steady_latencies: Vec<f64>,
    sent: u64,
    received: u64,
    bulk_bytes: u64,
}

fn summarize_gaming(label: &str, results: &[GamingResult]) {
    let mut trans: Vec<f64> = results
        .iter()
        .flat_map(|r| r.transition_latencies.clone())
        .collect();
    let mut steady: Vec<f64> = results
        .iter()
        .flat_map(|r| r.steady_latencies.clone())
        .collect();
    trans.sort_by(|a, b| a.partial_cmp(b).unwrap());
    steady.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let delivery = if results.iter().map(|r| r.sent).sum::<u64>() == 0 {
        0.0
    } else {
        results.iter().map(|r| r.received).sum::<u64>() as f64
            / results.iter().map(|r| r.sent).sum::<u64>() as f64
    };
    let bulk_total: u64 = results.iter().map(|r| r.bulk_bytes).sum();
    let bulk_secs = results.len() as f64 * dyn_run_secs() as f64;

    eprintln!(
        "[gaming {label}] trans p50/p90/p99={:.0}/{:.0}/{:.0} ms  steady p50/p90/p99={:.0}/{:.0}/{:.0} ms  bulk={:.3} MiB/s  delivery={:.3}",
        percentile(&trans, 0.50),
        percentile(&trans, 0.90),
        percentile(&trans, 0.99),
        percentile(&steady, 0.50),
        percentile(&steady, 0.90),
        percentile(&steady, 0.99),
        bulk_total as f64 / (1024.0 * 1024.0) / bulk_secs,
        delivery,
    );

    let arms = [
        ("steady", steady.as_slice()),
        ("transition", trans.as_slice()),
    ];
    if let Ok(path) = netem_test::report::dump_csv(&format!("gaming_{label}"), &arms) {
        eprintln!("[gaming {label}] samples: {}", path.display());
    }
    eprintln!("{}", netem_test::report::ab_report(label, "ms", &arms));

    // Gaming arms: the sticky variant is expected to have very poor
    // delivery (deltas pinned to the congested bulk lane).  Require only
    // that SOME deltas arrive so the percentiles are meaningful; the
    // ordering comparison (migrating beats sticky) is the real gate.
    assert!(
        !steady.is_empty() || !trans.is_empty(),
        "[gaming {label}] no delta latencies recorded at all"
    );
    if !steady.is_empty() {
        assert!(
            percentile(&steady, 0.50) > 0.0,
            "[gaming {label}] steady p50 must be positive finite"
        );
    }
}

fn spawn_bulk_pump(
    tasks: &mut tokio::task::JoinSet<()>,
    opener: mux::DualStreamOpener,
    payload: Arc<Vec<u8>>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    tasks.spawn(async move {
        let (_, mut w) = match opener.open(LaneClass::Bulk).await {
            Ok(v) => v,
            Err(_) => return,
        };
        let _ = w.write_all(BULK_TAG).await;
        let mut offset = 0usize;
        loop {
            tokio::select! {
                _ = stop_rx.changed() => break,
                result = w.write(&payload[offset..]) => match result {
                    Ok(0) | Err(_) => break,
                    Ok(n) => offset = (offset + n) % payload.len(),
                },
            }
        }
        let _ = w.shutdown();
    });
}

async fn run_game_sync_client(
    seed_base: u64,
    run_secs: u64,
    opener: &mux::DualStreamOpener,
    mut lat_rx: tokio::sync::mpsc::Receiver<f64>,
    base: Instant,
    migrating: bool,
) -> GamingResult {
    let run_for = Duration::from_secs(run_secs);
    let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));

    // The bulk pump saturates the bulk lane for the whole measurement. It is
    // spawned up front (before the game-sync write) so the raced body below
    // does not need to touch `bulk_tasks`; the watch signals shutdown after
    // the measurement and the pump is joined in the epilog.
    let (bulk_stop_tx, bulk_stop_rx) = tokio::sync::watch::channel(false);
    let bulk_opener = opener.clone();
    let mut bulk_tasks = tokio::task::JoinSet::new();
    spawn_bulk_pump(
        &mut bulk_tasks,
        bulk_opener,
        Arc::clone(&payload),
        bulk_stop_rx.clone(),
    );

    let mut sync_buf = Vec::with_capacity(GAMING_TAG.len() + GAMING_SYNC_BYTES);
    sync_buf.extend_from_slice(GAMING_TAG);
    sync_buf.extend_from_slice(&payload[..GAMING_SYNC_BYTES]);

    let mut transition_latencies = Vec::new();
    let mut steady_latencies = Vec::new();
    let mut sent = 0u64;

    let body = async move {
        if migrating {
            let logical_id = seed_base;
            let mut game_writer = opener.open_migrating(logical_id, LaneClass::Interactive);
            if game_writer.write_all(&sync_buf).await.is_err() {
                let _ = game_writer.finalize().await;
                return GamingResult {
                    transition_latencies,
                    steady_latencies,
                    sent,
                    received: 0,
                    bulk_bytes: 0,
                };
            }
            tokio::time::sleep(BULK_RAMP).await;
            let start = Instant::now();
            let phase2_start = Instant::now();
            while start.elapsed() < run_for {
                let frame = make_latency_frame(SMALL_MSG_BYTES, base);
                if game_writer.write_all(&frame).await.is_err() {
                    break;
                }
                sent += 1;
                match tokio::time::timeout(run_for, lat_rx.recv()).await {
                    Ok(Some(lat)) => {
                        if phase2_start.elapsed().as_secs() < GAMING_TRANSITION_SECS {
                            transition_latencies.push(lat);
                        } else {
                            steady_latencies.push(lat);
                        }
                    }
                    _ => break,
                }
                tokio::time::sleep(LATENCY_CADENCE).await;
            }
            let _ = game_writer.finalize().await;
        } else {
            let (auto_reader, mut auto_writer) = opener.open_auto();
            if auto_writer.write_all(&sync_buf).await.is_err() {
                let _ = auto_writer.shutdown();
                drop(auto_reader);
                return GamingResult {
                    transition_latencies,
                    steady_latencies,
                    sent,
                    received: 0,
                    bulk_bytes: 0,
                };
            }
            tokio::time::sleep(BULK_RAMP).await;
            let start = Instant::now();
            let phase2_start = Instant::now();
            let mut iters = 0u32;
            while start.elapsed() < run_for {
                let frame = make_latency_frame(SMALL_MSG_BYTES, base);
                if let Ok(Ok(())) =
                    tokio::time::timeout(Duration::from_secs(5), auto_writer.write_all(&frame))
                        .await
                {
                    sent += 1;
                    if let Ok(Some(lat)) =
                        tokio::time::timeout(Duration::from_secs(5), lat_rx.recv()).await
                    {
                        if phase2_start.elapsed().as_secs() < GAMING_TRANSITION_SECS {
                            transition_latencies.push(lat);
                        } else {
                            steady_latencies.push(lat);
                        }
                    }
                }
                iters += 1;
                tokio::time::sleep(LATENCY_CADENCE).await;
            }
            let _ = iters;
            let _ = auto_writer.shutdown();
            drop(auto_reader);
        }

        let received = (transition_latencies.len() + steady_latencies.len()) as u64;
        GamingResult {
            transition_latencies,
            steady_latencies,
            sent,
            received,
            bulk_bytes: 0,
        }
    };
    tokio::pin!(body);
    let result = tokio::select! {
        joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
            // The bulk pump ended before the measurement completed: fail the
            // test instead of measuring against a dead upload.
            joined.expect("bulk pump exists").unwrap();
            panic!("bulk pump ended before the measurement completed");
        }
        result = &mut body => result,
    };
    bulk_stop_tx.send(true).unwrap();
    // Epilog: join the pump so any panic surfaces.
    while let Some(result) = bulk_tasks.join_next().await {
        result.unwrap();
    }
    result
}

async fn dyn_game_sync_sticky_rep(seed_base: u64, run_secs: u64) -> GamingResult {
    let (c2s, s2c) = bottleneck_config(200 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let (mut result, bulk_counter) = tasks
        .run(async {
            let (server_addr, lat_rx, bulk_counter) =
                spawn_dual_mux_gaming_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();
            let result =
                run_game_sync_client(seed_base, run_secs, &opener, lat_rx, base, false).await;
            (result, bulk_counter)
        })
        .await;
    result.bulk_bytes = bulk_counter.load(Ordering::Relaxed);
    result
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_game_sync_sticky() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                GAMING_REP_TIMEOUT,
                "dyn_game_sync_sticky rep",
                dyn_game_sync_sticky_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize_gaming("game_sticky (G)", &results);
}

async fn dyn_game_sync_migrating_rep(seed_base: u64, run_secs: u64) -> GamingResult {
    let (c2s, s2c) = bottleneck_config(300 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let (mut result, bulk_counter) = tasks
        .run(async {
            let (server_addr, lat_rx, bulk_counter) =
                spawn_dual_mux_gaming_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();
            let result =
                run_game_sync_client(seed_base, run_secs, &opener, lat_rx, base, true).await;
            (result, bulk_counter)
        })
        .await;
    result.bulk_bytes = bulk_counter.load(Ordering::Relaxed);
    result
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_game_sync_migrating() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                GAMING_REP_TIMEOUT,
                "dyn_game_sync_migrating rep",
                dyn_game_sync_migrating_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize_gaming("game_migrating (G-mig)", &results);
}

async fn dyn_game_sync_single_mux_rep(seed_base: u64, run_secs: u64) -> GamingResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(400 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_mux_gaming_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();

            let (connected_read, connected_write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, rtp::udp::NO_FEC_MSS)
                    .await;
            let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let (_, mut bulk_write) = opener.open().await.unwrap();
            let (mut bulk_read, _) = opener.open().await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let _ = bulk_write.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = bulk_write.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = bulk_write.shutdown();
                });
            }

            let (mut _game_read, mut game_write) = opener.open().await.unwrap();
            // Parked until the stream closes; the owning JoinSet aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = _game_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            let mut sync_buf = Vec::with_capacity(GAMING_TAG.len() + GAMING_SYNC_BYTES);
            sync_buf.extend_from_slice(GAMING_TAG);
            sync_buf.extend_from_slice(&payload[..GAMING_SYNC_BYTES]);

            let mut transition_latencies = Vec::new();
            let mut steady_latencies = Vec::new();
            let mut sent = 0u64;

            let body = async move {
                if game_write.write_all(&sync_buf).await.is_err() {
                    let _ = game_write.shutdown();
                    return GamingResult {
                        transition_latencies,
                        steady_latencies,
                        sent,
                        received: 0,
                        bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                    };
                }
                let start = Instant::now();
                let phase2_start = Instant::now();
                while start.elapsed() < run_for {
                    let frame = make_latency_frame(SMALL_MSG_BYTES, base);
                    if game_write.write_all(&frame).await.is_err() {
                        break;
                    }
                    sent += 1;
                    match tokio::time::timeout(run_for, lat_rx.recv()).await {
                        Ok(Some(lat)) => {
                            if phase2_start.elapsed().as_secs() < GAMING_TRANSITION_SECS {
                                transition_latencies.push(lat);
                            } else {
                                steady_latencies.push(lat);
                            }
                        }
                        _ => break,
                    }
                    tokio::time::sleep(LATENCY_CADENCE).await;
                }

                let _ = game_write.shutdown();
                let received = (transition_latencies.len() + steady_latencies.len()) as u64;
                GamingResult {
                    transition_latencies,
                    steady_latencies,
                    sent,
                    received,
                    bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_game_sync_single_mux() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                GAMING_REP_TIMEOUT,
                "dyn_game_sync_single_mux rep",
                dyn_game_sync_single_mux_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize_gaming("game_single_mux (G-1mux)", &results);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arm H: B/C migrating variants — open_migrating instead of open_auto
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_migrating_latency_flow(
    base: Instant,
    seed_base: u64,
    run_for: Duration,
    game_writer: &mut MigratingStreamWriter,
    lat_rx: &mut tokio::sync::mpsc::Receiver<f64>,
    tag_written: bool,
) -> (Vec<f64>, Vec<f64>, u64) {
    let mut msg_rng = SplitMix64::new(MSG_SEED_BASE + seed_base);
    let mut small_latencies = Vec::new();
    let mut burst_latencies = Vec::new();
    let mut sent = 0u64;
    let start = Instant::now();
    while start.elapsed() < run_for {
        let msg_size = if msg_rng.next_u64().is_multiple_of(BURST_RATIO) {
            msg_rng.uniform_usize(4 * 1024, 64 * 1024)
        } else {
            SMALL_MSG_BYTES
        };
        let is_burst = msg_size > SMALL_MSG_BYTES;
        let frame = make_latency_frame(msg_size, base);
        let write_buf = if !tag_written && sent == 0 {
            let mut tagged = Vec::with_capacity(LATENCY_TAG.len() + frame.len());
            tagged.extend_from_slice(LATENCY_TAG);
            tagged.extend_from_slice(&frame);
            tagged
        } else {
            frame
        };
        if game_writer.write_all(&write_buf).await.is_err() {
            break;
        }
        sent += 1;
        match lat_rx.recv().await {
            Some(lat) => {
                if is_burst {
                    burst_latencies.push(lat);
                } else {
                    small_latencies.push(lat);
                }
            }
            None => break,
        }
        tokio::time::sleep(LATENCY_CADENCE).await;
    }
    (small_latencies, burst_latencies, sent)
}

async fn dyn_dual_auto_small_first_migrating_rep(
    seed_base: u64,
    run_secs: u64,
) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(500 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_mux_migrating_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let _ = w.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            tokio::time::sleep(BULK_RAMP).await;

            let body = async {
                let logical_id = 1000 + seed_base;
                let mut game_writer = opener.open_migrating(logical_id, LaneClass::Interactive);
                let (small, burst, sent) = run_migrating_latency_flow(
                    base,
                    seed_base,
                    run_for,
                    &mut game_writer,
                    &mut lat_rx,
                    false,
                )
                .await;
                let _ = game_writer.finalize().await;
                let bulk_bytes = bulk_counter.load(Ordering::Relaxed);
                let received = (small.len() + burst.len()) as u64;

                DynTrafficResult {
                    small_latencies: small,
                    burst_latencies: burst,
                    sent,
                    received,
                    bulk_bytes,
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_auto_small_first_migrating() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_auto_small_first_migrating rep",
                dyn_dual_auto_small_first_migrating_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("dual_auto_small_first_migrating (B-mig)", &results);
}

async fn dyn_dual_auto_big_first_migrating_rep(seed_base: u64, run_secs: u64) -> DynTrafficResult {
    let run_for = Duration::from_secs(run_secs);
    let (c2s, s2c) = bottleneck_config(600 + seed_base, RATE_BPS);
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut bulk_tasks = tokio::task::JoinSet::new();

            let (server_addr, mut lat_rx, bulk_counter) =
                spawn_dual_mux_migrating_latency_bulk_server_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let c2s_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let s2c_shaper = BottleneckShaper::new(RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                server_addr,
                c2s.clone(),
                s2c.clone(),
                Some(c2s_shaper.clone()),
                Some(s2c_shaper.clone()),
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(server_addr, c2s, s2c, Some(c2s_shaper), Some(s2c_shaper))
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                false,
            )
            .await
            .unwrap();

            let payload = Arc::new(cyclic_payload(64 * 1024 * 1024));
            let (bulk_stop_tx, mut bulk_stop_rx) = tokio::sync::watch::channel(false);
            let bulk_opener = opener.clone();
            {
                let payload = Arc::clone(&payload);
                bulk_tasks.spawn(async move {
                    let (_, mut w) = match bulk_opener.open(LaneClass::Bulk).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let _ = w.write_all(BULK_TAG).await;
                    let mut offset = 0usize;
                    loop {
                        tokio::select! {
                            _ = bulk_stop_rx.changed() => break,
                            result = w.write(&payload[offset..]) => match result {
                                Ok(0) | Err(_) => break,
                                Ok(n) => offset = (offset + n) % payload.len(),
                            },
                        }
                    }
                    let _ = w.shutdown();
                });
            }

            tokio::time::sleep(BULK_RAMP).await;

            let body = async {
                let logical_id = 2000 + seed_base;
                let mut game_writer = opener.open_migrating(logical_id, LaneClass::Interactive);

                let first_size: usize = 4 * 1024;
                let first_frame = make_latency_frame(first_size, base);
                let mut first_buf = Vec::with_capacity(LATENCY_TAG.len() + first_frame.len());
                first_buf.extend_from_slice(LATENCY_TAG);
                first_buf.extend_from_slice(&first_frame);
                if game_writer.write_all(&first_buf).await.is_err() {
                    let _ = game_writer.finalize().await;
                    return DynTrafficResult {
                        small_latencies: vec![],
                        burst_latencies: vec![],
                        sent: 0,
                        received: 0,
                        bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                    };
                }
                if let Some(lat) = lat_rx.recv().await {
                    let rest_run = run_for.saturating_sub(base.elapsed());
                    let (small, mut burst, mut sent) = run_migrating_latency_flow(
                        base,
                        seed_base,
                        rest_run,
                        &mut game_writer,
                        &mut lat_rx,
                        true,
                    )
                    .await;
                    burst.insert(0, lat);
                    sent += 1;
                    let _ = game_writer.finalize().await;
                    let received = (small.len() + burst.len()) as u64;
                    return DynTrafficResult {
                        small_latencies: small,
                        burst_latencies: burst,
                        sent,
                        received,
                        bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                    };
                }

                let _ = game_writer.finalize().await;
                DynTrafficResult {
                    small_latencies: vec![],
                    burst_latencies: vec![],
                    sent: 1,
                    received: 0,
                    bulk_bytes: bulk_counter.load(Ordering::Relaxed),
                }
            };
            tokio::pin!(body);
            let result = tokio::select! {
                joined = bulk_tasks.join_next(), if !bulk_tasks.is_empty() => {
                    // The bulk pump ended before the measurement completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = &mut body => result,
            };
            bulk_stop_tx.send(true).unwrap();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = bulk_tasks.join_next().await {
                result.unwrap();
            }
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "dynamic-packet-size latency/bulk battery; slow real-time scenario; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn dyn_dual_auto_big_first_migrating() {
    let reps = dyn_reps();
    let mut results = Vec::new();
    for rep in 0..reps {
        results.push(
            with_timeout(
                REP_TIMEOUT,
                "dyn_dual_auto_big_first_migrating rep",
                dyn_dual_auto_big_first_migrating_rep(rep as u64, dyn_run_secs()),
            )
            .await,
        );
    }
    summarize("dual_auto_big_first_migrating (C-mig)", &results);
}
