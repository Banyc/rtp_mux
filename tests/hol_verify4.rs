//! `DualMux`-v4 bulk-lane A/B report-only probes: the **mux** bulk-lane arms.
//!
//! The dual-lane `DualMux` architecture intends its bulk lane to be a full
//! `mux` session over its own `rtp` connection. These report-only probes
//! measure the mux bulk lane on the same seeded netem links the harness's raw
//! `rtp` arms (`netem_test/tests/tests/hol_verify4.rs`) run, so the two
//! halves of the A/B comparison stay on identically-seeded links while each
//! crate owns its own arm.
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test hol_verify4 -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use mux::testkit::mux::mux_client_connect_via;
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::presets::{burst_loss_link, clean_delay_link};
use netem_test::kit::submit_test_task;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::rtp_connect_with_mss_via;
use rtp_mux::testkit::mux_over_rtp::spawn_mux_over_rtp_server_with_mss_via;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Wall-clock budget for each probe (1.5 s ramp + 15 s run + 3 s grace + slack).
const BULK_WINDOW: Duration = Duration::from_millis(19_500);

/// Post-window drain before stopping the pair: lets the in-flight bytes a
/// back-to-back pump left in the shaped link reach the sink, so the mux half
/// counts the same delivery window as the harness's raw arm (which drains
/// 3 s after closing its writer) — the two halves of the A/B comparison are
/// only comparable when they count the same window.
const BULK_DRAIN: Duration = Duration::from_secs(3);

/// Bulk write chunk size: 251-aligned (~256 KiB).
const CHUNK: usize = 262_044;

/// Spawn a mux-over-RTP server that accepts a single `mux` session and treats
/// every accepted stream as a deterministic bulk byte sink.
///
/// Returns the RTP listener address and an atomic counter that is incremented
/// by the number of bytes read from each stream until `Ok(0)`/Err. There is
/// no payload verification; all bytes are counted.
async fn spawn_mux_bulk_sink(
    tx: &netem_test::kit::TestTaskSubmitter,
) -> std::io::Result<(std::net::SocketAddr, Arc<AtomicU64>)> {
    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_for_server = Arc::clone(&delivered);
    let addr = spawn_mux_over_rtp_server_with_mss_via(
        tx,
        false,
        rtp::udp::NO_FEC_MSS,
        move |mut stream_read, mut stream_write| {
            let delivered_for_stream = Arc::clone(&delivered_for_server);
            async move {
                let mut buf = vec![0u8; 64 * 1024];
                while let Ok(n) = stream_read.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    delivered_for_stream.fetch_add(n as u64, Ordering::Relaxed);
                }
                let _ = stream_write.shutdown();
            }
        },
    )
    .await?;
    Ok((addr, delivered))
}

/// Open a full `mux` session through a `NetemPair`, then write a single bulk
/// stream of deterministic cyclic payload for `BULK_WINDOW`.
///
/// `label` is used only for logging. Returns the total bytes delivered at the
/// server-side sink.
async fn run_muxbulk(label: &str, c2s: NetemConfig, s2c: NetemConfig) -> u64 {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let (elapsed, delivered_at_window, total) = tasks
        .run(async {
            let (server_addr, delivered) = spawn_mux_bulk_sink(&task_tx).await.unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();

            let (connected_read, connected_write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, rtp::udp::NO_FEC_MSS)
                    .await;
            let opener = mux_client_connect_via(&task_tx, connected_read, connected_write);

            let (mut stream_read, mut stream_write) = opener.open().await.unwrap();

            // Drain the stream read half in the background so flow-control ACKs keep
            // moving and the writer does not stall. Parked for the bulk window; the
            // owning JoinSet aborts it at scope end.
            submit_test_task(
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

            let payload = cyclic_payload(CHUNK);
            let start = Instant::now();
            let stop = Arc::new(AtomicBool::new(false));
            while !stop.load(Ordering::Relaxed) {
                if start.elapsed() >= BULK_WINDOW {
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
                match stream_write.write_all(&payload[..CHUNK]).await {
                    Ok(()) => {}
                    Err(_) => break,
                }
            }
            let elapsed = start.elapsed();
            let delivered_at_window = delivered.load(Ordering::Relaxed);
            let _ = stream_write.shutdown();

            // End the client session so the server-side sink stops counting before
            // the pair is stopped. The required drain task completes (unobserved)
            // once the session ends, after the raced body returned; if the session
            // has not ended yet, scope drop aborts it. That is outside the raced
            // body, so it is not an early exit.
            drop(opener);
            // Let the in-flight bytes left in the shaped link reach the sink
            // before stopping the pair, matching the raw arm's post-window
            // drain so both halves measure the same delivery window.
            tokio::time::sleep(BULK_DRAIN).await;
            pair.stop();

            let total = delivered.load(Ordering::Relaxed);
            (elapsed, delivered_at_window, total)
        })
        .await;

    let mibps = total as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON);
    let mibps_window =
        delivered_at_window as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON);
    eprintln!(
        "[v4 {label}] delivered={total}B (at-window={delivered_at_window}B) \
         elapsed={elapsed:?} bulk={mibps:.3} MiB/s bulk-window={mibps_window:.3} MiB/s",
    );
    total
}

// ────────────────────────────── report-only probes ───────────────────────────

/// `v4` bulk lane over Gilbert-Elliott 5% burst loss (c2s seed 33, s2c seed
/// 44) — the mux half of the A/B comparison with the harness's
/// `v4_ge5_rawbulk` arm on identically-seeded links.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "DualMux-v4 bulk-lane A/B report-only probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn v4_ge5_muxbulk() {
    with_timeout(
        Duration::from_secs(120),
        "v4 ge5 muxbulk",
        run_muxbulk(
            "ge5 muxbulk",
            burst_loss_link(5.0, 3.0, Duration::from_millis(50), 33),
            burst_loss_link(5.0, 3.0, Duration::from_millis(50), 44),
        ),
    )
    .await;
}

/// `v4` bulk lane on a clean 50 ms RTT link (c2s seed 11, s2c seed 22) — the
/// mux half of the A/B comparison with the harness's `v4_clean_rawbulk` arm.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "DualMux-v4 bulk-lane A/B report-only probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn v4_clean_muxbulk() {
    with_timeout(
        Duration::from_secs(120),
        "v4 clean muxbulk",
        run_muxbulk(
            "clean muxbulk",
            clean_delay_link(Duration::from_millis(50), 11),
            clean_delay_link(Duration::from_millis(50), 22),
        ),
    )
    .await;
}
