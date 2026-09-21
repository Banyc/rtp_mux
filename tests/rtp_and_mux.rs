//! Small compatibility smoke test that exercises the `rtp` and
//! `mux`-over-`rtp` stacks end-to-end through a [`netem_test::NetemPair`]
//! bidirectional impairment proxy.
//!
//! This target is kept intentionally small and is a thin wrapper over the
//! shared helpers (`mux::testkit`, `rtp_mux::testkit::mux_over_rtp`,
//! `netem_test::kit`, `rtp::testkit`). The focused, comprehensive scenarios live
//! in the sibling test targets and in the harness crate (`raw_netem_pair.rs`,
//! `rtp_clean.rs`, `rtp_loss.rs`, `rtp_fec.rs`, `mux_over_rtp.rs`,
//! `mux_over_rtp_perf.rs`).
//!
//! Runs in the default gate (seeded, sub-second):
//!
//! ```sh
//! cargo test -p rtp_mux --test rtp_and_mux
//! ```

use std::time::Duration;

use mux::testkit::mux::mux_client_connect_via;
use netem_test::NetemPair;
use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::presets::{clean, mild_loss};
use netem_test::kit::stats::combined_stats;
use rtp::testkit::rtp::{rtp_connect_via, rtp_echo_payload, spawn_rtp_echo_server_via};
use rtp_mux::testkit::mux_over_rtp::{mux_echo_round_trip, spawn_mux_over_rtp_echo_server_via};

/// `rtp` should deliver a byte stream reliably over a *clean* netem link.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_over_netem_clean_link_delivers_data() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_via(&task_tx, false).await.unwrap();

            let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;

            let payload = b"netem-rtp-integration";
            let got = with_timeout(
                Duration::from_secs(10),
                "rtp clean small echo",
                rtp_echo_payload(read, write, payload),
            )
            .await;
            assert_eq!(got, payload);

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert_eq!(stats.dropped, 0, "clean link should not drop");
    assert!(stats.forwarded > 0, "proxy should forward packets");
}

/// `rtp`'s reliable layer should recover from mild packet loss introduced by
/// the netem proxy — the byte stream must arrive intact despite ~5% loss in
/// both directions.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_over_netem_reliability_survives_mild_loss() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_via(&task_tx, false).await.unwrap();

            let pair = NetemPair::spawn(server_addr, mild_loss(), mild_loss()).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;

            let payload = payload(256 * 1024);
            let got = with_timeout(
                Duration::from_secs(60),
                "rtp lossy 256KiB echo",
                rtp_echo_payload(read, write, &payload),
            )
            .await;
            assert_eq!(got, payload, "reliable layer must recover all data");

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert!(
        stats.dropped > 0,
        "proxy should have dropped some packets, got {stats:?}"
    );
}

/// `mux` layered on `rtp` should multiplex a stream over the netem-impaired
/// link and echo data back intact on a clean link.
#[tokio::test(flavor = "multi_thread")]
async fn mux_over_rtp_over_netem_clean_link_echoes() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_mux_over_rtp_echo_server_via(&task_tx, false)
                .await
                .unwrap();

            let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);

            let payload = b"mux-rtp-netem";
            let got = with_timeout(
                Duration::from_secs(15),
                "mux-over-rtp clean echo",
                mux_echo_round_trip(&opener, payload),
            )
            .await;
            assert_eq!(got, payload);

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert_eq!(stats.dropped, 0, "clean link should not drop");
    assert!(stats.forwarded > 0, "proxy should forward packets");
}
