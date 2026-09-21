//! `mux` over `rtp` over [`netem_test::NetemPair`] — clean and latency-only
//! echo scenarios.
//!
//! Verifies the stream multiplexer layered on the reliable UDP transport
//! delivers a multiplexed stream intact over a clean link and over a
//! latency-only impaired link.
//!
//! Runs in the default gate (seeded, sub-second):
//!
//! ```sh
//! cargo test -p rtp_mux --test mux_over_rtp
//! ```

use std::time::Duration;

use mux::testkit::mux::mux_client_connect_via;
use netem_test::kit::payload::with_timeout;
use netem_test::kit::presets::clean;
use netem_test::kit::stats::combined_stats;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::rtp_connect_via;
use rtp_mux::testkit::mux_over_rtp::{mux_echo_round_trip, spawn_mux_over_rtp_echo_server_via};

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

/// `mux` over `rtp` should survive the netem proxy's added latency; `rtp`'s
/// reliable layer retransmits any lost datagrams so the multiplexed stream
/// arrives intact. A small payload is used because larger transfers can trip
/// rtp's broken-pipe heuristic when mux's control-plane stalls the ACK path.
#[tokio::test(flavor = "multi_thread")]
async fn mux_over_rtp_survives_netem_latency() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_mux_over_rtp_echo_server_via(&task_tx, false)
                .await
                .unwrap();

            let impaired = NetemConfig {
                latency: Duration::from_millis(20),
                seed: 42,
                ..NetemConfig::default()
            };
            let pair = NetemPair::spawn(server_addr, impaired.clone(), impaired).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;
            let opener = mux_client_connect_via(&task_tx, read, write);

            let payload = b"mux-over-rtp-through-netem";
            let got = with_timeout(
                Duration::from_secs(15),
                "mux-over-rtp latency echo",
                mux_echo_round_trip(&opener, payload),
            )
            .await;
            assert_eq!(got, payload, "mux stream must deliver all data intact");

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert!(stats.forwarded > 0, "proxy should forward packets");
}
