#![allow(clippy::disallowed_methods)]

//! The dual-lane listeners are usable only as an adjacent pair: the connector
//! derives the bulk destination as the interactive destination's port plus one
//! (`rtp_mux::shared::bulk_lane_addr`), so a server that ends up with a
//! non-adjacent pair is unreachable on its bulk lane.
//!
//! A caller asking for port `0` asks for *an* ephemeral interactive port, not
//! for a particular one. Concurrent binders therefore race each other: the
//! port one binder draws for its interactive lane is the port another binder
//! derives for its bulk lane. Each binder must still come away holding both
//! listeners on adjacent ports — only the interactive port may move.
//!
//! The host is loaded with held ephemeral ports so that the race is not left
//! to chance: a fraction of every draw has a held neighbour, which is the
//! condition that made the bind fail before the pair was acquired as a unit.

use rtp_mux::{RtpMuxServer, RtpMuxServerConfig};

/// Held ports, as a fraction of the host's ephemeral range, are the chance
/// that any one draw lands next to a port another process already owns.
const HELD_PORTS: usize = 256;
const BINDERS: usize = 8;
const ROUNDS: usize = 64;

/// Every concurrent binder acquires `ROUNDS` adjacent pairs while the others
/// (and the held ports) occupy ephemeral ports around it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_ephemeral_binds_each_obtain_an_adjacent_pair() {
    let mut held = Vec::with_capacity(HELD_PORTS);
    for _ in 0..HELD_PORTS {
        held.push(
            tokio::net::UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("hold an ephemeral port"),
        );
    }
    let mut binder_tasks = tokio::task::JoinSet::new();
    for binder in 0..BINDERS {
        binder_tasks.spawn(async move {
            for round in 0..ROUNDS {
                let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default())
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "binder {binder} round {round}: an ephemeral dual-lane pair \
                             could not be acquired: {error}"
                        )
                    });
                let interactive = server.listener().local_addr();
                let bulk = server.bulk_listener().local_addr();
                assert_eq!(
                    bulk.port(),
                    interactive.port() + 1,
                    "the bulk lane is addressed as the interactive port plus one, so a \
                     non-adjacent pair is unreachable on the bulk lane",
                );
                assert_eq!(
                    bulk.ip(),
                    interactive.ip(),
                    "both lanes of a pair must be bound on the same interface",
                );
            }
        });
    }
    while let Some(joined) = binder_tasks.join_next().await {
        joined.expect("a binder task panicked");
    }
}
