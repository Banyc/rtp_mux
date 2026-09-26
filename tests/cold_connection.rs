//! Cold-connection cost decomposition: bare rtp session birth vs mux pairing.
//!
//! A proxy-path iteration measured the deployed chain's start-of-window cost
//! — connection establishment — and found 7-8 base RTTs of cold-connection
//! round trip on the direct (proxy-free) arm as well, attributing it to
//! rtp_mux's dual-lane birth. This target decomposes that number against
//! rtp's own public API so the RTTs break into named parts, and gates the one
//! part rtp_mux owns:
//!
//! - `rtp handshake, one lane` — a bare `rtp::udp::FrameDeliveryIo::connect`
//!   with the opening handshake on, against a raw `rtp::udp::Listener`.
//! - `rtp no handshake, one lane` — the same connect with the handshake off;
//!   the difference is what the rtp opening handshake itself costs.
//! - `rtp handshake, two lanes sequential` — two bare sessions (on their own
//!   listeners, as production's two lane addresses are) dialed one after the
//!   other.
//! - `rtp handshake, two lanes concurrent` — the same two sessions dialed
//!   together, the order a concurrent lane birth would use.
//! - `mux pairing on established rtp` — two already-established rtp
//!   sessions, then the mux lane-hello / pairing / first-frame readiness
//!   added on top.
//! - `full production cold connect` — `RtpMuxConnector` against
//!   `RtpMuxServer`, the deployed path, for the whole number.
//!
//! The gate is `full < sequential`: the production dual-lane birth must not
//! pay two *serialized* rtp opening handshakes when both lanes are independent
//! and can be dialed together. Two sanity assertions make that non-vacuous:
//! the instrument must still see the serialization (`sequential` visibly
//! slower than `concurrent`), and the production birth cannot beat its own
//! rtp floor. `RTP_MUX_COLD_CONNECTION_FAULT=serialize` reproduces the
//! pre-fix critical path (sequential bare dials plus the mux pairing) in place
//! of the production arm, and must fail the gate.
//!
//! Every regime calibrates its achieved base RTT on the same clock and through
//! the same instrument, independently of rtp (a datagram to a plain UDP echo
//! server behind an identical `NetemPair`). The measurements are **loopback**:
//! they say what the protocol spends in round trips, not what a real path's
//! delay distribution does to a real client.

use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration, time::Instant};

use netem_test::kit::presets::clean_delay_link;
use netem_test::kit::{
    TEST_TASK_QUEUE_BOUND, TestScope, TestTaskSubmitter, submit_test_task,
    submit_test_task_required,
};
use netem_test::{NetemConfig, NetemPair};
use rtp_mux::{
    BindSelector, BulkAddrSelector, ExplorerConfig, FecTuning, LaneClass, RtpMuxConnector,
    RtpMuxConnectorConfig, RtpMuxServer, RtpMuxServerConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Environment selector for the vacuity injection: `serialize` replaces the
/// production cold-connect arm with a reproduction of the pre-fix critical
/// path (two sequential bare rtp dials plus the mux pairing), which the gate
/// below must reject.
const FAULT_ENV: &str = "RTP_MUX_COLD_CONNECTION_FAULT";

// ── regimes and repetitions ─────────────────────────────────────────────────

struct Regime {
    name: &'static str,
    owd: Duration,
    /// The predecessor's measured cold direct connection at this scale (ms).
    observed_ms: f64,
}

const REGIMES: [Regime; 2] = [
    Regime {
        name: "owd20 / nominal rtt40",
        owd: Duration::from_millis(20),
        observed_ms: 386.0,
    },
    Regime {
        name: "owd96 / nominal rtt192",
        owd: Duration::from_millis(96),
        observed_ms: 1310.0,
    },
];

/// Cold connections per arm per regime. The reported statistics are the
/// **median** (the gate, so both sides of the comparison are the same
/// quantile of the same distribution) and the **minimum** (the
/// least-contended cold connection, which is what a client on a good path
/// sees). The rtp opening handshake carries a random pre-handshake sleep, so
/// neither statistic is the mean.
const REPS: usize = 9;

// ── lane-faithful rtp configs ───────────────────────────────────────────────
//
// These mirror `crate::lane_transport`'s interactive-lane policy (FEC on,
// prompt tuning, receiver-side fast-forward, `Shared` congestion intent). The
// lane policy lives in rtp_mux's private module, so the test restates it; the
// pieces that can move a handshake cost (handshake flag, frame delivery,
// congestion lane) are what matter here — FEC parity rides with data, not
// with the handshake.

fn interactive_connect_config(handshake: bool) -> rtp::udp::ConnectConfig<'static> {
    rtp::udp::ConnectConfig {
        handshake,
        fec: true,
        fec_tuning: FecTuning::interactive_prompt(),
        frame_delivery: rtp::FrameMode::enabled_reordering(),
        congestion_lane: rtp::CongestionLane::Shared,
        ..rtp::udp::ConnectConfig::default()
    }
}

fn interactive_accept_config() -> rtp::udp::AcceptConfig {
    rtp::udp::AcceptConfig {
        fec: true,
        fec_tuning: FecTuning::interactive_prompt(),
        frame_delivery: rtp::FrameMode::enabled_reordering(),
        congestion_lane: rtp::CongestionLane::Shared,
        ..rtp::udp::AcceptConfig::default()
    }
}

fn client_mux_config() -> mux::MuxConfig {
    mux::MuxConfig {
        initiation: mux::Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}

fn server_mux_config() -> mux::MuxConfig {
    mux::MuxConfig {
        initiation: mux::Initiation::Server,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}

// ── raw rtp listener ────────────────────────────────────────────────────────

/// A raw `rtp::udp::Listener` whose accept loop hands each fully-established
/// session (handshake included when `handshake`) to the returned receiver.
/// The accept future is submitted separately, never awaited inline, so the
/// listener keeps dispatching datagrams while a handshake runs.
async fn raw_rtp_listener(
    task_tx: &TestTaskSubmitter,
    handshake: bool,
    accept_config: rtp::udp::AcceptConfig,
) -> io::Result<(SocketAddr, mpsc::Receiver<rtp::udp::FrameDeliveryIo>)> {
    let listener = Arc::new(
        rtp::udp::Listener::bind("127.0.0.1:0", rtp::udp::ListenerConfig::default()).await?,
    );
    let addr = listener.local_addr();
    let (tx, rx) = mpsc::channel(64);
    let task_tx = task_tx.clone();
    submit_test_task_required(
        &task_tx.clone(),
        "raw rtp accept loop",
        Box::pin(async move {
            loop {
                let accept = if handshake {
                    listener
                        .accept_frame_delivery_with_handshake(accept_config.clone())
                        .await
                } else {
                    listener.accept_frame_delivery(accept_config.clone()).await
                };
                let Ok(accept) = accept else { break };
                let tx = tx.clone();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        if let Ok(io) = accept.await {
                            let _ = tx.send(io).await;
                        }
                    }),
                );
            }
        }),
    );
    Ok((addr, rx))
}

/// One regime-fresh raw lane: the listener behind a `NetemPair`, plus the
/// receiver whose channel buffer keeps every established session alive.
struct RawLane {
    proxy_addr: SocketAddr,
    pair: NetemPair,
    _sessions: mpsc::Receiver<rtp::udp::FrameDeliveryIo>,
}

async fn raw_lane(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
    handshake: bool,
) -> io::Result<RawLane> {
    let (server, sessions) =
        raw_rtp_listener(task_tx, handshake, interactive_accept_config()).await?;
    let pair = NetemPair::spawn(server, c2s, s2c)?;
    Ok(RawLane {
        proxy_addr: pair.client_addr(),
        pair,
        _sessions: sessions,
    })
}

impl RawLane {
    fn stop(&self) {
        self.pair.stop();
    }
}

async fn rtp_connect(addr: SocketAddr, handshake: bool) -> io::Result<rtp::udp::FrameDeliveryIo> {
    rtp::udp::FrameDeliveryIo::connect("127.0.0.1:0", addr, interactive_connect_config(handshake))
        .await
}

// ── mux-pairing-on-established-rtp lanes ────────────────────────────────────

/// Two raw lanes (their own listeners and pairs, as production's two lane
/// addresses are) whose sessions are lane-hello-paired by nonce, exactly as
/// the production accept path pairs them.
async fn pairing_lanes(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
) -> io::Result<(RawLane, RawLane)> {
    let (server_int, sessions_int) =
        raw_rtp_listener(task_tx, true, interactive_accept_config()).await?;
    let (server_bulk, sessions_bulk) =
        raw_rtp_listener(task_tx, true, interactive_accept_config()).await?;
    let pair_int = NetemPair::spawn(server_int, c2s.clone(), s2c.clone())?;
    let pair_bulk = NetemPair::spawn(server_bulk, c2s, s2c)?;
    let lane_int = RawLane {
        proxy_addr: pair_int.client_addr(),
        pair: pair_int,
        _sessions: mpsc::channel(1).1,
    };
    let lane_bulk = RawLane {
        proxy_addr: pair_bulk.client_addr(),
        pair: pair_bulk,
        _sessions: mpsc::channel(1).1,
    };
    let (lane_tx, mut lane_rx) = mpsc::channel::<mux::UnpairedLane>(4);
    for mut sessions in [sessions_int, sessions_bulk] {
        let lane_tx = lane_tx.clone();
        let accept_tx = task_tx.clone();
        submit_test_task_required(
            &task_tx.clone(),
            "mux pairing lane accept",
            Box::pin(async move {
                let task_tx = accept_tx;
                let mut held = Vec::new();
                while let Some(io) = sessions.recv().await {
                    let rtp::udp::FrameDeliveryIo {
                        mut read,
                        mut write,
                        supervisor,
                        ..
                    } = io;
                    held.push(supervisor);
                    let lane_tx = lane_tx.clone();
                    submit_test_task(
                        &task_tx,
                        Box::pin(async move {
                            // Mirror the production accept path: read the lane
                            // hello under a deadline, answer with the birth
                            // heartbeat, then spawn the lane's server-side mux
                            // session and hand the unpaired lane to the registry.
                            let hello = tokio::time::timeout(
                                Duration::from_secs(3),
                                mux::read_lane_hello(&mut read),
                            )
                            .await;
                            let Ok(Ok((class, nonce, group))) = hello else {
                                return;
                            };
                            if mux::write_liveness_heartbeat(&mut write).await.is_err() {
                                return;
                            }
                            let mut lane_tasks = JoinSet::new();
                            let (opener, accepter) = mux::spawn_mux_no_reconnection(
                                read,
                                write,
                                server_mux_config(),
                                &mut lane_tasks,
                            );
                            let lane = mux::UnpairedLane::new(
                                class, nonce, group, opener, accepter, lane_tasks,
                            );
                            let _ = lane_tx.send(lane).await;
                        }),
                    );
                }
            }),
        );
    }
    submit_test_task_required(
        &task_tx.clone(),
        "mux pairing lane pair",
        Box::pin(async move {
            let mut pending: HashMap<mux::PairingNonce, mux::UnpairedLane> = HashMap::new();
            let mut finished: JoinSet<[u8; 0]> = JoinSet::new();
            while let Some(lane) = lane_rx.recv().await {
                let nonce = lane.nonce;
                if let Some(other) = pending.remove(&nonce) {
                    let mut set = JoinSet::new();
                    let _ = mux::complete_pairing(other, lane, &mut set);
                    finished.spawn(async move {
                        while let Some(joined) = set.join_next().await {
                            joined.unwrap();
                        }
                        []
                    });
                } else {
                    pending.insert(nonce, lane);
                }
            }
        }),
    );
    Ok((lane_int, lane_bulk))
}

/// Write both lane hellos, spawn both client mux sessions, and await their
/// first-frame readiness. Returns `false` if either lane never became ready.
/// The caller owns the timing.
async fn run_client_pairing(
    int_io: rtp::udp::FrameDeliveryIo,
    bulk_io: rtp::udp::FrameDeliveryIo,
) -> bool {
    let nonce = mux::PairingNonce::generate();
    let group = mux::GroupToken::generate();
    let rtp::udp::FrameDeliveryIo {
        read: int_read,
        write: mut int_write,
        supervisor: int_sup,
        ..
    } = int_io;
    let rtp::udp::FrameDeliveryIo {
        read: bulk_read,
        write: mut bulk_write,
        supervisor: bulk_sup,
        ..
    } = bulk_io;
    if mux::write_lane_hello(&mut int_write, mux::LaneClass::Interactive, nonce, group)
        .await
        .is_err()
    {
        return false;
    }
    let _ = int_write.flush().await;
    if mux::write_lane_hello(&mut bulk_write, mux::LaneClass::Bulk, nonce, group)
        .await
        .is_err()
    {
        return false;
    }
    let _ = bulk_write.flush().await;
    let mut int_tasks = JoinSet::new();
    let (int_opener, int_accepter, int_ready) =
        mux::spawn_mux_no_reconnection_with_first_receive_deadline_and_ready(
            int_read,
            int_write,
            client_mux_config(),
            Duration::from_secs(5),
            &mut int_tasks,
        );
    let mut bulk_tasks = JoinSet::new();
    let (bulk_opener, bulk_accepter, bulk_ready) =
        mux::spawn_mux_no_reconnection_with_first_receive_deadline_and_ready(
            bulk_read,
            bulk_write,
            client_mux_config(),
            Duration::from_secs(5),
            &mut bulk_tasks,
        );
    let mut supervisor = JoinSet::new();
    let (_opener, _accepter) = mux::spawn_dual_mux_paired_supervised(
        int_opener,
        int_accepter,
        int_tasks,
        bulk_opener,
        bulk_accepter,
        bulk_tasks,
        &mut supervisor,
    );
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::try_join!(int_ready, bulk_ready).is_ok()
    })
    .await
    .unwrap_or(false);
    drop((int_sup, bulk_sup, supervisor, _opener, _accepter));
    ready
}

// ── production rtp_mux server ───────────────────────────────────────────────

/// The production server, whose handler discards each accepted stream (this
/// arm measures only the client-side cold birth).
async fn spawn_production_server(
    task_tx: &TestTaskSubmitter,
) -> io::Result<(SocketAddr, SocketAddr)> {
    let task_tx = task_tx.clone();
    let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default()).await?;
    let interactive = server.listener().local_addr();
    let bulk = server.bulk_listener().local_addr();
    submit_test_task_required(
        &task_tx.clone(),
        "production rtp_mux server",
        Box::pin(async move {
            let spawner = rtp_mux::SessionSpawner::new({
                let task_tx = task_tx.clone();
                move |fut| submit_test_task(&task_tx, fut)
            });
            let _ = server
                .serve(spawner, move |stream| {
                    let task_tx = task_tx.clone();
                    submit_test_task(
                        &task_tx,
                        Box::pin(async move {
                            let (mut reader, _writer) = tokio::io::split(stream);
                            let mut buf = [0u8; 4096];
                            while let Ok(n) = reader.read(&mut buf).await {
                                if n == 0 {
                                    break;
                                }
                            }
                        }),
                    );
                })
                .await;
        }),
    );
    Ok((interactive, bulk))
}

fn cold_production_connector(
    task_tx: &TestTaskSubmitter,
    bulk_proxy_addr: SocketAddr,
) -> RtpMuxConnector {
    let bind: BindSelector = Arc::new(|_| "0.0.0.0:0".parse().unwrap());
    let bulk_addr: BulkAddrSelector = Arc::new(move |_| Ok(bulk_proxy_addr));
    let (connector, driver) = RtpMuxConnector::with_config(RtpMuxConnectorConfig {
        bulk_addr,
        explorer: ExplorerConfig {
            enabled: false,
            ..ExplorerConfig::default()
        },
        ..RtpMuxConnectorConfig::standard(bind)
    });
    submit_test_task(task_tx, Box::pin(driver));
    connector
}

// ── arms ────────────────────────────────────────────────────────────────────

/// One bare rtp session per repetition, handshake on or off.
async fn arm_one_lane(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
    handshake: bool,
) -> Vec<f64> {
    let mut samples = Vec::new();
    for _ in 0..REPS {
        let lane = raw_lane(task_tx, c2s.clone(), s2c.clone(), handshake)
            .await
            .unwrap();
        let started = Instant::now();
        let io = rtp_connect(lane.proxy_addr, handshake).await.unwrap();
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
        drop(io);
        lane.stop();
    }
    samples
}

/// Two bare rtp sessions, on their own lanes, dialed one after the other or
/// together.
async fn arm_two_lanes(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
    concurrent: bool,
) -> Vec<f64> {
    let mut samples = Vec::new();
    for _ in 0..REPS {
        let int_lane = raw_lane(task_tx, c2s.clone(), s2c.clone(), true)
            .await
            .unwrap();
        let bulk_lane = raw_lane(task_tx, c2s.clone(), s2c.clone(), true)
            .await
            .unwrap();
        let (int_addr, bulk_addr) = (int_lane.proxy_addr, bulk_lane.proxy_addr);
        let started = Instant::now();
        let (first, second) = if concurrent {
            tokio::join!(rtp_connect(int_addr, true), rtp_connect(bulk_addr, true))
        } else {
            let first = rtp_connect(int_addr, true).await;
            let second = rtp_connect(bulk_addr, true).await;
            (first, second)
        };
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
        drop((first.unwrap(), second.unwrap()));
        int_lane.stop();
        bulk_lane.stop();
    }
    samples
}

/// The mux lane-hello / pairing / first-frame readiness on top of two
/// already-established rtp sessions.
async fn arm_mux_pairing(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
) -> Vec<f64> {
    let mut samples = Vec::new();
    for _ in 0..REPS {
        let (int_lane, bulk_lane) = pairing_lanes(task_tx, c2s.clone(), s2c.clone())
            .await
            .unwrap();
        let int_io = rtp_connect(int_lane.proxy_addr, true).await.unwrap();
        let bulk_io = rtp_connect(bulk_lane.proxy_addr, true).await.unwrap();
        let started = Instant::now();
        let ready = run_client_pairing(int_io, bulk_io).await;
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
        assert!(ready, "the mux pairing arm never became ready");
        int_lane.stop();
        bulk_lane.stop();
    }
    samples
}

/// The production cold connect: a fresh connector with no cached session.
async fn arm_production(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
) -> Vec<f64> {
    let mut samples = Vec::new();
    for _ in 0..REPS {
        let (interactive, bulk) = spawn_production_server(task_tx).await.unwrap();
        let int_pair = NetemPair::spawn(interactive, c2s.clone(), s2c.clone()).unwrap();
        let bulk_pair = NetemPair::spawn(bulk, c2s.clone(), s2c.clone()).unwrap();
        let connector = cold_production_connector(task_tx, bulk_pair.client_addr());
        let started = Instant::now();
        let stream = connector
            .connect_stream_with_lane(int_pair.client_addr(), LaneClass::Interactive)
            .await
            .unwrap();
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
        drop(stream);
        int_pair.stop();
        bulk_pair.stop();
    }
    samples
}

/// The vacuity injection's arm: the pre-fix critical path reproduced from the
/// same public pieces — two *sequential* bare rtp dials, then the mux pairing
/// on top — timed as one cold birth.
async fn arm_serialized_birth_surrogate(
    task_tx: &TestTaskSubmitter,
    c2s: NetemConfig,
    s2c: NetemConfig,
) -> Vec<f64> {
    let mut samples = Vec::new();
    for _ in 0..REPS {
        let (int_lane, bulk_lane) = pairing_lanes(task_tx, c2s.clone(), s2c.clone())
            .await
            .unwrap();
        let started = Instant::now();
        let int_io = rtp_connect(int_lane.proxy_addr, true).await.unwrap();
        let bulk_io = rtp_connect(bulk_lane.proxy_addr, true).await.unwrap();
        let ready = run_client_pairing(int_io, bulk_io).await;
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
        assert!(ready, "the serialized-birth surrogate never became ready");
        int_lane.stop();
        bulk_lane.stop();
    }
    samples
}

// ── link calibration ────────────────────────────────────────────────────────

/// Calibrate the regime's achieved base RTT on the same clock and through the
/// same instrument, independently of rtp: one datagram out to a plain UDP echo
/// server behind an identical `NetemPair`, one datagram back, minimum of N.
fn calibrate_link_rtt(c2s: NetemConfig, s2c: NetemConfig, samples: usize) -> f64 {
    use std::net::UdpSocket;
    let echo = UdpSocket::bind("127.0.0.1:0").unwrap();
    echo.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let echo_addr = echo.local_addr().unwrap();
    let pair = NetemPair::spawn(echo_addr, c2s, s2c).unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let mut best = f64::INFINITY;
    for _ in 0..samples {
        let started = Instant::now();
        client.send_to(&[7u8], pair.client_addr()).unwrap();
        let mut buf = [0u8; 8];
        // The echo server runs on this thread: forward the datagram it
        // receives back through the pair by hand, so no extra thread is needed.
        let (len, from) = match echo.recv_from(&mut buf) {
            Ok(v) => v,
            Err(_) => break,
        };
        echo.send_to(&buf[..len], from).unwrap();
        let mut back = [0u8; 8];
        if client.recv_from(&mut back).is_err() {
            break;
        }
        best = best.min(started.elapsed().as_secs_f64() * 1000.0);
    }
    pair.stop();
    best
}

// ── reporting ───────────────────────────────────────────────────────────────

fn minimum(samples: &[f64]) -> f64 {
    samples.iter().cloned().fold(f64::INFINITY, f64::min)
}

/// The middle sample of the arm's reps (the arms are compared at the same
/// quantile, so the comparison is not a race between two different extremes).
fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    sorted[sorted.len() / 2]
}

fn report(name: &str, samples: &[f64], base_rtt: f64) {
    println!(
        "  {name:<34} median {:8.1} ms  min {:8.1} ms  [spread {:7.1} .. {:7.1}]  {:.2} x base RTT",
        median(samples),
        minimum(samples),
        minimum(samples),
        samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        median(samples) / base_rtt,
    );
}

// ── the gate ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "cold-connection decomposition; six arms over two RTT regimes; run with --ignored --nocapture --test-threads=1"]
async fn cold_connection_decomposition() {
    let fault = std::env::var(FAULT_ENV).ok();
    let fault_serialize = fault.as_deref() == Some("serialize");
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let mut failures: Vec<String> = Vec::new();
    tasks
        .run(async {
            for regime in &REGIMES {
                let c2s = clean_delay_link(regime.owd, 11);
                let s2c = clean_delay_link(regime.owd, 12);
                println!(
                    "\n=== {} | predecessor cold direct {:.0} ms | fault={:?} ===",
                    regime.name, regime.observed_ms, fault,
                );
                let base_rtt = calibrate_link_rtt(c2s.clone(), s2c.clone(), 7);
                println!("  calibrated base RTT: {:.1} ms", base_rtt);

                let one_lane = arm_one_lane(&task_tx, c2s.clone(), s2c.clone(), true).await;
                report("rtp handshake, one lane", &one_lane, base_rtt);
                let no_handshake = arm_one_lane(&task_tx, c2s.clone(), s2c.clone(), false).await;
                report("rtp no handshake, one lane", &no_handshake, base_rtt);
                let sequential = arm_two_lanes(&task_tx, c2s.clone(), s2c.clone(), false).await;
                report("rtp handshake, 2 lanes sequential", &sequential, base_rtt);
                let concurrent = arm_two_lanes(&task_tx, c2s.clone(), s2c.clone(), true).await;
                report("rtp handshake, 2 lanes concurrent", &concurrent, base_rtt);
                let production = if fault_serialize {
                    arm_serialized_birth_surrogate(&task_tx, c2s.clone(), s2c.clone()).await
                } else {
                    arm_production(&task_tx, c2s.clone(), s2c.clone()).await
                };
                report(
                    if fault_serialize {
                        "fault: sequential birth surrogate"
                    } else {
                        "full production cold connect"
                    },
                    &production,
                    base_rtt,
                );

                let one = median(&one_lane);
                let none = median(&no_handshake);
                let seq = median(&sequential);
                let conc = median(&concurrent);
                let prod = median(&production);
                // The serialization's own cost: what the second, sequential
                // handshake adds over dialing both lanes together.
                let serialization = seq - conc;
                let residue = prod - conc;
                println!(
                    "  accounting: one rtp handshake {one:.1} ms (of which the handshake is {:.1} ms \
                     over a no-handshake control of {none:.1} ms); two sequential handshakes {seq:.1} ms; \
                     production residue over the concurrent rtp floor {residue:.1} ms of the \
                     {serialization:.1} ms the serialization costs (the residue is the mux lane birth + \
                     stream open, attributed by `mux_lane_birth_is_one_round_trip`)",
                    one - none,
                );

                // Instrument sanity and vacuity guard: the two bare arms must
                // still separate serialized from concurrent dials, or the gate
                // below cannot see the defect it exists to catch.
                if seq <= conc * 1.3 {
                    failures.push(format!(
                        "[{}] the sequential bare-lane arm ({seq:.1} ms) is not visibly slower than \
                         the concurrent one ({conc:.1} ms); the instrument cannot distinguish a \
                         serialized lane birth, so the gate below is vacuous",
                        regime.name,
                    ));
                }
                // The production birth cannot beat its own rtp floor.
                if prod <= conc {
                    failures.push(format!(
                        "[{}] the production cold connect ({prod:.1} ms) beat the concurrent bare rtp \
                         floor ({conc:.1} ms), which is impossible: the production birth contains it",
                        regime.name,
                    ));
                }
                // The gate: the production birth's residue over the concurrent
                // rtp floor must stay well below what the serialization costs.
                // Three quarters of the serialization is the widest bound that
                // still rejects it: the residue is the mux lane birth (one base
                // RTT, pinned by `mux_lane_birth_is_one_round_trip`), while the
                // serialization is a whole second opening handshake.
                if residue >= 0.75 * serialization {
                    failures.push(format!(
                        "[{}] the production cold connect ({prod:.1} ms) sits {residue:.1} ms above the \
                         concurrent bare rtp floor ({conc:.1} ms), at least three quarters of the \
                         {serialization:.1} ms a second *serialized* opening handshake costs ({seq:.1} 
                         ms sequential); the two lanes are independent and must be dialed together",
                        regime.name,
                    ));
                }
            }
        })
        .await;
    assert!(
        failures.is_empty(),
        "cold-connection gate failed at {} regime(s):\n  - {}",
        failures.len(),
        failures.join("\n  - "),
    );
}

/// The mux lane birth's own cost, isolated from the rtp opening handshakes it
/// normally rides behind: on two *already-established* rtp sessions, the lane
/// hello / pairing / first-frame readiness is one base RTT (the hello out, the
/// server's birth heartbeat back). This pins the mux-side contribution the
/// decomposition above attributes, and moves independently of rtp's handshake.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mux lane birth on established rtp sessions at two RTT regimes; run with --ignored --nocapture --test-threads=1"]
async fn mux_lane_birth_is_one_round_trip() {
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let mut failures: Vec<String> = Vec::new();
    tasks
        .run(async {
            for regime in &REGIMES {
                let c2s = clean_delay_link(regime.owd, 11);
                let s2c = clean_delay_link(regime.owd, 12);
                let base_rtt = calibrate_link_rtt(c2s.clone(), s2c.clone(), 7);
                let nominal = 2.0 * regime.owd.as_secs_f64() * 1000.0;
                println!(
                    "\n=== {} | calibrated base RTT {:.1} ms (nominal {:.1} ms) ===",
                    regime.name, base_rtt, nominal,
                );
                let pairing = arm_mux_pairing(&task_tx, c2s.clone(), s2c.clone()).await;
                report("mux pairing on established rtp", &pairing, base_rtt);
                let mux = median(&pairing);
                let rtts = mux / base_rtt;
                println!("  mux lane birth: {mux:.1} ms = {rtts:.2} x base RTT");

                // Instrument sanity: the calibration must agree with the
                // configured one-way delay, or the ratio below is meaningless.
                if !(0.8..=1.2).contains(&(base_rtt / nominal)) {
                    failures.push(format!(
                        "[{}] the calibrated base RTT ({base_rtt:.1} ms) is more than 20 % off the \
                         configured round trip ({nominal:.1} ms); the instrument is not measuring the \
                         regime it claims",
                        regime.name,
                    ));
                }
                // The lane birth is one round trip: the hello reaches the
                // server, the birth heartbeat reaches the client.
                if !(0.6..=1.6).contains(&rtts) {
                    failures.push(format!(
                        "[{}] the mux lane birth on established rtp sessions cost {mux:.1} ms = \
                         {rtts:.2} base RTTs, not the one round trip (0.6..1.6) of the hello / \
                         birth-heartbeat exchange",
                        regime.name,
                    ));
                }
            }
        })
        .await;
    assert!(
        failures.is_empty(),
        "mux lane-birth gate failed at {} regime(s):\n  - {}",
        failures.len(),
        failures.join("\n  - "),
    );
}
