//! The session's *life* under a latency spike: `rtp_mux` must survive a spike,
//! not reconnect through one.
//!
//! The deployment's client multiplexes everything over **one long-lived mux
//! session**, and the field's own round trips reach 3205 ms (and 1063 ms on
//! another run) on a ~190 ms floor. Those are **spikes on a live path**, not a
//! dead path, so the property that matters is the positive one: the session
//! and its streams **survive** the spike, a write in flight when the spike
//! begins completes when it ends, and a **reconnect during the spike** —
//! which pays the cold-establishment charge and, worse, runs rtp's 3 s opening
//! handshake into a 3.2 s round trip — must not happen.
//!
//! The `establishment` family measures a *cold* birth on a clean link; nothing
//! before this target holds a **live** session under the field's spike.
//!
//! # The instrument
//!
//! The pinned harness has no runtime latency setter (`NetemConfig.latency` is
//! fixed at spawn and `NetemLink` exposes only `set_blackout`). A spike is
//! therefore injected by **swapping the lane's pair on fixed bind addresses**:
//! the pair is stopped and respawned with the same client-side and server-side
//! socket addresses and a latency-only config carrying the spike, so every
//! address the app and the server know — the app's peer, the RTP 4-tuple, the
//! server's per-session peer — is unchanged and only the path's delay moves.
//! Measured: the swap itself takes ~5.6 ms, and the same session id is intact
//! after it (see the run below). This is the closest the harness can come to a
//! transient delay excursion; the alternative, an *unpinned* pair variant,
//! races its learned client destination and is not used.
//!
//! # The timers that could fire from slowness rather than from death
//!
//! Checked by reading the code, per timer:
//!
//! * **mux heartbeat / liveness deadline.** The reader enforces a *sliding*
//!   deadline measured from the last byte that arrived
//!   (`mux/src/central_io/reader.rs:31-35`), armed at
//!   `heartbeat_interval * 4` = **20 s** for the production 5 s heartbeat
//!   (`reader.rs:25,117`). A spike's ~3.6 s gap is 5.6x below it: it does not
//!   fire. This is the timer the spike is aimed at, and the arm is the proof it
//!   is not tripped.
//! * **mux heartbeat writer.** A keepalive is sent every
//!   `heartbeat_interval + jitter` (0..20 %), `central_io/encoder.rs:26,58`.
//!   A delayed write delays the heartbeat; it has no timeout of its own.
//! * **rtp repair ladder.** `MIN_RTO = 1 s` (`rtp/src/traffic_shaping/recovery/rto.rs:37`)
//!   and `TAIL_PROBED_MIN_RTO = 300 ms`
//!   (`.../recovery/tlp.rs:42`) are *retransmission cadences*, not session
//!   deadlines. A 3.6 s spike exceeds them, so rtp retransmits — which is the
//!   mechanism that keeps the stream alive — and there is **no max-retry or
//!   give-up path** in the reliable layer (no `MAX_RETR`/retry-limit constant
//!   exists in `rtp`). No threshold to fire.
//! * **rtp opening handshake.** `OPENING_TIMEOUT = 3 s`
//!   (`rtp/src/traffic_shaping/control/handshake/opening/mod.rs:13`). This is a
//!   *birth* deadline: a session **re-established during a 3.2 s spike cannot
//!   complete its handshake**. It is the mechanical reason a reconnect during a
//!   spike is worse than the spike, and why the identity guard below is the
//!   point of the unit. (The arm does not exercise birth-on-spike; it asserts
//!   that birth does not happen. A birth-on-spike arm belongs to `rtp`.)
//! * **rtp teardown.** `GRACEFUL_CLOSE_TIMEOUT = 675 s` and
//!   `DRIVER_JOIN_TIMEOUT = 3 s` (`rtp/src/socket/session.rs:28,35`) run only
//!   once a session is already closing, not on the live path.
//! * **proxy connection-pool heartbeat.** `PoolHeartbeat` sends a keepalive
//!   with a **30 s** write timeout (`HEARTBEAT_INTERVAL`,
//!   `proxy/common/src/stream_runtime/pool.rs:19,169-177`; `send_keep_alive`'s
//!   `tokio::time::timeout`,
//!   `proxy/common/src/header/preamble.rs:10-22`). A 3.6 s delayed write is
//!   8.3x below it. (It guards the proxy's pooled TCP hops, not the rtp_mux
//!   session; cited because it is a timer that could fire on slowness.)
//!
//! No timer in the stack fires on a spike of the field's magnitude; the
//! nearest is mux's 20 s receive deadline, 5.6x above the largest field spike.
//! The arm holds that property.
//!
//! Arms (one declared dimension, `spike`, apart from the baseline):
//!
//! | arm | spike | what it pins |
//! | --- | --- | --- |
//! | baseline | none (95 ms one-way floor) | a live session and its stream stay usable and keep one id |
//! | member | the field's 3205 ms round trip, injected mid-round | the in-flight write completes, a spike round observes the injected delay, the session id is unchanged, and no session is ever absent |
//!
//! Vacuity: `SPIKE_SURVIVAL_FAULT=no_spike` skips the injection (the
//! delay-matches-injection check fails); `SPIKE_SURVIVAL_FAULT=churn_session`
//! calls `RtpMuxConnector::reset()` mid-spike (the in-flight rounds error, so
//! the survival check fails); `SPIKE_SURVIVAL_FAULT=late_churn` resets the
//! connector after every round has passed, so only the identity guard can catch
//! it (it does: the session id goes absent).
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test spike_survival -- \
//!     --ignored --nocapture --test-threads=1
//! ```

use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use netem_test::kit::payload::payload;
use netem_test::kit::presets::clean_delay_link;
use netem_test::{NetemPair, StdUdpTransport, UdpTransport};
use rtp_mux::{
    BindSelector, BulkAddrSelector, ExplorerConfig, LaneClass, RtpMuxConnector,
    RtpMuxConnectorConfig, RtpMuxServer, RtpMuxServerConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The field's floor: a ~190 ms round trip, i.e. 95 ms one-way.
const FLOOR_OWD: Duration = Duration::from_millis(95);
/// The field's largest recorded round trip is 3205 ms; a round trip of that
/// size is `FLOOR_OWD + 1602 ms` one-way each way.
const SPIKE_OWD: Duration = Duration::from_millis(1602);
const FLOOR_RTT_MS: f64 = 2.0 * 95.0;
const SPIKE_RTT_MS: f64 = 2.0 * 1602.0;
const ROUND_BYTES: usize = 64;
const ROUND_TIMEOUT: Duration = Duration::from_secs(30);
/// Floor rounds before the spike, so the session is warm and the baseline is
/// visible in the log.
const WARM_ROUNDS: usize = 4;
const FAULT_ENV: &str = "SPIKE_SURVIVAL_FAULT";

fn fault() -> Option<String> {
    std::env::var(FAULT_ENV).ok()
}

fn free_addr() -> SocketAddr {
    format!(
        "127.0.0.1:{}",
        StdUdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    )
    .parse()
    .unwrap()
}

async fn spawn_echo_server_via(
    task_tx: &netem_test::kit::TestTaskSubmitter,
) -> io::Result<(SocketAddr, SocketAddr)> {
    let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default()).await?;
    let interactive_addr = server.listener().local_addr();
    let bulk_addr = server.bulk_listener().local_addr();
    netem_test::kit::submit_test_task_required(task_tx, "spike-survival echo server", {
        let task_tx = task_tx.clone();
        async move {
            let spawner = rtp_mux::SessionSpawner::new({
                let task_tx = task_tx.clone();
                move |fut| {
                    netem_test::kit::submit_test_task(&task_tx, fut);
                }
            });
            let _ = server
                .serve(spawner, {
                    let task_tx = task_tx.clone();
                    move |stream| {
                        let task_tx = task_tx.clone();
                        netem_test::kit::submit_test_task(
                            &task_tx,
                            Box::pin(async move {
                                let (mut reader, mut writer) = tokio::io::split(stream);
                                let _ = tokio::io::copy(&mut reader, &mut writer).await;
                                let _ = writer.shutdown().await;
                            }),
                        );
                    }
                })
                .await;
        }
    });
    Ok((interactive_addr, bulk_addr))
}

fn connector_via(
    task_tx: &netem_test::kit::TestTaskSubmitter,
    bulk_proxy_addr: SocketAddr,
) -> Arc<RtpMuxConnector> {
    let bind: BindSelector = Arc::new(|addr| match addr {
        SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
    });
    let bulk_addr: BulkAddrSelector = Arc::new(move |_| Ok(bulk_proxy_addr));
    let (connector, driver) = RtpMuxConnector::with_config(RtpMuxConnectorConfig {
        bulk_addr,
        explorer: ExplorerConfig {
            enabled: false,
            ..ExplorerConfig::default()
        },
        ..RtpMuxConnectorConfig::standard(bind)
    });
    netem_test::kit::submit_test_task(task_tx, Box::pin(driver));
    Arc::new(connector)
}

/// The lane pair, on fixed bind addresses so it can be respawned mid-session
/// with a different latency and every address preserved.
fn lane_pair(server: SocketAddr, cbind: SocketAddr, sbind: SocketAddr, owd: Duration) -> NetemPair {
    NetemPair::spawn_on(
        server,
        clean_delay_link(owd, 900),
        clean_delay_link(owd, 901),
        cbind,
        sbind,
    )
    .unwrap()
}

struct Round {
    label: &'static str,
    latency_ms: f64,
    outcome: Result<(), String>,
}

struct Measured {
    rounds: Vec<Round>,
    session_before: Option<u64>,
    session_after: Option<u64>,
    /// Every id the sampler saw while the spike was in force; `None` is an
    /// interval with no session at all (a rebirth).
    observed_ids: Vec<Option<u64>>,
    swap_ms: Option<f64>,
}

async fn round(stream: &mut rtp_mux::ClientStream, msg: &[u8]) -> Result<(), String> {
    let mut echo = vec![0u8; ROUND_BYTES];
    stream
        .write_all(msg)
        .await
        .map_err(|e| format!("write:{e}"))?;
    stream
        .read_exact(&mut echo)
        .await
        .map_err(|e| format!("read:{e}"))?;
    Ok(())
}

async fn timed(stream: &mut rtp_mux::ClientStream, msg: &[u8], label: &'static str) -> Round {
    let at = Instant::now();
    let outcome = tokio::time::timeout(ROUND_TIMEOUT, round(stream, msg))
        .await
        .unwrap_or_else(|_| Err("ROUND-TIMEOUT".into()));
    Round {
        label,
        latency_ms: at.elapsed().as_secs_f64() * 1000.0,
        outcome,
    }
}

async fn run_arm(spike_structure: bool, inject: bool) -> Measured {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let fault = fault();

    let (interactive_server, bulk_server) = spawn_echo_server_via(&task_tx).await.unwrap();
    let (ic, is, bc, bs) = (free_addr(), free_addr(), free_addr(), free_addr());
    let mut ip = lane_pair(interactive_server, ic, is, FLOOR_OWD);
    let bp = lane_pair(bulk_server, bc, bs, FLOOR_OWD);
    let connector = connector_via(&task_tx, bp.client_addr());
    let peer = ip.client_addr();

    let mut stream = tokio::time::timeout(
        Duration::from_secs(30),
        connector.connect_stream_with_lane(peer, LaneClass::Interactive),
    )
    .await
    .expect("the cold connect timed out")
    .expect("the cold connect failed");

    let msg = payload(ROUND_BYTES);
    let mut out = Measured {
        rounds: Vec::new(),
        session_before: None,
        session_after: None,
        observed_ids: Vec::new(),
        swap_ms: None,
    };
    for _ in 0..WARM_ROUNDS {
        out.rounds.push(timed(&mut stream, &msg, "floor").await);
    }
    let subject = connector.probe_session(peer).map(|v| v.id());
    out.session_before = subject;

    // Sample the session identity across the spike: a reconnect shows up as a
    // changed id or as an interval with no session at all.
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut sampler = tokio::task::JoinSet::new();
    sampler.spawn({
        let connector = Arc::clone(&connector);
        let observed = Arc::clone(&observed);
        async move {
            loop {
                observed
                    .lock()
                    .unwrap()
                    .push(connector.probe_session(peer).map(|v| v.id()));
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    });

    if spike_structure {
        // A round in flight when the spike begins: write, then swap the path
        // under it, then read. The reply (and possibly the request) crosses
        // the spiked path, so this round completes with the spike's delay.
        let mut echo = vec![0u8; ROUND_BYTES];
        let at = Instant::now();
        stream.write_all(&msg).await.unwrap();
        if inject {
            let swap_at = Instant::now();
            ip.stop();
            ip = lane_pair(interactive_server, ic, is, FLOOR_OWD + SPIKE_OWD);
            out.swap_ms = Some(swap_at.elapsed().as_secs_f64() * 1000.0);
        }
        let outcome: Result<(), String> =
            match tokio::time::timeout(ROUND_TIMEOUT, stream.read_exact(&mut echo)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(format!("read:{e}")),
                Err(_) => Err("read:ROUND-TIMEOUT".into()),
            };
        out.rounds.push(Round {
            label: "onset",
            latency_ms: at.elapsed().as_secs_f64() * 1000.0,
            outcome,
        });
        if fault.as_deref() == Some("churn_session") {
            let _ = connector.reset().await;
        }
        // A round wholly inside the spike.
        out.rounds.push(timed(&mut stream, &msg, "spike").await);
        // The path returns to the floor.
        if inject {
            ip.stop();
            ip = lane_pair(interactive_server, ic, is, FLOOR_OWD);
        }
        out.rounds.push(timed(&mut stream, &msg, "restored").await);
    } else {
        out.rounds.push(timed(&mut stream, &msg, "floor").await);
    }

    // `late_churn` resets the connector after every round has passed, so only
    // the identity guard can catch it. Scoped to the spike arm so the baseline
    // stays green and the fault is visibly targeted at the guard.
    if spike_structure && fault.as_deref() == Some("late_churn") {
        let _ = connector.reset().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    out.session_after = connector.probe_session(peer).map(|v| v.id());
    out.observed_ids = observed.lock().unwrap().clone();
    sampler.abort_all();
    ip.stop();
    bp.stop();
    out
}

fn report(arm: &str, m: &Measured) {
    println!("[spike-survival] arm={arm}");
    for r in &m.rounds {
        println!(
            "[spike-survival]   {}_round_ms={:.0} outcome={:?}",
            r.label, r.latency_ms, r.outcome
        );
    }
    let absent = m.observed_ids.iter().filter(|id| id.is_none()).count();
    let ids: std::collections::BTreeSet<_> = m.observed_ids.iter().cloned().collect();
    println!(
        "[spike-survival] session_before={:?} session_after={:?} swap_ms={:?} sampled={} absent={} distinct_ids={:?}",
        m.session_before,
        m.session_after,
        m.swap_ms,
        m.observed_ids.len(),
        absent,
        ids
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "latency-spike survival over NetemPair; ~3 s; run with --ignored --nocapture --test-threads=1"]
async fn a_floor_link_keeps_the_session_and_its_stream_usable() {
    let m = run_arm(false, false).await;
    report("baseline", &m);
    assert_eq!(m.rounds.len(), WARM_ROUNDS + 1);
    for r in &m.rounds {
        assert!(
            r.outcome.is_ok(),
            "a floor-link round must deliver: {}_round {:.0} ms outcome={:?}",
            r.label,
            r.latency_ms,
            r.outcome
        );
    }
    let mean: f64 = m.rounds.iter().map(|r| r.latency_ms).sum::<f64>() / m.rounds.len() as f64;
    // The floor's own 190 ms round trip, bound 1.6x above it.
    assert!(
        mean <= FLOOR_RTT_MS * 1.6,
        "the floor arm's mean round trip is {mean:.0} ms, above the {FLOOR_RTT_MS:.0} ms floor's \
        1.6x bound"
    );
    assert_eq!(
        m.session_before, m.session_after,
        "an unspiked floor session must keep its identity"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "latency-spike survival over NetemPair; ~15 s; run with --ignored --nocapture --test-threads=1"]
async fn a_field_magnitude_latency_spike_is_survived_without_a_reconnect() {
    let m = run_arm(true, fault().as_deref() != Some("no_spike")).await;
    report("spike", &m);
    let get = |label: &str| {
        m.rounds
            .iter()
            .find(|r| r.label == label)
            .unwrap_or_else(|| panic!("the {label} round did not run"))
    };
    // Every round delivers. This is the positive property: a write in flight
    // when the spike begins completes when it ends — no error, no EOF, no
    // lost stream.
    for r in &m.rounds {
        assert!(
            r.outcome.is_ok(),
            "a live session must carry every round through the spike: {}_round {:.0} ms \
             outcome={:?}",
            r.label,
            r.latency_ms,
            r.outcome
        );
    }
    // Both spike rounds observe the injected spike. The steady round is the
    // delay-match check (bound: the injected round trip + a floor grace, against
    // a measured 3396 ms for 3204 ms); the onset round spans the transition, so
    // it carries the request's floor leg, the swap and possibly a retransmission
    // and is bounded loosely — its job is the positive property (it completes)
    // plus "the spike was in force".
    let bounds = [
        ("onset", SPIKE_RTT_MS + 2000.0),
        ("spike", SPIKE_RTT_MS + 3.0 * FLOOR_RTT_MS),
    ];
    for (label, bound_ms) in bounds {
        let r = get(label);
        assert!(
            r.latency_ms >= FLOOR_RTT_MS * 2.0,
            "the {label} round took {:.0} ms, indistinguishable from a floor round \
             ({FLOOR_RTT_MS:.0} ms): the spike was not applied",
            r.latency_ms
        );
        assert!(
            r.latency_ms <= bound_ms,
            "the {label} round took {:.0} ms, above its {bound_ms:.0} ms bound for the \
             injected {SPIKE_RTT_MS:.0} ms spike round trip",
            r.latency_ms
        );
    }
    // Removing the spike returns the stream to the floor: the delay tracks the
    // path, so the arm is measuring the injection and not a stuck queue.
    let restored = get("restored");
    assert!(
        restored.latency_ms <= FLOOR_RTT_MS * 2.0,
        "the restored round took {:.0} ms, above the floor's 2x bound ({:.0} ms)",
        restored.latency_ms,
        FLOOR_RTT_MS * 2.0
    );
    // The inverse guard: the session identity must not move across the spike,
    // and there must be no interval with no session. A reconnect during a spike
    // is worse than the spike: it pays the cold-establishment charge, and rtp's
    // 3 s opening handshake cannot complete against a 3.2 s round trip.
    assert_eq!(
        m.session_before, m.session_after,
        "the session identity changed across a latency spike: the stack reconnected \
         through the spike (before {:?}, after {:?})",
        m.session_before, m.session_after
    );
    assert!(
        m.observed_ids.len() >= 20,
        "the identity sampler saw only {} intervals; it is not coverage",
        m.observed_ids.len()
    );
    let absent = m.observed_ids.iter().filter(|id| id.is_none()).count();
    assert_eq!(
        absent,
        0,
        "the connector had no session for {absent} of {} sampled intervals during the \
         spike: a new session was established",
        m.observed_ids.len()
    );
    let distinct: std::collections::BTreeSet<_> = m.observed_ids.iter().cloned().collect();
    assert_eq!(
        distinct.len(),
        1,
        "the sampler saw {} distinct session identities during the spike ({distinct:?}); \
         exactly one live session must survive it",
        distinct.len()
    );
    assert!(
        distinct.iter().next().unwrap().is_some(),
        "the only sampled identity was absent"
    );
}
