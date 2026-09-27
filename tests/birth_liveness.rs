//! The dual-lane birth's liveness deadline is the one field-reachable timer in
//! `rtp_mux` whose expiry is pure elapsed time rather than evidence of a dead
//! path, and the field's own round trips reach **3205 ms** on a ~190 ms floor.
//! A birth that is merely *slow* must therefore not be killed by it: a birth
//! killed for slowness is retried, and the retry **re-pays the cold
//! dual-lane establishment** — the failure mode `AGENTS.md` names, "a reconnect
//! during a spike is worse than the spike".
//!
//! `BIRTH_LIVENESS_DEADLINE` (`src/shared.rs`) is armed at the connector for
//! the mux lanes' *first receive* and raced against the whole dual-lane birth;
//! expiry aborts and reaps the supervisor (both rtp lane sessions and both mux
//! tasks) and returns `TimedOut` from the attempt, after which
//! `retry_dual_connect` starts a **fresh** cold birth (`connect_dual_lane_once`
//! opens two new rtp sessions and a new nonce). `MAX_DUAL_CONNECT_ATTEMPTS = 3`
//! bounds the cost: a stall longer than `3 * (deadline + grace)` fails the dial
//! outright.
//!
//! This target measures the two sides of the deadline on the *birth* itself,
//! with the rtp opening handshake **off** so that the only birth timer in force
//! is the mux one (rtp's own opening leg budget is 4 s and would otherwise be
//! the term that fires first):
//!
//! | arm | one-way delay | round trip | what it pins |
//! | --- | --- | --- | --- |
//! | `clean` | 25 ms | 50 ms | a cold birth completes and its stream round-trips |
//! | `spike_scale` | 1300 ms | 2600 ms | a birth whose **first-receive gap is above the 2500 ms the deadline used to be** still completes |
//! | `beyond_budget` | 2500 ms | 5000 ms | a birth that genuinely does not arrive inside the budget still **fails** — the timer keeps its teeth |
//!
//! The `spike_scale` arm is the change's measurement: at 2600 ms round trip the
//! mux lanes' first receive arrives *after* the old 2500 ms deadline and before
//! the field's worst 3205 ms round trip, so it is exactly the "slow, not dead"
//! case. `beyond_budget` is the vacuity: it proves the instrument can see an
//! expiry, so `spike_scale`'s pass is a measurement and not an arm that cannot
//! fail. The mutation proof that `spike_scale` depends on the constant is the
//! probe run recorded in `rtp_mux/GATE.md` (with the deadline back at 2500 ms
//! the arm fails `TimedOut`).
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test birth_liveness -- \
//!     --ignored --nocapture --test-threads=1
//! ```

use std::{io, net::SocketAddr, sync::Arc, time::Duration, time::Instant};

use netem_test::NetemPair;
use netem_test::kit::presets::clean_delay_link;
use netem_test::kit::{
    TEST_TASK_QUEUE_BOUND, TestScope, TestTaskSubmitter, submit_test_task,
    submit_test_task_required,
};
use rtp_mux::testkit::{
    BIRTH_LIVENESS_DEADLINE_MS, BIRTH_LIVENESS_GRACE_MS, MAX_DUAL_CONNECT_ATTEMPTS,
};
use rtp_mux::{
    BindSelector, BulkAddrSelector, ExplorerConfig, LaneClass, RtpMuxConnector,
    RtpMuxConnectorConfig, RtpMuxServer, RtpMuxServerConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The field's floor, cited so the arm's scale is read against the deployment
/// rather than invented.
const FIELD_FLOOR_OWD: Duration = Duration::from_millis(95);
/// The field's worst recorded round trip: the number the deadline must outlast.
const FIELD_WORST_ROUND_TRIP: Duration = Duration::from_millis(3205);

/// The `spike_scale` arm's one-way delay: a round trip just **above the
/// 2500 ms** the deadline used to be and **below the field's worst** 3205 ms.
const SPIKE_SCALE_OWD: Duration = Duration::from_millis(1300);
/// The `beyond_budget` arm's one-way delay: one round trip of 5000 ms, above
/// the 4 s budget, so the birth's first receive cannot arrive in time.
const BEYOND_BUDGET_OWD: Duration = Duration::from_millis(2500);
/// Per-arm wall-clock bound: the birth pays at most one round trip of the arm's
/// own delay plus setup.
const ARM_TIMEOUT: Duration = Duration::from_secs(30);

struct Arm {
    name: &'static str,
    owd: Duration,
    /// `true` when the birth must complete; `false` when the only admissible
    /// outcome is the deadline's `TimedOut`.
    arrives: bool,
}

const ARMS: [Arm; 3] = [
    Arm {
        name: "clean",
        owd: Duration::from_millis(25),
        arrives: true,
    },
    Arm {
        name: "spike_scale",
        owd: SPIKE_SCALE_OWD,
        arrives: true,
    },
    Arm {
        name: "beyond_budget",
        owd: BEYOND_BUDGET_OWD,
        arrives: false,
    },
];

async fn spawn_server(task_tx: &TestTaskSubmitter) -> io::Result<(SocketAddr, SocketAddr)> {
    let task_tx = task_tx.clone();
    let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default())
        .await?
        .with_handshake(false);
    let interactive = server.listener().local_addr();
    let bulk = server.bulk_listener().local_addr();
    submit_test_task_required(
        &task_tx.clone(),
        "birth-liveness server",
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
                            let (mut reader, mut writer) = tokio::io::split(stream);
                            let mut buf = [0u8; 4096];
                            while let Ok(n) = reader.read(&mut buf).await {
                                if n == 0 {
                                    break;
                                }
                                if writer.write_all(&buf[..n]).await.is_err() {
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

fn connector(task_tx: &TestTaskSubmitter, bulk_proxy_addr: SocketAddr) -> RtpMuxConnector {
    let bind: BindSelector = Arc::new(|_| "0.0.0.0:0".parse().unwrap());
    let bulk_addr: BulkAddrSelector = Arc::new(move |_| Ok(bulk_proxy_addr));
    let (connector, driver) = RtpMuxConnector::with_config(RtpMuxConnectorConfig {
        bulk_addr,
        explorer: ExplorerConfig {
            enabled: false,
            ..ExplorerConfig::default()
        },
        // The rtp opening handshake is off so the mux birth deadline is the
        // only birth timer in force; the arm measures it, not rtp's opening
        // budget.
        handshake: false,
        ..RtpMuxConnectorConfig::standard(bind)
    });
    submit_test_task(task_tx, Box::pin(driver));
    connector
}

/// One birth on a path with the arm's delay, and — when it arrives — a byte
/// round trip on the stream it opened.
async fn run_arm(
    task: &Arm,
    task_tx: &TestTaskSubmitter,
) -> (Duration, Option<bool>, Option<io::ErrorKind>) {
    let (interactive, bulk) = spawn_server(task_tx).await.unwrap();
    let config = clean_delay_link(task.owd, 7);
    let int_pair = NetemPair::spawn(interactive, config.clone(), config.clone()).unwrap();
    let bulk_pair = NetemPair::spawn(bulk, config.clone(), config.clone()).unwrap();
    let connector = connector(task_tx, bulk_pair.client_addr());
    let started = Instant::now();
    let outcome = tokio::time::timeout(
        ARM_TIMEOUT,
        connector.connect_stream_with_lane(int_pair.client_addr(), LaneClass::Interactive),
    )
    .await;
    let elapsed = started.elapsed();
    let (echo, kind) = match outcome {
        Err(_) => (None, Some(io::ErrorKind::TimedOut)),
        Ok(Err(error)) => (None, Some(error.kind())),
        Ok(Ok(mut stream)) => {
            let mut echoed = [0u8; 8];
            let _ = stream.write_all(b"liveness").await;
            let read = tokio::time::timeout(ARM_TIMEOUT, stream.read_exact(&mut echoed)).await;
            (
                Some(matches!(read, Ok(Ok(_))) && &echoed == b"liveness"),
                None,
            )
        }
    };
    int_pair.stop();
    bulk_pair.stop();
    (elapsed, echo, kind)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "binds ephemeral ports and runs one ~17 s birth arm set on delayed links (the 5000 ms-round-trip arm dominates); run with --ignored --nocapture --test-threads=1 (see the module header)"]
async fn a_birth_is_not_killed_by_a_spike_scale_gap_but_still_times_out_beyond_its_budget() {
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let mut rows = Vec::new();
            for task in &ARMS {
                let (elapsed, echo, kind) = run_arm(task, &task_tx).await;
                eprintln!(
                    "[birth-liveness] arm={:<14} owd={:>5}ms rtt={:>5}ms deadline={BIRTH_LIVENESS_DEADLINE_MS}ms wall={:>7.1}ms arrived={} echo={echo:?} error={kind:?}",
                    task.name,
                    task.owd.as_millis(),
                    2 * task.owd.as_millis(),
                    elapsed.as_secs_f64() * 1000.0,
                    kind.is_none(),
                );
                rows.push((task, elapsed, echo, kind));
            }
            for (task, elapsed, echo, kind) in &rows {
                if task.arrives {
                    assert!(
                        kind.is_none(),
                        "[birth-liveness] the {} arm's birth did not complete (round trip {} ms, wall {:.1} ms): {kind:?}. A birth that is merely slow must not be killed by the liveness deadline, and the retry re-pays the cold dual-lane establishment",
                        task.name,
                        2 * task.owd.as_millis(),
                        elapsed.as_secs_f64() * 1000.0,
                    );
                    assert_eq!(
                        *echo,
                        Some(true),
                        "[birth-liveness] the {} arm's birth completed but its stream did not round-trip: {echo:?}",
                        task.name,
                    );
                } else {
                    // The vacuity: a birth that genuinely does not arrive inside
                    // the budget must fail, and its wall clock must be bounded
                    // by the retry budget (`attempts x (deadline + grace)`), so
                    // the arm measures both the expiry and what a dead birth
                    // costs.
                    assert!(
                        kind.is_some(),
                        "[birth-liveness] the {} arm (round trip {} ms, above the deadline) must not complete; got {kind:?} after {:.1} ms",
                        task.name,
                        2 * task.owd.as_millis(),
                        elapsed.as_secs_f64() * 1000.0,
                    );
                    let budget_ms = MAX_DUAL_CONNECT_ATTEMPTS
                        as u128
                        * (BIRTH_LIVENESS_DEADLINE_MS as u128 + BIRTH_LIVENESS_GRACE_MS as u128);
                    let wall_ms = elapsed.as_millis();
                    assert!(
                        wall_ms <= budget_ms + 2_000,
                        "[birth-liveness] the {} arm failed after {wall_ms} ms, past the retry budget {budget_ms} ms ({} attempts x ({BIRTH_LIVENESS_DEADLINE_MS} ms deadline + {BIRTH_LIVENESS_GRACE_MS} ms grace)) plus 2 s of slack: the cost of a dead birth is not bounded by the retry policy",
                        task.name,
                        MAX_DUAL_CONNECT_ATTEMPTS,
                    );
                }
            }
            // The relation the arms are read against, asserted here so the
            // measurement and the constant cannot drift apart silently.
            assert!(
                BIRTH_LIVENESS_DEADLINE_MS >= FIELD_WORST_ROUND_TRIP.as_millis() as u64,
                "[birth-liveness] the birth liveness deadline ({BIRTH_LIVENESS_DEADLINE_MS} ms) must outlast the field's worst recorded round trip ({} ms); a deadline below it kills a birth that is merely slow",
                FIELD_WORST_ROUND_TRIP.as_millis(),
            );
            assert!(
                FIELD_FLOOR_OWD < SPIKE_SCALE_OWD,
                "[birth-liveness] the spike_scale arm's delay must exceed the field's floor",
            );
        })
        .await;
}
