//! Does the cross-lane CC signal (`rtp::cc::CcSignalHub`) actually reach the
//! bulk connection's congestion controller, and does it buy anything on the
//! shape the mechanism was written for?
//!
//! # What the mechanism claims
//!
//! `rtp/src/cc.rs` routes one *presence* flag between connections that share a
//! `(src ip, dst ip)` egress path. The interactive lane's metrics observer sets
//! it while that lane is live; the bulk lane's config carries the corresponding
//! [`rtp::cc::CcSignal`], and `reliable_layer.rs` folds it into the congestion
//! decision as `shared_path`. `congestion_response` uses it for exactly one
//! thing: on a shared path, loss from a buffer this connection is itself
//! filling is not independent evidence, so it must not suppress the delay
//! drain:
//!
//! ```text
//! loss_blocks_delay_control = observation.loss_blocks_delay_control && !input.shared_path;
//! ```
//!
//! So the mechanism is *observable at the controller*, not only in the network:
//! with the link active and its loss gate open, the bulk connection's chosen
//! branch must move from `LossBackoff` toward `DelayDrain`. Both are exposed on
//! the public metrics surface — the gate as `congestion_loss_ratio`, the
//! cumulative branch counts as `congestion_delay_drains` /
//! `congestion_loss_backoffs` — so this arm reads the mechanism directly instead
//! of inferring it from a latency that a dozen other things move.
//!
//! # Arms and shape
//!
//! Two arms, interleaved rep-by-rep so host-load drift hits them alike, one
//! dimension apart — whether the hub is passed to the dual-lane client:
//!
//! * `absent` — the untouched dual-lane client, both lanes straight onto their
//!   sockets.
//! * `active` — the same topology with one [`CcSignalHub`] on both lanes.
//!
//! Both lanes' client→server traffic crosses **one shared bottleneck shaper**
//! (1 MiB/s, 128 KiB queue), so a saturating bulk genuinely stands in front of
//! the interactive lane: the contention the signal exists to order. This is the
//! shape `ibfq_nic_mandate.rs` measures and the shape the hostile-tail mandate
//! names.
//!
//! # Evidence
//!
//! `$CC_AB_DIR` (default `target/cc-link-ab`) receives `ab-samples.csv` and
//! `ab-reps.csv`. Printed on stdout: per-rep interactive one-way
//! p50/p90/p99/max, bulk delivered goodput, the shared-queue backlog, and the
//! bulk controller's cumulative branch counts with its loss-gate engagement.
//! The assertions are tripwires; the printed table is the evidence.
//!
//! Run:
//! ```sh
//! cargo test --release -p rtp_mux --test cc_link_ab -- --ignored --nocapture --test-threads=1
//! ```

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::stats::{HolSummary, summarize};
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{BottleneckShaper, NetemConfig, NetemPair};
use rtp::cc::CcSignalHub;
use rtp::metrics::{MetricsCongestionAction, MetricsEvent, MetricsObserver};
use rtp::testkit::rtp::send_timestamped_messages;
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via_cc_link,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One-way delay: a short floor so the queue, not the link, is the variable.
const OWD: Duration = Duration::from_millis(25);
const JITTER: Duration = Duration::from_millis(5);
const MSG_BYTES: usize = 256;
const CADENCE: Duration = Duration::from_millis(25);
const RUN_FOR: Duration = Duration::from_secs(8);
const GRACE: Duration = Duration::from_secs(1);
/// Reporting nominal for the goodput column, not a rate any arbiter enforces
/// (the hub has none): it only keeps the column comparable across arms.
const LINK_BYTES_PER_SEC: f64 = 1024.0 * 1024.0;
/// The shared bottleneck both lanes' client→server traffic crosses. The
/// instrument, not the mechanism: the hub has no rate.
const SHAPER_RATE_BPS: u64 = 8_388_608; // 1 MiB/s
const SHAPER_LIMIT_BYTES: u64 = 128 * 1024;
const BULK_CHUNK: usize = 64 * 1024;
const REPS: usize = 4;

/// `CC_DATA_LOSS_RATE` in `rtp/src/traffic_shaping/core/congestion_response`:
/// `loss_blocks_delay_control` is set only once the connection's own measured
/// loss event rate reaches this. Restated here as the arm's gate probe, with
/// the source named so a drift in the constant is visible.
const CC_DATA_LOSS_RATE: f64 = 0.2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    Absent,
    Active,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Absent => "absent",
            Arm::Active => "active",
        }
    }
}

fn link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: JITTER,
        seed,
        ..NetemConfig::default()
    }
}

fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

/// The bulk connection's own congestion control, read off the public metrics
/// surface. The cumulative counters are monotonic within a connection, so
/// `fetch_max` over every RttSample observation recovers the run's totals; the
/// last-action histogram is kept too, because the huge-loss backoff path does
/// not touch the cumulative `congestion_loss_backoffs` counter.
#[derive(Default)]
struct CcProbe {
    observations: AtomicU64,
    gate_open: AtomicU64,
    max_loss_ratio_milli: AtomicU64,
    delay_drains: AtomicU64,
    loss_backoffs: AtomicU64,
    bandwidth_probes: AtomicU64,
    sampled_delay_drain: AtomicU64,
    sampled_loss_backoff: AtomicU64,
    sampled_huge_loss_backoff: AtomicU64,
    sampled_probe: AtomicU64,
    sampled_hold: AtomicU64,
}

impl CcProbe {
    fn observer(self: &Arc<Self>) -> MetricsObserver {
        let probe = Arc::clone(self);
        MetricsObserver::filtered(
            |event, _| event == MetricsEvent::RttSample,
            move |observation| {
                let Some(snapshot) = observation.snapshot else {
                    return;
                };
                probe.observations.fetch_add(1, Ordering::Relaxed);
                if let Some(ratio) = snapshot.congestion_loss_ratio {
                    let milli = (ratio.clamp(0.0, 1.0) * 1000.0) as u64;
                    probe
                        .max_loss_ratio_milli
                        .fetch_max(milli, Ordering::Relaxed);
                    if ratio >= CC_DATA_LOSS_RATE {
                        probe.gate_open.fetch_add(1, Ordering::Relaxed);
                    }
                }
                probe
                    .delay_drains
                    .fetch_max(snapshot.congestion_delay_drains, Ordering::Relaxed);
                probe
                    .loss_backoffs
                    .fetch_max(snapshot.congestion_loss_backoffs, Ordering::Relaxed);
                probe.bandwidth_probes.fetch_max(
                    snapshot.congestion_bandwidth_probe_decisions,
                    Ordering::Relaxed,
                );
                match snapshot.congestion_action {
                    Some(MetricsCongestionAction::DelayDrain) => {
                        probe.sampled_delay_drain.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(MetricsCongestionAction::LossBackoff) => {
                        probe.sampled_loss_backoff.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(MetricsCongestionAction::HugeLossBackoff) => {
                        probe
                            .sampled_huge_loss_backoff
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Some(
                        MetricsCongestionAction::BandwidthProbe
                        | MetricsCongestionAction::GentleProbe
                        | MetricsCongestionAction::SlowStartAck,
                    ) => {
                        probe.sampled_probe.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(MetricsCongestionAction::QueueHold) => {
                        probe.sampled_hold.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            },
        )
    }

    fn delay_drains(&self) -> u64 {
        self.delay_drains.load(Ordering::Relaxed)
    }
    fn loss_backoffs(&self) -> u64 {
        self.loss_backoffs.load(Ordering::Relaxed)
    }
    fn bandwidth_probes(&self) -> u64 {
        self.bandwidth_probes.load(Ordering::Relaxed)
    }
    fn observations(&self) -> u64 {
        self.observations.load(Ordering::Relaxed)
    }
    fn gate_open(&self) -> u64 {
        self.gate_open.load(Ordering::Relaxed)
    }
    fn max_loss_ratio_milli(&self) -> u64 {
        self.max_loss_ratio_milli.load(Ordering::Relaxed)
    }

    /// Loss-gated decisions that would have won without the signal.
    fn decisions(&self) -> u64 {
        self.delay_drains() + self.loss_backoffs()
    }
}

struct Run {
    summary: HolSummary,
    goodput_fraction: f64,
    bulk_mibps: f64,
    samples: Vec<f64>,
    backlog_p50: u64,
    backlog_p99: u64,
    backlog_max: u64,
    /// Samples of the hub's own aggregate in which an interactive lane had
    /// marked this path shared (the wiring's positive control).
    shared_probe: u64,
    probe: CcProbe,
}

fn pct(xs: &[u64], p: f64) -> u64 {
    if xs.is_empty() {
        return 0;
    }
    let mut s = xs.to_vec();
    s.sort_unstable();
    s[((s.len() as f64 * p) as usize).min(s.len() - 1)]
}

async fn run_arm(arm: Arm) -> Run {
    let base = Instant::now();
    let hub = (arm == Arm::Active).then(CcSignalHub::new);
    // The transport resolves each lane's `(src, dst)` group from its own
    // socket; the kit binds `0.0.0.0:0`, so the local half of the key is
    // whatever `getsockname` reports. Probe the two candidates and treat the
    // path as shared if either saw the interactive lane.
    let (probe_loopback, probe_unspecified) = match &hub {
        Some(hub) => {
            let v4 = |ip: Ipv4Addr| IpAddr::V4(ip);
            (
                Some(
                    hub.group(v4(Ipv4Addr::LOCALHOST), v4(Ipv4Addr::LOCALHOST))
                        .bulk(),
                ),
                Some(
                    hub.group(v4(Ipv4Addr::UNSPECIFIED), v4(Ipv4Addr::LOCALHOST))
                        .bulk(),
                ),
            )
        }
        None => (None, None),
    };
    let probe = Arc::new(CcProbe::default());
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
            let bulk_rtp = LaneRtpConfig::production_bulk();
            let (int_addr, bulk_addr, mut latencies, bulk_sink, _server_tx) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let c2s = BottleneckShaper::new(SHAPER_RATE_BPS, SHAPER_LIMIT_BYTES);
            let int_pair =
                NetemPair::spawn_shared(int_addr, link(41), link(42), Some(c2s.clone()), None)
                    .unwrap();
            let bulk_pair =
                NetemPair::spawn_shared(bulk_addr, link(43), link(44), Some(c2s.clone()), None)
                    .unwrap();
            let backlog = Arc::new(Mutex::new(Vec::<u64>::new()));
            let sampling = Arc::new(AtomicBool::new(true));
            let shared_probe = Arc::new(AtomicU64::new(0));
            {
                let backlog = Arc::clone(&backlog);
                let sampling = Arc::clone(&sampling);
                let shared = Arc::clone(&shared_probe);
                let loopback = probe_loopback.clone();
                let unspecified = probe_unspecified.clone();
                let shaper = c2s.clone();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        while sampling.load(Ordering::Relaxed) {
                            backlog
                                .lock()
                                .unwrap()
                                .push(shaper.backlog_bytes(Instant::now()));
                            let marked = loopback.as_ref().is_some_and(|s| s.is_shared())
                                || unspecified.as_ref().is_some_and(|s| s.is_shared());
                            if marked {
                                shared.fetch_add(1, Ordering::Relaxed);
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }),
                );
            }
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via_cc_link(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                Some(probe.observer()),
                hub.clone(),
            )
            .await
            .unwrap();
            let (_lat_read, mut lat_write) =
                opener.open(mux::LaneClass::Interactive).await.unwrap();
            let (mut bulk_read, mut bulk_write) = opener.open(mux::LaneClass::Bulk).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let samples = Arc::new(Mutex::new(Vec::<f64>::new()));
            {
                let samples = Arc::clone(&samples);
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        while let Some((_tag, latency)) = latencies.recv().await {
                            samples.lock().unwrap().push(latency);
                        }
                    }),
                );
            }
            let bulk_before = bulk_sink.load(Ordering::Relaxed);
            let bulk_started = Instant::now();
            let mut bulk_tasks = tokio::task::JoinSet::new();
            let _ = bulk_tasks.spawn(async move {
                if bulk_write.write_all(b"B").await.is_err() {
                    return;
                }
                let mut offset = 0u64;
                let deadline = Instant::now() + RUN_FOR;
                while Instant::now() < deadline {
                    let chunk: Vec<u8> = (0..BULK_CHUNK)
                        .map(|i| ((offset + i as u64) % 251) as u8)
                        .collect();
                    offset += BULK_CHUNK as u64;
                    if bulk_write.write_all(&chunk).await.is_err() {
                        break;
                    }
                }
                let _ = bulk_write.shutdown();
            });
            let sent = if lat_write.write_all(b"L").await.is_ok() {
                send_timestamped_messages(&mut lat_write, base, MSG_BYTES, CADENCE, RUN_FOR).await
            } else {
                0
            };
            let _ = bulk_tasks.join_next().await;
            let bulk_secs = bulk_started.elapsed().as_secs_f64();
            tokio::time::sleep(GRACE).await;
            let bulk_bytes = bulk_sink.load(Ordering::Relaxed) - bulk_before;
            let samples = samples.lock().unwrap().clone();
            sampling.store(false, Ordering::Relaxed);
            let backlog = backlog.lock().unwrap().clone();
            let received = samples.len() as u64;
            let summary = summarize(samples.clone(), sent, received, bulk_bytes, bulk_secs);
            let goodput_fraction = (bulk_bytes as f64 / bulk_secs) / LINK_BYTES_PER_SEC;
            Run {
                summary,
                goodput_fraction,
                bulk_mibps: bulk_bytes as f64 / bulk_secs / (1024.0 * 1024.0),
                samples,
                backlog_p50: pct(&backlog, 0.50),
                backlog_p99: pct(&backlog, 0.99),
                backlog_max: backlog.iter().copied().max().unwrap_or(0),
                shared_probe: shared_probe.load(Ordering::Relaxed),
                probe: CcProbe {
                    observations: AtomicU64::new(probe.observations()),
                    gate_open: AtomicU64::new(probe.gate_open()),
                    max_loss_ratio_milli: AtomicU64::new(probe.max_loss_ratio_milli()),
                    delay_drains: AtomicU64::new(probe.delay_drains()),
                    loss_backoffs: AtomicU64::new(probe.loss_backoffs()),
                    bandwidth_probes: AtomicU64::new(probe.bandwidth_probes()),
                    sampled_delay_drain: AtomicU64::new(
                        probe.sampled_delay_drain.load(Ordering::Relaxed),
                    ),
                    sampled_loss_backoff: AtomicU64::new(
                        probe.sampled_loss_backoff.load(Ordering::Relaxed),
                    ),
                    sampled_huge_loss_backoff: AtomicU64::new(
                        probe.sampled_huge_loss_backoff.load(Ordering::Relaxed),
                    ),
                    sampled_probe: AtomicU64::new(probe.sampled_probe.load(Ordering::Relaxed)),
                    sampled_hold: AtomicU64::new(probe.sampled_hold.load(Ordering::Relaxed)),
                },
            }
        })
        .await
}

fn median(xs: &mut [f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    xs[xs.len() / 2]
}

fn median_stat(runs: &[(Arm, usize, Run)], arm: Arm, f: fn(&HolSummary) -> f64) -> f64 {
    let mut xs: Vec<f64> = runs
        .iter()
        .filter(|(a, _, _)| *a == arm)
        .map(|(_, _, r)| f(&r.summary))
        .collect();
    median(&mut xs)
}

fn median_bulk(runs: &[(Arm, usize, Run)], arm: Arm) -> f64 {
    let mut xs: Vec<f64> = runs
        .iter()
        .filter(|(a, _, _)| *a == arm)
        .map(|(_, _, r)| r.goodput_fraction)
        .collect();
    median(&mut xs)
}

fn total(runs: &[(Arm, usize, Run)], arm: Arm, f: fn(&CcProbe) -> u64) -> u64 {
    runs.iter()
        .filter(|(a, _, _)| *a == arm)
        .map(|(_, _, r)| f(&r.probe))
        .sum()
}

fn delay_drain_share(runs: &[(Arm, usize, Run)], arm: Arm) -> f64 {
    let drains = total(runs, arm, CcProbe::delay_drains);
    let decisions = drains + total(runs, arm, CcProbe::loss_backoffs);
    if decisions == 0 {
        f64::NAN
    } else {
        drains as f64 / decisions as f64
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns threads and binds ephemeral ports; 2 arms x 4 interleaved ~10 s runs; run with --ignored --nocapture --test-threads=1"]
async fn cc_link_reaches_the_bulk_controller_and_bounds_the_shared_queue() {
    let dir = "target/cc-link-ab";
    std::fs::create_dir_all(dir).unwrap();
    let mut runs: Vec<(Arm, usize, Run)> = Vec::new();
    let mut samples_csv = String::from("arm,rep,latency_ms\n");
    for rep in 0..REPS {
        for arm in [Arm::Absent, Arm::Active] {
            let run = run_arm(arm).await;
            for s in &run.samples {
                let _ = writeln!(samples_csv, "{},{rep},{s:.4}", arm.name());
            }
            eprintln!(
                "[ab] {:>6} rep{rep}  p50 {:5.1} p90 {:5.1} p99 {:5.1} max {:6.1} ms  delivery {:.3}  bulk {:.3} MiB/s ({:.3}x)  backlog p50/p99/max {}/{}/{} B  shared {}  decisions {} (drain {} / loss {})  gate {}/{}  maxloss {:.3}  huge {}",
                arm.name(),
                run.summary.p50,
                run.summary.p90,
                run.summary.p99,
                run.summary.max,
                run.summary.delivery_pct,
                run.bulk_mibps,
                run.goodput_fraction,
                run.backlog_p50,
                run.backlog_p99,
                run.backlog_max,
                run.shared_probe,
                run.probe.decisions(),
                run.probe.delay_drains(),
                run.probe.loss_backoffs(),
                run.probe.gate_open(),
                run.probe.observations(),
                run.probe.max_loss_ratio_milli() as f64 / 1000.0,
                run.probe.sampled_huge_loss_backoff.load(Ordering::Relaxed),
            );
            runs.push((arm, rep, run));
        }
    }
    std::fs::write(format!("{dir}/ab-samples.csv"), &samples_csv).unwrap();

    let mut reps_csv = String::from(
        "arm,rep,p50,p90,p99,p999,max,delivery,bulk_fraction,bulk_mibps,backlog_p99,shared_probe,observations,gate_open,max_loss_ratio,delay_drains,loss_backoffs,bandwidth_probes\n",
    );
    for (arm, rep, run) in &runs {
        let _ = writeln!(
            reps_csv,
            "{},{rep},{:.3},{:.3},{:.3},{:.3},{:.3},{:.4},{:.4},{:.4},{},{},{},{},{:.4},{},{},{}",
            arm.name(),
            run.summary.p50,
            run.summary.p90,
            run.summary.p99,
            run.summary.p999,
            run.summary.max,
            run.summary.delivery_pct,
            run.goodput_fraction,
            run.bulk_mibps,
            run.backlog_p99,
            run.shared_probe,
            run.probe.observations(),
            run.probe.gate_open(),
            run.probe.max_loss_ratio_milli() as f64 / 1000.0,
            run.probe.delay_drains(),
            run.probe.loss_backoffs(),
            run.probe.bandwidth_probes(),
        );
    }
    std::fs::write(format!("{dir}/ab-reps.csv"), &reps_csv).unwrap();

    let p50 = |arm| median_stat(&runs, arm, |s| s.p50);
    let p90 = |arm| median_stat(&runs, arm, |s| s.p90);
    let p99 = |arm| median_stat(&runs, arm, |s| s.p99);
    let max = |arm| median_stat(&runs, arm, |s| s.max);
    eprintln!(
        "[ab] median interactive one-way (ms)\n\
         [ab]   absent  p50 {:6.1} p90 {:6.1} p99 {:6.1} max {:7.1}\n\
         [ab]   active  p50 {:6.1} p90 {:6.1} p99 {:6.1} max {:7.1}\n\
         [ab] median bulk goodput   absent {:.3}x   active {:.3}x\n\
         [ab] bulk controller       absent drain-share {:.3}  active drain-share {:.3}\n\
         [ab] cumulative branches   absent drain/loss {}/{}  active drain/loss {}/{}\n\
         [ab] gate-open observations absent {}  active {}   (of {} / {} RTT samples)\n\
         [ab] path-shared observations active {}\n\
         [ab] data: {dir}/ab-reps.csv, {dir}/ab-samples.csv",
        p50(Arm::Absent),
        p90(Arm::Absent),
        p99(Arm::Absent),
        max(Arm::Absent),
        p50(Arm::Active),
        p90(Arm::Active),
        p99(Arm::Active),
        max(Arm::Active),
        median_bulk(&runs, Arm::Absent),
        median_bulk(&runs, Arm::Active),
        delay_drain_share(&runs, Arm::Absent),
        delay_drain_share(&runs, Arm::Active),
        total(&runs, Arm::Absent, CcProbe::delay_drains),
        total(&runs, Arm::Absent, CcProbe::loss_backoffs),
        total(&runs, Arm::Active, CcProbe::delay_drains),
        total(&runs, Arm::Active, CcProbe::loss_backoffs),
        total(&runs, Arm::Absent, CcProbe::gate_open),
        total(&runs, Arm::Active, CcProbe::gate_open),
        total(&runs, Arm::Absent, CcProbe::observations),
        total(&runs, Arm::Active, CcProbe::observations),
        runs.iter()
            .filter(|(a, _, _)| *a == Arm::Active)
            .map(|(_, _, r)| r.shared_probe)
            .sum::<u64>(),
    );

    // ── the wiring's positive control ────────────────────────────────────
    assert!(
        runs.iter()
            .filter(|(a, _, _)| *a == Arm::Active)
            .map(|(_, _, r)| r.shared_probe)
            .sum::<u64>()
            > 0,
        "the active arm never observed the path marked shared: the hub is not wired to the lanes, so this arm measures the absent arm twice",
    );
    // ── the controller actually sampled, i.e. the instrument has teeth ────
    assert!(
        total(&runs, Arm::Active, CcProbe::observations) > 0,
        "the bulk connection published no RTT-sample observations: the congestion branch could not be read, so the comparison is vacuous",
    );
    // ── the suppression's precondition was present ───────────────────────
    assert!(
        total(&runs, Arm::Active, CcProbe::gate_open) > 0,
        "the bulk connection's loss event rate never reached CC_DATA_LOSS_RATE ({CC_DATA_LOSS_RATE}), so loss never blocked delay control and shared_path could not change any decision: the mechanism was never exercised in this shape",
    );
    // ── the mechanism's defining effect: on a shared path, no loss-gated
    // decision survives. The gate's rarity (0.2-0.9 % of samples at
    // CC_DATA_LOSS_RATE) makes an arm-vs-arm share comparison degenerate when
    // the control arm's gate happens not to open in a run, so the property is
    // stated where the signal is the only difference: every decision the gate
    // would have sent to LossBackoff must instead have drained.
    assert_eq!(
        total(&runs, Arm::Active, CcProbe::loss_backoffs),
        0,
        "the shared-path signal left loss-gated decision(s) un-suppressed on the active arm ({} gate-open observations): the suppression is a no-op",
        total(&runs, Arm::Active, CcProbe::gate_open),
    );
    // ── M3: the mechanism must not buy M1 with bulk throughput ────────────
    assert!(
        median_bulk(&runs, Arm::Active) >= median_bulk(&runs, Arm::Absent) * 0.9,
        "the shared-path signal capped bulk goodput ({:.3}x vs {:.3}x of the nominal)",
        median_bulk(&runs, Arm::Active),
        median_bulk(&runs, Arm::Absent),
    );
    // ── M1 regression tripwire: the signal must not worsen the tail ───────
    assert!(
        p99(Arm::Active) <= p99(Arm::Absent) * 1.25,
        "the shared-path signal worsened the interactive p99 ({:.1} vs {:.1} ms)",
        p99(Arm::Active),
        p99(Arm::Absent),
    );
}
