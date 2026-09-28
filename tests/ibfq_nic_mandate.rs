//! A/B/C: does putting a per-NIC interactive/bulk fair queue under the
//! dual-lane client degrade the tri-mandate?
//!
//! Three arms, interleaved rep-by-rep in one process so host-load drift hits
//! them alike (the workspace's perf discipline: vary one dimension per arm,
//! interleave reps, compare medians, and **read the panel's shape** — not just
//! a p99):
//!
//! * `baseline` — the untouched production dual-lane client
//!   (`dual_mux_client_connect_lane_rtp_via`): two lanes straight onto their
//!   two sockets.
//! * `ibfq` — the same topology with both lanes' sends arbitrated by one
//!   [`rtp::nic::NicScheduler`]: strict interactive-before-bulk, no rate.
//!
//! Evidence goes to `$IBFQ_AB_DIR` (default `target/ibfq-ab`):
//! `ab-samples.csv` (arm, rep, one-way latency ms), `ab-reps.csv` (per-rep
//! percentiles, delivery, bulk goodput fraction) and a rendered CDF panel
//! `ab-cdf.svg`. The panel is the deliverable; the assertions are tripwires.
//!
//! Run:
//! ```sh
//! cargo test --release -p rtp_mux --test ibfq_nic_mandate -- --ignored --nocapture --test-threads=1
//! ```

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::stats::{HolSummary, summarize};
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, TestTaskSubmitter, submit_test_task};
use netem_test::{NetemConfig, NetemPair};
use rtp::nic::{Class, NicEndpoint, NicScheduler};
use rtp::testkit::rtp::send_timestamped_messages;
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;

const OWD: Duration = Duration::from_millis(25);
const JITTER: Duration = Duration::from_millis(5);
const MSG_BYTES: usize = 256;
const CADENCE: Duration = Duration::from_millis(25);
const RUN_FOR: Duration = Duration::from_secs(8);
const GRACE: Duration = Duration::from_secs(1);
/// A reporting nominal for the bulk-goodput column, not a rate the arbiter
/// enforces (it has none): it only keeps the column comparable across arms.
const LINK_BYTES_PER_SEC: f64 = 1024.0 * 1024.0;
const BULK_CHUNK: usize = 64 * 1024;
const REPS: usize = 3;
const ARMS: [Arm; 2] = [Arm::Baseline, Arm::Ibfq];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    Baseline,
    Ibfq,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Baseline => "baseline",
            Arm::Ibfq => "ibfq",
        }
    }
    /// Whether this arm routes both lanes through one per-NIC arbiter.
    fn use_scheduler(self) -> bool {
        matches!(self, Arm::Ibfq)
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

fn lane_connect_config(lane: LaneRtpConfig) -> rtp::udp::ConnectConfig<'static> {
    rtp::udp::ConnectConfig {
        handshake: false,
        fec: lane.fec,
        mss: if lane.frame_mode.enabled {
            rtp::udp::MssConfig::Default
        } else {
            rtp::udp::MssConfig::Custom(rtp::udp::NO_FEC_MSS)
        },
        fec_tuning: lane.fec_tuning,
        frame_delivery: lane.frame_mode,
        metrics_observer: None,
        congestion_lane: lane.congestion_lane,
        initial_send_rate: lane.initial_send_rate,
        ..rtp::udp::ConnectConfig::default()
    }
}

type BoxedRead = Box<dyn tokio::io::AsyncRead + Unpin + Send>;
type BoxedWrite = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;

async fn connect_lane_over_nic(
    tx: &TestTaskSubmitter,
    scheduler: &NicScheduler,
    class: Class,
    addr: SocketAddr,
    lane: LaneRtpConfig,
) -> (BoxedRead, BoxedWrite) {
    let ep = NicEndpoint::bind(scheduler.clone(), class, "127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind NIC endpoint");
    ep.connect(addr).await.expect("connect NIC endpoint");
    let layer = ep
        .into_rtp_layer(lane_connect_config(lane))
        .expect("build rtp layer over the NIC halves");
    let (read, write, supervisor) = rtp::socket::socket(layer, None);
    submit_test_task(
        tx,
        Box::pin(async move {
            let _ = supervisor.await;
        }),
    );
    (
        Box::new(read.into_async_read()),
        Box::new(write.into_async_write()),
    )
}

/// Mirrors `dual_mux_client_connect_lane_rtp_via`, but both lanes' sends pass
/// through one shared [`NicScheduler`].
async fn connect_over_nic(
    tx: &TestTaskSubmitter,
    scheduler: NicScheduler,
    int_addr: SocketAddr,
    bulk_addr: SocketAddr,
    int_rtp: LaneRtpConfig,
    bulk_rtp: LaneRtpConfig,
) -> Result<(mux::DualStreamOpener, mux::DualStreamAccepter), mux::DualMuxError> {
    let int_config = mux::MuxConfig {
        initiation: mux::Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: int_rtp.frame_mode.enabled,
    };
    let bulk_config = mux::MuxConfig {
        initiation: mux::Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: bulk_rtp.frame_mode.enabled,
    };
    let nonce = mux::PairingNonce::generate();
    let group = mux::GroupToken::generate();
    let (int_reader, mut int_writer) =
        connect_lane_over_nic(tx, &scheduler, Class::Interactive, int_addr, int_rtp).await;
    mux::write_lane_hello(&mut int_writer, mux::LaneClass::Interactive, nonce, group)
        .await
        .map_err(mux::DualMuxError::LaneHello)?;
    int_writer
        .flush()
        .await
        .map_err(|e| mux::DualMuxError::LaneHello(mux::LaneHelloError::Io(e.kind())))?;
    let (bulk_reader, mut bulk_writer) =
        connect_lane_over_nic(tx, &scheduler, Class::Bulk, bulk_addr, bulk_rtp).await;
    mux::write_lane_hello(&mut bulk_writer, mux::LaneClass::Bulk, nonce, group)
        .await
        .map_err(mux::DualMuxError::LaneHello)?;
    bulk_writer
        .flush()
        .await
        .map_err(|e| mux::DualMuxError::LaneHello(mux::LaneHelloError::Io(e.kind())))?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut int_spawner = JoinSet::new();
    let (int_opener, int_accepter) =
        mux::spawn_mux_no_reconnection(int_reader, int_writer, int_config, &mut int_spawner);
    let mut bulk_spawner = JoinSet::new();
    let (bulk_opener, bulk_accepter) =
        mux::spawn_mux_no_reconnection(bulk_reader, bulk_writer, bulk_config, &mut bulk_spawner);
    let mut super_spawner = JoinSet::new();
    let (opener, accepter) = mux::spawn_dual_mux_paired_supervised(
        int_opener,
        int_accepter,
        int_spawner,
        bulk_opener,
        bulk_accepter,
        bulk_spawner,
        &mut super_spawner,
    );
    let tx = tx.clone();
    submit_test_task(
        &tx,
        Box::pin(async move {
            if let Some(result) = super_spawner.join_next().await
                && let Err(err) = result
            {
                panic!("NIC dual-mux session supervision failed: {err:?}");
            }
        }),
    );
    Ok((opener, accepter))
}

struct Run {
    summary: HolSummary,
    goodput_fraction: f64,
    samples: Vec<f64>,
}

async fn run_arm(arm: Arm) -> Run {
    let base = Instant::now();
    // Strict priority, no rate: the arbiter decides order only, so the
    // baseline arm (no arbiter) is the control.
    let scheduler = arm.use_scheduler().then(NicScheduler::new);
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
            let int_pair = NetemPair::spawn(int_addr, link(41), link(42)).unwrap();
            let bulk_pair = NetemPair::spawn(bulk_addr, link(43), link(44)).unwrap();
            let (opener, _accepter) = match scheduler {
                Some(scheduler) => connect_over_nic(
                    &task_tx,
                    scheduler,
                    int_pair.client_addr(),
                    bulk_pair.client_addr(),
                    int_rtp,
                    bulk_rtp,
                )
                .await
                .unwrap(),
                None => dual_mux_client_connect_lane_rtp_via(
                    &task_tx,
                    int_pair.client_addr(),
                    bulk_pair.client_addr(),
                    int_rtp,
                    bulk_rtp,
                    None,
                    None,
                )
                .await
                .unwrap(),
            };
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
            let bulk_task = tokio::spawn(async move {
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
            let _ = bulk_task.await;
            let bulk_secs = bulk_started.elapsed().as_secs_f64();
            tokio::time::sleep(GRACE).await;
            let bulk_bytes = bulk_sink.load(Ordering::Relaxed) - bulk_before;
            let samples = samples.lock().unwrap().clone();
            let received = samples.len() as u64;
            let summary = summarize(samples.clone(), sent, received, bulk_bytes, bulk_secs);
            let goodput_fraction = (bulk_bytes as f64 / bulk_secs) / LINK_BYTES_PER_SEC;
            Run {
                summary,
                goodput_fraction,
                samples,
            }
        })
        .await
}

fn median(mut xs: Vec<f64>) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    xs[xs.len() / 2]
}

/// The empirical CDF of a sample set at the drawn points, as (latency, pct).
fn cdf_points(mut xs: Vec<f64>) -> Vec<(f64, f64)> {
    xs.sort_by(|a, b| a.total_cmp(b));
    let n = xs.len();
    if n == 0 {
        return Vec::new();
    }
    let step = (n / 300).max(1);
    let mut pts: Vec<(f64, f64)> = (0..n)
        .step_by(step)
        .map(|k| (xs[k], (k as f64 + 1.0) / n as f64 * 100.0))
        .collect();
    pts.push((xs[n - 1], 100.0));
    pts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns threads and binds ephemeral ports; 3 arms x 3 interleaved ~11 s runs; run with --ignored --nocapture --test-threads=1"]
async fn ibfq_nic_ab_against_the_untouched_dual_lane() {
    let dir = std::env::var("IBFQ_AB_DIR").unwrap_or_else(|_| "target/ibfq-ab".to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let mut reps: Vec<(Arm, usize, HolSummary, f64)> = Vec::new();
    let mut pooled: Vec<(Arm, Vec<f64>)> = ARMS.iter().map(|a| (*a, Vec::new())).collect();
    let mut samples_csv = String::from("arm,rep,latency_ms\n");
    for rep in 0..REPS {
        for arm in ARMS {
            let run = run_arm(arm).await;
            for s in &run.samples {
                let _ = writeln!(samples_csv, "{},{rep},{s:.4}", arm.name());
            }
            pooled
                .iter_mut()
                .find(|(a, _)| *a == arm)
                .unwrap()
                .1
                .extend_from_slice(&run.samples);
            eprintln!(
                "[ab] {:>13} rep{rep}  p50 {:5.1} p90 {:5.1} p99 {:5.1} max {:6.1}  delivery {:.3}  bulk {:.3}x  n {}",
                arm.name(),
                run.summary.p50,
                run.summary.p90,
                run.summary.p99,
                run.summary.max,
                run.summary.delivery_pct,
                run.goodput_fraction,
                run.samples.len(),
            );
            reps.push((arm, rep, run.summary, run.goodput_fraction));
        }
    }

    let mut reps_csv =
        String::from("arm,rep,p50,p90,p99,p999,max,delivery,bulk_goodput_fraction\n");
    for (arm, rep, s, g) in &reps {
        let _ = writeln!(
            reps_csv,
            "{},{rep},{:.3},{:.3},{:.3},{:.3},{:.3},{:.4},{:.4}",
            arm.name(),
            s.p50,
            s.p90,
            s.p99,
            s.p999,
            s.max,
            s.delivery_pct,
            g,
        );
    }
    std::fs::write(format!("{dir}/ab-samples.csv"), &samples_csv).unwrap();
    std::fs::write(format!("{dir}/ab-reps.csv"), &reps_csv).unwrap();

    let stat = |arm: Arm, f: fn(&HolSummary) -> f64| {
        median(
            reps.iter()
                .filter(|(a, _, _, _)| *a == arm)
                .map(|(_, _, s, _)| f(s))
                .collect(),
        )
    };
    let bulk = |arm: Arm| {
        median(
            reps.iter()
                .filter(|(a, _, _, _)| *a == arm)
                .map(|(_, _, _, g)| *g)
                .collect(),
        )
    };
    let p99 = |arm: Arm| stat(arm, |s| s.p99);

    let mut cdf_ascii = String::new();
    let _ = writeln!(cdf_ascii, "percentile   baseline        ibfq");
    for pct in [50.0, 90.0, 99.0, 100.0] {
        let _ = write!(cdf_ascii, "{pct:>9.0}%");
        for arm in ARMS {
            let col = pooled
                .iter()
                .find(|(a, _)| *a == arm)
                .map(|(_, v)| {
                    let points = cdf_points(v.clone());
                    let mut out = f64::NAN;
                    for (ms, p) in points {
                        if p >= pct {
                            out = ms;
                            break;
                        }
                    }
                    out
                })
                .unwrap_or(f64::NAN);
            let _ = write!(cdf_ascii, "{col:>14.1}");
        }
        let _ = writeln!(cdf_ascii);
    }
    eprintln!("[ab] pooled CDF (interactive one-way latency, ms):\n{cdf_ascii}");
    eprintln!(
        "[ab] median p99   baseline {:.1}  ibfq {:.1} ms\n\
         [ab] median bulk  baseline {:.3}  ibfq {:.3} x link",
        p99(Arm::Baseline),
        p99(Arm::Ibfq),
        bulk(Arm::Baseline),
        bulk(Arm::Ibfq),
    );

    render_cdf_svg(&dir, &pooled, &reps);
    eprintln!("[ab] panels: {dir}/ab-cdf.svg  data: {dir}/ab-reps.csv, {dir}/ab-samples.csv");

    // The arbiter must not degrade the interactive tail against the
    // untouched path.
    assert!(
        p99(Arm::Ibfq) <= p99(Arm::Baseline) * 1.25,
        "ibfq p99 {:.1} ms exceeds baseline {:.1} ms by >25%: the arbiter degraded M1",
        p99(Arm::Ibfq),
        p99(Arm::Baseline),
    );
    // And it must not cap throughput: bulk runs at the link's own rate, so its
    // goodput must stay in the same class as the untouched path.
    assert!(
        bulk(Arm::Ibfq) >= bulk(Arm::Baseline) * 0.5,
        "ibfq bulk goodput {:.3} x nominal fell below half the baseline {:.3}: the arbiter \
         throttled bulk",
        bulk(Arm::Ibfq),
        bulk(Arm::Baseline),
    );
}

/// A CDF panel: x = pooled one-way latency (ms, clamped), y = percentile, one
/// line per arm. Written as SVG so the run owns its plot.
fn render_cdf_svg(dir: &str, pooled: &[(Arm, Vec<f64>)], reps: &[(Arm, usize, HolSummary, f64)]) {
    let colors = ["#1f77b4", "#2ca02c", "#d62728"];
    let w = 960.0_f64;
    let h = 600.0_f64;
    let (left, right, top, bottom) = (80.0_f64, 210.0_f64, 40.0_f64, 60.0_f64);
    let plot_w = w - left - right;
    let plot_h = h - top - bottom;
    let x_max = 120.0_f64;
    let x = |ms: f64| left + (ms.clamp(0.0, x_max) / x_max) * plot_w;
    let y = |pct: f64| top + plot_h - (pct / 100.0) * plot_h;
    let mut svg = String::new();
    let _ = writeln!(
        svg,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w}\" height=\"{h}\" font-family=\"monospace\">"
    );
    let _ = writeln!(svg, "<rect width=\"{w}\" height=\"{h}\" fill=\"white\"/>");
    let _ = writeln!(
        svg,
        "<text x=\"{left}\" y=\"22\" font-size=\"14\">interactive one-way latency CDF — baseline vs ibfq (x clamped to {x_max:.0} ms)</text>"
    );
    for pct in [0.0, 25.0, 50.0, 75.0, 100.0] {
        let yy = y(pct);
        let _ = writeln!(
            svg,
            "<line x1=\"{left}\" y1=\"{yy:.1}\" x2=\"{:.1}\" y2=\"{yy:.1}\" stroke=\"#e5e5e5\"/>",
            left + plot_w
        );
        let _ = writeln!(
            svg,
            "<text x=\"{:.1}\" y=\"{:.1}\" font-size=\"11\" text-anchor=\"end\">{pct:.0}%</text>",
            left - 8.0,
            yy + 4.0
        );
    }
    for ms in [0.0, 20.0, 40.0, 60.0, 80.0, 100.0, 120.0] {
        let xx = x(ms);
        let _ = writeln!(
            svg,
            "<line x1=\"{xx:.1}\" y1=\"{top:.1}\" x2=\"{xx:.1}\" y2=\"{:.1}\" stroke=\"#f3f3f3\"/>",
            top + plot_h
        );
        let _ = writeln!(
            svg,
            "<text x=\"{xx:.1}\" y=\"{:.1}\" font-size=\"11\" text-anchor=\"middle\">{ms:.0}</text>",
            top + plot_h + 18.0
        );
    }
    let _ = writeln!(
        svg,
        "<text x=\"{:.1}\" y=\"{:.1}\" font-size=\"12\" text-anchor=\"middle\">one-way latency (ms)</text>",
        left + plot_w / 2.0,
        h - 12.0
    );
    for (i, arm) in ARMS.iter().enumerate() {
        let Some((_, samples)) = pooled.iter().find(|(a, _)| a == arm) else {
            continue;
        };
        let pts = cdf_points(samples.clone());
        if pts.is_empty() {
            continue;
        }
        let mut path = String::new();
        for (j, (ms, pct)) in pts.iter().enumerate() {
            let _ = write!(
                path,
                "{}{:.1},{:.1} ",
                if j == 0 { "M" } else { "L" },
                x(*ms),
                y(*pct)
            );
        }
        let _ = writeln!(
            svg,
            "<path d=\"{path}\" fill=\"none\" stroke=\"{}\" stroke-width=\"2.5\"/>",
            colors[i]
        );
        let ly = top + 18.0 + i as f64 * 46.0;
        let lx = left + plot_w + 24.0;
        let _ = writeln!(
            svg,
            "<line x1=\"{lx:.1}\" y1=\"{:.1}\" x2=\"{:.1}\" y2=\"{:.1}\" stroke=\"{}\" stroke-width=\"2.5\"/>",
            ly - 4.0,
            lx + 22.0,
            ly - 4.0,
            colors[i]
        );
        let _ = writeln!(
            svg,
            "<text x=\"{:.1}\" y=\"{:.1}\" font-size=\"12\">{}</text>",
            lx + 28.0,
            ly,
            arm.name()
        );
        let vals: Vec<f64> = reps
            .iter()
            .filter(|(a, _, _, _)| a == arm)
            .flat_map(|_| Vec::<f64>::new())
            .collect();
        let _ = &vals;
        let mut rep_p99: Vec<f64> = reps
            .iter()
            .filter(|(a, _, _, _)| a == arm)
            .map(|(_, _, s, _)| s.p99)
            .collect();
        rep_p99.sort_by(|a, b| a.total_cmp(b));
        let _ = writeln!(
            svg,
            "<text x=\"{:.1}\" y=\"{:.1}\" font-size=\"11\" fill=\"#666\">p99/reps {:?}</text>",
            lx + 28.0,
            ly + 14.0,
            rep_p99
                .iter()
                .map(|v| format!("{v:.0}"))
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    let _ = writeln!(svg, "</svg>");
    std::fs::write(format!("{dir}/ab-cdf.svg"), svg).unwrap();
}
