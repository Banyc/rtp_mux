// ═══════════════════════════════════════════════════════════════════════════════
// rtp_mux helpers
// ═══════════════════════════════════════════════════════════════════════════════

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use netem_test::kit::{
    LATENCY_SAMPLE_CAPACITY, TestScope, TestTask, TestTaskSubmitter, submit_test_task,
    submit_test_task_required, try_send_observation,
};

/// The kit's interactive-lane FEC preset for the FEC-recovery probe: the
/// maximum-diversity tuning (three parity copies for the trailing
/// single-symbol group) plus in-stream group FEC.
///
/// This is deliberately **stronger** than the composition's default policy
/// ([`crate::shared::interactive_lane_fec_policy`], which
/// [`crate::RtpMuxServer::new`] and [`crate::RtpMuxConnectorConfig::standard`]
/// install): the probe asserts parity per arm on a seeded loss realization,
/// and the prompt preset (one parity copy) opens the reactive gate on some
/// seeds while skipping it on others, so the per-arm parity gate is only
/// satisfiable with the three-copy preset. The composition's own default is
/// asserted by its own tests, not here; keeping both values in one place is
/// what stops this preset from being mistaken for the shipped policy (see
/// `the_fec_probe_preset_is_stronger_than_the_shipped_default` below).
pub(crate) fn probe_interactive_fec_tuning() -> (crate::FecTuning, bool) {
    (crate::FecTuning::max_diversity(), true)
}

/// Per-lane typed RTP metrics observers: the interactive lane keeps its own
/// observer and the bulk lane keeps its own, independent of the lane-aware
/// FEC policy.
#[derive(Clone, Default)]
pub struct RtpMuxMetricsObservers {
    pub interactive: Option<crate::MetricsObserver>,
    pub bulk: Option<crate::MetricsObserver>,
}

/// Typed FEC evidence for one lane endpoint: whether any RTP metrics were
/// observed at all, plus the connection-lifetime FEC counters when the lane
/// enabled FEC (`None` when the lane is non-FEC — never a fabricated zero).
#[derive(Debug, Clone, Copy, Default)]
pub struct LaneFecEvidence {
    pub observed: bool,
    pub counters: Option<crate::MetricsFecCounters>,
}

/// Captures per-lane FEC evidence through sampled RTP metrics observers.
/// Evidence is sampled at most every 50 ms so snapshot cost stays bounded.
#[derive(Clone)]
pub struct RtpMuxFecCapture {
    interactive: Arc<Mutex<LaneFecEvidence>>,
    bulk: Arc<Mutex<LaneFecEvidence>>,
}

impl RtpMuxFecCapture {
    pub fn observers(&self) -> RtpMuxMetricsObservers {
        RtpMuxMetricsObservers {
            interactive: Some(sampled_fec_observer(Arc::clone(&self.interactive))),
            bulk: Some(sampled_fec_observer(Arc::clone(&self.bulk))),
        }
    }

    pub fn interactive(&self) -> LaneFecEvidence {
        *self.interactive.lock().unwrap()
    }

    pub fn bulk(&self) -> LaneFecEvidence {
        *self.bulk.lock().unwrap()
    }
}

impl Default for RtpMuxFecCapture {
    fn default() -> Self {
        Self {
            interactive: Arc::new(Mutex::new(LaneFecEvidence::default())),
            bulk: Arc::new(Mutex::new(LaneFecEvidence::default())),
        }
    }
}

/// An RTP metrics observer that captures a state snapshot at most every
/// `SAMPLE_INTERVAL_US`, recording the observed flag and the snapshot's
/// typed FEC counters into `evidence`.
fn sampled_fec_observer(evidence: Arc<Mutex<LaneFecEvidence>>) -> crate::MetricsObserver {
    const SAMPLE_INTERVAL_US: u64 = 50_000;
    let last_sample_us = Arc::new(AtomicU64::new(u64::MAX));
    let filter_clock = Arc::clone(&last_sample_us);
    crate::MetricsObserver::filtered(
        move |_, elapsed| {
            let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
            let previous = filter_clock.load(Ordering::Relaxed);
            if previous != u64::MAX && elapsed_us.saturating_sub(previous) < SAMPLE_INTERVAL_US {
                return false;
            }
            filter_clock
                .compare_exchange(previous, elapsed_us, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        },
        move |observation| {
            let Some(snapshot) = observation.snapshot else {
                return;
            };
            *evidence.lock().unwrap() = LaneFecEvidence {
                observed: true,
                counters: snapshot.fec_counters,
            };
        },
    )
}

/// Shared core for [`spawn_rtp_mux_latency_bulk_server`] and its `_via`
/// variant: binds the server and hands the required serve loop to
/// `spawn_required` (either a [`TestScope`] spawn or the bounded reaper
/// submission). `task_tx` is the bounded submission channel that the serve
/// loop uses to submit session and per-stream sink tasks; it is returned so
/// callers can keep it alive and submit more.
async fn spawn_rtp_mux_latency_bulk_server_core(
    spawn_required: impl FnOnce(&'static str, TestTask),
    task_tx: TestTaskSubmitter,
    base: Instant,
    observers: RtpMuxMetricsObservers,
) -> std::io::Result<(
    std::net::SocketAddr,
    std::net::SocketAddr,
    mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
    TestTaskSubmitter,
)> {
    let fec_tuning = probe_interactive_fec_tuning();
    let server = crate::RtpMuxServer::bind("127.0.0.1:0", crate::RtpMuxServerConfig::default())
        .await?
        .with_metrics_observers(observers.interactive, observers.bulk)
        .with_interactive_fec_tuning(fec_tuning.0, fec_tuning.1);
    let interactive_addr = server.listener().local_addr();
    let bulk_addr = server.bulk_listener().local_addr();
    let (tx, rx) = mpsc::channel(LATENCY_SAMPLE_CAPACITY);
    let bulk_delivered = Arc::new(AtomicU64::new(0));
    let bulk_for_server = Arc::clone(&bulk_delivered);
    // The serve loop must stay alive for the whole test body; an early exit
    // fails the test instead of silently tearing down the server.
    spawn_required("rtp_mux server serve loop", {
        let task_tx = task_tx.clone();
        Box::pin(async move {
            let spawner = crate::SessionSpawner::new({
                let task_tx = task_tx.clone();
                move |fut| {
                    submit_test_task(&task_tx, fut);
                }
            });
            let _ = server
                .serve(spawner, move |stream| {
                    let source_lane = stream.source_lane();
                    let (reader, writer) = tokio::io::split(stream);
                    spawn_tagged_stream_sink(
                        &task_tx,
                        reader,
                        writer,
                        tx.clone(),
                        Arc::clone(&bulk_for_server),
                        base,
                        source_lane == crate::LaneClass::Interactive,
                    );
                })
                .await;
        })
    });
    Ok((interactive_addr, bulk_addr, rx, bulk_delivered, task_tx))
}

/// Spawn an rtp_mux server with interactive and bulk listeners. Accepted
/// streams are classified by lane class: interactive-lane streams are treated
/// as timestamped latency streams; bulk-lane streams are treated as
/// deterministic byte sinks.
///
/// Session futures spawned by the `SessionSpawner` and the per-stream sink
/// tasks are submitted through a bounded channel feeding one test-owned
/// reaper (spawned into `tasks`), which selects between submissions and
/// `join_next()` completions and unwraps every completion. The returned
/// handle is the submission channel; keep it alive to hold the channel open
/// and submit additional test tasks.
pub async fn spawn_rtp_mux_latency_bulk_server(
    tasks: &mut TestScope,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    std::net::SocketAddr,
    mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
    TestTaskSubmitter,
)> {
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    spawn_rtp_mux_latency_bulk_server_core(
        |name, fut| tasks.spawn_required(name, fut),
        task_tx.clone(),
        base,
        RtpMuxMetricsObservers::default(),
    )
    .await
}

/// [`spawn_rtp_mux_latency_bulk_server`] through the bounded task-submission
/// handle, for use inside [`TestScope::run`] bodies where `&mut TestScope`
/// is unavailable. The serve loop is submitted as required through the
/// handle; the returned handle is a clone of the caller's submission handle.
pub async fn spawn_rtp_mux_latency_bulk_server_via(
    tx: &TestTaskSubmitter,
    base: Instant,
) -> std::io::Result<(
    std::net::SocketAddr,
    std::net::SocketAddr,
    mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
    TestTaskSubmitter,
)> {
    spawn_rtp_mux_latency_bulk_server_core(
        |name, fut| submit_test_task_required(tx, name, fut),
        tx.clone(),
        base,
        RtpMuxMetricsObservers::default(),
    )
    .await
}

/// [`spawn_rtp_mux_latency_bulk_server_via`] with per-lane RTP metrics
/// observers attached to the interactive and bulk lanes. The serve loop is
/// submitted as required through the handle.
pub async fn spawn_rtp_mux_latency_bulk_server_observed_via(
    tx: &TestTaskSubmitter,
    base: Instant,
    observers: RtpMuxMetricsObservers,
) -> std::io::Result<(
    std::net::SocketAddr,
    std::net::SocketAddr,
    mpsc::Receiver<(u8, f64)>,
    Arc<AtomicU64>,
    TestTaskSubmitter,
)> {
    spawn_rtp_mux_latency_bulk_server_core(
        |name, fut| submit_test_task_required(tx, name, fut),
        tx.clone(),
        base,
        observers,
    )
    .await
}

/// Shared core for [`rtp_mux_connector`] and its `_via` variant: builds the
/// connector and hands the driver future to `spawn` (either a [`TestScope`]
/// spawn or the bounded reaper submission).
fn rtp_mux_connector_core(
    spawn: impl FnOnce(TestTask),
    bulk_proxy_addr: std::net::SocketAddr,
    observers: RtpMuxMetricsObservers,
) -> crate::RtpMuxConnector {
    let bind: crate::BindSelector = Arc::new(|addr| match addr {
        std::net::SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
        std::net::SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
    });
    let bulk_addr: crate::BulkAddrSelector = Arc::new(move |_| Ok(bulk_proxy_addr));
    let fec_tuning = probe_interactive_fec_tuning();
    let (connector, driver) = crate::RtpMuxConnector::with_config(crate::RtpMuxConnectorConfig {
        bulk_addr,
        interactive_fec_tuning: fec_tuning.0,
        interactive_instream_group_fec: fec_tuning.1,
        interactive_metrics_observer: observers.interactive,
        bulk_metrics_observer: observers.bulk,
        explorer: crate::ExplorerConfig {
            enabled: false,
            ..crate::ExplorerConfig::default()
        },
        ..crate::RtpMuxConnectorConfig::standard(bind)
    });
    // The connector driver is a non-required background keepalive: like the
    // mux/rtp client session supervisors (e5efa1f6), it may finish at any
    // point because the driver exits once the connector's last handle is
    // dropped — i.e. at normal teardown when the body ends, which the body
    // completing concurrently with the driver's exit would otherwise misread
    // as a premature end. A driver that genuinely fails (a panicked
    // supervisor join inside `run_connector`) still fails the test: the scope
    // or reaper unwraps the completed future and re-raises the panic, and a
    // dead connector surfaces as connect/write errors on the streams.
    spawn(Box::pin(driver));
    connector
}

pub fn rtp_mux_connector(
    tasks: &mut TestScope,
    bulk_proxy_addr: std::net::SocketAddr,
) -> crate::RtpMuxConnector {
    rtp_mux_connector_core(
        |fut| tasks.spawn(fut),
        bulk_proxy_addr,
        RtpMuxMetricsObservers::default(),
    )
}

/// [`rtp_mux_connector`] through the bounded task-submission handle, for use
/// inside [`TestScope::run`] bodies where `&mut TestScope` is unavailable.
/// The connector driver is submitted as a non-required background keepalive
/// through the handle.
pub fn rtp_mux_connector_via(
    tx: &TestTaskSubmitter,
    bulk_proxy_addr: std::net::SocketAddr,
) -> crate::RtpMuxConnector {
    rtp_mux_connector_core(
        |fut| submit_test_task(tx, fut),
        bulk_proxy_addr,
        RtpMuxMetricsObservers::default(),
    )
}

/// [`rtp_mux_connector_via`] with per-lane RTP metrics observers attached to
/// the interactive and bulk lanes. The connector driver is submitted as a
/// non-required background keepalive through the handle.
pub fn rtp_mux_connector_observed_via(
    tx: &TestTaskSubmitter,
    bulk_proxy_addr: std::net::SocketAddr,
    observers: RtpMuxMetricsObservers,
) -> crate::RtpMuxConnector {
    rtp_mux_connector_core(|fut| submit_test_task(tx, fut), bulk_proxy_addr, observers)
}

/// Stream tag selecting the *echo* handler: the tagged sink parses the same
/// timestamped frames as [`ECHO_TAG`]'s sibling `b'L'` and writes every frame
/// straight back to its sender. An interactive request/response client uses it
/// to withhold the next request until the one it just offered has come back,
/// which is the only way an application can make a fresh tail *lone* (one
/// unacked data packet on the connection) instead of pipelined.
pub const ECHO_TAG: u8 = b'E';

pub fn spawn_tagged_stream_sink(
    task_tx: &TestTaskSubmitter,
    mut reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    mut writer: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
    tx: mpsc::Sender<(u8, f64)>,
    bulk: Arc<AtomicU64>,
    base: Instant,
    is_interactive: bool,
) {
    let task_tx = task_tx.clone();
    submit_test_task(
        &task_tx,
        Box::pin(async move {
            let mut tag = [0u8; 1];
            if reader.read_exact(&mut tag).await.is_err() {
                let _ = writer.shutdown().await;
                return;
            }
            let is_latency = is_interactive || tag[0] != b'B';
            if tag[0] == ECHO_TAG {
                // Echo: parse the timestamped frames, record the one-way
                // latency of each so the arm keeps its per-echo attribution,
                // and write the frame back verbatim on the same stream. The
                // echo half is what makes the client's next request wait for
                // this one's delivery.
                let mut buf = vec![0u8; 64 * 1024];
                let mut offset = 0usize;
                'echo: while let Ok(n) = reader.read(&mut buf[offset..]).await {
                    if n == 0 {
                        break;
                    }
                    offset += n;
                    loop {
                        if offset < 4 {
                            break;
                        }
                        let frame_len =
                            u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                        if frame_len < 12 {
                            break;
                        }
                        if offset < frame_len {
                            break;
                        }
                        let payload_end = frame_len - 8;
                        let sent_us = u64::from_le_bytes([
                            buf[payload_end],
                            buf[payload_end + 1],
                            buf[payload_end + 2],
                            buf[payload_end + 3],
                            buf[payload_end + 4],
                            buf[payload_end + 5],
                            buf[payload_end + 6],
                            buf[payload_end + 7],
                        ]);
                        let now_us = base.elapsed().as_micros() as u64;
                        let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
                        if !try_send_observation(&tx, (tag[0], latency_ms), "latency sample") {
                            break 'echo;
                        }
                        if writer.write_all(&buf[..frame_len]).await.is_err() {
                            break 'echo;
                        }
                        buf.copy_within(frame_len..offset, 0);
                        offset -= frame_len;
                    }
                }
            } else if is_latency {
                let mut buf = vec![0u8; 64 * 1024];
                let mut offset = 0usize;
                while let Ok(n) = reader.read(&mut buf[offset..]).await {
                    if n == 0 {
                        break;
                    }
                    offset += n;
                    loop {
                        if offset < 4 {
                            break;
                        }
                        let frame_len =
                            u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                        if frame_len < 12 {
                            break;
                        }
                        if offset < frame_len {
                            break;
                        }
                        let payload_end = frame_len - 8;
                        let sent_us = u64::from_le_bytes([
                            buf[payload_end],
                            buf[payload_end + 1],
                            buf[payload_end + 2],
                            buf[payload_end + 3],
                            buf[payload_end + 4],
                            buf[payload_end + 5],
                            buf[payload_end + 6],
                            buf[payload_end + 7],
                        ]);
                        let now_us = base.elapsed().as_micros() as u64;
                        let latency_ms = now_us.saturating_sub(sent_us) as f64 / 1000.0;
                        if !try_send_observation(&tx, (tag[0], latency_ms), "latency sample") {
                            break;
                        }
                        buf.copy_within(frame_len..offset, 0);
                        offset -= frame_len;
                    }
                }
            } else {
                let mut buf = vec![0u8; 64 * 1024];
                let mut offset: u64 = 0;
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let mut ok = true;
                            for (j, &actual) in buf[..n].iter().enumerate() {
                                let expected = ((offset + j as u64) % 251) as u8;
                                if actual != expected {
                                    ok = false;
                                    break;
                                }
                            }
                            if ok {
                                offset += n as u64;
                                bulk.fetch_add(n as u64, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            let _ = writer.shutdown().await;
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The composition ships `interactive_prompt()` (one parity copy) plus the
    /// transport's in-stream group FEC default; the FEC-recovery probe needs
    /// the three-copy maximum-diversity preset for its per-arm parity gate.
    /// Both halves are asserted here so the probe preset cannot be silently
    /// re-pointed at the shipped default (which makes the probe's per-arm gate
    /// seed-dependent and eventually green-by-accident) or the other way
    /// round.
    #[test]
    fn the_fec_probe_preset_is_stronger_than_the_shipped_default() {
        let probe = probe_interactive_fec_tuning();
        assert_eq!(
            probe,
            (crate::FecTuning::max_diversity(), true),
            "the FEC-recovery probe's preset is its own value, not a re-statement of the shipped policy",
        );
        let shipped = crate::shared::interactive_lane_fec_policy();
        assert_eq!(
            shipped.0,
            crate::FecTuning::interactive_prompt(),
            "the shipped interactive-lane default must stay the prompt preset",
        );
        assert_ne!(
            probe, shipped,
            "the probe preset must stay the stronger one; if it ever equals the shipped default, \
             the probe's per-arm parity gate no longer tests what it claims",
        );
    }
}
