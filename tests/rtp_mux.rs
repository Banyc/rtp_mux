use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use netem_test::{NetemConfig, NetemPair};
use rtp_mux::{
    BindSelector, BulkAddrSelector, ExplorerConfig, LaneClass, RtpMuxConnector,
    RtpMuxConnectorConfig, RtpMuxServer, RtpMuxServerConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use netem_test::kit::fan::PerFlowNetem;
use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::presets::clean;
use netem_test::kit::stats::combined_stats;
use netem_test::kit::{LANE_EVENT_CAPACITY, submit_test_task, try_send_observation};

async fn spawn_echo_server_via(
    task_tx: &netem_test::kit::TestTaskSubmitter,
) -> io::Result<(
    SocketAddr,
    SocketAddr,
    tokio::sync::mpsc::Receiver<LaneClass>,
)> {
    let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default()).await?;
    let interactive_addr = server.listener().local_addr();
    let bulk_addr = server.bulk_listener().local_addr();
    let (lane_tx, lane_rx) = tokio::sync::mpsc::channel(LANE_EVENT_CAPACITY);
    // Session futures spawned by the SessionSpawner and the per-stream echo
    // tasks are submitted through a bounded channel feeding one test-owned
    // reaper, which selects between submissions and join_next() completions
    // and unwraps every completion so panics surface.
    netem_test::kit::submit_test_task_required(task_tx, "rtp_mux echo server", {
        let task_tx = task_tx.clone();
        async move {
            let spawner = rtp_mux::SessionSpawner::new({
                let task_tx = task_tx.clone();
                move |fut| {
                    submit_test_task(&task_tx, fut);
                }
            });
            let _ = server
                .serve(spawner, {
                    let task_tx = task_tx.clone();
                    move |stream| {
                        if !try_send_observation(&lane_tx, stream.source_lane(), "lane event") {
                            return;
                        }
                        let task_tx = task_tx.clone();
                        submit_test_task(
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
    Ok((interactive_addr, bulk_addr, lane_rx))
}

fn connector_via(
    task_tx: &netem_test::kit::TestTaskSubmitter,
    bulk_proxy_addr: SocketAddr,
) -> RtpMuxConnector {
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
    // The driver is a non-required background keepalive: it exits when the
    // connector's last handle is dropped (normal teardown at body end); a
    // panicked driver still surfaces through the scope's reaper unwrap.
    netem_test::kit::submit_test_task(task_tx, Box::pin(driver));
    connector
}

async fn echo_round_trip(
    connector: &RtpMuxConnector,
    interactive_proxy_addr: SocketAddr,
    lane: LaneClass,
    payload: &[u8],
) -> Vec<u8> {
    let mut stream = connector
        .connect_stream_with_lane(interactive_proxy_addr, lane)
        .await
        .unwrap();
    let first = payload.len().min(1);
    stream.write_all(&payload[..first]).await.unwrap();
    stream.write_all(&payload[first..]).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut echoed = Vec::with_capacity(payload.len());
    stream.read_to_end(&mut echoed).await.unwrap();
    echoed
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp_mux dual-lane scenario over NetemPair; slow end-to-end; run with --ignored --nocapture --test-threads=1"]
async fn rtp_mux_clean_dual_lane_echoes_interactive_and_bulk_streams() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (interactive_server, bulk_server, mut accepted_lanes) =
                spawn_echo_server_via(&task_tx).await.unwrap();
            let interactive_pair = NetemPair::spawn(interactive_server, clean(), clean()).unwrap();
            let bulk_pair = NetemPair::spawn(bulk_server, clean(), clean()).unwrap();
            let connector = connector_via(&task_tx, bulk_pair.client_addr());
            let interactive_payload = payload(64 * 1024);
            let bulk_payload = payload(512 * 1024);
            let (interactive_echo, bulk_echo) = with_timeout(
                Duration::from_secs(30),
                "rtp_mux clean dual-lane echo",
                async {
                    tokio::join!(
                        echo_round_trip(
                            &connector,
                            interactive_pair.client_addr(),
                            LaneClass::Interactive,
                            &interactive_payload
                        ),
                        echo_round_trip(
                            &connector,
                            interactive_pair.client_addr(),
                            LaneClass::Bulk,
                            &bulk_payload
                        ),
                    )
                },
            )
            .await;
            assert_eq!(interactive_echo, interactive_payload);
            assert_eq!(bulk_echo, bulk_payload);
            let first_lane = accepted_lanes.recv().await.unwrap();
            let second_lane = accepted_lanes.recv().await.unwrap();
            assert!(
                (first_lane == LaneClass::Interactive && second_lane == LaneClass::Bulk)
                    || (first_lane == LaneClass::Bulk && second_lane == LaneClass::Interactive)
            );
            assert!(combined_stats(&interactive_pair).forwarded > 0);
            assert!(combined_stats(&bulk_pair).forwarded > 0);
            interactive_pair.stop();
            bulk_pair.stop();
        })
        .await;
}

const DOWNLOAD_LEN: usize = 8 * 1024 * 1024;
const PING_LEN: usize = 8;
const PING_INTERVAL: Duration = Duration::from_millis(40);
const CMD_DOWNLOAD: u8 = b'D';
const CMD_PING: u8 = b'P';
async fn spawn_cmd_server_via(
    task_tx: &netem_test::kit::TestTaskSubmitter,
) -> io::Result<(SocketAddr, SocketAddr)> {
    let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default()).await?;
    let interactive_addr = server.listener().local_addr();
    let bulk_addr = server.bulk_listener().local_addr();
    // Session futures spawned by the SessionSpawner and the per-stream
    // handler tasks are submitted through a bounded channel feeding one
    // test-owned reaper, which selects between submissions and join_next()
    // completions and unwraps every completion so panics surface.
    netem_test::kit::submit_test_task_required(task_tx, "rtp_mux cmd server", {
        let task_tx = task_tx.clone();
        async move {
            let spawner = rtp_mux::SessionSpawner::new({
                let task_tx = task_tx.clone();
                move |fut| {
                    submit_test_task(&task_tx, fut);
                }
            });
            let _ = server
                .serve(spawner, {
                    let task_tx = task_tx.clone();
                    move |stream| {
                        let task_tx = task_tx.clone();
                        submit_test_task(
                            &task_tx,
                            Box::pin(async move {
                                let (mut reader, mut writer) = tokio::io::split(stream);
                                let mut cmd = [0u8; 1];
                                if reader.read_exact(&mut cmd).await.is_err() {
                                    return;
                                }
                                match cmd[0] {
                                    CMD_DOWNLOAD => {
                                        let chunk = vec![0xCDu8; 64 * 1024];
                                        let mut sent = 0;
                                        while sent < DOWNLOAD_LEN {
                                            if writer.write_all(&chunk).await.is_err() {
                                                return;
                                            }
                                            sent += chunk.len();
                                        }
                                        let _ = writer.shutdown().await;
                                    }
                                    CMD_UPLOAD => {
                                        let mut buf = vec![0u8; 64 * 1024];
                                        let mut total = 0usize;
                                        while total < UPLOAD_LEN {
                                            match reader.read(&mut buf).await {
                                                Ok(0) | Err(_) => break,
                                                Ok(n) => total += n,
                                            }
                                        }
                                        if total == UPLOAD_LEN {
                                            let _ = writer.write_all(&[1u8]).await;
                                            let _ = writer.flush().await;
                                        }
                                        let _ = writer.shutdown().await;
                                    }
                                    CMD_PING => {
                                        let mut buf = [0u8; PING_LEN];
                                        while reader.read_exact(&mut buf).await.is_ok() {
                                            if writer.write_all(&buf).await.is_err()
                                                || writer.flush().await.is_err()
                                            {
                                                break;
                                            }
                                        }
                                        let _ = writer.shutdown().await;
                                    }
                                    _ => {}
                                }
                            }),
                        );
                    }
                })
                .await;
        }
    });
    Ok((interactive_addr, bulk_addr))
}
fn contended_lane() -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(20),
        rate: 16_000_000,
        queue_limit_pkts: 120,
        seed: 811,
        ..NetemConfig::default()
    }
}
struct ResponseArm {
    ping_rtts_ms: Vec<f64>,
    downloaded: usize,
    download_secs: f64,
    bulk_lane_wire_pkts: u64,
}
async fn run_response_arm() -> ResponseArm {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (interactive_server, bulk_server) = spawn_cmd_server_via(&task_tx).await.unwrap();
            let interactive_pair =
                NetemPair::spawn(interactive_server, contended_lane(), contended_lane()).unwrap();
            let bulk_pair =
                NetemPair::spawn(bulk_server, contended_lane(), contended_lane()).unwrap();
            let connector = connector_via(&task_tx, bulk_pair.client_addr());
            let mut ping = connector
                .connect_stream(interactive_pair.client_addr())
                .await
                .unwrap();
            ping.write_all(&[CMD_PING]).await.unwrap();
            let mut download = connector
                .connect_stream(interactive_pair.client_addr())
                .await
                .unwrap();
            let mut download_tasks: tokio::task::JoinSet<(usize, f64)> =
                tokio::task::JoinSet::new();
            download_tasks.spawn(async move {
                let started = std::time::Instant::now();
                download.write_all(&[CMD_DOWNLOAD]).await.unwrap();
                let mut buf = vec![0u8; 64 * 1024];
                let mut total = 0usize;
                loop {
                    match download.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => total += n,
                    }
                }
                (total, started.elapsed().as_secs_f64())
            });
            let mut rtts = Vec::new();
            let mut seq = 0u64;
            let mut buf = [0u8; PING_LEN];
            let mut download = None;
            while download.is_none() {
                seq += 1;
                let sent = std::time::Instant::now();
                ping.write_all(&seq.to_le_bytes()).await.unwrap();
                ping.read_exact(&mut buf).await.unwrap();
                assert_eq!(u64::from_le_bytes(buf), seq, "ping echo out of sequence");
                rtts.push(sent.elapsed().as_secs_f64() * 1e3);
                if let Some(result) = download_tasks.try_join_next() {
                    download = Some(result);
                }
                tokio::time::sleep(PING_INTERVAL).await;
            }
            let (downloaded, download_secs) =
                download.expect("download task never completed").unwrap();
            let bulk_lane_wire_pkts = combined_stats(&bulk_pair).forwarded;
            interactive_pair.stop();
            bulk_pair.stop();
            ResponseArm {
                ping_rtts_ms: rtts,
                downloaded,
                download_secs,
                bulk_lane_wire_pkts,
            }
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp_mux dual-lane scenario over NetemPair; slow end-to-end; run with --ignored --nocapture --test-threads=1"]
async fn rtp_mux_survives_independent_impaired_lanes() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (interactive_server, bulk_server, _accepted_lanes) =
                spawn_echo_server_via(&task_tx).await.unwrap();
            let interactive_impairment = NetemConfig {
                latency: Duration::from_millis(15),
                jitter: Duration::from_millis(3),
                loss: u32::MAX / 100,
                seed: 801,
                ..NetemConfig::default()
            };
            let bulk_impairment = NetemConfig {
                latency: Duration::from_millis(35),
                jitter: Duration::from_millis(8),
                loss: u32::MAX / 50,
                seed: 802,
                ..NetemConfig::default()
            };
            let interactive_pair = NetemPair::spawn(
                interactive_server,
                interactive_impairment.clone(),
                interactive_impairment,
            )
            .unwrap();
            let bulk_pair =
                NetemPair::spawn(bulk_server, bulk_impairment.clone(), bulk_impairment).unwrap();
            let connector = connector_via(&task_tx, bulk_pair.client_addr());
            let interactive_payload = payload(16 * 1024);
            let bulk_payload = payload(256 * 1024);
            let (interactive_echo, bulk_echo) = with_timeout(
                Duration::from_secs(90),
                "rtp_mux independently impaired lanes",
                async {
                    tokio::join!(
                        echo_round_trip(
                            &connector,
                            interactive_pair.client_addr(),
                            LaneClass::Interactive,
                            &interactive_payload
                        ),
                        echo_round_trip(
                            &connector,
                            interactive_pair.client_addr(),
                            LaneClass::Bulk,
                            &bulk_payload
                        ),
                    )
                },
            )
            .await;
            assert_eq!(interactive_echo, interactive_payload);
            assert_eq!(bulk_echo, bulk_payload);
            assert!(combined_stats(&interactive_pair).forwarded > 0);
            assert!(combined_stats(&bulk_pair).forwarded > 0);
            interactive_pair.stop();
            bulk_pair.stop();
        })
        .await;
}

const CMD_UPLOAD: u8 = b'U';

const UPLOAD_LEN: usize = 8 * 1024 * 1024;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp_mux dual-lane scenario over NetemPair; slow end-to-end; run with --ignored --nocapture --test-threads=1"]
async fn rtp_mux_response_migration_offloads_download() {
    let arm = with_timeout(
        Duration::from_secs(120),
        "response migration",
        run_response_arm(),
    )
    .await;
    eprintln!(
        "[resp-mig] download {} B in {:.1}s ({:.2} MiB/s) pings={} bulk_lane_wire={} pkts",
        arm.downloaded,
        arm.download_secs,
        arm.downloaded as f64 / (1024.0 * 1024.0) / arm.download_secs,
        arm.ping_rtts_ms.len(),
        arm.bulk_lane_wire_pkts
    );
    assert_eq!(arm.downloaded, DOWNLOAD_LEN, "download truncated");
    assert!(arm.ping_rtts_ms.len() >= 20, "too few ping samples");
    let arms = [("migrating", arm.ping_rtts_ms.as_slice())];
    if let Ok(path) = netem_test::report::dump_csv("rtp_mux_response_migration", &arms) {
        eprintln!("[resp-mig] samples: {}", path.display());
    }
    eprintln!(
        "{}",
        netem_test::report::ab_report("response migration ping RTT", "ms", &arms)
    );
    assert!(
        arm.bulk_lane_wire_pkts > 2000,
        "the download should ride the bulk lane (saw {} pkts)",
        arm.bulk_lane_wire_pkts
    );
    let mut m = arm.ping_rtts_ms.clone();
    m.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p90 = netem_test::report::percentile(&m, 0.90);
    assert!(
        p90 < 65.0,
        "pings must stay clear of the download: p90={p90:.1}ms"
    );
}

struct BidirArm {
    ping_rtts_ms: Vec<f64>,
    downloaded: usize,
    uploaded_ok: bool,
    bulk_lane_wire_pkts: u64,
}

async fn run_bidir_arm() -> BidirArm {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (interactive_server, bulk_server) = spawn_cmd_server_via(&task_tx).await.unwrap();
            let interactive_pair =
                NetemPair::spawn(interactive_server, contended_lane(), contended_lane()).unwrap();
            let bulk_pair =
                NetemPair::spawn(bulk_server, contended_lane(), contended_lane()).unwrap();
            let connector = connector_via(&task_tx, bulk_pair.client_addr());
            let mut ping = connector
                .connect_stream(interactive_pair.client_addr())
                .await
                .unwrap();
            ping.write_all(&[CMD_PING]).await.unwrap();
            let mut download = connector
                .connect_stream(interactive_pair.client_addr())
                .await
                .unwrap();
            let mut download_tasks: tokio::task::JoinSet<usize> = tokio::task::JoinSet::new();
            download_tasks.spawn(async move {
                download.write_all(&[CMD_DOWNLOAD]).await.unwrap();
                let mut buf = vec![0u8; 64 * 1024];
                let mut total = 0usize;
                loop {
                    match download.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => total += n,
                    }
                }
                total
            });
            let mut upload = connector
                .connect_stream(interactive_pair.client_addr())
                .await
                .unwrap();
            let mut upload_tasks: tokio::task::JoinSet<bool> = tokio::task::JoinSet::new();
            upload_tasks.spawn(async move {
                upload.write_all(&[CMD_UPLOAD]).await.unwrap();
                let chunk = vec![0xC5u8; 64 * 1024];
                let mut sent = 0usize;
                while sent < UPLOAD_LEN {
                    if upload.write_all(&chunk).await.is_err() {
                        return false;
                    }
                    sent += chunk.len();
                }
                if upload.flush().await.is_err() {
                    return false;
                }
                let mut ack = [0u8; 1];
                upload.read_exact(&mut ack).await.is_ok() && ack[0] == 1
            });
            let mut rtts = Vec::new();
            let mut seq = 0u64;
            let mut buf = [0u8; PING_LEN];
            let mut download = None;
            let mut upload = None;
            while download.is_none() || upload.is_none() {
                seq += 1;
                let sent = std::time::Instant::now();
                ping.write_all(&seq.to_le_bytes()).await.unwrap();
                ping.read_exact(&mut buf).await.unwrap();
                assert_eq!(u64::from_le_bytes(buf), seq, "ping echo out of sequence");
                rtts.push(sent.elapsed().as_secs_f64() * 1e3);
                if let Some(result) = download_tasks.try_join_next() {
                    download = Some(result);
                }
                if let Some(result) = upload_tasks.try_join_next() {
                    upload = Some(result);
                }
                tokio::time::sleep(PING_INTERVAL).await;
            }
            let downloaded = download.expect("download task never completed").unwrap();
            let uploaded_ok = upload.expect("upload task never completed").unwrap();
            let bulk_lane_wire_pkts = combined_stats(&bulk_pair).forwarded;
            interactive_pair.stop();
            bulk_pair.stop();
            BidirArm {
                ping_rtts_ms: rtts,
                downloaded,
                uploaded_ok,
                bulk_lane_wire_pkts,
            }
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp_mux dual-lane scenario over NetemPair; slow end-to-end; run with --ignored --nocapture --test-threads=1"]
async fn rtp_mux_bidirectional_contention_offloads_both_transfers() {
    let arm = with_timeout(
        Duration::from_secs(180),
        "bidirectional contention",
        run_bidir_arm(),
    )
    .await;
    eprintln!(
        "[bidir] download {} B upload_ok={} pings={} bulk_lane_wire={} pkts",
        arm.downloaded,
        arm.uploaded_ok,
        arm.ping_rtts_ms.len(),
        arm.bulk_lane_wire_pkts,
    );
    assert_eq!(arm.downloaded, DOWNLOAD_LEN, "download truncated");
    assert!(arm.uploaded_ok, "upload not fully acked");
    assert!(arm.ping_rtts_ms.len() >= 20, "too few ping samples");
    let arms = [("migrating", arm.ping_rtts_ms.as_slice())];
    if let Ok(path) = netem_test::report::dump_csv("rtp_mux_bidirectional_contention", &arms) {
        eprintln!("[bidir] samples: {}", path.display());
    }
    eprintln!(
        "{}",
        netem_test::report::ab_report("bidirectional contention ping RTT", "ms", &arms)
    );
    assert!(
        arm.bulk_lane_wire_pkts > 12_000,
        "BOTH transfers should ride the bulk lane (saw {} pkts)",
        arm.bulk_lane_wire_pkts,
    );
    let mut m = arm.ping_rtts_ms.clone();
    m.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p90 = netem_test::report::percentile(&m, 0.90);
    assert!(
        p90 < 80.0,
        "pings must stay clear of both transfers: p90={p90:.1}ms"
    );
}

struct RecycleArm {
    ping_rtts_ms: Vec<f64>,
    downloaded: usize,
    download_clean: bool,
    download_secs: f64,
    old_session_died: bool,
    session_replaced: bool,
}

async fn run_recycle_arm() -> RecycleArm {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (interactive_server, bulk_server) = spawn_cmd_server_via(&task_tx).await.unwrap();
            let interactive_fan =
                PerFlowNetem::spawn(interactive_server, || (contended_lane(), contended_lane()))
                    .unwrap();
            let bulk_fan =
                PerFlowNetem::spawn(bulk_server, || (contended_lane(), contended_lane())).unwrap();
            let connector = connector_via(&task_tx, bulk_fan.client_addr());
            let addr = interactive_fan.client_addr();
            let mut ping = connector.connect_stream(addr).await.unwrap();
            ping.write_all(&[CMD_PING]).await.unwrap();
            let downloaded_gauge = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut download = connector.connect_stream(addr).await.unwrap();
            let mut download_tasks: tokio::task::JoinSet<(usize, bool, f64)> =
                tokio::task::JoinSet::new();
            download_tasks.spawn({
                let gauge = Arc::clone(&downloaded_gauge);
                async move {
                    let started = std::time::Instant::now();
                    download.write_all(&[CMD_DOWNLOAD]).await.unwrap();
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut total = 0usize;
                    let mut clean = true;
                    loop {
                        match download.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                clean &= buf[..n].iter().all(|b| *b == 0xCD);
                                total += n;
                                gauge.store(total, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                    (total, clean, started.elapsed().as_secs_f64())
                }
            });
            let old_probe = connector.probe_session(addr).expect("session must exist");
            let mut rtts = Vec::new();
            let mut seq = 0u64;
            let mut buf = [0u8; PING_LEN];
            let mut recycled = false;
            let mut download = None;
            while download.is_none() {
                seq += 1;
                let sent = std::time::Instant::now();
                ping.write_all(&seq.to_le_bytes()).await.unwrap();
                ping.read_exact(&mut buf).await.unwrap();
                assert_eq!(u64::from_le_bytes(buf), seq, "ping echo out of sequence");
                rtts.push(sent.elapsed().as_secs_f64() * 1e3);
                if !recycled
                    && downloaded_gauge.load(std::sync::atomic::Ordering::Relaxed) > 2 * 1024 * 1024
                {
                    recycled = true;
                    connector.force_redial(addr);
                }
                if let Some(result) = download_tasks.try_join_next() {
                    download = Some(result);
                }
                tokio::time::sleep(PING_INTERVAL).await;
            }
            let (downloaded, download_clean, download_secs) =
                download.expect("download task never completed").unwrap();
            let session_replaced = connector
                .probe_session(addr)
                .is_some_and(|probe| probe.id() != old_probe.id());
            let mut old_session_died = false;
            for _ in 0..100 {
                if !old_probe.is_alive() {
                    old_session_died = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            interactive_fan.stop();
            bulk_fan.stop();
            RecycleArm {
                ping_rtts_ms: rtts,
                downloaded,
                download_clean,
                download_secs,
                old_session_died,
                session_replaced,
            }
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp_mux dual-lane scenario over NetemPair; slow end-to-end; run with --ignored --nocapture --test-threads=1"]
async fn rtp_mux_recycle_migrates_live_streams() {
    let arm = with_timeout(
        Duration::from_secs(120),
        "recycle migration",
        run_recycle_arm(),
    )
    .await;
    eprintln!(
        "[recycle-mig] download {} B in {:.1}s ({:.2} MiB/s) pings={} replaced={} old_died={}",
        arm.downloaded,
        arm.download_secs,
        arm.downloaded as f64 / (1024.0 * 1024.0) / arm.download_secs,
        arm.ping_rtts_ms.len(),
        arm.session_replaced,
        arm.old_session_died,
    );
    let arms = [("migrating", arm.ping_rtts_ms.as_slice())];
    if let Ok(path) = netem_test::report::dump_csv("rtp_mux_recycle_migration", &arms) {
        eprintln!("[recycle-mig] samples: {}", path.display());
    }
    assert_eq!(arm.downloaded, DOWNLOAD_LEN, "download truncated");
    assert!(arm.download_clean, "download corrupted");
    assert!(
        arm.session_replaced,
        "recycle did not install a fresh session"
    );
    assert!(arm.old_session_died, "old session never released");
    assert!(
        arm.download_secs < 20.0,
        "download took {:.1}s - successor-deadline stall suspected",
        arm.download_secs
    );
    assert!(arm.ping_rtts_ms.len() >= 20, "too few ping samples");
    let mut m = arm.ping_rtts_ms.clone();
    m.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p90 = netem_test::report::percentile(&m, 0.90);
    assert!(
        p90 < 80.0,
        "pings must stay clean across the recycle: p90={p90:.1}ms"
    );
}

fn explorer_slow_lane(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(40),
        rate: 16_000_000,
        queue_limit_pkts: 120,
        seed,
        ..NetemConfig::default()
    }
}

fn explorer_fast_lane(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(5),
        rate: 16_000_000,
        queue_limit_pkts: 120,
        seed,
        ..NetemConfig::default()
    }
}

struct ExplorerArm {
    pre_rtts_ms: Vec<f64>,
    post_rtts_ms: Vec<f64>,
    downloaded: usize,
    download_clean: bool,
    download_secs: f64,
    session_replaced: bool,
    session_port: u16,
    fast_candidate_port: u16,
    noop_survived: bool,
}

async fn run_explorer_arm() -> ExplorerArm {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (interactive_server, bulk_server) = spawn_cmd_server_via(&task_tx).await.unwrap();
            let interactive_flows = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let interactive_fan = PerFlowNetem::spawn(interactive_server, {
                let flows = Arc::clone(&interactive_flows);
                move || {
                    let index = flows.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let seed = 900 + index as u64;
                    if index == 1 {
                        (explorer_fast_lane(seed), explorer_fast_lane(seed))
                    } else {
                        (explorer_slow_lane(seed), explorer_slow_lane(seed))
                    }
                }
            })
            .unwrap();
            let bulk_fan = PerFlowNetem::spawn(bulk_server, || {
                (explorer_slow_lane(950), explorer_slow_lane(951))
            })
            .unwrap();
            let bind: BindSelector = Arc::new(|addr| match addr {
                SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
                SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
            });
            let bulk_proxy_addr = bulk_fan.client_addr();
            let bulk_addr: BulkAddrSelector = Arc::new(move |_| Ok(bulk_proxy_addr));
            let connector = {
                let (connector, driver) = RtpMuxConnector::with_config(RtpMuxConnectorConfig {
                    bulk_addr,
                    explorer: ExplorerConfig {
                        enabled: true,
                        probe_mean_interval: Duration::from_millis(250),
                        rotation_period: Duration::from_secs(60),
                        ..ExplorerConfig::default()
                    },
                    ..RtpMuxConnectorConfig::standard(bind)
                });
                // The driver is a non-required background keepalive: it exits
                // when the connector's last handle is dropped (normal teardown
                // at body end); a panicked driver still surfaces through the
                // scope's reaper unwrap.
                netem_test::kit::submit_test_task(&task_tx, Box::pin(driver));
                connector
            };
            let addr = interactive_fan.client_addr();
            let mut ping = connector.connect_stream(addr).await.unwrap();
            ping.write_all(&[CMD_PING]).await.unwrap();
            let downloaded_gauge = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut download = connector.connect_stream(addr).await.unwrap();
            let mut download_tasks: tokio::task::JoinSet<(usize, bool, f64)> =
                tokio::task::JoinSet::new();
            download_tasks.spawn({
                let gauge = Arc::clone(&downloaded_gauge);
                async move {
                    let started = std::time::Instant::now();
                    download.write_all(&[CMD_DOWNLOAD]).await.unwrap();
                    let mut buf = vec![0u8; 64 * 1024];
                    let mut total = 0usize;
                    let mut clean = true;
                    loop {
                        match download.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                clean &= buf[..n].iter().all(|b| *b == 0xCD);
                                total += n;
                                gauge.store(total, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                    (total, clean, started.elapsed().as_secs_f64())
                }
            });
            let old_probe = connector.probe_session(addr).expect("session must exist");
            let mut pre_rtts = Vec::new();
            let mut post_rtts = Vec::new();
            let mut seq = 0u64;
            let mut buf = [0u8; PING_LEN];
            let mut fast_candidate_port: Option<u16> = None;
            let mut download = None;
            while download.is_none() {
                seq += 1;
                let sent = std::time::Instant::now();
                ping.write_all(&seq.to_le_bytes()).await.unwrap();
                ping.read_exact(&mut buf).await.unwrap();
                assert_eq!(u64::from_le_bytes(buf), seq, "ping echo out of sequence");
                let rtt = sent.elapsed().as_secs_f64() * 1e3;
                if fast_candidate_port.is_some() {
                    post_rtts.push(rtt);
                } else {
                    pre_rtts.push(rtt);
                }
                if fast_candidate_port.is_none()
                    && downloaded_gauge.load(std::sync::atomic::Ordering::Relaxed) > 2 * 1024 * 1024
                {
                    let report = connector.explorer_report(addr).await.unwrap();
                    let fast = report.candidates.iter().find(|candidate| {
                        candidate.alive
                            && candidate
                                .rtt
                                .is_some_and(|rtt| rtt < Duration::from_millis(40))
                    });
                    let active_probed = report.active.is_some_and(|active| active.alive);
                    if let (Some(fast), true) = (fast, active_probed) {
                        fast_candidate_port = Some(fast.local_addr.port());
                        connector.reoptimize(addr);
                    }
                }
                if let Some(result) = download_tasks.try_join_next() {
                    download = Some(result);
                }
                tokio::time::sleep(PING_INTERVAL).await;
            }
            let (downloaded, download_clean, download_secs) =
                download.expect("download task never completed").unwrap();
            let fast_candidate_port =
                fast_candidate_port.expect("explorer never converged on the fast tuple");
            let session_replaced = connector
                .probe_session(addr)
                .is_some_and(|probe| probe.id() != old_probe.id());
            let fresh = connector.connect_stream(addr).await.unwrap();
            let session_port = fresh.addr().local_addr.port();
            drop(fresh);
            let migrated_probe = connector.probe_session(addr).expect("migrated session");
            connector.reoptimize(addr);
            tokio::time::sleep(Duration::from_secs(2)).await;
            let noop_survived = connector
                .probe_session(addr)
                .is_some_and(|probe| probe.id() == migrated_probe.id());
            interactive_fan.stop();
            bulk_fan.stop();
            ExplorerArm {
                pre_rtts_ms: pre_rtts,
                post_rtts_ms: post_rtts,
                downloaded,
                download_clean,
                download_secs,
                session_replaced,
                session_port,
                fast_candidate_port,
                noop_survived,
            }
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp_mux dual-lane scenario over NetemPair; slow end-to-end; run with --ignored --nocapture --test-threads=1"]
async fn rtp_mux_explorer_relays_onto_better_path() {
    let arm = with_timeout(
        Duration::from_secs(120),
        "explorer re-lay",
        run_explorer_arm(),
    )
    .await;
    let p = |mut v: Vec<f64>, q| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        netem_test::report::percentile(&v, q)
    };
    eprintln!(
        "[explorer] download {} B in {:.1}s pings pre={} post={} pre_p50={:.1}ms post_p50={:.1}ms post_p90={:.1}ms port {} == {}",
        arm.downloaded,
        arm.download_secs,
        arm.pre_rtts_ms.len(),
        arm.post_rtts_ms.len(),
        p(arm.pre_rtts_ms.clone(), 0.50),
        p(arm.post_rtts_ms.clone(), 0.50),
        p(arm.post_rtts_ms.clone(), 0.90),
        arm.session_port,
        arm.fast_candidate_port,
    );
    let arms = [
        ("pre_relay", arm.pre_rtts_ms.as_slice()),
        ("post_relay", arm.post_rtts_ms.as_slice()),
    ];
    if let Ok(path) = netem_test::report::dump_csv("rtp_mux_explorer_relay", &arms) {
        eprintln!("[explorer] samples: {}", path.display());
    }
    eprintln!(
        "{}",
        netem_test::report::ab_report("explorer re-lay ping RTT", "ms", &arms)
    );
    assert_eq!(arm.downloaded, DOWNLOAD_LEN, "download truncated");
    assert!(arm.download_clean, "download corrupted");
    assert!(arm.session_replaced, "reoptimize never re-laid the session");
    assert_eq!(
        arm.session_port, arm.fast_candidate_port,
        "the session was not laid on the surrendered candidate's tuple"
    );
    assert!(
        arm.download_secs < 20.0,
        "download took {:.1}s - successor-deadline stall suspected",
        arm.download_secs
    );
    assert!(
        arm.post_rtts_ms.len() >= 20,
        "too few post-migration ping samples"
    );
    let pre_p50 = p(arm.pre_rtts_ms.clone(), 0.50);
    let post_p50 = p(arm.post_rtts_ms.clone(), 0.50);
    let post_p90 = p(arm.post_rtts_ms.clone(), 0.90);
    assert!(
        post_p90 < 80.0,
        "pings must ride the fast tuple after the re-lay: p90={post_p90:.1}ms"
    );
    assert!(
        post_p50 < pre_p50,
        "migration must actually improve the interactive path ({pre_p50:.1}ms -> {post_p50:.1}ms)"
    );
    assert!(
        arm.noop_survived,
        "reoptimize on the best tuple must be a no-op"
    );
}
