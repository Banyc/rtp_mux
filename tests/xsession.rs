#![allow(clippy::disallowed_methods)]

use rtp_mux::{RtpMuxConnector, RtpMuxConnectorConfig, RtpMuxServer};

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;

use support::TestScope;

const DOWNLOAD_LEN: usize = 12 * 1024 * 1024;

const UPLOAD_LEN: usize = 12 * 1024 * 1024;

const GATE: usize = 2 * 1024 * 1024;

const CHUNK: usize = 64 * 1024;

const CMD_DOWNLOAD: u8 = b'D';

const CMD_UPLOAD: u8 = b'U';

async fn spawn_cmd_server(scope: &mut TestScope) -> SocketAddr {
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
    let addr = server.listener().local_addr();
    // The serve loop and every future it spawns (session supervisors and
    // per-stream command handlers) are submitted through the bounded reaper,
    // so a child panic surfaces immediately through `scope.run`.
    let submitter = scope.submitter(support::TEST_TASK_QUEUE_BOUND);
    let spawner = rtp_mux::SessionSpawner::new({
        let submitter = submitter.clone();
        move |fut| {
            submitter.submit(fut);
        }
    });
    scope.spawn_required("rtp_mux server serve loop", async move {
        let _ = server
            .serve(spawner, {
                let submitter = submitter.clone();
                move |stream| {
                    submitter.submit(Box::pin(async move {
                        let (mut reader, mut writer) = tokio::io::split(stream);
                        let mut cmd = [0u8; 1];
                        if reader.read_exact(&mut cmd).await.is_err() {
                            return;
                        }
                        match cmd[0] {
                            CMD_DOWNLOAD => {
                                let chunk = vec![0xCDu8; CHUNK];
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
                                let mut buf = vec![0u8; CHUNK];
                                let mut total = 0usize;
                                let mut clean = true;
                                while total < UPLOAD_LEN {
                                    match reader.read(&mut buf).await {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => {
                                            clean &= buf[..n].iter().all(|b| *b == 0xC5);
                                            total += n;
                                        }
                                    }
                                }
                                if total == UPLOAD_LEN && clean {
                                    let _ = writer.write_all(&[1u8]).await;
                                    let _ = writer.flush().await;
                                }
                                let _ = writer.shutdown().await;
                            }
                            _ => {}
                        }
                    }));
                }
            })
            .await;
    });
    addr
}

async fn wait_until(mut cond: impl FnMut() -> bool, deadline: Duration, what: &str) {
    let started = std::time::Instant::now();
    while !cond() {
        assert!(started.elapsed() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn xsession_migration_end_to_end() {
    let mut scope = TestScope::new();
    let addr = spawn_cmd_server(&mut scope).await;
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    let (connector, driver) =
        RtpMuxConnector::with_config(RtpMuxConnectorConfig::standard(bind, false));
    scope.spawn_required("rtp_mux connector driver", driver);
    tokio::time::timeout(
        Duration::from_secs(120),
        scope.run(async {
            let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let downloaded = Arc::new(AtomicUsize::new(0));
            let mut down = connector.connect_stream(addr).await.unwrap();
            let mut down_tasks = tokio::task::JoinSet::new();
            down_tasks.spawn({
                let downloaded = Arc::clone(&downloaded);
                let released = Arc::clone(&released);
                async move {
                    down.write_all(&[CMD_DOWNLOAD]).await.unwrap();
                    let mut buf = vec![0u8; CHUNK];
                    let mut clean = true;
                    loop {
                        while downloaded.load(Ordering::Relaxed) >= GATE
                            && !released.load(Ordering::SeqCst)
                        {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        match down.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                clean &= buf[..n].iter().all(|b| *b == 0xCD);
                                downloaded.fetch_add(n, Ordering::Relaxed);
                            }
                            Err(error) => panic!("download read failed: {error}"),
                        }
                    }
                    clean
                }
            });
            let uploaded = Arc::new(AtomicUsize::new(0));
            let mut up = connector.connect_stream(addr).await.unwrap();
            let mut up_tasks = tokio::task::JoinSet::new();
            up_tasks.spawn({
                let uploaded = Arc::clone(&uploaded);
                let released = Arc::clone(&released);
                async move {
                    up.write_all(&[CMD_UPLOAD]).await.unwrap();
                    let chunk = vec![0xC5u8; CHUNK];
                    let mut sent = 0usize;
                    while sent < UPLOAD_LEN {
                        while sent >= GATE && !released.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        up.write_all(&chunk).await.unwrap();
                        sent += chunk.len();
                        uploaded.fetch_add(chunk.len(), Ordering::Relaxed);
                    }
                    up.flush().await.unwrap();
                    let mut ack = [0u8; 1];
                    up.read_exact(&mut ack).await.unwrap();
                    ack[0] == 1
                }
            });
            let old_probe = connector.probe_session(addr).expect("session must exist");
            assert_eq!(old_probe.live_streams(), Some(2));
            wait_until(
                || {
                    downloaded.load(Ordering::Relaxed) >= GATE
                        && uploaded.load(Ordering::Relaxed) >= GATE
                },
                Duration::from_secs(30),
                "transfers to reach the gate",
            )
            .await;
            connector.force_redial(addr);
            wait_until(
                || {
                    connector
                        .probe_session(addr)
                        .is_some_and(|p| p.id() != old_probe.id())
                },
                Duration::from_secs(15),
                "replacement session in the sessions map",
            )
            .await;
            let new_probe = connector.probe_session(addr).unwrap();
            wait_until(
                || new_probe.live_streams() == Some(2),
                Duration::from_secs(15),
                "live-stream gauge to reach the fresh session",
            )
            .await;
            wait_until(
                || !old_probe.is_alive(),
                Duration::from_secs(15),
                "old session Arc release",
            )
            .await;
            assert!(
                down_tasks.try_join_next().is_none() && up_tasks.try_join_next().is_none(),
                "old session released only after the transfers ended - migration not proven"
            );
            released.store(true, Ordering::SeqCst);
            let (down_clean, up_acked) = tokio::join!(down_tasks.join_next(), up_tasks.join_next());
            let down_clean = down_clean.unwrap().unwrap();
            let up_acked = up_acked.unwrap().unwrap();
            assert!(down_clean, "download corrupted");
            assert_eq!(
                downloaded.load(Ordering::Relaxed),
                DOWNLOAD_LEN,
                "download truncated"
            );
            assert!(up_acked, "upload not acked byte-exact");
        }),
    )
    .await
    .expect("xsession migration test timed out");
}
