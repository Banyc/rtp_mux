#![allow(clippy::disallowed_methods)]

use rtp_mux::{ExplorerConfig, RtpMuxConnector, RtpMuxConnectorConfig, RtpMuxServer};

use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn spawn_echo_server() -> SocketAddr {
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
    let addr = server.listener().local_addr();
    let sessions = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
    let spawner = rtp_mux::SessionSpawner::new(move |fut| {
        sessions.lock().unwrap().spawn(fut);
    });
    tokio::spawn(server.serve(spawner, |stream| {
        tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(stream);
            let _ = tokio::io::copy(&mut reader, &mut writer).await;
            let _ = writer.shutdown().await;
        });
    }));
    addr
}

async fn wait_until(mut cond: impl AsyncFnMut() -> bool, deadline: Duration, what: &str) {
    let started = std::time::Instant::now();
    while !cond().await {
        assert!(started.elapsed() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn redial_dial_lands_on_the_surrendered_candidate_port() {
    let test = async {
        let addr = spawn_echo_server().await;
        let bind: rtp_mux::BindSelector =
            Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
        let (connector, driver) = RtpMuxConnector::with_config(RtpMuxConnectorConfig {
            explorer: ExplorerConfig {
                enabled: true,
                candidates: 4,
                probe_mean_interval: Duration::from_millis(200),
                rotation_period: Duration::from_secs(3600),
            },
            ..RtpMuxConnectorConfig::standard(bind, false)
        });
        let _driver = tokio::spawn(driver);
        let mut stream = connector.connect_stream(addr).await.unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        let old_id = connector.probe_session(addr).unwrap().id();
        wait_until(
            async || {
                let report = connector.explorer_report(addr).await.unwrap();
                report.candidates.len() == 4 && report.candidates.iter().all(|c| c.alive)
            },
            Duration::from_secs(30),
            "all explorer candidates alive",
        )
        .await;
        let before: HashSet<u16> = connector
            .explorer_report(addr)
            .await
            .unwrap()
            .candidates
            .iter()
            .map(|c| c.local_addr.port())
            .collect();
        connector.force_redial(addr);
        wait_until(
            async || {
                connector
                    .probe_session(addr)
                    .is_some_and(|p| p.id() != old_id)
            },
            Duration::from_secs(15),
            "replacement session after redial",
        )
        .await;
        let fresh = connector.connect_stream(addr).await.unwrap();
        let session_port = fresh.addr().local_addr.port();
        assert!(
            before.contains(&session_port),
            "session port {session_port} is not one of the surrendered candidates {before:?}"
        );
        let after: HashSet<u16> = connector
            .explorer_report(addr)
            .await
            .unwrap()
            .candidates
            .iter()
            .map(|c| c.local_addr.port())
            .collect();
        assert!(
            !after.contains(&session_port),
            "surrendered candidate {session_port} still listed by the explorer"
        );
        stream.write_all(b"pong").await.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
        drop((stream, fresh));
    };
    tokio::time::timeout(Duration::from_secs(90), test)
        .await
        .expect("explorer handoff test timed out");
}
