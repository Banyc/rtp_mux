#![allow(clippy::disallowed_methods)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use mux::LaneClass;
use rtp_mux::{RtpMuxConnectorConfig, RtpMuxServer, connect_bidirectional_session};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;

use support::TestScope;

/// `submit_required` is used only by this test, so it lives here instead of
/// the shared `support` module — that module is compiled into every test
/// binary, and each binary that never calls the method would flag it as dead
/// code.
trait RequiredSubmit {
    /// Submit a test-owned task that must stay alive until the test body
    /// completes. Completing early (e.g. a session driver returning a
    /// `MuxError` while the body is still running) panics with the returned
    /// value, so the root reaper cascades the panic into the test instead of
    /// discarding the completion silently.
    fn submit_required<F, T>(&self, name: &'static str, fut: F)
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: std::fmt::Debug;
}

impl RequiredSubmit for support::task_scope::TestTaskSubmitter {
    fn submit_required<F, T>(&self, name: &'static str, fut: F)
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: std::fmt::Debug,
    {
        self.submit(Box::pin(async move {
            let output = fut.await;
            panic!("required task '{name}' exited before the test body completed: {output:?}");
        }));
    }
}

/// Serve `server` and run one full dual-lane birth plus a ping/pong payload
/// exchange over an interactive stream opened from the listening side.  The
/// server and the connector config must already agree on the handshake mode
/// (the default-enabled ping/pong test and the matching-disabled test both
/// route through here).
async fn run_ping_pong(mut scope: TestScope, server: RtpMuxServer, config: RtpMuxConnectorConfig) {
    let addr = server.listener().local_addr();
    let (session_tx, mut session_rx) = tokio::sync::mpsc::channel(1);
    let submitter = scope.submitter(support::TEST_TASK_QUEUE_BOUND);
    let spawner = rtp_mux::SessionSpawner::new({
        let submitter = submitter.clone();
        move |fut| submitter.submit(fut)
    });
    scope.spawn_required("rtp_mux session server", async move {
        let _ = server
            .serve_sessions(spawner, move |session| {
                session_tx
                    .try_send(session)
                    .expect("session receiver must be ready");
            })
            .await;
    });
    scope
        .run(async move {
            let client = connect_bidirectional_session(addr, config).await.unwrap();
            let server = session_rx.recv().await.unwrap();
            let (server_opener, _server_accepter, _, server_driver) = server.into_parts();
            let (_client_opener, mut client_accepter, _, client_driver) = client.into_parts();
            // Both session drivers must stay alive until the body finishes:
            // a driver returning its MuxError early (the session ended) is a
            // failure, so `submit_required` panics with the returned error
            // and the root reaper cascades it into the test.
            submitter.submit_required("server session driver", server_driver);
            submitter.submit_required("client session driver", client_driver);
            let (opened, accepted) = tokio::join!(
                server_opener.open(LaneClass::Interactive),
                client_accepter.accept(),
            );
            let (mut server_read, mut server_write) = opened.unwrap();
            let (mut client_read, mut client_write, lane) = accepted.unwrap();
            assert_eq!(lane, LaneClass::Interactive);
            server_write.write_all(b"ping").await.unwrap();
            let mut request = [0; 4];
            client_read.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"ping");
            client_write.write_all(b"pong").await.unwrap();
            let mut response = [0; 4];
            server_read.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"pong");
        })
        .await;
}

/// The default-enabled bidirectional ping/pong test: both the server and the
/// connector use the default handshake mode (enabled), so the full RTP
/// opening handshake runs on both lanes before the mux lane hello.
#[tokio::test(flavor = "multi_thread")]
async fn listening_side_can_open_a_stream_to_the_dialing_side() {
    let scope = TestScope::new();
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    run_ping_pong(scope, server, RtpMuxConnectorConfig::standard(bind, false)).await;
}

/// Matching-disabled handshake mode still opens a full dual-lane session with
/// payload exchange: both endpoints toggle the RTP opening handshake off and
/// agree.
#[tokio::test(flavor = "multi_thread")]
async fn matching_disabled_handshake_mode_opens_session() {
    let scope = TestScope::new();
    let server = RtpMuxServer::bind("127.0.0.1:0", false)
        .await
        .unwrap()
        .with_handshake(false);
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    run_ping_pong(
        scope,
        server,
        RtpMuxConnectorConfig::standard(bind, false).with_handshake(false),
    )
    .await;
}

/// A mismatched handshake mode is a deployment error that times out: the
/// server disabled the RTP opening handshake while the client (default)
/// expects it.  The client's opening handshake waits on its internal
/// three-second retries, so assert within a two-second outer timeout that no
/// session opens and no server session is delivered.
#[tokio::test(flavor = "multi_thread")]
async fn mismatched_handshake_mode_does_not_open_session() {
    let mut scope = TestScope::new();
    let server = RtpMuxServer::bind("127.0.0.1:0", false)
        .await
        .unwrap()
        .with_handshake(false);
    let addr = server.listener().local_addr();
    let (session_tx, mut session_rx) = tokio::sync::mpsc::channel(1);
    let submitter = scope.submitter(support::TEST_TASK_QUEUE_BOUND);
    let spawner = rtp_mux::SessionSpawner::new({
        let submitter = submitter.clone();
        move |fut| submitter.submit(fut)
    });
    scope.spawn_required("rtp_mux session server", async move {
        let _ = server
            .serve_sessions(spawner, move |session| {
                session_tx
                    .try_send(session)
                    .expect("session receiver must be ready");
            })
            .await;
    });
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    // The client keeps its default (handshake enabled) while the server
    // disabled it: never negotiated, retried without a handshake, or
    // silently downgraded — the connect must time out.
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        connect_bidirectional_session(addr, RtpMuxConnectorConfig::standard(bind, false)),
    )
    .await;
    assert!(
        outcome.is_err(),
        "mismatched handshake modes must not open a session"
    );
    assert!(
        session_rx.try_recv().is_err(),
        "a mismatched server delivered a session"
    );
}
