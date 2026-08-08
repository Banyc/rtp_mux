#![allow(clippy::disallowed_methods)]

use std::{net::SocketAddr, sync::Arc};

use mux::LaneClass;
use rtp_mux::{RtpMuxConnectorConfig, RtpMuxServer, connect_bidirectional_session};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;

use support::TestScope;

#[tokio::test(flavor = "multi_thread")]
async fn listening_side_can_open_a_stream_to_the_dialing_side() {
    let mut scope = TestScope::new();
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
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
    scope
        .run(async move {
            let client =
                connect_bidirectional_session(addr, RtpMuxConnectorConfig::standard(bind, false))
                    .await
                    .unwrap();
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
