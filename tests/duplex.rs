#![allow(clippy::disallowed_methods)]

use rtp_mux::{RtpMuxConnector, RtpMuxConnectorConfig, RtpMuxServer, ServerStream};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;

use support::TestScope;

const RESPONSE_CHUNKS: usize = 16;
const CHUNK: usize = 64 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn response_migration_end_to_end() {
    let mut scope = TestScope::new();
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
    let addr = server.listener().local_addr();
    let saw_duplex = Arc::new(AtomicBool::new(false));
    let saw_duplex_handler = Arc::clone(&saw_duplex);
    // The serve loop and every future it spawns (session supervisors and
    // per-stream handlers) are submitted through the bounded reaper, so a
    // child panic surfaces immediately through `scope.run`.
    let submitter = scope.submitter(support::TEST_TASK_QUEUE_BOUND);
    let spawner = rtp_mux::SessionSpawner::new({
        let submitter = submitter.clone();
        move |fut| {
            submitter.submit(fut);
        }
    });
    scope.spawn(async move {
        let _ = server
            .serve(spawner, {
                let submitter = submitter.clone();
                move |stream| {
                    saw_duplex_handler.store(
                        matches!(stream, ServerStream::MigratingDuplex { .. }),
                        Ordering::SeqCst,
                    );
                    submitter.submit(Box::pin(async move {
                        let mut stream = stream;
                        let mut req = [0u8; 4];
                        stream.read_exact(&mut req).await.unwrap();
                        assert_eq!(&req, b"ping");
                        let chunk = vec![0x5Au8; CHUNK];
                        for _ in 0..RESPONSE_CHUNKS {
                            stream.write_all(&chunk).await.unwrap();
                        }
                        stream.shutdown().await.unwrap();
                    }));
                }
            })
            .await;
    });
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    let (connector, driver) =
        RtpMuxConnector::with_config(RtpMuxConnectorConfig::standard(bind, false));
    scope.spawn(driver);
    scope
        .run(async {
            let mut stream = connector.connect_stream(addr).await.unwrap();
            stream.write_all(b"ping").await.unwrap();
            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await.unwrap();
            assert_eq!(resp.len(), RESPONSE_CHUNKS * CHUNK, "response truncated");
            assert!(resp.iter().all(|b| *b == 0x5A), "response corrupted");
            assert!(
                saw_duplex.load(Ordering::SeqCst),
                "server handler should have received a MigratingDuplex stream"
            );
        })
        .await;
}
