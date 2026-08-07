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

const RESPONSE_CHUNKS: usize = 16;
const CHUNK: usize = 64 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn response_migration_end_to_end() {
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
    let addr = server.listener().local_addr();
    let saw_duplex = Arc::new(AtomicBool::new(false));
    let saw_duplex_handler = Arc::clone(&saw_duplex);
    let sessions = Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new()));
    let spawner = rtp_mux::SessionSpawner::new(move |fut| {
        sessions.lock().unwrap().spawn(fut);
    });
    tokio::spawn(server.serve(spawner, move |stream| {
        saw_duplex_handler.store(
            matches!(stream, ServerStream::MigratingDuplex { .. }),
            Ordering::SeqCst,
        );
        tokio::spawn(async move {
            let mut stream = stream;
            let mut req = [0u8; 4];
            stream.read_exact(&mut req).await.unwrap();
            assert_eq!(&req, b"ping");
            let chunk = vec![0x5Au8; CHUNK];
            for _ in 0..RESPONSE_CHUNKS {
                stream.write_all(&chunk).await.unwrap();
            }
            stream.shutdown().await.unwrap();
        });
    }));
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    let (connector, driver) =
        RtpMuxConnector::with_config(RtpMuxConnectorConfig::standard(bind, false));
    let _driver = tokio::spawn(driver);
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
}
