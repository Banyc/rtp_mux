use rtp_mux::{RtpMuxConnector, RtpMuxConnectorConfig, RtpMuxServer};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PAYLOAD: usize = 256 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn a_session_counts_its_streams_and_the_bytes_they_carried() {
    let server = RtpMuxServer::bind("127.0.0.1:0", false).await.unwrap();
    let addr = server.listener().local_addr();
    tokio::spawn(server.serve(|stream| {
        tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(stream);
            let _ = tokio::io::copy(&mut reader, &mut writer).await;
            let _ = writer.shutdown().await;
        });
    }));
    let bind: rtp_mux::BindSelector = Arc::new(|addr: SocketAddr| SocketAddr::new(addr.ip(), 0));
    let connector = RtpMuxConnector::with_config(RtpMuxConnectorConfig::standard(bind, false));
    let mut first = connector.connect_stream(addr).await.unwrap();
    let second = connector.connect_stream(addr).await.unwrap();
    let probe = connector.probe_session(addr).expect("a session must exist");
    let born = probe.stats().expect("a live session must report stats");
    assert_eq!(born.live_streams, 2);
    assert_eq!(born.opened_streams, 2);
    let payload = vec![0xA7u8; PAYLOAD];
    first.write_all(&payload).await.unwrap();
    let mut echoed = vec![0u8; PAYLOAD];
    first.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, payload, "echo corrupted");
    let busy = probe.stats().expect("a live session must report stats");
    assert!(
        busy.tx_bytes >= PAYLOAD as u64,
        "sent {PAYLOAD} bytes but the session counted tx_bytes={}",
        busy.tx_bytes
    );
    assert!(
        busy.rx_bytes >= PAYLOAD as u64,
        "read {PAYLOAD} bytes back but the session counted rx_bytes={}",
        busy.rx_bytes
    );
    assert!(
        busy.tx_bytes_per_sec() > 0.0 && busy.rx_bytes_per_sec() > 0.0,
        "a session that moved bytes must report a rate: {busy}"
    );
    drop(second);
    let after = loop {
        let stats = probe.stats().expect("the session outlives one stream");
        if stats.live_streams == 1 {
            break stats;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(after.opened_streams, 2, "opened_streams must not go down");
    assert!(after.tx_bytes >= busy.tx_bytes);
}
