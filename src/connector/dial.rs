use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use mux::{
    GroupToken, LaneClass, MuxError, PairingNonce,
    spawn_mux_no_reconnection_with_first_receive_deadline_and_ready,
};
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::{
    shared::{
        BIRTH_LIVENESS_DEADLINE, BIRTH_LIVENESS_GRACE, MAX_DUAL_CONNECT_ATTEMPTS, client_mux_config,
    },
    traffic::SessionTraffic,
};

use super::{BindSelector, BulkAddrSelector};

pub(crate) struct ConnectedDualLaneBirth {
    pub(crate) opener: mux::DualStreamOpener,
    pub(crate) accepter: mux::DualStreamAccepter,
    pub(crate) local_addr: SocketAddr,
    pub(crate) nonce: PairingNonce,
    pub(crate) supervisor: JoinSet<MuxError>,
    pub(crate) probe_tap: Option<rtp::probe::ProbeTap>,
    pub(crate) traffic: Arc<SessionTraffic>,
}

pub(crate) type DualLaneDial = Pin<
    Box<dyn Future<Output = (SocketAddr, io::Result<ConnectedDualLaneBirth>)> + Send + 'static>,
>;
type DualLaneDialResult =
    Pin<Box<dyn Future<Output = io::Result<ConnectedDualLaneBirth>> + Send + 'static>>;
pub(crate) type DualLaneDialer = Arc<
    dyn Fn(SocketAddr, GroupToken, Option<tokio_udp::UdpSocket>) -> DualLaneDialResult
        + Send
        + Sync
        + 'static,
>;

pub(crate) fn dual_supervisor_result(
    result: Option<Result<MuxError, tokio::task::JoinError>>,
) -> MuxError {
    match result {
        Some(Ok(error)) => error,
        Some(Err(source)) => MuxError::TaskJoin {
            task: "dual_lane",
            source,
        },
        None => MuxError::TaskStopped { task: "dual_lane" },
    }
}

pub(crate) async fn connect_dual_lane(
    addr: SocketAddr,
    bind: BindSelector,
    bulk_addr: BulkAddrSelector,
    fec: bool,
    group: GroupToken,
    socket: Option<tokio_udp::UdpSocket>,
) -> io::Result<ConnectedDualLaneBirth> {
    let mut socket = socket;
    retry_dual_connect(addr, || {
        connect_dual_lane_once(
            addr,
            Arc::clone(&bind),
            Arc::clone(&bulk_addr),
            fec,
            group,
            socket.take(),
        )
    })
    .await
}

pub(crate) async fn retry_dual_connect<F, Fut>(
    addr: SocketAddr,
    mut once: F,
) -> io::Result<ConnectedDualLaneBirth>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = io::Result<ConnectedDualLaneBirth>>,
{
    let started = Instant::now();
    let mut failures = Vec::new();
    let mut last_kind = io::ErrorKind::Other;
    for attempt in 1..=MAX_DUAL_CONNECT_ATTEMPTS {
        let attempt_started = Instant::now();
        match once().await {
            Ok(birth) => {
                if attempt > 1 {
                    info!(?addr, attempt, failures = %failures.join(";"), elapsed_ms = started.elapsed().as_millis(), "RTP mux dual-lane birth recovered after retry");
                }
                return Ok(birth);
            }
            Err(error) => {
                let will_retry = attempt < MAX_DUAL_CONNECT_ATTEMPTS;
                last_kind = error.kind();
                failures.push(format!(
                    "attempt={attempt}, elapsed_ms={}, error={error}",
                    attempt_started.elapsed().as_millis(),
                ));
                debug!(
                    ?error,
                    ?addr,
                    attempt,
                    max_attempts = MAX_DUAL_CONNECT_ATTEMPTS,
                    will_retry,
                    elapsed_ms = attempt_started.elapsed().as_millis(),
                    "RTP mux dual-lane birth failed"
                );
                if will_retry {
                    tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                }
            }
        }
    }
    Err(io::Error::new(
        last_kind,
        format!(
            "RTP mux dual-lane birth failed after {} attempts in {} ms: {}",
            failures.len(),
            started.elapsed().as_millis(),
            failures.join("; "),
        ),
    ))
}

async fn connect_dual_lane_once(
    addr: SocketAddr,
    bind: BindSelector,
    bulk_addr: BulkAddrSelector,
    fec: bool,
    group: GroupToken,
    socket: Option<tokio_udp::UdpSocket>,
) -> io::Result<ConnectedDualLaneBirth> {
    let bind_addr = bind(addr);
    let bulk_addr = bulk_addr(addr)?;
    let config = || rtp::udp::ConnectConfig {
        handshake: false,
        fec,
        ..rtp::udp::ConnectConfig::default()
    };
    let mut interactive = match socket {
        Some(socket) => {
            rtp::udp::FrameDeliveryIo::connect_with_socket(socket, addr, config()).await?
        }
        None => rtp::udp::FrameDeliveryIo::connect(bind_addr, addr, config()).await?,
    };
    let interactive_local = interactive.local_addr;
    let probe_tap = interactive.probe_tap.take();
    let bulk =
        rtp::udp::FrameDeliveryIo::connect(SocketAddr::new(bind_addr.ip(), 0), bulk_addr, config())
            .await?;
    let nonce = PairingNonce::generate();
    let interactive_reader = interactive.read;
    let mut interactive_writer = interactive.write;
    let bulk_reader = bulk.read;
    let mut bulk_writer = bulk.write;
    if let Err(error) = mux::write_lane_hello(
        &mut interactive_writer,
        LaneClass::Interactive,
        nonce,
        group,
    )
    .await
    {
        return Err(io::Error::other(format!(
            "interactive lane hello: {error:?}"
        )));
    }
    if let Err(error) = mux::write_lane_hello(&mut bulk_writer, LaneClass::Bulk, nonce, group).await
    {
        let _ = interactive_writer.send_kill_and_abort().await;
        return Err(io::Error::other(format!("bulk lane hello: {error:?}")));
    }
    let traffic = Arc::new(SessionTraffic::default());
    let interactive_reader = traffic.count_read(interactive_reader);
    let interactive_writer = traffic.count_write(interactive_writer);
    let bulk_reader = traffic.count_read(bulk_reader);
    let bulk_writer = traffic.count_write(bulk_writer);
    let mut interactive_tasks = JoinSet::new();
    let (interactive_opener, interactive_accepter, interactive_ready) =
        spawn_mux_no_reconnection_with_first_receive_deadline_and_ready(
            interactive_reader,
            interactive_writer,
            client_mux_config(),
            BIRTH_LIVENESS_DEADLINE,
            &mut interactive_tasks,
        );
    let mut bulk_tasks = JoinSet::new();
    let (bulk_opener, bulk_accepter, bulk_ready) =
        spawn_mux_no_reconnection_with_first_receive_deadline_and_ready(
            bulk_reader,
            bulk_writer,
            client_mux_config(),
            BIRTH_LIVENESS_DEADLINE,
            &mut bulk_tasks,
        );
    let mut supervisor = JoinSet::new();
    let (opener, accepter) = mux::spawn_dual_mux_paired_supervised(
        interactive_opener,
        interactive_accepter,
        interactive_tasks,
        bulk_opener,
        bulk_accepter,
        bulk_tasks,
        &mut supervisor,
    );
    let birth_deadline = tokio::time::sleep(BIRTH_LIVENESS_DEADLINE + BIRTH_LIVENESS_GRACE);
    tokio::pin!(birth_deadline);
    tokio::select! {
        biased;
        result = supervisor.join_next() => { let error = dual_supervisor_result(result); return Err(io::Error::new(io::ErrorKind::BrokenPipe, format!("dual-lane birth liveness failed: {error:?}"))); }
        ready = async { tokio::try_join!(interactive_ready, bulk_ready) } => { if ready.is_err() { return Err(io::Error::new(io::ErrorKind::BrokenPipe, "dual-lane birth readiness channel closed")); } }
        () = &mut birth_deadline => { return Err(io::Error::new(io::ErrorKind::TimedOut, "dual-lane birth liveness deadline exceeded")); }
    }
    Ok(ConnectedDualLaneBirth {
        opener,
        accepter,
        local_addr: interactive_local,
        nonce,
        supervisor,
        probe_tap,
        traffic,
    })
}
