use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::stream::{FuturesUnordered, StreamExt as _};
use metrics::counter;
use mux::{
    LaneClass, MuxError, PairingNonce, StreamReader,
    spawn_mux_no_reconnection_with_first_receive_deadline_and_ready,
};
use tokio::{sync::oneshot, task::JoinSet};
use tracing::{debug, info, trace, warn};

use crate::{
    client_stream::ClientStream,
    shared::{
        BIRTH_LIVENESS_DEADLINE, BIRTH_LIVENESS_GRACE, MAX_CONCURRENT_DUAL_DIALS,
        MAX_DIAL_WAITERS_PER_ADDR, MAX_DUAL_CONNECT_ATTEMPTS, bulk_client_mux_config,
        interactive_client_mux_config,
    },
    stream::SocketAddrPair,
};

pub type BindSelector = Arc<dyn Fn(SocketAddr) -> SocketAddr + Send + Sync + 'static>;
pub type BulkAddrSelector =
    Arc<dyn Fn(SocketAddr) -> io::Result<SocketAddr> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct RtpMuxConnectorConfig {
    pub bind: BindSelector,
    pub bulk_addr: BulkAddrSelector,
    pub fec: bool,
}

impl RtpMuxConnectorConfig {
    pub fn standard(bind: BindSelector, fec: bool) -> Self {
        Self {
            bind,
            bulk_addr: Arc::new(crate::shared::bulk_lane_addr),
            fec,
        }
    }
}

#[derive(Debug)]
pub struct OpenedStream {
    pub writer: mux::MigratingStreamWriter,
    pub reader: oneshot::Receiver<StreamReader>,
    pub addr: SocketAddrPair,
}

impl OpenedStream {
    pub fn into_stream(self) -> ClientStream {
        ClientStream::new(self.writer, self.reader, self.addr)
    }
}

struct ConnectedDualLane {
    opener: mux::DualStreamOpener,
    addr: SocketAddrPair,
    nonce: PairingNonce,
    connected_at: Instant,
    opened_streams: u64,
}

struct ConnectedDualLaneBirth {
    session: ConnectedDualLane,
    supervisor: JoinSet<MuxError>,
}

type DualLaneDial = Pin<
    Box<dyn Future<Output = (SocketAddr, io::Result<ConnectedDualLaneBirth>)> + Send + 'static>,
>;
type DualLaneDialResult =
    Pin<Box<dyn Future<Output = io::Result<ConnectedDualLaneBirth>> + Send + 'static>>;
type DualLaneDialer = Arc<dyn Fn(SocketAddr) -> DualLaneDialResult + Send + Sync + 'static>;

enum ConnectorCommand {
    Connect {
        addr: SocketAddr,
        lane: LaneClass,
        response: oneshot::Sender<io::Result<OpenedStream>>,
    },
    Reset {
        completed: oneshot::Sender<()>,
    },
}

pub struct RtpMuxConnector {
    commands: tokio::sync::mpsc::Sender<ConnectorCommand>,
    _connector: JoinSet<()>,
}

impl std::fmt::Debug for RtpMuxConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtpMuxConnector").finish_non_exhaustive()
    }
}

impl RtpMuxConnector {
    pub fn new(bind: BindSelector, fec: bool) -> Self {
        Self::with_config(RtpMuxConnectorConfig::standard(bind, fec))
    }

    pub fn with_config(config: RtpMuxConnectorConfig) -> Self {
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        let RtpMuxConnectorConfig {
            bind,
            bulk_addr,
            fec,
        } = config;
        let dialer: DualLaneDialer = Arc::new(move |addr| {
            let bind = Arc::clone(&bind);
            let bulk_addr = Arc::clone(&bulk_addr);
            Box::pin(async move { connect_dual_lane(addr, bind, bulk_addr, fec).await })
        });
        let mut connector = JoinSet::new();
        connector.spawn(run_connector(command_rx, dialer));
        Self {
            commands,
            _connector: connector,
        }
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<OpenedStream> {
        self.connect_with_lane(addr, LaneClass::Interactive).await
    }

    pub async fn connect_with_lane(
        &self,
        addr: SocketAddr,
        lane: LaneClass,
    ) -> io::Result<OpenedStream> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(ConnectorCommand::Connect {
                addr,
                lane,
                response,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTP mux connector stopped"))?;
        result.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "RTP mux connector dropped connect response",
            )
        })?
    }

    pub async fn connect_stream(&self, addr: SocketAddr) -> io::Result<ClientStream> {
        self.connect(addr).await.map(OpenedStream::into_stream)
    }

    pub async fn connect_stream_with_lane(
        &self,
        addr: SocketAddr,
        lane: LaneClass,
    ) -> io::Result<ClientStream> {
        self.connect_with_lane(addr, lane)
            .await
            .map(OpenedStream::into_stream)
    }

    pub async fn reset(&self) -> io::Result<()> {
        let (completed, done) = oneshot::channel();
        self.commands
            .send(ConnectorCommand::Reset { completed })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTP mux connector stopped"))?;
        done.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "RTP mux connector dropped reset acknowledgement",
            )
        })
    }
}

async fn run_connector(
    mut commands: tokio::sync::mpsc::Receiver<ConnectorCommand>,
    dialer: DualLaneDialer,
) {
    let mut sessions: HashMap<SocketAddr, ConnectedDualLane> = HashMap::new();
    let mut supervisors: JoinSet<(SocketAddr, MuxError)> = JoinSet::new();
    let mut pending_dials: FuturesUnordered<DualLaneDial> = FuturesUnordered::new();
    let mut in_flight_dials: HashSet<SocketAddr> = HashSet::new();
    let mut dial_waiters: HashMap<SocketAddr, Vec<StreamRequest>> = HashMap::new();

    loop {
        tokio::select! {
            Some(res) = supervisors.join_next() => {
                match res {
                    Ok((addr, error)) => {
                        if let Some(session) = sessions.remove(&addr) {
                            warn!(
                                event = "rtp_mux_session_terminated",
                                ?error,
                                nonce = ?session.nonce,
                                up = ?session.addr.peer_addr,
                                up_local = ?session.addr.local_addr,
                                opened_streams = session.opened_streams,
                                uptime_ms = session.connected_at.elapsed().as_millis(),
                                "RTP mux dual-lane session terminated",
                            );
                        } else {
                            warn!(
                                event = "rtp_mux_session_terminated",
                                ?error,
                                up = ?addr,
                                "RTP mux dual-lane session terminated without connector state",
                            );
                        }
                    }
                    Err(error) if error.is_cancelled() => trace!(?error, "Dual-lane MUX task cancelled"),
                    Err(error) => warn!(?error, "Dual-lane MUX supervision task failed to join"),
                }
            }
            Some((addr, result)) = pending_dials.next() => {
                in_flight_dials.remove(&addr);
                match result {
                    Ok(birth) => {
                        let ConnectedDualLaneBirth {
                            mut session,
                            mut supervisor,
                        } = birth;
                        supervisors.spawn(async move {
                            let result = supervisor.join_next().await;
                            (addr, dual_supervisor_result(result))
                        });
                        if let Some(waiters) = dial_waiters.remove(&addr) {
                            for waiter in waiters {
                                send_connected_stream(&mut session, waiter);
                            }
                        }
                        sessions.insert(addr, session);
                    }
                    Err(error) => {
                        let kind = error.kind();
                        let message = error.to_string();
                        if let Some(waiters) = dial_waiters.remove(&addr) {
                            for waiter in waiters {
                                let _ = waiter
                                    .response
                                    .send(Err(io::Error::new(kind, message.clone())));
                            }
                        }
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    ConnectorCommand::Reset { completed } => {
                        for (_, waiters) in dial_waiters.drain() {
                            for waiter in waiters {
                                let _ = waiter.response.send(Err(io::Error::new(
                                    io::ErrorKind::ConnectionAborted,
                                    "connector reset",
                                )));
                            }
                        }
                        in_flight_dials.clear();
                        sessions.clear();
                        supervisors = JoinSet::new();
                        pending_dials = FuturesUnordered::new();
                        let _ = completed.send(());
                    }
                    ConnectorCommand::Connect {
                        addr,
                        lane,
                        response,
                    } => {
                        let request = StreamRequest { lane, response };
                        if let Some(session) = sessions.get_mut(&addr) {
                            send_connected_stream(session, request);
                            continue;
                        }
                        let waiters = dial_waiters.entry(addr).or_default();
                        waiters.retain(|waiter| !waiter.response.is_closed());
                        if waiters.len() >= MAX_DIAL_WAITERS_PER_ADDR {
                            let _ = request.response.send(Err(io::Error::new(
                                io::ErrorKind::WouldBlock,
                                format!(
                                    "too many dial waiters for {addr} (max {MAX_DIAL_WAITERS_PER_ADDR})",
                                ),
                            )));
                            continue;
                        }
                        let is_in_flight = in_flight_dials.contains(&addr);
                        if !is_in_flight && pending_dials.len() >= MAX_CONCURRENT_DUAL_DIALS {
                            let _ = request.response.send(Err(io::Error::new(
                                io::ErrorKind::WouldBlock,
                                "too many concurrent dual dials",
                            )));
                            continue;
                        }
                        waiters.push(request);
                        if !is_in_flight {
                            in_flight_dials.insert(addr);
                            let dialer = Arc::clone(&dialer);
                            pending_dials.push(Box::pin(async move {
                                let result = dialer(addr).await;
                                (addr, result)
                            }));
                        }
                    }
                }
            }
        }
    }
}

struct StreamRequest {
    lane: LaneClass,
    response: oneshot::Sender<io::Result<OpenedStream>>,
}

fn send_connected_stream(session: &mut ConnectedDualLane, request: StreamRequest) {
    if request.response.is_closed() {
        return;
    }
    let stream_id = rand::random::<u64>();
    let (writer, reader) = session
        .opener
        .open_migrating_with_reader(stream_id, request.lane);
    session.opened_streams += 1;
    counter!("stream.rtp_mux.rtp_connects").increment(1);
    counter!("stream.rtp_mux.mux_connects").increment(1);
    let _ = request.response.send(Ok(OpenedStream {
        writer,
        reader,
        addr: session.addr,
    }));
}

fn dual_supervisor_result(result: Option<Result<MuxError, tokio::task::JoinError>>) -> MuxError {
    match result {
        Some(Ok(error)) => error,
        Some(Err(source)) => MuxError::TaskJoin {
            task: "dual_lane",
            source,
        },
        None => MuxError::TaskStopped { task: "dual_lane" },
    }
}

async fn connect_dual_lane(
    addr: SocketAddr,
    bind: BindSelector,
    bulk_addr: BulkAddrSelector,
    fec: bool,
) -> io::Result<ConnectedDualLaneBirth> {
    let started = Instant::now();
    let mut failures = Vec::new();

    for attempt in 1..=MAX_DUAL_CONNECT_ATTEMPTS {
        let attempt_started = Instant::now();
        match connect_dual_lane_once(addr, Arc::clone(&bind), Arc::clone(&bulk_addr), fec).await {
            Ok(birth) => {
                if attempt > 1 {
                    info!(
                        ?addr,
                        attempt,
                        failures = %failures.join("; "),
                        elapsed_ms = started.elapsed().as_millis(),
                        "RTP mux dual-lane birth recovered after retry",
                    );
                }
                return Ok(birth);
            }
            Err(error) => {
                let will_retry = attempt < MAX_DUAL_CONNECT_ATTEMPTS;
                failures.push(format!(
                    "attempt={attempt},elapsed_ms={},error={error}",
                    attempt_started.elapsed().as_millis(),
                ));
                debug!(
                    ?error,
                    ?addr,
                    attempt,
                    max_attempts = MAX_DUAL_CONNECT_ATTEMPTS,
                    will_retry,
                    elapsed_ms = attempt_started.elapsed().as_millis(),
                    "RTP mux dual-lane birth failed",
                );
                if will_retry {
                    tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                }
            }
        }
    }

    Err(io::Error::other(format!(
        "RTP mux dual-lane birth failed after {} attempts in {} ms: {}",
        failures.len(),
        started.elapsed().as_millis(),
        failures.join("; "),
    )))
}

async fn connect_dual_lane_once(
    addr: SocketAddr,
    bind: BindSelector,
    bulk_addr: BulkAddrSelector,
    fec: bool,
) -> io::Result<ConnectedDualLaneBirth> {
    let bind_addr = bind(addr);
    let bulk_addr = bulk_addr(addr)?;

    let interactive = rtp::udp::FrameDeliveryIo::connect(
        bind_addr,
        addr,
        rtp::udp::FrameDeliveryConnectConfig {
            log_config: None,
            handshake: false,
            fec,
            mss: rtp::udp::MssConfig::Default,
            fec_tuning: rtp::transmission::fec_tuning::FecTuning::default(),
        },
    )
    .await?;
    let interactive_local = interactive.local_addr;

    let bulk = rtp::udp::FrameDeliveryIo::connect(
        SocketAddr::new(bind_addr.ip(), 0),
        bulk_addr,
        rtp::udp::FrameDeliveryConnectConfig {
            log_config: None,
            handshake: false,
            fec,
            mss: rtp::udp::MssConfig::Default,
            fec_tuning: rtp::transmission::fec_tuning::FecTuning::default(),
        },
    )
    .await?;

    let nonce = PairingNonce::generate();

    let interactive_reader = interactive.read;
    let mut interactive_writer = interactive.write;
    let bulk_reader = bulk.read;
    let mut bulk_writer = bulk.write;

    if let Err(error) =
        mux::write_lane_hello(&mut interactive_writer, LaneClass::Interactive, nonce).await
    {
        return Err(io::Error::other(format!(
            "interactive lane hello: {error:?}"
        )));
    }
    if let Err(error) = mux::write_lane_hello(&mut bulk_writer, LaneClass::Bulk, nonce).await {
        let _ = interactive_writer.send_kill_and_abort().await;
        return Err(io::Error::other(format!("bulk lane hello: {error:?}")));
    }

    let mut interactive_tasks = JoinSet::new();
    let (interactive_opener, interactive_accepter, interactive_ready) =
        spawn_mux_no_reconnection_with_first_receive_deadline_and_ready(
            interactive_reader,
            interactive_writer,
            interactive_client_mux_config(),
            BIRTH_LIVENESS_DEADLINE,
            &mut interactive_tasks,
        );

    let mut bulk_tasks = JoinSet::new();
    let (bulk_opener, bulk_accepter, bulk_ready) =
        spawn_mux_no_reconnection_with_first_receive_deadline_and_ready(
            bulk_reader,
            bulk_writer,
            bulk_client_mux_config(),
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
        result = supervisor.join_next() => {
            let error = dual_supervisor_result(result);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("dual-lane birth liveness failed: {error:?}"),
            ));
        }
        ready = async { tokio::try_join!(interactive_ready, bulk_ready) } => {
            if ready.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "dual-lane birth readiness channel closed",
                ));
            }
        }
        () = &mut birth_deadline => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "dual-lane birth liveness deadline exceeded",
            ));
        }
    }

    drop(accepter);

    Ok(ConnectedDualLaneBirth {
        session: ConnectedDualLane {
            opener,
            addr: SocketAddrPair {
                local_addr: interactive_local,
                peer_addr: addr,
            },
            nonce,
            connected_at: Instant::now(),
            opened_streams: 0,
        },
        supervisor,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn spawn_test_connector(
        dialer: DualLaneDialer,
    ) -> (
        tokio::sync::mpsc::Sender<ConnectorCommand>,
        tokio::task::JoinHandle<()>,
    ) {
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        let coordinator = tokio::spawn(run_connector(command_rx, dialer));
        (commands, coordinator)
    }

    async fn enqueue(
        commands: &tokio::sync::mpsc::Sender<ConnectorCommand>,
        addr: SocketAddr,
    ) -> oneshot::Receiver<io::Result<OpenedStream>> {
        let (response, result) = oneshot::channel();
        commands
            .send(ConnectorCommand::Connect {
                addr,
                lane: LaneClass::Interactive,
                response,
            })
            .await
            .unwrap();
        result
    }

    async fn reset(commands: &tokio::sync::mpsc::Sender<ConnectorCommand>) {
        let (completed, done) = oneshot::channel();
        commands
            .send(ConnectorCommand::Reset { completed })
            .await
            .unwrap();
        done.await.unwrap();
    }

    async fn stop(
        commands: tokio::sync::mpsc::Sender<ConnectorCommand>,
        coordinator: tokio::task::JoinHandle<()>,
    ) {
        drop(commands);
        tokio::time::timeout(Duration::from_secs(1), coordinator)
            .await
            .expect("connector coordinator did not stop")
            .unwrap();
    }

    #[tokio::test]
    async fn pending_dial_does_not_block_other_destinations() {
        let blocked_addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let fast_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();
        let blocked_started = Arc::new(tokio::sync::Notify::new());

        let dialer: DualLaneDialer = Arc::new({
            let blocked_started = Arc::clone(&blocked_started);
            move |addr| {
                if addr == blocked_addr {
                    blocked_started.notify_one();
                    Box::pin(std::future::pending())
                } else {
                    Box::pin(async {
                        Err(io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            "synthetic dial failure",
                        ))
                    })
                }
            }
        });

        let (commands, coordinator) = spawn_test_connector(dialer);

        let blocked = enqueue(&commands, blocked_addr).await;
        blocked_started.notified().await;

        let error = tokio::time::timeout(Duration::from_secs(1), enqueue(&commands, fast_addr))
            .await
            .expect("second destination was blocked by the first dial")
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);

        drop(blocked);
        stop(commands, coordinator).await;
    }

    #[tokio::test]
    async fn connector_enforces_concurrent_dial_capacity_at_boundary() {
        let dial_count = Arc::new(AtomicUsize::new(0));
        let dialer: DualLaneDialer = Arc::new({
            let dial_count = Arc::clone(&dial_count);
            move |_addr| {
                dial_count.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::pending())
            }
        });

        let (commands, coordinator) = spawn_test_connector(dialer);

        let mut responses = Vec::new();
        for port in 10_000..10_000 + MAX_CONCURRENT_DUAL_DIALS as u16 {
            responses.push(enqueue(&commands, SocketAddr::from(([192, 0, 2, 1], port))).await);
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while dial_count.load(Ordering::SeqCst) != MAX_CONCURRENT_DUAL_DIALS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coordinator did not start all admitted dials");

        let rejected = enqueue(&commands, SocketAddr::from(([192, 0, 2, 1], 20_000))).await;
        assert_eq!(
            rejected.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        reset(&commands).await;
        for response in responses {
            assert_eq!(
                response.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted,
            );
        }

        stop(commands, coordinator).await;
    }

    #[tokio::test]
    async fn connector_enforces_waiter_capacity_and_reset_fails_every_waiter() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let dialer: DualLaneDialer = Arc::new(|_addr| Box::pin(std::future::pending()));

        let (commands, coordinator) = spawn_test_connector(dialer);

        let mut responses = Vec::new();
        for _ in 0..MAX_DIAL_WAITERS_PER_ADDR {
            responses.push(enqueue(&commands, addr).await);
        }

        let rejected = enqueue(&commands, addr).await;
        assert_eq!(
            rejected.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
        );

        reset(&commands).await;
        for response in responses {
            assert_eq!(
                response.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted,
            );
        }

        stop(commands, coordinator).await;
    }

    #[tokio::test]
    async fn closed_waiters_do_not_consume_per_destination_capacity() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let barrier_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();

        let dialer: DualLaneDialer = Arc::new(move |dial_addr| {
            if dial_addr == addr {
                Box::pin(std::future::pending())
            } else {
                Box::pin(async {
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "synthetic barrier failure",
                    ))
                })
            }
        });

        let (commands, coordinator) = spawn_test_connector(dialer);

        let first = enqueue(&commands, addr).await;
        for _ in 0..MAX_DIAL_WAITERS_PER_ADDR * 2 {
            drop(enqueue(&commands, addr).await);
        }

        let live = enqueue(&commands, addr).await;
        let barrier = enqueue(&commands, barrier_addr).await;
        assert_eq!(
            barrier.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionRefused,
        );

        reset(&commands).await;
        assert_eq!(
            first.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted,
        );
        assert_eq!(
            live.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted,
        );

        stop(commands, coordinator).await;
    }

    #[tokio::test]
    async fn connector_can_redial_immediately_after_reset() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());

        let dialer: DualLaneDialer = Arc::new({
            let attempts = Arc::clone(&attempts);
            let first_started = Arc::clone(&first_started);
            move |_addr| {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_started.notify_one();
                    Box::pin(std::future::pending())
                } else {
                    Box::pin(async {
                        Err(io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            "synthetic redial failure",
                        ))
                    })
                }
            }
        });

        let (commands, coordinator) = spawn_test_connector(dialer);

        let first = enqueue(&commands, addr).await;
        first_started.notified().await;
        reset(&commands).await;
        assert_eq!(
            first.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted,
        );

        let error = tokio::time::timeout(Duration::from_secs(1), enqueue(&commands, addr))
            .await
            .expect("redial was not admitted after reset")
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        stop(commands, coordinator).await;
    }
}
