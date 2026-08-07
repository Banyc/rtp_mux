mod cross_session;
mod dial;

use std::{
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::stream::{FuturesUnordered, StreamExt as _};
use mux::{GroupToken, LaneClass, MuxError, StreamReader};
use tokio::{sync::oneshot, task::JoinSet};
use tracing::{debug, info, trace, warn};

use crate::{
    byte_count::SessionStats,
    client_stream::ClientStream,
    explorer::{
        ExplorerConfig, ExplorerReport, MigrationRule, MigrationVerdict, PathExplorer, ProbeIo,
        SocketCandidate,
    },
    shared::{MAX_CONCURRENT_DUAL_DIALS, MAX_DIAL_WAITERS_PER_ADDR},
    stream::SocketAddrPair,
};

use cross_session::{
    Session, SharedDraining, SharedSessions, live_session, prune_dead_addresses, rebind_streams,
};
pub(crate) use cross_session::{SessionGuard, StreamRebind};
use dial::{
    ConnectedDualLaneBirth, DualLaneDial, DualLaneDialer, connect_dual_lane, dual_supervisor_result,
};

const SESSION_LINGER: Duration = Duration::from_secs(3);

pub type BindSelector = Arc<dyn Fn(SocketAddr) -> SocketAddr + Send + Sync + 'static>;
pub type BulkAddrSelector =
    Arc<dyn Fn(SocketAddr) -> io::Result<SocketAddr> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct RtpMuxConnectorConfig {
    pub bind: BindSelector,
    pub bulk_addr: BulkAddrSelector,
    pub fec: bool,
    pub explorer: ExplorerConfig,
}

impl RtpMuxConnectorConfig {
    pub fn standard(bind: BindSelector, fec: bool) -> Self {
        Self {
            bind,
            bulk_addr: Arc::new(crate::shared::bulk_lane_addr),
            fec,
            explorer: ExplorerConfig::default(),
        }
    }
}

#[derive(Debug)]
pub struct OpenedStream {
    pub writer: mux::MigratingStreamWriter,
    pub reader: oneshot::Receiver<StreamReader>,
    pub addr: SocketAddrPair,
    pub response: (u64, mux::ResponseRouterHandle),
    guard: SessionGuard,
}

impl OpenedStream {
    pub fn into_stream(self) -> ClientStream {
        let (logical_id, router) = self.response;
        ClientStream::new_duplex(
            self.writer,
            self.reader,
            self.addr,
            logical_id,
            router,
            self.guard,
        )
    }
}

struct AddrGroup {
    token: GroupToken,
    router: Option<mux::ResponseRouter>,
    sessions: Vec<std::sync::Weak<Session>>,
}

struct ExplorerContext {
    config: ExplorerConfig,
    bind: BindSelector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedialTrigger {
    Forced,
    BetterPath,
}

impl RedialTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Forced => "forced",
            Self::BetterPath => "better_path",
        }
    }
}

struct RedialInFlight {
    old: Arc<Session>,
    trigger: RedialTrigger,
    verdict: Option<MigrationVerdict>,
}

enum ConnectorCommand {
    Connect {
        addr: SocketAddr,
        lane: LaneClass,
        response: oneshot::Sender<io::Result<OpenedStream>>,
    },
    Redial {
        addr: SocketAddr,
        trigger: RedialTrigger,
    },
    ExplorerReport {
        addr: SocketAddr,
        response: oneshot::Sender<ExplorerReport>,
    },
    Reset {
        completed: oneshot::Sender<()>,
    },
}

pub struct RtpMuxConnector {
    commands: tokio::sync::mpsc::Sender<ConnectorCommand>,
    sessions: SharedSessions,
    draining: SharedDraining,
}

impl std::fmt::Debug for RtpMuxConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtpMuxConnector").finish_non_exhaustive()
    }
}

/// The driver future for an [`RtpMuxConnector`].
///
/// [`RtpMuxConnector::with_config`] / [`RtpMuxConnector::new`] return the
/// handle paired with a `RtpMuxConnectorDriver`. The driver runs the
/// connector's command loop (reaping per-session supervisors and the
/// response-router driver) and must be spawned into a caller-owned,
/// actively-reaped `JoinSet` so its termination is observed and its drop
/// aborts the connector task. The handle alone holds no task and is safe
/// to share by `Arc`; it is inert until the driver is spawned.
pub struct RtpMuxConnectorDriver {
    inner: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>,
}

impl std::future::Future for RtpMuxConnectorDriver {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

#[derive(Debug, Clone)]
pub struct SessionView {
    id: u64,
    inner: std::sync::Weak<Session>,
}

impl SessionView {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn is_alive(&self) -> bool {
        self.inner.strong_count() > 0
    }
    pub fn live_streams(&self) -> Option<u64> {
        self.inner
            .upgrade()
            .map(|session| session.live_streams.load(Ordering::Relaxed))
    }
    pub fn stats(&self) -> Option<SessionStats> {
        self.inner.upgrade().map(|session| session.stats())
    }
}

impl RtpMuxConnector {
    pub fn new(bind: BindSelector, fec: bool) -> (Self, RtpMuxConnectorDriver) {
        Self::with_config(RtpMuxConnectorConfig::standard(bind, fec))
    }

    pub fn with_config(config: RtpMuxConnectorConfig) -> (Self, RtpMuxConnectorDriver) {
        let RtpMuxConnectorConfig {
            bind,
            bulk_addr,
            fec,
            explorer,
        } = config;
        let explorer = explorer.enabled.then(|| ExplorerContext {
            config: explorer,
            bind: Arc::clone(&bind),
        });
        let dialer: DualLaneDialer = Arc::new(move |addr, group, socket| {
            let bind = Arc::clone(&bind);
            let bulk_addr = Arc::clone(&bulk_addr);
            Box::pin(
                async move { connect_dual_lane(addr, bind, bulk_addr, fec, group, socket).await },
            )
        });
        Self::with_dialer_and_explorer(dialer, explorer)
    }

    #[cfg(test)]
    fn with_dialer(dialer: DualLaneDialer) -> (Self, RtpMuxConnectorDriver) {
        Self::with_dialer_and_explorer(dialer, None)
    }

    fn with_dialer_and_explorer(
        dialer: DualLaneDialer,
        explorer: Option<ExplorerContext>,
    ) -> (Self, RtpMuxConnectorDriver) {
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        let sessions: SharedSessions = Arc::new(Mutex::new(HashMap::new()));
        let draining: SharedDraining = Arc::new(Mutex::new(HashMap::new()));
        let driver = RtpMuxConnectorDriver {
            inner: Box::pin(run_connector(
                command_rx,
                dialer,
                explorer,
                Arc::clone(&sessions),
                Arc::clone(&draining),
            )),
        };
        (
            Self {
                commands,
                sessions,
                draining,
            },
            driver,
        )
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<OpenedStream> {
        self.connect_with_lane(addr, LaneClass::Interactive).await
    }

    pub async fn connect_with_lane(
        &self,
        addr: SocketAddr,
        lane: LaneClass,
    ) -> io::Result<OpenedStream> {
        if let Some(session) = live_session(&self.sessions, addr) {
            return Ok(session.open_stream(lane));
        }
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

    pub fn force_redial(&self, addr: SocketAddr) {
        self.request_redial(addr, RedialTrigger::Forced);
    }

    fn request_redial(&self, addr: SocketAddr, trigger: RedialTrigger) {
        if let Some(prev) = self
            .draining
            .lock()
            .unwrap()
            .get(&addr)
            .and_then(std::sync::Weak::upgrade)
        {
            info!(up = ?prev.addr.peer_addr, trigger = trigger.as_str(), draining = %prev.stats(), "RTP mux session redial skipped; previous generation still draining");
            return;
        }
        let _ = self
            .commands
            .try_send(ConnectorCommand::Redial { addr, trigger });
    }

    pub async fn explorer_report(&self, addr: SocketAddr) -> io::Result<ExplorerReport> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(ConnectorCommand::ExplorerReport { addr, response })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTP mux connector stopped"))?;
        result.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "RTP mux connector dropped explorer report",
            )
        })
    }

    pub fn probe_session(&self, addr: SocketAddr) -> Option<SessionView> {
        let sessions = self.sessions.lock().unwrap();
        sessions.get(&addr).map(|session| SessionView {
            id: session.id,
            inner: Arc::downgrade(session),
        })
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

    pub fn reoptimize(&self, addr: SocketAddr) {
        self.request_redial(addr, RedialTrigger::BetterPath);
    }
}

fn spawn_session_watcher(
    supervisors: &mut JoinSet<(SocketAddr, u64, MuxError)>,
    addr: SocketAddr,
    session_id: u64,
    mut supervisor: JoinSet<MuxError>,
    mut kill_rx: tokio::sync::mpsc::Receiver<()>,
) {
    supervisors.spawn(async move {
        let error = tokio::select! {
            result = supervisor.join_next() => dual_supervisor_result(result),
            kill = kill_rx.recv() => {
                if kill.is_none() {
                    tokio::time::sleep(SESSION_LINGER).await;
                }
                MuxError::TaskStopped {
                    task: "dual_lane_redialed",
                }
            }
        };
        (addr, session_id, error)
    });
}

fn install_session(
    addr: SocketAddr,
    birth: ConnectedDualLaneBirth,
    groups: &mut HashMap<SocketAddr, AddrGroup>,
    supervisors: &mut JoinSet<(SocketAddr, u64, MuxError)>,
    router_driver: &mut mux::ResponseRouterDriver,
) -> Arc<Session> {
    let ConnectedDualLaneBirth {
        opener,
        accepter,
        local_addr,
        nonce,
        supervisor,
        probe_tap: _,
        traffic,
    } = birth;
    let group = groups
        .get_mut(&addr)
        .expect("dial started without an address group");
    let router = match &mut group.router {
        Some(router) => {
            router.add_accepter(accepter, router_driver);
            router.handle()
        }
        None => {
            let mut router = mux::ResponseRouter::new();
            let handle = router.handle();
            router.add_accepter(accepter, router_driver);
            group.router = Some(router);
            handle
        }
    };
    let (kill_tx, kill_rx) = tokio::sync::mpsc::channel(1);
    let session = Arc::new(Session {
        id: rand::random(),
        opener,
        addr: SocketAddrPair {
            local_addr,
            peer_addr: addr,
        },
        nonce,
        connected_at: Instant::now(),
        opened_streams: AtomicU64::new(0),
        live_streams: AtomicU64::new(0),
        traffic,
        router,
        kill_tx,
        streams: Mutex::new(Vec::new()),
        successor: Mutex::new(None),
    });
    spawn_session_watcher(supervisors, addr, session.id, supervisor, kill_rx);
    let group = groups
        .get_mut(&addr)
        .expect("dial started without an address group");
    group.sessions.retain(|weak| weak.strong_count() > 0);
    group.sessions.push(Arc::downgrade(&session));
    session
}

async fn refill_candidates(
    explorer: &mut PathExplorer<SocketCandidate>,
    ctx: &ExplorerContext,
    addr: SocketAddr,
) {
    while explorer.deficit() > 0 {
        let bind_ip = (ctx.bind)(addr).ip();
        match SocketCandidate::bind_new(bind_ip, addr).await {
            Ok((candidate, local_addr)) => {
                explorer.add_candidate(candidate, local_addr, Instant::now());
            }
            Err(error) => {
                debug!(?error, up = ?addr, "path explorer candidate mint failed");
                explorer.defer_refill(Instant::now() + Duration::from_secs(5));
                break;
            }
        }
    }
}

async fn run_connector(
    mut commands: tokio::sync::mpsc::Receiver<ConnectorCommand>,
    dialer: DualLaneDialer,
    explorer_ctx: Option<ExplorerContext>,
    sessions: SharedSessions,
    draining: SharedDraining,
) {
    let mut supervisors: JoinSet<(SocketAddr, u64, MuxError)> = JoinSet::new();
    let mut router_driver: mux::ResponseRouterDriver = mux::ResponseRouterDriver::new();
    let mut pending_dials: FuturesUnordered<DualLaneDial> = FuturesUnordered::new();
    let mut in_flight_dials: HashSet<SocketAddr> = HashSet::new();
    let mut dial_waiters: HashMap<SocketAddr, Vec<StreamRequest>> = HashMap::new();
    let mut groups: HashMap<SocketAddr, AddrGroup> = HashMap::new();
    let mut redials: HashMap<SocketAddr, RedialInFlight> = HashMap::new();
    let mut explorers: HashMap<SocketAddr, PathExplorer<SocketCandidate>> = HashMap::new();
    let start_dial =
        |addr: SocketAddr,
         groups: &mut HashMap<SocketAddr, AddrGroup>,
         pending_dials: &mut FuturesUnordered<DualLaneDial>,
         in_flight_dials: &mut HashSet<SocketAddr>,
         explorers: &mut HashMap<SocketAddr, PathExplorer<SocketCandidate>>| {
            let token = groups
                .entry(addr)
                .or_insert_with(|| AddrGroup {
                    token: GroupToken::generate(),
                    router: None,
                    sessions: Vec::new(),
                })
                .token;
            if let Some(ctx) = &explorer_ctx {
                explorers
                    .entry(addr)
                    .or_insert_with(|| PathExplorer::new(ctx.config.clone(), Instant::now()));
            }
            let socket = explorers.get_mut(&addr).and_then(|explorer| explorer.pop_best().map(|(candidate, local_addr, score)| {
            info!(up = ?addr, up_local = ?local_addr, probe_rtt = ?score.rtt, probe_loss = score.loss, "RTP mux dial inherits explorer candidate tuple");
            candidate.into_socket()
        }));
            in_flight_dials.insert(addr);
            let dialer = Arc::clone(&dialer);
            pending_dials.push(Box::pin(async move {
                let result = dialer(addr, token, socket).await;
                (addr, result)
            }));
        };
    loop {
        let now = Instant::now();
        let explorer_wakeup = if explorers.values().any(|e| e.wants_refill(now)) {
            Some(now)
        } else {
            explorers.values().filter_map(|e| e.next_wakeup(now)).min()
        };
        tokio::select! {
            () = async { match explorer_wakeup { Some(at) => tokio::time::sleep_until(at.into()).await, None => std::future::pending().await } } => {
                if explorer_wakeup.is_some() {
                    let now = Instant::now();
                    for explorer in explorers.values_mut() { explorer.tick(now); }
                    if let Some(ctx) = &explorer_ctx {
                        for (addr, explorer) in explorers.iter_mut() {
                            if explorer.wants_refill(now) { refill_candidates(explorer, ctx, *addr).await; }
                        }
                    }
                }
            }
            Some(res) = supervisors.join_next() => {
                let (addr, session_id, error) = res.unwrap();
                let session = { let mut map = sessions.lock().unwrap(); match map.get(&addr) { Some(session) if session.id == session_id => map.remove(&addr), _ => None } };
                match session {
                    Some(session) => {
                        warn!(event = "rtp_mux_session_terminated", ?error, nonce = ?session.nonce, up = ?session.addr.peer_addr, up_local = ?session.addr.local_addr, mux = %session.stats(), "RTP mux dual-lane session terminated");
                        if let Some(explorer) = explorers.get_mut(&addr) { explorer.set_active(None, Instant::now()); }
                    }
                    None => debug!(?error, up = ?addr, "RTP mux dual-lane session ended after redial or reset"),
                }
                prune_dead_addresses(&mut groups, &mut explorers, &in_flight_dials);
            }
            Some(res) = router_driver.join_next() => {
                trace!("ResponseRouter accepter task stopped");
                res.unwrap();
            }
            Some((addr, result)) = pending_dials.next() => {
                in_flight_dials.remove(&addr);
                let redialed_old = redials.remove(&addr);
                match result {
                    Ok(mut birth) => {
                        let probe_tap = birth.probe_tap.take();
                        if let Some(explorer) = explorers.get_mut(&addr) {
                            explorer.set_active(probe_tap.map(|tap| (Box::new(tap) as Box<dyn ProbeIo>, birth.local_addr)), Instant::now());
                        }
                        let session = install_session(addr, birth, &mut groups, &mut supervisors, &mut router_driver);
                        if let Some(RedialInFlight { old, trigger, verdict }) = &redialed_old {
                            let moved = rebind_streams(old, &session);
                            info!(up = ?old.addr.peer_addr, up_local = ?old.addr.local_addr, new_local = ?session.addr.local_addr, trigger = trigger.as_str(), rule = verdict.and_then(|v| v.rule()).map(MigrationRule::as_str), self_rtt = ?verdict.and_then(|v| v.active()).map(|s| s.rtt), self_loss = verdict.and_then(|v| v.active()).map(|s| s.loss), best_rtt = ?verdict.and_then(|v| v.best()).map(|s| s.rtt), best_loss = verdict.and_then(|v| v.best()).map(|s| s.loss), old_mux = %old.stats(), migrated_streams = moved, "RTP mux session redialed");
                            draining.lock().unwrap().insert(addr, Arc::downgrade(old));
                        }
                        if let Some(waiters) = dial_waiters.remove(&addr) {
                            for waiter in waiters {
                                if waiter.response.is_closed() { continue; }
                                let _ = waiter.response.send(Ok(session.open_stream(waiter.lane)));
                            }
                        }
                        sessions.lock().unwrap().insert(addr, session);
                    }
                    Err(error) => {
                        if redialed_old.is_some() { warn!(?error, up = ?addr, "RTP mux redial dial failed; streams stay on the old session"); }
                        let kind = error.kind();
                        let message = error.to_string();
                        if let Some(waiters) = dial_waiters.remove(&addr) {
                            for waiter in waiters {
                                let _ = waiter.response.send(Err(io::Error::new(kind, message.clone())));
                            }
                        }
                        prune_dead_addresses(&mut groups, &mut explorers, &in_flight_dials);
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    ConnectorCommand::Reset { completed } => {
                        let sessions_to_kill: Vec<_> = sessions
                            .lock()
                            .unwrap()
                            .drain()
                            .map(|(_, session)| session)
                            .collect();
                        for session in sessions_to_kill {
                            session.kill();
                        }
                        let draining_to_kill: Vec<_> = draining
                            .lock()
                            .unwrap()
                            .drain()
                            .filter_map(|(_, weak)| weak.upgrade())
                            .collect();
                        for session in draining_to_kill {
                            session.kill();
                        }
                        for (_, waiters) in dial_waiters.drain() { for waiter in waiters { let _ = waiter.response.send(Err(io::Error::new(io::ErrorKind::ConnectionAborted, "connector reset"))); } }
                        in_flight_dials.clear();
                        pending_dials = FuturesUnordered::new();
                        redials.clear();
                        groups.clear();
                        explorers.clear();
                        router_driver = mux::ResponseRouterDriver::new();
                        let _ = completed.send(());
                    }
                    ConnectorCommand::Redial { addr, trigger } => {
                        let drainer_alive = draining.lock().unwrap().get(&addr).and_then(std::sync::Weak::upgrade).is_some();
                        if drainer_alive || in_flight_dials.contains(&addr) { continue; }
                        if pending_dials.len() >= MAX_CONCURRENT_DUAL_DIALS {
                            debug!(up = ?addr, trigger = trigger.as_str(), pending = pending_dials.len(), "RTP mux redial skipped; dial capacity is saturated");
                            continue;
                        }
                        let verdict = match trigger {
                            RedialTrigger::Forced => None,
                            RedialTrigger::BetterPath => {
                                let verdict = explorers.get(&addr).map(PathExplorer::reoptimize_verdict).unwrap_or(MigrationVerdict::ActiveUnmeasured);
                                if !verdict.wants_migration() {
                                    debug!(up = ?addr, outcome = verdict.as_str(), self_rtt = ?verdict.active().map(|s| s.rtt), self_loss = verdict.active().map(|s| s.loss), best_rtt = ?verdict.best().map(|s| s.rtt), best_loss = verdict.best().map(|s| s.loss), mux = live_session(&sessions, addr).map(|s| s.stats().to_string()), "RTP mux reoptimize declined");
                                    continue;
                                }
                                Some(verdict)
                            }
                        };
                        let Some(old) = sessions.lock().unwrap().get(&addr).cloned() else { continue; };
                        redials.insert(addr, RedialInFlight { old, trigger, verdict });
                        start_dial(addr, &mut groups, &mut pending_dials, &mut in_flight_dials, &mut explorers);
                    }
                    ConnectorCommand::ExplorerReport { addr, response } => {
                        let report = explorers.get(&addr).map(PathExplorer::report).unwrap_or_default();
                        let _ = response.send(report);
                    }
                    ConnectorCommand::Connect { addr, lane, response } => {
                        if let Some(session) = live_session(&sessions, addr) {
                            if !response.is_closed() { let _ = response.send(Ok(session.open_stream(lane))); }
                            continue;
                        }
                        let request = StreamRequest { lane, response };
                        if live_dial_waiters(&mut dial_waiters, addr) >= MAX_DIAL_WAITERS_PER_ADDR {
                            let _ = request.response.send(Err(io::Error::new(io::ErrorKind::WouldBlock, format!("too many dial waiters for {addr} (max {MAX_DIAL_WAITERS_PER_ADDR})"))));
                            continue;
                        }
                        let is_in_flight = in_flight_dials.contains(&addr);
                        if !is_in_flight && pending_dials.len() >= MAX_CONCURRENT_DUAL_DIALS {
                            let _ = request.response.send(Err(io::Error::new(io::ErrorKind::WouldBlock, "too many concurrent dual dials")));
                            continue;
                        }
                        dial_waiters.entry(addr).or_default().push(request);
                        if !is_in_flight {
                            start_dial(addr, &mut groups, &mut pending_dials, &mut in_flight_dials, &mut explorers);
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

fn live_dial_waiters(
    dial_waiters: &mut HashMap<SocketAddr, Vec<StreamRequest>>,
    addr: SocketAddr,
) -> usize {
    match dial_waiters.get_mut(&addr) {
        Some(waiters) => {
            waiters.retain(|waiter| !waiter.response.is_closed());
            waiters.len()
        }
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::dial::retry_dual_connect;
    use super::*;
    use crate::byte_count::SessionByteCounters;
    use crate::shared::{client_mux_config, server_mux_config};
    use mux::{PairingNonce, spawn_mux_no_reconnection};

    fn spawn_test_connector(
        dialer: DualLaneDialer,
        tasks: &mut JoinSet<()>,
    ) -> tokio::sync::mpsc::Sender<ConnectorCommand> {
        let (commands, _sessions) = spawn_test_connector_with_sessions(dialer, tasks);
        commands
    }

    fn spawn_test_connector_with_sessions(
        dialer: DualLaneDialer,
        tasks: &mut JoinSet<()>,
    ) -> (tokio::sync::mpsc::Sender<ConnectorCommand>, SharedSessions) {
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        let sessions: SharedSessions = Arc::new(Mutex::new(HashMap::new()));
        let draining: SharedDraining = Arc::new(Mutex::new(HashMap::new()));
        tasks.spawn(run_connector(
            command_rx,
            dialer,
            None,
            Arc::clone(&sessions),
            draining,
        ));
        (commands, sessions)
    }

    fn spawn_test_connector_with_explorer(
        dialer: DualLaneDialer,
        tasks: &mut JoinSet<()>,
    ) -> (tokio::sync::mpsc::Sender<ConnectorCommand>, SharedSessions) {
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        let sessions: SharedSessions = Arc::new(Mutex::new(HashMap::new()));
        let draining: SharedDraining = Arc::new(Mutex::new(HashMap::new()));
        let explorer = ExplorerContext {
            config: ExplorerConfig {
                enabled: true,
                candidates: 1,
                probe_mean_interval: Duration::from_secs(3600),
                rotation_period: Duration::from_secs(3600),
            },
            bind: Arc::new(|_| SocketAddr::from(([127, 0, 0, 1], 0))),
        };
        tasks.spawn(run_connector(
            command_rx,
            dialer,
            Some(explorer),
            Arc::clone(&sessions),
            draining,
        ));
        (commands, sessions)
    }

    async fn explorer_candidates(
        commands: &tokio::sync::mpsc::Sender<ConnectorCommand>,
        addr: SocketAddr,
    ) -> usize {
        let (response, result) = oneshot::channel();
        commands
            .send(ConnectorCommand::ExplorerReport { addr, response })
            .await
            .unwrap();
        result.await.unwrap().candidates.len()
    }

    async fn wait_for(mut cond: impl FnMut() -> bool, what: &str) {
        for _ in 0..1000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("timed out waiting for {what}");
    }

    async fn wait_for_candidates(
        commands: &tokio::sync::mpsc::Sender<ConnectorCommand>,
        addr: SocketAddr,
        want: usize,
        what: &str,
    ) {
        for _ in 0..1000 {
            if explorer_candidates(commands, addr).await == want {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("timed out waiting for {what}");
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

    async fn stop(commands: tokio::sync::mpsc::Sender<ConnectorCommand>, tasks: &mut JoinSet<()>) {
        drop(commands);
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("connector coordinator did not stop");
    }

    pub(super) fn fake_connected_birth(
        addr: SocketAddr,
        terminate: Option<oneshot::Receiver<()>>,
    ) -> ConnectedDualLaneBirth {
        let (interactive_local, interactive_peer) = tokio::io::duplex(64 * 1024);
        let (bulk_local, bulk_peer) = tokio::io::duplex(64 * 1024);
        let (interactive_local_read, interactive_local_write) = tokio::io::split(interactive_local);
        let (interactive_peer_read, interactive_peer_write) = tokio::io::split(interactive_peer);
        let (bulk_local_read, bulk_local_write) = tokio::io::split(bulk_local);
        let (bulk_peer_read, bulk_peer_write) = tokio::io::split(bulk_peer);
        let mut interactive_local_tasks = JoinSet::new();
        let (interactive_opener, interactive_accepter) = spawn_mux_no_reconnection(
            interactive_local_read,
            interactive_local_write,
            client_mux_config(),
            &mut interactive_local_tasks,
        );
        let mut bulk_local_tasks = JoinSet::new();
        let (bulk_opener, bulk_accepter) = spawn_mux_no_reconnection(
            bulk_local_read,
            bulk_local_write,
            client_mux_config(),
            &mut bulk_local_tasks,
        );
        let mut interactive_peer_tasks = JoinSet::new();
        let (interactive_peer_opener, interactive_peer_accepter) = spawn_mux_no_reconnection(
            interactive_peer_read,
            interactive_peer_write,
            server_mux_config(),
            &mut interactive_peer_tasks,
        );
        let mut bulk_peer_tasks = JoinSet::new();
        let (bulk_peer_opener, bulk_peer_accepter) = spawn_mux_no_reconnection(
            bulk_peer_read,
            bulk_peer_write,
            server_mux_config(),
            &mut bulk_peer_tasks,
        );
        let mut supervisor = JoinSet::new();
        let (opener, accepter) = mux::spawn_dual_mux_paired_supervised(
            interactive_opener,
            interactive_accepter,
            interactive_local_tasks,
            bulk_opener,
            bulk_accepter,
            bulk_local_tasks,
            &mut supervisor,
        );
        supervisor.spawn(async move {
            let _keep_peer_alive = (
                interactive_peer_opener,
                interactive_peer_accepter,
                interactive_peer_tasks,
                bulk_peer_opener,
                bulk_peer_accepter,
                bulk_peer_tasks,
            );
            std::future::pending::<MuxError>().await
        });
        if let Some(terminate) = terminate {
            supervisor.spawn(async move {
                let _ = terminate.await;
                MuxError::TaskStopped {
                    task: "synthetic_dual_lane",
                }
            });
        }
        let _ = addr;
        ConnectedDualLaneBirth {
            opener,
            accepter,
            local_addr: "192.0.2.100:40000".parse().unwrap(),
            nonce: PairingNonce::generate(),
            supervisor,
            probe_tap: None,
            traffic: Arc::new(SessionByteCounters::default()),
        }
    }

    pub(super) async fn fake_dead_birth() -> ConnectedDualLaneBirth {
        let lane = || {
            let (local, peer) = tokio::io::duplex(64 * 1024);
            drop(peer);
            let (read, write) = tokio::io::split(local);
            let mut tasks = JoinSet::new();
            let (opener, accepter) =
                spawn_mux_no_reconnection(read, write, client_mux_config(), &mut tasks);
            (opener, accepter, tasks)
        };
        let (int_opener, int_accepter, int_tasks) = lane();
        let (bulk_opener, bulk_accepter, bulk_tasks) = lane();
        let mut supervisor = JoinSet::new();
        let (opener, accepter) = mux::spawn_dual_mux_paired_supervised(
            int_opener,
            int_accepter,
            int_tasks,
            bulk_opener,
            bulk_accepter,
            bulk_tasks,
            &mut supervisor,
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while opener.is_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a dual-lane session with no peer left must go down");
        ConnectedDualLaneBirth {
            opener,
            accepter,
            local_addr: "192.0.2.100:40001".parse().unwrap(),
            nonce: PairingNonce::generate(),
            supervisor,
            probe_tap: None,
            traffic: Arc::new(SessionByteCounters::default()),
        }
    }

    pub(super) fn one_address_group(addr: SocketAddr) -> HashMap<SocketAddr, AddrGroup> {
        HashMap::from([(
            addr,
            AddrGroup {
                token: GroupToken::generate(),
                router: None,
                sessions: Vec::new(),
            },
        )])
    }

    #[tokio::test]
    async fn a_connect_does_not_hand_out_a_stream_on_a_dead_session() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let dials = Arc::new(AtomicUsize::new(0));
        let dialer: DualLaneDialer = Arc::new({
            let dials = Arc::clone(&dials);
            move |addr, _group, _socket| {
                dials.fetch_add(1, Ordering::Relaxed);
                Box::pin(async move { Ok(fake_connected_birth(addr, None)) })
            }
        });
        let mut connector_tasks = JoinSet::new();
        let (commands, sessions) = spawn_test_connector_with_sessions(dialer, &mut connector_tasks);
        let mut groups = one_address_group(addr);
        let mut supervisors = JoinSet::new();
        let mut router_driver = mux::ResponseRouterDriver::new();
        let dead = install_session(
            addr,
            fake_dead_birth().await,
            &mut groups,
            &mut supervisors,
            &mut router_driver,
        );
        let dead_id = dead.id;
        sessions.lock().unwrap().insert(addr, dead);
        let stream = enqueue(&commands, addr).await.await.unwrap().unwrap();
        assert_eq!(
            dials.load(Ordering::Relaxed),
            1,
            "a connect was served off a session that is already down instead of dialing",
        );
        assert_ne!(
            sessions.lock().unwrap().get(&addr).unwrap().id,
            dead_id,
            "the dead session is still the one serving this address",
        );
        drop(stream);
        stop(commands, &mut connector_tasks).await;
    }

    fn counting_fake_dialer(attempts: Arc<AtomicUsize>) -> DualLaneDialer {
        Arc::new(move |addr, _group, _socket| {
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(fake_connected_birth(addr, None)) })
        })
    }

    #[tokio::test]
    async fn a_dead_address_releases_its_group() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let (terminate_tx, terminate_rx) = oneshot::channel();
        let terminate_rx = Arc::new(Mutex::new(Some(terminate_rx)));
        let tokens: Arc<Mutex<Vec<GroupToken>>> = Arc::new(Mutex::new(Vec::new()));
        let dialer: DualLaneDialer = Arc::new({
            let tokens = Arc::clone(&tokens);
            let terminate_rx = Arc::clone(&terminate_rx);
            move |addr, group, _socket| {
                tokens.lock().unwrap().push(group);
                let terminate = terminate_rx.lock().unwrap().take();
                Box::pin(async move { Ok(fake_connected_birth(addr, terminate)) })
            }
        });
        let mut connector_tasks = JoinSet::new();
        let (commands, sessions) = spawn_test_connector_with_sessions(dialer, &mut connector_tasks);
        drop(enqueue(&commands, addr).await.await.unwrap().unwrap());
        terminate_tx.send(()).unwrap();
        wait_for(
            || sessions.lock().unwrap().is_empty(),
            "the dead session to be reaped",
        )
        .await;
        drop(enqueue(&commands, addr).await.await.unwrap().unwrap());
        let tokens = tokens.lock().unwrap().clone();
        assert_eq!(tokens.len(), 2, "the second dial did not happen");
        assert_ne!(
            tokens[0], tokens[1],
            "redial reused the group of the dead session, so its router leaked",
        );
        stop(commands, &mut connector_tasks).await;
    }

    #[test]
    fn counting_the_waiters_of_an_unknown_address_creates_no_entry() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let mut dial_waiters: HashMap<SocketAddr, Vec<StreamRequest>> = HashMap::new();
        assert_eq!(live_dial_waiters(&mut dial_waiters, addr), 0);
        assert!(
            dial_waiters.is_empty(),
            "counting left a waiter list behind; a rejected request starts no dial, and only a completing dial removes the entry, so every rejected address leaks one",
        );
    }

    #[test]
    fn counting_the_waiters_of_an_address_drops_the_closed_ones() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let mut dial_waiters: HashMap<SocketAddr, Vec<StreamRequest>> = HashMap::new();
        let (live, _live_rx) = oneshot::channel();
        let (closed, closed_rx) = oneshot::channel();
        drop(closed_rx);
        dial_waiters.insert(
            addr,
            vec![
                StreamRequest {
                    lane: LaneClass::Interactive,
                    response: closed,
                },
                StreamRequest {
                    lane: LaneClass::Interactive,
                    response: live,
                },
            ],
        );
        assert_eq!(live_dial_waiters(&mut dial_waiters, addr), 1);
    }

    #[tokio::test]
    async fn a_dead_address_releases_its_explorer() {
        let addr: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let (terminate_tx, terminate_rx) = oneshot::channel();
        let terminate_rx = Arc::new(Mutex::new(Some(terminate_rx)));
        let dialer: DualLaneDialer = Arc::new(move |addr, _group, _socket| {
            let terminate = terminate_rx.lock().unwrap().take();
            Box::pin(async move { Ok(fake_connected_birth(addr, terminate)) })
        });
        let mut connector_tasks = JoinSet::new();
        let (commands, sessions) = spawn_test_connector_with_explorer(dialer, &mut connector_tasks);
        drop(enqueue(&commands, addr).await.await.unwrap().unwrap());
        wait_for_candidates(
            &commands,
            addr,
            1,
            "the explorer to mint its candidate tuple",
        )
        .await;
        terminate_tx.send(()).unwrap();
        wait_for(
            || sessions.lock().unwrap().is_empty(),
            "the dead session to be reaped",
        )
        .await;
        wait_for_candidates(
            &commands,
            addr,
            0,
            "the dead address to release its explorer, which otherwise keeps minting sockets and probing an address with no session forever",
        )
        .await;
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn a_live_stream_keeps_its_group_alive() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let (terminate_tx, terminate_rx) = oneshot::channel();
        let terminate_rx = Arc::new(Mutex::new(Some(terminate_rx)));
        let tokens: Arc<Mutex<Vec<GroupToken>>> = Arc::new(Mutex::new(Vec::new()));
        let dialer: DualLaneDialer = Arc::new({
            let tokens = Arc::clone(&tokens);
            let terminate_rx = Arc::clone(&terminate_rx);
            move |addr, group, _socket| {
                tokens.lock().unwrap().push(group);
                let terminate = terminate_rx.lock().unwrap().take();
                Box::pin(async move { Ok(fake_connected_birth(addr, terminate)) })
            }
        });
        let mut connector_tasks = JoinSet::new();
        let (commands, sessions) = spawn_test_connector_with_sessions(dialer, &mut connector_tasks);
        let held = enqueue(&commands, addr).await.await.unwrap().unwrap();
        terminate_tx.send(()).unwrap();
        wait_for(
            || sessions.lock().unwrap().is_empty(),
            "the dead session to be reaped",
        )
        .await;
        drop(enqueue(&commands, addr).await.await.unwrap().unwrap());
        let recorded = tokens.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2, "the second dial did not happen");
        assert_eq!(
            recorded[0], recorded[1],
            "the group was dropped while a stream still routed responses through it",
        );
        drop(held);
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn pending_dial_does_not_block_other_destinations() {
        let blocked_addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let fast_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();
        let blocked_started = Arc::new(tokio::sync::Notify::new());
        let dialer: DualLaneDialer = Arc::new({
            let blocked_started = Arc::clone(&blocked_started);
            move |addr, _group, _socket| {
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
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
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
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn pending_dial_does_not_block_cached_session_requests() {
        let cached_addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let blocked_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();
        let blocked_started = Arc::new(tokio::sync::Notify::new());
        let dialer: DualLaneDialer = Arc::new({
            let blocked_started = Arc::clone(&blocked_started);
            move |addr, _group, _socket| {
                if addr == cached_addr {
                    Box::pin(async move { Ok(fake_connected_birth(addr, None)) })
                } else {
                    blocked_started.notify_one();
                    Box::pin(std::future::pending())
                }
            }
        });
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
        drop(
            enqueue(&commands, cached_addr)
                .await
                .await
                .unwrap()
                .unwrap(),
        );
        let blocked = enqueue(&commands, blocked_addr).await;
        blocked_started.notified().await;
        let cached = tokio::time::timeout(Duration::from_secs(1), enqueue(&commands, cached_addr))
            .await
            .expect("pending dial blocked a cached session request")
            .await
            .unwrap()
            .unwrap();
        drop(cached);
        reset(&commands).await;
        assert_eq!(
            blocked.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn pending_dial_does_not_block_dead_session_reaping() {
        let session_addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let blocked_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();
        let (terminate_tx, terminate_rx) = oneshot::channel();
        let terminate_rx = Arc::new(Mutex::new(Some(terminate_rx)));
        let attempts = Arc::new(AtomicUsize::new(0));
        let blocked_started = Arc::new(tokio::sync::Notify::new());
        let dialer: DualLaneDialer = Arc::new({
            let terminate_rx = Arc::clone(&terminate_rx);
            let attempts = Arc::clone(&attempts);
            let blocked_started = Arc::clone(&blocked_started);
            move |addr, _group, _socket| {
                if addr == blocked_addr {
                    blocked_started.notify_one();
                    return Box::pin(std::future::pending());
                }
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    let terminate = terminate_rx.lock().unwrap().take().unwrap();
                    Box::pin(async move { Ok(fake_connected_birth(addr, Some(terminate))) })
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
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
        drop(
            enqueue(&commands, session_addr)
                .await
                .await
                .unwrap()
                .unwrap(),
        );
        let blocked = enqueue(&commands, blocked_addr).await;
        blocked_started.notified().await;
        terminate_tx.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match enqueue(&commands, session_addr).await.await.unwrap() {
                    Ok(stream) => {
                        drop(stream);
                        tokio::task::yield_now().await;
                    }
                    Err(error) => break error,
                }
            }
        })
        .await
        .expect("pending dial blocked dead-session reaping");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        reset(&commands).await;
        assert_eq!(
            blocked.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn connector_enforces_concurrent_dial_capacity_at_boundary() {
        let dial_count = Arc::new(AtomicUsize::new(0));
        let dialer: DualLaneDialer = Arc::new({
            let dial_count = Arc::clone(&dial_count);
            move |_addr, _group, _socket| {
                dial_count.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::pending())
            }
        });
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
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
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn redial_respects_concurrent_dial_capacity() {
        let live_addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let dial_count = Arc::new(AtomicUsize::new(0));
        let dialer: DualLaneDialer = Arc::new({
            let dial_count = Arc::clone(&dial_count);
            move |addr, _group, _socket| {
                dial_count.fetch_add(1, Ordering::SeqCst);
                if addr == live_addr {
                    Box::pin(async move { Ok(fake_connected_birth(addr, None)) })
                } else {
                    Box::pin(std::future::pending())
                }
            }
        });
        let mut connector_tasks = JoinSet::new();
        let (commands, sessions) = spawn_test_connector_with_sessions(dialer, &mut connector_tasks);
        drop(enqueue(&commands, live_addr).await.await.unwrap().unwrap());
        let mut responses = Vec::new();
        for port in 10_000..10_000 + MAX_CONCURRENT_DUAL_DIALS as u16 {
            responses.push(enqueue(&commands, SocketAddr::from(([192, 0, 2, 2], port))).await);
        }
        wait_for(
            || dial_count.load(Ordering::SeqCst) == MAX_CONCURRENT_DUAL_DIALS + 1,
            "all admitted dials to start",
        )
        .await;
        let old_id = sessions.lock().unwrap().get(&live_addr).unwrap().id;
        commands
            .send(ConnectorCommand::Redial {
                addr: live_addr,
                trigger: RedialTrigger::Forced,
            })
            .await
            .unwrap();
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            dial_count.load(Ordering::SeqCst),
            MAX_CONCURRENT_DUAL_DIALS + 1,
            "redial dialed past the concurrent dial capacity",
        );
        assert_eq!(
            sessions.lock().unwrap().get(&live_addr).unwrap().id,
            old_id,
            "the session was replaced despite the dial budget being full",
        );
        reset(&commands).await;
        for response in responses {
            assert_eq!(
                response.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted
            );
        }
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn connector_enforces_waiter_capacity_and_reset_fails_every_waiter() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let dialer: DualLaneDialer =
            Arc::new(|_addr, _group, _socket| Box::pin(std::future::pending()));
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
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
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn closed_waiters_do_not_consume_per_destination_capacity() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let barrier_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();
        let dialer: DualLaneDialer = Arc::new(move |dial_addr, _group, _socket| {
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
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
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
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn connector_can_redial_immediately_after_reset() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let dialer: DualLaneDialer = Arc::new({
            let attempts = Arc::clone(&attempts);
            let first_started = Arc::clone(&first_started);
            move |_addr, _group, _socket| {
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
        let mut connector_tasks = JoinSet::new();
        let commands = spawn_test_connector(dialer, &mut connector_tasks);
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
        stop(commands, &mut connector_tasks).await;
    }

    #[tokio::test]
    async fn fast_path_serves_cached_sessions_without_the_connector_task() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let other_addr: SocketAddr = "192.0.2.2:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (connector, driver) =
            RtpMuxConnector::with_dialer(counting_fake_dialer(Arc::clone(&attempts)));
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        let first = connector.connect(addr).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        driver_tasks.abort_all();
        let second = connector.connect(addr).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let error = connector.connect(other_addr).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        drop((first, second));
    }

    #[tokio::test(start_paused = true)]
    async fn live_stream_gauge_and_graceful_redial() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (connector, driver) =
            RtpMuxConnector::with_dialer(counting_fake_dialer(Arc::clone(&attempts)));
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        let first = connector.connect(addr).await.unwrap();
        let second = connector.connect(addr).await.unwrap();
        let session = Arc::downgrade(connector.sessions.lock().unwrap().get(&addr).unwrap());
        let old_id = connector.probe_session(addr).unwrap().id();
        {
            let session = session.upgrade().unwrap();
            assert_eq!(session.live_streams.load(Ordering::Relaxed), 2);
            assert_eq!(session.opened_streams.load(Ordering::Relaxed), 2);
        }
        connector.force_redial(addr);
        wait_for(
            || {
                connector
                    .probe_session(addr)
                    .is_some_and(|p| p.id() != old_id)
            },
            "replacement session after redial",
        )
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "redial dials first");
        drop(first);
        assert_eq!(
            session
                .upgrade()
                .unwrap()
                .live_streams
                .load(Ordering::Relaxed),
            1
        );
        drop(second);
        assert!(session.upgrade().is_none());
        let fresh = connector.connect(addr).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        drop(fresh);
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn redial_is_skipped_while_the_previous_generation_drains() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (connector, driver) =
            RtpMuxConnector::with_dialer(counting_fake_dialer(Arc::clone(&attempts)));
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        let old_stream = connector.connect(addr).await.unwrap();
        let old_id = connector.probe_session(addr).unwrap().id();
        connector.force_redial(addr);
        wait_for(
            || {
                connector
                    .probe_session(addr)
                    .is_some_and(|p| p.id() != old_id)
            },
            "replacement session after redial",
        )
        .await;
        let fresh_id = connector.probe_session(addr).unwrap().id();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        connector.force_redial(addr);
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(connector.probe_session(addr).unwrap().id(), fresh_id);
        drop(connector.connect(addr).await.unwrap());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "no new dial while skipped"
        );
        drop(old_stream);
        connector.force_redial(addr);
        wait_for(
            || {
                connector
                    .probe_session(addr)
                    .is_some_and(|p| p.id() != fresh_id)
            },
            "third session after drain completed",
        )
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn a_stream_opened_before_a_redial_lands_on_the_new_session() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (connector, driver) =
            RtpMuxConnector::with_dialer(counting_fake_dialer(Arc::clone(&attempts)));
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        let opened = connector.connect(addr).await.unwrap();
        let old = connector.probe_session(addr).unwrap();
        connector.force_redial(addr);
        wait_for(
            || {
                connector
                    .probe_session(addr)
                    .is_some_and(|probe| probe.id() != old.id())
            },
            "replacement session after redial",
        )
        .await;
        let new = connector.probe_session(addr).unwrap();
        assert_eq!(
            new.live_streams(),
            Some(0),
            "the redial could not have migrated an untracked stream",
        );
        let stream = opened.into_stream();
        assert_eq!(
            new.live_streams(),
            Some(1),
            "the late-tracked stream must land on the live generation",
        );
        assert!(
            old.live_streams().is_none_or(|live| live == 0),
            "the drained generation must not keep the stream: {:?}",
            old.live_streams(),
        );
        drop(stream);
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_redial_dial_keeps_streams_on_the_old_session() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let dialer: DualLaneDialer = Arc::new({
            let attempts = Arc::clone(&attempts);
            move |addr, _group, _socket| {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Box::pin(async move { Ok(fake_connected_birth(addr, None)) })
                } else {
                    Box::pin(async {
                        Err(io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            "synthetic redial dial failure",
                        ))
                    })
                }
            }
        });
        let (connector, driver) = RtpMuxConnector::with_dialer(dialer);
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        let stream = connector.connect(addr).await.unwrap();
        let old_id = connector.probe_session(addr).unwrap().id();
        connector.force_redial(addr);
        wait_for(
            || attempts.load(Ordering::SeqCst) == 2,
            "redial dial attempt",
        )
        .await;
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let probe = connector.probe_session(addr).unwrap();
        assert_eq!(probe.id(), old_id);
        assert_eq!(probe.live_streams(), Some(1));
        drop(stream);
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn redial_presents_the_same_group_token() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let tokens: Arc<Mutex<Vec<GroupToken>>> = Arc::new(Mutex::new(Vec::new()));
        let dialer: DualLaneDialer = Arc::new({
            let tokens = Arc::clone(&tokens);
            move |addr, group, _socket| {
                tokens.lock().unwrap().push(group);
                Box::pin(async move { Ok(fake_connected_birth(addr, None)) })
            }
        });
        let (connector, driver) = RtpMuxConnector::with_dialer(dialer);
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        drop(connector.connect(addr).await.unwrap());
        let old_id = connector.probe_session(addr).unwrap().id();
        connector.force_redial(addr);
        wait_for(
            || {
                connector
                    .probe_session(addr)
                    .is_some_and(|p| p.id() != old_id)
            },
            "replacement session after redial",
        )
        .await;
        {
            let tokens = tokens.lock().unwrap();
            assert_eq!(tokens.len(), 2);
            assert_eq!(tokens[0], tokens[1], "redial must reuse the group token");
        }
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn reset_clears_the_draining_generation() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (connector, driver) =
            RtpMuxConnector::with_dialer(counting_fake_dialer(Arc::clone(&attempts)));
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        drop(connector.connect(addr).await.unwrap());
        let old_id = connector.probe_session(addr).unwrap().id();
        connector.force_redial(addr);
        wait_for(
            || {
                connector
                    .probe_session(addr)
                    .is_some_and(|p| p.id() != old_id)
            },
            "replacement session after redial",
        )
        .await;
        assert!(
            !connector.draining.lock().unwrap().is_empty(),
            "the redial did not record a draining generation",
        );
        connector.reset().await.unwrap();
        assert!(
            connector.draining.lock().unwrap().is_empty(),
            "reset left a stale draining entry, which gates the address forever",
        );
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reoptimize_without_explorer_data_never_redials() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (connector, driver) =
            RtpMuxConnector::with_dialer(counting_fake_dialer(Arc::clone(&attempts)));
        let mut driver_tasks = JoinSet::new();
        driver_tasks.spawn(driver);
        let stream = connector.connect(addr).await.unwrap();
        let old_id = connector.probe_session(addr).unwrap().id();
        connector.reoptimize(addr);
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "reoptimize must not dial"
        );
        assert_eq!(connector.probe_session(addr).unwrap().id(), old_id);
        drop(stream);
        drop(connector);
        while let Some(result) = driver_tasks.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dial_failure_preserves_the_error_kind_across_retries() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let result = retry_dual_connect(addr, || async {
            Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "synthetic dial failure",
            ))
        })
        .await;
        let error = match result {
            Ok(_) => panic!("dial must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        assert!(error.to_string().contains("attempt=3"), "{error}");
    }
}
