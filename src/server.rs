use std::{
    fmt::Debug,
    future::Future,
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use metrics::counter;
use mux::{
    AcceptedStream, LaneClass, MuxError, PairingNonce, complete_pairing, read_lane_hello,
    spawn_mux_no_reconnection, write_liveness_heartbeat,
};
use rtp::socket::{FrameByteReader, FrameByteWriter};
use thiserror::Error;
use tokio::{net::ToSocketAddrs, task::JoinSet};
use tracing::{info, instrument, trace, warn};

use crate::{
    accept_error::AcceptErrorBackoff,
    admission::{
        AdmittedLane, ExpiredPendingLane, PendingLane, PendingLaneAdmission, PendingLaneRegistry,
        PendingLaneWait, PreparedLane,
    },
    group::{PairMember, SessionPairRegistry},
    lane_rejection::{LaneRejectionClass, LaneRejectionLog, RejectedLaneContext},
    session::SessionSpawner,
    shared::{ADMISSION_REJECTION_LOG_INTERVAL, HELLO_DEADLINE, bulk_lane_addr, server_mux_config},
    stream::{ServerStream, SocketAddrPair},
};

type StreamHandler = Arc<dyn Fn(ServerStream) + Send + Sync + 'static>;

struct DualLaneSocketAddrs {
    interactive_peer: SocketAddr,
    interactive_local: SocketAddr,
    bulk_peer: SocketAddr,
    bulk_local: SocketAddr,
}

#[derive(Debug)]
pub struct RtpMuxServer {
    interactive_listener: rtp::udp::Listener,
    bulk_listener: rtp::udp::Listener,
    mux: JoinSet<MuxError>,
    fec: bool,
}

enum BirthHeartbeatFailure {
    Timeout(Duration),
    Io {
        source: io::Error,
        elapsed: Duration,
    },
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("Failed to accept connection: {source}, {addr}")]
    Accept {
        #[source]
        source: io::Error,
        addr: SocketAddr,
    },
    #[error("Pending-lane expiry worker terminated unexpectedly; addr={addr}")]
    ExpiryWorkerStopped { addr: SocketAddr },
}

impl RtpMuxServer {
    pub async fn bind(addr: impl ToSocketAddrs + Clone + Debug, fec: bool) -> io::Result<Self> {
        let interactive_listener = rtp::udp::Listener::bind(addr).await?;
        let bulk_addr = bulk_lane_addr(interactive_listener.local_addr())?;
        let bulk_listener = rtp::udp::Listener::bind(bulk_addr).await?;
        Ok(Self::new(interactive_listener, bulk_listener, fec))
    }

    pub fn new(
        interactive_listener: rtp::udp::Listener,
        bulk_listener: rtp::udp::Listener,
        fec: bool,
    ) -> Self {
        Self {
            interactive_listener,
            bulk_listener,
            mux: JoinSet::new(),
            fec,
        }
    }

    pub fn listener(&self) -> &rtp::udp::Listener {
        &self.interactive_listener
    }

    pub fn bulk_listener(&self) -> &rtp::udp::Listener {
        &self.bulk_listener
    }

    #[instrument(skip_all)]
    pub async fn serve(
        mut self,
        session_spawner: SessionSpawner,
        handler: impl Fn(ServerStream) + Send + Sync + 'static,
    ) -> Result<(), ServeError> {
        let addr = self.interactive_listener.local_addr();
        let bulk_addr = self.bulk_listener.local_addr();
        info!(
            ?addr,
            ?bulk_addr,
            "Listening (interactive + bulk dual-lane)"
        );
        let handler: StreamHandler = Arc::new(handler);
        let registry = PendingLaneRegistry::new();
        let groups = SessionPairRegistry::new();
        let rejections = LaneRejectionLog::default();
        let mut interactive_backoff = AcceptErrorBackoff::default();
        let mut bulk_backoff = AcceptErrorBackoff::default();
        let mut rejection_log = rejection_log_ticker();
        let mut expiry: JoinSet<()> = JoinSet::new();
        {
            let registry = Arc::clone(&registry);
            let rejections = rejections.clone();
            expiry.spawn(async move {
                run_pending_lane_expiry(registry, rejections).await;
            });
        }
        loop {
            trace!("Waiting for RTP mux Lane");
            tokio::select! {
                Some(result) = self.mux.join_next() => {
                    match result {
                        Ok(MuxError::TaskStopped { task: "lane_accept" }) => trace!(?addr, "Lane accept task stopped"),
                        Ok(error) => warn!(?error, ?addr, "MUX error"),
                        Err(error) if error.is_cancelled() => { trace!(?error, "MUX task cancelled (normal shutdown/reset)"); }
                        Err(error) => std::panic::resume_unwind(error.into_panic()),
                    }
                }
                Some(joined) = expiry.join_next() => {
                    joined.unwrap();
                    return Err(ServeError::ExpiryWorkerStopped { addr });
                }
                _ = rejection_log.tick() => rejections.flush(),
                result = self.interactive_listener.accept_frame_delivery(rtp::udp::AcceptConfig { fec: self.fec, ..rtp::udp::AcceptConfig::default() }) => {
                    handle_lane_accept(result, &handler, &registry, &groups, &rejections, &session_spawner, &mut self.mux, HandleLaneAcceptConfig { backoff: &mut interactive_backoff, backoff_name: "rtp_mux_interactive", addr, lane: LaneClass::Interactive }).await?;
                }
                result = self.bulk_listener.accept_frame_delivery(rtp::udp::AcceptConfig { fec: self.fec, ..rtp::udp::AcceptConfig::default() }) => {
                    handle_lane_accept(result, &handler, &registry, &groups, &rejections, &session_spawner, &mut self.mux, HandleLaneAcceptConfig { backoff: &mut bulk_backoff, backoff_name: "rtp_mux_bulk", addr: bulk_addr, lane: LaneClass::Bulk }).await?;
                }
            }
        }
    }
}

fn rejection_log_ticker() -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(ADMISSION_REJECTION_LOG_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker
}

async fn finish_frame_delivery_accept(
    accept: io::Result<rtp::udp::FrameDeliveryAccept>,
) -> io::Result<rtp::udp::FrameDeliveryIo> {
    accept?.await
}

/// Per-lane settings for [`handle_lane_accept`]: the bits that vary between
/// the interactive and bulk lanes but are not part of the shared accept
/// machinery (handler, registries, spawner, mux join set).
#[derive(Debug)]
struct HandleLaneAcceptConfig<'a> {
    backoff: &'a mut AcceptErrorBackoff,
    backoff_name: &'static str,
    addr: SocketAddr,
    lane: LaneClass,
}

#[allow(clippy::too_many_arguments)]
async fn handle_lane_accept(
    accept: io::Result<rtp::udp::FrameDeliveryAccept>,
    handler: &StreamHandler,
    registry: &Arc<PendingLaneRegistry>,
    groups: &Arc<SessionPairRegistry>,
    rejections: &LaneRejectionLog,
    session_spawner: &SessionSpawner,
    mux: &mut JoinSet<MuxError>,
    config: HandleLaneAcceptConfig<'_>,
) -> Result<(), ServeError> {
    let HandleLaneAcceptConfig {
        backoff,
        backoff_name,
        addr,
        lane,
    } = config;
    let stream = match finish_frame_delivery_accept(accept).await {
        Ok(stream) => {
            backoff.accepted(backoff_name, addr);
            stream
        }
        Err(error) => match backoff.failed_dispatching(backoff_name, addr, error) {
            Ok(()) => {
                backoff.pause().await;
                return Ok(());
            }
            Err(source) => return Err(ServeError::Accept { source, addr }),
        },
    };
    counter!("stream.rtp_mux.rtp.accepts").increment(1);
    let peer = stream.peer_addr;
    let read = stream.read;
    let write = stream.write;
    // The accepted lane's RTP session owner rides inside the AdmittedLane:
    // it is transferred into the paired session on success, and dropped
    // (aborting the session) on rejection, timeout, or failed pairing.
    let supervisor = stream.supervisor;
    let permit = match registry.try_admit(peer.ip()) {
        Ok(permit) => permit,
        Err(reason) => {
            rejections.record(RejectedLaneContext {
                class: LaneRejectionClass::Capacity,
                peer,
                local_addr: addr,
                expected_class: Some(lane),
                reason: reason.to_string(),
            });
            return Ok(());
        }
    };
    spawn_lane_accept(
        AdmittedLane {
            read,
            write,
            config: server_mux_config(),
            expected_class: lane,
            peer,
            local_addr: addr,
            permit,
            supervisor,
        },
        handler.clone(),
        Arc::clone(registry),
        Arc::clone(groups),
        rejections.clone(),
        session_spawner,
        mux,
    );
    Ok(())
}

fn spawn_lane_accept(
    admitted: AdmittedLane,
    handler: StreamHandler,
    registry: Arc<PendingLaneRegistry>,
    groups: Arc<SessionPairRegistry>,
    rejections: LaneRejectionLog,
    session_spawner: &SessionSpawner,
    mux: &mut JoinSet<MuxError>,
) {
    let AdmittedLane {
        mut read,
        mut write,
        config,
        expected_class,
        peer,
        local_addr,
        permit,
        supervisor,
    } = admitted;
    let session_spawner = (*session_spawner).clone();
    mux.spawn(async move {
        let started = Instant::now();
        let (class, nonce, group) =
            match tokio::time::timeout(HELLO_DEADLINE, read_lane_hello(&mut read)).await {
                Ok(Ok(result)) => result,
                Err(_) => {
                    let elapsed = started.elapsed();
                    drop(read);
                    drop(write);
                    record_rejected_lane(
                        &rejections,
                        RejectedLaneContext {
                            class: LaneRejectionClass::HelloTimeout,
                            peer,
                            local_addr,
                            expected_class: Some(expected_class),
                            reason: "hello deadline elapsed".to_string(),
                        },
                        elapsed,
                    );
                    drop(permit);
                    return MuxError::TaskStopped {
                        task: "lane_accept",
                    };
                }
                Ok(Err(error)) => {
                    let elapsed = started.elapsed();
                    drop(permit);
                    signal_rejected_lane(
                        &mut write,
                        &rejections,
                        RejectedLaneContext {
                            class: LaneRejectionClass::HelloParse,
                            peer,
                            local_addr,
                            expected_class: Some(expected_class),
                            reason: format!("hello read/parse error: {error:?}"),
                        },
                        elapsed,
                    )
                    .await;
                    return MuxError::TaskStopped {
                        task: "lane_accept",
                    };
                }
            };
        let elapsed = started.elapsed();
        if class != expected_class {
            drop(permit);
            signal_rejected_lane(
                &mut write,
                &rejections,
                RejectedLaneContext {
                    class: LaneRejectionClass::ClassMismatch,
                    peer,
                    local_addr,
                    expected_class: Some(expected_class),
                    reason: format!("lane class mismatch: got {class:?}"),
                },
                elapsed,
            )
            .await;
            return MuxError::TaskStopped {
                task: "lane_accept",
            };
        }
        if groups.is_full(&group) {
            drop(permit);
            signal_rejected_lane(
                &mut write,
                &rejections,
                RejectedLaneContext {
                    class: LaneRejectionClass::GroupFull,
                    peer,
                    local_addr,
                    expected_class: Some(expected_class),
                    reason: "session group is full".to_string(),
                },
                elapsed,
            )
            .await;
            return MuxError::TaskStopped {
                task: "lane_accept",
            };
        }
        let mut permit = Some(permit);
        match registry.register_admitted(nonce, class, peer, local_addr, group, &mut permit) {
            PendingLaneAdmission::Reserved => {
                if let Err(failure) = write_birth_heartbeat_result(&mut write).await {
                    registry.cancel_reservation(nonce, peer, class);
                    reject_birth_heartbeat(
                        read,
                        write,
                        &rejections,
                        RejectedLaneContext {
                            class: LaneRejectionClass::BirthHeartbeat,
                            peer,
                            local_addr,
                            expected_class: Some(expected_class),
                            reason: "birth heartbeat failed after reservation".to_string(),
                        },
                        failure,
                    )
                    .await;
                    return MuxError::TaskStopped {
                        task: "lane_accept",
                    };
                }
                let mut tasks = JoinSet::new();
                let (opener, accepter) = spawn_mux_no_reconnection(read, write, config, &mut tasks);
                let lane = PreparedLane {
                    pending: mux::UnpairedLane::new(class, nonce, group, opener, accepter, tasks),
                    peer,
                    local_addr,
                    supervisor,
                };
                confirm_reservation_or_reject(
                    &registry,
                    &rejections,
                    nonce,
                    lane,
                    expected_class,
                    started.elapsed(),
                );
            }
            PendingLaneAdmission::Wait {
                changed,
                expires_at,
            } => {
                if let Err(failure) = write_birth_heartbeat_result(&mut write).await {
                    reject_birth_heartbeat(
                        read,
                        write,
                        &rejections,
                        RejectedLaneContext {
                            class: LaneRejectionClass::BirthHeartbeat,
                            peer,
                            local_addr,
                            expected_class: Some(expected_class),
                            reason: "birth heartbeat failed while waiting".to_string(),
                        },
                        failure,
                    )
                    .await;
                    return MuxError::TaskStopped {
                        task: "lane_accept",
                    };
                }
                match registry
                    .wait_for_pair(nonce, class, peer, expires_at, changed)
                    .await
                {
                    PendingLaneWait::Pair(other) => {
                        let mut tasks = JoinSet::new();
                        let (opener, accepter) =
                            spawn_mux_no_reconnection(read, write, config, &mut tasks);
                        let lane = PendingLane {
                            pending: mux::UnpairedLane::new(
                                class, nonce, group, opener, accepter, tasks,
                            ),
                            peer,
                            local_addr,
                            group,
                            _permit: permit.take().unwrap(),
                            supervisor,
                        };
                        pair_lanes_inner(
                            lane,
                            other,
                            handler.clone(),
                            nonce,
                            &session_spawner,
                            &groups,
                        );
                    }
                    PendingLaneWait::Timeout => {
                        drop(permit.take());
                        signal_rejected_lane(
                            &mut write,
                            &rejections,
                            RejectedLaneContext {
                                class: LaneRejectionClass::PairingTimeout,
                                peer,
                                local_addr,
                                expected_class: Some(expected_class),
                                reason: "pairing deadline expired while waiting".to_string(),
                            },
                            started.elapsed(),
                        )
                        .await;
                    }
                    PendingLaneWait::ReservationLost(reason) => {
                        drop(permit.take());
                        signal_rejected_lane(
                            &mut write,
                            &rejections,
                            RejectedLaneContext {
                                class: LaneRejectionClass::ReservationLost,
                                peer,
                                local_addr,
                                expected_class: Some(expected_class),
                                reason: reason.to_string(),
                            },
                            started.elapsed(),
                        )
                        .await;
                    }
                }
            }
            PendingLaneAdmission::Pair {
                lane: other,
                expires_at,
            } => {
                if let Err(failure) = write_birth_heartbeat_result(&mut write).await {
                    reinsert_ready_lane_or_reject(
                        &registry,
                        &rejections,
                        nonce,
                        other,
                        expires_at,
                        started.elapsed(),
                    );
                    reject_birth_heartbeat(
                        read,
                        write,
                        &rejections,
                        RejectedLaneContext {
                            class: LaneRejectionClass::BirthHeartbeat,
                            peer,
                            local_addr,
                            expected_class: Some(expected_class),
                            reason: "birth heartbeat failed before pairing".to_string(),
                        },
                        failure,
                    )
                    .await;
                    return MuxError::TaskStopped {
                        task: "lane_accept",
                    };
                }
                let mut tasks = JoinSet::new();
                let (opener, accepter) = spawn_mux_no_reconnection(read, write, config, &mut tasks);
                let lane = PendingLane {
                    pending: mux::UnpairedLane::new(class, nonce, group, opener, accepter, tasks),
                    peer,
                    local_addr,
                    group,
                    _permit: permit.take().unwrap(),
                    supervisor,
                };
                pair_lanes_inner(
                    lane,
                    other,
                    handler.clone(),
                    nonce,
                    &session_spawner,
                    &groups,
                );
            }
            PendingLaneAdmission::Reject(reason) => {
                drop(permit);
                signal_rejected_lane(
                    &mut write,
                    &rejections,
                    RejectedLaneContext {
                        class: LaneRejectionClass::Admission,
                        peer,
                        local_addr,
                        expected_class: Some(expected_class),
                        reason: reason.to_string(),
                    },
                    started.elapsed(),
                )
                .await;
            }
        }
        MuxError::TaskStopped {
            task: "lane_accept",
        }
    });
}

async fn write_birth_heartbeat_result(
    writer: &mut FrameByteWriter,
) -> Result<(), BirthHeartbeatFailure> {
    await_birth_heartbeat(write_liveness_heartbeat(writer), HELLO_DEADLINE).await
}

async fn await_birth_heartbeat<F>(
    heartbeat: F,
    deadline: Duration,
) -> Result<(), BirthHeartbeatFailure>
where
    F: Future<Output = io::Result<()>>,
{
    let started = Instant::now();
    match tokio::time::timeout(deadline, heartbeat).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BirthHeartbeatFailure::Io {
            source,
            elapsed: started.elapsed(),
        }),
        Err(_) => Err(BirthHeartbeatFailure::Timeout(started.elapsed())),
    }
}

async fn reject_birth_heartbeat(
    read: FrameByteReader,
    mut writer: FrameByteWriter,
    rejections: &LaneRejectionLog,
    mut context: RejectedLaneContext,
    failure: BirthHeartbeatFailure,
) {
    match failure {
        BirthHeartbeatFailure::Timeout(elapsed) => {
            reject_timed_out_birth_heartbeat(read, writer, rejections, context, elapsed)
        }
        BirthHeartbeatFailure::Io { source, elapsed } => {
            context.reason = format!("{}: {source}", context.reason);
            signal_rejected_lane(&mut writer, rejections, context, elapsed).await;
            drop(read);
        }
    }
}

fn reject_timed_out_birth_heartbeat<R, W>(
    read: R,
    writer: W,
    rejections: &LaneRejectionLog,
    mut context: RejectedLaneContext,
    elapsed: Duration,
) {
    drop(read);
    drop(writer);
    context.reason = format!("{}: deadline elapsed", context.reason);
    record_rejected_lane(rejections, context, elapsed);
}

fn reinsert_ready_lane_or_reject(
    registry: &PendingLaneRegistry,
    rejections: &LaneRejectionLog,
    nonce: PairingNonce,
    lane: PendingLane,
    expires_at: Instant,
    elapsed: Duration,
) {
    let (peer, local_addr, class) = (lane.peer, lane.local_addr, lane.pending.class);
    let Err(lane) = registry.reinsert_ready_lane(nonce, lane, expires_at) else {
        return;
    };
    drop(lane);
    record_rejected_lane(
        rejections,
        RejectedLaneContext {
            class: LaneRejectionClass::ReservationLost,
            peer,
            local_addr,
            expected_class: Some(class),
            reason: "pairing slot was reclaimed before the partner could be restored".to_string(),
        },
        elapsed,
    );
}

fn confirm_reservation_or_reject(
    registry: &PendingLaneRegistry,
    rejections: &LaneRejectionLog,
    nonce: PairingNonce,
    lane: PreparedLane,
    expected_class: LaneClass,
    elapsed: Duration,
) {
    let (peer, local_addr) = (lane.peer, lane.local_addr);
    let Err(lane) = registry.confirm_reservation(nonce, lane) else {
        return;
    };
    drop(lane);
    record_rejected_lane(
        rejections,
        RejectedLaneContext {
            class: LaneRejectionClass::ReservationLost,
            peer,
            local_addr,
            expected_class: Some(expected_class),
            reason: "reservation vanished before the built lane could claim it".to_string(),
        },
        elapsed,
    );
}

async fn signal_rejected_lane(
    writer: &mut FrameByteWriter,
    rejections: &LaneRejectionLog,
    context: RejectedLaneContext,
    elapsed: Duration,
) {
    let _ = tokio::time::timeout(HELLO_DEADLINE, writer.send_kill_and_abort()).await;
    record_rejected_lane(rejections, context, elapsed);
}

fn record_rejected_lane(
    rejections: &LaneRejectionLog,
    mut context: RejectedLaneContext,
    elapsed: Duration,
) {
    context.reason = format!("{}; elapsed_ms={}", context.reason, elapsed.as_millis());
    rejections.record(context);
}

fn pair_lanes_inner(
    lane_a: PendingLane,
    lane_b: PendingLane,
    handler: StreamHandler,
    nonce: PairingNonce,
    session_spawner: &SessionSpawner,
    groups: &Arc<SessionPairRegistry>,
) {
    counter!("stream.rtp_mux.paired").increment(1);
    let addrs = classify_lane_addrs(
        lane_a.pending.class,
        lane_a.peer,
        lane_a.local_addr,
        lane_b.peer,
        lane_b.local_addr,
    );
    pair_lanes(
        lane_a,
        lane_b,
        handler,
        addrs,
        nonce,
        session_spawner,
        groups,
    );
}

async fn run_pending_lane_expiry(registry: Arc<PendingLaneRegistry>, rejections: LaneRejectionLog) {
    loop {
        let changed = registry.changed.notified();
        let Some(expires_at) = registry.next_expiry() else {
            changed.await;
            continue;
        };
        tokio::select! {
            () = tokio::time::sleep_until(expires_at.into()) => {
                for expired in registry.expire(Instant::now()) {
                    match expired {
                        ExpiredPendingLane::Building { nonce, peer, local_addr, class, .. } => {
                            rejections.record(RejectedLaneContext {
                                class: LaneRejectionClass::PairingTimeout,
                                peer,
                                local_addr,
                                expected_class: Some(class),
                                reason: format!("pairing deadline expired while preparing lane; nonce={nonce:?}"),
                            });
                        }
                        ExpiredPendingLane::Ready { nonce, lane } => {
                            rejections.record(RejectedLaneContext {
                                class: LaneRejectionClass::PairingTimeout,
                                peer: lane.peer,
                                local_addr: lane.local_addr,
                                expected_class: Some(lane.pending.class),
                                reason: format!("pairing deadline expired; nonce={nonce:?}"),
                            });
                        }
                    }
                }
            }
            () = changed => {}
        }
    }
}

fn classify_lane_addrs(
    lane_a_class: LaneClass,
    lane_a_peer: SocketAddr,
    lane_a_local: SocketAddr,
    lane_b_peer: SocketAddr,
    lane_b_local: SocketAddr,
) -> DualLaneSocketAddrs {
    let (interactive_peer, interactive_local, bulk_peer, bulk_local) = match lane_a_class {
        LaneClass::Interactive => (lane_a_peer, lane_a_local, lane_b_peer, lane_b_local),
        LaneClass::Bulk => (lane_b_peer, lane_b_local, lane_a_peer, lane_a_local),
    };
    DualLaneSocketAddrs {
        interactive_peer,
        interactive_local,
        bulk_peer,
        bulk_local,
    }
}

fn pair_lanes(
    lane_a: PendingLane,
    lane_b: PendingLane,
    handler: StreamHandler,
    addrs: DualLaneSocketAddrs,
    nonce: PairingNonce,
    session_spawner: &SessionSpawner,
    groups: &Arc<SessionPairRegistry>,
) {
    let group_token = lane_a.group;
    let mut tasks = JoinSet::new();
    match complete_pairing(lane_a.pending, lane_b.pending, &mut tasks) {
        Ok((opener, accepter)) => {
            let member = match groups.join(group_token, opener.clone()) {
                Ok(member) => member,
                Err(reason) => {
                    warn!(reason, ?nonce, dn_interactive = ?addrs.interactive_peer, "RTP mux session rejected at group join");
                    counter!("stream.rtp_mux.group_full").increment(1);
                    return;
                }
            };
            let addr = SocketAddrPair {
                local_addr: addrs.interactive_local,
                peer_addr: addrs.interactive_peer,
            };
            // Both lanes' RTP session owners are awaited for the session's
            // whole life, so their completion and any driver panic surfaces
            // here instead of being silently detached. A rejected, timed-out,
            // or failed pairing never reaches here — it aborts them via the
            // PendingLane drop instead.
            let supervisor_a = lane_a.supervisor;
            let supervisor_b = lane_b.supervisor;
            session_spawner.spawn(async move {
                let lane_sessions = async {
                    tokio::join!(supervisor_a, supervisor_b);
                };
                let mux_session = async {
                    let paired_at = Instant::now();
                    let accepted_streams =
                        run_dual_mux_accepter(accepter, opener, addr, handler, member).await;
                    let error = match tasks.join_next().await {
                        Some(result) => result.unwrap(),
                        None => MuxError::TaskStopped { task: "dual_lane" },
                    };
                    warn!(event = "rtp_mux_session_terminated", ?error, ?nonce, dn_interactive = ?addrs.interactive_peer, dn_interactive_local = ?addrs.interactive_local, dn_bulk = ?addrs.bulk_peer, dn_bulk_local = ?addrs.bulk_local, accepted_streams, uptime_ms = paired_at.elapsed().as_millis(), "RTP mux dual-lane session terminated");
                };
                tokio::join!(lane_sessions, mux_session);
            });
        }
        Err(error) => {
            warn!(?error, ?nonce, dn_interactive = ?addrs.interactive_peer, dn_interactive_local = ?addrs.interactive_local, dn_bulk = ?addrs.bulk_peer, dn_bulk_local = ?addrs.bulk_local, "RTP mux dual-lane pairing failed");
            counter!("stream.rtp_mux.pairing_failed").increment(1);
        }
    }
}

async fn run_dual_mux_accepter(
    accepter: mux::DualStreamAccepter,
    opener: mux::DualStreamOpener,
    addr: SocketAddrPair,
    handler: StreamHandler,
    member: PairMember,
) -> u64 {
    let mut accepter = accepter.into_migrating_duplex_with_feed(opener, member.feed());
    let mut accepted_streams = 0;
    loop {
        let accepted = match accepter.accept().await {
            Ok(accepted) => accepted,
            Err(_) => break,
        };
        let stream = match accepted {
            AcceptedStream::Migrating {
                reader,
                writer,
                source_lane,
            } => ServerStream::Migrating {
                reader,
                writer,
                addr,
                source_lane,
            },
            AcceptedStream::MigratingDuplex {
                reader,
                writer,
                source_lane,
            } => {
                let (writer, rebind) =
                    crate::migrating_write_half::MigratingWriteHalf::new_with_rebind(writer);
                member.register_writer(rebind);
                ServerStream::MigratingDuplex {
                    reader,
                    writer,
                    addr,
                    source_lane,
                }
            }
            AcceptedStream::Plain {
                reader,
                writer,
                source_lane,
            } => ServerStream::Plain {
                reader,
                writer,
                addr,
                source_lane,
            },
        };
        counter!("stream.rtp_mux.accepts").increment(1);
        handler(stream);
        accepted_streams += 1;
    }
    accepted_streams
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll},
    };

    use tokio::io::AsyncWrite;

    use super::*;

    #[test]
    fn bulk_lane_port_rejects_overflow() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), u16::MAX);
        assert!(bulk_lane_addr(addr).is_err());
    }

    #[test]
    fn canonical_server_address_is_independent_of_lane_arrival_order() {
        let int_peer: SocketAddr = "10.0.0.1:1000".parse().unwrap();
        let int_local: SocketAddr = "10.0.0.2:2000".parse().unwrap();
        let bulk_peer: SocketAddr = "10.0.0.1:1001".parse().unwrap();
        let bulk_local: SocketAddr = "10.0.0.2:2001".parse().unwrap();
        let first = classify_lane_addrs(
            LaneClass::Interactive,
            int_peer,
            int_local,
            bulk_peer,
            bulk_local,
        );
        assert_eq!(first.interactive_peer, int_peer);
        assert_eq!(first.interactive_local, int_local);
        assert_eq!(first.bulk_peer, bulk_peer);
        assert_eq!(first.bulk_local, bulk_local);
        let second =
            classify_lane_addrs(LaneClass::Bulk, bulk_peer, bulk_local, int_peer, int_local);
        assert_eq!(second.interactive_peer, int_peer);
        assert_eq!(second.interactive_local, int_local);
        assert_eq!(second.bulk_peer, bulk_peer);
        assert_eq!(second.bulk_local, bulk_local);
    }

    struct ReadDropProbe {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ReadDropProbe {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    struct PendingWriter {
        dropped: Arc<AtomicBool>,
        io_polls: Arc<AtomicUsize>,
    }

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.io_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.io_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.io_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }
    }

    impl Drop for PendingWriter {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    struct CancellationProbe<F> {
        inner: Pin<Box<F>>,
        cancelled: Arc<AtomicBool>,
        completed: bool,
    }

    impl<F> CancellationProbe<F> {
        fn new(inner: F, cancelled: Arc<AtomicBool>) -> Self {
            Self {
                inner: Box::pin(inner),
                cancelled,
                completed: false,
            }
        }
    }

    impl<F: Future> Future for CancellationProbe<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let result = this.inner.as_mut().poll(cx);
            if result.is_ready() {
                this.completed = true;
            }
            result
        }
    }

    impl<F> Drop for CancellationProbe<F> {
        fn drop(&mut self) {
            if !self.completed {
                self.cancelled.store(true, Ordering::SeqCst);
            }
        }
    }

    #[tokio::test]
    async fn birth_heartbeat_timeout_cancels_write_and_drops_both_rtp_halves() {
        let read_dropped = Arc::new(AtomicBool::new(false));
        let writer_dropped = Arc::new(AtomicBool::new(false));
        let heartbeat_cancelled = Arc::new(AtomicBool::new(false));
        let io_polls = Arc::new(AtomicUsize::new(0));

        let read = ReadDropProbe {
            dropped: Arc::clone(&read_dropped),
        };
        let mut writer = PendingWriter {
            dropped: Arc::clone(&writer_dropped),
            io_polls: Arc::clone(&io_polls),
        };

        let failure = await_birth_heartbeat(
            CancellationProbe::new(
                write_liveness_heartbeat(&mut writer),
                Arc::clone(&heartbeat_cancelled),
            ),
            Duration::from_millis(1),
        )
        .await
        .expect_err("pending birth heartbeat must time out");

        assert!(matches!(failure, BirthHeartbeatFailure::Timeout(_)));
        assert!(heartbeat_cancelled.load(Ordering::SeqCst));

        let io_polls_at_timeout = io_polls.load(Ordering::SeqCst);
        assert!(io_polls_at_timeout > 0);

        reject_timed_out_birth_heartbeat(
            read,
            writer,
            &LaneRejectionLog::default(),
            RejectedLaneContext {
                class: LaneRejectionClass::BirthHeartbeat,
                peer: "127.0.0.1:1000".parse().unwrap(),
                local_addr: "127.0.0.1:2000".parse().unwrap(),
                expected_class: Some(LaneClass::Interactive),
                reason: "birth heartbeat failed after reservation".to_string(),
            },
            Duration::from_millis(1),
        );

        assert!(read_dropped.load(Ordering::SeqCst));
        assert!(writer_dropped.load(Ordering::SeqCst));
        assert_eq!(io_polls.load(Ordering::SeqCst), io_polls_at_timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn the_rejection_log_timer_fires_while_the_accept_loop_stays_busy() {
        let mut ticker = rejection_log_ticker();
        let mut fired = 0_u32;
        let deadline = tokio::time::Instant::now() + ADMISSION_REJECTION_LOG_INTERVAL * 3;
        while tokio::time::Instant::now() < deadline {
            tokio::select! {
                _ = ticker.tick() => fired += 1,
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
        assert!(
            fired >= 2,
            "the rejection summary timer fired {fired} times over three logging intervals of a busy accept loop, so a rejection flood is never logged",
        );
    }

    fn prepared_lane(
        nonce: PairingNonce,
        group: mux::GroupToken,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> PreparedLane {
        prepared_lane_with_class(nonce, group, LaneClass::Interactive, peer, local_addr)
    }

    fn prepared_lane_with_class(
        nonce: PairingNonce,
        group: mux::GroupToken,
        class: LaneClass,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> PreparedLane {
        let (io, _peer_io) = tokio::io::duplex(64);
        let (read, write) = tokio::io::split(io);
        let mut tasks = JoinSet::new();
        let (opener, accepter) = spawn_mux_no_reconnection(
            read,
            write,
            mux::MuxConfig::new(mux::Initiation::Server, Duration::from_secs(5)),
            &mut tasks,
        );
        PreparedLane {
            pending: mux::UnpairedLane::new(class, nonce, group, opener, accepter, tasks),
            peer,
            local_addr,
            supervisor: rtp::socket::SessionHandle::idle(),
        }
    }

    fn pending_lane(
        registry: &Arc<PendingLaneRegistry>,
        nonce: PairingNonce,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> PendingLane {
        let PreparedLane {
            pending,
            peer,
            local_addr,
            supervisor,
        } = prepared_lane(nonce, mux::GroupToken::generate(), peer, local_addr);
        PendingLane {
            pending,
            peer,
            local_addr,
            group: mux::GroupToken::generate(),
            _permit: registry.try_admit(peer.ip()).unwrap(),
            supervisor,
        }
    }

    fn pending_lane_with_class(
        registry: &Arc<PendingLaneRegistry>,
        nonce: PairingNonce,
        group: mux::GroupToken,
        class: LaneClass,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> PendingLane {
        let PreparedLane {
            pending,
            peer,
            local_addr,
            supervisor,
        } = prepared_lane_with_class(nonce, group, class, peer, local_addr);
        PendingLane {
            pending,
            peer,
            local_addr,
            group,
            _permit: registry.try_admit(peer.ip()).unwrap(),
            supervisor,
        }
    }

    #[tokio::test]
    async fn the_session_scope_supervisor_drains_on_normal_shutdown() {
        let sessions: std::sync::Arc<std::sync::Mutex<JoinSet<()>>> =
            std::sync::Arc::new(std::sync::Mutex::new(JoinSet::new()));
        let spawner = SessionSpawner::new({
            let sessions = std::sync::Arc::clone(&sessions);
            move |fut| {
                sessions.lock().unwrap().spawn(fut);
            }
        });
        let registry = PendingLaneRegistry::new();
        let groups = SessionPairRegistry::new();
        let nonce = PairingNonce::generate();
        let group = mux::GroupToken::generate();
        let peer: SocketAddr = "10.0.0.1:1000".parse().unwrap();
        let interactive_local: SocketAddr = "10.0.0.2:2000".parse().unwrap();
        let bulk_local: SocketAddr = "10.0.0.2:2001".parse().unwrap();
        let lane_a = pending_lane_with_class(
            &registry,
            nonce,
            group,
            LaneClass::Interactive,
            peer,
            interactive_local,
        );
        let lane_b =
            pending_lane_with_class(&registry, nonce, group, LaneClass::Bulk, peer, bulk_local);
        let addrs = classify_lane_addrs(
            LaneClass::Interactive,
            peer,
            interactive_local,
            peer,
            bulk_local,
        );
        let handler: StreamHandler = Arc::new(|_| {});
        pair_lanes(lane_a, lane_b, handler, addrs, nonce, &spawner, &groups);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(result) = sessions.lock().unwrap().try_join_next() {
                    return result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the session-scope supervisor never drained on normal shutdown")
        .expect("the session-scope supervisor panicked");
    }

    #[tokio::test]
    async fn a_partner_that_cannot_be_restored_is_recorded_not_silently_dropped() {
        let registry = PendingLaneRegistry::new();
        let rejections = LaneRejectionLog::default();
        let peer: SocketAddr = "10.0.0.1:1000".parse().unwrap();
        let local_addr: SocketAddr = "10.0.0.2:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let mut permit = Some(registry.try_admit(peer.ip()).unwrap());
        registry.register_admitted(
            nonce,
            LaneClass::Bulk,
            peer,
            local_addr,
            mux::GroupToken::generate(),
            &mut permit,
        );
        reinsert_ready_lane_or_reject(
            &registry,
            &rejections,
            nonce,
            pending_lane(&registry, nonce, peer, local_addr),
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(1),
        );
        assert_eq!(
            rejections.recorded(LaneRejectionClass::ReservationLost),
            1,
            "a live lane that could not be restored vanished without a rejection, so the totals under-count by exactly the lanes lost to this race",
        );
    }

    #[tokio::test]
    async fn a_lane_whose_reservation_vanished_is_recorded_not_silently_dropped() {
        let registry = PendingLaneRegistry::new();
        let rejections = LaneRejectionLog::default();
        let peer: SocketAddr = "10.0.0.1:1000".parse().unwrap();
        let local_addr: SocketAddr = "10.0.0.2:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        confirm_reservation_or_reject(
            &registry,
            &rejections,
            nonce,
            prepared_lane(nonce, mux::GroupToken::generate(), peer, local_addr),
            LaneClass::Interactive,
            Duration::from_millis(1),
        );
        assert_eq!(
            rejections.recorded(LaneRejectionClass::ReservationLost),
            1,
            "a fully built lane was dropped without recording a rejection, so the peer sees a bare close and the operator sees nothing"
        );
    }
}
