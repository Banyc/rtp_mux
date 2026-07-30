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
    spawn_mux_no_reconnection, write_birth_heartbeat,
};
use rtp::{
    socket::{FrameReader, FrameWriter},
    transmission::fec_tuning::FecTuning,
};
use thiserror::Error;
use tokio::{net::ToSocketAddrs, task::JoinSet};
use tracing::{info, instrument, trace, warn};

use crate::{
    accept_error::AcceptErrorBackoff,
    admission::{
        AdmittedLane, ExpiredPendingLane, LaneRejectionClass, LaneRejectionLog, PendingLane,
        PendingLaneAdmission, PendingLaneRegistry, PendingLaneWait, PreparedLane,
        RejectedLaneContext,
    },
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
    response_migration: bool,
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
            response_migration: false,
        }
    }

    pub fn with_response_migration(mut self, enabled: bool) -> Self {
        self.response_migration = enabled;
        self
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
        handler: impl Fn(ServerStream) + Send + Sync + 'static,
    ) -> Result<(), ServeError> {
        let addr = self.interactive_listener.local_addr();
        let bulk_addr = self.bulk_listener.local_addr();
        info!(
            ?addr,
            ?bulk_addr,
            "Listening (interactive + bulk dual-lane)"
        );
        let env = AcceptEnv {
            handler: Arc::new(handler),
            response_migration: self.response_migration,
        };
        let registry = PendingLaneRegistry::new();
        let rejections = LaneRejectionLog::default();
        let mut interactive_backoff = AcceptErrorBackoff::default();
        let mut bulk_backoff = AcceptErrorBackoff::default();
        {
            let registry = Arc::clone(&registry);
            let rejections = rejections.clone();
            self.mux.spawn(async move {
                run_pending_lane_expiry(registry, rejections).await;
                MuxError::TaskStopped {
                    task: "pending_lane_expiry",
                }
            });
        }
        loop {
            trace!("Waiting for RTP mux lane");
            tokio::select! {
                Some(result) = self.mux.join_next() => {
                    match result {
                        Ok(error) => warn!(?error, ?addr, "MUX error"),
                        Err(error) if error.is_cancelled() => trace!(?error, "MUX task cancelled (normal shutdown/reset)"),
                        Err(error) => warn!(?error, ?addr, "MUX supervision task failed to join")
                    }
                }
                () = tokio::time::sleep(ADMISSION_REJECTION_LOG_INTERVAL) => rejections.flush(),
                result = self.interactive_listener.accept_frame_delivery(rtp::udp::FrameDeliveryAcceptConfig {
                    handshake: false,
                    fec: self.fec,
                    mss: rtp::udp::MssConfig::Default,
                    fec_tuning: FecTuning::default(),
                }) => {
                    let stream = match finish_frame_delivery_accept(result).await {
                        Ok(stream) => {
                            interactive_backoff.accepted("rtp_mux_interactive", addr);
                            stream
                        }
                        Err(error) => match interactive_backoff.failed_dispatching("rtp_mux_interactive", addr, error) {
                            Ok(()) => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                            Err(source) => return Err(ServeError::Accept { source, addr })
                        }
                    };
                    counter!("stream.rtp_mux.rtp.accepts").increment(1);
                    let peer = stream.peer_addr;
                    let read = stream.read;
                    let write = stream.write;
                    let permit = match registry.try_acquire(peer.ip()) {
                        Ok(permit) => permit,
                        Err(reason) => {
                            rejections.record(RejectedLaneContext {
                                class: LaneRejectionClass::Capacity,
                                peer,
                                local_addr: addr,
                                expected_class: Some(LaneClass::Interactive),
                                reason: reason.to_string(),
                            });
                            continue;
                        }
                    };
                    spawn_lane_accept(
                        AdmittedLane {
                            read,
                            write,
                            config: server_mux_config(),
                            expected_class: LaneClass::Interactive,
                            peer,
                            local_addr: addr,
                            permit,
                        },
                        env.clone(),
                        Arc::clone(&registry),
                        rejections.clone(),
                    );
                }
                result = self.bulk_listener.accept_frame_delivery(rtp::udp::FrameDeliveryAcceptConfig {
                    handshake: false,
                    fec: self.fec,
                    mss: rtp::udp::MssConfig::Default,
                    fec_tuning: FecTuning::default(),
                }) => {
                    let stream = match finish_frame_delivery_accept(result).await {
                        Ok(stream) => {
                            bulk_backoff.accepted("rtp_mux_bulk", bulk_addr);
                            stream
                        }
                        Err(error) => match bulk_backoff.failed_dispatching("rtp_mux_bulk", bulk_addr, error) {
                            Ok(()) => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                            Err(source) => return Err(ServeError::Accept { source, addr: bulk_addr })
                        }
                    };
                    counter!("stream.rtp_mux.rtp.accepts").increment(1);
                    let peer = stream.peer_addr;
                    let read = stream.read;
                    let write = stream.write;
                    let permit = match registry.try_acquire(peer.ip()) {
                        Ok(permit) => permit,
                        Err(reason) => {
                            rejections.record(RejectedLaneContext {
                                class: LaneRejectionClass::Capacity,
                                peer,
                                local_addr: bulk_addr,
                                expected_class: Some(LaneClass::Bulk),
                                reason: reason.to_string(),
                            });
                            continue;
                        }
                    };
                    spawn_lane_accept(
                        AdmittedLane {
                            read,
                            write,
                            config: server_mux_config(),
                            expected_class: LaneClass::Bulk,
                            peer,
                            local_addr: bulk_addr,
                            permit,
                        },
                        env.clone(),
                        Arc::clone(&registry),
                        rejections.clone(),
                    );
                }
            }
        }
    }
}

async fn finish_frame_delivery_accept(
    accept: io::Result<rtp::udp::FrameDeliveryAccept>,
) -> io::Result<rtp::udp::FrameDeliveryIo> {
    accept?.await.map_err(io::Error::other)?
}

#[derive(Clone)]
struct AcceptEnv {
    handler: StreamHandler,
    response_migration: bool,
}

fn spawn_lane_accept(
    admitted: AdmittedLane,
    env: AcceptEnv,
    registry: Arc<PendingLaneRegistry>,
    rejections: LaneRejectionLog,
) {
    let AdmittedLane {
        mut read,
        mut write,
        config,
        expected_class,
        peer,
        local_addr,
        permit,
    } = admitted;
    tokio::spawn(async move {
        let started = Instant::now();
        let (class, nonce) =
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
                    counter!("stream.rtp_mux.hello_timeout").increment(1);
                    return;
                }
                Ok(Err(error)) => {
                    let elapsed = started.elapsed();
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
                    drop(permit);
                    counter!("stream.rtp_mux.hello_timeout").increment(1);
                    return;
                }
            };
        let elapsed = started.elapsed();
        if class != expected_class {
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
            drop(permit);
            counter!("stream.rtp_mux.class_mismatch").increment(1);
            return;
        }
        let mut permit = Some(permit);
        match registry.admit(nonce, class, peer, local_addr, &mut permit) {
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
                    counter!("stream.rtp_mux.birth_heartbeat_error").increment(1);
                    return;
                }
                let mut tasks = JoinSet::new();
                let (opener, accepter) = spawn_mux_no_reconnection(read, write, config, &mut tasks);
                let lane = PreparedLane {
                    pending: mux::PendingAcceptor::new(class, nonce, opener, accepter, tasks),
                    peer,
                    local_addr,
                };
                let _ = registry.finish_reservation(nonce, lane);
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
                    counter!("stream.rtp_mux.birth_heartbeat_error").increment(1);
                    return;
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
                            pending: mux::PendingAcceptor::new(
                                class, nonce, opener, accepter, tasks,
                            ),
                            peer,
                            local_addr,
                            _permit: permit.take().unwrap(),
                        };
                        pair_lanes_inner(lane, other, env.clone(), nonce);
                    }
                    PendingLaneWait::Timeout => {
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
                        counter!("stream.rtp_mux.pairing_timeout").increment(1);
                    }
                    PendingLaneWait::ReservationLost(reason) => {
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
                        counter!("stream.rtp_mux.pairing_timeout").increment(1);
                    }
                }
            }
            PendingLaneAdmission::Pair {
                lane: other,
                expires_at,
            } => {
                if let Err(failure) = write_birth_heartbeat_result(&mut write).await {
                    registry.restore_ready(nonce, other, expires_at);
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
                    counter!("stream.rtp_mux.birth_heartbeat_error").increment(1);
                    return;
                }
                let mut tasks = JoinSet::new();
                let (opener, accepter) = spawn_mux_no_reconnection(read, write, config, &mut tasks);
                let lane = PendingLane {
                    pending: mux::PendingAcceptor::new(class, nonce, opener, accepter, tasks),
                    peer,
                    local_addr,
                    _permit: permit.take().unwrap(),
                };
                pair_lanes_inner(lane, other, env.clone(), nonce);
            }
            PendingLaneAdmission::Reject(reason) => {
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
                drop(permit);
                counter!("stream.rtp_mux.pairing_timeout").increment(1);
            }
        }
    });
}

async fn write_birth_heartbeat_result(
    writer: &mut FrameWriter,
) -> Result<(), BirthHeartbeatFailure> {
    await_birth_heartbeat(write_birth_heartbeat(writer), HELLO_DEADLINE).await
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
    read: FrameReader,
    mut writer: FrameWriter,
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

async fn signal_rejected_lane(
    writer: &mut FrameWriter,
    rejections: &LaneRejectionLog,
    context: RejectedLaneContext,
    elapsed: Duration,
) {
    let _ = writer.send_kill_and_abort().await;
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

fn pair_lanes_inner(lane_a: PendingLane, lane_b: PendingLane, env: AcceptEnv, nonce: PairingNonce) {
    counter!("stream.rtp_mux.paired").increment(1);
    let addrs = classify_lane_addrs(
        lane_a.pending.class,
        lane_a.peer,
        lane_a.local_addr,
        lane_b.peer,
        lane_b.local_addr,
    );
    pair_lanes(lane_a, lane_b, env, addrs, nonce);
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
                counter!("stream.rtp_mux.pairing_timeout").increment(1);
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
    env: AcceptEnv,
    addrs: DualLaneSocketAddrs,
    nonce: PairingNonce,
) {
    let mut tasks = JoinSet::new();
    match complete_pairing(lane_a.pending, lane_b.pending, &mut tasks) {
        Ok((opener, accepter)) => {
            let addr = SocketAddrPair {
                local_addr: addrs.interactive_local,
                peer_addr: addrs.interactive_peer,
            };
            tokio::spawn(async move {
                let paired_at = Instant::now();
                let accepted_streams = run_dual_mux_accepter(accepter, opener, addr, env).await;
                let error = match tasks.join_next().await {
                    Some(Ok(error)) => error,
                    Some(Err(source)) => MuxError::TaskJoin {
                        task: "dual_lane",
                        source,
                    },
                    None => MuxError::TaskStopped { task: "dual_lane" },
                };
                warn!(
                    event = "rtp_mux_session_terminated",
                    ?error,
                    ?nonce,
                    dn_interactive = ?addrs.interactive_peer,
                    dn_interactive_local = ?addrs.interactive_local,
                    dn_bulk = ?addrs.bulk_peer,
                    dn_bulk_local = ?addrs.bulk_local,
                    accepted_streams,
                    uptime_ms = paired_at.elapsed().as_millis(),
                    "RTP mux dual-lane session terminated"
                );
            });
        }
        Err(error) => {
            warn!(
                ?error,
                ?nonce,
                dn_interactive = ?addrs.interactive_peer,
                dn_interactive_local = ?addrs.interactive_local,
                dn_bulk = ?addrs.bulk_peer,
                dn_bulk_local = ?addrs.bulk_local,
                "RTP mux dual-lane pairing failed"
            );
            counter!("stream.rtp_mux.pairing_timeout").increment(1);
        }
    }
}

async fn run_dual_mux_accepter(
    accepter: mux::DualStreamAccepter,
    opener: mux::DualStreamOpener,
    addr: SocketAddrPair,
    env: AcceptEnv,
) -> u64 {
    let mut accepter = if env.response_migration {
        accepter.into_migrating_duplex(opener)
    } else {
        drop(opener);
        accepter.into_migrating_only()
    };
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
            } => ServerStream::MigratingDuplex {
                reader,
                writer: crate::migrating_write_half::MigratingWriteHalf::new(writer),
                addr,
                source_lane,
            },
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
        (env.handler)(stream);
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
                write_birth_heartbeat(&mut writer),
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
}
