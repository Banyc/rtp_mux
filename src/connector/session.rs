use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use metrics::counter;
use mux::{LaneClass, PairingNonce};
use tracing::trace;

use crate::{
    explorer::{Explorer, SocketCandidate},
    migrating_write_half::RebindHandle,
    stream::SocketAddrPair,
    traffic::{SessionStats, SessionTraffic},
};

use super::{AddrGroup, OpenedStream};

pub(crate) struct Session {
    pub(crate) id: u64,
    pub(crate) opener: mux::DualStreamOpener,
    pub(crate) addr: SocketAddrPair,
    pub(crate) nonce: PairingNonce,
    pub(crate) connected_at: Instant,
    pub(crate) opened_streams: AtomicU64,
    pub(crate) live_streams: AtomicU64,
    pub(crate) traffic: Arc<SessionTraffic>,
    pub(crate) router: mux::ResponseRouterHandle,
    pub(crate) kill_tx: tokio::sync::mpsc::Sender<()>,
    pub(crate) streams: Mutex<Vec<std::sync::Weak<StreamRebind>>>,
    pub(crate) successor: Mutex<Option<Arc<Session>>>,
}

impl Session {
    pub(crate) fn open_stream(self: &Arc<Self>, lane: LaneClass) -> OpenedStream {
        let stream_id = rand::random::<u64>();
        let (writer, reader) = self.opener.open_migrating_with_reader(stream_id, lane);
        self.opened_streams.fetch_add(1, Ordering::Relaxed);
        counter!("stream.rtp_mux.rtp.connects").increment(1);
        counter!("stream.rtp_mux.mux.connects").increment(1);
        OpenedStream {
            writer,
            reader,
            addr: self.addr,
            response: (stream_id, self.router.clone()),
            guard: self.guard(),
        }
    }
    fn guard(self: &Arc<Self>) -> SessionGuard {
        self.live_streams.fetch_add(1, Ordering::Relaxed);
        SessionGuard(Arc::clone(self))
    }
    fn track(&self, stream: std::sync::Weak<StreamRebind>) {
        let mut streams = self.streams.lock().unwrap();
        streams.retain(|weak| weak.strong_count() > 0);
        streams.push(stream);
    }
    fn track_if_current(&self, stream: std::sync::Weak<StreamRebind>) -> bool {
        let mut streams = self.streams.lock().unwrap();
        if self.successor.lock().unwrap().is_some() {
            return false;
        }
        streams.retain(|weak| weak.strong_count() > 0);
        streams.push(stream);
        true
    }
    pub(crate) fn kill(&self) {
        let _ = self.kill_tx.try_send(());
    }
    pub(crate) fn stats(&self) -> SessionStats {
        SessionStats {
            live_streams: self.live_streams.load(Ordering::Relaxed),
            opened_streams: self.opened_streams.load(Ordering::Relaxed),
            tx_bytes: self.traffic.tx_bytes(),
            rx_bytes: self.traffic.rx_bytes(),
            uptime: self.connected_at.elapsed(),
        }
    }
}

pub(crate) struct SessionGuard(Arc<Session>);

impl SessionGuard {
    fn session(&self) -> &Arc<Session> {
        &self.0
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.0.live_streams.fetch_sub(1, Ordering::Relaxed);
    }
}

impl fmt::Debug for SessionGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionGuard")
    }
}

pub(crate) struct StreamRebind {
    rebind: RebindHandle,
    guard: Mutex<SessionGuard>,
}

impl StreamRebind {
    pub(crate) fn track(rebind: RebindHandle, guard: SessionGuard) -> Arc<Self> {
        let session = Arc::clone(guard.session());
        let handle = Arc::new(Self {
            rebind,
            guard: Mutex::new(guard),
        });
        if !session.track_if_current(Arc::downgrade(&handle)) {
            let live = latest_generation(&session);
            match Arc::ptr_eq(&live, &session) {
                true => session.track(Arc::downgrade(&handle)),
                false => {
                    rebind_one(&handle, &live);
                }
            }
        }
        handle
    }
}

fn latest_generation(session: &Arc<Session>) -> Arc<Session> {
    let mut newest_live = Arc::clone(session);
    let mut cursor = Arc::clone(session);
    for _ in 0..MAX_SUCCESSOR_HOPS {
        let Some(next) = cursor.successor.lock().unwrap().clone() else {
            break;
        };
        cursor = next;
        if cursor.opener.is_alive() {
            newest_live = Arc::clone(&cursor);
        }
    }
    newest_live
}
const MAX_SUCCESSOR_HOPS: usize = 8;

impl fmt::Debug for StreamRebind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StreamRebind")
    }
}

pub(crate) type SharedSessions = Arc<Mutex<HashMap<SocketAddr, Arc<Session>>>>;
pub(crate) type SharedDraining = Arc<Mutex<HashMap<SocketAddr, std::sync::Weak<Session>>>>;

pub(crate) fn live_session(sessions: &SharedSessions, addr: SocketAddr) -> Option<Arc<Session>> {
    let session = sessions.lock().unwrap().get(&addr).cloned()?;
    session.opener.is_alive().then_some(session)
}

pub(crate) fn prune_dead_addresses(
    groups: &mut HashMap<SocketAddr, AddrGroup>,
    explorers: &mut HashMap<SocketAddr, Explorer<SocketCandidate>>,
    in_flight_dials: &HashSet<SocketAddr>,
) {
    groups.retain(|addr, group| {
        group.sessions.retain(|weak| weak.strong_count() > 0);
        in_flight_dials.contains(addr) || !group.sessions.is_empty()
    });
    explorers.retain(|addr, _| groups.contains_key(addr));
}

fn rebind_one(stream: &Arc<StreamRebind>, new: &Arc<Session>) -> bool {
    if !stream.rebind.rebind(new.opener.clone()) {
        return false;
    }
    *stream.guard.lock().unwrap() = new.guard();
    new.track(Arc::downgrade(stream));
    true
}

pub(crate) fn rebind_streams(old: &Session, new: &Arc<Session>) -> usize {
    if !new.opener.is_alive() {
        return 0;
    }
    let handles: Vec<_> = {
        let mut streams = old.streams.lock().unwrap();
        let handles: Vec<_> = streams.drain(..).collect();
        *old.successor.lock().unwrap() = Some(Arc::clone(new));
        handles
    };
    let mut moved = 0;
    for weak in handles {
        let Some(stream) = weak.upgrade() else {
            continue;
        };
        if !rebind_one(&stream, new) {
            trace!(up = ?old.addr.peer_addr, "RTP mux stream writer gone before rebind");
            continue;
        }
        moved += 1;
    }
    moved
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::install_session;
    use crate::connector::tests::{fake_connected_birth, fake_dead_birth, one_address_group};
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn a_recycle_onto_a_dead_session_leaves_streams_where_they_are() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let mut groups = one_address_group(addr);
        let mut supervisors = JoinSet::new();
        let old = install_session(
            addr,
            fake_connected_birth(addr, None),
            &mut groups,
            &mut supervisors,
        );
        let dead = install_session(addr, fake_dead_birth().await, &mut groups, &mut supervisors);
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        let _stream = StreamRebind::track(slot.handle(), old.guard());
        assert_eq!(old.live_streams.load(Ordering::Relaxed), 1);
        assert_eq!(
            rebind_streams(&old, &dead),
            0,
            "no stream may be moved onto a session that is already down"
        );
        assert!(
            slot.take().is_none(),
            "a live stream was repointed at a dead session"
        );
        assert_eq!(old.live_streams.load(Ordering::Relaxed), 1);
        assert!(
            old.successor.lock().unwrap().is_none(),
            "a dead session must not be published as the successor",
        );
        let live = install_session(
            addr,
            fake_connected_birth(addr, None),
            &mut groups,
            &mut supervisors,
        );
        assert_eq!(rebind_streams(&old, &live), 1);
        assert!(slot.take().is_some());
    }

    #[tokio::test]
    async fn a_late_tracked_stream_skips_a_successor_that_has_died() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let mut groups = one_address_group(addr);
        let mut supervisors = JoinSet::new();
        let old = install_session(
            addr,
            fake_connected_birth(addr, None),
            &mut groups,
            &mut supervisors,
        );
        let dead = install_session(addr, fake_dead_birth().await, &mut groups, &mut supervisors);
        *old.successor.lock().unwrap() = Some(Arc::clone(&dead));
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        let _stream = StreamRebind::track(slot.handle(), old.guard());
        assert!(
            slot.take().is_none(),
            "the stream was repointed at a dead successor",
        );
        assert_eq!(
            old.live_streams.load(Ordering::Relaxed),
            1,
            "the stream must stay on the generation that is still reachable",
        );
        assert_eq!(dead.live_streams.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_stream_registering_during_the_drain_is_not_stranded() {
        let addr: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let mut groups = one_address_group(addr);
        let mut supervisors = JoinSet::new();
        let old = install_session(
            addr,
            fake_connected_birth(addr, None),
            &mut groups,
            &mut supervisors,
        );
        let new = install_session(
            addr,
            fake_connected_birth(addr, None),
            &mut groups,
            &mut supervisors,
        );
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        let held = old.streams.lock().unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let registering = {
            let old = Arc::clone(&old);
            let handle = slot.handle();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                StreamRebind::track(handle, old.guard())
            })
        };
        barrier.wait();
        *old.successor.lock().unwrap() = Some(Arc::clone(&new));
        drop(held);
        let _stream = registering.join().unwrap();
        assert!(
            slot.take().is_some(),
            "a stream that registered during the drain was left on the drained generation",
        );
        assert_eq!(new.live_streams.load(Ordering::Relaxed), 1);
        assert_eq!(old.live_streams.load(Ordering::Relaxed), 0);
    }
}
