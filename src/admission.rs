use crate::shared::{MAX_PENDING_LANES, MAX_PENDING_LANES_PER_PEER, PAIRING_DEADLINE};
use mux::{GroupToken, LaneClass, PairingNonce};
use rtp::socket::{FrameByteReader, FrameByteWriter, SessionHandle};
use std::{
    collections::{HashMap, hash_map},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, Weak},
    time::Instant,
};
use tokio::sync::{Notify, watch};
pub(crate) struct AdmittedLane {
    pub(crate) read: FrameByteReader,
    pub(crate) write: FrameByteWriter,
    pub(crate) config: mux::MuxConfig,
    pub(crate) expected_class: LaneClass,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) permit: PendingLanePermit,
    /// The accepted lane's RTP session owner. Dropping it aborts the session,
    /// so rejection, timeout, or failed pairing aborts the lane naturally.
    pub(crate) supervisor: SessionHandle,
}
pub(crate) struct PendingLane {
    pub(crate) pending: mux::UnpairedLane,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) group: GroupToken,
    pub(crate) _permit: PendingLanePermit,
    pub(crate) supervisor: SessionHandle,
}
pub(crate) struct PreparedLane {
    pub(crate) pending: mux::UnpairedLane,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) supervisor: SessionHandle,
}
pub(crate) struct PendingLanePermit {
    registry: Weak<PendingLaneRegistry>,
    peer: IpAddr,
}
impl Drop for PendingLanePermit {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut state = registry.state.lock().unwrap();
            state.admitted = state.admitted.saturating_sub(1);
            if let hash_map::Entry::Occupied(mut e) = state.per_peer.entry(self.peer) {
                let count = e.get_mut();
                *count = count.saturating_sub(1);
                if *count == 0 {
                    e.remove();
                }
            }
        }
    }
}
pub(crate) enum PendingLaneAdmission {
    Reserved,
    Wait {
        changed: watch::Receiver<u64>,
        expires_at: Instant,
    },
    Pair {
        lane: PendingLane,
        expires_at: Instant,
    },
    Reject(&'static str),
}
enum PendingLaneEntry {
    Building {
        peer: SocketAddr,
        local_addr: SocketAddr,
        class: LaneClass,
        group: GroupToken,
        expires_at: Instant,
        changed: watch::Sender<u64>,
        permit: PendingLanePermit,
    },
    Ready {
        lane: PendingLane,
        expires_at: Instant,
    },
}
pub(crate) enum ExpiredPendingLane {
    Building {
        nonce: PairingNonce,
        peer: SocketAddr,
        local_addr: SocketAddr,
        class: LaneClass,
        _permit: PendingLanePermit,
    },
    Ready {
        nonce: PairingNonce,
        lane: PendingLane,
    },
}
impl PendingLaneEntry {
    fn peer(&self) -> SocketAddr {
        match self {
            Self::Building { peer, .. } => *peer,
            Self::Ready { lane, .. } => lane.peer,
        }
    }
    fn class(&self) -> LaneClass {
        match self {
            Self::Building { class, .. } => *class,
            Self::Ready { lane, .. } => lane.pending.class,
        }
    }
    fn expires_at(&self) -> Instant {
        match self {
            Self::Building { expires_at, .. } | Self::Ready { expires_at, .. } => *expires_at,
        }
    }
    fn group(&self) -> GroupToken {
        match self {
            Self::Building { group, .. } => *group,
            Self::Ready { lane, .. } => lane.group,
        }
    }
}
#[derive(Default)]
struct PendingLaneRegistryState {
    entries: HashMap<PairingNonce, PendingLaneEntry>,
    admitted: usize,
    per_peer: HashMap<IpAddr, usize>,
}
#[derive(Default)]
pub(crate) struct PendingLaneRegistry {
    state: Mutex<PendingLaneRegistryState>,
    pub(crate) changed: Notify,
}
pub(crate) enum PendingLaneWait {
    Pair(PendingLane),
    Timeout,
    ReservationLost(&'static str),
}
enum PairingPoll {
    Done(PendingLaneWait),
    Wait,
}
impl PendingLaneRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PendingLaneRegistryState::default()),
            changed: Notify::new(),
        })
    }
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        peer: IpAddr,
    ) -> Result<PendingLanePermit, &'static str> {
        let mut state = self.state.lock().unwrap();
        if state.admitted >= MAX_PENDING_LANES {
            return Err("total pending lane capacity exhausted");
        }
        let count = state.per_peer.get(&peer).copied().unwrap_or(0);
        if count >= MAX_PENDING_LANES_PER_PEER {
            return Err("per-peer pending lane capacity exhausted");
        }
        state.admitted += 1;
        state.per_peer.insert(peer, count + 1);
        Ok(PendingLanePermit {
            registry: Arc::downgrade(self),
            peer,
        })
    }
    pub(crate) fn register_admitted(
        &self,
        nonce: PairingNonce,
        class: LaneClass,
        peer: SocketAddr,
        local_addr: SocketAddr,
        group: GroupToken,
        permit: &mut Option<PendingLanePermit>,
    ) -> PendingLaneAdmission {
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.entries.get_mut(&nonce) {
            if entry.peer().ip() != peer.ip() {
                return PendingLaneAdmission::Reject("pairing nonce peer mismatch");
            }
            if entry.class() == class {
                return PendingLaneAdmission::Reject("duplicate lane class for pairing nonce");
            }
            if entry.group() != group {
                return PendingLaneAdmission::Reject("group token mismatch between lanes");
            }
            if let PendingLaneEntry::Building {
                expires_at,
                changed,
                ..
            } = entry
            {
                return PendingLaneAdmission::Wait {
                    changed: changed.subscribe(),
                    expires_at: *expires_at,
                };
            }
            if matches!(
                state.entries.get(&nonce),
                Some(PendingLaneEntry::Ready { .. })
            ) {
                let PendingLaneEntry::Ready { lane, expires_at } =
                    state.entries.remove(&nonce).unwrap()
                else {
                    unreachable!()
                };
                drop(state);
                self.changed.notify_one();
                return PendingLaneAdmission::Pair { lane, expires_at };
            }
            unreachable!()
        }
        let (changed, _) = watch::channel(0);
        state.entries.insert(
            nonce,
            PendingLaneEntry::Building {
                peer,
                local_addr,
                class,
                group,
                expires_at: Instant::now() + PAIRING_DEADLINE,
                changed,
                permit: permit
                    .take()
                    .expect("accepted RTP mux lane must retain its admission permit"),
            },
        );
        drop(state);
        self.changed.notify_one();
        PendingLaneAdmission::Reserved
    }
    pub(crate) fn confirm_reservation(
        &self,
        nonce: PairingNonce,
        lane: PreparedLane,
    ) -> Result<(), Box<PreparedLane>> {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.entries.remove(&nonce) else {
            return Err(Box::new(lane));
        };
        let PendingLaneEntry::Building {
            peer,
            local_addr,
            class,
            group,
            expires_at,
            changed,
            permit,
        } = entry
        else {
            state.entries.insert(nonce, entry);
            return Err(Box::new(lane));
        };
        if peer != lane.peer || class != lane.pending.class {
            state.entries.insert(
                nonce,
                PendingLaneEntry::Building {
                    peer,
                    local_addr,
                    class,
                    group,
                    expires_at,
                    changed,
                    permit,
                },
            );
            return Err(Box::new(lane));
        }
        let lane = PendingLane {
            pending: lane.pending,
            peer: lane.peer,
            local_addr: lane.local_addr,
            group,
            _permit: permit,
            supervisor: lane.supervisor,
        };
        state
            .entries
            .insert(nonce, PendingLaneEntry::Ready { lane, expires_at });
        drop(state);
        changed.send_modify(|generation| *generation = generation.wrapping_add(1));
        self.changed.notify_one();
        Ok(())
    }
    pub(crate) async fn wait_for_pair(
        &self,
        nonce: PairingNonce,
        class: LaneClass,
        peer: SocketAddr,
        expires_at: Instant,
        mut changed: watch::Receiver<u64>,
    ) -> PendingLaneWait {
        loop {
            match self.pair_wait_step(nonce, class, peer, expires_at) {
                PairingPoll::Done(result) => return result,
                PairingPoll::Wait => {}
            }
            if tokio::time::timeout_at(
                tokio::time::Instant::from_std(expires_at),
                changed.changed(),
            )
            .await
            .is_err()
            {
                return match self.pair_wait_step(nonce, class, peer, expires_at) {
                    PairingPoll::Done(result) => result,
                    PairingPoll::Wait => PendingLaneWait::Timeout,
                };
            }
        }
    }
    fn pair_wait_step(
        &self,
        nonce: PairingNonce,
        class: LaneClass,
        peer: SocketAddr,
        expires_at: Instant,
    ) -> PairingPoll {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.entries.get(&nonce) else {
            return PairingPoll::Done(PendingLaneWait::ReservationLost(
                "pairing reservation disappeared",
            ));
        };
        if entry.peer().ip() != peer.ip() {
            return PairingPoll::Done(PendingLaneWait::ReservationLost(
                "pairing nonce peer mismatch",
            ));
        }
        if entry.class() == class {
            return PairingPoll::Done(PendingLaneWait::ReservationLost(
                "duplicate lane class for pairing nonce",
            ));
        }
        if matches!(entry, PendingLaneEntry::Ready { .. }) {
            let PendingLaneEntry::Ready { lane, .. } = state.entries.remove(&nonce).unwrap() else {
                unreachable!()
            };
            drop(state);
            self.changed.notify_one();
            return PairingPoll::Done(PendingLaneWait::Pair(lane));
        }
        if Instant::now() >= expires_at {
            return PairingPoll::Done(PendingLaneWait::Timeout);
        }
        drop(state);
        PairingPoll::Wait
    }
    pub(crate) fn reinsert_ready_lane(
        &self,
        nonce: PairingNonce,
        lane: PendingLane,
        expires_at: Instant,
    ) -> Result<(), Box<PendingLane>> {
        let mut state = self.state.lock().unwrap();
        if state.entries.contains_key(&nonce) {
            drop(state);
            return Err(Box::new(lane));
        }
        state
            .entries
            .insert(nonce, PendingLaneEntry::Ready { lane, expires_at });
        drop(state);
        self.changed.notify_one();
        Ok(())
    }
    pub(crate) fn cancel_reservation(
        &self,
        nonce: PairingNonce,
        peer: SocketAddr,
        class: LaneClass,
    ) {
        let mut state = self.state.lock().unwrap();
        let should_remove = state.entries.get(&nonce).is_some_and(|entry| {
            matches!(entry, PendingLaneEntry::Building { .. })
                && entry.peer() == peer
                && entry.class() == class
        });
        if !should_remove {
            return;
        }
        let entry = state.entries.remove(&nonce).unwrap();
        drop(state);
        drop(entry);
        self.changed.notify_one();
    }
    pub(crate) fn next_expiry(&self) -> Option<Instant> {
        let state = self.state.lock().unwrap();
        state.entries.values().map(|e| e.expires_at()).min()
    }
    pub(crate) fn expire(&self, now: Instant) -> Vec<ExpiredPendingLane> {
        let mut state = self.state.lock().unwrap();
        let expired: Vec<PairingNonce> = state
            .entries
            .iter()
            .filter(|(_, e)| e.expires_at() <= now)
            .map(|(k, _)| *k)
            .collect();
        let mut removed = Vec::new();
        let mut changed = Vec::new();
        for nonce in &expired {
            if let Some(entry) = state.entries.remove(nonce) {
                match entry {
                    PendingLaneEntry::Building {
                        peer,
                        local_addr,
                        class,
                        changed: entry_changed,
                        permit,
                        ..
                    } => {
                        changed.push(entry_changed);
                        removed.push(ExpiredPendingLane::Building {
                            nonce: *nonce,
                            peer,
                            local_addr,
                            class,
                            _permit: permit,
                        });
                    }
                    PendingLaneEntry::Ready { lane, .. } => {
                        removed.push(ExpiredPendingLane::Ready {
                            nonce: *nonce,
                            lane,
                        })
                    }
                }
            }
        }
        drop(state);
        drop(changed);
        removed
    }
}
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn test_pending_lane(
        registry: &Arc<PendingLaneRegistry>,
        nonce: PairingNonce,
        peer: SocketAddr,
        local_addr: SocketAddr,
    ) -> PendingLane {
        let (io, _peer_io) = tokio::io::duplex(64);
        let (read, write) = tokio::io::split(io);
        let mut tasks = tokio::task::JoinSet::new();
        let (opener, accepter) = mux::spawn_mux_no_reconnection(
            read,
            write,
            mux::MuxConfig::new(mux::Initiation::Server, Duration::from_secs(5)),
            &mut tasks,
        );
        let group = GroupToken::generate();
        PendingLane {
            pending: mux::UnpairedLane::new(
                LaneClass::Interactive,
                nonce,
                group,
                opener,
                accepter,
                tasks,
            ),
            peer,
            local_addr,
            group,
            _permit: registry.try_admit(peer.ip()).unwrap(),
            supervisor: SessionHandle::idle(),
        }
    }
    #[tokio::test]
    async fn reinsert_ready_lane_hands_back_the_lane_it_cannot_restore() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let mut permit = Some(registry.try_admit(peer.ip()).unwrap());
        registry.register_admitted(
            nonce,
            LaneClass::Bulk,
            peer,
            local,
            GroupToken::generate(),
            &mut permit,
        );
        let restored = registry.reinsert_ready_lane(
            nonce,
            test_pending_lane(&registry, nonce, peer, local),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(
            restored.is_err(),
            "a lane that could not be restored was dropped inside the registry, so nothing upstream can record that it was lost",
        );
    }

    #[test]
    fn pending_lane_registry_try_admit_within_limits() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permit = registry.try_admit(ip);
        assert!(permit.is_ok());
    }
    #[test]
    fn pending_lane_registry_permit_releases_slot_on_drop() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        {
            let _permit = registry.try_admit(ip).unwrap();
        }
        let permit2 = registry.try_admit(ip);
        assert!(permit2.is_ok());
    }
    #[test]
    fn pending_lane_registry_permit_drop_releases_global_and_per_peer() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permit = registry.try_admit(ip).unwrap();
        {
            let state = registry.state.lock().unwrap();
            assert_eq!(state.admitted, 1);
            assert_eq!(state.per_peer.get(&ip), Some(&1));
        }
        drop(permit);
        {
            let state = registry.state.lock().unwrap();
            assert_eq!(state.admitted, 0);
            assert!(!state.per_peer.contains_key(&ip));
        }
    }
    #[test]
    fn pending_lane_registry_enforces_per_peer_limit() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mut permits = Vec::new();
        for _ in 0..MAX_PENDING_LANES_PER_PEER {
            permits.push(registry.try_admit(ip).unwrap());
        }
        let extra = registry.try_admit(ip);
        assert!(extra.is_err());
    }
    #[test]
    fn pending_lane_registry_duplicate_class_rejection() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        let admission = registry.register_admitted(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut first,
        );
        assert!(matches!(admission, PendingLaneAdmission::Reserved));
        assert!(first.is_none());
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        let admission = registry.register_admitted(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut second,
        );
        assert!(matches!(
            admission,
            PendingLaneAdmission::Reject("duplicate lane class for pairing nonce")
        ));
        assert!(second.is_some());
    }
    #[test]
    fn pending_lane_registry_foreign_peer_rejection() {
        let registry = PendingLaneRegistry::new();
        let peer1: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let peer2: SocketAddr = "192.168.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer1.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer1,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_admit(peer2.ip()).unwrap());
        let admission =
            registry.register_admitted(nonce, LaneClass::Bulk, peer2, local, group, &mut second);
        assert!(matches!(
            admission,
            PendingLaneAdmission::Reject("pairing nonce peer mismatch")
        ));
        assert!(second.is_some());
    }
    #[test]
    fn pending_lane_registry_pairing_is_not_blocked() {
        let registry = PendingLaneRegistry::new();
        let addr: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        assert!(registry.try_admit(addr.ip()).is_ok());
    }
    #[test]
    fn pending_lane_expiry_releases_per_peer_capacity() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permit = registry.try_admit(ip).unwrap();
        assert_eq!(registry.state.lock().unwrap().admitted, 1);
        drop(permit);
        assert_eq!(registry.state.lock().unwrap().admitted, 0);
    }
    #[test]
    fn register_admitted_reject_duplicate_class() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut second
            ),
            PendingLaneAdmission::Reject("duplicate lane class for pairing nonce")
        ));
    }
    #[test]
    fn register_admitted_wait_for_opposite_class_while_building() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        registry.register_admitted(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut first,
        );
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        let admission =
            registry.register_admitted(nonce, LaneClass::Bulk, peer, local, group, &mut second);
        assert!(
            matches!(admission, PendingLaneAdmission::Wait { .. }),
            "opposite class while Building should return Wait"
        );
    }
    #[test]
    fn register_admitted_allows_replacement_waiter_after_first_waiter_is_dropped() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        let PendingLaneAdmission::Wait { changed, .. } =
            registry.register_admitted(nonce, LaneClass::Bulk, peer, local, group, &mut second)
        else {
            panic!("first opposite lane did not enter the pairing wait");
        };
        drop(changed);
        let mut third = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(nonce, LaneClass::Bulk, peer, local, group, &mut third),
            PendingLaneAdmission::Wait { .. }
        ));
    }
    #[tokio::test]
    async fn watch_waiter_observes_cancelled_reservation_without_readmitting() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        let (changed, expires_at) = match registry.register_admitted(
            nonce,
            LaneClass::Bulk,
            peer,
            local,
            group,
            &mut second,
        ) {
            PendingLaneAdmission::Wait {
                changed,
                expires_at,
            } => (changed, expires_at),
            _ => panic!("opposite lane did not enter the pairing wait"),
        };
        let waiting_registry = Arc::clone(&registry);
        let mut waiter_tasks = tokio::task::JoinSet::new();
        waiter_tasks.spawn(async move {
            matches!(
                waiting_registry
                    .wait_for_pair(nonce, LaneClass::Bulk, peer, expires_at, changed)
                    .await,
                PendingLaneWait::ReservationLost(_)
            )
        });
        registry.cancel_reservation(nonce, peer, LaneClass::Interactive);
        assert!(waiter_tasks.join_next().await.unwrap().unwrap());
        assert!(second.is_some());
    }
    #[tokio::test]
    async fn watch_waiter_honors_pairing_deadline() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        let changed = match registry.register_admitted(
            nonce,
            LaneClass::Bulk,
            peer,
            local,
            group,
            &mut second,
        ) {
            PendingLaneAdmission::Wait { changed, .. } => changed,
            _ => panic!("opposite lane did not enter the pairing wait"),
        };
        assert!(matches!(
            registry
                .wait_for_pair(
                    nonce,
                    LaneClass::Bulk,
                    peer,
                    Instant::now() + Duration::from_millis(10),
                    changed
                )
                .await,
            PendingLaneWait::Timeout
        ));
    }
    #[test]
    fn watch_notification_is_isolated_to_its_pairing_nonce() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let first_nonce = PairingNonce::generate();
        let second_nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                first_nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut first_waiter = Some(registry.try_admit(peer.ip()).unwrap());
        let first_changed = match registry.register_admitted(
            first_nonce,
            LaneClass::Bulk,
            peer,
            local,
            group,
            &mut first_waiter,
        ) {
            PendingLaneAdmission::Wait { changed, .. } => changed,
            _ => panic!("first nonce did not create a waiter"),
        };
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                second_nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut second
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second_waiter = Some(registry.try_admit(peer.ip()).unwrap());
        let second_changed = match registry.register_admitted(
            second_nonce,
            LaneClass::Bulk,
            peer,
            local,
            group,
            &mut second_waiter,
        ) {
            PendingLaneAdmission::Wait { changed, .. } => changed,
            _ => panic!("second nonce did not create a waiter"),
        };
        registry.cancel_reservation(first_nonce, peer, LaneClass::Interactive);
        assert!(first_changed.has_changed().is_err());
        assert!(matches!(second_changed.has_changed(), Ok(false)));
    }
    #[test]
    #[should_panic(expected = "accepted RTP mux lane must retain its admission permit")]
    fn register_admitted_reject_no_permit() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut permit: Option<PendingLanePermit> = None;
        let _ = registry.register_admitted(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut permit,
        );
    }
    #[test]
    fn register_admitted_reject_foreign_peer() {
        let registry = PendingLaneRegistry::new();
        let peer1: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let peer2: SocketAddr = "192.168.1.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer1.ip()).unwrap());
        registry.register_admitted(
            nonce,
            LaneClass::Interactive,
            peer1,
            local,
            group,
            &mut first,
        );
        let mut second = Some(registry.try_admit(peer2.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(nonce, LaneClass::Bulk, peer2, local, group, &mut second),
            PendingLaneAdmission::Reject("pairing nonce peer mismatch")
        ));
    }
    #[test]
    fn ready_lane_rejects_foreign_peer_and_duplicate_class() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let foreign_peer: SocketAddr = "192.168.1.1:1000".parse().unwrap();
        let mut foreign = Some(registry.try_admit(foreign_peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Bulk,
                foreign_peer,
                local,
                group,
                &mut foreign
            ),
            PendingLaneAdmission::Reject("pairing nonce peer mismatch")
        ));
        let same_peer_ip: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let mut duplicate = Some(registry.try_admit(same_peer_ip.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                same_peer_ip,
                local,
                group,
                &mut duplicate
            ),
            PendingLaneAdmission::Reject("duplicate lane class for pairing nonce")
        ));
    }
    #[test]
    fn ready_lane_pairs_opposite_class() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        registry.register_admitted(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut first,
        );
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(nonce, LaneClass::Bulk, peer, local, group, &mut second),
            PendingLaneAdmission::Wait { .. }
        ));
    }
    #[test]
    fn register_admitted_reject_group_token_mismatch_between_lanes() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let other_group = GroupToken::generate();
        let mut second = Some(registry.try_admit(peer.ip()).unwrap());
        assert!(matches!(
            registry.register_admitted(
                nonce,
                LaneClass::Bulk,
                peer,
                local,
                other_group,
                &mut second
            ),
            PendingLaneAdmission::Reject("group token mismatch between lanes")
        ));
        assert!(second.is_some());
    }
}
