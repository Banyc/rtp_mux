use crate::shared::{MAX_PENDING_LANES, MAX_PENDING_LANES_PER_PEER, PAIRING_DEADLINE};
use mux::{GroupToken, LaneClass, PairingNonce};
use rtp::socket::{FrameReader, FrameWriter};
use std::{
    collections::{HashMap, hash_map},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, Weak},
    time::Instant,
};
use tokio::sync::{Notify, watch};
use tracing::warn;
pub(crate) struct AdmittedLane {
    pub(crate) read: FrameReader,
    pub(crate) write: FrameWriter,
    pub(crate) config: mux::MuxConfig,
    pub(crate) expected_class: LaneClass,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) permit: PendingLanePermit,
}
pub(crate) struct PendingLane {
    pub(crate) pending: mux::PendingAcceptor,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) group: GroupToken,
    pub(crate) _permit: PendingLanePermit,
}
pub(crate) struct PreparedLane {
    pub(crate) pending: mux::PendingAcceptor,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
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
enum PendingLaneWaitStep {
    Done(PendingLaneWait),
    Wait,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LaneRejectionClass {
    Capacity,
    HelloTimeout,
    HelloParse,
    ClassMismatch,
    Admission,
    GroupFull,
    PairingTimeout,
    BirthHeartbeat,
    ReservationLost,
}
#[derive(Debug, Clone)]
pub(crate) struct RejectedLaneContext {
    pub(crate) class: LaneRejectionClass,
    pub(crate) peer: SocketAddr,
    pub(crate) local_addr: SocketAddr,
    pub(crate) expected_class: Option<LaneClass>,
    pub(crate) reason: String,
}
#[derive(Debug, Default)]
struct LaneRejectionSummary {
    total: u64,
    by_class: HashMap<LaneRejectionClass, u64>,
    first: Option<RejectedLaneContext>,
    last: Option<RejectedLaneContext>,
}
#[derive(Debug, Default)]
struct LaneRejectionLogInner {
    summary: Mutex<LaneRejectionSummary>,
}
#[derive(Debug, Clone, Default)]
pub(crate) struct LaneRejectionLog {
    inner: Arc<LaneRejectionLogInner>,
}
impl LaneRejectionLog {
    pub(crate) fn record(&self, context: RejectedLaneContext) {
        let mut summary = self.inner.summary.lock().unwrap();
        summary.total = summary.total.saturating_add(1);
        *summary.by_class.entry(context.class).or_default() = summary
            .by_class
            .get(&context.class)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        summary.first.get_or_insert_with(|| context.clone());
        summary.last = Some(context);
    }
    pub(crate) fn flush(&self) {
        let summary = {
            let mut summary = self.inner.summary.lock().unwrap();
            if summary.total == 0 {
                return;
            }
            std::mem::take(&mut *summary)
        };
        let first = summary.first.unwrap();
        let last = summary.last.unwrap();
        warn!(event = "rtp_mux_lane_rejected", rejected = summary.total, rejection_classes = ?summary.by_class, first_class = ?first.class, first_dn = ?first.peer, first_dn_local = ?first.local_addr, first_expected_class = ?first.expected_class, first_reason = %first.reason, last_class = ?last.class, last_dn = ?last.peer, last_dn_local = ?last.local_addr, last_expected_class = ?last.expected_class, last_reason = %last.reason, "Rejected RTP mux lanes");
    }
}
impl PendingLaneRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PendingLaneRegistryState::default()),
            changed: Notify::new(),
        })
    }
    pub(crate) fn try_acquire(
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
    pub(crate) fn admit(
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
    pub(crate) fn finish_reservation(
        &self,
        nonce: PairingNonce,
        lane: PreparedLane,
    ) -> Result<(), PreparedLane> {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.entries.remove(&nonce) else {
            return Err(lane);
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
            return Err(lane);
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
            return Err(lane);
        }
        let lane = PendingLane {
            pending: lane.pending,
            peer: lane.peer,
            local_addr: lane.local_addr,
            group,
            _permit: permit,
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
                PendingLaneWaitStep::Done(result) => return result,
                PendingLaneWaitStep::Wait => {}
            }
            if tokio::time::timeout_at(
                tokio::time::Instant::from_std(expires_at),
                changed.changed(),
            )
            .await
            .is_err()
            {
                return match self.pair_wait_step(nonce, class, peer, expires_at) {
                    PendingLaneWaitStep::Done(result) => result,
                    PendingLaneWaitStep::Wait => PendingLaneWait::Timeout,
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
    ) -> PendingLaneWaitStep {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.entries.get(&nonce) else {
            return PendingLaneWaitStep::Done(PendingLaneWait::ReservationLost(
                "pairing reservation disappeared",
            ));
        };
        if entry.peer().ip() != peer.ip() {
            return PendingLaneWaitStep::Done(PendingLaneWait::ReservationLost(
                "pairing nonce peer mismatch",
            ));
        }
        if entry.class() == class {
            return PendingLaneWaitStep::Done(PendingLaneWait::ReservationLost(
                "duplicate lane class for pairing nonce",
            ));
        }
        if matches!(entry, PendingLaneEntry::Ready { .. }) {
            let PendingLaneEntry::Ready { lane, .. } = state.entries.remove(&nonce).unwrap() else {
                unreachable!()
            };
            drop(state);
            self.changed.notify_one();
            return PendingLaneWaitStep::Done(PendingLaneWait::Pair(lane));
        }
        if Instant::now() >= expires_at {
            return PendingLaneWaitStep::Done(PendingLaneWait::Timeout);
        }
        drop(state);
        PendingLaneWaitStep::Wait
    }
    pub(crate) fn restore_ready(
        &self,
        nonce: PairingNonce,
        lane: PendingLane,
        expires_at: Instant,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.entries.contains_key(&nonce) {
            drop(state);
            drop(lane);
            return;
        }
        state
            .entries
            .insert(nonce, PendingLaneEntry::Ready { lane, expires_at });
        drop(state);
        self.changed.notify_one();
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
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    #[test]
    fn pending_lane_registry_try_acquire_within_limits() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permit = registry.try_acquire(ip);
        assert!(permit.is_ok());
    }
    #[test]
    fn pending_lane_registry_permit_releases_slot_on_drop() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        {
            let _permit = registry.try_acquire(ip).unwrap();
        }
        let permit2 = registry.try_acquire(ip);
        assert!(permit2.is_ok());
    }
    #[test]
    fn pending_lane_registry_permit_drop_releases_global_and_per_peer() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permit = registry.try_acquire(ip).unwrap();
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
            permits.push(registry.try_acquire(ip).unwrap());
        }
        let extra = registry.try_acquire(ip);
        assert!(extra.is_err());
    }
    #[test]
    fn pending_lane_registry_duplicate_class_rejection() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        let admission = registry.admit(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut first,
        );
        assert!(matches!(admission, PendingLaneAdmission::Reserved));
        assert!(first.is_none());
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        let admission = registry.admit(
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
        let mut first = Some(registry.try_acquire(peer1.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                nonce,
                LaneClass::Interactive,
                peer1,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_acquire(peer2.ip()).unwrap());
        let admission = registry.admit(nonce, LaneClass::Bulk, peer2, local, group, &mut second);
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
        assert!(registry.try_acquire(addr.ip()).is_ok());
    }
    #[test]
    fn lane_rejection_log_aggregates_across_classes_peers_and_lanes() {
        let log = LaneRejectionLog::default();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        log.record(RejectedLaneContext {
            class: LaneRejectionClass::HelloTimeout,
            peer,
            local_addr: local,
            expected_class: Some(LaneClass::Interactive),
            reason: "test".to_string(),
        });
        log.record(RejectedLaneContext {
            class: LaneRejectionClass::HelloTimeout,
            peer,
            local_addr: local,
            expected_class: Some(LaneClass::Interactive),
            reason: "test".to_string(),
        });
        log.record(RejectedLaneContext {
            class: LaneRejectionClass::HelloParse,
            peer,
            local_addr: local,
            expected_class: Some(LaneClass::Bulk),
            reason: "test".to_string(),
        });
        let summary = log.inner.summary.lock().unwrap();
        assert_eq!(summary.total, 3);
        assert_eq!(
            summary.by_class.get(&LaneRejectionClass::HelloTimeout),
            Some(&2)
        );
        assert_eq!(
            summary.by_class.get(&LaneRejectionClass::HelloParse),
            Some(&1)
        );
    }
    #[test]
    fn pending_lane_expiry_releases_per_peer_capacity() {
        let registry = PendingLaneRegistry::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permit = registry.try_acquire(ip).unwrap();
        assert_eq!(registry.state.lock().unwrap().admitted, 1);
        drop(permit);
        assert_eq!(registry.state.lock().unwrap().admitted, 0);
    }
    #[test]
    fn admit_reject_duplicate_class() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
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
    fn admit_wait_for_opposite_class_while_building() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        registry.admit(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut first,
        );
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        let admission = registry.admit(nonce, LaneClass::Bulk, peer, local, group, &mut second);
        assert!(
            matches!(admission, PendingLaneAdmission::Wait { .. }),
            "opposite class while Building should return Wait"
        );
    }
    #[test]
    fn admit_allows_replacement_waiter_after_first_waiter_is_dropped() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        let PendingLaneAdmission::Wait { changed, .. } =
            registry.admit(nonce, LaneClass::Bulk, peer, local, group, &mut second)
        else {
            panic!("first opposite lane did not enter the pairing wait");
        };
        drop(changed);
        let mut third = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(nonce, LaneClass::Bulk, peer, local, group, &mut third),
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
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        let (changed, expires_at) =
            match registry.admit(nonce, LaneClass::Bulk, peer, local, group, &mut second) {
                PendingLaneAdmission::Wait {
                    changed,
                    expires_at,
                } => (changed, expires_at),
                _ => panic!("opposite lane did not enter the pairing wait"),
            };
        let waiting_registry = Arc::clone(&registry);
        let waiter = tokio::spawn(async move {
            matches!(
                waiting_registry
                    .wait_for_pair(nonce, LaneClass::Bulk, peer, expires_at, changed)
                    .await,
                PendingLaneWait::ReservationLost(_)
            )
        });
        registry.cancel_reservation(nonce, peer, LaneClass::Interactive);
        assert!(waiter.await.unwrap());
        assert!(second.is_some());
    }
    #[tokio::test]
    async fn watch_waiter_honors_pairing_deadline() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        let changed = match registry.admit(nonce, LaneClass::Bulk, peer, local, group, &mut second)
        {
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
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                first_nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut first
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut first_waiter = Some(registry.try_acquire(peer.ip()).unwrap());
        let first_changed = match registry.admit(
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
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
                second_nonce,
                LaneClass::Interactive,
                peer,
                local,
                group,
                &mut second
            ),
            PendingLaneAdmission::Reserved
        ));
        let mut second_waiter = Some(registry.try_acquire(peer.ip()).unwrap());
        let second_changed = match registry.admit(
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
    fn admit_reject_no_permit() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut permit: Option<PendingLanePermit> = None;
        let _ = registry.admit(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut permit,
        );
    }
    #[test]
    fn admit_reject_foreign_peer() {
        let registry = PendingLaneRegistry::new();
        let peer1: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let peer2: SocketAddr = "192.168.1.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer1.ip()).unwrap());
        registry.admit(
            nonce,
            LaneClass::Interactive,
            peer1,
            local,
            group,
            &mut first,
        );
        let mut second = Some(registry.try_acquire(peer2.ip()).unwrap());
        assert!(matches!(
            registry.admit(nonce, LaneClass::Bulk, peer2, local, group, &mut second),
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
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
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
        let mut foreign = Some(registry.try_acquire(foreign_peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
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
        let mut duplicate = Some(registry.try_acquire(same_peer_ip.ip()).unwrap());
        assert!(matches!(
            registry.admit(
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
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        registry.admit(
            nonce,
            LaneClass::Interactive,
            peer,
            local,
            group,
            &mut first,
        );
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(nonce, LaneClass::Bulk, peer, local, group, &mut second),
            PendingLaneAdmission::Wait { .. }
        ));
    }
    #[test]
    fn admit_reject_group_token_mismatch_between_lanes() {
        let registry = PendingLaneRegistry::new();
        let peer: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let nonce = PairingNonce::generate();
        let group = GroupToken::generate();
        let mut first = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
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
        let mut second = Some(registry.try_acquire(peer.ip()).unwrap());
        assert!(matches!(
            registry.admit(
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
