use mux::{DualStreamOpener, GroupToken};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone)]
pub(crate) struct GroupDriverSubmitter(tokio::sync::mpsc::Sender<tokio::task::JoinSet<()>>);

/// Why a session pair could not join its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupJoinError {
    /// The group already holds its full complement of members.
    GroupFull,
    /// The bounded group-driver submission channel is at capacity; the
    /// server's driver-drain reaper has not caught up.
    DriverQueueFull,
    /// The group-driver submission channel is closed; the server's driver
    /// scope has shut down.
    DriverScopeClosed,
}

impl fmt::Display for GroupJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupJoinError::GroupFull => write!(f, "session group is full"),
            GroupJoinError::DriverQueueFull => {
                write!(f, "group-driver submission queue is full")
            }
            GroupJoinError::DriverScopeClosed => write!(f, "group-driver scope is closed"),
        }
    }
}

impl GroupDriverSubmitter {
    fn try_submit(&self, driver: tokio::task::JoinSet<()>) -> Result<(), GroupJoinError> {
        match self.0.try_send(driver) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(GroupJoinError::DriverQueueFull)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(GroupJoinError::DriverScopeClosed)
            }
        }
    }
}

/// Server-side half of the group-driver submission channel: the bounded
/// receiver of submitted driver `JoinSet`s plus a `JoinSet` of drain futures
/// (one per submitted driver). [`crate::server::RtpMuxServer::serve`] owns
/// this scope and drives both: it receives submissions and spawns each
/// driver's drain into `drivers`, and it reaps `drivers` concurrently, so a
/// long-lived driver can never block later submissions from being drained
/// (and the bounded channel from being freed).
pub(crate) struct GroupDriverScope {
    pub(crate) submissions: tokio::sync::mpsc::Receiver<tokio::task::JoinSet<()>>,
    pub(crate) drivers: tokio::task::JoinSet<()>,
}

impl GroupDriverScope {
    /// Spawn one submitted driver's drain into the drain set. The
    /// drain unwraps every completion, so a child panic surfaces as a panic
    /// of the drain task (observed by [`GroupDriverScope::reap_driver`] / the
    /// server's join boundary) instead of being swallowed. The caller keeps
    /// driving [`GroupDriverScope::reap_driver`] concurrently, so a long-lived
    /// driver can never block later submissions from being drained (and the
    /// bounded submission channel from being freed).
    #[cfg(test)]
    pub(crate) fn submit_driver(&mut self, driver: tokio::task::JoinSet<()>) {
        Self::submit_driver_into(&mut self.drivers, driver);
    }

    /// Wait for one submitted driver's drain to complete and re-raise any
    /// panic it surfaced. Returns `None` when no drain is running (an empty
    /// drain set), which `serve`'s select loop treats as a skipped arm.
    #[cfg(test)]
    pub(crate) async fn reap_driver(&mut self) -> Option<()> {
        Self::reap_driver_from(&mut self.drivers).await
    }

    /// Field-level variant of [`GroupDriverScope::submit_driver`] so `serve`
    /// can drive the submission receiver and the drain set from disjoint
    /// fields inside one `select!` loop.
    pub(crate) fn submit_driver_into(
        drivers: &mut tokio::task::JoinSet<()>,
        mut driver: tokio::task::JoinSet<()>,
    ) {
        drivers.spawn(async move {
            while let Some(result) = driver.join_next().await {
                result.unwrap();
            }
        });
    }

    /// Field-level variant of [`GroupDriverScope::reap_driver`].
    pub(crate) async fn reap_driver_from(drivers: &mut tokio::task::JoinSet<()>) -> Option<()> {
        let joined = drivers.join_next().await?;
        joined.unwrap();
        Some(())
    }
}

pub(crate) fn group_driver_scope(bound: usize) -> (GroupDriverSubmitter, GroupDriverScope) {
    let (tx, submissions) = tokio::sync::mpsc::channel::<tokio::task::JoinSet<()>>(bound);
    let scope = GroupDriverScope {
        submissions,
        drivers: tokio::task::JoinSet::new(),
    };
    (GroupDriverSubmitter(tx), scope)
}

pub(crate) struct SessionPairRegistry {
    groups: Mutex<HashMap<GroupToken, Weak<SessionPair>>>,
    group_drivers: GroupDriverSubmitter,
}
pub(crate) struct SessionPair {
    feed: mux::SpliceRouterHandle,
    state: Mutex<PairState>,
}
struct PairState {
    members: usize,
    next_seq: u64,
    newest_seq: u64,
    newest_opener: Option<DualStreamOpener>,
    writers: Vec<crate::migrating_write_half::RebindHandle>,
    next_purge: usize,
}
pub(crate) struct PairMember {
    group: Arc<SessionPair>,
    seq: u64,
}
impl Drop for PairMember {
    fn drop(&mut self) {
        self.group.state.lock().unwrap().members -= 1;
    }
}

impl SessionPairRegistry {
    pub(crate) fn new(group_drivers: GroupDriverSubmitter) -> Arc<Self> {
        Arc::new(Self {
            groups: Mutex::new(HashMap::new()),
            group_drivers,
        })
    }
    pub(crate) fn is_full(&self, token: &GroupToken) -> bool {
        let groups = self.groups.lock().unwrap();
        groups
            .get(token)
            .and_then(Weak::upgrade)
            .is_some_and(|group| group.state.lock().unwrap().members >= 2)
    }
    pub(crate) fn join(
        &self,
        token: GroupToken,
        opener: DualStreamOpener,
    ) -> Result<PairMember, GroupJoinError> {
        let group = {
            let mut groups = self.groups.lock().unwrap();
            groups.retain(|_, weak| weak.strong_count() > 0);
            match groups.get(&token).and_then(Weak::upgrade) {
                Some(group) => group,
                None => {
                    let (feed, driver) = mux::spawn_splice_router();
                    self.group_drivers.try_submit(driver)?;
                    let group = Arc::new(SessionPair {
                        feed,
                        state: Mutex::new(PairState {
                            members: 0,
                            next_seq: 0,
                            newest_seq: 0,
                            newest_opener: None,
                            writers: Vec::new(),
                            next_purge: 64,
                        }),
                    });
                    groups.insert(token, Arc::downgrade(&group));
                    group
                }
            }
        };
        let mut state = group.state.lock().unwrap();
        if state.members >= 2 {
            return Err(GroupJoinError::GroupFull);
        }
        state.members += 1;
        state.next_seq += 1;
        let seq = state.next_seq;
        state.newest_seq = seq;
        state.newest_opener = Some(opener.clone());
        state
            .writers
            .retain(crate::migrating_write_half::RebindHandle::is_alive);
        let waiting = state.writers.len();
        let joiner_alive = opener.is_alive();
        if joiner_alive {
            for writer in &state.writers {
                writer.rebind(opener.clone());
            }
        }
        drop(state);
        if waiting != 0 {
            tracing::info!(
                seq,
                streams = waiting,
                joiner_alive,
                "RTP mux group rebind; a newer session joined the group",
            );
        }
        Ok(PairMember { group, seq })
    }
}

impl PairMember {
    pub(crate) fn feed(&self) -> mux::SpliceRouterHandle {
        self.group.feed.clone()
    }
    pub(crate) fn register_writer(&self, writer: crate::migrating_write_half::RebindHandle) {
        let mut state = self.group.state.lock().unwrap();
        if state.newest_seq != self.seq {
            let target = state
                .newest_opener
                .as_ref()
                .filter(|opener| opener.is_alive());
            if let Some(opener) = target {
                writer.rebind(opener.clone());
            }
            tracing::debug!(
                seq = self.seq,
                newest_seq = state.newest_seq,
                rebound = target.is_some(),
                "RTP mux group rebind; a stream opened on a stale member"
            );
        }
        if state.writers.len() >= state.next_purge {
            state
                .writers
                .retain(crate::migrating_write_half::RebindHandle::is_alive);
            state.next_purge = (state.writers.len() * 2).max(64);
        }
        state.writers.push(writer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mux::spawn_mux_no_reconnection;
    use tokio::task::JoinSet;

    fn registry_with_scope(bound: usize) -> (Arc<SessionPairRegistry>, GroupDriverScope) {
        let (group_drivers, scope) = group_driver_scope(bound);
        (SessionPairRegistry::new(group_drivers), scope)
    }

    fn test_opener(tasks: &mut JoinSet<mux::MuxError>) -> DualStreamOpener {
        let lane = || {
            let (a, peer) = tokio::io::duplex(4096);
            let (r, w) = tokio::io::split(a);
            let mut spawner = JoinSet::new();
            let (opener, accepter) =
                spawn_mux_no_reconnection(r, w, crate::shared::server_mux_config(), &mut spawner);
            (opener, accepter, spawner, peer)
        };
        let (int_op, int_acc, int_s, int_peer) = lane();
        let (bulk_op, bulk_acc, bulk_s, bulk_peer) = lane();
        let (opener, _accepter) = mux::spawn_dual_mux_paired_supervised(
            int_op, int_acc, int_s, bulk_op, bulk_acc, bulk_s, tasks,
        );
        tasks.spawn(async move {
            let _keep_peers_alive = (int_peer, bulk_peer);
            std::future::pending::<mux::MuxError>().await
        });
        opener
    }

    #[tokio::test]
    async fn third_concurrent_member_is_rejected() {
        let mut tasks = JoinSet::new();
        let (registry, _driver_scope) = registry_with_scope(8);
        let token = GroupToken::generate();
        let first = registry.join(token, test_opener(&mut tasks)).unwrap();
        let second = registry.join(token, test_opener(&mut tasks)).unwrap();
        assert!(registry.is_full(&token));
        assert!(registry.join(token, test_opener(&mut tasks)).is_err());
        drop(first);
        assert!(!registry.is_full(&token));
        let third = registry.join(token, test_opener(&mut tasks)).unwrap();
        drop((second, third));
    }
    #[tokio::test]
    async fn group_is_dropped_with_its_last_member() {
        let mut tasks = JoinSet::new();
        let (registry, _driver_scope) = registry_with_scope(8);
        let token = GroupToken::generate();
        let member = registry.join(token, test_opener(&mut tasks)).unwrap();
        let weak = Arc::downgrade(&member.group);
        drop(member);
        assert!(weak.upgrade().is_none(), "group must die with last member");
    }
    async fn dead_opener(tasks: &mut JoinSet<mux::MuxError>) -> DualStreamOpener {
        let lane = || {
            let (a, b) = tokio::io::duplex(4096);
            drop(b);
            let (r, w) = tokio::io::split(a);
            let mut spawner = JoinSet::new();
            let (opener, accepter) =
                spawn_mux_no_reconnection(r, w, crate::shared::server_mux_config(), &mut spawner);
            (opener, accepter, spawner)
        };
        let (int_op, int_acc, int_s) = lane();
        let (bulk_op, bulk_acc, bulk_s) = lane();
        let (opener, _accepter) = mux::spawn_dual_mux_paired_supervised(
            int_op, int_acc, int_s, bulk_op, bulk_acc, bulk_s, tasks,
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while opener.is_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a dual-lane session with no IO left must go down");
        opener
    }
    #[tokio::test]
    async fn a_dead_newest_member_never_takes_over_live_streams() {
        let mut tasks = JoinSet::new();
        let (registry, _driver_scope) = registry_with_scope(8);
        let token = GroupToken::generate();
        let old = registry.join(token, test_opener(&mut tasks)).unwrap();
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        old.register_writer(slot.handle());
        assert!(slot.take().is_none(), "no rebind before a newer member");
        let _dead = registry
            .join(token, dead_opener(&mut tasks).await)
            .expect("a second member still joins");
        assert!(
            slot.take().is_none(),
            "join rebound a live stream onto a session that was already dead",
        );
        let (late, _late_wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        old.register_writer(late.handle());
        assert!(
            late.take().is_none(),
            "a newly accepted stream was rebound onto a dead session",
        );
    }
    #[tokio::test]
    async fn late_registered_writer_is_rebound_to_newest_member() {
        let mut tasks = JoinSet::new();
        let (registry, _driver_scope) = registry_with_scope(8);
        let token = GroupToken::generate();
        let old = registry.join(token, test_opener(&mut tasks)).unwrap();
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        let _new = registry.join(token, test_opener(&mut tasks)).unwrap();
        old.register_writer(slot.handle());
        assert!(slot.take().is_some(), "stale writer was not rebound");
    }
    #[tokio::test]
    async fn join_rebinds_existing_writers() {
        let mut tasks = JoinSet::new();
        let (registry, _driver_scope) = registry_with_scope(8);
        let token = GroupToken::generate();
        let old = registry.join(token, test_opener(&mut tasks)).unwrap();
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        old.register_writer(slot.handle());
        assert!(slot.take().is_none(), "no rebind before a newer member");
        let _new = registry.join(token, test_opener(&mut tasks)).unwrap();
        assert!(slot.take().is_some(), "join must rebind existing writers");
    }
    #[tokio::test]
    async fn rebinds_collapse_instead_of_queueing() {
        let mut tasks = JoinSet::new();
        let (slot, _wake_rx) = crate::migrating_write_half::RebindSlot::detached();
        let handle = slot.handle();
        assert!(handle.rebind(test_opener(&mut tasks)));
        assert!(
            handle.rebind(test_opener(&mut tasks)),
            "a rebind was refused because an earlier one was still unconsumed",
        );
        assert!(slot.take().is_some(), "no rebind landed");
        assert!(
            slot.take().is_none(),
            "rebinds queued up instead of collapsing to the newest",
        );
    }
    #[tokio::test]
    async fn parked_driver_does_not_block_later_driver_submissions() {
        let (_submitter, mut scope) = group_driver_scope(2);
        // Driver A parks forever: one child task that never completes keeps
        // its drain alive for the whole test.
        let mut driver_a = JoinSet::new();
        driver_a.spawn(std::future::pending::<()>());
        scope.submit_driver(driver_a);
        // Driver B is submitted while A's drain is still parked and completes
        // immediately. Its drain must be reaped within a timeout even though
        // A's drain never finishes, proving later submissions are received
        // while the first remains alive.
        let mut driver_b = JoinSet::new();
        driver_b.spawn(async {});
        scope.submit_driver(driver_b);
        tokio::time::timeout(std::time::Duration::from_secs(5), scope.reap_driver())
            .await
            .expect("driver B must be reaped while driver A is still parked")
            .expect("driver B's drain completed");
        // A is still parked: a second reap must not complete.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), scope.reap_driver())
                .await
                .is_err(),
            "driver A's drain was reaped before it finished"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "simulated group-driver child panic")]
    async fn panicking_child_panics_the_drain_task() {
        let (_submitter, mut scope) = group_driver_scope(2);
        let mut driver = JoinSet::new();
        driver.spawn(async {
            panic!("simulated group-driver child panic");
        });
        scope.submit_driver(driver);
        // The drain unwraps every joined child, so the child panic propagates
        // out of `reap_driver` (which returns `Option<()>`) and cascades into
        // this test instead of being inspected as a JoinError.
        let _ = scope.reap_driver().await;
    }

    #[tokio::test]
    async fn queue_full_and_scope_closed_are_reported_separately() {
        let (submitter, scope) = group_driver_scope(1);
        // The scope's receiver is never polled, so the single bounded slot
        // stays occupied after the first submission: the second one is
        // refused as DriverQueueFull.
        let mut driver_a = JoinSet::new();
        driver_a.spawn(std::future::pending::<()>());
        assert_eq!(submitter.try_submit(driver_a), Ok(()));
        let mut driver_b = JoinSet::new();
        driver_b.spawn(std::future::pending::<()>());
        assert_eq!(
            submitter.try_submit(driver_b),
            Err(GroupJoinError::DriverQueueFull)
        );
        // Dropping the scope closes the submission channel; submissions are
        // then refused as DriverScopeClosed.
        drop(scope);
        let mut driver_c = JoinSet::new();
        driver_c.spawn(std::future::pending::<()>());
        assert_eq!(
            submitter.try_submit(driver_c),
            Err(GroupJoinError::DriverScopeClosed)
        );
    }

    #[tokio::test]
    async fn failed_driver_submission_does_not_create_a_group() {
        let mut tasks = JoinSet::new();
        let (registry, _driver_scope) = registry_with_scope(1);
        // Occupy the single slot with a driver whose task parks forever, so the
        // bounded channel stays full and every later submission is refused.
        let (_feed_a, mut driver_a) = mux::spawn_splice_router();
        driver_a.spawn(std::future::pending::<()>());
        registry.group_drivers.try_submit(driver_a).unwrap();
        let (_feed_b, driver_b) = mux::spawn_splice_router();
        let _ = registry.group_drivers.try_submit(driver_b);
        let token = GroupToken::generate();
        assert!(
            registry.join(token, test_opener(&mut tasks)).is_err(),
            "a group must be rejected when its driver submission is refused",
        );
        assert!(
            registry.groups.lock().unwrap().is_empty(),
            "a rejected group must not have been inserted",
        );
    }
}
