use mux::{DualStreamOpener, GroupToken};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::session::SessionSpawner;

pub(crate) struct SessionPairRegistry {
    groups: Mutex<HashMap<GroupToken, Weak<SessionPair>>>,
    session_spawner: SessionSpawner,
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
    pub(crate) fn new(session_spawner: SessionSpawner) -> Arc<Self> {
        Arc::new(Self {
            groups: Mutex::new(HashMap::new()),
            session_spawner,
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
    ) -> Result<PairMember, &'static str> {
        let group = {
            let mut groups = self.groups.lock().unwrap();
            groups.retain(|_, weak| weak.strong_count() > 0);
            match groups.get(&token).and_then(Weak::upgrade) {
                Some(group) => group,
                None => {
                    let (feed, mut driver) = mux::spawn_splice_router();
                    self.session_spawner.spawn(async move {
                        while let Some(result) = driver.join_next().await {
                            result.unwrap();
                        }
                    });
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
            return Err("session group is full");
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
    use crate::session::SessionFuture;
    use mux::spawn_mux_no_reconnection;
    use tokio::task::JoinSet;

    fn test_session_spawner(driver_tasks: &mut JoinSet<()>) -> SessionSpawner {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SessionFuture>(8);
        driver_tasks.spawn(async move {
            let mut inner = JoinSet::new();
            loop {
                tokio::select! {
                    submitted = rx.recv() => match submitted {
                        Some(fut) => {
                            inner.spawn(fut);
                        }
                        None => break,
                    },
                    joined = inner.join_next(), if !inner.is_empty() => {
                        joined.unwrap().unwrap();
                    }
                }
            }
            while let Some(result) = inner.join_next().await {
                result.unwrap();
            }
        });
        SessionSpawner::new(move |fut| {
            let _ = tx.try_send(fut);
        })
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
        let mut driver_tasks = JoinSet::new();
        let registry = SessionPairRegistry::new(test_session_spawner(&mut driver_tasks));
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
        let mut driver_tasks = JoinSet::new();
        let registry = SessionPairRegistry::new(test_session_spawner(&mut driver_tasks));
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
        let mut driver_tasks = JoinSet::new();
        let registry = SessionPairRegistry::new(test_session_spawner(&mut driver_tasks));
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
        let mut driver_tasks = JoinSet::new();
        let registry = SessionPairRegistry::new(test_session_spawner(&mut driver_tasks));
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
        let mut driver_tasks = JoinSet::new();
        let registry = SessionPairRegistry::new(test_session_spawner(&mut driver_tasks));
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
}
