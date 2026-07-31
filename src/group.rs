use mux::{DualStreamOpener, GroupToken};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

pub(crate) struct SessionGroupRegistry {
    groups: Mutex<HashMap<GroupToken, Weak<SessionGroup>>>,
}
pub(crate) struct SessionGroup {
    feed: mux::SpliceFeed,
    state: Mutex<GroupState>,
}
struct GroupState {
    members: usize,
    next_seq: u64,
    newest_seq: u64,
    newest_opener: Option<DualStreamOpener>,
    writers: Vec<tokio::sync::mpsc::WeakSender<DualStreamOpener>>,
    next_purge: usize,
}
pub(crate) struct GroupMember {
    group: Arc<SessionGroup>,
    seq: u64,
}
impl Drop for GroupMember {
    fn drop(&mut self) {
        self.group.state.lock().unwrap().members -= 1;
    }
}

impl SessionGroupRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            groups: Mutex::new(HashMap::new()),
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
    ) -> Result<GroupMember, &'static str> {
        let group = {
            let mut groups = self.groups.lock().unwrap();
            groups.retain(|_, weak| weak.strong_count() > 0);
            match groups.get(&token).and_then(Weak::upgrade) {
                Some(group) => group,
                None => {
                    let group = Arc::new(SessionGroup {
                        feed: mux::spawn_splice_feed(),
                        state: Mutex::new(GroupState {
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
        state.writers.retain(|weak| weak.upgrade().is_some());
        for weak in &state.writers {
            if let Some(tx) = weak.upgrade() {
                let _ = tx.try_send(opener.clone());
            }
        }
        drop(state);
        Ok(GroupMember { group, seq })
    }
}

impl GroupMember {
    pub(crate) fn feed(&self) -> mux::SpliceFeedHandle {
        self.group.feed.handle()
    }
    pub(crate) fn register_writer(&self, tx: &tokio::sync::mpsc::Sender<DualStreamOpener>) {
        let mut state = self.group.state.lock().unwrap();
        if state.newest_seq != self.seq {
            if let Some(opener) = &state.newest_opener {
                let _ = tx.try_send(opener.clone());
            }
        }
        if state.writers.len() >= state.next_purge {
            state.writers.retain(|weak| weak.upgrade().is_some());
            state.next_purge = (state.writers.len() * 2).max(64);
        }
        state.writers.push(tx.downgrade());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mux::spawn_mux_no_reconnection;
    use tokio::task::JoinSet;

    fn test_opener(tasks: &mut JoinSet<mux::MuxError>) -> DualStreamOpener {
        let mut lane = || {
            let (a, _b) = tokio::io::duplex(4096);
            let (r, w) = tokio::io::split(a);
            spawn_mux_no_reconnection(r, w, crate::shared::server_mux_config(), tasks)
        };
        let (int_op, int_acc) = lane();
        let (bulk_op, bulk_acc) = lane();
        let (opener, _accepter) = mux::spawn_dual_mux_paired(int_op, int_acc, bulk_op, bulk_acc);
        opener
    }

    #[tokio::test]
    async fn third_concurrent_member_is_rejected() {
        let mut tasks = JoinSet::new();
        let registry = SessionGroupRegistry::new();
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
        let registry = SessionGroupRegistry::new();
        let token = GroupToken::generate();
        let member = registry.join(token, test_opener(&mut tasks)).unwrap();
        let weak = Arc::downgrade(&member.group);
        drop(member);
        assert!(weak.upgrade().is_none(), "group must die with last member");
    }
    #[tokio::test]
    async fn late_registered_writer_is_rebound_to_newest_member() {
        let mut tasks = JoinSet::new();
        let registry = SessionGroupRegistry::new();
        let token = GroupToken::generate();
        let old = registry.join(token, test_opener(&mut tasks)).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let _new = registry.join(token, test_opener(&mut tasks)).unwrap();
        old.register_writer(&tx);
        assert!(rx.try_recv().is_ok(), "stale writer was not rebound");
    }
    #[tokio::test]
    async fn join_rebinds_existing_writers() {
        let mut tasks = JoinSet::new();
        let registry = SessionGroupRegistry::new();
        let token = GroupToken::generate();
        let old = registry.join(token, test_opener(&mut tasks)).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        old.register_writer(&tx);
        assert!(rx.try_recv().is_err(), "no rebind before a newer member");
        let _new = registry.join(token, test_opener(&mut tasks)).unwrap();
        assert!(rx.try_recv().is_ok(), "join must rebind existing writers");
    }
}
