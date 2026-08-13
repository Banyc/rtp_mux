//! Task-scope epilogs for the RTP mux connector and session drivers.
//!
//! A connector handle is inert without its command-loop driver, and a
//! bidirectional session is incomplete without its mux supervisor.  These
//! helpers make the driver the explicit owner of every child: when the
//! first (selected) result is known, siblings are aborted and reaped so a
//! completed value, error, or panic stays observable across the boundary
//! instead of being hidden by `JoinSet`'s drop epilog.

use tokio::task::{JoinError, JoinSet};

/// Finish a race where `first` (an already-joined result, if any) selects
/// the outcome: abort every sibling, reap the set, and return the first
/// completed outcome — the selected one when it exists, otherwise the first
/// non-cancelled sibling result.  A completed sibling panic still
/// propagates through `unwrap`, so a racing panic crosses the boundary.
pub(crate) async fn finish_with_first<T: 'static>(
    tasks: &mut JoinSet<T>,
    first: Option<Result<T, JoinError>>,
) -> Option<T> {
    let mut outcome = first.map(Result::unwrap);
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        if joined.as_ref().is_err_and(JoinError::is_cancelled) {
            continue;
        }
        let completed = joined.unwrap();
        if outcome.is_none() {
            outcome = Some(completed);
        }
    }
    outcome
}

/// Abort and reap every child of `tasks`, discarding completed values.
pub(crate) async fn abort_and_reap<T: 'static>(tasks: &mut JoinSet<T>) {
    let _ = finish_with_first(tasks, None).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn selected_result_survives_sibling_cancellation() {
        let mut tasks: JoinSet<u32> = JoinSet::new();
        tasks.spawn(std::future::pending());
        tasks.spawn(std::future::pending());
        let selected = Some(Ok(7));
        let outcome = finish_with_first(&mut tasks, selected).await;
        assert_eq!(outcome, Some(7));
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn completed_sibling_supplies_a_result_when_no_owner_won() {
        let mut tasks: JoinSet<u32> = JoinSet::new();
        // A sibling that already completed before the owner aborts must
        // supply the outcome (this is the real-world order: the owner awaits
        // the first join, so anything else that finished is observed here).
        let handle = tasks.spawn(async { 3 });
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let outcome = finish_with_first(&mut tasks, None).await;
        assert_eq!(outcome, Some(3));
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    #[should_panic(expected = "sibling panic")]
    async fn racing_completed_panic_cascades_after_an_ordinary_result() {
        let mut tasks: JoinSet<u32> = JoinSet::new();
        // A sibling that already completed with a panic (before this task
        // aborted it) must not be swallowed as cancellation.
        let handle = tasks.spawn(async {
            panic!("sibling panic");
        });
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let selected = Some(Ok(1));
        finish_with_first(&mut tasks, selected).await;
    }
}
