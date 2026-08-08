//! An actively-polled root [`TestScope`] plus a bounded [`TestTaskSubmitter`]
//! for server-owned futures, shared by the `rtp_mux` integration tests.

use std::future::Future;

/// A boxed test-owned task future: session supervisors spawned by the
/// [`rtp_mux::SessionSpawner`], the server serve loop, per-stream handler
/// tasks, and connector drivers all submit through [`TestTaskSubmitter`] /
/// [`TestScope::spawn`].
pub(crate) type TestTask = std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A bounded submission channel feeding one test-owned reaper task (spawned
/// into the root [`TestScope`] by [`TestScope::submitter`]). The reaper
/// selects between new submissions and `join_next()` completions, unwrapping
/// every completion, so a child panic surfaces immediately. The channel is
/// bounded so a stalled reaper is detected as a full channel instead of
/// unbounded memory growth.
///
/// Clone it into every scope that spawns owned tasks and keep at least one
/// clone alive for the channel to stay open.
#[derive(Clone)]
pub(crate) struct TestTaskSubmitter {
    tx: tokio::sync::mpsc::Sender<TestTask>,
}

impl TestTaskSubmitter {
    /// Submit one test-owned task future. `Full` means the reaper is not
    /// draining (a bug in the harness) and `Closed` means the reaper stopped
    /// unexpectedly; both fail the test rather than dropping the future
    /// silently.
    pub(crate) fn submit(&self, fut: TestTask) {
        match self.tx.try_send(fut) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                panic!("test task submission channel is full; the reaper is not draining")
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                panic!("test task reaper stopped unexpectedly")
            }
        }
    }
}

/// An actively-polled scope of test-owned background tasks. The test body
/// runs through [`TestScope::run`], which races it against `join_next()` on
/// the scope, so a background task that panics (in particular one that
/// unwraps a panicked child join) fails the test immediately instead of
/// being observed only when the scope is dropped. Background tasks that end
/// normally are drained silently (legitimate shutdowns); dropping the scope
/// remains the abort backstop for tasks still running when the body
/// completes.
pub(crate) struct TestScope {
    pub(crate) tasks: tokio::task::JoinSet<()>,
}

impl TestScope {
    pub(crate) fn new() -> Self {
        Self {
            tasks: tokio::task::JoinSet::new(),
        }
    }

    pub(crate) fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) {
        self.tasks.spawn(task);
    }

    /// Spawn a task that must stay alive for the whole [`TestScope::run`] body.
    /// A normal completion while the body is still running panics the test
    /// with a message naming the task (the wrapper turns the completion into
    /// a panic); a panic inside the future propagates unchanged.
    pub(crate) fn spawn_required(
        &mut self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) {
        self.tasks.spawn(async move {
            future.await;
            panic!("required task '{name}' exited before the test body completed");
        });
    }

    /// Spawn the bounded inner reaper into this scope and return its
    /// submission channel. Keep the returned submitter alive for the channel
    /// to stay open.
    pub(crate) fn submitter(&mut self, bound: usize) -> TestTaskSubmitter {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<TestTask>(bound);
        self.spawn(async move {
            let mut owned = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    Some(fut) = rx.recv() => {
                        owned.spawn(fut);
                    }
                    Some(joined) = owned.join_next() => {
                        joined.unwrap();
                    }
                    else => break,
                }
            }
        });
        TestTaskSubmitter { tx }
    }

    pub(crate) async fn run<F: Future>(mut self, body: F) -> F::Output {
        tokio::pin!(body);
        loop {
            tokio::select! {
                biased;
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    // A background task exited before the body. Re-raise any
                    // panic it surfaced immediately; a normal completion is a
                    // legitimate shutdown (e.g. a session supervisor ending
                    // when its session closes) and is drained silently.
                    let joined = joined.expect("background task exists");
                    joined.unwrap();
                }
                value = &mut body => {
                    // The body completed. Drain tasks that exited in the same
                    // poll cycle so a required task that ended right as the
                    // body finished still fails the test.
                    while let Some(joined) = self.tasks.try_join_next() {
                        joined.unwrap();
                    }
                    return value;
                }
            }
        }
    }
}
