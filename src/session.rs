use std::{future::Future, pin::Pin, sync::Arc};

/// A boxed, complete session future. `()` output: the supervisor logs its own termination.
pub type SessionFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Caller-provided session scope. Cloneable. `spawn` submits a complete,
/// boxed session future to the caller's process session scope.
#[derive(Clone)]
pub struct SessionSpawner(Arc<dyn Fn(SessionFuture) + Send + Sync + 'static>);

impl SessionSpawner {
    pub fn new(spawn: impl Fn(SessionFuture) + Send + Sync + 'static) -> Self {
        Self(Arc::new(spawn))
    }

    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        (self.0)(Box::pin(fut));
    }
}
