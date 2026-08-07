//! Shared integration-test plumbing for `rtp_mux`'s own tests.
//!
//! Every end-to-end test runs its body through an actively-polled
//! [`TestScope`] and routes server-owned futures (session supervisors, the
//! serve loop, per-stream handlers) and the connector driver through it, so
//! a background task that panics or exits early fails the test immediately
//! instead of being observed only when the scope is dropped.

pub(crate) mod task_scope;

pub(crate) const TEST_TASK_QUEUE_BOUND: usize = 256;

pub(crate) use task_scope::TestScope;
