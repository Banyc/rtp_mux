//! rtp_mux layer-testing kit: the dual-lane mux server/connector plumbing,
//! the per-lane RTP transport configuration, the tagged-stream latency/bulk
//! sink machinery, and the transport-mediated `mux`-over-`rtp` scaffolding
//! used by the rtp_mux scenario suites, compiled behind the `testing`
//! feature.
//!
//! This is the owning crate's half of the shared scenario scaffolding: the
//! generic helpers (payload, task scopes, reporting, presets, contention
//! fans) live in the `netem-test` harness kit (`netem_test::kit`, behind its
//! `test-kit` feature), the rtp echo/connect/sink/frame/perf-trace
//! scaffolding in the `rtp` layer kit (`rtp::testkit`, behind rtp's `testing`
//! feature), and the transport-free mux half in the `mux` layer kit
//! (`mux::testkit`, behind mux's `testing` feature); all are consumed here.
//! The `mux_over_rtp` submodule is the one place that sees `mux` and `rtp`
//! together. Imports only ever go downward (rtp_mux kit → mux kit / rtp kit /
//! harness kit), so no layer depends on a sibling's kit and `netem-test`
//! stays a leaf.
//!
//! The operator's product constitution — the three mandates that are the
//! joint acceptance criterion for every change to the interactive path — is
//! stated module-level in `tests/dual_lane_mandates.rs` and in `GATE.md`
//! ("Performance"): rtp_mux owns the dual-lane topology, so all three
//! mandates are asserted by rtp_mux's own scenario gates.

pub mod dual;
pub mod mux_over_rtp;
pub mod payload;
pub mod profile;
pub mod rtp_mux;
pub mod standoff;

/// The production dual-lane birth's liveness deadline, in milliseconds —
/// re-exported so a measurement arm can assert its relation to the field's
/// worst recorded round trip without the constant leaving the crate's private
/// surface. One authority for its value and derivation:
/// [`crate::shared::BIRTH_LIVENESS_DEADLINE`].
pub const BIRTH_LIVENESS_DEADLINE_MS: u64 =
    crate::shared::BIRTH_LIVENESS_DEADLINE.as_millis() as u64;

/// The grace added to the birth's liveness deadline for the whole dual-lane
/// birth's race, in milliseconds — re-exported with the deadline so a
/// measurement arm can state the retry budget (`MAX_DUAL_CONNECT_ATTEMPTS x
/// (deadline + grace)`) it bounds a dead birth by.
pub const BIRTH_LIVENESS_GRACE_MS: u64 = crate::shared::BIRTH_LIVENESS_GRACE.as_millis() as u64;

/// The bound on a dead birth's dial cost: the number of fresh cold-birth
/// attempts `retry_dual_connect` makes. Re-exported for the same arithmetic.
pub const MAX_DUAL_CONNECT_ATTEMPTS: usize = crate::shared::MAX_DUAL_CONNECT_ATTEMPTS;
