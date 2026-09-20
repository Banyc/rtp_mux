//! rtp_mux layer-testing kit: the dual-lane mux server/connector plumbing,
//! the per-lane RTP transport configuration, and the tagged-stream
//! latency/bulk sink machinery used by the rtp_mux scenario suites, compiled
//! behind the `testing` feature.
//!
//! This is the owning crate's half of the shared scenario scaffolding: the
//! generic helpers (payload, task scopes, reporting, presets, contention
//! fans) live in the `netem-test` harness kit (`netem_test::kit`, behind its
//! `test-kit` feature), the rtp echo/connect/sink/frame/perf-trace
//! scaffolding lives in the `rtp` layer kit (`rtp::testkit`, behind rtp's
//! `testing` feature), and the mux-over-rtp scaffolding lives in the `mux`
//! layer kit (`mux::testkit`, behind mux's `testing` feature); all are
//! consumed here. Imports only ever go downward (rtp_mux kit → mux kit / rtp
//! kit / harness kit), so no layer depends on a sibling's kit and
//! `netem-test` stays a leaf.
//!
//! The operator's product constitution — the three mandates that are the
//! joint acceptance criterion for every change to the interactive path — is
//! stated module-level in `tests/dual_lane_mandates.rs` and in `GATE.md`
//! ("Performance"): rtp_mux owns the dual-lane topology, so all three
//! mandates are asserted by rtp_mux's own scenario gates.

pub mod dual;
pub mod rtp_mux;
