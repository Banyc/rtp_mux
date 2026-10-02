//! The stand-off scenario's shared burst and measurement shape.
//!
//! The `standoff_burst` and `standoff_nonconvergent` gates run the same
//! burst/interleaved-rep scenario and the `standoff_beta_sweep` gate integrates
//! its share on the same sampling cadence, so the shape below is declared once
//! here and resolved by all three. The gates' own windows, rep counts,
//! measurement offsets, betas and floors are *different* regimes and stay in
//! the file that owns them.

use std::time::Duration;

/// The burst shape: a short active stretch, a long idle gap (several times the
/// stand-off window, so the stand-off genuinely arms inside each gap).
pub const BURST_ON: Duration = Duration::from_millis(250);
pub const BURST_OFF: Duration = Duration::from_secs(4);

/// Delay before the first burst, so the lane handshakes settle first.
pub const FIRST_BURST: Duration = Duration::from_millis(500);

pub const BURSTS: usize = 4;

/// A rep whose bulk or competitor connection delivered no bytes is not a
/// sample of the mechanism -- the flow was absent. Re-run it (a bounded number
/// of times) rather than folding a degenerate ratio into the spread.
pub const MAX_REP_ATTEMPTS: usize = 4;

/// How often both bulk byte counters are sampled. The share is integrated over
/// these samples, so the cadence is the share's time resolution.
pub const SAMPLE_STEP: Duration = Duration::from_millis(5);

/// The part of each gap the share is read over: from the moment the stand-off
/// window has elapsed (plus slack for the interactive staleness horizon) to the
/// next burst. A gap shorter than this contributes no samples.
pub const GAP_MEASURE_TAIL: Duration = Duration::from_millis(1600);

/// How many interactive samples after each resume the hold's first packets are
/// read from. The whole-window p99 is repair-dominated (a ~357 ms plateau), so
/// the hold's queue contribution is only visible in the first few packets of
/// each resume, before a repair has had time to fire.
pub const RESUME_FIRST_N: usize = 8;

/// The resume window: the burst plus one hold and a scheduling margin.
pub const RESUME_SPAN: Duration = Duration::from_millis(750);
