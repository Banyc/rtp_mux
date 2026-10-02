//! The deployment link profile and bulk-load shape shared by the
//! performance/mandate scenario suites.
//!
//! Every arm that measures the deployed path (the mandate smoke set, the
//! jitter battery, the dual-lane mandates, the standoff family, and the
//! contested/HOL probes) pairs the same 25 ms one-way delay with the same 5 ms
//! jitter and a 256-byte interactive message, and drains the bulk lane through
//! the same 1 MiB/s bottleneck with the same 2 MiB / 3 s burst. Those are one
//! setting each, so they are declared once here and resolved from this module
//! by every arm.
//!
//! **Deliberate divergence, kept in the owners.** A file that runs a
//! *different* regime keeps its own constant rather than this one:
//! `minecraft_contested`'s 50 ms one-way / 10 ms jitter Minecraft profile,
//! the 5 ms vs 25 ms interactive cadences, and the per-arm drain (`GRACE`),
//! window, rep count and run length. Equal values that bound different regimes
//! (the per-arm M1 guards, the M2/M4 delivery floors, the M3 denominator) are
//! likewise separate settings and stay in the file that owns them.

use std::time::Duration;

/// One-way delay applied to every packet of the deployment link profile, in
/// both directions.
pub const OWD: Duration = Duration::from_millis(25);

/// Uniform jitter around [`OWD`] on the deployment link profile.
pub const JITTER: Duration = Duration::from_millis(5);

/// The interactive message size: a typical game ping.
pub const MSG_BYTES: usize = 256;

/// `u32` loss threshold equal to `pct` percent per packet.
pub const fn loss_pct(pct: u32) -> u32 {
    (u32::MAX / 100) * pct
}

/// The deployment profile's independent per-packet loss (2 %).
pub const LOSS_2: u32 = loss_pct(2);

/// The bulk lane's configured rate cap (bits per second): the 1 MiB/s
/// bottleneck the periodic burst shape drains against.
pub const BULK_RATE_BPS: u64 = 8 * 1024 * 1024;

/// Bytes offered per bulk burst (2 MiB).
pub const BULK_BURST_BYTES: usize = 2 * 1024 * 1024;

/// Interval between bulk bursts.
pub const BULK_PERIOD: Duration = Duration::from_secs(3);

/// Let the interactive stream establish a solo floor before the first burst.
pub const BULK_RAMP: Duration = Duration::from_millis(1500);

/// The shared uplink bottleneck both lanes' writes cross on a `shared_shaper`
/// arm (1 MiB/s). The shaper is the *instrument*; the CC signal has no rate of
/// its own.
///
/// It carries the same numeric value as [`BULK_RATE_BPS`] because both are the
/// deployment's 1 MiB/s link, but they are separate settings: [`BULK_RATE_BPS`]
/// caps one lane's own link, this is the one queue both lanes contend for, and
/// either may be retuned without the other.
pub const SHAPER_RATE_BPS: u64 = 8_388_608;

/// The shared shaper's drop-tail buffer. At [`SHAPER_RATE_BPS`] a full buffer
/// is 125.0 ms of standing queue.
pub const SHAPER_LIMIT_BYTES: u64 = 128 * 1024;

/// A reporting nominal for the bulk-goodput column, not a rate any arbiter
/// enforces: it only keeps the column comparable across arms.
pub const LINK_BYTES_PER_SEC: f64 = 1024.0 * 1024.0;

/// How far the measured offer count may fall below the arm's schedule before
/// the lane is no longer being offered the mandate's known throughput.
///
/// The tolerance is slack for **the transport refusing writes**, not for the
/// sender's schedule: a deadline-driven cadence sender owes its schedule the
/// message count by construction, so on an idle host an arm lands on exactly
/// its scheduled count and a shortfall means the lane would not take the load.
pub const M2_OFFER_TOLERANCE: f64 = 0.02;
