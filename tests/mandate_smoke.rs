//! The tri-mandate performance smoke set: one short, always-run measurement
//! per mandate, plus the panels a reader checks the numbers against.
//!
//! The operator's product constitution is three mandates — **M1** interactive
//! tail latency, **M2** the interactive lane's latency **not degrading under a
//! known offered throughput**, **M3** bulk goodput as a fraction of the link
//! rate — and this target is the one command that measures all three and
//! leaves machine-checkable evidence for each:
//!
//! ```sh
//! cargo test --release -p rtp_mux --test mandate_smoke -- --nocapture
//! ```
//!
//! The one authority for the mandate bounds (their values, their derivation
//! and the arms they are asserted on) is `rtp_mux/GATE.md` ("Performance");
//! the existing opt-in constitution gates
//! (`rtp_mux_jitter::jitter_duallane_constitution_gate_p99`,
//! `rtp_mux_jitter::jitter_duallane_constitution_gate`,
//! `dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction`)
//! remain their owners. This file is a **smoke set alongside** them: it does
//! not retune, re-arm or replace any of them.
//!
//! # The smoke arms
//!
//! All three mandates are measured on the production dual-lane topology (the
//! interactive lane on its own RTP connection in frame fast-forward + prompt
//! FEC, the bulk lane on a second, separate `LaneRtpConfig::production_bulk`
//! connection). M1 and M2 share three arms, in the shape the field sends:
//!
//! | arm | impairment | interactive load | bulk |
//! | --- | --- | --- | --- |
//! | `clean` | 2 % iid, 25 ms one-way, 5 ms jitter | 256 B cadence | 2 MiB / 3 s |
//! | `hostile` | GE `gilbert_elliott_loss(5, 8)`, 25 ms one-way, 100 ms jitter | 256 B cadence | 2 MiB / 3 s |
//! | `lone_tail` | GE `gilbert_elliott_loss(5, 8)`, 25 ms one-way, 100 ms jitter | `RequestResponse { depth: 1 }` (the lone tail) | none |
//!
//! The windows are short by measurement, not by habit: the goodput signal
//! settles within a few seconds of a long window, so every window here is
//! `<= 15 s` and the interactive cadence is `~5 ms` so the tail percentiles
//! have thousands of samples per arm instead of hundreds. `MANDATE_SMOKE_QUICK=1`
//! (set by `tools/mandate-check --quick`) takes the shortest windows while
//! still printing all three verdict lines and writing all six evidence files.
//!
//! # Bounds: mandate assertions and regression guards
//!
//! **M1** asserts the mandate bound — `p99 <= 250 ms` and **zero** samples
//! `> 250 ms` — on the `clean` arm. The `hostile` and `lone_tail` arms carry
//! the product's **known, measured** hostile defect (the 1 s `MIN_RTO` repair
//! floor: measured GE lone-tail p99 1053–1542 ms, `> 250 ms` up to 2.7 %), so
//! they assert a *regression bound* derived from that measurement with
//! documented headroom instead of a bound that is currently false. **M2**
//! asserts, on `clean`, that the lane was **offered the known throughput**
//! (`MSG_BYTES / CADENCE`, read from the measured `sent`, written on an
//! absolute-deadline schedule so the host's timer cannot cut the offer and a
//! shortfall means the lane would not take the load), that it
//! **delivered** all of it (`delivery == 1.000`), and that its **latency did
//! not degrade under that offer** (`p99 <= M2_NONDEGRADING_P99_MS`); the
//! hostile and lone-tail arms keep their delivery floors as regression guards.
//! **M3** asserts the within-run delivered/shaper-forwarded fraction against
//! the `0.35x` floor. The hostile panels draw the mandate ceiling/floor lines
//! regardless, so the breach stays visible even where the assertion is only a
//! guard — the assertion is a tripwire, the panel is the evidence.
//!
//! The regression bounds and their derivation are recorded in `GATE.md`; the
//! constants below carry a one-line pointer rather than restating it.
//!
//! # M4: the interactive lane's split across several flows
//!
//! M1 and M2 measure one interactive flow, so a mandate result obtained by
//! starving one of several flows sharing the interactive lane would pass them.
//! **M4** closes that: `M4_FLOWS` interactive flows are multiplexed on ONE
//! interactive lane (the same production `LaneRtpConfig::frame_reordering`
//! lane as M1/M2's clean arm), each offering the same payload at the same
//! cadence, and the arm asserts the outcome pair the fairness mandate names —
//! **no starvation** (every flow delivers what it is offered) and **fair
//! share** (no flow's share of the lane's delivered bytes departs from the
//! equal share by more than [`M4_IMBALANCE_BOUND`]) — plus a fair-latency bound
//! (no flow's p90 exceeds the best flow's p90 by more than
//! [`M4_LATENCY_SPREAD_BOUND`] on the clean arm, the dimension the share
//! statistic cannot see; the hostile arm keeps M1's absolute guard). The p90 is
//! the quantile that statistic is read at and the reason is on the constant:
//! the p99 ratio is a quotient of two rare-event counts once the start-up
//! transient is gone.
//!
//! "Delivers what it is offered" is read as a **delivery** claim and not as a
//! latency one: a flow's samples are split by when the sink observed them —
//! **on time** inside the `window + GRACE` cutoff, **late** inside the
//! [`M4_LATE_HORIZON`] drain that follows, and **lost** when the arm never
//! observes them at all. The floor is asserted on `received / sent` with
//! `received = on_time + late`, so a head-of-line block the repair ladder clears
//! is a late arrival and not a starvation event; `on_time`, `late` and `lost`
//! are printed per flow, written to `M4.csv` and drawn on their own panels.
//!
//! The per-flow latencies are reported and the panel draws M1's ceiling, so a
//! fair-but-slow split is visible; M1 remains the authority for the absolute
//! interactive ceiling. The statistic, its derived
//! bound and the arms are stated in `rtp_mux/GATE.md` ("Performance"), one
//! authority with the rest of the mandate bounds; the constants below carry a
//! pointer, not a restatement.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::future::join_all;
use mux::LaneClass;
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::presets::gilbert_elliott_loss;
use netem_test::kit::stats::{HolSummary, summarize};
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{BottleneckShaper, LossModel, NetemConfig, NetemPair};
use rtp::metrics::{MetricsEvent, MetricsInterest, MetricsObservation, MetricsObserver};
use rtp::testkit::rtp::{spawn_rtp_bulk_upload_with_options_via, spawn_rtp_byte_sink_server_via};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via,
    dual_mux_client_connect_lane_rtp_via_cc_link,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use rtp_mux::testkit::mux_over_rtp::send_timestamped_messages;
use rtp_mux::testkit::rtp_mux::ECHO_TAG;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::MissedTickBehavior;

// ─────────────────────────────── arm constants ───────────────────────────────

/// One-way delay applied to every interactive packet: the deployment profile.
const OWD: Duration = Duration::from_millis(25);
/// Uniform jitter around [`OWD`] on the clean arm (the existing arms' 5 ms).
const JITTER: Duration = Duration::from_millis(5);
/// Uniform jitter on the hostile arms: the field's ~100 ms excursion regime.
const HOSTILE_JITTER: Duration = Duration::from_millis(100);
/// `u32` loss threshold equal to `pct` percent per packet.
const fn loss_pct(pct: u32) -> u32 {
    (u32::MAX / 100) * pct
}
/// The clean/mild arm's independent per-packet loss.
const LOSS_2: u32 = loss_pct(2);
/// The interactive message size, a typical game ping.
const MSG_BYTES: usize = 256;
/// Interactive cadence: ~5 ms, so a short window still buys thousands of
/// samples rather than hundreds.
const CADENCE: Duration = Duration::from_millis(5);
/// Interactive window (full tier).
const WINDOW: Duration = Duration::from_secs(12);
/// Interactive window (quick tier) — the shortest window with a usable tail.
const QUICK_WINDOW: Duration = Duration::from_secs(4);
/// Request/response (lone-tail) window. A lone-tail lane offers one round trip
/// at a time, so it needs a longer window than the cadence arms for a usable
/// sample count; still `<= 15 s`.
const RR_WINDOW: Duration = Duration::from_secs(15);
/// Request/response window (quick tier).
const RR_QUICK_WINDOW: Duration = Duration::from_secs(5);
/// Drains stragglers before the summary is read, so a message offered at the
/// window's edge is not counted as lost.
const GRACE: Duration = Duration::from_secs(2);
/// The bulk burst shape on the M1/M2 arms (the existing production load).
const BULK_RATE_BPS: u64 = 8 * 1024 * 1024;
const BULK_BURST_BYTES: usize = 2 * 1024 * 1024;
const BULK_PERIOD: Duration = Duration::from_secs(3);
const BULK_RAMP: Duration = Duration::from_millis(1500);
/// The bulk lane's configured capacity, the M3 denominator.
const M3_CAPACITY_BPS: u64 = 8 * 1024 * 1024;

/// The one uplink bottleneck both lanes' writes cross on a `shared_shaper` arm: the
/// operator's own Minecraft trace's mid-range capacity (1 MiB/s). The shaper is
/// the *instrument*; the CC signal has no rate of its own.
const SHARED_UP_RATE_BPS: u64 = 8_388_608;
/// The saturating bulk window (full / quick tier).
const BULK_WINDOW: Duration = Duration::from_secs(6);
const QUICK_BULK_WINDOW: Duration = Duration::from_secs(2);
const M3_REPS: usize = 3;

// ─────────────────────── mandate bounds (authority: GATE.md) ─────────────────

/// The M1 ceiling: the mandate bound asserted on the clean arm. One authority
/// for its value and derivation: `rtp_mux/GATE.md` ("Performance").
const M1_CEILING_MS: f64 = 250.0;
/// The M2 non-degradation bound: the interactive lane's p99 must stay at the
/// link's floor while it is offered its known throughput. The link's one-way
/// floor is `OWD + JITTER` = 30 ms and the measured clean-arm p99 is ~26 ms; a
/// lane whose goodput fell would have to show the backlog as latency, and the
/// value is ~4x the measured p99 and well below the M1 ceiling, so it bites on
/// a backlog rather than on the 2 % loss realisation. One authority for the
/// value and derivation: `rtp_mux/GATE.md` ("Performance").
const M2_NONDEGRADING_P99_MS: f64 = 100.0;
/// The M3 goodput floor as a fraction of the configured link rate. One
/// authority: `rtp_mux/GATE.md` ("Performance").
const M3_CAPACITY_FRACTION: f64 = 0.35;

// ───────────────── hostile regression guards (derivation: GATE.md) ──────────

/// M1 hostile cadence-arm p99 guard. The 12 s GE `5 %`/mean-8 + 100 ms-jitter
/// cadence arm measured p99 212-280 ms across runs; the guard is ~3x that
/// band, so a change that doubles the hostile tail fails while the measured
/// defect (which the arm exists to keep visible against the 250 ms line) does
/// not.
const M1_HOSTILE_P99_GUARD_MS: f64 = 900.0;
/// M1 hostile cadence-arm `> 250 ms` sample-count guard (measured 0-2.75 %).
const M1_HOSTILE_OVER250_GUARD_PCT: f64 = 8.0;

/// The number of interleaved hostile reps [`m1_hostile_p99_replicated`] takes.
/// Derived, not picked. The hostile p99's between-rep standard deviation is
/// `M1_HOSTILE_P99_REP_SD_MS`, so the difference of two revisions' medians has
/// `se = sqrt(2) * 1.2533 * sd / sqrt(R)` and clears a move of `D` at 95 %
/// confidence and 80 % power when `R >= (2.802 * sqrt(2) * 1.2533 * sd / D)^2`.
/// For `D = 60 ms` and the recorded `sd = 38.4 ms` that is `10.12`, so `R = 11`.
const M1_HOSTILE_P99_REP_REPS: usize = 11;

/// The between-rep standard deviation of a **single** hostile p99 over the 43
/// healthy observations on record: the 20 load-logged interleaved reps of the
/// campaign recorded in `rtp_mux/GATE.md` (range 117.5-220.3 ms, sd 27.1 ms) and
/// the 23 archived battery runs of the same arm under
/// `netem_test/.net-perf-history/` (113.3-281.9 ms, sd 46.1 ms; the two
/// consecutive same-revision observations `162.5` and `281.9 ms` that
/// `crates/AUDIT_COVERAGE.md` records are two of them). The archive is included
/// deliberately and is the reason the count is eleven and not six: a
/// before/after comparison is exposed to the cross-revision movement the archive
/// holds and the one-revision campaign cannot see, so `sd = 38.4` is the honest
/// input rather than the campaign's own 27.1.
const M1_HOSTILE_P99_REP_SD_MS: f64 = 38.4;

/// The bound on the **median** hostile p99 across
/// [`M1_HOSTILE_P99_REP_REPS`] interleaved reps, asserted by
/// [`m1_hostile_p99_replicated`].
///
/// Derived, not picked: `mean + 4 sd` of the *median's* own sampling
/// distribution, over the same 43 healthy observations. Their mean is `161.5 ms`
/// and the median-of-eleven sd, bootstrapped from them (400 000 resamples), is
/// `14.93 ms`, so the bound is `161.5 + 4 * 14.93 = 221.2 ms`, rounded up to the
/// next whole 5 ms. Thirty-seven in a million healthy draws of that median
/// exceed it. The arm's own vacuity fault (`MANDATE_SMOKE_FAULT=M1_IMPAIRED_slow`,
/// `+300 ms` one-way on the hostile link, which pushes the arm past the 1 s
/// repair floor) measured a median of `2021.2 ms` on this arm's ten-rep form, so
/// a healthy draw and a bad lane stay an order of magnitude apart. The bound is
/// `1.32x` tighter than the single-rep limit the deployed baseline carries
/// (`298 ms`) and is asserted on a statistic whose noise is known, so a change
/// that moves the hostile tail by `60 ms` fails here rather than hiding inside a
/// single rep's draw.
const M1_HOSTILE_P99_MEDIAN_BOUND_MS: f64 = 225.0;

/// The bound on the **median** hostile p90 across [`M1_HOSTILE_P99_REP_REPS`]
/// interleaved reps, asserted by [`m1_hostile_p99_replicated`].
///
/// The p90 is the quantile the same campaign measured as a *lane property*:
/// over the 20 controlled reps it spans `78.7-93.2 ms` (1.18x, sd `3.8 ms`,
/// cv `0.04`) where the same runs' p99 spans `1.87x` -- the body does not move
/// and the tail's count does. So this is the arm's level claim that does not
/// depend on a rare-event draw at all, and it is asserted beside the tail so a
/// regression that shifts the whole distribution (the fault, `+300 ms`) is named
/// whichever statistic is read. Derived the same way: `mean + 4 sd` over the 20
/// controlled reps is `87.2 + 4 * 3.8 = 102.4 ms`, rounded up to the next whole
/// 5 ms.
const M1_HOSTILE_P90_MEDIAN_BOUND_MS: f64 = 105.0;
/// M1 lone-tail p99 guard. The field's 60 s GE lone-tail arms measured p99
/// 1053-1542 ms (the 1 s `MIN_RTO` repair floor plus backoff); the guard is
/// ~2x the top of that band, so a change that at least doubles the known
/// lone-tail defect fails. It is a guard, not the 250 ms mandate ceiling.
const M1_LONE_P99_GUARD_MS: f64 = 3200.0;
/// M1 lone-tail p99.9 guard. The smoke arm's 15 s window measured p999
/// 797-1636 ms and the field's slowest 60 s RTO ladder reached 5315 ms; the
/// guard clears the measured ladder with headroom, so a defect that doubles
/// the ladder fails.
const M1_LONE_P999_GUARD_MS: f64 = 8000.0;
/// M1 lone-tail `> 250 ms` sample-count guard. The field measured up to 2.7 %
/// and the smoke arm 0-0.7 %; ~3x the field band.
const M1_LONE_OVER250_GUARD_PCT: f64 = 8.0;
/// One-way delay of the field-RTT lone-tail arm. The smoke arms above run the
/// deployment's 25 ms profile (~50 ms round trip); the deployed client reports
/// a ~190 ms *minimum* round trip, so this arm moves the same request/response
/// shape onto a ~100 ms one-way path and asks whether the tail follows the
/// RTT. It does not: the repair ladder's step is a constant (the 1 s
/// `MIN_RTO` floor, `rtp/src/traffic_shaping/recovery/rto.rs`), not an
/// RTT-derived value, so the arm only gets *smaller* as the RTT grows.
const FIELD_RTT_OWD: Duration = Duration::from_millis(100);
/// M1 field-RTT lone-tail p99 guard. The band spans the two revisions this
/// crate has run: on `rtp v0.0.94` (the pin at the time) three 15 s runs measured p99
/// 427-719 ms, and on the landed `rtp` dev (`bdacf5c0`) four runs measured
/// 293-432 ms. The guard clears the top of the *pinned* band at ~2.1x, so a
/// change that doubles the arm's tail fails on either revision. It is a
/// regression tripwire, not the 250 ms mandate ceiling, for the same reason
/// the other impaired arms carry guards: the tail defect is open. The
/// revision-to-revision delta is recorded in `GATE.md` as the arm's reading of
/// what the landed transport bought at the field's RTT.
const M1_FIELD_RTT_P99_GUARD_MS: f64 = 1500.0;
/// M1 field-RTT lone-tail `> 250 ms` guard. The pinned arm measured 3.3-5.6 %
/// of samples over the ceiling and the landed arm 2.6-5.3 %; ~2.7x the worst
/// of the band.
const M1_FIELD_RTT_OVER250_GUARD_PCT: f64 = 15.0;
/// M2 hostile cadence-arm delivery floor (regression guard; measured 1.000).
const M2_HOSTILE_DELIVERY_FLOOR: f64 = 0.995;
/// M2 lone-tail delivery floor (measured 1.000).
const M2_LONE_DELIVERY_FLOOR: f64 = 0.995;
/// How far the measured offer count may fall below the arm's schedule before
/// the lane is no longer being offered the mandate's known throughput.
///
/// The tolerance is slack for **the transport refusing writes**, not for the
/// sender's schedule: `offer_cadence_on_deadline` owes its schedule the message
/// count by construction, so on an idle host the arms land on exactly 2400 of
/// 2400 and a shortfall means the lane would not take the load. It was
/// originally documented as slack for scheduler jitter, which was the wrong
/// instrument: the wake-count sender it was written for lost whole ticks on a
/// loaded host (2313 and 2300 of 2400 at load average 31), so the tolerance was
/// silently absorbing the host's scheduling rather than any refusal.
const M2_OFFER_TOLERANCE: f64 = 0.02;

// ───────── the deployed baseline the impaired tail must not regress past ─────

/// One metric of one impaired arm's recorded **deployed** baseline.
///
/// `median_ms` is the deployed `rtp v0.0.98` baseline's own median (six
/// full-window reps) and `limit_ms` is `mean + 4 sample standard deviations`
/// over the **wider** rep set [`M1_IMPAIRED_LIMIT_REPS`], rounded up to the next
/// whole millisecond — a *derived* limit, not a round number. `reps_min`/
/// `reps_max` carry that wider set's own range so the derivation is checkable
/// beside the run that reads it. `band` is `Some` only for a metric the gate
/// **asserts**.
struct M1ImpairedBaselineMetric {
    metric: &'static str,
    median_ms: f64,
    limit_ms: f64,
    reps_min: f64,
    reps_max: f64,
    /// The noise band as a fraction of `median_ms` when the metric is asserted,
    /// `None` when it is only reported. See [`M1_IMPAIRED_BASELINE`].
    band: Option<f64>,
}

/// One impaired arm's recorded **deployed** baseline: the `rtp` `v0.0.98`
/// source (`rtp` dev `559fc2b3` — pacer seed `INIT_SEND_RATE = 1024` with the
/// fresh-tail armour cover at 4/5 copies, `m = 6`), measured as six full-window
/// reps of the very arms below on this revision: same seeds, windows, cadence,
/// impairment and bulk shape.
///
/// The clean arm is recorded with the same baseline in `rtp_mux/GATE.md`
/// ("The deployed baseline the impaired tail must not regress past"): clean
/// `p99` **26.5 ms** (median of the same six reps, bound 27 ms).
struct M1ImpairedBaseline {
    arm: &'static str,
    metrics: [M1ImpairedBaselineMetric; 2],
}

/// Reps behind the deployed baseline's own median (the six full-window reps of
/// `rtp` `v0.0.98`).
const M1_IMPAIRED_BASELINE_REPS: usize = 6;

/// Reps behind every `limit_ms`: the six deployed baseline reps, the four
/// runner-configuration reps recorded in `rtp_mux/GATE.md`, and the twenty
/// healthy reps measured for this change — 30 fault-free full-window runs of
/// the very arms below, every one listed in `rtp_mux/GATE.md` beside the arms.
const M1_IMPAIRED_LIMIT_REPS: usize = 30;

/// The relative rise over the deployed median at which an asserted impaired
/// percentile counts as a regression. It is the **settled band** of the
/// perf-history rule (`netem-test/src/bin/perf-history.rs`, `noise_band`): an
/// impaired arm's `p50`/`p90`/`p99` get `0.40`, because the *same* hostile p99
/// has read 122.2 / 127.2 / 187.2 / 211.7 ms at fixed settings (1.7x) — a 10 %
/// band there rejects a good run about half the time. The band is applied here
/// against the deployed median rather than against the previous run's value, so
/// the same tolerance that separates two consecutive runs separates a run from
/// the recorded baseline.
const M1_IMPAIRED_PERCENTILE_BAND: f64 = 0.40;

/// The absolute rise an asserted metric must also clear, so that a percentage
/// near zero cannot reject a run: the perf-history rule's `MINIMUM_DELTA_MS`
/// (5 ms, 2 % of the M1 ceiling). For `lone_tail` the band's own absolute width
/// (`0.40 x 159 ms` = 63.6 ms) already exceeds it, so this half of the rule
/// binds on no arm here; it is kept because it is the rule, and it is the half
/// that would bite first on a metric whose median is small.
const M1_IMPAIRED_DELTA_FLOOR_MS: f64 = 5.0;

/// The deployed `rtp v0.0.98` impaired arms. The per-arm p99 reps of the six
/// deployed reps are `hostile` 218.4/156.7/210.7/168.6/164.7/152.3 and
/// `lone_tail` 155.2/176.5/162.8/177.1/151.3/151.2; the p999 reps are
/// `hostile` 306.3/226.9/255.2/225.2/208.7/243.4 and `lone_tail`
/// 670.4/937.3/198.6/410.9/288.4/267.5. Over the wider 30-rep set the p99 range
/// is `hostile` 126.0-275.0 and `lone_tail` 131.0-200.4; the p999 range is
/// `hostile` 143.8-375.5 and `lone_tail` 185.6-1015.9.
///
/// # Which bound is asserted: the stable percentile, not the order statistic
///
/// **`p99` is asserted; `p999` is reported, not enforced.** A `p99` over a
/// 15 s window is two dozen samples; a `p999` is **one** sample in a thousand —
/// `lone_tail` yields ~975 samples a run, so its `p999` is a single draw from
/// the ladder's tail — and it is therefore fixed by whether the arm's heaviest
/// recovery episode happened to land inside the window. The six deployed reps
/// alone span 198.6-937.3 ms (4.7x, coefficient of variation 0.63), the 30-rep
/// set spans 185.6-1015.9 ms (cv 0.54), and the arm has read **2723.3 ms** on a
/// healthy transport at the deployed baseline — past the `mean+3sd` limit the
/// six reps give (1321.0 ms). Enforcing an order statistic rejects good runs at
/// random, so the `p999` limit is printed with every run and asserted nowhere.
/// This is the rule the perf-history tool already settled for M1
/// (`netem-test/src/bin/perf-history.rs`, `M1_REJECTION_METRICS`:
/// `p50`/`p90`/`p99` reject a run, `p999`/`max` are reported) after three false
/// rejections (`lone_tail max`, `lone_tail p999`, and a `hostile p50` printed as
/// `+54.2%`); the same reasoning decides the bound here.
///
/// # Why the limit is four sigma over 30 reps, and why a band on top
///
/// The previous revision's limits were `mean + 3sd` over **six** reps (265 ms
/// hostile, 199 ms `lone_tail`) and they **flaked**: over 20 healthy
/// full-window runs of the deployed build the gate failed twice — `lone_tail
/// p99` 200.4 ms against 199.0, and `hostile p99` 275.0 ms against 265.0 (that
/// run also read `hostile p999` 375.5 against 348.0). Both were contented
/// windows (the same runs' clean arm read `p99` 37.5, 52.5 and 78.0 ms against
/// its usual 26.5 ms), which is the runner's normal configuration — libtest's
/// default threading runs six tests at once. Three sigma is not a rejection
/// rule at this sample size: over the 30-rep set `mean + 3sd` is **266.0 ms**
/// hostile and 208.4 ms `lone_tail`, and the hostile arm has a healthy rep at
/// **275.0 ms** — above it. Four sigma is the smallest standard margin that
/// covers the observed healthy range (297.8 ms hostile, 222.8 ms `lone_tail`),
/// and 4 is also RFC 6298's `K`, the variance margin this transport's own RTO
/// uses. On top of the limit the gate keeps the perf-history rule's second half:
/// a rise is a regression only when it clears the limit **and** the arm's own
/// measured 40 % noise band, so the effective bound is
/// `max(limit_ms, median_ms * (1 + band))` and the log prints both halves.
///
/// What still catches a genuine impaired-tail regression: the asserted `p99`
/// (a +300 ms one-way fault is +353 % on `lone_tail` and +1054 % on `hostile`,
/// far past both halves of the rule), the `> 250 ms` **share** guards
/// (`M1_HOSTILE_OVER250_GUARD_PCT` / `M1_LONE_OVER250_GUARD_PCT` — proportions
/// of the whole sample, the statistic a taller ladder moves first, and the one
/// the +300 ms fault drives to ~100 %), the coarse `p999` guards
/// (`M1_HOSTILE_P999_GUARD_MS`, `M1_LONE_P999_GUARD_MS`), and the `p99` guards.
/// The residual is stated rather than hidden: a systematic hostile-p99 rise
/// **under 79 %** (166.7 -> below 298 ms) is no longer rejected by this baseline
/// assertion, because the healthy contented spread reaches +65 %; the share
/// guards are what bound that regime.
const M1_IMPAIRED_BASELINE: [M1ImpairedBaseline; 2] = [
    M1ImpairedBaseline {
        arm: "hostile",
        metrics: [
            M1ImpairedBaselineMetric {
                metric: "p99",
                median_ms: 166.7,
                limit_ms: 298.0,
                reps_min: 126.0,
                reps_max: 275.0,
                band: Some(M1_IMPAIRED_PERCENTILE_BAND),
            },
            M1ImpairedBaselineMetric {
                metric: "p999",
                median_ms: 235.2,
                limit_ms: 405.0,
                reps_min: 143.8,
                reps_max: 375.5,
                band: None,
            },
        ],
    },
    M1ImpairedBaseline {
        arm: "lone_tail",
        metrics: [
            M1ImpairedBaselineMetric {
                metric: "p99",
                median_ms: 159.0,
                limit_ms: 223.0,
                reps_min: 131.0,
                reps_max: 200.4,
                band: Some(M1_IMPAIRED_PERCENTILE_BAND),
            },
            M1ImpairedBaselineMetric {
                metric: "p999",
                median_ms: 349.7,
                limit_ms: 1324.0,
                reps_min: 185.6,
                reps_max: 1015.9,
                band: None,
            },
        ],
    },
];

/// Read one impaired arm against its recorded deployed baseline: one
/// `(row, failure)` pair per metric. The row carries the observation, the
/// recorded median, the limit, the effective bound, whether the metric is
/// asserted and the verdict, so the run's own log is the comparison. The
/// failure is `Some` only for an **asserted** metric past its effective bound,
/// and names the arm, the metric, the observed value, the baseline, the bound
/// and the direction the way the M2 owner gate names its own breach. `arm` is
/// an arm of [`M1ImpairedBaseline`], so a caller that passes the clean arm gets
/// no rows: the clean arm is recorded but not guarded here (M1's mandate bound
/// already asserts it).
fn m1_impaired_baseline_rows(run: &ArmRun) -> Vec<(String, Option<String>)> {
    let Some(baseline) = M1_IMPAIRED_BASELINE
        .iter()
        .find(|baseline| baseline.arm == run.name)
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for reading in &baseline.metrics {
        let observed = match reading.metric {
            "p99" => run.summary.p99,
            "p999" => run.summary.p999,
            other => unreachable!("M1ImpairedBaseline names an unknown metric {other}"),
        };
        // The effective bound: the recorded derived limit, and -- for an
        // asserted percentile -- the arm's own measured noise band. A rise must
        // clear both, and the absolute floor as well.
        let band_bound = reading.band.map(|band| reading.median_ms * (1.0 + band));
        let effective_bound = band_bound.map_or(reading.limit_ms, |band_bound| {
            reading.limit_ms.max(band_bound)
        });
        let regressed = reading.band.is_some()
            && observed > effective_bound
            && observed - reading.median_ms > M1_IMPAIRED_DELTA_FLOOR_MS;
        let verdict = if reading.band.is_none() {
            "REPORTED"
        } else if regressed {
            "REGRESSED"
        } else {
            "OK"
        };
        let row = format!(
            "[m1-baseline] arm={arm:<10} metric={metric:<4} observed={observed:8.1} \
             baseline_v0.0.98={recorded:8.1} limit={limit:8.1} banded_bound={banded:8.1} \
             asserted={asserted:<5} baseline_reps={baseline_reps} limit_reps={limit_reps} \
             limit_reps_range={lo:.1}..{hi:.1} verdict={verdict}\n",
            arm = baseline.arm,
            metric = reading.metric,
            recorded = reading.median_ms,
            limit = reading.limit_ms,
            banded = effective_bound,
            asserted = reading.band.is_some(),
            baseline_reps = M1_IMPAIRED_BASELINE_REPS,
            limit_reps = M1_IMPAIRED_LIMIT_REPS,
            lo = reading.reps_min,
            hi = reading.reps_max,
        );
        let failure = regressed.then(|| {
            format!(
                "[M1] the {arm} arm's {metric} is {observed:.1} ms — WORSE than the deployed rtp v0.0.98 \
                 baseline ({recorded:.1} ms, the deployed {baseline_reps} reps' median) beyond its effective \
                 bound {bound:.1} ms (the {limit_reps} reps on record give a limit of {limit:.1} ms as their \
                 mean + 4 sample standard deviations, and the arm's own measured {band:.0}% noise band gives \
                 {banded:.1} ms; the wider of the two binds): {pct:+.1}% against the recorded baseline. M1's \
                 impaired tail is a hard floor — it must not be traded for M2's offered-load latency or the clean \
                 arm, and a candidate that does is rejected here rather than absorbed inside a guard.",
                arm = baseline.arm,
                metric = reading.metric,
                observed = observed,
                recorded = reading.median_ms,
                baseline_reps = M1_IMPAIRED_BASELINE_REPS,
                limit_reps = M1_IMPAIRED_LIMIT_REPS,
                bound = effective_bound,
                limit = reading.limit_ms,
                band = 100.0 * reading.band.unwrap_or(0.0),
                banded = band_bound.unwrap_or(reading.limit_ms),
                pct = 100.0 * (observed / reading.median_ms - 1.0),
            )
        });
        out.push((row, failure));
    }
    out
}

// ─────────────────────────────── diagnostics ─────────────────────────────────

/// Serialises the three mandate measurements: this target is run with no
/// `--test-threads` flag, and the latency assertions are wall-clock, so the
/// windows must not overlap. A `tokio` mutex (not `std`) so the guard may be
/// held across `.await` without parking a runtime worker.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The per-arm deadline every smoke arm is measured under: a ladder that starts
/// at the arm's last offer must be awaited within it. It bounds the
/// request/response arms' observation room, which no shorter timer does, and
/// the value is the one the arms were already run under — naming it is what
/// keeps the censoring instrument's room input from being a second authority.
const ARM_DEADLINE: Duration = Duration::from_secs(120);

fn quick() -> bool {
    matches!(std::env::var("MANDATE_SMOKE_QUICK").as_deref(), Ok("1"))
}

fn cadence_window() -> Duration {
    if quick() { QUICK_WINDOW } else { WINDOW }
}

fn rr_window() -> Duration {
    if quick() { RR_QUICK_WINDOW } else { RR_WINDOW }
}

fn bulk_window() -> Duration {
    if quick() {
        QUICK_BULK_WINDOW
    } else {
        BULK_WINDOW
    }
}

/// The evidence directory: `$MANDATE_CHECK_DIR` when the runner set it (it
/// always does), else a sane default under `target/` for a plain
/// `cargo test -p rtp_mux`.
fn out_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("MANDATE_CHECK_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    let root = std::env::var("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
    root.join("mandate-smoke")
}

/// The deliberate-fault selector used only by the vacuity demonstrations:
/// `M1_latency`, `M1_IMPAIRED_slow`, `M1_FIELD_RTT_slow`,
/// `M1_LOSS_MODEL_uncorrelated`,
/// `M1_LOSS_MODEL_correlated`, `M2_delivery`, `M2_offer`, `M3_starve`,
/// `M4_starve`, `M4_late`, `M4_drop`, `M4_CLEAN_LEVEL_double`,
/// `M4_CLEAN_LEVEL_slow`, `M4_HOSTILE_LEVEL_double` or
/// `M4_HOSTILE_LEVEL_slow`. Unset in every real
/// run (the runner never sets it). Faults perturb an arm's *input* — the
/// impairment or the offered payload — never the assertion, so the failure is
/// produced by the measurement path. The two `M4_CLEAN_LEVEL_*` values name
/// [`m4_clean_lane_p99_ceiling`]'s own namespace rather than M4's, and the two
/// `M4_HOSTILE_LEVEL_*` values name [`m4_hostile_lane_p99_ceiling`]'s, so a
/// probe of either arm's level assertion cannot read as a probe of M4's.
fn fault(mandate: &str) -> Option<String> {
    let value = std::env::var("MANDATE_SMOKE_FAULT").ok()?;
    let value = value.trim();
    if value.is_empty() || !value.starts_with(mandate) {
        return None;
    }
    Some(value.to_owned())
}

fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

/// One-way delay `MANDATE_SMOKE_FAULT=M1_IMPAIRED_slow` adds to **both**
/// impaired arms' links. It is a deterministic input perturbation, one ladder
/// step and a half, so the red proof of the deployed-baseline bound is
/// reproducible rather than a draw: the bound it must cross is
/// [`M1_IMPAIRED_BASELINE`]'s tightest value (`lone_tail` p99, 199 ms) while
/// the arm's own guards sit at 900 ms and 3200 ms, so the failure names the
/// baseline gate and not a superseded tripwire.
const IMPAIRED_TAIL_FAULT_SHIFT_MS: u64 = 300;

// ────────────────────────────── arm definitions ──────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Load {
    /// One [`MSG_BYTES`] message every `CADENCE * cadence_divisor`. The offer is
    /// the arm's *input*, so its schedule belongs to the arm; `cadence_divisor`
    /// is 1 for every real arm and is what the `M2_offer` fault raises to offer
    /// a fraction of the cadence the mandate names.
    Cadence {
        cadence_divisor: u32,
    },
    RequestResponse {
        depth: usize,
    },
}

#[derive(Clone)]
struct ArmSpec {
    name: &'static str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    bulk: bool,
    /// Push the bulk lane back to back instead of on the 2 MiB / 3 s clock.
    /// `false` is every existing arm; the Minecraft arm saturates.
    saturating_bulk: bool,
    load: Load,
    window: Duration,
    msg_bytes: usize,
    /// The interactive cadence. `CADENCE` for every existing arm; the
    /// Minecraft shape's 300 B / 20 ms sets its own.
    cadence: Duration,
    /// Route both lanes through one shared downstream `BottleneckShaper`, so
    /// they contend for one queue the way they do on a real egress.
    shared_shaper: bool,
    /// Attach both lanes to one congestion-signalling hub (the mechanism arm).
    cc_link: Option<rtp::cc::CcSignalHub>,
}

/// One impairment direction: fixed delay + jitter, an independent-loss
/// threshold, and an optional per-flow rate cap.
fn link(seed: u64, latency: Duration, jitter: Duration, loss: u32, rate_bps: u64) -> NetemConfig {
    NetemConfig {
        latency,
        jitter,
        rate: rate_bps,
        loss,
        seed,
        ..NetemConfig::default()
    }
}

fn hostile_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: HOSTILE_JITTER,
        loss_model: gilbert_elliott_loss(5.0, 8.0),
        seed,
        ..NetemConfig::default()
    }
}

/// [`hostile_link`] moved onto the field's ~190 ms round trip: the same GE
/// model and jitter with the one-way delay the deployed client reports.
fn field_rtt_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: FIELD_RTT_OWD,
        ..hostile_link(seed)
    }
}

/// The field-RTT lone-tail arm: the M1/M2 `lone_tail` shape (one unacked 256 B
/// message at a time, no bulk lane) at [`FIELD_RTT_OWD`]. The fault selector
/// `MANDATE_SMOKE_FAULT=M1_FIELD_RTT_slow` injects +1000 ms one-way delay on
/// both directions, the arm's own vacuity demonstration: it perturbs the arm's
/// *input*, so the failure it produces comes from the measurement path.
fn field_rtt_arm() -> ArmSpec {
    let slow = fault("M1_FIELD_RTT").is_some();
    let extra = if slow {
        Duration::from_millis(1000)
    } else {
        Duration::ZERO
    };
    let shift = |mut link: NetemConfig| {
        link.latency += extra;
        link
    };
    ArmSpec {
        name: "field_rtt",
        int_c2s: shift(field_rtt_link(41)),
        int_s2c: shift(field_rtt_link(42)),
        bulk: false,
        saturating_bulk: false,
        load: Load::RequestResponse { depth: 1 },
        window: rr_window(),
        msg_bytes: MSG_BYTES,
        cadence: CADENCE,
        shared_shaper: false,
        cc_link: None,
    }
}

/// The M1/M2 arm set, with the fault for `mandate` applied to the `clean` arm
/// when one is selected. `clean` is the mandate-bound arm; `hostile` and
/// `lone_tail` are the regression-guard arms.
fn mandate_arms(mandate: &str) -> Vec<ArmSpec> {
    let clean_fault = fault(mandate);
    let mut clean_c2s = link(41, OWD, JITTER, LOSS_2, 0);
    let mut clean_s2c = link(42, OWD, JITTER, LOSS_2, 0);
    let clean_bulk = true;
    let mut clean_cadence_divisor = 1u32;
    let clean_window = cadence_window();
    if let Some(fault) = clean_fault.as_deref() {
        match fault {
            // Blow the latency ceiling: +500 ms one-way on both directions.
            "M1_latency" => {
                clean_c2s.latency = OWD + Duration::from_millis(500);
                clean_s2c.latency = OWD + Duration::from_millis(500);
            }
            // Suppress goodput: starve the interactive lane to 90 % iid loss, so
            // the lane's delivery falls and the backlog its unrepaired data
            // builds shows up as latency. Either failure names the arm, the
            // observed value and the bound.
            "M2_delivery" => {
                clean_c2s.loss = loss_pct(90);
                clean_s2c.loss = loss_pct(90);
            }
            // Offer the clean arm a tenth of the cadence the mandate names:
            // the lane is healthy and delivers everything it is offered, so the
            // only clause that can fail is the offer — the vacuity
            // demonstration for M2's offer floor, which is the premise that
            // stops the latency assertion passing against an unloaded lane.
            "M2_offer" => clean_cadence_divisor = 10,
            _ => {}
        }
    }
    let cadence = cadence_window();
    // The impaired arms' degeneracy, and the red proof of the deployed-baseline
    // bound: `M1_IMPAIRED_slow` shifts the hostile *and* lone_tail links by one
    // fixed delay, so the tail those arms report is produced by their own
    // impairment rather than by an assertion. The clean arm is untouched, so a
    // probe of the bound cannot read as a probe of the mandate ceiling.
    let impaired_shift = if fault("M1_IMPAIRED").is_some() {
        Duration::from_millis(IMPAIRED_TAIL_FAULT_SHIFT_MS)
    } else {
        Duration::ZERO
    };
    let impaired_link = |seed: u64| {
        let mut link = hostile_link(seed);
        link.latency += impaired_shift;
        link
    };
    vec![
        ArmSpec {
            name: "clean",
            int_c2s: clean_c2s,
            int_s2c: clean_s2c,
            bulk: clean_bulk,
            saturating_bulk: false,
            load: Load::Cadence {
                cadence_divisor: clean_cadence_divisor,
            },
            window: clean_window,
            msg_bytes: MSG_BYTES,
            cadence: CADENCE,
            shared_shaper: false,
            cc_link: None,
        },
        ArmSpec {
            name: "hostile",
            int_c2s: impaired_link(41),
            int_s2c: impaired_link(42),
            bulk: true,
            saturating_bulk: false,
            load: Load::Cadence { cadence_divisor: 1 },
            window: cadence,
            msg_bytes: MSG_BYTES,
            cadence: CADENCE,
            shared_shaper: false,
            cc_link: None,
        },
        ArmSpec {
            name: "lone_tail",
            int_c2s: impaired_link(41),
            int_s2c: impaired_link(42),
            bulk: false,
            saturating_bulk: false,
            load: Load::RequestResponse { depth: 1 },
            window: rr_window(),
            msg_bytes: MSG_BYTES,
            cadence: CADENCE,
            shared_shaper: false,
            cc_link: None,
        },
    ]
}

// ───────────────────────────────── the runner ────────────────────────────────

struct ArmRun {
    name: &'static str,
    summary: HolSummary,
    /// The measured latency samples: the server's one-way reading for the
    /// cadence arms, the client's own round trip for the lone-tail arm (the
    /// deadline the application waits on).
    samples: Vec<f64>,
    /// `(elapsed seconds, latency ms)` per sample, in delivery order.
    timeline: Vec<(f64, f64)>,
    int_c2s_wire_bytes: u64,
    /// Datagrams the interactive lane's c2s direction accepted before
    /// impairment, i.e. the datagrams its own link offered to the loss
    /// process over the window ([`LadderInputs::datagrams`]).
    int_c2s_packets: u64,
    /// The same direction's full counter snapshot: the loss the link *applied*
    /// (`dropped / received`, with no rate shaper and no queue limit on this
    /// link to mix an overflow drop in) and the impairment it actually ran
    /// (`delayed`). The loss a model names is a claim about the link, and this
    /// is the link confirming or contradicting it.
    int_c2s_counters: netem_test::Counters,
    /// The interactive lane's own send-path evidence, tapped from rtp's metrics
    /// events and cumulative counters when the arm was run with an observer.
    /// [`CoverWire::default`] (all zero) is the exact meaning of "this arm was
    /// not observed", so a reader must not read a zero as a measurement.
    cover: CoverWire,
    /// Whether the arm ran with [`cover_wire_observer`] attached.
    cover_observed: bool,
    /// The s2c direction's counters, for the same reading on the return path.
    int_s2c_counters: netem_test::Counters,
    /// The payload the arm offered, `sent * msg_bytes`: M2's *input*, printed
    /// so the throughput the mandate asserts on is a number the run carries.
    offered_bytes: u64,
    bulk_sink_bytes: u64,
    bulk_wire_bytes: u64,
    window: Duration,
    wall: Duration,
}

/// Offer request/response rounds until `run_for` elapses: write `depth`
/// timestamped [`MSG_BYTES`] frames back-to-back, read their echoes, then
/// offer the next round. Returns the offered round count and one
/// `(elapsed seconds, round-trip ms)` per completed round. At `depth` 1 the
/// tracked tail is *lone* — the only unacked data packet on the connection.
async fn request_response_timed(
    write: &mut (impl AsyncWrite + Unpin),
    read: &mut (impl AsyncRead + Unpin),
    base: Instant,
    tag: u8,
    depth: usize,
    msg_bytes: usize,
    run_for: Duration,
) -> (u64, Vec<(f64, f64)>) {
    let payload_bytes = msg_bytes - 12;
    let payload: Vec<u8> = (0..payload_bytes).map(|i| (i % 251) as u8).collect();
    let mut frame = Vec::with_capacity(msg_bytes);
    let mut echoed = vec![0u8; msg_bytes];
    let mut sent = 0u64;
    let mut rounds = Vec::new();
    if write.write_all(&[tag]).await.is_err() {
        return (sent, rounds);
    }
    let start = Instant::now();
    while start.elapsed() < run_for {
        let sent_us = base.elapsed().as_micros() as u64;
        for _ in 0..depth {
            frame.clear();
            frame.extend_from_slice(&((msg_bytes as u32).to_le_bytes()));
            frame.extend_from_slice(&payload);
            frame.extend_from_slice(&sent_us.to_le_bytes());
            if write.write_all(&frame).await.is_err() {
                return (sent, rounds);
            }
            sent += 1;
        }
        for _ in 0..depth {
            if read.read_exact(&mut echoed).await.is_err() {
                return (sent, rounds);
            }
            let elapsed = base.elapsed().as_secs_f64();
            rounds.push((elapsed, elapsed * 1000.0 - sent_us as f64 / 1000.0));
        }
    }
    (sent, rounds)
}

/// A periodic bulk burst: `burst_bytes` offered every `period`, as fast as the
/// transport accepts, for the duration of the run (the production load shape).
async fn periodic_burst(
    write: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    burst_bytes: usize,
    period: Duration,
    ramp: Duration,
    run_for: Duration,
) -> u64 {
    let start = Instant::now();
    tokio::time::sleep(ramp).await;
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    let mut cursor = 0usize;
    let mut written = 0u64;
    loop {
        if start.elapsed() >= run_for {
            break;
        }
        let mut remaining = burst_bytes;
        while remaining > 0 {
            if start.elapsed() >= run_for {
                return written;
            }
            let avail = payload.len() - cursor;
            let take = remaining.min(avail);
            if write
                .write_all(&payload[cursor..cursor + take])
                .await
                .is_err()
            {
                return written;
            }
            cursor = (cursor + take) % payload.len();
            remaining -= take;
            written += take as u64;
        }
        interval.tick().await;
    }
    written
}

/// Offer one timestamped [`MSG_BYTES`] message per `interval` over `run_for`
/// on an **absolute-deadline schedule**, returning how many the transport
/// accepted.
///
/// The arm's offer is M2's *input* — the known throughput the mandate names —
/// so it must be a property of the schedule rather than of the test host's
/// ability to wake a 200 Hz timer. The pinned cadence sender
/// (`rtp::testkit::rtp::send_timestamped_messages`) drives the same cadence
/// through `tokio::time::interval` with `MissedTickBehavior::Delay`, which
/// **drops** a tick whenever the runtime wakes the task more than one cadence
/// late, so the count the offer floor reads measures the host. Measured on this
/// machine at load average 31 (16 spinners on 10 cores) that sender offered
/// 2313 and 2300 of the 2400 messages its schedule requires, while the
/// transport accepted **every** write it attempted (attempts == accepts, zero
/// write errors, 6 ms of `write_all` await over a 12 s window, and latenesses
/// at the window's quarter points of 139/268/324/435 ms and 138/232/358/500 ms
/// — accumulating from the start rather than stalling once).
/// So the shortfall was the sender's own wake schedule, not the lane refusing
/// load, and the arm failed on host load rather than on the product.
///
/// This sender instead owes the schedule its full `floor(run_for / interval)`
/// messages and, when a wake is late, writes the messages whose deadline has
/// already passed back-to-back. The count is then host-independent, and a
/// shortfall means the **transport refused the offer** — which is what the
/// offer floor exists to catch. It ends no earlier than its schedule, by at
/// most one wake's lateness.
///
/// The frame layout is the pinned encoder's, which the arm's sink decodes;
/// `clean_delivered` (`received == sent`) fails loudly if the two ever drift.
async fn offer_cadence_on_deadline(
    write: &mut (impl AsyncWrite + Unpin),
    base: Instant,
    msg_bytes: usize,
    interval: Duration,
    run_for: Duration,
) -> u64 {
    assert!(msg_bytes >= 12, "message framing needs at least 12 bytes");
    // Integer nanoseconds, so the schedule count is exact rather than a float
    // floor that can land a message short.
    let schedule = (run_for.as_nanos() / interval.as_nanos()) as u64;
    let payload_bytes = msg_bytes - 12;
    let payload: Vec<u8> = (0..payload_bytes).map(|i| (i % 251) as u8).collect();
    let mut frame = Vec::with_capacity(msg_bytes);
    let start = Instant::now();
    let mut deadline = Duration::ZERO;
    let mut sent = 0u64;
    while sent < schedule {
        while sent < schedule && start.elapsed() >= deadline {
            let sent_us = base.elapsed().as_micros() as u64;
            frame.clear();
            frame.extend_from_slice(&((msg_bytes as u32).to_le_bytes()));
            frame.extend_from_slice(&payload);
            frame.extend_from_slice(&sent_us.to_le_bytes());
            if write.write_all(&frame).await.is_err() {
                return sent;
            }
            sent += 1;
            deadline += interval;
        }
        if sent < schedule {
            tokio::time::sleep(deadline.saturating_sub(start.elapsed())).await;
        }
    }
    sent
}

/// Run one dual-lane smoke arm and read back everything the three mandates
/// need from it: the latency summary, the timeline (for the panel), the
/// interactive lane's own client->server wire, the offered payload, and the
/// bulk sink/shaper counters.
async fn run_arm(spec: ArmSpec) -> ArmRun {
    run_arm_with(spec, LaneRtpConfig::frame_reordering(true, prompt_tuning())).await
}

/// [`run_arm`] with the interactive lane's transport configuration named by the
/// caller. The default `run_arm` passes the deployment's own
/// `frame_reordering(true, prompt_tuning())`, so every arm's behaviour is
/// unchanged; this entry point exists so a *lever* probe can measure an
/// alternative lane policy on the very same arm without retuning it.
async fn run_arm_with(spec: ArmSpec, int_rtp: LaneRtpConfig) -> ArmRun {
    run_arm_observed(spec, int_rtp, None).await
}

/// The interactive lane's own send-path evidence for one arm: what the lane's
/// transmission actually put on the wire, read from rtp's own metrics events
/// and cumulative counters -- never restated from a constant.
///
/// `armor_duplicates` counts `RetransmissionArmorDuplicate` observations, each
/// of which is one armour copy datagram the lane *actually wrote* (rtp logs the
/// event only after the underlay send succeeds). `rungs` is the newest
/// snapshot's `retransmission_counters.attempts + tail_probes`: the send
/// space's own count of the transmissions it fired as repairs, one per rung,
/// whether a tail-loss probe or a full-RTO selection (that is exactly the pair
/// `pkt_send_space::the_lone_tail_ladder_is_measured_from_the_wire_and_steps_by_the_repair_floor`
/// reads off the same counters). `parity_sent` is the newest snapshot's
/// `fec.parity_sent`, the parity datagrams the FEC flush actually emitted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CoverWire {
    armor_duplicates: u64,
    rungs: u64,
    parity_sent: u64,
    /// FEC groups the flush actually emitted parity for, and the histogram of
    /// their data-symbol counts (`1`, `2-4`, `5-7`, `8`). Printed so the parity
    /// datum count is explained by the groups it came from rather than read as
    /// a per-message constant.
    groups_flushed: u64,
    group_sizes: [u64; 4],
}

/// The cell [`cover_wire_observer`] writes and the arm reads once its run is
/// over. Cumulative counters are recorded as a running maximum, so a reading
/// cannot regress when the underlay is momentarily backpressured.
#[derive(Default)]
struct CoverTaps {
    armor_duplicates: AtomicU64,
    rungs: AtomicU64,
    parity_sent: AtomicU64,
    groups_flushed: AtomicU64,
    group_sizes: [AtomicU64; 4],
}

impl CoverTaps {
    fn snapshot(&self) -> CoverWire {
        CoverWire {
            armor_duplicates: self.armor_duplicates.load(Ordering::Relaxed),
            rungs: self.rungs.load(Ordering::Relaxed),
            parity_sent: self.parity_sent.load(Ordering::Relaxed),
            groups_flushed: self.groups_flushed.load(Ordering::Relaxed),
            group_sizes: self
                .group_sizes
                .each_ref()
                .map(|slot| slot.load(Ordering::Relaxed)),
        }
    }
}

/// An observer that taps the interactive lane's per-message wire.
///
/// A state snapshot is captured only for the events the decomposition needs --
/// the armour duplicate (one per copy datagram, so the copy count is the event
/// count) and the two application-write events (`SendFrameBuffer` on the
/// frame-delivery lane every interactive arm here runs, and `SendDataBuffer`
/// on the byte-stream lane; both are where the transport takes the snapshot
/// that carries the cumulative repair and parity counters) -- while the
/// high-rate packet attempts take no snapshot at all: the transport counts
/// those itself, so an observer that scanned send state for each of them would
/// pay a cost the measurement does not need. RTT samples are skipped entirely.
fn cover_wire_observer() -> (MetricsObserver, Arc<CoverTaps>) {
    let taps = Arc::new(CoverTaps::default());
    let observer = MetricsObserver::selective(
        |event, _elapsed| match event {
            MetricsEvent::RetransmissionArmorDuplicate
            | MetricsEvent::SendFrameBuffer
            | MetricsEvent::SendDataBuffer => MetricsInterest::Snapshot,
            MetricsEvent::RttSample => MetricsInterest::Skip,
            _ => MetricsInterest::EventOnly,
        },
        {
            let taps = Arc::clone(&taps);
            move |observation: MetricsObservation| {
                if observation.event == MetricsEvent::RetransmissionArmorDuplicate {
                    taps.armor_duplicates.fetch_add(1, Ordering::Relaxed);
                }
                let Some(snapshot) = observation.snapshot else {
                    return;
                };
                let counters = snapshot.retransmission_counters;
                taps.rungs
                    .fetch_max(counters.attempts + counters.tail_probes, Ordering::Relaxed);
                if let Some(fec) = snapshot.fec_counters {
                    taps.parity_sent
                        .fetch_max(fec.parity_sent, Ordering::Relaxed);
                    taps.groups_flushed
                        .fetch_max(fec.groups_flushed, Ordering::Relaxed);
                    let buckets = fec.flushed_group_sizes;
                    for (slot, value) in taps.group_sizes.iter().zip([
                        buckets.one,
                        buckets.two_to_four,
                        buckets.five_to_seven,
                        buckets.full_eight,
                    ]) {
                        slot.fetch_max(value, Ordering::Relaxed);
                    }
                }
            }
        },
    );
    (observer, taps)
}

/// [`run_arm_with`] with an optional metrics observer on the interactive lane.
/// `None` reproduces `run_arm_with` exactly, which is why every arm that does
/// not ask for the decomposition is measured on the connection it always was.
async fn run_arm_observed(
    spec: ArmSpec,
    int_rtp: LaneRtpConfig,
    observed: Option<(MetricsObserver, Arc<CoverTaps>)>,
) -> ArmRun {
    let ArmSpec {
        name,
        int_c2s,
        int_s2c,
        bulk,
        saturating_bulk,
        load,
        window,
        msg_bytes,
        cadence,
        shared_shaper,
        cc_link,
    } = spec;
    let wall = Instant::now();
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let bulk_c2s = link(43, OWD, JITTER, LOSS_2, BULK_RATE_BPS);
    let bulk_s2c = link(44, OWD, JITTER, LOSS_2, BULK_RATE_BPS);
    let bulk_off = NetemConfig::default();

    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let outcome = tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            // One downstream queue both lanes cross, when the arm asks for it:
            // the shared-buffer contention the CC signal exists to order.
            let shared_uplink = shared_shaper.then(|| BottleneckShaper::new(SHARED_UP_RATE_BPS, 0));
            let mut bulk_c2s_link = if bulk {
                bulk_c2s.clone()
            } else {
                bulk_off.clone()
            };
            let bulk_s2c_link = if bulk {
                bulk_s2c.clone()
            } else {
                bulk_off.clone()
            };
            let mut int_c2s_link = int_c2s.clone();
            if shared_shaper {
                // The shared shaper *is* the uplink rate where both lanes' writes
                // contend; a per-link rate on the same direction is a
                // double-shape the instrument refuses.
                int_c2s_link.rate = 0;
                bulk_c2s_link.rate = 0;
            }
            let int_pair = match &shared_uplink {
                Some(up) => NetemPair::spawn_shared(
                    int_addr,
                    int_c2s_link.clone(),
                    int_s2c.clone(),
                    Some(up.clone()),
                    None,
                )
                .unwrap(),
                None => NetemPair::spawn(int_addr, int_c2s_link.clone(), int_s2c.clone()).unwrap(),
            };
            let bulk_pair = match &shared_uplink {
                Some(up) => NetemPair::spawn_shared(
                    bulk_addr,
                    bulk_c2s_link.clone(),
                    bulk_s2c_link.clone(),
                    Some(up.clone()),
                    None,
                )
                .unwrap(),
                None => NetemPair::spawn(bulk_addr, bulk_c2s_link.clone(), bulk_s2c_link.clone())
                    .unwrap(),
            };
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via_cc_link(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                observed.as_ref().map(|(observer, _)| observer.clone()),
                None,
                cc_link,
            )
            .await
            .unwrap();

            let (mut lat_read, mut lat_write) = opener.open(LaneClass::Interactive).await.unwrap();
            let bulk_write = if bulk {
                let (mut bulk_read, bulk_write) = opener.open(LaneClass::Bulk).await.unwrap();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        while let Ok(n) = bulk_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                Some(bulk_write)
            } else {
                None
            };

            // The sink publishes one row per parsed frame into a bounded
            // channel; a collector drains it for the whole arm, so a lane whose
            // sample rate exceeds the channel depth cannot overflow it or leave
            // samples undrained.
            let collector_sink = Arc::new(Mutex::new(Vec::<(f64, f64)>::new()));
            let sink_for_task = Arc::clone(&collector_sink);
            let task_tx_int = task_tx.clone();
            let interactive = async move {
                submit_test_task(
                    &task_tx_int,
                    Box::pin(async move {
                        while let Some((_tag, latency)) = latencies.recv().await {
                            sink_for_task
                                .lock()
                                .unwrap()
                                .push((base.elapsed().as_secs_f64(), latency));
                        }
                    }),
                );
                match load {
                    Load::Cadence { cadence_divisor } => {
                        submit_test_task(
                            &task_tx_int,
                            Box::pin(async move {
                                let mut buf = vec![0u8; 8 * 1024];
                                while let Ok(n) = lat_read.read(&mut buf).await {
                                    if n == 0 {
                                        break;
                                    }
                                }
                            }),
                        );
                        let sent = if lat_write.write_all(b"L").await.is_err() {
                            0
                        } else {
                            offer_cadence_on_deadline(
                                &mut lat_write,
                                base,
                                msg_bytes,
                                cadence * cadence_divisor,
                                window,
                            )
                            .await
                        };
                        (sent, Vec::new())
                    }
                    Load::RequestResponse { depth } => {
                        request_response_timed(
                            &mut lat_write,
                            &mut lat_read,
                            base,
                            ECHO_TAG,
                            depth,
                            msg_bytes,
                            window,
                        )
                        .await
                    }
                }
            };
            let bulk_fut = async {
                let Some(mut write) = bulk_write else {
                    return 0;
                };
                if write.write_all(b"B").await.is_err() {
                    return 0;
                }
                let payload = cyclic_payload(BULK_BURST_BYTES);
                if saturating_bulk {
                    // Back to back, no clock: the offer is whatever the window
                    // admits, so the shared queue is the limit the arm measures.
                    let deadline = Instant::now() + window;
                    let mut written = 0u64;
                    while Instant::now() < deadline {
                        if write.write_all(&payload).await.is_err() {
                            break;
                        }
                        written += BULK_BURST_BYTES as u64;
                    }
                    written
                } else {
                    periodic_burst(
                        &mut write,
                        &payload,
                        BULK_BURST_BYTES,
                        BULK_PERIOD,
                        BULK_RAMP,
                        window,
                    )
                    .await
                }
            };
            let ((sent, rtts), _bulk_written) = tokio::join!(interactive, bulk_fut);

            tokio::time::sleep(GRACE).await;
            let int_c2s = int_pair.stats_c2s();
            let int_c2s_wire_bytes = int_c2s.forwarded_bytes;
            let int_c2s_packets = int_c2s.received;
            let bulk_sink_bytes = bulk_counter.load(Ordering::Relaxed);
            let bulk_wire_bytes = bulk_pair.stats_c2s().forwarded_bytes;
            // The collector's sink: which rows become the arm's measured
            // sample is the load shape's choice — the server's one-way reading
            // for a cadence arm, the client's round trip for a request/response
            // arm.
            let collected = std::mem::take(&mut *collector_sink.lock().unwrap());
            let (samples, timeline) = match load {
                Load::Cadence { .. } => {
                    let samples: Vec<f64> = collected.iter().map(|(_, l)| *l).collect();
                    (samples, collected)
                }
                Load::RequestResponse { .. } => {
                    let samples: Vec<f64> = rtts.iter().map(|(_, l)| *l).collect();
                    (samples, rtts)
                }
            };
            let int_s2c = int_pair.stats_s2c();
            int_pair.stop();
            bulk_pair.stop();
            (
                sent,
                samples,
                timeline,
                int_c2s_wire_bytes,
                int_c2s_packets,
                int_c2s,
                int_s2c,
                bulk_sink_bytes,
                bulk_wire_bytes,
            )
        })
        .await;
    let (
        sent,
        samples,
        timeline,
        int_c2s_wire_bytes,
        int_c2s_packets,
        int_c2s_counters,
        int_s2c_counters,
        bulk_sink_bytes,
        bulk_wire_bytes,
    ) = outcome;
    // Read the taps only after the run: the observer's last snapshot is the
    // newest cumulative counters the lane published, and a reading taken while
    // the arm was still running would under-count its own tail.
    let cover_observed = observed.is_some();
    let cover = observed
        .as_ref()
        .map(|(_, taps)| taps.snapshot())
        .unwrap_or_default();
    let received = samples.len() as u64;
    let bulk_active_secs = if bulk {
        (window.saturating_sub(BULK_RAMP)).as_secs_f64()
    } else {
        0.0
    };
    let offered_bytes = sent.saturating_mul(msg_bytes as u64);
    let summary = summarize(
        samples.clone(),
        sent,
        received,
        bulk_wire_bytes,
        bulk_active_secs,
    );
    ArmRun {
        name,
        summary,
        samples,
        timeline,
        int_c2s_wire_bytes,
        int_c2s_packets,
        int_c2s_counters,
        cover,
        cover_observed,
        int_s2c_counters,
        offered_bytes,
        bulk_sink_bytes,
        bulk_wire_bytes,
        window,
        wall: wall.elapsed(),
    }
}

// ──────────────────── the M1/M2 shared arm measurement ───────────────────────

/// The arm runs M1 and M2 both read, measured once.
///
/// [`mandate_arms`] returns the **same** three arms for M1 and M2 — same names,
/// impairment, seeds and windows — and the two mandates differ only in which
/// fields of each [`ArmRun`] they assert on ([`m1_rows`] versus [`m2_rows`]), so
/// measuring them twice is a duplicated run rather than extra coverage.
/// Whichever test reaches this cache first measures the arms and stores them;
/// the other reads the very same runs.
///
/// The state is keyed by [`fault`]'s selection, the one input that changes the
/// arms. A fault perturbs an arm's *input* (impairment or offered load), which
/// makes it a different measurement, so a fault run is **never** stored and
/// never served: only the clean `MANDATE_SMOKE_FAULT`-unset run — fault key
/// `""` — is cacheable. At most one mandate matches a given fault value
/// ([`fault`] returns `Some` only for the mandate whose prefix the value
/// carries), so a single slot is a complete cache. That is what keeps the
/// deliberate-fault isolation intact: `MANDATE_SMOKE_FAULT=M2_delivery` moves
/// M2 alone, and a clean M1 can never inherit it.
static ARM_RUNS: std::sync::Mutex<Option<(String, Arc<Vec<ArmRun>>)>> = std::sync::Mutex::new(None);

/// The three [`mandate_arms`] runs, measured unless already cached under
/// `mandate`'s fault key. The arms themselves are untouched: the same specs,
/// seeds, windows, cadence and `GRACE` as before, driven through the same
/// [`run_arm`] and [`with_timeout`]; only the duplicated execution is gone.
/// The cache lock is never held across an `await` — [`SERIAL`] already serialises
/// the callers, so the critical sections are two plain field reads.
async fn mandate_runs(mandate: &str) -> Arc<Vec<ArmRun>> {
    mandate_runs_with(mandate, true).await
}

/// The same runs for a caller that does **not** own the arms' attribution.
///
/// `tools/mandate-check` attributes an arm row to the mandate whose `MANDATE`
/// line follows it, so a reader that is not the mandate asserting on the arms
/// must not print them: the rows would land in the next mandate's section and
/// the runner refuses a cell an arm covers none of (measured: the censoring
/// instrument's reprint of the M1 rows, finishing after the M1 and M2 verdict
/// lines, was attributed to M4 as `M4/clean` — `M4/hostile`, `M4/lone_tail`).
/// A quiet caller therefore prints no arm row on either path — cache hit or
/// cache miss — and the mandate that does assert on them still prints its own.
async fn mandate_runs_quiet(mandate: &str) -> Arc<Vec<ArmRun>> {
    mandate_runs_with(mandate, false).await
}

async fn mandate_runs_with(mandate: &str, report_arms: bool) -> Arc<Vec<ArmRun>> {
    let key = fault(mandate).unwrap_or_default();
    let cached = ARM_RUNS
        .lock()
        .expect("the arm-run cache mutex is never poisoned")
        .clone();
    if let Some((cached_key, runs)) = cached
        && cached_key == key
    {
        eprintln!(
            "[mandate-smoke] {mandate} reads the arm runs already measured under the {cached_key:?} fault key"
        );
        // Both mandates report the arms they assert on: `tools/mandate-check`
        // attributes every arm line to the mandate whose `MANDATE` line follows
        // it and refuses a mandate with no arm line (`M2/clean`, `M2/hostile`
        // and `M2/lone_tail` are declared cells in `tools/mandate-arms.json`).
        // These rows are the measurement the mandate reads, reprinted under
        // the reading mandate's attribution; they are not a second run.
        if report_arms {
            for run in runs.iter() {
                print_arm(run);
            }
        }
        return runs;
    }
    let runs = Arc::new(measure_arms(mandate, report_arms).await);
    if key.is_empty() {
        *ARM_RUNS
            .lock()
            .expect("the arm-run cache mutex is never poisoned") = Some((key, Arc::clone(&runs)));
    }
    runs
}

/// Measure the [`mandate_arms`] set in order, printing each arm's row, exactly
/// as the M1 and M2 runner loops used to before the measurement was shared.
/// `report_arms` is false for a caller that is not the mandate asserting on
/// them, which owns none of the rows' attribution ([`mandate_runs_quiet`]).
async fn measure_arms(mandate: &str, report_arms: bool) -> Vec<ArmRun> {
    let mut runs = Vec::new();
    for spec in mandate_arms(mandate) {
        let label = format!("{}/{}", mandate.to_lowercase(), spec.name);
        let run = with_timeout(ARM_DEADLINE, &label, run_arm(spec)).await;
        if report_arms {
            print_arm(&run);
        }
        runs.push(run);
    }
    runs
}

fn over250_count(samples: &[f64]) -> usize {
    samples.iter().filter(|x| **x > M1_CEILING_MS).count()
}

fn over250_pct(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        100.0 * over250_count(samples) as f64 / samples.len() as f64
    }
}

/// A nearest-rank CDF on the mandated 0-100 percentile axis: `(latency ms,
/// percentile)` pairs, so the renderer can plot it without deriving anything.
fn cdf_points(samples: &[f64], points: usize) -> Vec<(f64, f64)> {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if sorted.is_empty() {
        return vec![(0.0, 0.0)];
    }
    let mut out = Vec::with_capacity(points);
    for index in 0..points {
        let pct = 100.0 * index as f64 / (points - 1).max(1) as f64;
        let rank = ((sorted.len() - 1) as f64 * pct / 100.0).round() as usize;
        out.push((sorted[rank.min(sorted.len() - 1)], pct));
    }
    out
}

fn print_arm(run: &ArmRun) {
    let s = &run.summary;
    let row = format!(
        "[mandate-smoke {name:<9}] sent={sent:>5} recv={recv:>5} delivery={del:.3} \
         p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} p999={p999:7.1} max={max:8.1} \
         over250={o25:>4} wire={w:>10}B offered={offered:>10}B bulk_sink={bs:>10}B bulk_wire={bw:>10}B wall={wall:.1}s window={win:?}\n",
        name = run.name,
        sent = s.sent,
        recv = s.received,
        del = s.delivery_pct,
        p50 = s.p50,
        p90 = s.p90,
        p99 = s.p99,
        p999 = s.p999,
        max = s.max,
        o25 = over250_count(&run.samples),
        w = run.int_c2s_wire_bytes,
        offered = run.offered_bytes,
        bs = run.bulk_sink_bytes,
        bw = run.bulk_wire_bytes,
        wall = run.wall.as_secs_f64(),
        win = run.window,
    );
    // One locked `write_all` of a whole row: `eprintln!` issues one write per
    // format segment, and the four smoke tests share one merged stdout/stderr
    // stream, so a row printed while another test finishes can be split
    // mid-field and become unparseable for `tools/mandate-check`'s arm-line
    // reader. A single write under `PIPE_BUF` cannot interleave.
    let mut stderr = std::io::stderr().lock();
    let _ = std::io::Write::write_all(&mut stderr, row.as_bytes());
}

// ───────────────────────────── evidence writing ──────────────────────────────

fn write_evidence(
    dir: &Path,
    mandate: &str,
    declaration: &str,
    rows: &[(String, String, f64, f64)],
) {
    std::fs::create_dir_all(dir)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot create evidence directory {dir:?}: {e}"));
    std::fs::write(dir.join(format!("{mandate}.json")), declaration)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot write declaration: {e}"));
    let mut csv = String::from("panel,series,x,y\n");
    for (panel, series, x, y) in rows {
        csv.push_str(&format!("{panel},{series},{x:.6},{y:.6}\n"));
    }
    std::fs::write(dir.join(format!("{mandate}.csv")), csv)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot write data CSV: {e}"));
    eprintln!("[mandate-smoke] wrote {mandate}.json + {mandate}.csv into {dir:?}");
}

fn verdict(pass: bool) -> &'static str {
    if pass { "PASS" } else { "FAIL" }
}

// ─────────────── the repair ladder and the window that must hold it ───────────
//
// The interactive lane repairs a lost lone tail with a ladder: one lost tail is
// retransmitted as `TAIL_DATAGRAMS_PER_TRANSMISSION` datagrams carrying the
// same message, and each further rung waits `LADDER_STEP_MS`. A loss burst is
// consumed one forwarded datagram at a time (the lone tail is the only source
// on its direction, so nothing else drains it), so a burst of `l` datagrams
// yields `floor(l / m)` rungs and the ladder's wall clock is
// `floor(l / m) * step` — a burst property, not a cadence property. An arm
// therefore needs an observation window of `rungs * step + rtt`, and its own
// drain decides how much of that it can see:
//
// * a **cadence** arm's samples come from the server sink, so a ladder still
//   running when the offer window closes is observed only if it completes
//   inside `GRACE` — the drain the summary is read after;
// * a **request/response** arm awaits each round's echo inside the offer loop,
//   so a ladder that starts at the last offer is observed whenever it
//   finishes and the arm's own `with_timeout` is the only bound.
//
// A window that is shorter than one of those requirements truncates the climb
// it reports: the reported maximum is then a *lower bound*, and the arm
// under-reports by construction. [`censoring`] is the instrument that says so
// from the arm's own per-sample series, and [`LadderInputs`] is the arithmetic
// that says what the window had to be.

/// Datagrams one interactive tail transmission emits: the primary plus its
/// armour copies and the message-sized parity symbol. The value is the
/// fresh-tail armour's `primary + 5 copies` cover (`m = 6`), **derived** from
/// `rtp`'s armour configuration and recorded in `rtp/GATE.md` -- it is a
/// declaration about the pinned transport, not a reading of any run in this
/// file.
///
/// `m1_lone_tail_cover_wire` is the arm that measures it, and its measurement
/// does **not** reproduce this number: on `rtp v0.0.101` (the pin at the time) the
/// deployment's own lone-tail lane writes `1 + 3.84 copies + 1.99 parity`
/// datagrams per transmission, and its FEC flush emits ~2 single-symbol parity
/// groups per message rather than the one this budget assumes. The constant is
/// left at its declared value because retuning it would retune the rung-count
/// law below, which is a frozen arm; the measurement is recorded beside the
/// derivation instead of silently replacing it.
const TAIL_DATAGRAMS_PER_TRANSMISSION: u64 = 6;

/// The repair ladder's steady rung interval: rtp's post-probe repair-deadline
/// floor, `TailLossProber::TAIL_PROBED_MIN_RTO` = 300 ms
/// (`rtp/src/traffic_shaping/recovery/tlp.rs`). The competing term, the
/// corroborated deadline `srtt + max(rttvar, srtt / 4)`, is below the floor on
/// every M1 arm — at the field arm's `FIELD_RTT_OWD` it is
/// `200 + max(50, 50) = 250 ms` — so the floor is the step on all of them, and
/// rtp's own ladder probe measures the same 300 ms spacing at 50 ms and 190 ms
/// round trips with and without jitter.
const LADDER_STEP_MS: f64 = 300.0;

/// The fewest samples a series needs before its tail is worth reading: a
/// terminal ascent is a coincidence of the last order statistic, so a series
/// short enough for one to be unremarkable is reported unclassifiable rather
/// than clear.
const CENSORING_MIN_SAMPLES: usize = 32;

/// The longest gap between two consecutive samples that can still be one climb.
/// A ladder's rungs are one `step` apart, so a pair further apart than that is
/// two separate observations rather than a rising run — and the M1-latency
/// panel draws the gap between them as a straight line, which is what makes a
/// long round trip look like a wall: the final run's lone-tail record (1892.3 ms
/// at 11.004 s) follows a 0.4 s silence and is drawn as a near-vertical climb
/// even though it is a single completed round.
const MAX_CLIMB_GAP_MS: f64 = LADDER_STEP_MS;

/// The arm's own ladder inputs, every one read from the arm's configuration or
/// from its own counters — none assumed and none tuned.
struct LadderInputs {
    /// Datagrams one tail transmission emits -- **derived**, not measured:
    /// [`TAIL_DATAGRAMS_PER_TRANSMISSION`], which `m1_lone_tail_cover_wire`
    /// measures and does not reproduce. Every field of the row this feeds is a
    /// prediction *from* this declared value, so the row's `rungs=` is the
    /// law's output and not a reading of the arm.
    datagrams_per_transmission: u64,
    /// The steady rung interval, ms.
    step_ms: f64,
    /// Mean loss-burst length of the arm's own impairment, in forwarded
    /// datagrams; `1` for independent loss, which has no burst to consume.
    mean_burst: f64,
    /// Long-run loss probability of the arm's own impairment.
    loss: f64,
    /// Datagrams the arm's own c2s link accepted before impairment.
    datagrams: u64,
    /// Base round trip, ms, from the arm's own one-way delay.
    rtt_ms: f64,
    /// How long after the arm's last offer a ladder may still be observed.
    observation_room_ms: f64,
}

/// Decode the arm's own impairment into a mean burst length and a long-run loss
/// probability. A `FourState` model is geometric in the burst state (`p31` is
/// the per-datagram probability of leaving it, so the mean burst is `1 / p31`),
/// and its steady-state loss share is `p13 / (p13 + p31)`; independent loss has
/// burst length 1 by definition, which is what makes `m = 6` armour copies
/// consume it whole.
fn loss_shape(spec: &ArmSpec) -> (f64, f64) {
    match spec.int_c2s.loss_model {
        LossModel::FourState(p) => {
            let p13 = f64::from(p.p13) / f64::from(u32::MAX);
            let p31 = f64::from(p.p31) / f64::from(u32::MAX);
            (1.0 / p31, p13 / (p13 + p31))
        }
        _ => (1.0, f64::from(spec.int_c2s.loss) / f64::from(u32::MAX)),
    }
}

/// The arm's ladder inputs, plus the room its own drain leaves a ladder.
///
/// `datagrams` and `window` are the arm's own measured inputs: the c2s
/// datagram count its link accepted ([`ArmRun::int_c2s_packets`]) and the offer
/// window it ran [`ArmRun::window`]. They are parameters rather than the
/// `ArmRun` itself so the vacuity demonstrations can drive the same arithmetic
/// from a perturbation of the arm's own configuration.
///
/// The rungs are computed for every interactive arm, including the cadence and
/// depth-2 arms whose tracked tail is not lone: `m` is the lane's cover and the
/// burst is the arm's own, so the same arithmetic can only *over*-estimate the
/// rungs of a shape whose recovery is dupack-driven. That over-estimate is what
/// the arm is then held to.
///
/// The room, by contrast, follows the **drain mechanism**, which is the load
/// shape's and not the depth's: a round's `read_exact` is awaited inside the
/// offer loop, so a request/response arm (at any depth) observes whatever
/// ladder it is inside its own arm deadline, while a cadence arm's sample is
/// read from the server sink and must therefore reach the collector snapshot
/// `GRACE` past the last offer.
fn ladder_inputs(spec: &ArmSpec, datagrams: u64, window: Duration) -> LadderInputs {
    let (mean_burst, loss) = loss_shape(spec);
    let requests_a_round = matches!(spec.load, Load::RequestResponse { .. });
    LadderInputs {
        datagrams_per_transmission: TAIL_DATAGRAMS_PER_TRANSMISSION,
        step_ms: LADDER_STEP_MS,
        mean_burst,
        loss,
        datagrams,
        rtt_ms: 2.0 * spec.int_c2s.latency.as_secs_f64() * 1000.0,
        observation_room_ms: if requests_a_round {
            (ARM_DEADLINE - window).as_secs_f64() * 1000.0
        } else {
            GRACE.as_secs_f64() * 1000.0
        },
    }
}

/// The longest burst the arm's own window is expected to contain once: the
/// window offers `datagrams` to a loss process that starts a burst every
/// `mean_burst / loss` datagrams, so `E = datagrams * loss / mean_burst` bursts
/// occur in it, and `P(L >= l) = (1 - 1 / mean_burst)^(l - 1)` inverts at
/// `l = 1 + ln(E) / ln(1 / (1 - 1 / mean_burst))`.
///
/// This is a *design* burst, not a ceiling: the geometric tail is unbounded, so
/// no finite window makes the observed maximum anything but a lower bound of
/// the distribution. What the arm's window must do is contain the worst ladder
/// it will plausibly show; the margin the derivation leaves to that burst is
/// reported beside it.
fn expected_bursts(inputs: &LadderInputs) -> f64 {
    inputs.datagrams as f64 * inputs.loss / inputs.mean_burst
}

/// The probability that the arm's own impairment draws a burst of at least `l`
/// forwarded datagrams: the burst state is geometric in the datagram that
/// leaves it (`p31` is that per-datagram probability, so `mean_burst = 1 /
/// p31` and `P(L >= l) = (1 - 1 / mean_burst)^(l - 1)`). Independent loss has
/// burst length 1 exactly, which is the step function the armour cover needs.
fn burst_tail_probability(inputs: &LadderInputs, l: f64) -> f64 {
    if inputs.mean_burst <= 1.0 {
        return if l <= 1.0 { 1.0 } else { 0.0 };
    }
    (1.0 - 1.0 / inputs.mean_burst).powf(l - 1.0)
}

fn worst_plannable_burst(inputs: &LadderInputs) -> f64 {
    if inputs.mean_burst <= 1.0 {
        return 1.0;
    }
    let bursts = expected_bursts(inputs);
    if bursts <= 1.0 {
        return 1.0;
    }
    let l = 1.0 + bursts.ln() / (1.0 / (1.0 - 1.0 / inputs.mean_burst)).ln();
    l.min(inputs.datagrams as f64)
}

/// The rungs that burst costs: `floor(burst / datagrams_per_transmission)`.
fn ladder_rungs(inputs: &LadderInputs) -> u64 {
    (worst_plannable_burst(inputs) / inputs.datagrams_per_transmission as f64) as u64
}

/// The window the arm's own ladder needs: every rung waits `step`, and the
/// climb starts one round trip after the offer.
fn required_window_ms(inputs: &LadderInputs) -> f64 {
    ladder_rungs(inputs) as f64 * inputs.step_ms + inputs.rtt_ms
}

/// What reading an arm's latency series for a truncated climb found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Censoring {
    /// The series ends on a climb that is longer than the arm's own
    /// observation room: the reported maximum is a censored lower bound.
    Censored,
    /// The series ends on a record, but the arm's room can contain the climb
    /// that set it, so the record is a completed observation.
    EdgeRecordContained,
    /// The series does not end on a climb.
    Clear,
    /// Too few samples for the tail to mean anything.
    Unclassifiable,
}

/// Whether an arm's latency series ends mid-climb, and what the numbers behind
/// that answer are.
struct CensorReading {
    verdict: Censoring,
    /// The last sample is the series maximum (last attainment of it).
    record_at_edge: bool,
    /// Rungs the last sample stands above every earlier sample.
    rungs_at_edge: f64,
    /// Length of the maximal terminal strictly-increasing run, counting only
    /// steps taken within `MAX_CLIMB_GAP_MS`.
    rise_run: usize,
    /// The gap between the last two samples, ms.
    edge_gap_ms: f64,
    /// The last sample, ms.
    final_ms: f64,
}

impl CensorReading {
    /// Conjunct 1 of [`censoring`]'s criterion: the series ends on a climb.
    fn ends_on_a_climb(&self) -> bool {
        self.record_at_edge && (self.rungs_at_edge >= 1.0 || self.rise_run >= 2)
    }
}

/// Read a latency series for a climb truncated by the arm's own observation.
///
/// The series is `(elapsed seconds, latency ms)` pairs, the shape every arm
/// already hands the panel; the times matter because a rise is only a climb if
/// its steps are one rung apart ([`MAX_CLIMB_GAP_MS`]).
///
/// The criterion is a conjunction of two facts about the arm's own series and
/// the arm's own room:
///
/// 1. **the series ends on a climb** — the final sample is the series maximum
///    (its last attainment), *and* it either stands at least one whole ladder
///    rung above every earlier sample or closes a strictly-increasing run of
///    two or more samples taken within a rung of each other. A genuine maximum
///    is a *peak*: it is followed by samples that decay back to the series'
///    body, so a series can only end on its own upward movement if observation
///    stopped while it was still rising. The record alone is not evidence —
///    with only a handful of extreme samples per run, the largest of them being
///    the last is common: across the 52 M1 lone-tail runs on record the final
///    sample is the series maximum in 15 of them (29 %) — which is why the
///    climb has to hold a whole rung, or be a rising run, and not merely be the
///    largest sample.
/// 2. **the climb is wider than the arm's room** — the final sample exceeds
///    every value the arm's drain could have observed (`observation_room_ms`).
///    This is the part that makes the reading a statement about truncation and
///    not about shape: a record the arm's own room can contain was observed to
///    completion, so it is a maximum however it sits in the panel, while a
///    record past that room was necessarily cut off at the room's edge and is a
///    lower bound.
///
/// Both conjuncts are needed. The shape alone is a screen with a false-positive
/// rate this test measures and prints; the room alone cannot see a climb.
fn censoring(samples: &[(f64, f64)], step_ms: f64, room_ms: f64) -> CensorReading {
    let (rise_run, edge_gap_ms) = {
        let mut run = 1usize;
        let mut gap = f64::INFINITY;
        for pair in samples.windows(2).rev() {
            let (previous_t, previous_ms) = pair[0];
            let (next_t, next_ms) = pair[1];
            let step = next_t - previous_t;
            if run == 1 {
                gap = step;
            }
            if next_ms > previous_ms && (0.0..=MAX_CLIMB_GAP_MS / 1000.0).contains(&step) {
                run += 1;
            } else {
                break;
            }
        }
        (run, gap)
    };
    if samples.len() < CENSORING_MIN_SAMPLES {
        return CensorReading {
            verdict: Censoring::Unclassifiable,
            record_at_edge: false,
            rungs_at_edge: 0.0,
            rise_run,
            edge_gap_ms,
            final_ms: samples.last().map_or(0.0, |pair| pair.1),
        };
    }
    let final_ms = samples.last().expect("a classified series is non-empty").1;
    let earlier_max = samples[..samples.len() - 1]
        .iter()
        .map(|pair| pair.1)
        .fold(f64::NEG_INFINITY, f64::max);
    let rungs_at_edge = (final_ms - earlier_max) / step_ms;
    let record_at_edge = rungs_at_edge >= 0.0;
    let ends_on_a_climb = record_at_edge && (rungs_at_edge >= 1.0 || rise_run >= 2);
    let verdict = if !ends_on_a_climb {
        Censoring::Clear
    } else if final_ms > room_ms {
        Censoring::Censored
    } else {
        Censoring::EdgeRecordContained
    };
    let reading = CensorReading {
        verdict,
        record_at_edge,
        rungs_at_edge,
        rise_run,
        edge_gap_ms,
        final_ms,
    };
    debug_assert_eq!(ends_on_a_climb, reading.ends_on_a_climb());
    reading
}

/// One censoring line, written whole under one lock so it cannot interleave
/// with another test's row.
fn print_censoring_row(row: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = std::io::Write::write_all(&mut stderr, row.as_bytes());
}

/// One vacuity case's reading, printed so the demonstration is evidence in the
/// run's log and not only an assertion that passed.
fn print_censoring_vacuity(case: &str, reading: &CensorReading, room_ms: f64) {
    print_censoring_row(&format!(
        "[m1-censoring] vacuity={case:<18} final={final:8.1} rungs_at_edge={redge:5.2} \
         rise_run={rise:<3} edge_gap_ms={gap:8.1} room={room:9.1} verdict={verdict:?}\n",
        final = reading.final_ms,
        redge = reading.rungs_at_edge,
        rise = reading.rise_run,
        gap = reading.edge_gap_ms * 1000.0,
        room = room_ms,
        verdict = reading.verdict,
    ));
}

/// One arm's censoring line: the series reading, the ladder the arm's own link
/// can produce, and whether its own room holds that ladder. Printed rather than
/// asserted — a red reading is a finding about the arm, not a licence to retune
/// it — but the instrument's own vacuity pair is asserted in
/// `m1_latency_window_censoring`.
fn report_censoring(arm: &str, spec: &ArmSpec, run: &ArmRun) {
    let inputs = ladder_inputs(spec, run.int_c2s_packets, run.window);
    let reading = censoring(&run.timeline, inputs.step_ms, inputs.observation_room_ms);
    let required = required_window_ms(&inputs);
    let window_holds = required <= inputs.observation_room_ms;
    let lone = matches!(spec.load, Load::RequestResponse { depth: 1 });
    let row = format!(
        "[m1-censoring] arm={arm:<10} samples={n:<5} last={last:8.1} rungs_at_edge={redge:5.2} \
         rise_run={rise:<3} edge_gap_ms={gap:8.1} screen={screen:<9} verdict={verdict:<21} \
         max={max:8.1} burst={burst:8.1} rungs={rungs:>3} step={step:.0} rtt={rtt:.0} \
         required={required:8.1} room={room:9.1} window_holds={holds:<5} lone_tail={lone} \
         datagrams={datagrams}(derived)\n",
        n = run.timeline.len(),
        last = reading.final_ms,
        redge = reading.rungs_at_edge,
        rise = reading.rise_run,
        gap = reading.edge_gap_ms * 1000.0,
        screen = if reading.ends_on_a_climb() {
            "climb"
        } else {
            "flat"
        },
        verdict = format!("{:?}", reading.verdict),
        max = run.summary.max,
        burst = worst_plannable_burst(&inputs),
        rungs = ladder_rungs(&inputs),
        step = inputs.step_ms,
        rtt = inputs.rtt_ms,
        required = required,
        room = inputs.observation_room_ms,
        holds = window_holds,
        datagrams = inputs.datagrams,
    );
    // One locked write of the whole row, like [`print_arm`]'s, so a row cannot
    // interleave with another test's line. The prefix is deliberately not
    // `[mandate-smoke …]`: `tools/mandate-check` reads those as arm lines and
    // attributes them to the mandate that follows, and this row is an
    // instrument reading rather than an arm the declaration carries.
    print_censoring_row(&row);
}

// ─────────────────────────────── M1: latency ─────────────────────────────────

fn m1_declaration() -> String {
    format!(
        r#"{{"mandate":"M1","title":"M1 interactive tail latency (clean vs hostile GE+jitter vs hostile lone tail)","x_label":"elapsed time (s)","y_label":"latency (ms)","panels":[{{"id":"latency","chart":"line","series":[{{"name":"clean"}},{{"name":"hostile"}},{{"name":"lone_tail"}}],"bounds":[{{"y":{M1_CEILING_MS},"label":"M1 ceiling 250 ms"}}]}},{{"id":"cdf","chart":"cdf","x_label":"latency (ms)","y_label":"percentile (%)","series":[{{"name":"clean"}},{{"name":"hostile"}},{{"name":"lone_tail"}}],"bounds":[]}}]}}"#
    )
}

fn m1_rows(runs: &[ArmRun]) -> Vec<(String, String, f64, f64)> {
    let mut rows = Vec::new();
    for run in runs {
        for (x, y) in &run.timeline {
            rows.push(("latency".to_owned(), run.name.to_owned(), *x, *y));
        }
        for (x, y) in cdf_points(&run.samples, 101) {
            rows.push(("cdf".to_owned(), run.name.to_owned(), x, y));
        }
    }
    rows
}

/// Mandate 1: interactive tail latency. The clean arm asserts the mandate
/// bound (`p99 <= 250 ms`, zero samples `> 250 ms`); the hostile and lone-tail
/// arms assert the documented regression guards and still draw the ceiling.
#[tokio::test(flavor = "multi_thread")]
async fn m1_interactive_tail_latency() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let runs = mandate_runs("M1").await;
    write_evidence(&dir, "M1", &m1_declaration(), &m1_rows(&runs));

    let clean = &runs[0];
    let hostile = &runs[1];
    let lone = &runs[2];
    // The deployed baseline the impaired tail must not regress past, read and
    // printed for every impaired arm before the guards below: a candidate that
    // is worse on either arm's asserted `p99` is rejected here (the failure
    // names the arm, the metric, the baseline value, the observed value and the
    // direction) rather than weighed against M2's offered-load latency or the
    // clean arm. The `p999` rows are printed with the same comparison and a
    // `REPORTED` verdict, and are asserted nowhere — a single order statistic of
    // a heavy-tailed quantity rejects healthy runs at random (see
    // [`M1_IMPAIRED_BASELINE`]); the rate-shaped guards below are what catch an
    // extreme-tail regression.
    let mut impaired_regressions = Vec::new();
    for run in [hostile, lone] {
        for (row, failure) in m1_impaired_baseline_rows(run) {
            print_censoring_row(&row);
            if let Some(failure) = failure {
                impaired_regressions.push(failure);
            }
        }
    }
    let pass = clean.summary.p99 <= M1_CEILING_MS
        && over250_count(&clean.samples) == 0
        && hostile.summary.p99 <= M1_HOSTILE_P99_GUARD_MS
        && over250_pct(&hostile.samples) <= M1_HOSTILE_OVER250_GUARD_PCT
        && lone.summary.p99 <= M1_LONE_P99_GUARD_MS
        && lone.summary.p999 <= M1_LONE_P999_GUARD_MS
        && over250_pct(&lone.samples) <= M1_LONE_OVER250_GUARD_PCT
        && impaired_regressions.is_empty();
    println!(
        "MANDATE M1 {} clean_p50={:.1} clean_p90={:.1} clean_p99={:.1} clean_p999={:.1} clean_max={:.1} clean_over250={} hostile_p50={:.1} hostile_p90={:.1} hostile_p99={:.1} hostile_p999={:.1} hostile_max={:.1} hostile_over250={} lone_p50={:.1} lone_p90={:.1} lone_p99={:.1} lone_p999={:.1} lone_max={:.1} lone_over250={} ceiling={:.1} hostile_p99_guard={:.1} hostile_over250_guard={:.1} lone_p99_guard={:.1} lone_p999_guard={:.1} lone_over250_guard={:.1}",
        verdict(pass),
        clean.summary.p50,
        clean.summary.p90,
        clean.summary.p99,
        clean.summary.p999,
        clean.summary.max,
        over250_count(&clean.samples),
        hostile.summary.p50,
        hostile.summary.p90,
        hostile.summary.p99,
        hostile.summary.p999,
        hostile.summary.max,
        over250_count(&hostile.samples),
        lone.summary.p50,
        lone.summary.p90,
        lone.summary.p99,
        lone.summary.p999,
        lone.summary.max,
        over250_count(&lone.samples),
        M1_CEILING_MS,
        M1_HOSTILE_P99_GUARD_MS,
        M1_HOSTILE_OVER250_GUARD_PCT,
        M1_LONE_P99_GUARD_MS,
        M1_LONE_P999_GUARD_MS,
        M1_LONE_OVER250_GUARD_PCT,
    );

    // The deployed-baseline non-regression gate, and the **first** assertion of
    // the mandate: it carries the tightest bound on each impaired arm (265 ms
    // against the 900 ms guard, 199 ms against the 3200 ms guard), and the
    // standing rule is that an impaired tail may not regress at all, so a
    // candidate that bought M2's wire or the clean arm at the impaired tail's
    // expense reports here rather than inside a superseded tripwire.
    assert!(
        impaired_regressions.is_empty(),
        "[M1] the impaired tail regressed past the deployed rtp v0.0.98 baseline:\n{}",
        impaired_regressions.join("\n"),
    );
    assert!(
        clean.summary.p99 <= M1_CEILING_MS,
        "[M1] clean-arm p99 {:.1} ms exceeds the {M1_CEILING_MS} ms ceiling: the interactive tail must stay at the one-way floor on the mild 2% iid arm",
        clean.summary.p99,
    );
    assert_eq!(
        over250_count(&clean.samples),
        0,
        "[M1] clean arm has {} sample(s) > {M1_CEILING_MS} ms (p99 {:.1} ms, max {:.1} ms): the mandate requires zero spikes over the ceiling",
        over250_count(&clean.samples),
        clean.summary.p99,
        clean.summary.max,
    );
    assert!(
        hostile.summary.p99 <= M1_HOSTILE_P99_GUARD_MS,
        "[M1] hostile (GE+jitter) arm p99 {:.1} ms exceeds its {M1_HOSTILE_P99_GUARD_MS} ms regression guard: the known hostile tail defect has at least doubled",
        hostile.summary.p99,
    );
    assert!(
        over250_pct(&hostile.samples) <= M1_HOSTILE_OVER250_GUARD_PCT,
        "[M1] hostile arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_HOSTILE_OVER250_GUARD_PCT}% regression guard",
        over250_pct(&hostile.samples),
    );
    assert!(
        lone.summary.p99 <= M1_LONE_P99_GUARD_MS,
        "[M1] hostile lone-tail arm p99 {:.1} ms exceeds its {M1_LONE_P99_GUARD_MS} ms regression guard: the known GE lone-tail defect has at least doubled",
        lone.summary.p99,
    );
    assert!(
        lone.summary.p999 <= M1_LONE_P999_GUARD_MS,
        "[M1] hostile lone-tail arm p999 {:.1} ms exceeds its {M1_LONE_P999_GUARD_MS} ms regression guard (max {:.1} ms): the known RTO-ladder defect has at least doubled",
        lone.summary.p999,
        lone.summary.max,
    );
    assert!(
        over250_pct(&lone.samples) <= M1_LONE_OVER250_GUARD_PCT,
        "[M1] lone-tail arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_LONE_OVER250_GUARD_PCT}% regression guard",
        over250_pct(&lone.samples),
    );
}

/// One reading of [`m1_hostile_p99_replicated`], printed so the arm's per-rep
/// distribution is evidence in the run's own log rather than only a summary:
/// which rep, its own percentiles, and its own `> 250 ms` sample count. The
/// prefix is deliberately not `[mandate-smoke …]`: `tools/mandate-check` reads
/// those as arm lines and attributes them to the mandate whose `MANDATE` line
/// follows them, and these rows are an instrument reading rather than an arm the
/// declaration carries (see [`mandate_runs_quiet`]).
fn print_hostile_replicated_row(role: &str, rep: &str, run: &ArmRun) {
    let s = &run.summary;
    let row = format!(
        "[m1-hostile-replicated] role={role:<7} rep={rep:<10} p50={p50:7.1} p90={p90:7.1} \
         p99={p99:7.1} p999={p999:8.1} max={max:8.1} over250={o25:>4} \
         recv={recv:>5} sent={sent:>5}\n",
        p50 = s.p50,
        p90 = s.p90,
        p99 = s.p99,
        p999 = s.p999,
        max = s.max,
        o25 = over250_count(&run.samples),
        recv = s.received,
        sent = s.sent,
    );
    print_censoring_row(&row);
}

/// The median of a slice, by value. `NaN` for an empty slice, so a caller that
/// somehow measured no rep fails its bound rather than passing a zero.
fn median_of(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    match n {
        0 => f64::NAN,
        _ if n % 2 == 1 => sorted[n / 2],
        _ => 0.5 * (sorted[n / 2 - 1] + sorted[n / 2]),
    }
}

/// Mandate 1's hostile arm read as a **median of interleaved reps** rather than
/// as one draw.
///
/// A **new arm alongside, with no existing arm retuned**: the `clean`, `hostile`
/// and `lone_tail` arms keep their impairment, seeds, windows, cadence, tier and
/// guards exactly, and this test measures the very same `hostile` spec
/// [`mandate_arms`] builds -- `MANDATE_SMOKE_FAULT` reaches it unchanged, so the
/// arm's vacuity is the same input perturbation M1's own impaired-baseline gate
/// reads, and a probe of one arm's assertion cannot read as a probe of the
/// other's because each failure names its own bound.
///
/// It exists because **one hostile rep is not a measurement of the hostile
/// lane**. Measured on this revision (20 interleaved `m1_interactive_tail_latency`
/// invocations, `uptime` logged per rep): the hostile p99 spans
/// **117.5-220.3 ms (1.87x)** with a between-rep sd of `27.1 ms` and cv `0.17`,
/// while the same runs' p75/p90/p95 span only 1.34x/1.18x/1.20x. The spread is a
/// **rare-event count, not a host drift and not a body shift**: the p99 is
/// `+0.95` correlated with the count of samples over `150 ms` and `-0.33`
/// correlated with the paired clean arm's own p99, and the seven reps at load
/// `6.9-18.2` (where the clean arm's p99 reached `84.6 ms`) read the *lowest*
/// hostile p99 in the set. The samples inside a run are not scattered either: a
/// severe head-of-line repair episode releases its whole queued cascade at one
/// instant, so its members read a gradient (`247.2` down to `153.1 ms` at a
/// single `t = 2.746 s` in one rep) and the p99 is fixed by how far up that
/// cascade the 99th percentile reaches. So the statistic this arm asserts is the
/// **median of `M1_HOSTILE_P99_REP_REPS` interleaved reps**, whose sampling sd is
/// `14.93 ms` rather than the `38.4 ms` of a single draw.
///
/// The rep count and both bounds are derived on their constants. The control
/// **brackets** the rep set rather than repeating per rep: the 20 paired reps
/// above show the clean arm has no explanatory power for the hostile p99, so a
/// per-rep control would double this arm's cost for coverage the measurement
/// says is not there. Its readings say whether the host was contended while the
/// reps ran, which is what attributes a breached bound.
///
/// The arm's vacuity is `MANDATE_SMOKE_FAULT=M1_IMPAIRED_slow`, which shifts the
/// hostile link by `+300 ms` one-way on both directions and leaves the control
/// untouched: the hostile median moves from `161.5` to `2021.2 ms` and both
/// bounds fail -- the p90 and the p99 are collected and named together, so one
/// fault shows each assertion biting -- from the measurement path rather than
/// from a moved assertion.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns threads and binds ephemeral ports; eleven ~16 s hostile reps plus two clean control readings; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn m1_hostile_p99_replicated() {
    let _serial = SERIAL.lock().await;
    // The very specs M1 builds, so the fault selector reaches the hostile arm
    // exactly as it does for the mandate gate; the clean arm is the control and
    // the fault leaves it untouched.
    let specs = mandate_arms("M1");
    let clean = specs[0].clone();
    let hostile = specs[1].clone();

    let open = with_timeout(
        ARM_DEADLINE,
        "m1_hostile_replicated/control-open",
        run_arm(clean.clone()),
    )
    .await;
    print_hostile_replicated_row("control", "open", &open);

    let mut hostile_p99 = Vec::with_capacity(M1_HOSTILE_P99_REP_REPS);
    let mut hostile_p90 = Vec::with_capacity(M1_HOSTILE_P99_REP_REPS);
    for rep in 0..M1_HOSTILE_P99_REP_REPS {
        let run = with_timeout(
            ARM_DEADLINE,
            &format!("m1_hostile_replicated/rep{}", rep + 1),
            run_arm(hostile.clone()),
        )
        .await;
        // The arm's own sanity: something was measured, and the percentile is
        // not degenerate. An arm that took its measurement path and returned no
        // samples has deleted the coverage it exists to provide.
        assert!(
            run.summary.received > 0 && run.summary.p99.is_finite() && run.summary.p99 > 0.0,
            "[M1-hostile-replicated] rep {} measured {} samples with a non-finite p99 {:.1}: the arm must measure the hostile lane, not skip it",
            rep + 1,
            run.summary.received,
            run.summary.p99,
        );
        print_hostile_replicated_row("hostile", &format!("rep{}", rep + 1), &run);
        hostile_p99.push(run.summary.p99);
        hostile_p90.push(run.summary.p90);
    }

    let close = with_timeout(
        ARM_DEADLINE,
        "m1_hostile_replicated/control-close",
        run_arm(clean.clone()),
    )
    .await;
    print_hostile_replicated_row("control", "close", &close);
    let control_p99 = [open.summary.p99, close.summary.p99];

    let p99_median = median_of(&hostile_p99);
    let p90_median = median_of(&hostile_p90);
    let p99_min = hostile_p99.iter().cloned().fold(f64::INFINITY, f64::min);
    let p99_max = hostile_p99
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);
    let sd = {
        let mean = hostile_p99.iter().sum::<f64>() / hostile_p99.len() as f64;
        let var = hostile_p99
            .iter()
            .map(|v| (v - mean) * (v - mean))
            .sum::<f64>()
            / (hostile_p99.len() - 1) as f64;
        var.sqrt()
    };
    // The half-width the arm's own reps give the median, and the two-revision
    // resolution they imply: printed so a reader comparing this run with another
    // reads the number the rep count was derived from rather than re-deriving it.
    let ci95 = 1.96 * 1.2533 * sd / (hostile_p99.len() as f64).sqrt();
    let basis_points = 2.802 * (2.0f64).sqrt() * 1.2533 * sd / (hostile_p99.len() as f64).sqrt();
    print_censoring_row(&format!(
        "[m1-hostile-replicated] summary reps={reps} p99_median={p99m:7.1} p99_min={p99lo:7.1} \
         p99_max={p99hi:7.1} p99_sd={sd:6.2} recorded_sd={rsd:5.1} ci95_halfwidth={ci95:6.2} \
         two_arm_move_at_80pct_power={basis:6.2} p90_median={p90m:7.1} \
         control_p99={c0:7.1}/{c1:7.1} p99_bound={b99:.1} p90_bound={b90:.1}\n",
        reps = hostile_p99.len(),
        p99m = p99_median,
        p99lo = p99_min,
        p99hi = p99_max,
        sd = sd,
        rsd = M1_HOSTILE_P99_REP_SD_MS,
        ci95 = ci95,
        basis = basis_points,
        p90m = p90_median,
        c0 = control_p99[0],
        c1 = control_p99[1],
        b99 = M1_HOSTILE_P99_MEDIAN_BOUND_MS,
        b90 = M1_HOSTILE_P90_MEDIAN_BOUND_MS,
    ));

    // The control first: a contended host is named before the hostile bounds are
    // read, so a breach is attributed rather than assumed.
    for (slot, value) in [("open", control_p99[0]), ("close", control_p99[1])] {
        assert!(
            value <= M1_CEILING_MS,
            "[M1-hostile-replicated] the control (clean) arm read p99 {value:.1} ms at the {slot} of the rep set, over M1's {M1_CEILING_MS} ms ceiling: the host was contended while the hostile reps ran, so this run's hostile p99 (median {p99_median:.1} ms) is inconclusive rather than a lane reading",
        );
    }
    // Both breached bounds are collected and named together: the two say
    // different things (the body moved / the tail moved), and a run that
    // breaches both must report both rather than let the first assertion fire
    // and hide the second.
    let mut breaches = Vec::new();
    if p90_median > M1_HOSTILE_P90_MEDIAN_BOUND_MS {
        breaches.push(format!(
            "[M1-hostile-replicated] the median hostile p90 over {reps} interleaved reps is {p90_median:.1} ms, over its {M1_HOSTILE_P90_MEDIAN_BOUND_MS} ms bound (per-rep p90: {hostile_p90:?}): the hostile lane's body moved, which no rare-event draw of its tail can explain",
            reps = hostile_p99.len(),
        ));
    }
    if p99_median > M1_HOSTILE_P99_MEDIAN_BOUND_MS {
        breaches.push(format!(
            "[M1-hostile-replicated] the median hostile p99 over {reps} interleaved reps is {p99_median:.1} ms, over its {M1_HOSTILE_P99_MEDIAN_BOUND_MS} ms bound (per-rep p99: {hostile_p99:?}; control p99: {control_p99:?}): the hostile lane's repair tail regressed past the level a replicated median can establish, at a noise of {sd:.2} ms between reps",
            reps = hostile_p99.len(),
        ));
    }
    assert!(
        breaches.is_empty(),
        "[M1-hostile-replicated] the replicated hostile level regressed past its derived bounds:\n{}",
        breaches.join("\n"),
    );
}

/// The M1 arms' observation windows, read for a climb the window truncated.
///
/// A **new instrument, with no arm retuned**: `clean`, `hostile` and
/// `lone_tail` keep their impairment, seeds, windows, cadence, tier and guards
/// exactly, and this test reads the latency series they already produce —
/// through the same shared arm-run cache the M2 gate reads, so a full-target
/// run measures nothing twice and an `--exact` run of this test alone pays the
/// same one measurement the M1 gate pays.
///
/// It exists because the panel cannot answer the question it is read for. The
/// M1-latency panel's x range is the data's own extent, so the lone tail's
/// largest sample sits at the frame's right edge and a climb cut off there
/// looks exactly like one that finished there — and the panel draws the gap a
/// long round trip leaves as a straight line, so the same record reads as a
/// near-vertical wall. [`censoring`] resolves both from the series and the
/// arm's own room, and the five vacuity cases below prove the instrument can go
/// red: a detector that cannot fail is not coverage.
#[tokio::test(flavor = "multi_thread")]
async fn m1_latency_window_censoring() {
    // ── vacuity 1: a climb cut off before the arm's room ends is Censored.
    // The series is a body of `CENSORING_MIN_SAMPLES` samples at the arm's own
    // p50 and cadence with a terminal ladder of four consecutive rungs one
    // `LADDER_STEP_MS` apart, its last sample past the 1200 ms room the sketch
    // passes: the arm's room cannot contain the climb, so the reported maximum
    // is a lower bound.
    let mut truncated: Vec<(f64, f64)> = (0..CENSORING_MIN_SAMPLES)
        .map(|index| (index as f64 * 0.005, 24.0))
        .collect();
    for (index, value) in [560.0, 860.0, 1160.0, 1460.0].into_iter().enumerate() {
        truncated.push((0.16 + index as f64 * 0.25, value));
    }
    let cut_off = censoring(&truncated, LADDER_STEP_MS, 1200.0);
    print_censoring_vacuity("truncated-climb", &cut_off, 1200.0);
    assert_eq!(
        cut_off.verdict,
        Censoring::Censored,
        "[M1-censoring] vacuity 1: the final sample {:.1} ms is a climb {:.2} rungs wide and past the 1200 ms room, so the red reading must be Censored; got {:?} (record_at_edge={} rise_run={} edge_gap_ms={:.1})",
        cut_off.final_ms,
        cut_off.rungs_at_edge,
        cut_off.verdict,
        cut_off.record_at_edge,
        cut_off.rise_run,
        cut_off.edge_gap_ms * 1000.0,
    );

    // ── vacuity 2: the same climb with the room to contain it is not censored.
    // This is the case that separates "ends on a climb" from "truncated": the
    // shape is identical and only the room moved, so a criterion that ignored
    // the room would call a completed observation red.
    let contained = censoring(&truncated, LADDER_STEP_MS, 4000.0);
    print_censoring_vacuity("contained-climb", &contained, 4000.0);
    assert_eq!(
        contained.verdict,
        Censoring::EdgeRecordContained,
        "[M1-censoring] vacuity 2: a climb the room can contain is an observed maximum, not a censored one",
    );

    // ── vacuity 3: a record with its decay after it is Clear. A genuine
    // maximum is a peak, so the series' last sample is below it and there is no
    // terminal climb to read.
    let mut completed = truncated[..CENSORING_MIN_SAMPLES].to_vec();
    for (index, value) in [1460.0, 700.0, 240.0, 26.0, 24.0].into_iter().enumerate() {
        completed.push((0.16 + index as f64 * 0.25, value));
    }
    let peak = censoring(&completed, LADDER_STEP_MS, 1200.0);
    print_censoring_vacuity("decayed-peak", &peak, 1200.0);
    assert_eq!(
        peak.verdict,
        Censoring::Clear,
        "[M1-censoring] vacuity 3: a record followed by its decay is a peak, not a truncated climb",
    );

    // ── vacuity 4: a rise whose steps are further apart than one rung is not
    // one climb. The last sample stands 0.8 of a rung above the previous record,
    // so the rung test has nothing to bite on, and the gap bound leaves the
    // terminal run one sample long: the reading is Clear, where a criterion
    // counting adjacency alone would call those two samples a rising run.
    let mut drawn_as_a_wall = truncated[..CENSORING_MIN_SAMPLES].to_vec();
    drawn_as_a_wall.push((0.16, 560.0));
    drawn_as_a_wall.push((1.16, 800.0));
    let widened = censoring(&drawn_as_a_wall, LADDER_STEP_MS, 1200.0);
    print_censoring_vacuity("widened-rungs", &widened, 1200.0);
    assert_eq!(
        widened.verdict,
        Censoring::Clear,
        "[M1-censoring] vacuity 4: steps {} ms apart are not a rising run, so the two samples are not a climb; got {:?} (rise_run={} rungs_at_edge={:.2})",
        widened.edge_gap_ms * 1000.0,
        widened.verdict,
        widened.rise_run,
        widened.rungs_at_edge,
    );

    // ── vacuity 5 (window side): the derived requirement must fail when the
    // arm's own impairment can outrun its own room. The hostile arm's own
    // config with a 400-datagram mean burst needs 26 rungs of the 300 ms step —
    // 7.85 s — against the 2 s `GRACE` a cadence arm's sink snapshot leaves.
    // The measurement (`int_c2s_packets`, `window`) is the hostile arm's own.
    let hostile_spec = mandate_arms("M1")
        .into_iter()
        .nth(1)
        .expect("the M1 arm set carries the hostile arm at index 1");
    let mut outrunning = hostile_spec.clone();
    outrunning.int_c2s.loss_model = gilbert_elliott_loss(5.0, 400.0);
    let outrun = ladder_inputs(&outrunning, 11_950, Duration::from_secs(12));
    print_censoring_row(&format!(
        "[m1-censoring] vacuity={case:<18} mean_burst={burst:<8.1} required={required:8.1} \
         room={room:8.1} verdict={verdict}\n",
        case = "outrun-window",
        burst = outrun.mean_burst,
        required = required_window_ms(&outrun),
        room = outrun.observation_room_ms,
        verdict = if required_window_ms(&outrun) > outrun.observation_room_ms {
            "RED"
        } else {
            "green"
        },
    ));
    assert!(
        required_window_ms(&outrun) > outrun.observation_room_ms,
        "[M1-censoring] vacuity 5: a mean burst of {} datagrams needs {:.1} ms against the cadence arm's {:.1} ms room, so the window check must go red",
        outrun.mean_burst,
        required_window_ms(&outrun),
        outrun.observation_room_ms,
    );

    // ── the real arms. The runs are the M1 gate's own (same cache key), so no
    // arm is measured twice and no arm setting is touched; the quiet accessor
    // is what keeps this reader from claiming the rows' attribution
    // ([`mandate_runs_quiet`]).
    let _serial = SERIAL.lock().await;
    let specs = mandate_arms("M1");
    let runs = mandate_runs_quiet("M1").await;
    assert_eq!(
        specs.len(),
        runs.len(),
        "[M1-censoring] the arm set and its runs must line up one for one",
    );

    let mut censored = 0usize;
    let mut window_short = 0usize;
    for (spec, run) in specs.iter().zip(runs.iter()) {
        report_censoring(spec.name, spec, run);
        let inputs = ladder_inputs(spec, run.int_c2s_packets, run.window);
        let reading = censoring(&run.timeline, inputs.step_ms, inputs.observation_room_ms);
        assert_ne!(
            reading.verdict,
            Censoring::Unclassifiable,
            "[M1-censoring] the {} arm's {} samples cannot classify its tail; an arm this instrument cannot read is a finding, not a pass",
            spec.name,
            run.timeline.len(),
        );
        if reading.verdict == Censoring::Censored {
            censored += 1;
        }
        if required_window_ms(&inputs) > inputs.observation_room_ms {
            window_short += 1;
        }
        assert!(
            required_window_ms(&inputs) <= inputs.observation_room_ms,
            "[M1-censoring] the {} arm's own window is shorter than the ladder its own link's {} -datagram mean burst can build: {} rungs x {} ms + {} ms round trip = {:.1} ms against its {:.1} ms room",
            spec.name,
            inputs.mean_burst,
            ladder_rungs(&inputs),
            inputs.step_ms,
            inputs.rtt_ms,
            required_window_ms(&inputs),
            inputs.observation_room_ms,
        );
    }
    assert_eq!(
        censored, 0,
        "[M1-censoring] {censored} M1 arm(s) report a maximum past their own observation room: the window truncates the climb it asserts on",
    );
    println!(
        "CENSORING M1 arms={} censored={} window_short={} step_ms={:.0} min_samples={} verdict=PASS",
        specs.len(),
        censored,
        window_short,
        LADDER_STEP_MS,
        CENSORING_MIN_SAMPLES,
    );
}

// ─────────── the lone tail's rung counts, measured against the law ───────────
//
// The window derivation below rests on two quantities that are not the same
// kind of object. `E = datagrams * loss / mean_burst` is the number of loss
// events the arm's own link applies, and it is *arithmetic on the link*: the
// impairment advances one Markov step per datagram it receives, the steady
// state makes `loss` the share of datagrams it drops, and `1 / p31` is the mean
// run of drops, so `E` is exact for the datagrams the link saw. The rung count
// is not. `floor(l / m)` is what one burst *costs in full transmissions* **when
// it begins on a transmission's first datagram** — the case the deterministic
// probe in `rtp`'s send space drives, and the only case it drives — and whether
// a window's rounds show those rungs is a measurement of the arm.
//
// So the law is checked against the arm rather than against itself. This probe
// measures the rung distribution of the arm's own rounds and prints the law's
// prediction beside it, per run and pooled. It exists because the two numbers
// had drifted apart by an order of magnitude in `GATE.md` with nothing in the
// battery to notice.

/// Runs of the `lone_tail` arm this probe measures. Four pooled windows are
/// what the reading needs: the ladder is a rare event (`> 250 ms` rounds run at
/// 2.5 per window) and a two-window sample cannot separate a law that predicts
/// three such rounds per window from one that predicts thirty with any
/// confidence. The cost is declared in `GATE.md` against the `full` tier's
/// ceiling, which this row is the reason to raise.
const RUNG_DIST_RUNS: usize = 4;
/// Histogram bins, in rungs: one bin per 300 ms rung, wide enough to hold the
/// deepest ladder the arm's recording holds.
const RUNG_DIST_BINS: usize = 12;
/// The rung thresholds the law is checked at, in rungs: `k` rungs is a ladder
/// of `k` repair transmissions and `k * m` datagrams of burst.
const RUNG_DIST_THRESHOLDS: [usize; 4] = [1, 2, 4, 6];
/// How far the pooled count may sit below/above a law's prediction, as a
/// ratio. The low side is what rejects a law that predicts *too many* rungs —
/// the arm's own measurement is the smaller number — and it is also the
/// instrument's sanity: a ladder frequency of zero means the probe measured
/// nothing, and a count that cannot notice that is not coverage. The high side
/// rejects a law that predicts too few.
const RUNG_DIST_BAND_LOW: f64 = 0.25;
const RUNG_DIST_BAND_HIGH: f64 = 3.0;
/// A round trip that waited for no repair can reach `latency + jitter` per
/// direction and no more, because `sample_delay` clamps at zero and nothing
/// else on this arm delays a datagram; so **any** round trip above the
/// two-direction sum waited for a repair, and the count of those rounds is a
/// lower bound on the rounds with at least one rung — no assumption about the
/// rung's duration enters. On `lone_tail` that bound is `2 * (25 + 100) = 250`
/// ms, which is also the M1 ceiling; the two are the same number here rather
/// than by construction, so the constant is derived from the arm's own
/// impairment (`OWD` and `HOSTILE_JITTER`) and not reused from the ceiling.
const LONE_TAIL_LADDER_FLOOR_MS: f64 = 2.0 * (25.0 + 100.0);
/// The rung thresholds are read off that floor: `k` rungs is a ladder of `k`
/// repair transmissions, the first of which waits the ladder's own step after
/// the floor, so the `k`-th threshold is `floor + (k - 1) * step`. The floor
/// term is the rigorous half (any rung at all lifts the round trip above it);
/// the step term reads the rungs off the 300 ms grid the arm's own series
/// shows, and under-counts if the first rung ever waits longer than one step.
fn rung_threshold_ms(k: usize) -> f64 {
    LONE_TAIL_LADDER_FLOOR_MS + (k - 1) as f64 * LADDER_STEP_MS
}

/// Read the `lone_tail` arm for its rung counts and its applied loss: the
/// ground truth the window derivation is checked against.
///
/// Every input is the arm's own — the round-trip series it produced, the
/// datagrams its c2s link carried and the datagrams that link actually dropped
/// — so a discrepancy is a discrepancy in the law, not in a transcription.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "lone-tail rung-distribution probe; four ~19 s windows; run with --ignored --nocapture"]
async fn m1_lone_tail_rung_distribution() {
    let _serial = SERIAL.lock().await;
    let spec = mandate_arms("M1")
        .into_iter()
        .find(|spec| spec.name == "lone_tail")
        .expect("[rung-dist] the M1 arm set must carry the lone_tail arm");

    let mut pooled_by_threshold = [0usize; RUNG_DIST_THRESHOLDS.len()];
    let mut pooled_rounds = 0usize;
    let mut pooled_bins = vec![0usize; RUNG_DIST_BINS];
    let mut pooled_corrected = [0.0f64; RUNG_DIST_THRESHOLDS.len()];
    let mut pooled_uncorrected = [0.0f64; RUNG_DIST_THRESHOLDS.len()];
    let mut pooled_applied = 0.0f64;
    let mut pooled_nominal = 0.0f64;

    for rep in 0..RUNG_DIST_RUNS {
        let run = with_timeout(ARM_DEADLINE, "rung-dist/lone_tail", run_arm(spec.clone())).await;
        let inputs = ladder_inputs(&spec, run.int_c2s_packets, run.window);
        assert_eq!(
            spec.name, run.name,
            "[rung-dist] run {rep} is not the arm the probe asked for",
        );
        assert!(
            run.samples.len() >= CENSORING_MIN_SAMPLES,
            "[rung-dist] run {rep} produced {} rounds, too few for a rung histogram: the probe measured nothing",
            run.samples.len(),
        );
        // The loss the link *applied*, counted by the link, against the share
        // its own impairment declares. The arm's link carries no rate shaper and
        // no queue limit, so `dropped / received` is the loss model's own output
        // with no overflow drop mixed in, and the two must agree: a mismatch is
        // the loss model not being the one this law names.
        let applied = run.int_c2s_counters.dropped as f64 / run.int_c2s_packets as f64;
        assert!(
            applied >= 0.5 * inputs.loss && applied <= 2.0 * inputs.loss,
            "[rung-dist] run {rep} applied {:.4} loss on {} c2s datagrams but its impairment declares {:.4}: the law's `loss` term is not this link's",
            applied,
            run.int_c2s_packets,
            inputs.loss,
        );

        let floor_ms = LONE_TAIL_LADDER_FLOOR_MS;
        let run_floor_ms = run.samples.iter().cloned().fold(f64::INFINITY, f64::min);
        let mut by_threshold = [0usize; RUNG_DIST_THRESHOLDS.len()];
        let mut bins = vec![0usize; RUNG_DIST_BINS];
        let mut max_rungs = 0.0f64;
        for rtt in &run.samples {
            let rungs = (rtt - run_floor_ms) / inputs.step_ms;
            max_rungs = max_rungs.max(rungs);
            for (slot, threshold) in by_threshold.iter_mut().zip(RUNG_DIST_THRESHOLDS) {
                if *rtt > rung_threshold_ms(threshold) {
                    *slot += 1;
                }
            }
            // Bin 0 is the rounds that waited for nothing; bin k > 0 is the
            // rounds a `k`-rung ladder explains, read on the same floor and
            // step the thresholds use.
            let bin = if *rtt <= floor_ms {
                0
            } else {
                1 + (((rtt - floor_ms) / inputs.step_ms) as usize).min(RUNG_DIST_BINS - 2)
            };
            bins[bin] += 1;
        }
        let corrected = corrected_rung_counts(&inputs, run.samples.len() as u64);
        let uncorrected = law_rung_counts(&inputs);
        eprintln!(
            "[rung-dist] run={rep} rounds={} c2s={} dropped={} forwarded={} delayed={} applied={:.4} nominal={:.4} floor_ms={:.0} step_ms={:.0} E={:.1} max_rungs={:.2}",
            run.samples.len(),
            run.int_c2s_packets,
            run.int_c2s_counters.dropped,
            run.int_c2s_counters.forwarded,
            run.int_c2s_counters.delayed,
            applied,
            inputs.loss,
            floor_ms,
            inputs.step_ms,
            expected_bursts(&inputs),
            max_rungs,
        );
        eprintln!(
            "[rung-dist] run={rep} s2c_recv={} s2c_dropped={} s2c_forwarded={} s2c_delayed={}",
            run.int_s2c_counters.received,
            run.int_s2c_counters.dropped,
            run.int_s2c_counters.forwarded,
            run.int_s2c_counters.delayed,
        );
        eprintln!(
            "[rung-dist] run={rep} measured {} corrected {} uncorrected {}",
            rung_count_row(&by_threshold),
            predicted_row(&corrected),
            predicted_row(&uncorrected),
        );
        eprintln!(
            "[rung-dist] run={rep} rounds_per_rung_bin(0..{RUNG_DIST_BINS})={bins:?} run_floor_ms={run_floor_ms:.2}"
        );
        let waited: Vec<String> = run
            .samples
            .iter()
            .filter(|rtt| **rtt > floor_ms)
            .map(|rtt| format!("{rtt:.1}"))
            .collect();
        eprintln!(
            "[rung-dist] run={rep} rounds_with_a_rung=[{}] (each is one round that waited; the rung it waited is that value less the floor, over the step)",
            waited.join(" "),
        );

        pooled_rounds += run.samples.len();
        for (slot, value) in pooled_by_threshold.iter_mut().zip(by_threshold) {
            *slot += value;
        }
        for (slot, value) in pooled_bins.iter_mut().zip(bins) {
            *slot += value;
        }
        for (slot, value) in pooled_corrected.iter_mut().zip(corrected) {
            *slot += value;
        }
        for (slot, value) in pooled_uncorrected.iter_mut().zip(uncorrected) {
            *slot += value;
        }
        pooled_applied += applied;
        pooled_nominal += inputs.loss;
    }

    let runs = RUNG_DIST_RUNS as f64;
    let measured = pooled_by_threshold.map(|n| n as f64);
    eprintln!(
        "[rung-dist] pooled runs={RUNG_DIST_RUNS} rounds={pooled_rounds} applied={:.4} nominal={:.4} rounds_per_rung_bin(0..{RUNG_DIST_BINS})={pooled_bins:?}",
        pooled_applied / runs,
        pooled_nominal / runs,
    );
    eprintln!(
        "[rung-dist] pooled measured {} corrected {} uncorrected {}",
        pooled_row(&measured, runs),
        pooled_row(&pooled_corrected, runs),
        pooled_row(&pooled_uncorrected, runs),
    );

    // The check, against the arm's own measurement, on pooled counts: the
    // per-window counts are what the law predicts per window and four windows
    // are what the arm offered. The low side is the instrument's sanity (a
    // ladder frequency of zero means the probe measured nothing, and a count
    // that cannot notice that is not coverage) and the discriminator against a
    // law that predicts too much; the high side rejects a law that predicts too
    // little, which is what a collapsed armour cover would produce.
    let observed = measured[0];
    let corrected = pooled_corrected[0];
    let uncorrected = pooled_uncorrected[0];
    let corrected_ok =
        observed >= RUNG_DIST_BAND_LOW * corrected && observed <= RUNG_DIST_BAND_HIGH * corrected;
    // The same measurement read against the law this file carried before the
    // alignment correction, printed so the band is shown to be able to fail
    // rather than asserted to be tight: the law the correction replaced is
    // rejected by the very series it was derived from.
    let uncorrected_ok = observed >= RUNG_DIST_BAND_LOW * uncorrected
        && observed <= RUNG_DIST_BAND_HIGH * uncorrected;
    eprintln!(
        "[rung-dist] vacuity=uncorrected-law observed_ge1={observed:.0} law_ge1={uncorrected:.1} band=[{:.2},{:.2}] verdict={}",
        RUNG_DIST_BAND_LOW * uncorrected,
        RUNG_DIST_BAND_HIGH * uncorrected,
        verdict(uncorrected_ok),
    );
    eprintln!(
        "[rung-dist] check=corrected-law observed_ge1={observed:.0} law_ge1={corrected:.1} band=[{:.2},{:.2}] verdict={}",
        RUNG_DIST_BAND_LOW * corrected,
        RUNG_DIST_BAND_HIGH * corrected,
        verdict(corrected_ok),
    );
    assert!(
        observed >= RUNG_DIST_BAND_LOW * corrected && observed <= RUNG_DIST_BAND_HIGH * corrected,
        "[rung-dist] the lone_tail arm's {RUNG_DIST_RUNS} windows hold {observed:.0} rounds that waited for a repair, outside the [{:.2}, {:.2}] the corrected law predicts over the same windows ({corrected:.2}): the transmission-boundary rate is not the arm's",
        RUNG_DIST_BAND_LOW * corrected,
        RUNG_DIST_BAND_HIGH * corrected,
    );
    assert!(
        !uncorrected_ok,
        "[rung-dist] the uncorrected law ({uncorrected:.2} rounds over the same windows) is now inside the band around the measured {observed:.0}: the alignment correction this file records is no longer what reconciles the arm, so the correction needs re-deriving rather than trusting",
    );
    println!(
        "RUNG_DIST PASS runs={RUNG_DIST_RUNS} rounds={pooled_rounds} applied={:.4} observed_ge1={observed:.0} corrected_ge1={corrected:.2} uncorrected_ge1={uncorrected:.2}",
        pooled_applied / runs,
    );
}

/// A pooled count vector as `ge1=sum(mean) .. `, so the sum the band is applied
/// to and the per-window mean a reader compares with the ten-run table are both
/// on the line.
fn pooled_row(counts: &[f64], runs: f64) -> String {
    RUNG_DIST_THRESHOLDS
        .iter()
        .zip(counts)
        .map(|(&k, n)| format!("ge{k}={n:.0}({:.2})", n / runs))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `ge1=.. ge2=.. .. ` for a measured rung-count vector.
fn rung_count_row(counts: &[usize]) -> String {
    RUNG_DIST_THRESHOLDS
        .iter()
        .zip(counts)
        .map(|(&k, n)| format!("ge{k}={n}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The same row for a predicted count vector, to two decimals.
fn predicted_row(counts: &[f64]) -> String {
    RUNG_DIST_THRESHOLDS
        .iter()
        .zip(counts)
        .map(|(&k, n)| format!("ge{k}={n:.2}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The law as this file carried it: every burst is charged the rung cost of an
/// *aligned* one, so the rung-producing rate is the all-datagram burst rate `E`.
fn law_rung_counts(inputs: &LadderInputs) -> [f64; RUNG_DIST_THRESHOLDS.len()] {
    let expected = expected_bursts(inputs);
    RUNG_DIST_THRESHOLDS.map(|k| expected * burst_tail_probability(inputs, rung_burst(inputs, k)))
}

/// The rungs a burst of `k` swallowed transmissions needs, in datagrams.
fn rung_burst(inputs: &LadderInputs, k: usize) -> f64 {
    (k * inputs.datagrams_per_transmission as usize) as f64
}

/// The rounds a window can show at each rung threshold, from the arm's own
/// numbers and the burst's **alignment**.
///
/// A burst costs `floor(l / m)` rungs only when it begins on a transmission's
/// first datagram: a burst beginning `r` datagrams into a group leaves the
/// other `m - r` copies delivered, the message arrives, and no rung fires at
/// all — a case the deterministic probe does not exercise, because it drives the
/// aligned burst only. The rung-producing events are therefore the bursts that
/// start on a transmission boundary, and a request/response round starts exactly
/// one of those. Their rate is `rounds * loss / mean_burst`, the per-datagram
/// burst-start probability evaluated at the transmission starts rather than at
/// every datagram the link carried.
fn corrected_rung_counts(inputs: &LadderInputs, rounds: u64) -> [f64; RUNG_DIST_THRESHOLDS.len()] {
    let aligned = rounds as f64 * inputs.loss / inputs.mean_burst;
    RUNG_DIST_THRESHOLDS.map(|k| aligned * burst_tail_probability(inputs, rung_burst(inputs, k)))
}

// ────────────── the loss model, as one attributable dimension ───────────────

/// The mean per-datagram loss rate both arms of the loss-model probe run at:
/// the `gilbert_elliott_loss(5.0, 8.0)` preset's steady-state share. The
/// independent twin is given this threshold, and the probe re-reads both arms'
/// rates out of the configuration the link actually carries
/// ([`loss_shape`]) rather than trusting this constant.
const LOSS_MODEL_PCT: u32 = 5;

/// Windows **per model** in the loss-model probe. The two models run inside one
/// test, alternately, at the same seeds on the same host, so the pairing — not
/// a longer sample — is what makes the dimension attributable. Four windows is
/// the sibling probe's own sample size, which is what the correlated arm's
/// first-rung band assertion needs to carry the same exposure it carries
/// there: the law predicts `~930 * 0.05/8 * (7/8)^5 = 2.99` rungs a window, so
/// four windows put its pooled count at ~12 against a band's low side of
/// ~3, and the deeper reading the discrimination rests on is the *second*
/// rung (`floor + one step`), which the law puts at `~930 * 0.05/8 *
/// (7/8)^11 = 1.34` a window — a pooled ~5.4, whose probability of a zero draw
/// is `0.4 %`.
const LOSS_MODEL_RUNS: usize = 4;

/// One loss model's pooled reading in the loss-model probe, every number read
/// from that arm's own runs and its own configured impairment.
struct LossModelArm {
    label: &'static str,
    /// The model's mean loss-burst length, in forwarded datagrams.
    mean_burst: f64,
    /// The model's long-run loss probability.
    loss: f64,
    runs: usize,
    rounds: u64,
    /// Rounds over each [`RUNG_DIST_THRESHOLDS`] threshold, pooled.
    threshold: [usize; RUNG_DIST_THRESHOLDS.len()],
    /// The law's aligned-burst prediction for the same pooled windows.
    corrected: [f64; RUNG_DIST_THRESHOLDS.len()],
    /// The law without the alignment term, printed beside it.
    uncorrected: [f64; RUNG_DIST_THRESHOLDS.len()],
    /// Rounds per rung bin, pooled.
    bins: Vec<usize>,
    /// The deepest ladder any sample reached, in rungs.
    max_rungs: f64,
    /// The pooled round trips, for the tail summary.
    samples: Vec<f64>,
    sent: u64,
    received: u64,
    /// Mean loss the link applied, and the share its model declares.
    applied: f64,
    declared: f64,
}

impl LossModelArm {
    fn new(spec: &ArmSpec) -> Self {
        let (mean_burst, loss) = loss_shape(spec);
        Self {
            label: spec.name,
            mean_burst,
            loss,
            runs: 0,
            rounds: 0,
            threshold: [0; RUNG_DIST_THRESHOLDS.len()],
            corrected: [0.0; RUNG_DIST_THRESHOLDS.len()],
            uncorrected: [0.0; RUNG_DIST_THRESHOLDS.len()],
            bins: vec![0; RUNG_DIST_BINS],
            max_rungs: 0.0,
            samples: Vec::new(),
            sent: 0,
            received: 0,
            applied: 0.0,
            declared: 0.0,
        }
    }

    /// Pooled `> 250 ms` rounds: the rigorous rung indicator, since a round
    /// trip on this arm is at most `2 * (OWD + HOSTILE_JITTER)` unless it
    /// waited for a repair.
    fn over250(&self) -> usize {
        self.threshold[0]
    }
}

/// The loss model as a **dimension** of the lone-tail regime: the `lone_tail`
/// arm's lane, request/response shape, depth, window, seeds, one-way delay,
/// jitter and message size held fixed, with the impairment's loss model
/// replaced — the four-state Gilbert-Elliott burst model against independent
/// loss at the same mean rate — and the rung distribution and the tail read
/// off both.
///
/// **Comparability, by arithmetic rather than assertion.**
/// `gilbert_elliott_loss(pct, mean_burst)` builds the two-state model
/// `p14 = p23 = p32 = 0`: a delivered packet leaves the gap state with `p13`, a
/// lost packet returns to it with `p31`, so the mean burst is `1 / p31` and the
/// steady-state loss share is `p13 / (p13 + p31)`. The preset chooses
/// `p31 = 1 / mean_burst` and `p13 = pct / (mean_burst * (1 - pct))`, which
/// makes that share exactly `pct`; at `pct = 5`, `mean_burst = 8` the scaled
/// integers are `p31 = 536870912` and `p13 = 28256364`, so the model's rate is
/// `28256364 / 565127276 = 0.0500000004`. The independent twin's threshold is
/// `loss_pct(5) = 214748360` of `u32::MAX`, i.e. `0.0500000` — the two agree
/// to `~4e-9`, a relative difference of `~8e-8`. The probe prints both rates
/// beside the loss each link's own counters measured, so the pair is shown to
/// be comparable rather than declared so.
///
/// **What it varies.** The loss model, and only the loss model: every other
/// setting is the sibling `mandate_smoke::m1_lone_tail_rung_distribution`'s
/// (itself the `lone_tail` arm's), so the two rows differ in exactly one
/// declared cell dimension, `loss-model=`.
///
/// **What it gates, and what it only measures.** The law the declaration
/// carries is `n = floor(burst / m)` — a statement about *burst length*, so at
/// one mean rate the correlated model's `mean_burst` sets the ladder and the
/// independent model's cannot. These quantities are asserted:
///
/// * the correlated arm's first-rung count sits inside its own law's band (the
///   sibling probe's check, so this arm carries the same law);
/// * the independent arm reaches the **second** rung **never** — its burst is
///   one datagram, `floor(1 / 6) = 0`, and the six-datagram cover consumes it
///   whole, while the correlated arm's own law puts `~5.4` such rounds in the
///   same pool;
/// * the independent arm's first-rung count stays below the *upper* band of the
///   correlated model's law, so a collapsed cover (which would put ~70 rounds a
///   window there) cannot pass as independence.
///
/// The tail's *level* (max, p99, p999) and the ladder's **depth** are printed
/// and not asserted. A maximum is one draw of
/// a geometric burst — `GATE.md` records why no M1 arm asserts one — and a
/// lower bound on the correlated arm's second-rung count would be a guard with
/// no margin over the one or two events four windows produce, which is the
/// family of check that fails on its own noise. What the arm asserts on the
/// direction is the side the law makes a hard statement about: an independent
/// burst cannot climb a rung.
///
/// It is a **new** arm: `clean`, `hostile`, `lone_tail`, the field-RTT arms and
/// the rung-distribution probe keep their impairment, windows, cadence, seeds,
/// tiers and assertions. Its vacuity demonstrations are input faults —
/// `MANDATE_SMOKE_FAULT=M1_LOSS_MODEL_uncorrelated` breaks the correlated
/// model's burst state (a mean burst of one datagram is independent loss
/// wearing the four-state model's name) and
/// `MANDATE_SMOKE_FAULT=M1_LOSS_MODEL_correlated` gives the control arm the
/// correlated model — so each failure is produced by the measurement path.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "lone-tail loss-model probe (the iid/GE pair at one mean loss); eight ~19 s windows; run with --ignored --nocapture"]
async fn m1_lone_tail_loss_model() {
    let _serial = SERIAL.lock().await;
    let base = mandate_arms("M1")
        .into_iter()
        .find(|spec| spec.name == "lone_tail")
        .expect("[loss-model] the M1 arm set must carry the lone_tail arm");

    // The same regime, independent loss at the correlated model's own mean
    // rate: only `loss_model` (and the threshold it reads) moves.
    let mut iid_c2s = base.int_c2s.clone();
    let mut iid_s2c = base.int_s2c.clone();
    iid_c2s.loss_model = LossModel::Random;
    iid_c2s.loss = loss_pct(LOSS_MODEL_PCT);
    iid_s2c.loss_model = LossModel::Random;
    iid_s2c.loss = loss_pct(LOSS_MODEL_PCT);
    let mut ge_c2s = base.int_c2s.clone();
    let mut ge_s2c = base.int_s2c.clone();
    match fault("M1_LOSS_MODEL").as_deref() {
        // Break the correlation: the four-state model with a mean burst of one
        // datagram has no burst state to consume the cover, so the correlated
        // arm collapses onto the independent one and fails its own law's band.
        Some("M1_LOSS_MODEL_uncorrelated") => {
            ge_c2s.loss_model = gilbert_elliott_loss(5.0, 1.0);
            ge_s2c.loss_model = gilbert_elliott_loss(5.0, 1.0);
        }
        // Break the independence: the control arm is handed the correlated
        // model, so it leaves the region the law says an independent model must
        // stay in and the discrimination assertion fails.
        Some("M1_LOSS_MODEL_correlated") => {
            iid_c2s.loss_model = gilbert_elliott_loss(5.0, 8.0);
            iid_s2c.loss_model = gilbert_elliott_loss(5.0, 8.0);
        }
        _ => {}
    }
    let specs = [
        ArmSpec {
            name: "lone_tail_iid",
            int_c2s: iid_c2s,
            int_s2c: iid_s2c,
            ..base.clone()
        },
        ArmSpec {
            name: "lone_tail_ge",
            int_c2s: ge_c2s,
            int_s2c: ge_s2c,
            ..base.clone()
        },
    ];

    eprintln!(
        "[loss-model] regime lane=dual shape=request-response depth=1 link=owd{}ms-jitter{}ms, seeds {} and {}, no bulk lane",
        base.int_c2s.latency.as_millis(),
        base.int_c2s.jitter.as_millis(),
        base.int_c2s.seed,
        base.int_s2c.seed,
    );
    let mut arms: Vec<LossModelArm> = specs.iter().map(LossModelArm::new).collect();
    for arm in &arms {
        eprintln!(
            "[loss-model] arm={} mean_burst={:.4} datagrams_per_transmission={} declared_loss={:.9}",
            arm.label, arm.mean_burst, TAIL_DATAGRAMS_PER_TRANSMISSION, arm.loss,
        );
    }

    for rep in 0..LOSS_MODEL_RUNS {
        for (index, spec) in specs.iter().enumerate() {
            let run =
                with_timeout(ARM_DEADLINE, "loss-model/lone_tail", run_arm(spec.clone())).await;
            let inputs = ladder_inputs(spec, run.int_c2s_packets, run.window);
            let arm = &mut arms[index];
            assert_eq!(
                spec.name, run.name,
                "[loss-model] run {rep} of {} is not the arm the probe asked for",
                spec.name,
            );
            assert!(
                run.samples.len() >= CENSORING_MIN_SAMPLES,
                "[loss-model] {} run {rep} produced {} rounds, too few for a rung histogram: the probe measured nothing",
                spec.name,
                run.samples.len(),
            );
            // The loss the link *applied*, counted by the link, against the
            // share its own model declares: the two models' comparability is a
            // measurement, not a claim.
            let applied = run.int_c2s_counters.dropped as f64 / run.int_c2s_packets as f64;
            assert!(
                applied >= 0.5 * inputs.loss && applied <= 2.0 * inputs.loss,
                "[loss-model] {} run {rep} applied {:.4} loss on {} c2s datagrams but its impairment declares {:.4}: the law's `loss` term is not this link's",
                spec.name,
                applied,
                run.int_c2s_packets,
                inputs.loss,
            );

            let floor_ms = LONE_TAIL_LADDER_FLOOR_MS;
            let run_floor_ms = run.samples.iter().cloned().fold(f64::INFINITY, f64::min);
            let mut by_threshold = [0usize; RUNG_DIST_THRESHOLDS.len()];
            let mut bins = vec![0usize; RUNG_DIST_BINS];
            let mut max_rungs = 0.0f64;
            for rtt in &run.samples {
                max_rungs = max_rungs.max((rtt - run_floor_ms) / inputs.step_ms);
                for (slot, threshold) in by_threshold.iter_mut().zip(RUNG_DIST_THRESHOLDS) {
                    if *rtt > rung_threshold_ms(threshold) {
                        *slot += 1;
                    }
                }
                let bin = if *rtt <= floor_ms {
                    0
                } else {
                    1 + (((rtt - floor_ms) / inputs.step_ms) as usize).min(RUNG_DIST_BINS - 2)
                };
                bins[bin] += 1;
            }
            let corrected = corrected_rung_counts(&inputs, run.samples.len() as u64);
            let uncorrected = law_rung_counts(&inputs);
            eprintln!(
                "[loss-model] arm={} run={rep} rounds={} c2s={} dropped={} applied={:.4} declared={:.4} E={:.1} max_rungs={:.2} over250={} p50={:.1} p99={:.1} p999={:.1} max={:.1}",
                arm.label,
                run.samples.len(),
                run.int_c2s_packets,
                run.int_c2s_counters.dropped,
                applied,
                inputs.loss,
                expected_bursts(&inputs),
                max_rungs,
                by_threshold[0],
                run.summary.p50,
                run.summary.p99,
                run.summary.p999,
                run.summary.max,
            );
            eprintln!(
                "[loss-model] arm={} run={rep} measured {} corrected {} uncorrected {} rounds_per_rung_bin(0..{RUNG_DIST_BINS})={bins:?}",
                arm.label,
                rung_count_row(&by_threshold),
                predicted_row(&corrected),
                predicted_row(&uncorrected),
            );

            arm.runs += 1;
            arm.rounds += run.samples.len() as u64;
            for (slot, value) in arm.threshold.iter_mut().zip(by_threshold) {
                *slot += value;
            }
            for (slot, value) in arm.bins.iter_mut().zip(bins) {
                *slot += value;
            }
            for (slot, value) in arm.corrected.iter_mut().zip(corrected) {
                *slot += value;
            }
            for (slot, value) in arm.uncorrected.iter_mut().zip(uncorrected) {
                *slot += value;
            }
            arm.max_rungs = arm.max_rungs.max(max_rungs);
            arm.samples.extend(run.samples.iter().copied());
            arm.sent += run.summary.sent;
            arm.received += run.summary.received;
            arm.applied += applied;
            arm.declared += inputs.loss;
        }
    }

    let runs = LOSS_MODEL_RUNS as f64;
    for arm in &arms {
        let tail = summarize(arm.samples.clone(), arm.sent, arm.received, 0, 0.0);
        eprintln!(
            "[loss-model] pooled arm={} runs={} rounds={} applied={:.4} declared={:.4} measured {} corrected {} uncorrected {} rounds_per_rung_bin(0..{RUNG_DIST_BINS})={:?}",
            arm.label,
            arm.runs,
            arm.rounds,
            arm.applied / runs,
            arm.declared / runs,
            rung_count_row(&arm.threshold),
            predicted_row(&arm.corrected),
            predicted_row(&arm.uncorrected),
            arm.bins,
        );
        eprintln!(
            "[loss-model] pooled arm={} max_rungs={:.2} over250={} ({:.3}%) p50={:.1} p99={:.1} p999={:.1} max={:.1}",
            arm.label,
            arm.max_rungs,
            arm.over250(),
            tail.over250_pct * 100.0,
            tail.p50,
            tail.p99,
            tail.p999,
            tail.max,
        );
        let waited: Vec<String> = arm
            .samples
            .iter()
            .filter(|rtt| **rtt > LONE_TAIL_LADDER_FLOOR_MS)
            .map(|rtt| format!("{rtt:.1}"))
            .collect();
        eprintln!(
            "[loss-model] pooled arm={} rounds_above_the_{LONE_TAIL_LADDER_FLOOR_MS:.0}ms_floor=[{}] (the rung each waited is that value less the floor, over the {LADDER_STEP_MS:.0} ms step)",
            arm.label,
            waited.join(" "),
        );
    }

    let iid = &arms[0];
    let ge = &arms[1];
    // The correlated arm's own law, at the first rung, on its own windows: the
    // sibling probe's check, so this arm carries the same law the declaration
    // states.
    let ge_band_ok = ge.over250() as f64 >= RUNG_DIST_BAND_LOW * ge.corrected[0]
        && ge.over250() as f64 <= RUNG_DIST_BAND_HIGH * ge.corrected[0];
    // The law's discrimination, stated at the depth it is a statement about.
    // `RUNG_DIST_THRESHOLDS[1]` is the second rung (`floor + one step`), and
    // `n = floor(burst / m) = 2` needs a 12-datagram aligned burst: the
    // correlated model's own aligned-burst rate puts ~5.4 such windows in the
    // pool, while the independent model's `floor(1 / 6) = 0` forbids one
    // outright. Only the independent side is *gated*: its bound is a property
    // of the law's burst term, whereas a lower bound on the correlated side
    // would be a guard with no margin over the one-to-two events four windows
    // produce, so the measured direction is reported beside it instead.
    let iid_shallow_only = iid.threshold[1] == 0;
    // Independence must not be *worse* than the correlated model's own upper
    // band: a cover that collapsed would put ~70 first-rung rounds a window on
    // the independent arm, far above the band the law budgets for bursts.
    let iid_within_the_ge_band = iid.over250() as f64 <= RUNG_DIST_BAND_HIGH * ge.corrected[0];
    eprintln!(
        "[loss-model] check=correlated-law observed={} corrected={:.1} band=[{:.2},{:.2}] verdict={}",
        ge.over250(),
        ge.corrected[0],
        RUNG_DIST_BAND_LOW * ge.corrected[0],
        RUNG_DIST_BAND_HIGH * ge.corrected[0],
        verdict(ge_band_ok),
    );
    eprintln!(
        "[loss-model] check=independent-cannot-climb iid_ge2={} correlated_ge2={} (the law's own rate over this pool is {:.2} correlated, 0.00 independent) verdict={}",
        iid.threshold[1],
        ge.threshold[1],
        ge.corrected[1],
        verdict(iid_shallow_only),
    );
    eprintln!(
        "[loss-model] check=independence-not-worse iid_ge1={} allowance=<= {:.1} (band-high {RUNG_DIST_BAND_HIGH} x the correlated law's own first-rung prediction {:.2}) verdict={}",
        iid.over250(),
        RUNG_DIST_BAND_HIGH * ge.corrected[0],
        ge.corrected[0],
        verdict(iid_within_the_ge_band),
    );
    let ge_tail = summarize(ge.samples.clone(), ge.sent, ge.received, 0, 0.0);
    let iid_tail = summarize(iid.samples.clone(), iid.sent, iid.received, 0, 0.0);
    eprintln!(
        "[loss-model] measured_direction max_rungs ge={:.2} iid={:.2} | deepest_ladder_ms ge={:.1} iid={:.1} | p99 ge={:.1} iid={:.1} | p999 ge={:.1} iid={:.1} | max ge={:.1} iid={:.1} | over250 ge={} iid={} | ge2 ge={} iid={} (the depth is where the correlated arm is longer; the first-rung count is not far apart and the arm reports it rather than claiming it)",
        ge.max_rungs,
        iid.max_rungs,
        ge_tail.max,
        iid_tail.max,
        ge_tail.p99,
        iid_tail.p99,
        ge_tail.p999,
        iid_tail.p999,
        ge_tail.max,
        iid_tail.max,
        ge.over250(),
        iid.over250(),
        ge.threshold[1],
        iid.threshold[1],
    );

    assert!(
        ge_band_ok,
        "[loss-model] the correlated arm's {} rounds over {LONE_TAIL_LADDER_FLOOR_MS:.0} ms over {} windows sit outside the [{:.2}, {:.2}] its own law predicts ({:.2}), so the ladder the declaration's `n = floor(burst / m)` names is not what this arm measured",
        ge.over250(),
        ge.runs,
        RUNG_DIST_BAND_LOW * ge.corrected[0],
        RUNG_DIST_BAND_HIGH * ge.corrected[0],
        ge.corrected[0],
    );
    assert!(
        iid_shallow_only,
        "[loss-model] at {:.4} mean loss the independent arm reached the second rung {} time(s); the correlated arm reached it {} (its own law puts {:.2} in the pool over {} windows), so `n = floor(burst / m)` says a 12-datagram aligned burst is what climbs a second rung, and the independent model's burst is one datagram that the six-datagram cover consumes whole",
        iid.declared / runs,
        iid.threshold[1],
        ge.threshold[1],
        ge.corrected[1],
        ge.runs,
    );
    assert!(
        iid_within_the_ge_band,
        "[loss-model] the independent arm's {} rounds over {LONE_TAIL_LADDER_FLOOR_MS:.0} ms exceeds the {:.1} the correlated model's own upper band allows: independent loss must not be worse than the law already budgets for a burst model, and a cover that had collapsed would sit an order of magnitude above this",
        iid.over250(),
        RUNG_DIST_BAND_HIGH * ge.corrected[0],
    );
    println!(
        "LOSS_MODEL PASS runs_per_model={LOSS_MODEL_RUNS} rounds_ge={} rounds_iid={} ge_over250={} (corrected {:.2}) ge_ge2={} iid_over250={} iid_ge2={}",
        ge.rounds,
        iid.rounds,
        ge.over250(),
        ge.corrected[0],
        ge.threshold[1],
        iid.over250(),
        iid.threshold[1],
    );
}

// ───────────────────── M1 at the field's RTT scale ─────────────────────────

/// Mandate 1 at the deployed client's round-trip scale.
///
/// The M1/M2 arms above run the deployment's 25 ms one-way profile (~50 ms
/// round trip). The deployed client reports a ~190 ms *minimum* round trip, so
/// "the tail holds on the `clean` arm" says nothing about the RTT the field
/// sees: the M1 breach is a repair ladder, and a ladder's step and rung count
/// are not RTT-invariant. This arm re-runs the `lone_tail` shape
/// (request/response, depth 1, one unacked 256 B message) on
/// [`FIELD_RTT_OWD`]'s ~190 ms round trip and asserts a derived regression
/// guard on the same two quantities M1 asserts on the smoke arms.
///
/// It is a **new** arm, not a retuned one: the `clean`, `hostile` and
/// `lone_tail` arms keep their settings, tiers and guards. It is `#[ignore]`d
/// (`full` tier) because it needs its own ~20 s window on top of the smoke
/// set's ~3 minutes, and because it is a measurement of the open tail defect
/// rather than a mandate bound that currently holds. The guard's derivation is
/// in `GATE.md`; the constants above carry a pointer to it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "field-RTT lone-tail arm; ~20 s; run with --ignored --nocapture"]
async fn m1_lone_tail_field_rtt() {
    let _serial = SERIAL.lock().await;
    let spec = field_rtt_arm();
    let run = with_timeout(
        ARM_DEADLINE,
        "m1-field-rtt/lone_tail",
        run_arm(spec.clone()),
    )
    .await;
    print_arm(&run);
    // The arm's own censoring reading: reported, never asserted here. The arm's
    // window, cadence, seeds, guards and tier are unchanged, and the instrument
    // that would turn this line into a gate is the default-tier
    // `m1_latency_window_censoring` test.
    report_censoring(spec.name, &spec, &run);

    let pass = run.summary.p99 <= M1_FIELD_RTT_P99_GUARD_MS
        && over250_pct(&run.samples) <= M1_FIELD_RTT_OVER250_GUARD_PCT;
    println!(
        "MANDATE M1_FIELD_RTT {} owd_ms={} samples={} p50={:.1} p90={:.1} p99={:.1} p999={:.1} max={:.1} over250={} over250_pct={:.3} p99_guard={:.1} over250_guard={:.1} ceiling={:.1}",
        verdict(pass),
        FIELD_RTT_OWD.as_millis(),
        run.samples.len(),
        run.summary.p50,
        run.summary.p90,
        run.summary.p99,
        run.summary.p999,
        run.summary.max,
        over250_count(&run.samples),
        over250_pct(&run.samples),
        M1_FIELD_RTT_P99_GUARD_MS,
        M1_FIELD_RTT_OVER250_GUARD_PCT,
        M1_CEILING_MS,
    );

    assert!(
        run.summary.p99 <= M1_FIELD_RTT_P99_GUARD_MS,
        "[M1] field-RTT ({FIELD_RTT_OWD:?} one-way) lone-tail arm p99 {:.1} ms exceeds its {M1_FIELD_RTT_P99_GUARD_MS} ms regression guard (max {:.1} ms): the lone-tail defect at the deployed client's ~190 ms round trip has grown by at least 2x",
        run.summary.p99,
        run.summary.max,
    );
    assert!(
        over250_pct(&run.samples) <= M1_FIELD_RTT_OVER250_GUARD_PCT,
        "[M1] field-RTT lone-tail arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_FIELD_RTT_OVER250_GUARD_PCT}% regression guard",
        over250_pct(&run.samples),
    );
}

/// The field-RTT lone-tail arm's `depth` dimension, measured instead of
/// inferred: the same field impairment ([`field_rtt_link`]'s ~190 ms round
/// trip, the 5 %-mean-8 GE model and 100 ms jitter), the same
/// request/response shape and the same window as
/// [`m1_lone_tail_field_rtt`], offered at `depth` 1 and then at `depth` 2.
///
/// It is the arm that makes the field row attributable. Against the family's
/// reference (`rtp_mux_jitter::jitter_request_response_arms`, the 25 ms
/// impairment sweep at `depth=1-and-2`) it varies exactly one declared
/// dimension — its impairment, which is the field's own — and against
/// [`m1_lone_tail_field_rtt`] it varies exactly one — the second, pipelined
/// message per round. The depth-1 field arm alone moves its impairment and its
/// depth together, so a tail it reports cannot be assigned to either; the
/// sweep is what separates them.
///
/// Each depth asserts both M1 regression guards, so a depth effect that
/// breached them is a gate rather than a printed table. It is `#[ignore]`d
/// (`full` tier) because it needs two more ~20 s windows on top of the smoke
/// set's ~3 minutes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "field-RTT lone-tail depth sweep; two ~20 s arms; run with --ignored --nocapture"]
async fn m1_lone_tail_field_rtt_depth_sweep() {
    let _serial = SERIAL.lock().await;
    for (name, depth) in [("field_rtt_d1", 1usize), ("field_rtt_d2", 2usize)] {
        let mut spec = field_rtt_arm();
        spec.name = name;
        spec.load = Load::RequestResponse { depth };
        let run = with_timeout(
            ARM_DEADLINE,
            "m1-field-rtt/depth_sweep",
            run_arm(spec.clone()),
        )
        .await;
        print_arm(&run);
        // The arm's own censoring reading, reported and not asserted, for the
        // same reason as [`m1_lone_tail_field_rtt`]'s.
        report_censoring(spec.name, &spec, &run);

        let pass = run.summary.p99 <= M1_FIELD_RTT_P99_GUARD_MS
            && over250_pct(&run.samples) <= M1_FIELD_RTT_OVER250_GUARD_PCT;
        println!(
            "MANDATE M1_FIELD_RTT_DEPTH {} depth={} owd_ms={} samples={} p50={:.1} p90={:.1} p99={:.1} p999={:.1} max={:.1} over250={} over250_pct={:.3} p99_guard={:.1} over250_guard={:.1} ceiling={:.1}",
            verdict(pass),
            depth,
            FIELD_RTT_OWD.as_millis(),
            run.samples.len(),
            run.summary.p50,
            run.summary.p90,
            run.summary.p99,
            run.summary.p999,
            run.summary.max,
            over250_count(&run.samples),
            over250_pct(&run.samples),
            M1_FIELD_RTT_P99_GUARD_MS,
            M1_FIELD_RTT_OVER250_GUARD_PCT,
            M1_CEILING_MS,
        );

        assert!(
            run.summary.p99 <= M1_FIELD_RTT_P99_GUARD_MS,
            "[M1] field-RTT depth-{depth} arm p99 {:.1} ms exceeds its {M1_FIELD_RTT_P99_GUARD_MS} ms regression guard (max {:.1} ms): the depth-{depth} field-RTT tail has grown past the guard the depth-1 field arm fixes",
            run.summary.p99,
            run.summary.max,
        );
        assert!(
            over250_pct(&run.samples) <= M1_FIELD_RTT_OVER250_GUARD_PCT,
            "[M1] field-RTT depth-{depth} arm has {:.3}% of samples > {M1_CEILING_MS} ms, over its {M1_FIELD_RTT_OVER250_GUARD_PCT}% regression guard",
            over250_pct(&run.samples),
        );
    }
}

// ────────────────────────── M2: delivery and wire ────────────────────────────

// ───────── M2: the offered load's latency does not degrade ──────────────────

/// The **known offered throughput** of a cadence arm: `MSG_BYTES` every
/// [`CADENCE`]. This is M2's *input*: the mandate asserts that a lane offered
/// this rate keeps its latency at the floor, and infers the goodput from that.
fn cadence_offer_bps() -> f64 {
    MSG_BYTES as f64 / CADENCE.as_secs_f64()
}

/// The number of messages a cadence arm's schedule offers over `window`.
///
/// Integer nanoseconds, matching [`offer_cadence_on_deadline`]'s own schedule
/// count exactly: `window / CADENCE` in `f64` lands on 2399.9999999999995 for
/// the arms' own 12 s / 5 ms, which is the same count to any reader but not the
/// same number as the sender's schedule.
fn cadence_offer_messages(window: Duration) -> f64 {
    (window.as_nanos() / CADENCE.as_nanos()) as f64
}

fn m2_declaration() -> String {
    format!(
        r#"{{"mandate":"M2","title":"M2 interactive delivery and latency under a known offer (1=clean 2=hostile 3=lone_tail)","x_label":"arm (1=clean 2=hostile 3=lone_tail)","y_label":"value","panels":[{{"id":"delivery","chart":"bar","series":[{{"name":"delivery"}}],"bounds":[{{"y":1.0,"label":"M2 delivery floor 1.000"}}]}},{{"id":"latency","chart":"bar","series":[{{"name":"p99_ms"}}],"bounds":[{{"y":{M2_NONDEGRADING_P99_MS},"label":"M2 non-degrading p99 bound (ms)","x":[1]}}]}}]}}"#
    )
}

fn m2_rows(runs: &[ArmRun]) -> Vec<(String, String, f64, f64)> {
    // One bar per arm per panel: the delivery relation and the latency the
    // mandate asserts under the offer, each against its own bound.
    let mut rows = Vec::new();
    for (index, run) in runs.iter().enumerate() {
        let x = (index + 1) as f64;
        rows.push((
            "delivery".to_owned(),
            "delivery".to_owned(),
            x,
            run.summary.delivery_pct,
        ));
        rows.push((
            "latency".to_owned(),
            "p99_ms".to_owned(),
            x,
            run.summary.p99,
        ));
    }
    rows
}

/// Mandate 2: the interactive lane is offered a **known throughput** and its
/// latency does **not degrade** under that offer. The `clean` cadence arm
/// asserts the mandate — the offered message count is its schedule (the
/// input, written by [`offer_cadence_on_deadline`] so that it is the schedule's
/// count and not the host's wake success), it delivers all of it (`delivery ==
/// 1.000`), and its p99 stays at the link's floor
/// ([`M2_NONDEGRADING_P99_MS`]) — so the goodput is inferred from the latency
/// holding: a lane draining what it is offered cannot be accumulating a queue,
/// and a lane whose goodput fell would have to show the backlog as latency or
/// stop offering. The `hostile` cadence arm asserts the same known offer plus
/// its delivery floor; the lone-tail arm keeps its delivery floor as a
/// regression guard.
#[tokio::test(flavor = "multi_thread")]
async fn m2_offered_load_latency() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let runs = mandate_runs("M2").await;
    write_evidence(&dir, "M2", &m2_declaration(), &m2_rows(&runs));

    let clean = &runs[0];
    let hostile = &runs[1];
    let lone = &runs[2];

    // The mandate's input: the known offered throughput and the message count
    // the two cadence arms' schedules must have produced over their windows.
    let offer_bps = cadence_offer_bps();
    let clean_offer_floor = cadence_offer_messages(clean.window) * (1.0 - M2_OFFER_TOLERANCE);
    let hostile_offer_floor = cadence_offer_messages(hostile.window) * (1.0 - M2_OFFER_TOLERANCE);
    let clean_offer_met = clean.summary.sent as f64 >= clean_offer_floor;
    let hostile_offer_met = hostile.summary.sent as f64 >= hostile_offer_floor;
    let clean_delivered = clean.summary.received == clean.summary.sent;
    let clean_latency_held = clean.summary.p99 <= M2_NONDEGRADING_P99_MS;
    let pass = clean_offer_met
        && clean_delivered
        && clean_latency_held
        && hostile_offer_met
        && hostile.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR
        && lone.summary.delivery_pct >= M2_LONE_DELIVERY_FLOOR;
    println!(
        "MANDATE M2 {} clean_offer_msgs={} clean_offer_floor={:.0} clean_offered_bps={:.0} clean_delivery={:.3} clean_p99={:.1} hostile_offer_msgs={} hostile_offer_floor={:.0} hostile_delivery={:.3} lone_delivery={:.3} offer_bps={:.0} offer_tolerance={:.2} nondergrading_p99_ms={:.1} delivery_floor={:.3}",
        verdict(pass),
        clean.summary.sent,
        clean_offer_floor,
        offer_bps,
        clean.summary.delivery_pct,
        clean.summary.p99,
        hostile.summary.sent,
        hostile_offer_floor,
        hostile.summary.delivery_pct,
        lone.summary.delivery_pct,
        offer_bps,
        M2_OFFER_TOLERANCE,
        M2_NONDEGRADING_P99_MS,
        M2_LONE_DELIVERY_FLOOR,
    );

    assert!(
        clean_latency_held,
        "[M2] the clean arm's p99 {:.1} ms exceeds the {M2_NONDEGRADING_P99_MS} ms non-degradation bound under its {:.0} B/s offer: the lane's latency degraded, so the backlog was not drained and the goodput is not what was offered",
        clean.summary.p99, offer_bps,
    );
    assert_eq!(
        clean.summary.received, clean.summary.sent,
        "[M2] clean-arm interactive delivery must be exactly 1.000: {}/{} messages delivered ({:.3}) — the interactive lane ate its own goodput",
        clean.summary.received, clean.summary.sent, clean.summary.delivery_pct,
    );
    assert!(
        clean_offer_met,
        "[M2] the clean arm offered {} messages of the {:.0} its {:.0} B/s schedule requires over {:?} (a {:.2} tolerance): a lane that was never offered the known throughput has no goodput to infer, so the offer is asserted as the mandate's input",
        clean.summary.sent,
        cadence_offer_messages(clean.window),
        offer_bps,
        clean.window,
        M2_OFFER_TOLERANCE,
    );
    assert!(
        hostile_offer_met,
        "[M2] the hostile arm offered {} messages of the {:.0} its {:.0} B/s schedule requires over {:?} (a {:.2} tolerance): the hostile delivery floor is only a statement about that offer",
        hostile.summary.sent,
        cadence_offer_messages(hostile.window),
        offer_bps,
        hostile.window,
        M2_OFFER_TOLERANCE,
    );
    assert!(
        hostile.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR,
        "[M2] hostile (GE+jitter) arm delivery {:.3} fell below its {M2_HOSTILE_DELIVERY_FLOOR} regression floor",
        hostile.summary.delivery_pct,
    );
    assert!(
        lone.summary.delivery_pct >= M2_LONE_DELIVERY_FLOOR,
        "[M2] hostile lone-tail arm delivery {:.3} fell below its {M2_LONE_DELIVERY_FLOOR} regression floor",
        lone.summary.delivery_pct,
    );
}

// ─────────────────────────────── M3: goodput ─────────────────────────────────

struct BulkRep {
    delivered_mib_s: f64,
    shaper_mib_s: f64,
    capacity_mib_s: f64,
    fraction: f64,
    delivered_bytes: u64,
    shaper_bytes: u64,
    elapsed: Duration,
}

fn m3_declaration() -> String {
    // The fraction floor is the configured-rate fraction; the goodput panel's
    // floor line is that same fraction expressed in MiB/s at the configured
    // capacity, so the reader sees both the raw rates and the ratio.
    let capacity_mib_s = M3_CAPACITY_BPS as f64 / 8.0 / (1024.0 * 1024.0);
    let floor_mib_s = capacity_mib_s * M3_CAPACITY_FRACTION;
    format!(
        r#"{{"mandate":"M3","title":"M3 bulk goodput vs the shaped clock and the configured link rate","x_label":"seed","y_label":"MiB/s","panels":[{{"id":"goodput","chart":"bar","series":[{{"name":"delivered"}},{{"name":"shaper_forwarded"}}],"bounds":[{{"y":{floor_mib_s:.6},"label":"M3 floor {M3_CAPACITY_FRACTION}x link rate"}}]}},{{"id":"fraction","chart":"bar","series":[{{"name":"fraction"}}],"bounds":[{{"y":{M3_CAPACITY_FRACTION},"label":"M3 floor {M3_CAPACITY_FRACTION}x link rate"}}]}}]}}"#
    )
}

fn m3_rows(reps: &[BulkRep]) -> Vec<(String, String, f64, f64)> {
    let mut rows = Vec::new();
    // Every series carries a point at every rep's x so the renderer groups the
    // two goodput bars side by side at each rep (its grouped-bar slot math
    // assumes a full matrix).
    for (index, rep) in reps.iter().enumerate() {
        let x = (index + 1) as f64;
        rows.push((
            "goodput".to_owned(),
            "delivered".to_owned(),
            x,
            rep.delivered_mib_s,
        ));
        rows.push((
            "goodput".to_owned(),
            "shaper_forwarded".to_owned(),
            x,
            rep.shaper_mib_s,
        ));
        rows.push((
            "fraction".to_owned(),
            "fraction".to_owned(),
            x,
            rep.fraction,
        ));
    }
    rows
}

/// One M3 rep: the production dual-lane composition with the bulk lane
/// saturated, sampled at both ends of the offered window while the pump still
/// runs. `delivered` is the sink's delta and `shaper_forwarded` the shaped
/// link's own forwarded delta over the same interval — the in-process
/// reference clock the within-run ratio is taken against.
async fn run_bulk_rep(window: Duration, starve: bool) -> BulkRep {
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    // A starved bulk lane: the shaper reference (the configured capacity) is
    // unchanged, so the achieved fraction collapses.
    let link_rate = if starve {
        M3_CAPACITY_BPS / 10
    } else {
        M3_CAPACITY_BPS
    };
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_sink, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(
                int_addr,
                link(41, OWD, JITTER, 0, 0),
                link(42, OWD, JITTER, 0, 0),
            )
            .unwrap();
            let bulk_pair = NetemPair::spawn(
                bulk_addr,
                link(43, OWD, JITTER, 0, link_rate),
                link(44, OWD, JITTER, 0, link_rate),
            )
            .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
            )
            .await
            .unwrap();

            let (mut lat_read, mut lat_write) = opener.open(LaneClass::Interactive).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = lat_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let (mut bulk_read, mut bulk_write) = opener.open(LaneClass::Bulk).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            // The interactive lane's light latency stream runs for the whole
            // rep so the topology is the true dual-lane one; its load is
            // negligible against the shaped bulk lane.
            let interactive = async {
                let _ = lat_write.write_all(b"L").await;
                send_timestamped_messages(
                    &mut lat_write,
                    base,
                    MSG_BYTES,
                    CADENCE,
                    BULK_RAMP + window + GRACE,
                )
                .await
            };
            let mut pump = tokio::task::JoinSet::new();
            let (pump_stop_tx, mut pump_stop_rx) = tokio::sync::watch::channel(false);
            pump.spawn(async move {
                let payload = cyclic_payload(64 * 1024 * 1024);
                let mut offset = 0usize;
                if bulk_write.write_all(b"B").await.is_err() {
                    return;
                }
                loop {
                    tokio::select! {
                        _ = pump_stop_rx.changed() => break,
                        result = bulk_write.write(&payload[offset..]) => match result {
                            Ok(0) => break,
                            Ok(n) => offset = (offset + n) % payload.len(),
                            Err(_) => break,
                        },
                    }
                }
            });

            tokio::time::sleep(BULK_RAMP).await;
            let window_start = Instant::now();
            let delivered_before = bulk_sink.load(Ordering::Relaxed);
            let forwarded_before = bulk_pair.stats_c2s().forwarded_bytes;
            tokio::select! {
                joined = pump.join_next(), if !pump.is_empty() => {
                    joined.expect("bulk pump exists").unwrap();
                    panic!("[M3] bulk pump ended before the measurement window completed");
                }
                _ = tokio::time::sleep(window) => {}
            }
            let elapsed = window_start.elapsed();
            let delivered = bulk_sink
                .load(Ordering::Relaxed)
                .saturating_sub(delivered_before);
            let forwarded = bulk_pair
                .stats_c2s()
                .forwarded_bytes
                .saturating_sub(forwarded_before);
            pump_stop_tx.send(true).unwrap();
            // Drain the window's stragglers before teardown; the drain is not
            // part of the measured interval (the clock runs while the sender
            // pumps).
            let _ = interactive.await;
            tokio::time::sleep(GRACE).await;
            while let Some(result) = pump.join_next().await {
                result.unwrap();
            }
            while latencies.try_recv().is_ok() {}
            let capacity_mib_s = M3_CAPACITY_BPS as f64 / 8.0 / (1024.0 * 1024.0);
            let secs = elapsed.as_secs_f64().max(f64::EPSILON);
            let delivered_mib_s = delivered as f64 / (1024.0 * 1024.0) / secs;
            let shaper_mib_s = forwarded as f64 / (1024.0 * 1024.0) / secs;
            int_pair.stop();
            bulk_pair.stop();
            BulkRep {
                delivered_mib_s,
                shaper_mib_s,
                capacity_mib_s,
                fraction: delivered_mib_s / capacity_mib_s,
                delivered_bytes: delivered,
                shaper_bytes: forwarded,
                elapsed,
            }
        })
        .await
}

/// Mandate 3: bulk goodput on the production dual-lane topology, as a
/// within-run fraction of the shaped clock / configured link rate, median of
/// three seeded reps, `>= 0.35x`.
#[tokio::test(flavor = "multi_thread")]
async fn m3_bulk_goodput_fraction() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let starve = fault("M3").as_deref() == Some("M3_starve");
    let window = bulk_window();
    let mut reps = Vec::new();
    for rep in 1..=M3_REPS {
        let label = format!("m3/rep{rep}");
        let result = with_timeout(
            Duration::from_secs(90),
            &label,
            run_bulk_rep(window, starve),
        )
        .await;
        eprintln!(
            "[mandate-smoke m3/rep{rep}] delivered {:.3} MiB/s over {:?}, shaper forwarded {:.3} MiB/s, capacity {:.3} MiB/s, fraction {:.3} ({} / {} bytes)",
            result.delivered_mib_s,
            result.elapsed,
            result.shaper_mib_s,
            result.capacity_mib_s,
            result.fraction,
            result.delivered_bytes,
            result.shaper_bytes,
        );
        reps.push(result);
    }
    let mut fractions: Vec<f64> = reps.iter().map(|r| r.fraction).collect();
    fractions.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = fractions[fractions.len() / 2];
    let delivered_median = {
        let mut v: Vec<f64> = reps.iter().map(|r| r.delivered_mib_s).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let shaper_median = {
        let mut v: Vec<f64> = reps.iter().map(|r| r.shaper_mib_s).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let wall: f64 = reps.iter().map(|r| r.elapsed.as_secs_f64()).sum::<f64>();
    write_evidence(&dir, "M3", &m3_declaration(), &m3_rows(&reps));

    let pass = median >= M3_CAPACITY_FRACTION;
    println!(
        "MANDATE M3 {} delivered_mib_s={:.3} shaper_mib_s={:.3} capacity_mib_s={:.3} fraction={:.3} floor={:.3} reps={} measured_s={:.1}",
        verdict(pass),
        delivered_median,
        shaper_median,
        reps[0].capacity_mib_s,
        median,
        M3_CAPACITY_FRACTION,
        M3_REPS,
        wall,
    );

    assert!(
        median >= M3_CAPACITY_FRACTION,
        "[M3] median bulk goodput fraction {median:.3} < the {M3_CAPACITY_FRACTION} floor (delivered {delivered_median:.3} MiB/s of the {:.3} MiB/s configured link rate; per-rep fractions {fractions:?}): the bulk lane must keep a high fraction of its link's capacity on the dual-lane topology",
        reps[0].capacity_mib_s,
    );
}

// ───────────────────────── M4: interactive lane fairness ─────────────────────
//
// M1 and M2 measure ONE interactive flow. A mandate result achieved by
// starving one of several flows sharing the interactive lane is not a pass, so
// M4 measures the *split* of the same production interactive lane across
// several flows offering the same payload at the same cadence: every flow must
// deliver what it is offered (no starvation), no flow's share of the lane's
// delivered bytes may depart from the equal share by more than the bound
// derived in `rtp_mux/GATE.md` (fair share), and no flow's p99 may depart from
// its peers' the way the share statistic cannot see. The per-flow latencies
// are also reported and drawn against M1's own ceiling, so a result that is
// fair and slow is visible; M1 stays the authority for that ceiling.
//
// The arm is the M1/M2 `clean` interactive lane with `M4_FLOWS` interactive
// streams on it instead of one, and the second arm is the M1/M2 `hostile`
// impairment with the same multi-flow offer — the same link, the same tagged-
// stream sink (`spawn_tagged_stream_sink` buckets every sample by the flow's
// first-byte tag as it already does for the two-interactive battery), the same
// `send_timestamped_messages` offer. The bulk lane is connected (the topology
// is the production dual-lane one) but carries no stream: M4 isolates the
// interactive lane's own split, which is the quantity the mandate-3 arms do
// not measure.

/// The interactive flows multiplexed on the one interactive lane. Four is the
/// smallest count that makes an unfair split a *share* rather than a binary
/// win/lose, and it is the count the existing 4-flow scaling probe uses, so a
/// skew seen here is comparable with that arm's per-flow floors.
const M4_FLOWS: usize = 4;

/// The per-flow delivery floor. Derived from M4's own measurement (GATE.md):
/// both arms delivered every offered message on every flow across the 29 runs
/// the bound is derived from, so the floor carries the same slack M2's hostile
/// floor uses -- a flow that loses more than ~0.5 % of its own offer is
/// starved, while ordinary tail-repair jitter never trips it.
///
/// The floor bounds `lost`: a message this arm *never* observed. It did not
/// always: the ratio used to be read at the `window + GRACE` cutoff, so a
/// message still riding the repair ladder when the drain expired was counted as
/// lost although the lane delivered it -- which made the floor race the ladder
/// rather than measure starvation. See [`M4_LATE_HORIZON`].
const M4_DELIVERY_FLOOR: f64 = 0.995;

/// How long past the `window + GRACE` cutoff the arm keeps observing before a
/// message it has not seen is a *delivery loss*.
///
/// `GRACE` (2 s) is the drain a cadence arm's summary is read after, and it sits
/// **inside** the repair ladder this lane's tail rides: the ladder's post-probe
/// step is `TAIL_PROBED_MIN_RTO` (300 ms,
/// `rtp/src/traffic_shaping/recovery/tlp.rs`) and its retransmission deadline is
/// floored at the 1 s `MIN_RTO` (`rtp/src/traffic_shaping/recovery/rto.rs`)
/// with exponential backoff, and M1's lone-tail arm -- the same ladder on the
/// same lane -- measures its p99 at 1530 ms and guards it at
/// [`M1_LONE_P99_GUARD_MS`] (3200 ms). The documented tail therefore outlives
/// the cutoff by `3200 - 2000 = 1200 ms`, which is this horizon: a message
/// observed inside it is **late** (the lane delivered it, so it is a latency
/// event and not a delivery loss), and only a message this arm never observes
/// is lost. The horizon is a new constant rather than a wider `GRACE` because
/// the cutoff is the *on-time* boundary the arm already measured: the two cells
/// answer different questions and neither may be moved to make the other pass.
const M4_LATE_HORIZON: Duration = Duration::from_millis(1_200);

/// The late drain's poll interval. The horizon is a deadline the arm leaves as
/// soon as every flow's whole offer has been observed, so this is the
/// resolution of that early exit rather than a cost every run pays.
const M4_LATE_POLL: Duration = Duration::from_millis(25);

/// The fair-share imbalance bound: the worst flow's share of the lane's
/// delivered bytes may not depart from the equal share `1/M4_FLOWS` by more
/// than this fraction of the equal share. Derived from M4's own measurement
/// (GATE.md): across the 29 runs the bound is derived from, the worst
/// departure was `0.46 %` (clean arm; hostile `0.43 %`), while one
/// delivered frame is `1 / (4 x 2064) = 0.012 %` of the lane -- `0.048 %` of
/// the equal share -- so the observed skew is a handful of frames of
/// connection ramp at the window edges. The bound is `2.2x` the worst measured
/// departure, so a change that at least doubles the imbalance fails while
/// frame-edge ramp cannot reach it.
const M4_IMBALANCE_BOUND: f64 = 0.01;

/// The fair-latency bound: the worst flow's **90th-percentile** one-way
/// latency may not exceed the best flow's by more than this factor, asserted on
/// the **clean** arm. One authority for the quantile is
/// [`fair_latency_spread`].
///
/// **Why the p90 and not the p99.** The bound claims a *lane property*: the
/// lane's service is not ordered preferentially towards one flow. On the
/// pre-transient clean lane the p99 was a usable proxy, because every flow's
/// p99 sat on the shared start-up transient's own level. Once the transient is
/// gone (pacer seed 4096, `rtp/GATE.md`) each flow's p99 is the `floor +` the
/// size of *that flow's* 24th-largest sample, and on this lane's 2 % iid link
/// those 24 samples are rare-event draws: measured per-flow p99 at seed 4096
/// over 13 reps spans `26.2-99.2 ms` within a single arm while every flow's
/// p50 stays at `21.2-23.4 ms`, so `max p99 / min p99` is a ratio of two small
/// integer counts and reached `2.25` with **delivery 1.000 on every flow** and
/// no scheduler asymmetry at all. A bound that fires on a count draw is not
/// coverage. The p90 is the quantile the transient moves and the rare-event
/// count does not: at seed 1024 it sits inside the transient (`137.7-158.8 ms`,
/// spread `1.006-1.098`) and at 4096 on the floor (`23.6-29.7 ms`, spread
/// `1.10-1.14`), so over both seeds and 13 reps the measured spread is
/// `1.006-1.144` -- 1.75x inside this bound -- while a flow whose ordering is
/// genuinely deprioritised moves its whole body and fails it (the
/// `M4_FAIR_LATENCY_hold` probe below fails at `+30 ms` of one-flow hold).
/// The p99 spread is deliberately **not** asserted anywhere: it is the count
/// draw described above, and `m4_clean_lane_fair_latency`'s doc records what
/// that leaves uncovered.
///
/// It asserts the dimension the share statistic cannot see: with equal offers
/// and per-flow delivery at 1.000, a scheduler that favours one flow's
/// *ordering* rather than its goodput would show up here and not in the shares.
/// It is deliberately **not** asserted on the hostile arm, where the per-flow
/// differences are a GE loss realization rather than a scheduler property: that
/// arm measured a spread of up to `2.93x` across 10 runs, so an asserted spread
/// there would measure which flow caught the burst. The hostile arm keeps M1's
/// absolute guard.
const M4_LATENCY_SPREAD_BOUND: f64 = 2.0;

/// The fair-latency statistic: one arm's worst flow's **p90** one-way latency
/// over its best flow's. One authority for the quantile [`M4_LATENCY_SPREAD_BOUND`]
/// bounds, so the fair-latency arms cannot drift apart.
///
/// A flow that delivered nothing has no p90 (`summarize` yields NaN); report 0
/// so a verdict line stays machine-parseable. The per-flow delivery floor is
/// what names such a run, and it fires before this statistic is reached.
fn fair_latency_spread(run: &FairRun) -> f64 {
    let floor = run
        .flows
        .iter()
        .map(|flow| flow.summary.p90)
        .fold(f64::INFINITY, f64::min);
    let ceiling = run
        .flows
        .iter()
        .map(|flow| flow.summary.p90)
        .fold(0.0, f64::max);
    if floor.is_finite() && floor > 0.0 {
        ceiling / floor
    } else {
        0.0
    }
}

/// The bound the four-flow **clean level** arm asserts: the production flow
/// count must meet the same interactive ceiling M1 asserts for one flow. One
/// authority for the value: `M1_CEILING_MS` / `rtp_mux/GATE.md` ("Performance").
///
/// Why the ceiling and not a looser measurement-derived guard: the four-flow
/// clean p99 measured 174.8-188.9 ms across the runs this bound is derived from
/// (0.70-0.76 of the ceiling), so the file's usual guard rule -- a multiple of
/// the worst measured, as [`M4_LATENCY_SPREAD_BOUND`]'s 1.67x and
/// [`M4_IMBALANCE_BOUND`]'s 2.2x are -- would want `2 x 188.9 = 377.8 ms`,
/// which is **above** the ceiling and therefore bounds nothing the product
/// promises. The ceiling is therefore the tightest level bound this arm's own
/// distribution supports, and it is not a round number picked here: it is the
/// mandate's own number, reused. Its sensitivity is a measured quantity too: it
/// fires on any regression of `250 / 188.9 = 1.32x` or more, so a lane whose
/// clean p99 doubles (2 x 178 = 356 ms) fails it with 106 ms to spare.
const M4_CLEAN_P99_CEILING_MS: f64 = M1_CEILING_MS;

/// The bound the four-flow **hostile level** arm asserts: the production flow
/// count must meet a level of its own on the GE `5 %`/mean-8 + 100 ms-jitter
/// link, where the one-flow mandate ceiling ([`M1_CEILING_MS`], 250 ms) is
/// **currently false**.
///
/// Why not [`M1_CEILING_MS`]: the clean-lane arm could reuse the mandate's own
/// number because the product promises it and the four-flow clean p99 meets it
/// (`0.70-0.76` of it); on the hostile link the same statistic measures
/// `274.9-423.0 ms`, i.e. `1.10-1.69x` the ceiling, so a 250 ms bound here would
/// be false on every run. The arm therefore asserts the tightest level bound
/// its own distribution supports, derived by the mechanism this file already
/// uses for the impaired tail (`rtp_mux/GATE.md`, "The deployed baseline the
/// impaired tail must not regress past"): `mean + 4` sample standard
/// deviations over the healthy reps on record. The reps are the eighteen
/// full-window reps of M4's own `hostile` arm measured on `rtp v0.0.98` (the
/// pin at the time) (`274.9`, `296.9`, `297.1`, `301.0`, `308.0`, `315.6`,
/// `315.7`, `316.5`, `323.5`, `324.1`, `331.1`, `334.4`, `334.6`, `341.8`,
/// `345.1`, `345.9`, `351.8`, `358.3`), the harness baseline's recorded
/// `hostile_p99_max=306.5`
/// (`crates/netem_test/tools/mandate-baseline.json`) and the `<= 423 ms`
/// worst per-flow p99 M4's own bounds table records: `n = 20`, mean `327.3`,
/// `sd 31.0`, limit `451.1`, rounded up to the next whole millisecond. The
/// file's guard rule for a regression bound -- a multiple of the worst
/// measured, `2 x 423 = 846 ms` -- is the `900 ms` guard `m4_interactive_lane_fairness`
/// already asserts per flow, so it would add no level of its own; this bound is
/// **1.99x tighter** than that guard and fires on a regression of `452 / 423 =
/// 1.07x` over the recorded worst (`1.26x` over the worst fresh rep). It
/// exceeds the M1 ceiling by `452 / 250 = 1.81x`, and the arm says so: the
/// four-flow hostile tail is a **known, named hole**, not a bound that hides
/// the product's 250 ms promise.
const M4_HOSTILE_P99_CEILING_MS: f64 = 452.0;

/// The per-flow tag byte, the same A/L/C/D convention the multi-flow scaling
/// probe uses (`b'B'` is the reserved bulk-sink tag, so it is skipped). The
/// server's tagged sink routes every interactive-lane stream through its
/// latency parser and labels each sample with the stream's tag, which is what
/// makes per-flow attribution possible.
fn m4_flow_tag(flow: usize) -> u8 {
    match flow {
        0 => b'A',
        1 => b'L',
        _ => b'A' + flow as u8,
    }
}

/// One M4 arm: the production interactive lane, `M4_FLOWS` flows offering the
/// same payload at the same cadence, and the impairment the clean/hostile arms
/// already use.
struct FairArmSpec {
    name: &'static str,
    int_c2s: NetemConfig,
    int_s2c: NetemConfig,
    window: Duration,
    /// The preferential-service fault injection: every flow but the first
    /// starts offering this long into the window (zero in every real run).
    stagger: Duration,
    /// The **fair-latency** fault injection: the flow at this index has every
    /// offered message held this long between its send timestamp and its write
    /// to the lane ([`HoldWriter`]), so the sink observes that flow's messages
    /// as served later than the others -- the sender-side simulation of a lane
    /// that orders one flow's traffic preferentially. `None` in every real run,
    /// and only ever set by [`m4_clean_lane_fair_latency`]'s own fault
    /// namespace, so no other arm's behaviour moves.
    hold: Option<(usize, Duration)>,
}

/// One flow's measured outcome: what it offered, what the lane delivered for
/// it, its share of the lane's delivered bytes, and its latency summary.
struct FlowSample {
    tag: u8,
    sent: u64,
    /// Messages the sink observed inside the arm's `window + GRACE` cutoff.
    on_time: u64,
    /// Messages observed after that cutoff but inside [`M4_LATE_HORIZON`]:
    /// delivered, late. Its own cell, with its own count, because the mandate's
    /// claim is that the flow is not *starved* -- and a late arrival is the
    /// repair ladder completing, not starvation.
    late: u64,
    /// Messages never observed: `sent - on_time - late`. This is the delivery
    /// loss [`M4_DELIVERY_FLOOR`] bounds, so the floor is decided on what the
    /// lane failed to deliver rather than on how fast it delivered it.
    lost: u64,
    /// `on_time + late`: the messages the lane delivered for this flow. The
    /// printed `delivery` is `received / sent`, the quotient of the two counts
    /// the arm prints beside it (`sent` and `recv`), so the report's unit budget
    /// for the floor is read from this arm's own numbers.
    received: u64,
    offered_bytes: u64,
    delivered_bytes: u64,
    share: f64,
    summary: HolSummary,
}

/// One arm's per-flow cells, summed: the run totals the arm line and the
/// `MANDATE M4` line report, and the shares' denominator.
fn arm_totals(run: &FairRun) -> (u64, u64, u64, u64) {
    run.flows.iter().fold(
        (0u64, 0u64, 0u64, 0u64),
        |(sent, on_time, late, lost), flow| {
            (
                sent + flow.sent,
                on_time + flow.on_time,
                late + flow.late,
                lost + flow.lost,
            )
        },
    )
}

/// One M4 arm's outcome, plus the aggregate statistic the fair-share bound is
/// asserted on.
struct FairRun {
    name: &'static str,
    flows: Vec<FlowSample>,
    ideal_share: f64,
    min_share: f64,
    max_share: f64,
    /// The worst flow's relative departure from the equal share:
    /// `max_i |share_i - 1/N| / (1/N)`. Zero means every flow received exactly
    /// its equal share of the lane's delivered bytes; it is the statistic the
    /// fair-share bound is derived from.
    imbalance: f64,
    window: Duration,
    wall: Duration,
    /// Every delivered sample as `(flow tag, elapsed s, one-way latency ms)`,
    /// on-time and late alike -- the series the percentiles are read from, kept
    /// so a band composition can be read off the same numbers the summary was.
    samples: Vec<(u8, f64, f64)>,
    /// Datagrams the arm's own forward shaper (client to server) dropped.
    /// The loss realization the latency tail is made of, read from the
    /// instrument rather than inferred from the latency.
    c2s_dropped: u64,
    /// Datagrams the forward shaper received and forwarded, so the drop count
    /// above has a denominator the arm measured rather than one it assumed.
    c2s_received: u64,
    c2s_forwarded: u64,
}

/// The fairness window. The share statistic's resolution is one delivered
/// frame: `1 / (M4_FLOWS x frames-per-flow)` of the lane, and the flows are
/// opened in sequence and each runs its own 5 ms interval, so their frame
/// counts differ by a few frames of connection ramp. At the arms' 12 s window
/// that structural skew measured under 0.5 %, but at a 4 s window it reached
/// the 1 % bound on a handful of frames alone, so M4's quick window is longer
/// than the other mandates' (still well under the full 12 s).
const M4_QUICK_WINDOW: Duration = Duration::from_secs(8);

fn fairness_window() -> Duration {
    if quick() { M4_QUICK_WINDOW } else { WINDOW }
}

/// The M4 arm set. `clean` is the mandate arm (M1/M2's clean interactive link)
/// and carries the fault injection when one is selected; `hostile` is the
/// regression-guard arm (M1/M2's GE `5 %`/mean-8 + 100 ms-jitter link).
fn fairness_arms(mandate: &str) -> Vec<FairArmSpec> {
    let clean_fault = fault(mandate);
    let mut clean_c2s = link(41, OWD, JITTER, LOSS_2, 0);
    let mut clean_s2c = link(42, OWD, JITTER, LOSS_2, 0);
    let mut stagger = Duration::ZERO;
    if let Some(fault) = clean_fault.as_deref() {
        match fault {
            // Serve one flow preferentially: flows 1.. offer only the second
            // half of the window, so the lane's delivered bytes concentrate on
            // flow 0 and the fair-share bound must fail while every flow still
            // delivers everything it offers.
            "M4_starve" => stagger = fairness_window() / 2,
            // Hold the last stretch of every flow's offer past the arm's
            // `window + GRACE` cutoff without losing a message: the c2s link is
            // delayed by just over the cutoff's drain, so the offers of the
            // window's last ~50 ms land in the *late* cell while the rest of
            // the offer still arrives on time. The per-flow delivery floor must
            // stay green (nothing is lost) and `late` must be non-zero on every
            // flow -- the late cell's own vacuity demonstration. Only c2s is
            // shifted: the sink is downstream of it, while the s2c return path
            // keeps the round trip short enough that the arm is not measuring a
            // stalled link. The shift is deliberately the smallest that crosses
            // the cutoff, because a repaired message's total is about twice the
            // one-way delay (the shaper's own delay plus an srtt-sized rung),
            // and a shift large enough for that sum to pass the horizon would
            // put real losses in the `lost` cell instead of isolating `late`.
            "M4_late" => {
                clean_c2s.latency = GRACE + Duration::from_millis(50);
            }
            // Collapse every flow's delivery: the per-flow delivery floor must
            // fail naming M4.
            "M4_drop" => {
                clean_c2s.loss = loss_pct(99);
                clean_s2c.loss = loss_pct(99);
            }
            _ => {}
        }
    }
    vec![
        FairArmSpec {
            name: "clean",
            int_c2s: clean_c2s,
            int_s2c: clean_s2c,
            window: fairness_window(),
            stagger,
            hold: None,
        },
        FairArmSpec {
            name: "hostile",
            int_c2s: hostile_link(41),
            int_s2c: hostile_link(42),
            window: fairness_window(),
            stagger: Duration::ZERO,
            hold: None,
        },
    ]
}

/// A write half that holds every write by a fixed delay before it reaches the
/// lane, so a message's **send timestamp** (taken by
/// [`send_timestamped_messages`] before its `write_all`) and its arrival are
/// separated by that delay, and the sink measures it as one-way latency. It is
/// the fair-latency fault's injector: the flagged flow's messages are observed
/// as served `held_for` later than the others, which is what a lane ordering
/// one flow's traffic last would look like at the sink. It changes no other
/// arm: every real run leaves
/// [`FairArmSpec::hold`] `None` and the wrapper is never constructed.
struct HoldWriter<'a, W> {
    inner: &'a mut W,
    held_for: Duration,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<'a, W> HoldWriter<'a, W> {
    fn new(inner: &'a mut W, held_for: Duration) -> Self {
        Self {
            inner,
            held_for,
            sleep: None,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for HoldWriter<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if let Some(sleep) = this.sleep.as_mut() {
            if sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.sleep = None;
        } else {
            this.sleep = Some(Box::pin(tokio::time::sleep(this.held_for)));
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Pin::new(&mut *this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.inner).poll_shutdown(cx)
    }
}

/// Run one fairness arm: `M4_FLOWS` interactive streams on ONE interactive
/// lane, all tagged, all offered the same `MSG_BYTES` payload at `CADENCE` for
/// `window`, all drained by one collector that buckets the tagged sink's
/// samples per flow.
///
/// Each flow's samples are split by *when* the sink observed them, not only by
/// whether it did: everything observed by the `window + GRACE` cutoff is
/// **on time**, everything observed inside the [`M4_LATE_HORIZON`] drain that
/// follows is **late**, and everything never observed is **lost**. The split is
/// what makes the delivery floor a starvation bound: a head-of-line block that
/// the repair ladder clears inside the horizon leaves the floor green and the
/// backfill in the `late` cell, where it is visible, instead of being counted
/// as a message the lane never delivered.
async fn run_fairness_arm(spec: FairArmSpec) -> FairRun {
    let FairArmSpec {
        name,
        int_c2s,
        int_s2c,
        window,
        stagger,
        hold,
    } = spec;
    let wall = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let outcome = tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, _bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair = NetemPair::spawn(int_addr, int_c2s, int_s2c).unwrap();
            // The bulk lane is connected (the production topology pairs both
            // lanes at connect) but never opened: M4 measures the interactive
            // lane's own split.
            let bulk_pair =
                NetemPair::spawn(bulk_addr, NetemConfig::default(), NetemConfig::default())
                    .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                None,
                None,
            )
            .await
            .unwrap();

            // One collector drains the shared tagged channel for the whole arm,
            // keeping `(tag, elapsed, latency)` so each sample is attributable
            // to its flow: the sink's channel is bounded and a lane carrying N
            // flows produces N times the sample rate.
            let collector = Arc::new(Mutex::new(Vec::<(u8, f64, f64)>::new()));
            let collector_sink = Arc::clone(&collector);
            let task_tx_collector = task_tx.clone();
            submit_test_task(
                &task_tx_collector,
                Box::pin(async move {
                    while let Some((tag, latency)) = latencies.recv().await {
                        collector_sink.lock().unwrap().push((
                            tag,
                            base.elapsed().as_secs_f64(),
                            latency,
                        ));
                    }
                }),
            );

            let mut streams = Vec::with_capacity(M4_FLOWS);
            for flow in 0..M4_FLOWS {
                let (mut read, write) = opener.open(LaneClass::Interactive).await.unwrap();
                // Parked until the streams close; the owning scope aborts them.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                streams.push((m4_flow_tag(flow), write));
            }

            // The fair-latency fault's own flow, if any: that one flow's write
            // half is wrapped so every offered message waits `held_for` between
            // its send timestamp and its write ([`HoldWriter`]).
            // Tag first, then offer every flow *concurrently*: the arm is N
            // flows multiplexed on one lane, not N sequential sweeps. In every
            // real run `delay` is zero and every flow offers for the whole
            // window; the preferential-service fault gives flow 0 the whole
            // window while the rest offer only its second half, so the lane's
            // delivered bytes concentrate on flow 0.
            let mut futs = Vec::with_capacity(M4_FLOWS);
            for (index, (tag, write)) in streams.iter_mut().enumerate() {
                if write.write_all(&[*tag]).await.is_err() {
                    return (vec![0u64; M4_FLOWS], Vec::new(), Vec::new(), (0, 0, 0));
                }
                let delay = if index == 0 { Duration::ZERO } else { stagger };
                let run_for = window.saturating_sub(delay);
                let write = &mut *write;
                let held_for = hold
                    .filter(|(held_index, _)| *held_index == index)
                    .map(|(_, d)| d);
                futs.push(async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    match held_for {
                        Some(held_for) => {
                            let mut held = HoldWriter::new(write, held_for);
                            send_timestamped_messages(&mut held, base, MSG_BYTES, CADENCE, run_for)
                                .await
                        }
                        None => {
                            send_timestamped_messages(write, base, MSG_BYTES, CADENCE, run_for)
                                .await
                        }
                    }
                });
            }
            let sent_per_flow: Vec<u64> = join_all(futs).await;
            let offered_total: u64 = sent_per_flow.iter().sum();
            for (_, write) in streams.iter_mut() {
                let _ = write.shutdown();
            }

            // The cutoff: the `window + GRACE` drain the summary used to be read
            // after. A sample observed by now is *on time*.
            tokio::time::sleep(GRACE).await;
            let on_time = std::mem::take(&mut *collector.lock().unwrap());

            // Past the cutoff the arm keeps observing, because the repair
            // ladder that carries a head-of-line-blocked message outlives
            // `GRACE`. Everything observed in here is *late*: the lane did
            // deliver it. The drain ends the moment the whole offer has been
            // observed, so a healthy run pays a poll interval and not the
            // horizon.
            let horizon = Instant::now() + M4_LATE_HORIZON;
            let mut late = Vec::new();
            loop {
                late.extend(std::mem::take(&mut *collector.lock().unwrap()));
                if on_time.len() as u64 + late.len() as u64 >= offered_total
                    || Instant::now() >= horizon
                {
                    break;
                }
                tokio::time::sleep(M4_LATE_POLL).await;
            }
            // One last drain, so a sample that raced the loop's own check is
            // classified rather than read as lost.
            late.extend(std::mem::take(&mut *collector.lock().unwrap()));
            let c2s = int_pair.snapshot_c2s().stats;
            let c2s_dropped = c2s.dropped;
            let (c2s_received, c2s_forwarded) = (c2s.received, c2s.forwarded);
            int_pair.stop();
            bulk_pair.stop();
            (
                sent_per_flow,
                on_time,
                late,
                (c2s_dropped, c2s_received, c2s_forwarded),
            )
        })
        .await;
    let (sent_per_flow, on_time, late, (c2s_dropped, c2s_received, c2s_forwarded)) = outcome;

    let bucket = |samples: &[(u8, f64, f64)]| {
        let mut per_flow: Vec<Vec<f64>> = vec![Vec::new(); M4_FLOWS];
        for (tag, _elapsed, latency) in samples.iter().copied() {
            if let Some(flow) = (0..M4_FLOWS).find(|&i| m4_flow_tag(i) == tag) {
                per_flow[flow].push(latency);
            }
        }
        per_flow
    };
    let mut samples: Vec<(u8, f64, f64)> = on_time.clone();
    samples.extend_from_slice(&late);
    let on_time_per_flow = bucket(&on_time);
    let late_per_flow = bucket(&late);
    let delivered: Vec<u64> = (0..M4_FLOWS)
        .map(|flow| {
            (on_time_per_flow[flow].len() + late_per_flow[flow].len()) as u64 * MSG_BYTES as u64
        })
        .collect();
    let total_delivered: u64 = delivered.iter().sum();
    let ideal_share = 1.0 / M4_FLOWS as f64;
    let mut flows = Vec::with_capacity(M4_FLOWS);
    for flow in 0..M4_FLOWS {
        let sent = sent_per_flow[flow];
        let on_time = on_time_per_flow[flow].len() as u64;
        let late = late_per_flow[flow].len() as u64;
        let received = on_time + late;
        let lost = sent.saturating_sub(received);
        let offered_bytes = sent.saturating_mul(MSG_BYTES as u64);
        let share = if total_delivered == 0 {
            0.0
        } else {
            delivered[flow] as f64 / total_delivered as f64
        };
        // The series the percentiles are read from is every delivered sample --
        // on time and late alike -- so a late arrival raises the tail it is: the
        // reading is the latency the lane took, not only the latency it took
        // within the cutoff.
        let mut samples = on_time_per_flow[flow].clone();
        samples.extend_from_slice(&late_per_flow[flow]);
        let summary = summarize(samples, sent, received, 0, 0.0);
        flows.push(FlowSample {
            tag: m4_flow_tag(flow),
            sent,
            on_time,
            late,
            lost,
            received,
            offered_bytes,
            delivered_bytes: delivered[flow],
            share,
            summary,
        });
    }
    let min_share = flows.iter().map(|f| f.share).fold(f64::INFINITY, f64::min);
    let max_share = flows.iter().map(|f| f.share).fold(0.0, f64::max);
    let imbalance = flows
        .iter()
        .map(|f| ((f.share - ideal_share) / ideal_share).abs())
        .fold(0.0, f64::max);
    FairRun {
        name,
        flows,
        ideal_share,
        min_share,
        max_share,
        imbalance,
        window,
        samples,
        c2s_dropped,
        c2s_received,
        c2s_forwarded,
        wall: wall.elapsed(),
    }
}

fn print_fair_arm(run: &FairRun) {
    for flow in &run.flows {
        eprintln!(
            "[mandate-smoke m4/{name} flow {tag}] sent={sent:>5} recv={recv:>5} \
             on_time={on_time:>5} late={late:>5} lost={lost:>5} delivery={del:.3} \
             share={share:.4} offered={offered:>8}B delivered={delivered:>8}B \
             p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} max={max:8.1}",
            name = run.name,
            tag = flow.tag as char,
            sent = flow.sent,
            recv = flow.received,
            on_time = flow.on_time,
            late = flow.late,
            lost = flow.lost,
            del = flow.summary.delivery_pct,
            share = flow.share,
            offered = flow.offered_bytes,
            delivered = flow.delivered_bytes,
            p50 = flow.summary.p50,
            p90 = flow.summary.p90,
            p99 = flow.summary.p99,
            max = flow.summary.max,
        );
    }
    let (sent, on_time, late, lost) = arm_totals(run);
    eprintln!(
        "[mandate-smoke m4/{name}] ideal_share={ideal:.4} min_share={min:.4} max_share={max:+.4} \
         imbalance={imbalance:.4} sent={sent} on_time={on_time} late={late} lost={lost} \
         window={window:?} wall={wall:.1}s",
        name = run.name,
        ideal = run.ideal_share,
        min = run.min_share,
        max = run.max_share,
        imbalance = run.imbalance,
        window = run.window,
        wall = run.wall.as_secs_f64(),
    );
}

// ────────────────────────── M4: evidence writing ─────────────────────────────

fn m4_declaration(runs: &[FairRun]) -> String {
    let ideal = 1.0 / M4_FLOWS as f64;
    let ideal_pct = ideal * 100.0;
    let bound_pct = M4_IMBALANCE_BOUND * 100.0;
    let bound = M4_IMBALANCE_BOUND;
    let flows = M4_FLOWS;
    let delivery_floor = M4_DELIVERY_FLOOR;
    let ceiling = M1_CEILING_MS;
    // The floor's own unit budget at this run's smallest offer: the count of
    // never-delivered messages the floor tolerates, and the count that first
    // breaches it. The panel states the floor in the units it is made of, so a
    // breach is read as an event size rather than as the ratio's third decimal
    // -- the same arithmetic `tools/mandate-check` prints on its `delivery:`
    // line, from the same two quantities (`sent` and the floor).
    let offered_min = runs
        .iter()
        .flat_map(|run| run.flows.iter())
        .map(|flow| flow.sent)
        .min()
        .unwrap_or(0);
    let lost_budget = ((offered_min as f64) * (1.0 - M4_DELIVERY_FLOOR)).floor() as u64;
    let first_breach = lost_budget + 1;
    // The line is the **first failing count**, not the tolerance: a count of
    // `first_breach` lost units is a breach, and drawing the tolerance instead
    // would put the failing value one unit above the panel's own top (the axis
    // carries each bound and `FRAME_HEADROOM` of the span, so a series at
    // `bound + 1` is off the frame). The label states both numbers.
    // The fair-share line is drawn on the share panel; the floor is the same
    // line pulled in by the imbalance bound, so drawing both there overprints
    // two labels one percent apart. The floor is drawn instead on the imbalance
    // panel, whose axis is the deviation itself, where the two bounds and every
    // flow's departure are legible.
    //
    // `lost` and `late` are the floor's two cells split in the evidence: `lost`
    // is the count the floor bounds and is drawn against that count's own unit
    // budget, and `late` is the backfill the arm would otherwise have counted as
    // lost -- reported, with no line, because a late arrival is bounded by the
    // latency panels and not by the delivery floor.
    format!(
        r#"{{"mandate":"M4","title":"M4 interactive lane fairness: {flows} flows on one interactive lane","x_label":"flow (1..{flows})","y_label":"share of the lane's delivered bytes","panels":[{{"id":"shares","chart":"bar","series":[{{"name":"clean"}},{{"name":"hostile"}}],"bounds":[{{"y":{ideal:.6},"label":"fair share {ideal_pct:.1}%"}}]}},{{"id":"imbalance","chart":"bar","y_label":"departure from the fair share","x_label":"flow (1..{flows})","series":[{{"name":"clean"}},{{"name":"hostile"}}],"bounds":[{{"y":{bound},"label":"fair-share bound \u00b1{bound_pct:.1}%"}}]}},{{"id":"delivery","chart":"bar","y_label":"delivery (received / offered)","series":[{{"name":"clean"}},{{"name":"hostile"}}],"bounds":[{{"y":{delivery_floor},"label":"M4 per-flow delivery floor {delivery_floor}"}}]}},{{"id":"lost","chart":"line","y_label":"never observed (messages)","x_label":"flow (1..{flows})","series":[{{"name":"lost_clean"}},{{"name":"lost_hostile"}}],"bounds":[{{"y":{first_breach},"label":"{first_breach} lost breaches the floor ({lost_budget} tolerated)"}}]}},{{"id":"late","chart":"line","y_label":"observed late (messages)","x_label":"flow (1..{flows})","series":[{{"name":"late_clean"}},{{"name":"late_hostile"}}],"bounds":[]}},{{"id":"latency","chart":"bar","y_label":"latency (ms)","series":[{{"name":"clean_p50"}},{{"name":"clean_p99"}},{{"name":"hostile_p50"}},{{"name":"hostile_p99"}}],"bounds":[{{"y":{ceiling},"label":"M1 ceiling {ceiling} ms"}}]}}]}}"#
    )
}

fn m4_rows(runs: &[FairRun]) -> Vec<(String, String, f64, f64)> {
    let mut rows = Vec::new();
    for run in runs {
        for (index, flow) in run.flows.iter().enumerate() {
            let x = (index + 1) as f64;
            rows.push(("shares".to_owned(), run.name.to_owned(), x, flow.share));
            rows.push((
                "imbalance".to_owned(),
                run.name.to_owned(),
                x,
                (flow.share - run.ideal_share) / run.ideal_share,
            ));
            rows.push((
                "delivery".to_owned(),
                run.name.to_owned(),
                x,
                flow.summary.delivery_pct,
            ));
            rows.push((
                "lost".to_owned(),
                format!("lost_{}", run.name),
                x,
                flow.lost as f64,
            ));
            rows.push((
                "late".to_owned(),
                format!("late_{}", run.name),
                x,
                flow.late as f64,
            ));
            rows.push((
                "latency".to_owned(),
                format!("{}_p50", run.name),
                x,
                flow.summary.p50,
            ));
            rows.push((
                "latency".to_owned(),
                format!("{}_p99", run.name),
                x,
                flow.summary.p99,
            ));
        }
    }
    rows
}

/// Mandate 4: the interactive lane's split across several flows. Every flow
/// must deliver what it is offered (no starvation), no flow's share of the
/// lane's delivered bytes may depart from the equal share by more than
/// [`M4_IMBALANCE_BOUND`] (fair share), the clean arm's worst flow's p90 may
/// not exceed the best flow's p90 by more than [`M4_LATENCY_SPREAD_BOUND`] (`fair_latency_spread`)
/// (fair latency), and no hostile-arm flow's p99 may cross M1's hostile guard. The absolute
/// interactive ceiling is **not** re-asserted per flow here: the 4-flow arm
/// measures p99 179-231 ms, 0.72-0.92 of M1's 250 ms ceiling, so an absolute
/// per-flow assertion would sit within 1.1x of the arm's own measurement and
/// fire on host noise. M1 owns the ceiling, the M4 latency panel draws it, and
/// the `MANDATE M4` line reports `clean_p99_max` -- which is what makes a
/// multi-flow latency regression visible.
///
/// The delivery cells are the split of "delivered when": `delivery` is
/// `received / sent` where `received = on_time + late` is everything the lane
/// delivered inside the arm's whole observation horizon, so the floor bounds a
/// **loss** (`lost = sent - received`) rather than a repair that outran a
/// cutoff; `late` and `lost` are reported per flow and per arm on the lines, in
/// `M4.csv` and on their own panels. Late arrivals are not unasserted: they are
/// in the percentile series every M4 latency bound reads.
#[tokio::test(flavor = "multi_thread")]
async fn m4_interactive_lane_fairness() {
    let _serial = SERIAL.lock().await;
    let dir = out_dir();
    let arms = fairness_arms("M4");
    let mut runs = Vec::new();
    for spec in arms {
        let label = format!("m4/{}", spec.name);
        let run = with_timeout(ARM_DEADLINE, &label, run_fairness_arm(spec)).await;
        print_fair_arm(&run);
        runs.push(run);
    }
    write_evidence(&dir, "M4", &m4_declaration(&runs), &m4_rows(&runs));

    let clean = &runs[0];
    let hostile = &runs[1];
    let delivery_floor_of = |run: &FairRun| {
        run.flows
            .iter()
            .map(|f| f.summary.delivery_pct)
            .fold(f64::INFINITY, f64::min)
    };
    let p90_floor_of = |run: &FairRun| {
        run.flows
            .iter()
            .map(|f| f.summary.p90)
            .fold(f64::INFINITY, f64::min)
    };
    let p99_ceiling_of =
        |run: &FairRun| run.flows.iter().map(|f| f.summary.p99).fold(0.0, f64::max);
    let p50_ceiling_of =
        |run: &FairRun| run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max);
    let clean_floor = delivery_floor_of(clean);
    let hostile_floor = delivery_floor_of(hostile);
    // The fair-latency statistic is [`fair_latency_spread`] -- one authority
    // for the quantile, shared with the two four-flow level arms and with
    // [`m4_clean_lane_fair_latency`]. Both arms' spreads are reported; only the
    // clean arm's is asserted (the hostile per-flow p90 differences are a GE
    // loss realization, not a scheduler property).
    let clean_spread = fair_latency_spread(clean);
    let hostile_spread = fair_latency_spread(hostile);
    let clean_p99_max = p99_ceiling_of(clean);
    let clean_p50_max = p50_ceiling_of(clean);
    let hostile_p99_max = p99_ceiling_of(hostile);
    let wall = clean.wall.as_secs_f64() + hostile.wall.as_secs_f64();
    let (clean_sent, clean_on_time, clean_late, clean_lost) = arm_totals(clean);
    let (hostile_sent, hostile_on_time, hostile_late, hostile_lost) = arm_totals(hostile);
    let pass = clean_floor >= M4_DELIVERY_FLOOR
        && hostile_floor >= M4_DELIVERY_FLOOR
        && clean.imbalance <= M4_IMBALANCE_BOUND
        && hostile.imbalance <= M4_IMBALANCE_BOUND
        && clean_spread <= M4_LATENCY_SPREAD_BOUND
        && hostile_p99_max <= M1_HOSTILE_P99_GUARD_MS;
    println!(
        "MANDATE M4 {} flows={} clean_delivery_min={:.3} hostile_delivery_min={:.3} clean_share_min={:.4} clean_share_max={:.4} hostile_share_min={:.4} hostile_share_max={:.4} clean_imbalance={:.4} hostile_imbalance={:.4} imbalance_bound={:.3} fair_share={:.4} delivery_floor={:.3} clean_sent={} clean_on_time={} clean_late={} clean_lost={} hostile_sent={} hostile_on_time={} hostile_late={} hostile_lost={} late_horizon_s={:.1} clean_p90_spread={:.3} hostile_p90_spread={:.3} spread_bound={:.1} clean_p50_max={:.1} clean_p99_max={:.1} hostile_p99_max={:.1} ceiling={:.1} hostile_p99_guard={:.1} window_s={:.1} wall_s={:.1}",
        verdict(pass),
        M4_FLOWS,
        clean_floor,
        hostile_floor,
        clean.min_share,
        clean.max_share,
        hostile.min_share,
        hostile.max_share,
        clean.imbalance,
        hostile.imbalance,
        M4_IMBALANCE_BOUND,
        clean.ideal_share,
        M4_DELIVERY_FLOOR,
        clean_sent,
        clean_on_time,
        clean_late,
        clean_lost,
        hostile_sent,
        hostile_on_time,
        hostile_late,
        hostile_lost,
        M4_LATE_HORIZON.as_secs_f64(),
        clean_spread,
        hostile_spread,
        M4_LATENCY_SPREAD_BOUND,
        clean_p50_max,
        clean_p99_max,
        hostile_p99_max,
        M1_CEILING_MS,
        M1_HOSTILE_P99_GUARD_MS,
        clean.window.as_secs_f64(),
        wall,
    );

    for (index, flow) in clean.flows.iter().enumerate() {
        assert!(
            flow.summary.delivery_pct >= M4_DELIVERY_FLOOR,
            "[M4] clean-arm flow {} (tag {}) was delivered {}/{} messages ({:.3} < the {M4_DELIVERY_FLOOR} floor) and {} were never observed inside the {:.1}s horizon (sent {} = on_time {} + late {} + lost {}): a flow sharing the interactive lane was starved of what it offered",
            index + 1,
            flow.tag as char,
            flow.received,
            flow.sent,
            flow.summary.delivery_pct,
            flow.lost,
            M4_LATE_HORIZON.as_secs_f64(),
            flow.sent,
            flow.on_time,
            flow.late,
            flow.lost,
        );
    }
    for (index, flow) in hostile.flows.iter().enumerate() {
        assert!(
            flow.summary.delivery_pct >= M4_DELIVERY_FLOOR,
            "[M4] hostile-arm flow {} (tag {}) was delivered {}/{} messages ({:.3} < the {M4_DELIVERY_FLOOR} floor) and {} were never observed inside the {:.1}s horizon (sent {} = on_time {} + late {} + lost {}): a flow sharing the interactive lane was starved of what it offered under the hostile impairment",
            index + 1,
            flow.tag as char,
            flow.received,
            flow.sent,
            flow.summary.delivery_pct,
            flow.lost,
            M4_LATE_HORIZON.as_secs_f64(),
            flow.sent,
            flow.on_time,
            flow.late,
            flow.lost,
        );
    }
    assert!(
        clean.imbalance <= M4_IMBALANCE_BOUND,
        "[M4] clean-arm fair-share breach: the worst flow's share of the lane's delivered bytes departs {:.4} from the equal share {:.4} (shares {:?}), over the {M4_IMBALANCE_BOUND} bound -- one flow is being served preferentially on the shared interactive lane",
        clean.imbalance,
        clean.ideal_share,
        clean.flows.iter().map(|f| f.share).collect::<Vec<_>>(),
    );
    assert!(
        hostile.imbalance <= M4_IMBALANCE_BOUND,
        "[M4] hostile-arm fair-share breach: the worst flow's share of the lane's delivered bytes departs {:.4} from the equal share {:.4} (shares {:?}), over the {M4_IMBALANCE_BOUND} bound -- one flow is being served preferentially on the shared interactive lane under the hostile impairment",
        hostile.imbalance,
        hostile.ideal_share,
        hostile.flows.iter().map(|f| f.share).collect::<Vec<_>>(),
    );
    for (index, flow) in clean.flows.iter().enumerate() {
        assert!(
            flow.summary.p90 <= p90_floor_of(clean) * M4_LATENCY_SPREAD_BOUND,
            "[M4] clean-arm flow {} (tag {}) p90 {:.1} ms is more than {M4_LATENCY_SPREAD_BOUND}x the best flow's p90 {:.1} ms (p50 {:.1}, max {:.1}), over the fair-latency bound -- one flow's latency body is being served preferentially",
            index + 1,
            flow.tag as char,
            flow.summary.p90,
            p90_floor_of(clean),
            flow.summary.p50,
            flow.summary.max,
        );
    }
    for (index, flow) in hostile.flows.iter().enumerate() {
        assert!(
            flow.summary.p99 <= M1_HOSTILE_P99_GUARD_MS,
            "[M4] hostile-arm flow {} (tag {}) p99 {:.1} ms exceeds M1's {M1_HOSTILE_P99_GUARD_MS} ms hostile guard (p50 {:.1}, max {:.1}): the known hostile tail defect has at least doubled with several flows on the lane",
            index + 1,
            flow.tag as char,
            flow.summary.p99,
            flow.summary.p50,
            flow.summary.max,
        );
    }
}

// ─────────── M4 level: the production flow count's own tail, bounded ─────────

/// The arm the four-flow clean **level** assertion reads: M4's own `clean` arm
/// — the same `link(41/42, OWD, JITTER, LOSS_2, 0)` interactive link, the same
/// `M4_FLOWS` tagged flows offering the same payload at the same `CADENCE` over
/// the same window, and the same connected-but-unladen bulk lane — taken from
/// [`fairness_arms`] rather than restated, so the two cannot drift apart.
///
/// It carries its own fault namespace (`M4_CLEAN_LEVEL_*`) rather than M4's,
/// because the gap this arm closes is that M4's own arm *reports* the level
/// without asserting it: a probe of this assertion must not read as a probe of
/// M4's.
fn m4_clean_level_arm() -> FairArmSpec {
    let mut spec = fairness_arms("M4")
        .into_iter()
        .next()
        .expect("the M4 arm set always carries its clean arm first");
    if let Some(fault) = fault("M4_CLEAN_LEVEL") {
        // The level bound's own vacuity probes, as a further one-way delay on
        // the arm's clean link. `double` is sized so the aggregated p99 lands
        // past 2x the measured band -- the magnitude the brief's non-vacuity
        // requirement names -- and `slow` is the deeper probe, which is also
        // **composite**: at +200 ms it lengthens the round trip enough that it
        // slows the arm's own rate ramp as well as the one-way hop, so its
        // reading is a floor on what a doubling costs, not a measurement of
        // one.
        let extra_ms = match fault.as_str() {
            "M4_CLEAN_LEVEL_double" => Some(100),
            "M4_CLEAN_LEVEL_slow" => Some(200),
            _ => None,
        };
        if let Some(extra_ms) = extra_ms {
            spec.int_c2s.latency += Duration::from_millis(extra_ms);
            spec.int_s2c.latency += Duration::from_millis(extra_ms);
        }
    }
    spec
}

/// Mandate 4's **level** arm: the four-flow clean lane against M1's interactive
/// ceiling.
///
/// M1 asserts the 250 ms ceiling on its **one-flow** clean arm, and M4 reports
/// the four-flow clean p99 without asserting it, so the production flow count's
/// own tail was measured but bounded by no arm — one bad day from the ceiling
/// that no gate would name (the `m1-four-flow-clean@flows=4+impairment=clean+
/// metric=p99-ceiling` gap in `GATE.md`). This arm closes that gap as a **new**
/// arm beside M4 rather than by retuning it: it takes M4's clean arm unchanged
/// and asserts [`M4_CLEAN_P99_CEILING_MS`] on the aggregated `clean_p99_max`.
///
/// Two further assertions keep a level pass meaningful, and the level assertion
/// is what they are read *with* rather than instead of: the per-flow delivery
/// floor (a lane that meets its p99 by starving a flow has not met it) and the
/// clean-arm p99 spread (the level must be met by every flow, not by three fast
/// ones and one slow one), plus the instrument sanity without which a
/// degenerate percentile — no samples, a NaN, a p99 below the link's own
/// one-way floor — would read as a pass.
///
/// Vacuity: `MANDATE_SMOKE_FAULT=M4_CLEAN_LEVEL_slow` perturbs the arm's own
/// input and fails this bound by name.
#[tokio::test(flavor = "multi_thread")]
async fn m4_clean_lane_p99_ceiling() {
    let _serial = SERIAL.lock().await;
    let run = with_timeout(
        ARM_DEADLINE,
        "m4/clean_level",
        run_fairness_arm(m4_clean_level_arm()),
    )
    .await;
    let p99_max = run.flows.iter().map(|f| f.summary.p99).fold(0.0, f64::max);
    let p99_min = run
        .flows
        .iter()
        .map(|f| f.summary.p99)
        .fold(f64::INFINITY, f64::min);
    let p50_max = run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max);
    let delivery_min = run
        .flows
        .iter()
        .map(|f| f.summary.delivery_pct)
        .fold(f64::INFINITY, f64::min);
    let samples: u64 = run.flows.iter().map(|f| f.summary.received).sum();
    // The fair-latency statistic is [`fair_latency_spread`] -- the same
    // authority the M4 arm and [`m4_clean_lane_fair_latency`] read. It is the
    // p90 ratio, not the p99 ratio: see [`M4_LATENCY_SPREAD_BOUND`] for the
    // measurement that makes the p99 ratio a count draw on this lane.
    let spread = fair_latency_spread(&run);
    let pass = samples > 0
        && delivery_min >= M4_DELIVERY_FLOOR
        && spread <= M4_LATENCY_SPREAD_BOUND
        && p99_max.is_finite()
        && p99_max > 0.0
        && p99_max <= M4_CLEAN_P99_CEILING_MS;
    // Deliberately not a `MANDATE` line and deliberately not an
    // `[mandate-smoke …]` arm row: `tools/mandate-check` owns the M1-M4 id set
    // and attributes every arm row to the mandate whose `MANDATE` line follows
    // it, so a row or an id printed here would be read as M4's own measurement
    // (or refused as a second declaration of one). This arm's verdict is its
    // own line and its exit status.
    println!(
        "[m4-clean-level] {} flows={} p99_max={:.1} p99_min={:.1} p50_max={:.1} spread={:.3} \
         delivery_min={:.3} samples={} ceiling={:.1} spread_bound={:.1} delivery_floor={:.3} \
         level_ratio={:.3} fire_ratio={:.3} window_s={:.1} wall_s={:.1}",
        if pass { "PASS" } else { "FAIL" },
        M4_FLOWS,
        p99_max,
        p99_min,
        p50_max,
        spread,
        delivery_min,
        samples,
        M4_CLEAN_P99_CEILING_MS,
        M4_LATENCY_SPREAD_BOUND,
        M4_DELIVERY_FLOOR,
        p99_max / M4_CLEAN_P99_CEILING_MS,
        M4_CLEAN_P99_CEILING_MS / p99_max,
        run.window.as_secs_f64(),
        run.wall.as_secs_f64(),
    );
    assert!(
        samples > 0 && p99_max.is_finite() && p99_max > 0.0,
        "[M4 level] the arm measured {samples} delivered message(s) and an aggregate p99 of {p99_max} ms: a lane with no samples, no percentile or a zero percentile is an instrument failure, not a level that passes",
    );
    assert!(
        delivery_min >= M4_DELIVERY_FLOOR,
        "[M4 level] clean-arm flow delivery {delivery_min:.3} is under the {M4_DELIVERY_FLOOR} floor (the arm's own per-flow counts: sent={:?} received={:?}): a level met by starving a flow is not met",
        run.flows.iter().map(|f| f.sent).collect::<Vec<_>>(),
        run.flows.iter().map(|f| f.received).collect::<Vec<_>>(),
    );
    assert!(
        spread <= M4_LATENCY_SPREAD_BOUND,
        "[M4 level] clean-arm fair-latency spread {spread:.3} exceeds the {M4_LATENCY_SPREAD_BOUND} bound (per-flow p90 {:?}): the aggregate level must be met by every flow on the lane, not by three fast flows carrying one slow one",
        run.flows.iter().map(|f| f.summary.p90).collect::<Vec<_>>(),
    );
    assert!(
        p99_max <= M4_CLEAN_P99_CEILING_MS,
        "[M4 level] the four-flow clean lane's p99 {p99_max:.1} ms exceeds the {M4_CLEAN_P99_CEILING_MS} ms interactive ceiling (p50 {p50_max:.1}, per-flow p99 {:?}): the production shape runs {M4_FLOWS} flows on the one interactive lane, and it must meet the same ceiling M1 asserts for one -- a multi-flow tail breach is a product regression even though M1's one-flow arm cannot see it",
        run.flows.iter().map(|f| f.summary.p99).collect::<Vec<_>>(),
    );
}

// ────────── M4 clean band: what the clean lane's tail is made of ──────────

/// The largest one-way latency the clean link can produce with **no loss and
/// no repair**: `OWD + JITTER`. One authority for the number, read by the band
/// probe below. A message above it required either a dropped datagram (the
/// lane repaired one) or a mechanism the link's own profile cannot produce.
const CLEAN_ONE_WAY_CEILING_MS: f64 = 30.0;

/// The band probe's bins, in ms. The first two are at or below the link's own
/// no-loss ceiling; the rest are the repair band the four-flow tail is made of,
/// split finely enough to tell a *clump* (isolated rung repairs at one offset)
/// from a *spread* (a queue draining, i.e. a stall).
const BAND_EDGES: [(f64, f64); 12] = [
    (0.0, 25.0),
    (25.0, 30.0),
    (30.0, 40.0),
    (40.0, 50.0),
    (50.0, 60.0),
    (60.0, 70.0),
    (70.0, 80.0),
    (80.0, 90.0),
    (90.0, 100.0),
    (100.0, 150.0),
    (150.0, 250.0),
    (250.0, f64::INFINITY),
];

/// The arm the band probe reads: M4's own clean arm, with the link's loss
/// optionally closed (`noloss`, the attribution diagnostic's own arm) and
/// carrying its own fault namespace (`M4_BAND_*`) so a probe of this instrument
/// cannot read as a probe of M4's or of the level arm's.
///
/// Its one probe, `M4_BAND_delay`, adds one-way delay to the forward shaper and
/// closes the link's loss, so every offered message lands above the no-loss
/// ceiling while the shaper drops nothing: it is the **vacuity demonstration**
/// for the body assertion below, which must reject a lane whose body is not the
/// clean link it declares, and it fails naming the observed p50.
fn m4_band_arm(noloss: bool) -> FairArmSpec {
    let mut spec = fairness_arms("M4")
        .into_iter()
        .next()
        .expect("the M4 arm set always carries its clean arm first");
    // A new arm's window is its own, and the family's own cheapest one is the
    // one this reading needs: the composition is a *rate*, so 8 s of the 4-flow
    // cadence (about 6300 delivered samples) resolves it as well as 12 s does,
    // and the shortened window is what lets both this arm and its loss-closed
    // diagnostic be declared inside the `perf` tier's remaining headroom
    // (`rtp_mux/GATE.md`, gate-budgets).
    spec.window = M4_QUICK_WINDOW;
    if noloss {
        spec.int_c2s.loss = 0;
        spec.int_s2c.loss = 0;
    }
    if let Some(fault) = fault("M4_BAND") {
        let extra_ms = match fault.as_str() {
            "M4_BAND_delay" => Some(40),
            _ => None,
        };
        if let Some(extra_ms) = extra_ms {
            spec.int_c2s.latency += Duration::from_millis(extra_ms);
            spec.int_c2s.loss = 0;
            spec.int_s2c.loss = 0;
        }
    }
    spec
}

/// The four-flow clean lane's tail, attributed to the loss realization it is
/// made of, and binned so its *shape* is readable.
///
/// The level arm asserts the clean p99 against M1's ceiling, and M4 asserts the
/// split; neither says **what the tail above the link's own no-loss ceiling is
/// made of** — a lane whose tail is a clump at one rung's offset and a lane
/// whose tail is a queue draining are both "inside the ceiling" and are not the
/// same product. The reading is report-only (no mandated bound: the
/// instrument's own sanity is what it asserts), and it is the cause attribution
/// the level and fairness arms cannot give. Its assertions are instrument
/// integrity -- the body sits in the clean link's own `OWD +- JITTER` band, and
/// the bins cover the series -- with `M4_BAND_delay` as the body assertion's
/// vacuity demonstration.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "report-only clean-lane band composition probe; ~16 s; run with --ignored --nocapture"]
async fn probe_m4_clean_band_composition() {
    let _serial = SERIAL.lock().await;
    band_composition(m4_band_arm(false)).await;
}

/// The same arm with the link's **loss closed** and nothing else changed: the
/// band instrument's own attribution diagnostic, as a declared arm of the same
/// family rather than a fault flag on the arm above.
///
/// It is what showed the tail is not all loss: with the shaper's drop count at
/// zero the lane still produced a non-empty tail in every run measured (16 of
/// 9600 at the second run, 41 of 9568 at the first), so "a message above the
/// link's own no-loss ceiling was paid for by a dropped datagram" is **false on
/// this instrument** and is not asserted — the reading is the two counts side
/// by side. What the pair does give is the split: the loss-on arm reaches ~90 ms
/// and the loss-off arm stops at ~39 ms, so the part of the band the loss
/// realization cannot explain is the part below ~39 ms.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "report-only clean-lane band composition probe (loss closed); ~16 s; run with --ignored --nocapture"]
async fn probe_m4_clean_band_composition_noloss() {
    let _serial = SERIAL.lock().await;
    band_composition(m4_band_arm(true)).await;
}

/// One band-composition measurement: run the arm, bin every delivered sample's
/// one-way latency, and print the composition, the tail's own values, its
/// per-flow split and its time clustering, beside the forward shaper's own
/// received/forwarded/dropped counts.
///
/// The assertions are instrument integrity, and each is falsifiable: the arm
/// delivered something; the bins hold exactly the series; and the body sits in
/// the clean link's own `OWD +- JITTER` band, which `M4_BAND_delay` fails by
/// name with the observed p50 (`63.0 ms, outside the clean link's own [20,30] ms
/// band`).
async fn band_composition(spec: FairArmSpec) {
    let run = with_timeout(ARM_DEADLINE, "m4/band", run_fairness_arm(spec)).await;
    let samples = &run.samples;
    let mut counts = [0usize; BAND_EDGES.len()];
    for s in samples {
        let y = s.2;
        for (i, (lo, hi)) in BAND_EDGES.iter().enumerate() {
            if y >= *lo && y < *hi {
                counts[i] += 1;
                break;
            }
        }
    }
    let total = samples.len();
    let tail: usize = samples
        .iter()
        .filter(|s| s.2 > CLEAN_ONE_WAY_CEILING_MS)
        .count();
    // Per flow, and in time: a per-packet random delay spreads evenly across
    // the four flows and scatters in time, while a stall concentrates.
    let mut per_flow_tail = [0usize; M4_FLOWS];
    let mut per_flow_max = [0.0f64; M4_FLOWS];
    for s in samples {
        let flow = (0..M4_FLOWS).find(|&i| m4_flow_tag(i) == s.0);
        if let Some(flow) = flow {
            per_flow_max[flow] = per_flow_max[flow].max(s.2);
            if s.2 > CLEAN_ONE_WAY_CEILING_MS {
                per_flow_tail[flow] += 1;
            }
        }
    }
    let mut tail_times: Vec<f64> = samples
        .iter()
        .filter(|s| s.2 > CLEAN_ONE_WAY_CEILING_MS)
        .map(|s| s.1)
        .collect();
    tail_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let clustered = tail_times
        .windows(2)
        .filter(|w| w[1] - w[0] <= 0.100)
        .count();
    let bands: Vec<String> = BAND_EDGES
        .iter()
        .zip(counts.iter())
        .map(|((lo, hi), c)| {
            let hi = if hi.is_finite() {
                format!("{hi:.0}")
            } else {
                "inf".to_owned()
            };
            if total == 0 {
                format!("[{lo:.0},{hi}):{c}")
            } else {
                format!(
                    "[{lo:.0},{hi}):{c}({:.2}%)",
                    100.0 * *c as f64 / total as f64
                )
            }
        })
        .collect();
    println!(
        "[m4-band] flows={} samples={} c2s_received={} c2s_forwarded={} c2s_dropped={} drop_pct={:.3} tail_over_{}ms={} ({:.3}%) p50={:.1} p99={:.1} max={:.1} bands={}",
        M4_FLOWS,
        total,
        run.c2s_received,
        run.c2s_forwarded,
        run.c2s_dropped,
        if run.c2s_received == 0 {
            0.0
        } else {
            100.0 * run.c2s_dropped as f64 / run.c2s_received as f64
        },
        CLEAN_ONE_WAY_CEILING_MS as u64,
        tail,
        if total == 0 {
            0.0
        } else {
            100.0 * tail as f64 / total as f64
        },
        run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max),
        run.flows.iter().map(|f| f.summary.p99).fold(0.0, f64::max),
        run.flows.iter().map(|f| f.summary.max).fold(0.0, f64::max),
        bands.join(" "),
    );
    // The tail's own values, not only its bins: a clump at one repair offset
    // and a ramp read the same in a histogram when the ramp is short.
    let mut tail_values: Vec<f64> = samples
        .iter()
        .map(|s| s.2)
        .filter(|&y| y > CLEAN_ONE_WAY_CEILING_MS)
        .collect();
    tail_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "[m4-band] tail values (sorted, ms): {}",
        tail_values
            .iter()
            .map(|y| format!("{y:.2}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!(
        "[m4-band] per-flow tail={per_flow_tail:?} max={:?} tail_clustered_within_100ms={clustered} of {} tail samples",
        per_flow_max.map(|m| format!("{m:.1}")),
        tail_times.len(),
    );
    println!(
        "[m4-band] tail times (s): {}",
        tail_times
            .iter()
            .map(|t| format!("{t:.2}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    // Instrument sanity, in the order a degenerate reading would break it: the
    // arm measured something, every measured sample landed in exactly one band,
    // and the lane it measured is the clean link it declares (its body sits
    // inside `OWD + JITTER`).
    assert!(
        total > 0,
        "[m4-band] the arm delivered no sample: a band composition of nothing is not a measurement",
    );
    assert_eq!(
        counts.iter().sum::<usize>(),
        total,
        "[m4-band] the band bins hold {} of {total} samples: the composition is not the series",
        counts.iter().sum::<usize>(),
    );
    let p50_max = run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max);
    // Reported, not asserted, and the `M4_BAND_noloss` diagnostic is why: the
    // same arm with the link's loss closed still produced a non-empty tail in
    // every run measured, so "a message above the ceiling was paid for by a
    // dropped datagram" is **false on this instrument** and asserting it would
    // be asserting that the arm has loss configured. What the two numbers
    // together say -- how much of the tail the drop count can account for -- is
    // the reading, so they are printed side by side.
    println!(
        "[m4-band] tail_over_ceiling={tail} c2s_dropped={} tail_per_drop={:.4}",
        run.c2s_dropped,
        if run.c2s_dropped == 0 {
            f64::INFINITY
        } else {
            tail as f64 / run.c2s_dropped as f64
        },
    );
    assert!(
        (20.0..=CLEAN_ONE_WAY_CEILING_MS).contains(&p50_max),
        "[m4-band] the arm's body p50 is {p50_max:.1} ms, outside the clean link's own [{:.0},{CLEAN_ONE_WAY_CEILING_MS:.0}] ms band: this is not the link the arm declares",
        OWD.as_millis() as f64 - JITTER.as_millis() as f64,
    );
}

// ────────── M4 fair latency: the starved-flow bound, made to bite ────────────

/// How long the fair-latency fault holds the flagged flow's messages. Sized
/// from the bound it must cross: the arm's own one-way floor is `OWD` = 25 ms
/// (measured p90 `23.6-29.7 ms` at seed 4096 and `137.7-158.8 ms` at 1024), so
/// a hold that moves **one** flow's p90 has to exceed
/// `(M4_LATENCY_SPREAD_BOUND - 1) x floor` = `25 ms` to raise the ratio past
/// the bound on the floor; `30 ms` is the smallest whole value above that, and
/// on the transient-free lane it lands the flagged flow's p90 at
/// `~54-60 ms` against the others' `~24-26 ms`, a ratio of `~2.3`.
const M4_FAIR_LATENCY_HOLD_MS: u64 = 30;

/// The fair-latency arm's own fault namespace, and its only probe: hold the
/// **last** flow's messages `M4_FAIR_LATENCY_HOLD_MS` between their send
/// timestamp and their write ([`HoldWriter`]). That is the sender-side
/// simulation of a lane that orders one flow's traffic last, which is the
/// property [`M4_LATENCY_SPREAD_BOUND`] claims to catch, and it perturbs the
/// arm's own *input* (where the flag sits in the write path), never the
/// assertion. The namespace is its own (`M4_FAIR_LATENCY_*`) rather than M4's,
/// so a probe of this bound cannot read as a probe of M4's.
fn m4_fair_latency_hold() -> Option<(usize, Duration)> {
    match fault("M4_FAIR_LATENCY")?.as_str() {
        "M4_FAIR_LATENCY_hold" => {
            Some((M4_FLOWS - 1, Duration::from_millis(M4_FAIR_LATENCY_HOLD_MS)))
        }
        _ => None,
    }
}

/// The arm the fair-latency bound reads: M4's own `clean` arm -- the same
/// `link(41/42, OWD, JITTER, LOSS_2, 0)` interactive link, the same `M4_FLOWS`
/// tagged flows offering the same payload at the same `CADENCE` over the same
/// window, and the same connected-but-unladen bulk lane -- taken from
/// [`fairness_arms`] rather than restated, so the two cannot drift apart. Its
/// only difference from [`m4_clean_level_arm`] is the fault namespace it reads.
fn m4_fair_latency_arm() -> FairArmSpec {
    let mut spec = fairness_arms("M4")
        .into_iter()
        .next()
        .expect("the M4 arm set always carries its clean arm first");
    spec.hold = m4_fair_latency_hold();
    spec
}

/// Mandate 4's **fair-latency** arm: the bound that catches a flow being served
/// preferentially, asserted on a statistic that is a property of the lane.
///
/// **Why this arm exists.** [`M4_LATENCY_SPREAD_BOUND`] was asserted on
/// `max p99 / min p99` on the clean lane, and on the pre-4096 transport that
/// ratio was held at `~1.01` by the *start-up transient*: every flow's p99 sat
/// on the shared transient's body rather than on anything about that flow's
/// service. Removing the transient (`rtp/GATE.md`, the pacer seed) puts each
/// flow's p99 on the link floor plus that flow's own `p99`-th-largest
/// uncovered-loss stall, so the ratio became a quotient of two small integer
/// counts -- measured per-flow p99 at seed 4096 spanning `26.2-99.2 ms` inside
/// one arm while all four p50s stayed at `21.2-23.4 ms` and every flow's
/// delivery stayed `1.000`. A bound that fires on that draw is not coverage: it
/// reports `2.25` on a lane with no scheduler asymmetry at all.
///
/// This arm asserts the same claim on the same bound at the same window,
/// cadence and tier, but on the per-flow **p90** ([`fair_latency_spread`]) --
/// the quantile the transient moves and a rare-event count does not. Measured
/// over 13 reps at seeds 1024 and 4096 the p90 spread is `1.006-1.144`,
/// `1.75x` inside the bound, while the read of the same lane's per-flow p90s is
/// reported per flow so a breach names the flow. It is the bound's own red
/// proof the file was missing: `M4_LATENCY_SPREAD_BOUND` had **no** vacuity
/// probe on the clean lane (the two `M4_CLEAN_LEVEL_*` probes shift the whole
/// link and leave all four flows equal), so it had never been shown to fail on
/// a genuinely preferential lane.
///
/// The per-flow delivery floor is read *with* the bound, so a lane that meets
/// its spread by starving a flow fails it.
///
/// Vacuity: `MANDATE_SMOKE_FAULT=M4_FAIR_LATENCY_hold` holds the last flow's
/// messages `M4_FAIR_LATENCY_HOLD_MS` at the sender and fails this bound by
/// name, with the other flows still at the floor and delivery still `1.000`.
///
/// **What it does not cover, stated rather than implied.** It bounds the
/// *body* of the per-flow distribution. A starving scheduler that delayed only
/// a flow's rare messages, leaving its p90 at the floor, would not move this
/// statistic; on this lane that regime is indistinguishable from the iid loss
/// draw that already moves p99/p999 by a factor of 2-3 between identical runs,
/// and the cells that do bound it are the aggregate ceiling
/// ([`M4_CLEAN_P99_CEILING_MS`]), the per-flow delivery floor
/// ([`M4_DELIVERY_FLOOR`]) and the share imbalance ([`M4_IMBALANCE_BOUND`]),
/// each asserted on this same run.
#[tokio::test(flavor = "multi_thread")]
async fn m4_clean_lane_fair_latency() {
    let _serial = SERIAL.lock().await;
    let run = with_timeout(
        ARM_DEADLINE,
        "m4/clean_fair_latency",
        run_fairness_arm(m4_fair_latency_arm()),
    )
    .await;
    let p90: Vec<f64> = run.flows.iter().map(|f| f.summary.p90).collect();
    let p50_max = run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max);
    let delivery_min = run
        .flows
        .iter()
        .map(|f| f.summary.delivery_pct)
        .fold(f64::INFINITY, f64::min);
    let samples: u64 = run.flows.iter().map(|f| f.summary.received).sum();
    let spread = fair_latency_spread(&run);
    let pass = samples > 0
        && delivery_min >= M4_DELIVERY_FLOOR
        && spread.is_finite()
        && spread > 0.0
        && spread <= M4_LATENCY_SPREAD_BOUND;
    // Its own verdict line, not a `MANDATE` line and not an `[mandate-smoke …]`
    // arm row, for the reason [`m4_clean_lane_p99_ceiling`] states.
    println!(
        "[m4-fair-latency] {} flows={} spread={:.3} spread_bound={:.1} p90={:?} p50_max={:.1} \
         delivery_min={:.3} samples={} window_s={:.1} wall_s={:.1}",
        if pass { "PASS" } else { "FAIL" },
        M4_FLOWS,
        spread,
        M4_LATENCY_SPREAD_BOUND,
        p90,
        p50_max,
        delivery_min,
        samples,
        run.window.as_secs_f64(),
        run.wall.as_secs_f64(),
    );
    assert!(
        samples > 0 && spread.is_finite() && spread > 0.0,
        "[M4 fair-latency] the arm measured {samples} delivered message(s) and a spread of {spread}: a lane with no samples or no percentile is an instrument failure, not a spread that passes",
    );
    assert!(
        delivery_min >= M4_DELIVERY_FLOOR,
        "[M4 fair-latency] clean-arm flow delivery {delivery_min:.3} is under the {M4_DELIVERY_FLOOR} floor (sent={:?} received={:?}): a spread met by starving a flow is not met",
        run.flows.iter().map(|f| f.sent).collect::<Vec<_>>(),
        run.flows.iter().map(|f| f.received).collect::<Vec<_>>(),
    );
    assert!(
        spread <= M4_LATENCY_SPREAD_BOUND,
        "[M4 fair-latency] per-flow p90 spread {spread:.3} exceeds the {M4_LATENCY_SPREAD_BOUND} fair-latency bound (per-flow p90 {p90:?}, p50_max {p50_max:.1}): one flow's latency body is {spread:.2}x the best flow's, so that flow is being served preferentially on the shared interactive lane",
    );
}

// ────── M4 hostile level: the production flow count's hostile tail, bounded ──

/// The arm the four-flow hostile **level** assertion reads: M4's own
/// `hostile` arm -- the same `hostile_link(41/42)` GE `5 %`/mean-8 + 100 ms
/// jitter link, the same `M4_FLOWS` tagged flows offering the same payload at
/// the same `CADENCE` over the same window, and the same connected-but-unladen
/// bulk lane -- taken from [`fairness_arms`] rather than restated, so the two
/// cannot drift apart.
///
/// It carries its own fault namespace (`M4_HOSTILE_LEVEL_*`) rather than M4's,
/// because the gap this arm closes is that M4's own hostile arm guards each
/// flow against M1's loose `900 ms` regression guard while nothing bounds the
/// production flow count's **level**: a probe of this assertion must not read
/// as a probe of M4's.
fn m4_hostile_level_arm() -> FairArmSpec {
    let mut spec = fairness_arms("M4")
        .into_iter()
        .nth(1)
        .expect("the M4 arm set always carries its hostile arm second");
    if let Some(fault) = fault("M4_HOSTILE_LEVEL") {
        // The level bound's own vacuity probes, as a further one-way delay on
        // the arm's hostile link. `double` is sized so the aggregate p99 lands
        // past the bound while the per-flow delivery floor stays green (the
        // shift lengthens the round trip but does not starve the lane: every
        // message is still observed inside the arm's `window + GRACE` cutoff),
        // and `slow` is the deeper probe, which is also **composite**: at
        // `+300 ms` it lengthens the round trip enough to slow the arm's own
        // rate ramp as well as the one-way hop, so its reading is a floor on
        // what the shift costs, not a measurement of one.
        let extra_ms = match fault.as_str() {
            "M4_HOSTILE_LEVEL_double" => Some(100),
            "M4_HOSTILE_LEVEL_slow" => Some(300),
            _ => None,
        };
        if let Some(extra_ms) = extra_ms {
            spec.int_c2s.latency += Duration::from_millis(extra_ms);
            spec.int_s2c.latency += Duration::from_millis(extra_ms);
        }
    }
    spec
}

/// Mandate 4's **hostile level** arm: the four-flow hostile lane against a
/// ceiling of its own.
///
/// M1 asserts its 250 ms ceiling on its **one-flow** clean arm; M4 reports the
/// four-flow hostile p99 and guards each flow against M1's loose `900 ms`
/// regression guard, so the production flow count's own hostile level was
/// bounded by no arm -- the eleven `hostile_p99` bars of a passing battery sat
/// **above** the 250 ms ceiling drawn on the panel while no gate named their
/// level (the `m1-four-flow-hostile@flows=4+impairment=hostile+metric=p99-ceiling`
/// gap in `GATE.md`). This arm closes that gap as a **new** arm beside M4 (M4
/// itself is untouched): it takes `fairness_arms("M4")`'s hostile arm -- the
/// same link, flows, cadence and window -- and asserts
/// [`M4_HOSTILE_P99_CEILING_MS`] on the aggregated `hostile_p99_max`.
///
/// The bound exceeds the M1 ceiling and the arm says so in its own line: the
/// four-flow hostile tail is the product's **known** hostile defect (the 1 s
/// `MIN_RTO` repair floor plus backoff), and the honest closure is to bound it
/// where the measurement supports it and name the hole, not to bound it at a
/// ceiling that is currently false. The per-flow delivery floor is read *with*
/// the level, so a lane that meets its p99 by starving a flow fails it; the
/// clean level's fair-latency spread is deliberately **not** asserted here,
/// because on the hostile link the per-flow p99 differences are a GE loss
/// realization rather than a scheduler property (M4's own arm records a spread
/// up to `2.93x` and asserts M1's absolute guard there instead).
///
/// Vacuity: `MANDATE_SMOKE_FAULT=M4_HOSTILE_LEVEL_double` perturbs the arm's
/// own input and fails this bound by name.
#[tokio::test(flavor = "multi_thread")]
async fn m4_hostile_lane_p99_ceiling() {
    let _serial = SERIAL.lock().await;
    let run = with_timeout(
        ARM_DEADLINE,
        "m4/hostile_level",
        run_fairness_arm(m4_hostile_level_arm()),
    )
    .await;
    let p99_max = run.flows.iter().map(|f| f.summary.p99).fold(0.0, f64::max);
    let p99_min = run
        .flows
        .iter()
        .map(|f| f.summary.p99)
        .fold(f64::INFINITY, f64::min);
    let p50_max = run.flows.iter().map(|f| f.summary.p50).fold(0.0, f64::max);
    let delivery_min = run
        .flows
        .iter()
        .map(|f| f.summary.delivery_pct)
        .fold(f64::INFINITY, f64::min);
    let samples: u64 = run.flows.iter().map(|f| f.summary.received).sum();
    let guard_tighter = M1_HOSTILE_P99_GUARD_MS / M4_HOSTILE_P99_CEILING_MS;
    let pass = samples > 0
        && delivery_min >= M4_DELIVERY_FLOOR
        && p99_max.is_finite()
        && p99_max > 0.0
        && p99_max <= M4_HOSTILE_P99_CEILING_MS;
    // Deliberately not a `MANDATE` line and deliberately not an
    // `[mandate-smoke …]` arm row: `tools/mandate-check` owns the M1-M4 id set
    // and attributes every arm row to the mandate whose `MANDATE` line follows
    // it, so a row or an id printed here would be read as M4's own measurement
    // (or refused as a second declaration of one). This arm's verdict is its
    // own line and its exit status.
    println!(
        "[m4-hostile-level] {} flows={} p99_max={:.1} p99_min={:.1} p50_max={:.1} \
         delivery_min={:.3} samples={} ceiling={:.1} ceiling_over_m1={:.3} \
         fire_ratio={:.3} delivery_floor={:.3} window_s={:.1} wall_s={:.1}",
        if pass { "PASS" } else { "FAIL" },
        M4_FLOWS,
        p99_max,
        p99_min,
        p50_max,
        delivery_min,
        samples,
        M4_HOSTILE_P99_CEILING_MS,
        M4_HOSTILE_P99_CEILING_MS / M1_CEILING_MS,
        M4_HOSTILE_P99_CEILING_MS / p99_max,
        M4_DELIVERY_FLOOR,
        run.window.as_secs_f64(),
        run.wall.as_secs_f64(),
    );
    assert!(
        samples > 0 && p99_max.is_finite() && p99_max > 0.0,
        "[M4 hostile level] the arm measured {samples} delivered message(s) and an aggregate p99 of {p99_max} ms: a lane with no samples, no percentile or a zero percentile is an instrument failure, not a level that passes",
    );
    assert!(
        delivery_min >= M4_DELIVERY_FLOOR,
        "[M4 hostile level] hostile-arm flow delivery {delivery_min:.3} is under the {M4_DELIVERY_FLOOR} floor (the arm's own per-flow counts: sent={:?} received={:?}): a level met by starving a flow is not met",
        run.flows.iter().map(|f| f.sent).collect::<Vec<_>>(),
        run.flows.iter().map(|f| f.received).collect::<Vec<_>>(),
    );
    assert!(
        p99_max <= M4_HOSTILE_P99_CEILING_MS,
        "[M4 hostile level] the four-flow hostile lane's p99 {p99_max:.1} ms exceeds its {M4_HOSTILE_P99_CEILING_MS:.1} ms ceiling (p50 {p50_max:.1}, per-flow p99 {:?}): the production shape runs {M4_FLOWS} flows on the one interactive lane, and its hostile tail is bounded only by M4's loose {M1_HOSTILE_P99_GUARD_MS:.0} ms per-flow guard -- a level regression past {M4_HOSTILE_P99_CEILING_MS:.1} ms (1.07x the recorded worst, {guard_tighter:.2}x tighter than that guard) is a product regression M1's one-flow clean arm cannot see",
        run.flows.iter().map(|f| f.summary.p99).collect::<Vec<_>>(),
    );
}

// ───── M4/TCP: what competing with a loss-based flow costs the interactive lane ─────
//
// M4 runs the bulk lane **idle**, so it cannot see the trade the product makes
// the moment that lane competes. This arm adds the missing half: the production
// dual-lane mux client with its bulk lane **saturating** one shared
// `BottleneckShaper`, alongside an `rtp` connection running the
// `#[cfg(feature = "testing")]` AIMD reference law (`reference_aimd = true`)
// on the same shaper. It reads both quantities the trade is made of, and
// neither may be omitted:
//
//   * the bulk lane's delivered share against the competitor's, with the pair's
//     aggregate as a fraction of the shaper's capacity -- the competitor must be
//     seen to saturate, or the share means nothing;
//   * the interactive flows' p99 and max, per flow, printed against M1's
//     ceiling, so a fair-but-slow split (the bulk lane yielding capacity, but
//     filling the bottleneck's queue while it does) is visible.
//
// **Asserted:** the bulk lane's share against `M4_TCP_BULK_SHARE_FLOOR`, a
// product bound derived from the arm's first measurement. **Reported, not
// asserted:** the interactive tail, whose first reading is 2.1-3.1x M1's
// ceiling -- a passing guard there would launder a real M1 breach as a pass, so
// the ceiling is drawn on `M4-tcp_interactive_tail` and the breach is declared
// as an open defect in `GATE.md` instead. The instrument's own sanity -- the
// competitor saturates, the interactive samples are non-empty, and every
// interactive flow delivered -- is asserted too. Its own fault namespace is
// `M4_TCP_*`, so a probe of this arm cannot read as a probe of any other.

/// The arm's fault selector, owned by this arm alone. `M4_TCP_STALL_BULK`
/// writes nothing on the mux bulk lane for the whole window, so the bulk
/// delivered-byte counter stays zero and the arm's own bulk-presence sanity
/// (the denominator of the share it measures) fails by name while every other
/// reading -- the competitor's saturation, the interactive flows' delivery and
/// latency -- still prints. It is the demonstration that the arm's reading can
/// fail from the measurement path.
fn m4_tcp_fault() -> Option<String> {
    let value = std::env::var("MANDATE_SMOKE_FAULT").ok()?;
    let value = value.trim();
    if value.is_empty() || !value.starts_with("M4_TCP") {
        return None;
    }
    Some(value.to_owned())
}

/// The saturation sanity floor: the two bulk flows (the product's mux bulk lane
/// and the AIMD reference) together must deliver at least this fraction of the
/// shaper's serialization capacity, or the competitor is not contesting the
/// link and the share this arm measures is not a comparison against a competent
/// loss-based competitor. This is an **instrument** floor on the reference, not
/// a product goodput bound: rtp's own loss-based A/B sets its analogous floor at
/// `0.80` against a two-reference aggregate (see
/// `MIN_LOSS_AB_AGGREGATE_FRACTION_OF_CAP` in `rtp/tests/shared_bottleneck.rs`);
/// this arm's aggregate also carries the interactive lane's own tiny share of
/// the same queue, so its floor is deliberately slack. The first measurement
/// this arm produces is the input the product bound is derived from; the floor
/// only has to catch a reference that stopped contesting.
const M4_TCP_SATURATION_FLOOR: f64 = 0.5;

/// The **product** bulk lane's share floor, asserted by [`m4_tcp_competition`]
/// and drawn on the `tcp_bulk_share` panel. Derived from the arm's first
/// measurement, not picked: the 12 s window on the pinned build reads the bulk
/// lane's share of the two bulk flows as `0.349`, and the floor is that reading
/// halved (`0.349 / 2 = 0.1745`) and rounded down to two decimals, `0.17`. The
/// headroom is deliberate: the measured share passes at `2.05x` the floor, so
/// run-to-run movement in a 12 s window cannot fail the bound, while the fault
/// this arm's own namespace produces (`MANDATE_SMOKE_FAULT=M4_TCP_STALL_BULK`,
/// bulk delivered bytes `0`, share `0.000`) fails it by `0.17`. It is a
/// **presence** floor, not a fairness claim: the measured reading is `0.70x`
/// the equal split, so a floor at the fair share would fail every run.
const M4_TCP_BULK_SHARE_FLOOR: f64 = 0.17;

/// The shared uplink's drop-tail buffer, in bytes. It must be **finite**: those
/// drops are the loss signal the reference's multiplicative decrease acts on,
/// so an unbounded (loss-free) queue leaves the reference a greedy flow that
/// never decreases and the arm measures queue growth without bound (measured:
/// interactive p99 ~1015 ms, 4.1x M1's ceiling, with the buffer at 0). The
/// 128 KiB value is the one the `rtp` loss-based A/B (`tests/shared_bottleneck`)
/// and the `cc_link`/`ibfq` arms use, so the competitor's AIMD engages rather
/// than the queue growing without a loss to react to.
const M4_TCP_SHAPER_LIMIT_BYTES: u64 = 128 * 1024;

/// The bottleneck's own queueing over one M4/TCP window, plus the interactive
/// lane's client->server link counters. The shared-buffer backlog is sampled
/// from [`BottleneckShaper::backlog_bytes`] while the window runs; its maximum
/// is the largest queueing delay the bottleneck itself imposed (converted to
/// time at [`SHARED_UP_RATE_BPS`]) and is the whole of term (a). The shaper's
/// `dropped` counter is the tail-drop signal the reference's AIMD acts on; the
/// interactive counters are its `NetemPair`'s c2s direction, read before the
/// pair is stopped.
#[derive(Clone, Copy, Debug, Default)]
struct M4LinkEvidence {
    shaper_max_backlog_bytes: u64,
    shaper_mean_backlog_bytes: u64,
    shaper_backlog_samples: u64,
    shaper_dropped: u64,
    interactive_received: u64,
    interactive_forwarded: u64,
    interactive_dropped: u64,
}

/// The interactive lane's repair activity over one window, read from an rtp
/// metrics observer on the **client** connection (the same counters the crate's
/// wire-measured repair ladder reads). `rungs` is `attempts + tail_probes`: one
/// per repair transmission the send space fired. The reason fields name which
/// evidence armed each rung; `armor_duplicates` counts the
/// `RetransmissionArmorDuplicate` events (one per copy datagram actually
/// written) and `parity_sent` is the FEC flush's cumulative parity datagrams.
/// `max_write_waiters`/`max_in_flight` are the client-side send-path queue
/// depth (term (c)): a writer awaiting the connection is a message held in the
/// client's own egress before the shaper.
#[derive(Clone, Copy, Debug, Default)]
struct M4RepairEvidence {
    rungs: u64,
    first_attempts: u64,
    repeat_attempts: u64,
    rto_reason: u64,
    reorder_reason: u64,
    fast_loss_reason: u64,
    pre_outage_reason: u64,
    tail_probes: u64,
    armor_duplicates: u64,
    parity_sent: u64,
    max_write_waiters: u64,
    max_in_flight: u64,
}

/// Atomic cell written by [`m4_repair_observer`]. Cumulative counters are kept
/// as running maxima so a reading cannot regress when the underlay is briefly
/// backpressured; `rung_timeline` keeps one `base`-clock stamp per rung so a
/// repair can be read against the message arrivals the sink reports.
#[derive(Default)]
struct M4RepairTaps {
    rungs: AtomicU64,
    first_attempts: AtomicU64,
    repeat_attempts: AtomicU64,
    rto_reason: AtomicU64,
    reorder_reason: AtomicU64,
    fast_loss_reason: AtomicU64,
    pre_outage_reason: AtomicU64,
    tail_probes: AtomicU64,
    parity_sent: AtomicU64,
    max_write_waiters: AtomicU64,
    max_in_flight: AtomicU64,
    armor_duplicates: AtomicU64,
    rung_timeline: Mutex<Vec<f64>>,
}

impl M4RepairTaps {
    fn repair(&self) -> M4RepairEvidence {
        M4RepairEvidence {
            rungs: self.rungs.load(Ordering::Relaxed),
            first_attempts: self.first_attempts.load(Ordering::Relaxed),
            repeat_attempts: self.repeat_attempts.load(Ordering::Relaxed),
            rto_reason: self.rto_reason.load(Ordering::Relaxed),
            reorder_reason: self.reorder_reason.load(Ordering::Relaxed),
            fast_loss_reason: self.fast_loss_reason.load(Ordering::Relaxed),
            pre_outage_reason: self.pre_outage_reason.load(Ordering::Relaxed),
            tail_probes: self.tail_probes.load(Ordering::Relaxed),
            armor_duplicates: self.armor_duplicates.load(Ordering::Relaxed),
            parity_sent: self.parity_sent.load(Ordering::Relaxed),
            max_write_waiters: self.max_write_waiters.load(Ordering::Relaxed),
            max_in_flight: self.max_in_flight.load(Ordering::Relaxed),
        }
    }

    fn rung_times(&self) -> Vec<f64> {
        self.rung_timeline.lock().unwrap().clone()
    }
}

/// An observer on the interactive lane's **client** connection that records the
/// repair counters and the client-side send-path queue depth. A state snapshot
/// is taken on each application-frame write (`SendFrameBuffer`), each received
/// ACK (`ReceiveAckPacket`) and each RTT sample, which brackets every rung the
/// send space fires to within the interactive cadence. The
/// `RetransmissionArmorDuplicate` event is counted without a snapshot: rtp
/// emits exactly one per copy datagram, and the copy is already on the wire.
/// Every rung is stamped on the arm's own `base` clock, so a repair can be read
/// against the message arrivals the sink reports on that same clock.
fn m4_repair_observer(base: Instant) -> (MetricsObserver, Arc<M4RepairTaps>) {
    let taps = Arc::new(M4RepairTaps::default());
    let observer = MetricsObserver::selective(
        |event, _elapsed| match event {
            MetricsEvent::RetransmissionArmorDuplicate => MetricsInterest::EventOnly,
            MetricsEvent::SendFrameBuffer
            | MetricsEvent::ReceiveAckPacket
            | MetricsEvent::RttSample => MetricsInterest::Snapshot,
            _ => MetricsInterest::Skip,
        },
        {
            let taps = Arc::clone(&taps);
            move |observation: MetricsObservation| {
                if observation.event == MetricsEvent::RetransmissionArmorDuplicate {
                    taps.armor_duplicates.fetch_add(1, Ordering::Relaxed);
                }
                let Some(snapshot) = observation.snapshot else {
                    return;
                };
                let counters = snapshot.retransmission_counters;
                let rungs = counters.attempts + counters.tail_probes;
                let prior = taps.rungs.fetch_max(rungs, Ordering::Relaxed);
                if rungs > prior {
                    taps.rung_timeline
                        .lock()
                        .unwrap()
                        .push(base.elapsed().as_secs_f64());
                }
                taps.first_attempts
                    .fetch_max(counters.first_attempts, Ordering::Relaxed);
                taps.repeat_attempts
                    .fetch_max(counters.repeat_attempts, Ordering::Relaxed);
                taps.rto_reason
                    .fetch_max(counters.rto_reason, Ordering::Relaxed);
                taps.reorder_reason
                    .fetch_max(counters.reorder_reason, Ordering::Relaxed);
                taps.fast_loss_reason
                    .fetch_max(counters.fast_loss_reason, Ordering::Relaxed);
                taps.pre_outage_reason
                    .fetch_max(counters.pre_outage_reason, Ordering::Relaxed);
                taps.tail_probes
                    .fetch_max(counters.tail_probes, Ordering::Relaxed);
                if let Some(fec) = snapshot.fec_counters {
                    taps.parity_sent
                        .fetch_max(fec.parity_sent, Ordering::Relaxed);
                }
                taps.max_write_waiters
                    .fetch_max(snapshot.application_write_waiters as u64, Ordering::Relaxed);
                taps.max_in_flight
                    .fetch_max(snapshot.in_flight_packets as u64, Ordering::Relaxed);
            }
        },
    );
    (observer, taps)
}

/// One interactive flow's outcome on the competing lane: what it offered, what
/// it got back, and the latency summary its tail bound is read from, plus the
/// split of its samples at the no-loss ceiling the arm measured
/// (`min_latency + shaper_max_queue_ms`). A sample at or below the ceiling is
/// explainable by propagation plus the bottleneck's own largest queue; a sample
/// above it cannot be, and its extra latency is a repair that unblocked the
/// stream.
struct M4TcpFlow {
    tag: u8,
    sent: u64,
    received: u64,
    summary: HolSummary,
    min_ms: f64,
    ceiling_ms: f64,
    no_repair_count: u64,
    no_repair_p50: f64,
    no_repair_p99: f64,
    repaired_count: u64,
    repaired_p50: f64,
    repaired_p99: f64,
    repaired_max: f64,
}

/// One M4/TCP arm's outcome: the two bulk flows' delivered bytes and the share
/// and saturation they imply, plus the interactive flows that share their queue
/// and the measured evidence the tail is decomposed from.
struct M4TcpRun {
    flows: Vec<M4TcpFlow>,
    /// Bytes the product's mux bulk lane delivered to its sink over the window.
    bulk_delivered: u64,
    /// Bytes the rtp AIMD reference delivered to its own sink over the window.
    comp_delivered: u64,
    /// `bulk / (bulk + comp)`; the product lane's share of the two bulk flows.
    bulk_share: f64,
    /// The competitor's own delivered bytes as a fraction of the shaper's
    /// capacity -- positive `bulk_share` against a non-saturated competitor is
    /// not a comparison.
    comp_fraction: f64,
    /// The two bulk flows' aggregate as a fraction of the shaper's capacity.
    aggregate_fraction: f64,
    /// Term (a): the largest queueing delay the shared bottleneck imposed over
    /// the window, in ms, from the sampled backlog and [`SHARED_UP_RATE_BPS`].
    shaper_max_queue_ms: f64,
    /// The bottleneck's own counters and the interactive lane's c2s counters.
    link: M4LinkEvidence,
    /// Term (b): the interactive lane's repair activity over the window.
    repair: M4RepairEvidence,
    /// The `base`-clock stamp of every rung the interactive lane fired.
    rung_times: Vec<f64>,
    window: Duration,
    wall: Duration,
    /// Every interactive sample the sink reported, `(tag, base-clock seconds,
    /// latency ms)`. Kept raw because the per-flow summaries above integrate
    /// the *whole* window: the stand-off resume arm splits this series at its
    /// own resume instant, which no per-flow summary can express.
    samples: Vec<(u8, f64, f64)>,
    /// The shared shaper's own backlog over the window, `(base-clock seconds,
    /// bytes)`, sampled at ~1 ms. `shaper_max_queue_ms`/`link` hold the window
    /// aggregates; this is the same series unresolved, so a resume arm can read
    /// the queue the lane crossed at the instant it resumed.
    backlog_timeline: Vec<(f64, u64)>,
    /// The product bulk lane's own `congestion_control_rtt`, `(base-clock
    /// seconds, ms)`, from an observer on the mux bulk lane's client
    /// connection. Empty unless a caller attached the observer.
    bulk_rtt_timeline: Vec<(f64, f64)>,
    /// Both bulk flows' cumulative delivered-byte counters, `(base-clock
    /// seconds, product bytes, reference bytes)`, sampled with the backlog. The
    /// share over any sub-window is the increment of these counters, so a resume
    /// arm can read the share *before* the lane resumed (while the stand-off
    /// competes) separately from the whole window.
    bytes_timeline: Vec<(f64, u64, u64)>,
    /// The stand-off factor the hub read back on this window's path, or `None`
    /// when the arm ran without a resume spec (the frozen arm's default hub).
    beta_readback: Option<f64>,
    /// `base`-clock seconds at which the interactive lane's gated resume offered
    /// its first message, or `None` when there was no gate.
    resume_at: Option<f64>,
    /// Whether the resume's own gate (the bulk stand-off armed for the requested
    /// margin past `STANDOFF_WINDOW`) was observed before its deadline. Always
    /// `true` when there was no gate.
    resume_armed: bool,
}

/// A gated resume schedule for [`run_m4_tcp_arm_with`]'s interactive lane: hold
/// the offer until the bulk stand-off has been **armed for `margin` past
/// `rtp::cc::STANDOFF_WINDOW`** -- the interactive lane's own offer clock quiet
/// long enough that a competing episode is running and the queue it holds is at
/// its steady state -- then offer at the production cadence for `active`.
///
/// This is the only way to reach the stand-off's per-path decrease factor `beta`
/// *while* the interactive lane transmits: the gate that arms the stand-off is
/// the interactive lane's own offer clock, so a lane offering continuously keeps
/// the stand-off disarmed and `beta` inert (which is why the no-competitor (B)
/// arms read the shipped policy by construction). The lever can only be measured
/// as a **resume**: quiet long enough to build the queue, then transmit across
/// it.
#[derive(Clone, Copy)]
struct M4ResumeSpec {
    /// The stand-off's per-path multiplicative-decrease factor
    /// (`rtp::cc::CcSignalHub::with_standoff_decrease_factor`).
    beta: f64,
    /// How long the offer clock must have been quiet past `STANDOFF_WINDOW`
    /// before the lane resumes, so the competing episode's queue is steady.
    margin: Duration,
    /// How long the resumed lane offers.
    active: Duration,
    /// The bounded wait for the gate; expiring without arming makes the window
    /// an instrument failure (`resume_armed == false`), never a reading.
    deadline: Duration,
}

/// An observer on the **product mux bulk lane's** client connection that records
/// its `congestion_control_rtt` on the arm's own `base` clock, so the RTT
/// inflation the stand-off's larger share is bought with is read from the flow
/// that pays it (the same quantity `standoff_beta_sweep` reads from its direct
/// rtp bulk).
fn m4_bulk_rtt_observer(base: Instant) -> (MetricsObserver, Arc<Mutex<Vec<(f64, f64)>>>) {
    let series = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&series);
    let observer = MetricsObserver::filtered(
        |event, _| event == MetricsEvent::RttSample,
        move |observation| {
            let Some(snapshot) = observation.snapshot else {
                return;
            };
            let Some(rtt) = snapshot.congestion_control_rtt else {
                return;
            };
            sink.lock()
                .unwrap()
                .push((base.elapsed().as_secs_f64(), rtt.as_secs_f64() * 1000.0));
        },
    );
    (observer, series)
}

/// Write a continuous cyclic payload back to back until `run_for` elapses --
/// the saturating offer both bulk flows carry. `offset` is advanced by exactly
/// the bytes each write commits, so the byte stream's own pattern is continuous
/// across writes (both sinks verify `(offset + j) % 251` byte by byte, so a
/// burst that restarts the pattern would silently stop the counter).
async fn m4_tcp_saturate(write: &mut (impl AsyncWrite + Unpin), payload: &[u8], run_for: Duration) {
    let deadline = Instant::now() + run_for;
    let mut offset = 0usize;
    while Instant::now() < deadline {
        match write.write(&payload[offset..]).await {
            Ok(0) | Err(_) => break,
            Ok(n) => offset = (offset + n) % payload.len(),
        }
    }
}

/// Run one M4/TCP arm: the production dual-lane client, `M4_FLOWS` interactive
/// flows on the interactive lane, a **saturating** stream on the mux bulk lane,
/// and an rtp AIMD reference on the same shared `BottleneckShaper`.
///
/// The interactive lane runs with the real cross-lane `CcSignalHub`
/// ([`dual_mux_client_connect_lane_rtp_via_cc_link`]), so the bulk lane's path
/// reads `shared` and its delay-first controller yields -- the production
/// behaviour this arm measures the cost of. The shared shaper has a finite
/// drop-tail buffer ([`M4_TCP_SHAPER_LIMIT_BYTES`]); those drops are the loss
/// signal the reference's multiplicative decrease acts on, so it is a
/// loss-based competitor rather than a greedy flow. The three links add no
/// per-link random loss -- the queue's own overflow is the loss.
async fn run_m4_tcp_arm(window: Duration) -> M4TcpRun {
    run_m4_tcp_arm_with(window, None).await
}

/// [`run_m4_tcp_arm`] with an optional gated interactive **resume** (see
/// [`M4ResumeSpec`]). `resume: None` is the frozen arm's topology unchanged: the
/// interactive lane offers from the start and the stand-off stays disarmed.
/// `Some(spec)` builds the cross-lane hub with the per-path decrease factor
/// `spec.beta` and holds the interactive offer until the stand-off has been
/// armed for `spec.margin` past `STANDOFF_WINDOW`.
async fn run_m4_tcp_arm_with(window: Duration, resume: Option<M4ResumeSpec>) -> M4TcpRun {
    let fault = m4_tcp_fault();
    let stall_bulk = fault.as_deref() == Some("M4_TCP_STALL_BULK");
    let wall = Instant::now();
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::production_bulk();
    let base = Instant::now();
    let (repair_observer, repair_taps) = m4_repair_observer(base);
    let (bulk_rtt_observer, bulk_rtt_timeline) = m4_bulk_rtt_observer(base);
    // The cross-lane hub. A resume builds it with the stand-off's per-path
    // decrease factor; the default hub is the shipped one (`0.5`), so the frozen
    // arm's topology is unchanged. The gate is read from the same `(local,
    // remote)` key the transport resolves, so the arm can witness the stand-off
    // arming before it declares the resume instant.
    let loopback = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let hub = match resume {
        Some(spec) => rtp::cc::CcSignalHub::with_standoff_decrease_factor(spec.beta),
        None => rtp::cc::CcSignalHub::new(),
    };
    let gate = hub.group(loopback, loopback).bulk();
    let beta_readback = resume.map(|_| gate.standoff_decrease_factor());
    let resume_started = Arc::new(AtomicU64::new(0));
    let resume_armed = Arc::new(AtomicBool::new(resume.is_none()));
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let outcome = tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let (comp_addr, comp_delivered) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();

            // One uplink queue all three flows cross: the interactive lane's
            // packets queue behind the two bulk flows, which is the cost this
            // arm exists to measure.
            let shaper = BottleneckShaper::new(SHARED_UP_RATE_BPS, M4_TCP_SHAPER_LIMIT_BYTES);
            // Sample the shared buffer's own backlog for the whole window: the
            // largest value is the largest queueing delay the bottleneck itself
            // imposed, and its mean is the queue the interactive lane lived in.
            // The sampler is a scoped test task, so a panic in it is caught and
            // a forgotten stop cannot outlive the arm's scope.
            let shaper_backlog_max = Arc::new(AtomicU64::new(0));
            let shaper_backlog_sum = Arc::new(AtomicU64::new(0));
            let shaper_backlog_samples = Arc::new(AtomicU64::new(0));
            let shaper_backlog_timeline = Arc::new(Mutex::new(Vec::<(f64, u64)>::new()));
            let bytes_timeline = Arc::new(Mutex::new(Vec::<(f64, u64, u64)>::new()));
            let shaper_sampler_stop = Arc::new(AtomicBool::new(false));
            {
                let shaper = shaper.clone();
                let max = Arc::clone(&shaper_backlog_max);
                let sum = Arc::clone(&shaper_backlog_sum);
                let count = Arc::clone(&shaper_backlog_samples);
                let timeline = Arc::clone(&shaper_backlog_timeline);
                let bytes = Arc::clone(&bytes_timeline);
                let our_bytes = Arc::clone(&bulk_counter);
                let comp_bytes = Arc::clone(&comp_delivered);
                let stop = Arc::clone(&shaper_sampler_stop);
                let task_tx_sampler = task_tx.clone();
                submit_test_task(
                    &task_tx_sampler,
                    Box::pin(async move {
                        while !stop.load(Ordering::Relaxed) {
                            let now = base.elapsed().as_secs_f64();
                            let backlog = shaper.backlog_bytes(Instant::now());
                            max.fetch_max(backlog, Ordering::Relaxed);
                            sum.fetch_add(backlog, Ordering::Relaxed);
                            count.fetch_add(1, Ordering::Relaxed);
                            timeline.lock().unwrap().push((now, backlog));
                            bytes.lock().unwrap().push((
                                now,
                                our_bytes.load(Ordering::Relaxed),
                                comp_bytes.load(Ordering::Relaxed),
                            ));
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }),
                );
            }
            let int_pair = NetemPair::spawn_shared(
                int_addr,
                link(41, OWD, JITTER, 0, 0),
                link(42, OWD, JITTER, 0, 0),
                Some(shaper.clone()),
                None,
            )
            .unwrap();
            let bulk_pair = NetemPair::spawn_shared(
                bulk_addr,
                link(43, OWD, JITTER, 0, 0),
                link(44, OWD, JITTER, 0, 0),
                Some(shaper.clone()),
                None,
            )
            .unwrap();
            let comp_pair = NetemPair::spawn_shared(
                comp_addr,
                link(45, OWD, JITTER, 0, 0),
                link(46, OWD, JITTER, 0, 0),
                Some(shaper.clone()),
                None,
            )
            .unwrap();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via_cc_link(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                Some(repair_observer.clone()),
                resume.is_some().then(|| bulk_rtt_observer.clone()),
                Some(hub),
            )
            .await
            .unwrap();

            // One collector drains the shared tagged channel for the whole arm,
            // keeping `(tag, elapsed, latency)` so each sample is attributable
            // to its flow.
            let collector = Arc::new(Mutex::new(Vec::<(u8, f64, f64)>::new()));
            let collector_sink = Arc::clone(&collector);
            let task_tx_collector = task_tx.clone();
            submit_test_task(
                &task_tx_collector,
                Box::pin(async move {
                    while let Some((tag, latency)) = latencies.recv().await {
                        collector_sink.lock().unwrap().push((
                            tag,
                            base.elapsed().as_secs_f64(),
                            latency,
                        ));
                    }
                }),
            );

            let mut streams = Vec::with_capacity(M4_FLOWS);
            for flow in 0..M4_FLOWS {
                let (mut read, write) = opener.open(LaneClass::Interactive).await.unwrap();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                streams.push((m4_flow_tag(flow), write));
            }
            let mut futs = Vec::with_capacity(M4_FLOWS);
            for (flow, (tag, write)) in streams.iter_mut().enumerate() {
                if write.write_all(&[*tag]).await.is_err() {
                    break;
                }
                let write = &mut *write;
                let gate = gate.clone();
                let resume_started = Arc::clone(&resume_started);
                let resume_armed = Arc::clone(&resume_armed);
                futs.push(async move {
                    let Some(spec) = resume else {
                        return send_timestamped_messages(write, base, MSG_BYTES, CADENCE, window)
                            .await;
                    };
                    if flow == 0 {
                        // Flow 0 witnesses the stand-off arming and declares the
                        // resume instant; the others wait on it, so the first
                        // offer cannot re-open the yield window under a sibling
                        // that is still waiting for its own gate.
                        let stop_at = Instant::now() + spec.deadline;
                        loop {
                            if gate
                                .offered_quiet_for()
                                .is_some_and(|q| q >= rtp::cc::STANDOFF_WINDOW + spec.margin)
                            {
                                resume_armed.store(true, Ordering::Relaxed);
                                break;
                            }
                            if Instant::now() >= stop_at {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        resume_started.store(
                            (base.elapsed().as_secs_f64() * 1e6) as u64,
                            Ordering::Relaxed,
                        );
                    } else {
                        while resume_started.load(Ordering::Relaxed) == 0 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                    send_timestamped_messages(write, base, MSG_BYTES, CADENCE, spec.active).await
                });
            }

            let (mut bulk_read, bulk_write) = opener.open(LaneClass::Bulk).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let mut comp_write = spawn_rtp_bulk_upload_with_options_via(
                &task_tx,
                comp_pair.client_addr(),
                false,
                rtp::CongestionLane::Dedicated,
                rtp::FrameMode::default(),
                true,
                None,
                None,
            )
            .await
            .unwrap();

            let payload = cyclic_payload(BULK_BURST_BYTES);
            let interactive = async move { join_all(futs).await };
            let bulk_payload = payload.clone();
            let bulk_fut = async move {
                if stall_bulk {
                    tokio::time::sleep(window).await;
                    return;
                }
                let mut write = bulk_write;
                if write.write_all(b"B").await.is_err() {
                    return;
                }
                m4_tcp_saturate(&mut write, &bulk_payload, window).await;
            };
            let comp_fut = async move {
                m4_tcp_saturate(&mut comp_write, &payload, window).await;
            };
            let (sent_per_flow, (), ()) = tokio::join!(interactive, bulk_fut, comp_fut);

            // The shaper's own queue is read before the pairs are stopped, at
            // the same point in the run as the latency samples. Stop the
            // sampler first so its maximum cannot pick up the post-window
            // drain.
            shaper_sampler_stop.store(true, Ordering::Relaxed);
            let shaper_max_backlog = shaper_backlog_max.load(Ordering::Relaxed);
            let shaper_backlog_sample_count = shaper_backlog_samples.load(Ordering::Relaxed);
            let shaper_mean_backlog = if shaper_backlog_sample_count == 0 {
                0
            } else {
                shaper_backlog_sum.load(Ordering::Relaxed) / shaper_backlog_sample_count
            };
            let shaper_dropped = shaper.dropped();
            let int_c2s = int_pair.stats_c2s();
            let backlog_timeline = shaper_backlog_timeline.lock().unwrap().clone();
            let bulk_rtt = bulk_rtt_timeline.lock().unwrap().clone();
            let bytes_timeline = bytes_timeline.lock().unwrap().clone();
            let resume_at = {
                let stamp = resume_started.load(Ordering::Relaxed);
                (resume.is_some() && stamp != 0).then(|| stamp as f64 / 1e6)
            };
            let resume_armed = resume_armed.load(Ordering::Relaxed);

            for (_, write) in streams.iter_mut() {
                let _ = write.shutdown();
            }

            // Drain the repair/queue stragglers before reading the summary, then
            // read both bulk counters and the interactive samples at the same
            // point in the run.
            tokio::time::sleep(GRACE).await;
            let samples = std::mem::take(&mut *collector.lock().unwrap());
            let bulk_delivered = bulk_counter.load(Ordering::Relaxed);
            let comp_delivered = comp_delivered.load(Ordering::Relaxed);
            int_pair.stop();
            bulk_pair.stop();
            comp_pair.stop();
            (
                sent_per_flow,
                samples,
                bulk_delivered,
                comp_delivered,
                shaper_max_backlog,
                shaper_mean_backlog,
                shaper_backlog_sample_count,
                shaper_dropped,
                int_c2s,
                backlog_timeline,
                bulk_rtt,
                bytes_timeline,
                resume_at,
                resume_armed,
            )
        })
        .await;
    let (
        sent_per_flow,
        samples,
        bulk_delivered,
        comp_delivered,
        shaper_max_backlog,
        shaper_mean_backlog,
        shaper_backlog_sample_count,
        shaper_dropped,
        int_c2s,
        backlog_timeline,
        bulk_rtt_timeline,
        bytes_timeline,
        resume_at,
        resume_armed,
    ) = outcome;

    // Term (a): the shaper's own queue, in ms. A message can queue behind the
    // bulk flows for at most this long; the ceiling the samples are split at
    // adds it to the flow's own measured floor, which is the firmest bound the
    // arm's own evidence supports on a message no repair touched.
    let shaper_max_queue_ms = if SHARED_UP_RATE_BPS > 0 {
        shaper_max_backlog as f64 * 8.0 * 1000.0 / SHARED_UP_RATE_BPS as f64
    } else {
        0.0
    };

    let mut per_flow: Vec<Vec<(f64, f64)>> = vec![Vec::new(); M4_FLOWS];
    for (tag, elapsed, latency) in samples.iter().copied() {
        if let Some(flow) = (0..M4_FLOWS).find(|&i| m4_flow_tag(i) == tag) {
            per_flow[flow].push((elapsed, latency));
        }
    }
    let mut flows = Vec::with_capacity(M4_FLOWS);
    for (flow, raw) in per_flow.into_iter().enumerate() {
        let sent = sent_per_flow.get(flow).copied().unwrap_or(0);
        let received = raw.len() as u64;
        let min_ms = raw
            .iter()
            .map(|(_, latency)| *latency)
            .fold(f64::INFINITY, f64::min);
        let ceiling_ms = if min_ms.is_finite() {
            min_ms + shaper_max_queue_ms
        } else {
            f64::INFINITY
        };
        let mut no_repair: Vec<f64> = Vec::new();
        let mut repaired: Vec<f64> = Vec::new();
        for (_, latency) in raw.iter().copied() {
            if latency <= ceiling_ms {
                no_repair.push(latency);
            } else {
                repaired.push(latency);
            }
        }
        let no_repair_count = no_repair.len() as u64;
        let no_repair_summary = summarize(no_repair, no_repair_count, no_repair_count, 0, 0.0);
        let repaired_count = repaired.len() as u64;
        let repaired_summary = summarize(repaired, repaired_count, repaired_count, 0, 0.0);
        let all: Vec<f64> = raw.iter().map(|(_, latency)| *latency).collect();
        let summary = summarize(all, sent, received, 0, 0.0);
        flows.push(M4TcpFlow {
            tag: m4_flow_tag(flow),
            sent,
            received,
            summary,
            min_ms,
            ceiling_ms,
            no_repair_count,
            no_repair_p50: no_repair_summary.p50,
            no_repair_p99: no_repair_summary.p99,
            repaired_count,
            repaired_p50: repaired_summary.p50,
            repaired_p99: repaired_summary.p99,
            repaired_max: repaired_summary.max,
        });
    }
    // The shaper's serialization capacity over the window, in bytes. The
    // interactive lane crosses the same queue, but the share this arm reads is
    // between the two bulk flows; only the aggregate is compared against the
    // shaper, so the reference's saturation is visible beside the share.
    let cap_bytes = (SHARED_UP_RATE_BPS as f64 / 8.0) * window.as_secs_f64();
    let agg = bulk_delivered + comp_delivered;
    let bulk_share = if agg == 0 {
        0.0
    } else {
        bulk_delivered as f64 / agg as f64
    };
    let comp_fraction = if cap_bytes > 0.0 {
        comp_delivered as f64 / cap_bytes
    } else {
        0.0
    };
    let aggregate_fraction = if cap_bytes > 0.0 {
        agg as f64 / cap_bytes
    } else {
        0.0
    };
    M4TcpRun {
        flows,
        bulk_delivered,
        comp_delivered,
        bulk_share,
        comp_fraction,
        aggregate_fraction,
        shaper_max_queue_ms,
        link: M4LinkEvidence {
            shaper_max_backlog_bytes: shaper_max_backlog,
            shaper_mean_backlog_bytes: shaper_mean_backlog,
            shaper_backlog_samples: shaper_backlog_sample_count,
            shaper_dropped,
            interactive_received: int_c2s.received,
            interactive_forwarded: int_c2s.forwarded,
            interactive_dropped: int_c2s.dropped,
        },
        repair: repair_taps.repair(),
        rung_times: repair_taps.rung_times(),
        window,
        wall: wall.elapsed(),
        samples,
        backlog_timeline,
        bulk_rtt_timeline,
        bytes_timeline,
        beta_readback,
        resume_at,
        resume_armed,
    }
}

fn print_m4_tcp_arm(run: &M4TcpRun) {
    for flow in &run.flows {
        eprintln!(
            "[m4-tcp flow {tag}] sent={sent:>5} recv={recv:>5} delivery={del:.3} \
             p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} max={max:8.1} p99_vs_ceiling={ratio:.3}",
            tag = flow.tag as char,
            sent = flow.sent,
            recv = flow.received,
            del = flow.summary.delivery_pct,
            p50 = flow.summary.p50,
            p90 = flow.summary.p90,
            p99 = flow.summary.p99,
            max = flow.summary.max,
            ratio = flow.summary.p99 / M1_CEILING_MS,
        );
    }
    // The interactive lane crosses the same queue, so its own delivered bytes
    // are the third term of the shaper's saturation: the bulk-pair aggregate
    // plus this is what shows the whole bottleneck is full, and explains why
    // the bulk+reference aggregate alone can sit below 1.0 with the shaper busy.
    let interactive_delivered: u64 = run
        .flows
        .iter()
        .map(|f| f.received.saturating_mul(MSG_BYTES as u64))
        .sum();
    let shaper_bytes = (SHARED_UP_RATE_BPS as f64 / 8.0) * run.window.as_secs_f64();
    let all_flows_fraction = if shaper_bytes > 0.0 {
        (run.bulk_delivered + run.comp_delivered + interactive_delivered) as f64 / shaper_bytes
    } else {
        0.0
    };
    eprintln!(
        "[m4-tcp] bulk_delivered={bulk} comp_delivered={comp} interactive_delivered={int} \
         bulk_share={share:.4} comp_fraction_of_shaper={comp_frac:.4} \
         aggregate_fraction_of_shaper={agg:.4} all_flows_fraction_of_shaper={all:.4} \
         shaper_bytes={cap:.0} window={window:?} wall={wall:.1}s",
        bulk = run.bulk_delivered,
        comp = run.comp_delivered,
        int = interactive_delivered,
        share = run.bulk_share,
        comp_frac = run.comp_fraction,
        agg = run.aggregate_fraction,
        all = all_flows_fraction,
        cap = shaper_bytes,
        window = run.window,
        wall = run.wall.as_secs_f64(),
    );
    // Term (a): the bottleneck's own queue, sampled while the window ran.
    eprintln!(
        "[m4-decomp shaper] max_backlog_bytes={mb} max_queue_ms={mq:.1} \
         mean_backlog_bytes={mean} backlog_samples={ns} dropped={sd} \
         interactive_received={ir} interactive_forwarded={ifw} interactive_dropped={idp}",
        mb = run.link.shaper_max_backlog_bytes,
        mq = run.shaper_max_queue_ms,
        mean = run.link.shaper_mean_backlog_bytes,
        ns = run.link.shaper_backlog_samples,
        sd = run.link.shaper_dropped,
        ir = run.link.interactive_received,
        ifw = run.link.interactive_forwarded,
        idp = run.link.interactive_dropped,
    );
    // Term (b): the interactive lane's repair activity, read from the client
    // connection's own metrics observer. The reason fields name the evidence
    // that armed each rung; the rung timeline is the `base`-clock stamp of each
    // one, and the last field is how many stamps were kept.
    let repair = run.repair;
    eprintln!(
        "[m4-decomp repair] rungs={rungs} first={first} repeat={repeat} rto={rto} \
         reorder={reorder} fast_loss={fast_loss} pre_outage={pre_outage} \
         tail_probes={tail} armor_duplicates={armor} parity_sent={parity} \
         max_write_waiters={waiters} max_in_flight={inflight} rung_times_kept={nrt}",
        rungs = repair.rungs,
        first = repair.first_attempts,
        repeat = repair.repeat_attempts,
        rto = repair.rto_reason,
        reorder = repair.reorder_reason,
        fast_loss = repair.fast_loss_reason,
        pre_outage = repair.pre_outage_reason,
        tail = repair.tail_probes,
        armor = repair.armor_duplicates,
        parity = repair.parity_sent,
        waiters = repair.max_write_waiters,
        inflight = repair.max_in_flight,
        nrt = run.rung_times.len(),
    );
    // The per-flow split at the no-loss ceiling.
    for flow in &run.flows {
        eprintln!(
            "[m4-decomp flow {tag}] min={min:6.1} ceiling={ceil:6.1} \
             no_repair n={nr:>4} p50={nrp50:6.1} p99={nrp99:6.1} \
             repaired n={rp:>4} p50={rpp50:6.1} p99={rpp99:6.1} max={rpmax:7.1} \
             repaired_share={share:.3}",
            tag = flow.tag as char,
            min = flow.min_ms,
            ceil = flow.ceiling_ms,
            nr = flow.no_repair_count,
            nrp50 = flow.no_repair_p50,
            nrp99 = flow.no_repair_p99,
            rp = flow.repaired_count,
            rpp50 = flow.repaired_p50,
            rpp99 = flow.repaired_p99,
            rpmax = flow.repaired_max,
            share = flow.repaired_count as f64 / flow.received.max(1) as f64,
        );
    }
    // The decomposition of the interactive p99. The queue's share is capped at
    // the shaper's own measured maximum because a message cannot wait longer
    // than the buffer it is queued in; the residue above `floor + queue` is the
    // part only a repair can explain. Client-side waiters are reported beside
    // it: if the send path never blocked, term (c) is not material and the
    // split stands.
    let floor_ms = run
        .flows
        .iter()
        .map(|flow| flow.min_ms)
        .fold(f64::INFINITY, f64::min);
    let p99_ms = run
        .flows
        .iter()
        .map(|flow| flow.summary.p99)
        .fold(0.0, f64::max);
    let p99_excess = (p99_ms - floor_ms).max(0.0);
    let queue_attributable = run.shaper_max_queue_ms.min(p99_excess);
    let repair_attributable = (p99_excess - queue_attributable).max(0.0);
    let dominant = if repair_attributable > queue_attributable {
        "loss-repair(b)"
    } else {
        "bottleneck-queue(a)"
    };
    let repaired_total: u64 = run.flows.iter().map(|flow| flow.repaired_count).sum();
    let received_total: u64 = run.flows.iter().map(|flow| flow.received).sum();
    eprintln!(
        "[m4-decomp total] floor_ms={floor:.1} p99_ms={p99:.1} p99_excess_ms={excess:.1} \
         shaper_queue_attributable_ms={qs:.1} repair_attributable_ms={rs:.1} \
         repaired_samples={rep}/{recv} client_waiters={waiters} client_in_flight={inflight} \
         dominant={dominant}",
        floor = floor_ms,
        p99 = p99_ms,
        excess = p99_excess,
        qs = queue_attributable,
        rs = repair_attributable,
        rep = repaired_total,
        recv = received_total,
        waiters = run.repair.max_write_waiters,
        inflight = run.repair.max_in_flight,
    );
}

// ──────────────── M4/TCP: the panels the arm contributes to M4 ────────────────

/// The M4/TCP panels, written into `M4_extra.json`/`M4_extra.csv` by
/// [`write_supplement`]. They carry the arm's two readings as the mandate's own
/// panels once `tools/mandate-check` merges the supplement into M4's evidence:
///
/// * `tcp_bulk_share` -- the product lane's share of the two bulk flows against
///   the rtp AIMD reference's, with [`M4_TCP_BULK_SHARE_FLOOR`] drawn and
///   labelled. This bound **is** asserted; the fault namespace's stall drives
///   the share to `0.000` and fails it by name.
/// * `tcp_interactive_tail` -- each interactive flow's p99 and max with M1's
///   [`M1_CEILING_MS`] drawn and labelled. The ceiling is drawn and the reading
///   stated, **not asserted**: the first measurement is `523`-`764` ms, i.e.
///   `2.1`-`3.1x` the ceiling, and a passing "guard" above it would launder a
///   real M1 breach as a pass (the workspace forbids that; see `GATE.md`). The
///   open defect is declared beside the arm.
///
/// Panel series names are prefixed `tcp_` so they collide with neither M4's
/// own run values (`clean_share_min`, `hostile_p99_guard`, ...) nor the
/// plotter's per-arm guard vocabulary; the two panels' bounds are the ones the
/// declaration's own labels name.
fn m4_tcp_declaration() -> String {
    let floor = M4_TCP_BULK_SHARE_FLOOR;
    let ceiling = M1_CEILING_MS;
    format!(
        r#"{{"mandate":"M4","title":"M4/TCP: the bulk lane's share against a loss-based competitor, and the interactive tail it costs","x_label":"flow (1..{flows})","y_label":"value","panels":[{{"id":"tcp_bulk_share","chart":"bar","x_label":"bulk pair (1)","y_label":"share of the pair's delivered bytes","series":[{{"name":"tcp_bulk"}},{{"name":"tcp_competitor"}}],"bounds":[{{"y":{floor},"label":"M4/TCP bulk-share floor {floor}"}}]}},{{"id":"tcp_interactive_tail","chart":"bar","y_label":"latency (ms)","series":[{{"name":"tcp_tail99"}},{{"name":"tcp_peak"}}],"bounds":[{{"y":{ceiling},"label":"M1 ceiling {ceiling} ms"}}]}},{{"id":"tcp_decomp","chart":"bar","x_label":"flow (1..{flows})","y_label":"latency (ms)","series":[{{"name":"tcp_no_repair_tail99"}},{{"name":"tcp_repaired_tail99"}},{{"name":"tcp_no_loss_ceiling"}}],"bounds":[{{"y":{ceiling},"label":"M1 ceiling {ceiling} ms"}}]}}]}}"#,
        flows = M4_FLOWS,
    )
}

fn m4_tcp_rows(run: &M4TcpRun) -> Vec<(String, String, f64, f64)> {
    let competitor = 1.0 - run.bulk_share;
    let mut rows = vec![
        (
            "tcp_bulk_share".to_owned(),
            "tcp_bulk".to_owned(),
            1.0,
            run.bulk_share,
        ),
        (
            "tcp_bulk_share".to_owned(),
            "tcp_competitor".to_owned(),
            1.0,
            competitor,
        ),
    ];
    for (index, flow) in run.flows.iter().enumerate() {
        let x = (index + 1) as f64;
        rows.push((
            "tcp_interactive_tail".to_owned(),
            "tcp_tail99".to_owned(),
            x,
            flow.summary.p99,
        ));
        rows.push((
            "tcp_interactive_tail".to_owned(),
            "tcp_peak".to_owned(),
            x,
            flow.summary.max,
        ));
        rows.push((
            "tcp_decomp".to_owned(),
            "tcp_no_repair_tail99".to_owned(),
            x,
            flow.no_repair_p99,
        ));
        rows.push((
            "tcp_decomp".to_owned(),
            "tcp_repaired_tail99".to_owned(),
            x,
            flow.repaired_p99,
        ));
        rows.push((
            "tcp_decomp".to_owned(),
            "tcp_no_loss_ceiling".to_owned(),
            x,
            flow.ceiling_ms,
        ));
    }
    rows
}

/// Write one mandate's **supplement**: an opt-in arm's panels beside the
/// mandate's own evidence (`{mandate}_extra.json`/`.csv`), the pair
/// `tools/mandate-check` merges into `{mandate}.json`/`.csv` before it renders
/// the mandate. The supplement is a separate file pair rather than an append to
/// `{mandate}.json` because the base evidence is written by a different test in
/// the same binary and neither may clobber the other.
fn write_supplement(
    dir: &Path,
    mandate: &str,
    declaration: &str,
    rows: &[(String, String, f64, f64)],
) {
    std::fs::create_dir_all(dir)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot create evidence directory {dir:?}: {e}"));
    std::fs::write(dir.join(format!("{mandate}_extra.json")), declaration)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot write supplement declaration: {e}"));
    let mut csv = String::from("panel,series,x,y\n");
    for (panel, series, x, y) in rows {
        csv.push_str(&format!("{panel},{series},{x:.6},{y:.6}\n"));
    }
    std::fs::write(dir.join(format!("{mandate}_extra.csv")), csv)
        .unwrap_or_else(|e| panic!("[{mandate}] cannot write supplement data CSV: {e}"));
    eprintln!("[mandate-smoke] wrote {mandate}_extra.json + {mandate}_extra.csv into {dir:?}");
}

/// M4's missing half: the production bulk lane competing with a loss-based
/// (TCP-family) flow for one bottleneck's queue, and the interactive lane's tail
/// while it does.
///
/// The deliverable is the printed pair of readings: the bulk lane's share
/// against the reference (with the reference's saturation made visible so the
/// share is meaningful), and each interactive flow's p99/max against M1's
/// [`M1_CEILING_MS`]. It asserts the instrument's own sanity -- a positive
/// offered count on every interactive flow, non-empty samples, both bulk flows
/// present, and the reference pair saturating the shaper -- **and** one product
/// bound, the bulk lane's share against [`M4_TCP_BULK_SHARE_FLOOR`], derived
/// from this arm's first measurement. The interactive p99 is **reported, not
/// asserted**: the first measurement is `523`-`764` ms, `2.1`-`3.1x` the
/// ceiling, and a passing guard there would launder a real M1 breach as a pass.
/// The ceiling is drawn on the `tcp_interactive_tail` panel and the breach is
/// declared as an open defect in `GATE.md` instead.
///
/// The arm also **decomposes** that tail, from its own instruments, into the
/// three additive terms a sender can or cannot move:
///
/// * **(a) bottleneck queueing** -- the shared shaper's own backlog, sampled
///   while the window runs; its maximum, at [`SHARED_UP_RATE_BPS`], is the
///   longest a message can wait in the buffer, and is drawn beside the arm's
///   tail panels on `tcp_decomp` as `tcp_no_loss_ceiling`;
/// * **(b) loss repair** -- an rtp metrics observer on the interactive lane's
///   client connection counts the repair rungs and their reasons, and each
///   flow's samples are split at the no-loss ceiling
///   (`flow_min_latency + shaper_max_queue_ms`): samples above it cannot be
///   propagation plus the bottleneck's queue, so their excess is a repair;
/// * **(c) client-side queueing** -- the connection's own
///   `application_write_waiters`/`in_flight_packets`, sampled by the same
///   observer. If the send path never blocked, no message waited in the
///   client's egress.
///
/// The floor of the split is the flow's own minimum latency and the queue term
/// is the shaper's own measured maximum, so the split rests on two measurements,
/// not on a constant. The `tcp_decomp` panel draws the no-repair and repaired
/// p99 beside that ceiling; when repair dominates, `bottleneck-queue(a)` versus
/// `loss-repair(b)` is printed on the `[m4-decomp total]` line. The interactive
/// p99 itself remains **unasserted** -- its first reading is still 2.2-2.8x the
/// ceiling, and the decomposition is evidence about the defect, not a licence to
/// assert it.
///
/// Vacuity: `MANDATE_SMOKE_FAULT=M4_TCP_STALL_BULK` stalls the mux bulk lane for
/// the whole window, so its delivered-byte counter stays zero and the
/// bulk-share floor fails by name (`bulk_share=0.0000` against the `0.17`
/// floor) while the rest of the reading still prints.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "asserts the bulk-share floor, reports the interactive tail: the bulk lane vs an rtp AIMD reference on one shared shaper; ~20 s; run with --ignored --nocapture"]
async fn m4_tcp_competition() {
    let _serial = SERIAL.lock().await;
    let run = with_timeout(ARM_DEADLINE, "m4/tcp", run_m4_tcp_arm(WINDOW)).await;
    print_m4_tcp_arm(&run);
    write_supplement(&out_dir(), "M4", &m4_tcp_declaration(), &m4_tcp_rows(&run));
    let samples: u64 = run.flows.iter().map(|f| f.summary.received).sum();
    let delivered_min = run
        .flows
        .iter()
        .map(|f| f.summary.received)
        .fold(u64::MAX, u64::min);
    let sent_min = run.flows.iter().map(|f| f.sent).fold(u64::MAX, u64::min);
    let p99_max = run.flows.iter().map(|f| f.summary.p99).fold(0.0, f64::max);
    let max_max = run.flows.iter().map(|f| f.summary.max).fold(0.0, f64::max);
    let pass = samples > 0
        && delivered_min > 0
        && sent_min > 0
        && run.bulk_delivered > 0
        && run.comp_delivered > 0
        && run.aggregate_fraction >= M4_TCP_SATURATION_FLOOR;
    // Deliberately not a `MANDATE` line and not a `[mandate-smoke …]` arm row:
    // `tools/mandate-check` owns the M1-M4 id set and attributes arm rows to the
    // mandate whose `MANDATE` line follows them, so a row printed here would be
    // read as M4's own measurement. This arm's verdict is its own line and its
    // own exit status.
    println!(
        "[m4-tcp] {} flows={} samples={} bulk_share={:.4} comp_delivered={} \
         comp_fraction_of_shaper={:.4} aggregate_fraction_of_shaper={:.4} \
         saturation_floor={:.2} bulk_delivered={} p99_max={:.1} max_max={:.1} \
         ceiling={:.1} window_s={:.1} wall_s={:.1}",
        verdict(pass),
        M4_FLOWS,
        samples,
        run.bulk_share,
        run.comp_delivered,
        run.comp_fraction,
        run.aggregate_fraction,
        M4_TCP_SATURATION_FLOOR,
        run.bulk_delivered,
        p99_max,
        max_max,
        M1_CEILING_MS,
        run.window.as_secs_f64(),
        run.wall.as_secs_f64(),
    );
    assert!(
        sent_min > 0 && samples > 0 && delivered_min > 0,
        "[m4-tcp] the arm measured {samples} delivered interactive sample(s) with per-flow sent {sent_min}.. and delivered min {delivered_min}: an interactive lane with no offer, no samples or a flow that delivered nothing is an instrument failure, not a reading",
    );
    assert!(
        run.comp_delivered > 0,
        "[m4-tcp] the rtp AIMD reference delivered {} bytes over the window: the loss-based competitor this arm measures the bulk lane against was not on the link",
        run.comp_delivered,
    );
    assert!(
        run.bulk_share >= M4_TCP_BULK_SHARE_FLOOR,
        "[m4-tcp] the product bulk lane's share of the two bulk flows' delivered \
         bytes is {:.4}, below the {M4_TCP_BULK_SHARE_FLOOR:.2} bulk-share floor \
         (M4_TCP_BULK_SHARE_FLOOR, derived from the first measurement 0.349; the \
         mux bulk lane delivered {} B against the reference's {} B): the lane \
         competing for the bottleneck is not getting its share, and \
         `MANDATE_SMOKE_FAULT=M4_TCP_STALL_BULK` drives exactly this reading to \
         0.000",
        run.bulk_share,
        run.bulk_delivered,
        run.comp_delivered,
    );
    assert!(
        run.bulk_delivered > 0,
        "[m4-tcp] the mux bulk lane delivered {} bytes over the window, so the bulk-vs-reference share is undefined (the lane is not competing; `MANDATE_SMOKE_FAULT=M4_TCP_STALL_BULK` produces exactly this): the arm's own bulk-presence sanity would fail here while the competitor and the interactive flows still print",
        run.bulk_delivered,
    );
    assert!(
        run.aggregate_fraction >= M4_TCP_SATURATION_FLOOR,
        "[m4-tcp] the two bulk flows together delivered {:.0} B = {:.4} of the {:.0} B shaper capacity, below the {M4_TCP_SATURATION_FLOOR:.2} saturation floor: the reference is not contesting the link, so the bulk share {:.4} cannot be read as a comparison against a competent loss-based competitor",
        (run.bulk_delivered + run.comp_delivered) as f64,
        run.aggregate_fraction,
        (SHARED_UP_RATE_BPS as f64 / 8.0) * run.window.as_secs_f64(),
        run.bulk_share,
    );
    // The decomposition's own instruments must have measured something, or a
    // zero would read as "no queue, no repair". The backlog sampler ran, the
    // interactive link counters were read, and -- unless the bottleneck never
    // dropped a datagram, in which case no repair is owed -- the repair observer
    // saw a rung fire. Instrument sanity, not product bounds: the interactive
    // tail itself stays reported and unasserted.
    assert!(
        run.link.shaper_backlog_samples > 0,
        "[m4-tcp] the shaper-backlog sampler took {} sample(s): a zero backlog series would make the queueing term (a) read as zero whether or not the bottleneck queued",
        run.link.shaper_backlog_samples,
    );
    assert!(
        run.link.interactive_received > 0,
        "[m4-tcp] the interactive lane's c2s link received {} datagram(s): the link counters the decomposition reads were not wired",
        run.link.interactive_received,
    );
    assert!(
        run.repair.rungs > 0 || run.link.shaper_dropped == 0,
        "[m4-tcp] the interactive repair observer saw {} rung(s) while the shared shaper tail-dropped {} datagram(s): drops occurred but no repair was observed, so the repair term (b) would read as zero from a detached observer",
        run.repair.rungs,
        run.link.shaper_dropped,
    );
}

// ─────── M4/TCP + stand-off beta + a resuming interactive lane (the composite) ───────
//
// Requirement (A)'s lever is the bulk stand-off's per-path decrease factor
// (`rtp::cc::STANDOFF_DECREASE_FACTOR`), and `standoff_beta_sweep` measured that
// a gentler factor takes a larger share of a shared bottleneck from the AIMD
// reference -- at the cost of a deeper standing queue (114.2 ms of the buffer's
// 125.0 ms at beta 0.9). That queue is on the SAME drop-tail buffer the
// interactive lane must cross, and requirement (B) is "no latency drop on the
// interactive lane". The arms that currently read (B) have no competitor, so
// the stand-off never arms and they pass by construction.
//
// This arm is the composite: `m4_tcp_competition`'s topology (the production
// dual-lane mux session -- `M4_FLOWS` interactive flows, a saturating mux bulk
// lane, and an rtp AIMD reference on one shared `BottleneckShaper`) with the
// stand-off's per-path factor set to `beta` through the existing testing hook,
// and the interactive lane held quiet until the stand-off has been **armed for
// `M4_RESUME_MARGIN` past `STANDOFF_WINDOW`** and then offering at the
// production cadence. That is the only shape in which the lever is reachable at
// all: the gate that arms the stand-off is the interactive lane's own offer
// clock, so a lane offering continuously keeps it disarmed and `beta` inert.
// (B)'s reading here is therefore necessarily a **resume**.

/// The betas this composite sweeps. `0.5` is the control: the stand-off and the
/// AIMD reference then run the *same* law, so a fair split is the only possible
/// reading and the sweep's deltas are against an unbiased control. The default
/// the product ships is also `0.5`, so the control is the shipped behaviour.
const M4_STANDOFF_BETAS: [f64; 4] = [0.5, 0.75, 0.9, 1.0];
/// Interleaved reps: every rep runs all four betas, order alternated by rep, so
/// a slow drift is common mode across the cells rather than attributed to beta.
const M4_STANDOFF_REPS: usize = 8;
/// How long the offer clock must have been quiet past `STANDOFF_WINDOW` before
/// the lane resumes: `STANDOFF_WINDOW` (1.5 s) plus this margin, so the
/// competing episode's AIMD ramp and queue depth are at their steady state when
/// the lane starts. `standoff_beta_sweep` measures its tail from 4 s for the
/// same reason; `1.5 + 2.5 = 4.0` s is the same instant on this clock.
const M4_RESUME_MARGIN: Duration = Duration::from_millis(2500);
/// How long the resumed lane offers. The window is `WINDOW` (12 s) and the
/// resume lands near 4 s, so this leaves slack before the window closes; at
/// `M4_FLOWS` flows and a 5 ms cadence it yields ~5 600 interactive samples.
const M4_RESUME_ACTIVE: Duration = Duration::from_millis(7000);
/// The composite's window. Longer than [`WINDOW`] by 2 s so the resumed active
/// phase (`resume_at + M4_RESUME_ACTIVE`, with `resume_at` near 5.5 s on the
/// measured gate) always closes *before* the two saturating bulk flows stop -- a
/// tail that ran past the window's end would be measured without competition
/// and would launder the very cost this arm exists to read.
const M4_RESUME_WINDOW: Duration = Duration::from_secs(14);
/// The bounded wait for the stand-off to arm. Expiring without arming leaves
/// `resume_armed == false` and fails the arm by name: a window in which the
/// stand-off never competed cannot be read as the lever's cost.
const M4_RESUME_DEADLINE: Duration = Duration::from_secs(9);
/// How many of each flow's first samples after the resume the concentrated
/// reading takes. `standoff_burst` uses 8; the same count keeps the two arms'
/// resume readings comparable.
const M4_RESUME_FIRST_N: usize = 8;
/// The sub-window before the resume the quiet-phase share, RTT and backlog are
/// integrated over: the last 3 s of the competing episode, so the reading is
/// the steady state rather than the AIMD ramp.
const M4_RESUME_QUIET_LOOKBACK: f64 = 3.0;

/// One window's resume-cell readings.
#[derive(Clone, Copy)]
struct M4ResumeCell {
    beta: f64,
    resume_at: f64,
    resume_armed: bool,
    beta_readback: f64,
    /// Pooled interactive tail over the resumed phase `[resume_at, resume_at +
    /// M4_RESUME_ACTIVE)`, across all `M4_FLOWS` flows.
    active_n: u64,
    active_p50: f64,
    active_p90: f64,
    active_p99: f64,
    active_p999: f64,
    active_max: f64,
    active_over250: usize,
    sent: u64,
    received: u64,
    /// The concentrated resume reading: the max and p50 over each flow's first
    /// [`M4_RESUME_FIRST_N`] samples after the resume (pooled), and the pooled
    /// **minimum** of those first samples -- the best-case crossing of the queue
    /// the competing episode built, which a repair cannot inflate.
    first_max: f64,
    first_p50: f64,
    first_min: f64,
    first_n: u64,
    /// The **very first** sample of each flow after the resume (pooled): the
    /// first message across the standing queue.
    lead_p50: f64,
    lead_max: f64,
    /// Our bulk lane's share of the two bulk flows' delivered bytes over the
    /// last `M4_RESUME_QUIET_LOOKBACK` s before the resume, and over the whole
    /// window.
    quiet_share: f64,
    quiet_our: u64,
    quiet_comp: u64,
    whole_share: f64,
    /// The shared shaper's backlog over the quiet lookback and over the resumed
    /// phase, in ms at `SHARED_UP_RATE_BPS` (a full 128 KiB buffer is 125.0 ms).
    quiet_backlog_mean_ms: f64,
    quiet_backlog_max_ms: f64,
    active_backlog_mean_ms: f64,
    /// The product bulk lane's own `congestion_control_rtt` over the quiet
    /// lookback: mean and p95, in ms.
    rtt_mean_ms: f64,
    rtt_p95_ms: f64,
}

fn m4_resume_mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn m4_resume_sd(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return f64::NAN;
    }
    let m = m4_resume_mean(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt()
}

/// The delivered-byte increment of both bulk counters over `[from, to)`, from
/// the arm's own sampled byte timeline (midpoint rule, so a partial interval at
/// either edge is not counted).
fn m4_resume_bytes(timeline: &[(f64, u64, u64)], from: f64, to: f64) -> (u64, u64, f64) {
    let mut our = 0u64;
    let mut comp = 0u64;
    let mut secs = 0.0f64;
    for pair in timeline.windows(2) {
        let mid = 0.5 * (pair[0].0 + pair[1].0);
        if mid >= from && mid < to {
            our += pair[1].1.saturating_sub(pair[0].1);
            comp += pair[1].2.saturating_sub(pair[0].2);
            secs += pair[1].0 - pair[0].0;
        }
    }
    (our, comp, secs)
}

/// The shaper backlog over `[from, to)`: mean and max in bytes and the sample
/// count, so a zero is distinguishable from an absent series.
fn m4_resume_backlog(timeline: &[(f64, u64)], from: f64, to: f64) -> (f64, f64, u64) {
    let xs: Vec<u64> = timeline
        .iter()
        .filter(|(t, _)| *t >= from && *t < to)
        .map(|(_, b)| *b)
        .collect();
    if xs.is_empty() {
        return (f64::NAN, f64::NAN, 0);
    }
    let mean = xs.iter().sum::<u64>() as f64 / xs.len() as f64;
    (mean, *xs.iter().max().unwrap() as f64, xs.len() as u64)
}

/// The bulk lane's own control RTT over `[from, to)`: mean, p95 and count.
fn m4_resume_rtt(timeline: &[(f64, f64)], from: f64, to: f64) -> (f64, f64, u64) {
    let mut xs: Vec<f64> = timeline
        .iter()
        .filter(|(t, _)| *t >= from && *t < to)
        .map(|(_, r)| *r)
        .filter(|r| r.is_finite())
        .collect();
    if xs.is_empty() {
        return (f64::NAN, f64::NAN, 0);
    }
    let n = xs.len() as u64;
    let mean = xs.iter().sum::<f64>() / n as f64;
    xs.sort_by(|a, b| a.total_cmp(b));
    let idx = (((n as f64) * 0.95) as usize).min(xs.len() - 1);
    (mean, xs[idx], n)
}

fn m4_resume_cell(run: &M4TcpRun, beta: f64) -> M4ResumeCell {
    let resume_at = run
        .resume_at
        .expect("the resume arm must record the instant the lane resumed");
    let active_to = resume_at + M4_RESUME_ACTIVE.as_secs_f64() + 0.5;
    let quiet_from = (resume_at - M4_RESUME_QUIET_LOOKBACK).max(0.0);

    let active: Vec<f64> = run
        .samples
        .iter()
        .filter(|(_, t, _)| *t >= resume_at && *t < active_to)
        .map(|(_, _, l)| *l)
        .collect();
    let sent: u64 = run.flows.iter().map(|f| f.sent).sum();
    let received = active.len() as u64;
    let active_summary = summarize(active.clone(), sent, received, 0, 0.0);

    // The concentrated reading: per flow, the first N samples at or after the
    // resume (in arrival order), pooled. This is the window in which the queue
    // the competing episode built is crossed, before a repair can mask it.
    let mut first: Vec<f64> = Vec::new();
    let mut lead: Vec<f64> = Vec::new();
    for flow in 0..M4_FLOWS {
        let tag = m4_flow_tag(flow);
        let mut flow_samples: Vec<(f64, f64)> = run
            .samples
            .iter()
            .filter(|(t, time, _)| *t == tag && *time >= resume_at)
            .map(|(_, time, latency)| (*time, *latency))
            .collect();
        flow_samples.sort_by(|a, b| a.0.total_cmp(&b.0));
        first.extend(flow_samples.iter().take(M4_RESUME_FIRST_N).map(|(_, l)| *l));
        if let Some((_, latency)) = flow_samples.first() {
            lead.push(*latency);
        }
    }
    let first_summary = summarize(
        first.clone(),
        first.len() as u64,
        first.len() as u64,
        0,
        0.0,
    );
    let lead_summary = summarize(lead.clone(), lead.len() as u64, lead.len() as u64, 0, 0.0);
    let first_min = first.iter().copied().fold(f64::INFINITY, f64::min);

    let (quiet_our, quiet_comp, _) = m4_resume_bytes(&run.bytes_timeline, quiet_from, resume_at);
    let quiet_total = quiet_our + quiet_comp;
    let (whole_our, whole_comp, _) = m4_resume_bytes(&run.bytes_timeline, 0.0, active_to);
    let whole_total = whole_our + whole_comp;
    let (q_mean_b, q_max_b, _) = m4_resume_backlog(&run.backlog_timeline, quiet_from, resume_at);
    let (a_mean_b, _, _) = m4_resume_backlog(&run.backlog_timeline, resume_at, active_to);
    let (rtt_mean, rtt_p95, _) = m4_resume_rtt(&run.bulk_rtt_timeline, quiet_from, resume_at);
    let ms_per_byte = 8.0 * 1000.0 / SHARED_UP_RATE_BPS as f64;

    M4ResumeCell {
        beta,
        resume_at,
        resume_armed: run.resume_armed,
        beta_readback: run.beta_readback.unwrap_or(f64::NAN),
        active_n: received,
        active_p50: active_summary.p50,
        active_p90: active_summary.p90,
        active_p99: active_summary.p99,
        active_p999: active_summary.p999,
        active_max: active_summary.max,
        active_over250: over250_count(&active),
        sent,
        received,
        first_max: first_summary.max,
        first_p50: first_summary.p50,
        first_min,
        first_n: first.len() as u64,
        lead_p50: lead_summary.p50,
        lead_max: lead_summary.max,
        quiet_share: if quiet_total == 0 {
            f64::NAN
        } else {
            quiet_our as f64 / quiet_total as f64
        },
        quiet_our,
        quiet_comp,
        whole_share: if whole_total == 0 {
            f64::NAN
        } else {
            whole_our as f64 / whole_total as f64
        },
        quiet_backlog_mean_ms: q_mean_b * ms_per_byte,
        quiet_backlog_max_ms: q_max_b * ms_per_byte,
        active_backlog_mean_ms: a_mean_b * ms_per_byte,
        rtt_mean_ms: rtt_mean,
        rtt_p95_ms: rtt_p95,
    }
}

/// The composite: beta's share gain (requirement (A)) against the interactive
/// lane's resumed tail (requirement (B)).
///
/// The **verdict** is the reading the arm exists to produce: if a beta above the
/// control takes share (the quiet-phase share clears the control's) **and** the
/// interactive lane's resumed p99 stays inside the control's own rep-to-rep
/// spread, the lever is a candidate to ship; if the tail degrades beyond that
/// spread, that is a mandate trade and this arm states which mandate gains and
/// which loses, by how much -- and a rise in an impaired arm's p99 IS an M1
/// regression however comfortably it sits inside a guard.
///
/// Vacuity is by the arm's own fault hook and the hub readback: hardcoding the
/// per-path factor to the default fails `beta_readback == beta`, and
/// `MANDATE_SMOKE_FAULT=M4_BETA_RESUME_OFFER_NOW` zeroes the gate's margin and
/// deadline so the lane offers immediately -- the stand-off then never arms and
/// `resume_armed` fails by name while every other reading still prints.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "the beta lever vs the interactive resume tail: 8 interleaved reps x 4 betas x ~14 s; run with --ignored --nocapture --test-threads=1"]
async fn m4_tcp_standoff_beta_resume() {
    let _serial = SERIAL.lock().await;
    let offer_now = std::env::var("MANDATE_SMOKE_FAULT")
        .is_ok_and(|value| value.trim() == "M4_BETA_RESUME_OFFER_NOW");
    let dir = out_dir().join("standoff-beta-resume");
    std::fs::create_dir_all(&dir).unwrap();
    let mut cells: Vec<(usize, M4ResumeCell)> = Vec::new();
    let mut cells_csv = String::from(
        "rep,beta,resume_at,resume_armed,beta_readback,active_n,active_p50,active_p90,active_p99,\
         active_p999,active_max,active_over250,sent,received,first_n,first_max,first_p50,\
         first_min,lead_p50,lead_max,\
         quiet_share,quiet_our,quiet_comp,whole_share,quiet_backlog_mean_ms,quiet_backlog_max_ms,\
         active_backlog_mean_ms,rtt_mean_ms,rtt_p95_ms\n",
    );
    for rep in 0..M4_STANDOFF_REPS {
        let mut order: Vec<f64> = M4_STANDOFF_BETAS.to_vec();
        if rep % 2 == 1 {
            order.reverse();
        }
        for beta in order {
            let spec = M4ResumeSpec {
                beta,
                margin: if offer_now {
                    Duration::ZERO
                } else {
                    M4_RESUME_MARGIN
                },
                active: M4_RESUME_ACTIVE,
                deadline: if offer_now {
                    Duration::ZERO
                } else {
                    M4_RESUME_DEADLINE
                },
            };
            let run = with_timeout(
                ARM_DEADLINE * 3,
                "m4/beta-resume window",
                run_m4_tcp_arm_with(M4_RESUME_WINDOW, Some(spec)),
            )
            .await;
            let cell = m4_resume_cell(&run, beta);
            eprintln!(
                "[m4-beta-resume rep{rep}] beta {beta:.2}  resume_at {:.2}s armed {}  \
                 active n {} p50 {:7.1} p90 {:7.1} p99 {:7.1} p999 {:7.1} max {:8.1} over250 {}  \
                 first n {} p50 {:7.1} min {:7.1} max {:8.1}  lead p50 {:7.1} max {:7.1}  quiet share {:6.4} ({} / {} B)  whole {:6.4}  \
                 backlog quiet {:6.1}/{:6.1} ms active {:6.1} ms  rtt mean {:6.1} p95 {:6.1} ms  \
                 readback {:.2}",
                cell.resume_at,
                cell.resume_armed,
                cell.active_n,
                cell.active_p50,
                cell.active_p90,
                cell.active_p99,
                cell.active_p999,
                cell.active_max,
                cell.active_over250,
                cell.first_n,
                cell.first_p50,
                cell.first_min,
                cell.first_max,
                cell.lead_p50,
                cell.lead_max,
                cell.quiet_share,
                cell.quiet_our,
                cell.quiet_comp,
                cell.whole_share,
                cell.quiet_backlog_mean_ms,
                cell.quiet_backlog_max_ms,
                cell.active_backlog_mean_ms,
                cell.rtt_mean_ms,
                cell.rtt_p95_ms,
                cell.beta_readback,
            );
            cells_csv.push_str(&format!(
                "{rep},{beta:.2},{:.3},{},{:.2},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.6},{},{},{:.6},{:.3},{:.3},{:.3},{:.3},{:.3}\n",
                cell.resume_at,
                cell.resume_armed,
                cell.beta_readback,
                cell.active_n,
                cell.active_p50,
                cell.active_p90,
                cell.active_p99,
                cell.active_p999,
                cell.active_max,
                cell.active_over250,
                cell.sent,
                cell.received,
                cell.first_n,
                cell.first_max,
                cell.first_p50,
                cell.first_min,
                cell.lead_p50,
                cell.lead_max,
                cell.quiet_share,
                cell.quiet_our,
                cell.quiet_comp,
                cell.whole_share,
                cell.quiet_backlog_mean_ms,
                cell.quiet_backlog_max_ms,
                cell.active_backlog_mean_ms,
                cell.rtt_mean_ms,
                cell.rtt_p95_ms,
            ));
            cells.push((rep, cell));
        }
    }
    std::fs::write(dir.join("cells.csv"), &cells_csv).unwrap();

    // The control is the shipped default (`0.5`). Its interactive p99's own
    // rep-to-rep spread is the band a beta above it may move inside without
    // being an effect.
    let control_p99: Vec<f64> = cells
        .iter()
        .filter(|(_, c)| c.beta == M4_STANDOFF_BETAS[0])
        .map(|(_, c)| c.active_p99)
        .collect();
    let control_share: Vec<f64> = cells
        .iter()
        .filter(|(_, c)| c.beta == M4_STANDOFF_BETAS[0])
        .map(|(_, c)| c.quiet_share)
        .collect();
    let control_p99_mean = m4_resume_mean(&control_p99);
    let control_p99_sd = m4_resume_sd(&control_p99);
    let control_p99_lo = control_p99.iter().copied().fold(f64::INFINITY, f64::min);
    let control_p99_hi = control_p99.iter().copied().fold(0.0f64, f64::max);
    let control_share_mean = m4_resume_mean(&control_share);
    eprintln!(
        "[m4-beta-resume control] beta 0.50 interactive p99 mean {control_p99_mean:.1} ms sd \
         {control_p99_sd:.1} ms span [{control_p99_lo:.1}, {control_p99_hi:.1}] ms; quiet share \
         mean {control_share_mean:.4}"
    );

    let mut summary = String::from(
        "beta,n,resume_at_mean,quiet_share_mean,quiet_share_paired_delta,ci95_lo,ci95_hi,\
         active_p50,active_p90,active_p99,active_p999,active_max,active_over250,first_max,\
         first_min,lead_p50,lead_max,\
         first_p50,whole_share,quiet_backlog_mean_ms,quiet_backlog_max_ms,active_backlog_mean_ms,\
         rtt_mean_ms,p99_delta_vs_control\n",
    );
    for beta in M4_STANDOFF_BETAS {
        let cs: Vec<&M4ResumeCell> = cells
            .iter()
            .filter(|(_, c)| c.beta == beta)
            .map(|(_, c)| c)
            .collect();
        let share: Vec<f64> = cs.iter().map(|c| c.quiet_share).collect();
        let p99: Vec<f64> = cs.iter().map(|c| c.active_p99).collect();
        let paired: Vec<f64> = (0..M4_STANDOFF_REPS)
            .filter_map(|rep| {
                let c0 = cells
                    .iter()
                    .find(|(r, c)| *r == rep && c.beta == M4_STANDOFF_BETAS[0])
                    .map(|(_, c)| c.quiet_share)?;
                let cb = cells
                    .iter()
                    .find(|(r, c)| *r == rep && c.beta == beta)
                    .map(|(_, c)| c.quiet_share)?;
                Some(cb - c0)
            })
            .collect();
        let d_mean = m4_resume_mean(&paired);
        let sem = m4_resume_sd(&paired) / (paired.len() as f64).sqrt();
        let (ci_lo, ci_hi) = if sem.is_finite() {
            (d_mean - 1.96 * sem, d_mean + 1.96 * sem)
        } else {
            (f64::NAN, f64::NAN)
        };
        let share_mean = m4_resume_mean(&share);
        let p99_mean = m4_resume_mean(&p99);
        let p99_delta = p99_mean - control_p99_mean;
        eprintln!(
            "[m4-beta-resume] beta {beta:.2}  n {}  quiet share {:6.4} (paired {:+7.4} CI [{:+.4},{:+.4}])\
              p50 {:7.1} p90 {:7.1} p99 {:7.1} p999 {:8.1} max {:8.1} over250 sum {}  \
             first p50 {:7.1} min {:7.1} max {:8.1}  lead p50 {:7.1} max {:7.1}  whole {:6.4}  backlog q {:6.1}/{:6.1} a {:6.1} ms  \
             rtt {:6.1} ms  p99 delta vs control {:+7.1} ms",
            cs.len(),
            share_mean,
            d_mean,
            ci_lo,
            ci_hi,
            m4_resume_mean(&cs.iter().map(|c| c.active_p50).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.active_p90).collect::<Vec<f64>>()),
            p99_mean,
            m4_resume_mean(&cs.iter().map(|c| c.active_p999).collect::<Vec<f64>>()),
            cs.iter().map(|c| c.active_max).fold(0.0f64, f64::max),
            cs.iter().map(|c| c.active_over250).sum::<usize>(),
            m4_resume_mean(&cs.iter().map(|c| c.first_p50).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.first_min).collect::<Vec<f64>>()),
            cs.iter().map(|c| c.first_max).fold(0.0f64, f64::max),
            m4_resume_mean(&cs.iter().map(|c| c.lead_p50).collect::<Vec<f64>>()),
            cs.iter().map(|c| c.lead_max).fold(0.0f64, f64::max),
            m4_resume_mean(&cs.iter().map(|c| c.whole_share).collect::<Vec<f64>>()),
            m4_resume_mean(
                &cs.iter()
                    .map(|c| c.quiet_backlog_mean_ms)
                    .collect::<Vec<f64>>()
            ),
            m4_resume_mean(
                &cs.iter()
                    .map(|c| c.quiet_backlog_max_ms)
                    .collect::<Vec<f64>>()
            ),
            m4_resume_mean(
                &cs.iter()
                    .map(|c| c.active_backlog_mean_ms)
                    .collect::<Vec<f64>>()
            ),
            m4_resume_mean(&cs.iter().map(|c| c.rtt_mean_ms).collect::<Vec<f64>>()),
            p99_delta,
        );
        summary.push_str(&format!(
            "{beta:.2},{},{:.3},{share_mean:.6},{d_mean:.6},{ci_lo:.6},{ci_hi:.6},{:.3},{:.3},\
             {p99_mean:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.6},{:.3},{:.3},{:.3},{:.3},{p99_delta:.3}\n",
            cs.len(),
            m4_resume_mean(&cs.iter().map(|c| c.resume_at).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.active_p50).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.active_p90).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.active_p999).collect::<Vec<f64>>()),
            cs.iter().map(|c| c.active_max).fold(0.0f64, f64::max),
            cs.iter().map(|c| c.active_over250).sum::<usize>(),
            m4_resume_mean(&cs.iter().map(|c| c.first_max).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.first_min).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.lead_p50).collect::<Vec<f64>>()),
            cs.iter().map(|c| c.lead_max).fold(0.0f64, f64::max),
            m4_resume_mean(&cs.iter().map(|c| c.first_p50).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.whole_share).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.quiet_backlog_mean_ms).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.quiet_backlog_max_ms).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.active_backlog_mean_ms).collect::<Vec<f64>>()),
            m4_resume_mean(&cs.iter().map(|c| c.rtt_mean_ms).collect::<Vec<f64>>()),
        ));
    }
    std::fs::write(dir.join("summary.csv"), &summary).unwrap();
    eprintln!("[m4-beta-resume] data: {}", dir.display());

    // ---- assertions: instrument sanity, not the lever's direction ----------
    for (rep, cell) in &cells {
        assert!(
            cell.resume_armed,
            "[m4-beta-resume] rep{rep} beta {:.2}: the bulk stand-off was never armed (the \
             interactive lane's offer clock never stayed quiet past STANDOFF_WINDOW + {:?}) \
             before the resume, so this window measured the shipped delay-first policy rather \
             than the competing episode the lever sets and cannot be read as the lever's cost \
             (MANDATE_SMOKE_FAULT=M4_BETA_RESUME_OFFER_NOW produces exactly this)",
            cell.beta, M4_RESUME_MARGIN,
        );
        assert!(
            (cell.beta_readback - cell.beta).abs() < 1e-9,
            "[m4-beta-resume] rep{rep} beta {:.2}: the hub read back {:.2} -- the arm did not \
             apply the per-path factor it labelled the cell with, so a flat sweep would be a \
             property of the arm and not of the lever",
            cell.beta,
            cell.beta_readback,
        );
        assert!(
            cell.sent > 0 && cell.received > 0 && cell.active_n > 0,
            "[m4-beta-resume] rep{rep} beta {:.2}: sent {} / received {} interactive message(s) \
             over the resumed phase -- an interactive lane that offered nothing after its resume \
             is an instrument failure, not a reading",
            cell.beta,
            cell.sent,
            cell.received,
        );
        assert!(
            cell.quiet_our > 0 && cell.quiet_comp > 0,
            "[m4-beta-resume] rep{rep} beta {:.2}: our bulk delivered {} B and the competitor \
             {} B over the quiet lookback -- one bulk flow was absent, so the quiet-phase share \
             is undefined",
            cell.beta,
            cell.quiet_our,
            cell.quiet_comp,
        );
        assert!(
            cell.first_n > 0,
            "[m4-beta-resume] rep{rep} beta {:.2}: the resumed phase yielded no samples for the \
             concentrated first-N reading",
            cell.beta,
        );
    }
    assert!(
        !control_share.is_empty(),
        "[m4-beta-resume] the control cells are missing"
    );
}

// ────────── the hostile tail's decomposition: one dimension per arm ──────────
//
// The hostile arm's p99 is a composite of mechanisms and its own one number
// cannot say which of them it is: the link's own one-way jitter, the
// transport's repair of what the GE model drops, and the lane's serialization
// of a competing bulk burst. This probe measures **one arm per dimension** from
// a stated baseline -- the M1 hostile arm itself, taken from [`mandate_arms`]
// rather than restated -- so each reading attributes to the dimension it moves.
// Two further arms are composites and are labelled as such: the link's own
// floor (both loss and bulk removed) and the loss-only link (jitter and bulk
// removed).
//
// What the decomposition is *for* is the question the hostile arm's guard
// cannot answer: is the residual tail the link the product runs over, or is it
// something the transport adds? The arms below separate the two, and the
// jitter-only arm in particular is the no-spurious-repair control: with nothing
// lost there is nothing to repair, so any sample past the link's own
// `OWD + HOSTILE_JITTER` ceiling is a repair the transport armed for a loss
// that did not happen.
//
// Report-only: the printed table is the deliverable. It asserts only its own
// instrument sanity -- every arm measured samples, every arm delivered what it
// offered, the reference arm's link actually applied the GE model (the counter
// it names fired), and the loss-free arms' links dropped nothing -- because a
// probe that returns no samples and exits green would have deleted the coverage
// it exists to provide.

/// The decomposition arms, each one dimension from the M1 hostile reference.
fn hostile_decomposition_arms() -> Vec<ArmSpec> {
    let window = cadence_window();
    let reference = mandate_arms("M1")
        .into_iter()
        .nth(1)
        .expect("the M1 arm set carries the hostile arm at index 1");
    // Jitter + delay, no loss: the link's own one-way spread, nothing to repair.
    let jitter_only = |seed: u64| link(seed, OWD, HOSTILE_JITTER, 0, 0);
    // The GE model with the jitter removed: what the transport's repair alone
    // costs on a quiet link.
    let loss_no_jitter = |seed: u64| NetemConfig {
        latency: OWD,
        jitter: Duration::ZERO,
        loss_model: gilbert_elliott_loss(5.0, 8.0),
        seed,
        ..NetemConfig::default()
    };
    // The lane's own floor: neither jitter nor loss, so this is the mux/rtp
    // serialization the impaired arms sit on top of.
    let clean_floor = |seed: u64| link(seed, OWD, Duration::ZERO, 0, 0);
    let load = Load::Cadence { cadence_divisor: 1 };
    let arm =
        |name: &'static str, int_c2s: NetemConfig, int_s2c: NetemConfig, bulk: bool| ArmSpec {
            name,
            int_c2s,
            int_s2c,
            bulk,
            saturating_bulk: false,
            load,
            window,
            msg_bytes: MSG_BYTES,
            cadence: CADENCE,
            shared_shaper: false,
            cc_link: None,
        };
    vec![
        // baseline: the M1 hostile arm, unchanged.
        reference,
        // orthogonal(loss): the hostile link with nothing to repair.
        arm("no_loss", jitter_only(41), jitter_only(42), true),
        // orthogonal(jitter): the hostile loss on a quiet link.
        arm("no_jitter", loss_no_jitter(41), loss_no_jitter(42), true),
        // orthogonal(bulk): the hostile link with no competing burst.
        arm("no_bulk", hostile_link(41), hostile_link(42), false),
        // composite(loss,bulk): the link's own jitter alone.
        arm("floor", jitter_only(41), jitter_only(42), false),
        // composite(jitter,bulk): the transport's repair alone on a quiet link.
        arm("loss_only", loss_no_jitter(41), loss_no_jitter(42), false),
        // composite(loss,jitter,bulk): the floor the impaired arms sit on.
        arm("clean_floor", clean_floor(41), clean_floor(42), false),
    ]
}

/// The M1 hostile tail's decomposition. Ignored: it measures the same arms' own
/// mechanisms, not a mandate, and its cost is seven full-window runs.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "hostile-tail decomposition; seven ~25 s arms; run with --ignored --nocapture --test-threads=1"]
async fn m1_hostile_tail_decomposition() {
    let _serial = SERIAL.lock().await;
    let mut runs = Vec::new();
    for spec in hostile_decomposition_arms() {
        let label = format!("hostile-decomp/{}", spec.name);
        let run = with_timeout(ARM_DEADLINE, &label, run_arm(spec)).await;
        runs.push(run);
    }
    let reference = runs
        .iter()
        .find(|run| run.name == "hostile")
        .expect("the decomposition always carries the M1 hostile arm as its reference");
    for run in &runs {
        let s = &run.summary;
        let dropped = run.int_c2s_counters.dropped;
        let received = run.int_c2s_counters.received.max(1);
        let wire_multiple = if run.offered_bytes == 0 {
            0.0
        } else {
            run.int_c2s_wire_bytes as f64 / run.offered_bytes as f64
        };
        let dp99 = s.p99 - reference.summary.p99;
        let row = format!(
            "[hostile-decomp {name:<9}] p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} \
             p999={p999:7.1} max={max:8.1} over250={o25:>4} del={del:.3} recv={recv:>5} \
             applied_loss={loss:>5}/{received:>5} wire={wire:>8}B offered={offered:>8}B \
             wire_x={wx:5.2} dp99_vs_hostile={dp99:+8.1} wall={wall:.1}s\n",
            name = run.name,
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            p999 = s.p999,
            max = s.max,
            o25 = over250_count(&run.samples),
            del = s.delivery_pct,
            recv = s.received,
            loss = dropped,
            received = received,
            wire = run.int_c2s_wire_bytes,
            offered = run.offered_bytes,
            wx = wire_multiple,
            dp99 = dp99,
            wall = run.wall.as_secs_f64(),
        );
        let mut stderr = std::io::stderr().lock();
        let _ = std::io::Write::write_all(&mut stderr, row.as_bytes());
    }
    for run in &runs {
        assert!(
            !run.samples.is_empty(),
            "[hostile-decomp] the {} arm measured no delivered sample: a probe that returns nothing is an instrument failure, not a decomposition",
            run.name,
        );
        assert!(
            run.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR,
            "[hostile-decomp] the {} arm delivered {:.3} of its offer, under the {M2_HOSTILE_DELIVERY_FLOOR} floor: the arm measured a starved lane, not the mechanism it varies",
            run.name,
            run.summary.delivery_pct,
        );
    }
    // The counter the reference arm's mechanism names must have fired, and the
    // control arms' must not have: a "no loss" arm whose link dropped datagrams
    // is not a no-spurious-repair control.
    assert!(
        reference.int_c2s_counters.dropped > 0,
        "[hostile-decomp] the reference arm's link applied {} drops over {} datagrams: the GE model did not fire, so the arm measured a loss-free link while claiming the hostile one",
        reference.int_c2s_counters.dropped,
        reference.int_c2s_counters.received,
    );
    // The loss-free arms' links must have applied no loss, and their lanes must
    // have fired no rung: with nothing lost there is nothing to repair, so a
    // sample past the M1 ceiling can only be a repair the transport armed for a
    // loss that did not happen (a rung costs at least `TAIL_PROBED_MIN_RTO`,
    // 300 ms > the ceiling). This is the probe's real property, and the reason
    // the jitter-only arm is a control rather than just another reading.
    for name in ["no_loss", "floor"] {
        let run = runs.iter().find(|run| run.name == name).unwrap();
        assert_eq!(
            run.int_c2s_counters.dropped, 0,
            "[hostile-decomp] the {name} arm's link dropped {} datagram(s), so it is not the loss-free control its name claims",
            run.int_c2s_counters.dropped,
        );
        let over = over250_count(&run.samples);
        assert_eq!(
            over, 0,
            "[hostile-decomp] the loss-free {name} arm reported {over} sample(s) over {M1_CEILING_MS} ms (max {:.1} ms, p99 {:.1} ms) on a link that dropped nothing: a loss-free path has nothing to repair, so no sample may reach a repair rung -- the lane armed a repair for a loss that did not happen",
            run.summary.max, run.summary.p99,
        );
    }
}

// ────── the rtp_mux-owned levers on the hostile lane: FEC tuning, frame mode ──
//
// The decomposition above says the hostile p99 is the link's own jitter plus the
// transport's repair. The repair is `rtp`'s ladder, but the *lane policy* that
// decides what the repair has to work with is `rtp_mux`'s: the FEC tuning
// (`FecTuning::small_group_parity_count`, `instream_flush`) and the receiver's
// frame mode (fast-forward or strict). This probe sweeps those on the M1
// hostile arm **and** the M1 lone-tail arm at the same time, because the record
// is that a parity increase aimed at one arm read worse on the other: a lever is
// only a lever if it does not buy the hostile tail with the lone one.
//
// Report-only *for the lever decision*, but it asserts its own instrument sanity
// (every arm measured samples and delivered its offer), so a zero-sample sweep
// cannot read as a refusal.

#[tokio::test(flavor = "multi_thread")]
#[ignore = "interactive-lane lever sweep; seven ~25 s arms; run with --ignored --nocapture --test-threads=1"]
async fn m1_hostile_tail_lever() {
    let _serial = SERIAL.lock().await;
    let hostile = mandate_arms("M1")
        .into_iter()
        .nth(1)
        .expect("the M1 arm set carries the hostile arm at index 1");
    let lone = mandate_arms("M1")
        .into_iter()
        .nth(2)
        .expect("the M1 arm set carries the lone_tail arm at index 2");
    let tunings: [(&str, rtp::FecTuning); 3] = [
        ("stock", rtp::FecTuning::default()),
        ("prompt1", prompt_tuning()),
        (
            "parity3",
            rtp::FecTuning {
                instream_flush: true,
                small_group_parity_count: 3,
            },
        ),
    ];
    let mut rows: Vec<(String, ArmRun)> = Vec::new();
    for (label, tuning) in tunings {
        for (arm, spec) in [("hostile", hostile.clone()), ("lone", lone.clone())] {
            let lane = LaneRtpConfig::frame_reordering(true, tuning);
            let name = format!("{arm}/{label}");
            let run = with_timeout(ARM_DEADLINE, &name, run_arm_with(spec, lane)).await;
            rows.push((name, run));
        }
    }
    // The frame-mode lever alone, at the deployment's own tuning.
    let strict = with_timeout(
        ARM_DEADLINE,
        "hostile/strict",
        run_arm_with(
            hostile.clone(),
            LaneRtpConfig::frame_strict_tuned(true, prompt_tuning()),
        ),
    )
    .await;
    rows.push(("hostile/strict".to_owned(), strict));
    // The congestion-intent lever: the deployment declares the interactive lane
    // `Shared` (it shares the host's bottleneck with whatever else is on the
    // wire). The harness's hostile arm has no competing flow on the lane's own
    // link, so `Dedicated` is the one alternative intent the crate could name;
    // it is measured because it is a lane policy this crate owns, not because
    // the deployment would take it.
    let dedicated = with_timeout(
        ARM_DEADLINE,
        "hostile/dedicated",
        run_arm_with(
            hostile.clone(),
            LaneRtpConfig::frame_reordering(true, prompt_tuning())
                .with_congestion_lane(rtp::CongestionLane::Dedicated),
        ),
    )
    .await;
    rows.push(("hostile/dedicated".to_owned(), dedicated));

    for (name, run) in &rows {
        let s = &run.summary;
        let row = format!(
            "[hostile-lever {name:<16}] p50={p50:7.1} p90={p90:7.1} p99={p99:7.1} \
             p999={p999:7.1} max={max:8.1} over250={o25:>4} del={del:.3} recv={recv:>5} \
             wire_x={wx:5.2} wall={wall:.1}s\n",
            name = name,
            p50 = s.p50,
            p90 = s.p90,
            p99 = s.p99,
            p999 = s.p999,
            max = s.max,
            o25 = over250_count(&run.samples),
            del = s.delivery_pct,
            recv = s.received,
            wx = if run.offered_bytes == 0 {
                0.0
            } else {
                run.int_c2s_wire_bytes as f64 / run.offered_bytes as f64
            },
            wall = run.wall.as_secs_f64(),
        );
        let mut stderr = std::io::stderr().lock();
        let _ = std::io::Write::write_all(&mut stderr, row.as_bytes());
    }
    for (name, run) in &rows {
        assert!(
            !run.samples.is_empty(),
            "[hostile-lever] the {name} arm measured no delivered sample: a sweep that measures nothing cannot refuse a lever",
        );
        assert!(
            run.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR,
            "[hostile-lever] the {name} arm delivered {:.3} of its offer, under the {M2_HOSTILE_DELIVERY_FLOOR} floor",
            run.summary.delivery_pct,
        );
    }
}

// ─────── the lone tail's cover, measured from the run instead of assumed ─────
//
// `TAIL_DATAGRAMS_PER_TRANSMISSION` (6) is the cover every rung-law in this file
// is calibrated on: the rung-distribution arm's predicted frequency, the
// censoring row's `rungs=` field and the window arithmetic all restate it. Until
// this arm, no run in this crate could say what the interactive lane actually
// writes per message, so the constant was a premise no measurement in this file
// could contradict -- and `rtp/GATE.md` records the consequence for the cover
// decision: "no existing hostile arm can observe `m`".
//
// This probe observes it. For a stated burst shape and link -- the M1 lone-tail
// arm, one unacked 256 B message at a time over the hostile GE link -- it reads
// the interactive lane's per-message wire from rtp's own send path rather than
// from the constant:
//
//   * the armour copy datagrams the lane wrote, from the
//     `RetransmissionArmorDuplicate` event (rtp emits exactly one per copy
//     datagram, after the underlay send succeeds);
//   * the parity datagrams its FEC flush emitted, from `fec.parity_sent`;
//   * the repairs it fired, from `retransmission_counters.attempts +
//     tail_probes` -- the same pair the crate's wire-measured ladder replay
//     reads, one per rung whether a tail-loss probe or a full-RTO selection.
//
// The c2s link's own accepted-datagram count is the cross-check: the send path
// cannot have written more datagrams than the link carried, and the residual
// between the two is the ACK/control traffic the interactive messages share
// their direction with.
//
// One dimension is varied from the deployed policy: the lane's FEC tuning (the
// parity count, and whether the interactive tail is force-flushed at all), which
// is the only cover knob `rtp_mux` owns. The armour *copy* count is `rtp`'s, so
// this crate cannot move it; what the sweep shows is what each policy's cover
// costs, what the tail is, and whether the rung repairs it fires match the law
// the constant encodes.

/// One cover-wire arm: the M1 lone-tail shape and link, with the lane policy
/// named. The other dimension -- the impairment, the load shape, the window --
/// is the M1 lone-tail arm's own, taken from [`mandate_arms`] rather than
/// restated.
fn cover_wire_arms() -> Vec<(&'static str, ArmSpec, rtp::FecTuning)> {
    let lone = mandate_arms("M1")
        .into_iter()
        .nth(2)
        .expect("the M1 arm set carries the lone_tail arm at index 2");
    vec![
        // The deployment's policy: force-flush the interactive tail, one parity.
        ("deployed", lone.clone(), prompt_tuning()),
        // The stock policy: no force-flush, so the armour never qualifies.
        ("stock", lone.clone(), rtp::FecTuning::default()),
        // A larger parity at the same force-flush: the one cover increase this
        // crate owns.
        (
            "parity3",
            lone,
            rtp::FecTuning {
                instream_flush: true,
                small_group_parity_count: 3,
            },
        ),
    ]
}

/// The interactive lane's per-message wire, decomposed from one run's own send
/// path: `(transmissions, datagrams, copies, parity)`.
///
/// `transmissions` is the messages the arm offered plus the rungs the send space
/// fired: every fresh message is one transmission, and every repair of one is
/// another. `datagrams` is what those transmissions wrote -- one primary each,
/// plus their armour copies and their parity symbols -- summed from the events
/// and counters the transport published, never from the per-message budget the
/// constants name.
/// The upper bound on the link's *unaccounted* datagrams, per offered message:
/// the ACK/control traffic the interactive messages share their direction with.
/// Measured across the arm's reps it runs `1.2-2.0` on every policy, so `3` is a
/// tripwire with ~1.5x headroom rather than a target -- a decomposition that
/// lost a term (or a lane emitting control traffic its messages do not explain)
/// widens the gap past it.
const RESIDUAL_DATAGRAMS_PER_MESSAGE: u64 = 3;

fn cover_wire_decomposition(run: &ArmRun) -> (u64, u64, u64, u64) {
    let messages = run.summary.sent;
    let transmissions = messages + run.cover.rungs;
    let datagrams = transmissions + run.cover.armor_duplicates + run.cover.parity_sent;
    (
        transmissions,
        datagrams,
        run.cover.armor_duplicates,
        run.cover.parity_sent,
    )
}

/// The lone tail's cover, measured. `full` tier and `#[ignore]`d: it is three
/// full request/response windows, and it measures the lane's mechanism rather
/// than asserting a mandate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "lone-tail cover measured from the run; three ~15 s arms; run with --ignored --nocapture --test-threads=1"]
async fn m1_lone_tail_cover_wire() {
    let _serial = SERIAL.lock().await;
    let mut rows: Vec<(String, ArmRun)> = Vec::new();
    for (label, spec, tuning) in cover_wire_arms() {
        let (observer, taps) = cover_wire_observer();
        let lane = LaneRtpConfig::frame_reordering(true, tuning);
        let run = with_timeout(
            ARM_DEADLINE,
            label,
            run_arm_observed(spec, lane, Some((observer, taps))),
        )
        .await;
        rows.push((label.to_owned(), run));
    }
    for (label, run) in &rows {
        let (transmissions, datagrams, _copies, _parity) = cover_wire_decomposition(run);
        let summary = &run.summary;
        let row = format!(
            "[lone-cover {label:<9}] messages={messages:>4} rungs={rungs:>3} \
             transmissions={transmissions:>4} datagrams={datagrams:>5} copies={copies:>4} \
             parity={parity:>5} groups={groups:>4} group_sizes={sizes:?} \
             cover_per_tx={cover:.3} wire_datagrams={wire:>5} residual={residual:>4} \
             wire_x={wx:5.2} p50={p50:7.1} p99={p99:8.1} max={max:8.1} over250={o25:>4} \
             del={del:.3} wall={wall:.1}s\n",
            messages = summary.sent,
            rungs = run.cover.rungs,
            copies = run.cover.armor_duplicates,
            parity = run.cover.parity_sent,
            groups = run.cover.groups_flushed,
            sizes = run.cover.group_sizes,
            wire = run.int_c2s_packets,
            cover = if transmissions == 0 {
                0.0
            } else {
                datagrams as f64 / transmissions as f64
            },
            residual = run.int_c2s_packets as i64 - datagrams as i64,
            wx = if run.offered_bytes == 0 {
                0.0
            } else {
                run.int_c2s_wire_bytes as f64 / run.offered_bytes as f64
            },
            p50 = summary.p50,
            p99 = summary.p99,
            max = summary.max,
            o25 = over250_count(&run.samples),
            del = summary.delivery_pct,
            wall = run.wall.as_secs_f64(),
        );
        let mut stderr = std::io::stderr().lock();
        let _ = std::io::Write::write_all(&mut stderr, row.as_bytes());
    }

    // Instrument sanity: every arm was observed and measured something.
    for (label, run) in &rows {
        assert!(
            run.cover_observed,
            "[lone-cover] the {label} arm ran without its metrics observer, so its cover row would report the default zero as if it were a measurement",
        );
        assert!(
            !run.samples.is_empty() && run.summary.sent > 0,
            "[lone-cover] the {label} arm offered {} message(s) and measured {} sample(s): a probe that returns nothing cannot say what the cover is",
            run.summary.sent,
            run.samples.len(),
        );
        assert!(
            run.summary.delivery_pct >= M2_HOSTILE_DELIVERY_FLOOR,
            "[lone-cover] the {label} arm delivered {:.3} of its offer, under the {M2_HOSTILE_DELIVERY_FLOOR} floor: it measured a starved lane, not the cover it varies",
            run.summary.delivery_pct,
        );
    }

    // The decomposition is a decomposition: the send path's own datagram count
    // cannot exceed what the link accepted, and the remainder it does not
    // account for is the small ACK/control traffic the interactive messages
    // share their direction with -- bounded, on this lane, by a handful of
    // datagrams per message. This is the assertion that makes the reading a wire
    // measurement rather than an oracle over its own arithmetic: a mis-read
    // counter (attempts that emitted nothing, a parity counted twice) pushes the
    // attributable total past the link's own count, and an unexplained gap wider
    // than the control share means the decomposition is missing a term.
    for (label, run) in &rows {
        let (transmissions, datagrams, copies, parity) = cover_wire_decomposition(run);
        assert!(
            datagrams <= run.int_c2s_packets,
            "[lone-cover] the {label} arm's send path reports {datagrams} datagrams \
             ({transmissions} transmissions + {copies} armour copies + {parity} parity) but its own \
             c2s link accepted only {}: the decomposition counts datagrams the wire never carried",
            run.int_c2s_packets,
        );
        let residual = run.int_c2s_packets - datagrams;
        assert!(
            residual <= RESIDUAL_DATAGRAMS_PER_MESSAGE * run.summary.sent,
            "[lone-cover] the {label} arm's c2s link carried {} datagram(s) and its send path \
             accounts for {datagrams}, leaving {residual} ({:.2} per offered message) against the \
             {RESIDUAL_DATAGRAMS_PER_MESSAGE}-per-message control share: the decomposition is \
             missing a term, or the lane is sending control traffic its messages do not explain",
            run.int_c2s_packets,
            residual as f64 / run.summary.sent.max(1) as f64,
        );
    }

    // The withdrawn-cover control: the stock policy never force-flushes the
    // interactive tail, so it writes no armour copy at all. If copies appear
    // here, the event is not the armour's and the deployment's copy count cannot
    // be read from it.
    let (_, stock) = rows
        .iter()
        .find(|(label, _)| label == "stock")
        .expect("the cover sweep always carries the stock arm");
    assert_eq!(
        stock.cover.armor_duplicates, 0,
        "[lone-cover] the stock policy (no force-flush) wrote {} armour copy datagram(s), so the \
         `RetransmissionArmorDuplicate` event is not the armour's own and the deployed arm's copy \
         count cannot be read from it",
        stock.cover.armor_duplicates,
    );

    // The sweep moved the observable quantity: a force-flushed tail writes more
    // datagrams per message than one that is not. This is a measurement of the
    // lever, not a bound on it.
    let (_, deployed) = rows
        .iter()
        .find(|(label, _)| label == "deployed")
        .expect("the cover sweep always carries the deployed arm");
    let (deployed_tx, deployed_datagrams, _, _) = cover_wire_decomposition(deployed);
    let (stock_tx, stock_datagrams, _, _) = cover_wire_decomposition(stock);
    let deployed_cover = deployed_datagrams as f64 / deployed_tx.max(1) as f64;
    let stock_cover = stock_datagrams as f64 / stock_tx.max(1) as f64;
    assert!(
        deployed_cover > stock_cover,
        "[lone-cover] the deployed policy writes {deployed_cover:.3} datagrams per transmission \
         and the stock policy {stock_cover:.3}: the sweep did not move the cover it varies, so the \
         three rows are one reading",
    );

    // The deployed policy's armour must be present and doing work. Both halves
    // have teeth: a cover that silently withdrew (the copy event stops firing,
    // or the eligibility test stops matching the interactive frame) drives the
    // first red, and an armour that no longer absorbs any burst drives the
    // second -- the repair rate would converge on the unarmoured policy's.
    assert!(
        deployed.cover.armor_duplicates > 0,
        "[lone-cover] the deployed policy wrote no armour copy datagram in {} transmissions, so \
         the cover the deployment ships is not on the wire and the copies it reports are the \
         event's, not the armour's",
        deployed_tx,
    );
    let deployed_rung_rate = deployed.cover.rungs as f64 / deployed_tx.max(1) as f64;
    let stock_rung_rate = stock.cover.rungs as f64 / stock_tx.max(1) as f64;
    assert!(
        deployed_rung_rate < stock_rung_rate,
        "[lone-cover] the deployed policy needed a repair on {:.3} of its transmissions and the \
         unarmoured policy on {:.3}: the armour did not absorb any burst its stock counterpart \
         could not, so the cover is not what its own copy count says",
        deployed_rung_rate,
        stock_rung_rate,
    );
}

// ───────── the cross-connection CC signal, in the Minecraft shape ─────────

/// The mechanism arm. Every dimension is the mandate baseline except the two
/// the Minecraft shape names — a realistic interactive cadence (300 B / 20 ms)
/// and a **saturating downstream** bulk — and the one under test: whether the
/// cross-connection CC signal is wired (`Some(hub)` vs `None`). Both lanes cross
/// one shared 1 MiB/s downstream bottleneck, so a difference attributes to the
/// signal.
fn mc_nic_arm(name: &'static str, cc_link: Option<rtp::cc::CcSignalHub>) -> ArmSpec {
    ArmSpec {
        name,
        int_c2s: link(41, OWD, JITTER, LOSS_2, 0),
        int_s2c: link(42, OWD, JITTER, LOSS_2, 0),
        bulk: true,
        saturating_bulk: true,
        load: Load::Cadence { cadence_divisor: 1 },
        window: cadence_window(),
        msg_bytes: 300,
        cadence: Duration::from_millis(20),
        shared_shaper: true,
        cc_link,
    }
}

/// M1 under the Minecraft shape: with the CC signal the interactive tail must
/// not worsen, and the bulk lane must not be capped — the user's hard
/// constraint, asserted on the bulk sink's own delivered bytes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "opt-in: the Minecraft-shape CC-signal arm prints [mandate-smoke mc_*] rows \
            without a MANDATE line, so it belongs to a declared full-tier arm rather than \
            the always-run M1-M4 set; run with --ignored --nocapture"]
async fn m1_nic_minecraft_saturating_downstream() {
    let plain = run_arm(mc_nic_arm("mc_plain", None)).await;
    print_arm(&plain);
    let signalled = run_arm(mc_nic_arm(
        "mc_signalled",
        Some(rtp::cc::CcSignalHub::new()),
    ))
    .await;
    print_arm(&signalled);
    let (p, s) = (&plain.summary, &signalled.summary);
    eprintln!(
        "[mc-cc] interactive one-way   plain p50 {:6.1} p90 {:6.1} p99 {:6.1} max {:6.1}\n\
         [mc-cc]                       cc    p50 {:6.1} p90 {:6.1} p99 {:6.1} max {:6.1}",
        p.p50, p.p90, p.p99, p.max, s.p50, s.p90, s.p99, s.max,
    );
    eprintln!(
        "[mc-cc] interactive delivery  plain {:.4}  cc {:.4}   bulk sink bytes  plain {}  cc {}",
        p.delivery_pct, s.delivery_pct, plain.bulk_sink_bytes, signalled.bulk_sink_bytes,
    );
    // The mandate bound, plus a regression tripwire against the untouched arm.
    // A single rep each is p99-noisy, so the *bound* is the assertion and the
    // printed pair is the evidence; the mechanism's own verdict is in the
    // p90/p99/max it prints beside the plain arm.
    assert!(
        s.p99 <= M1_CEILING_MS,
        "the CC signal's interactive p99 {:.1} ms breaches the mandate ceiling {M1_CEILING_MS}",
        s.p99,
    );
    assert!(
        s.p99 <= p.p99 * 1.25,
        "the CC signal worsened the interactive p99 ({:.1} vs {:.1} ms)",
        s.p99,
        p.p99,
    );
    assert!(
        s.delivery_pct >= p.delivery_pct * 0.99,
        "the CC signal capped the interactive lane's delivery ({:.4} vs {:.4})",
        s.delivery_pct,
        p.delivery_pct,
    );
    assert!(
        signalled.bulk_sink_bytes >= plain.bulk_sink_bytes * 9 / 10,
        "the CC signal capped the bulk lane ({} vs {} bytes delivered)",
        signalled.bulk_sink_bytes,
        plain.bulk_sink_bytes,
    );
}

/// The Minecraft arm's interactive **offer pattern**, measured from the arm's
/// own delivery timeline.
///
/// Requirement (A) ("out-compete while the interactive lane is not running
/// traffic at the moment") is only meaningful where the lane actually *stops*
/// offering; requirement (B) ("no latency drop when it starts transmitting")
/// is only meaningful where it starts again. Both are statements about the
/// offer-gap distribution, so this probe measures that distribution for the
/// shape rather than assuming a gap exists.
///
/// Each cadence sample's send time is recoverable exactly: the frame carries
/// the sender's `base` stamp and the sink reports `base`-clock arrival minus it
/// ([`ArmRun::timeline`]), so `send = arrival - latency`. The gap distribution
/// is then read between consecutive sends, and the quiet fractions are the
/// excess of each gap over a threshold divided by the offered span.
#[derive(Debug, Clone, Copy)]
struct OfferShape {
    offers: usize,
    sent: u64,
    received: u64,
    window_s: f64,
    span_s: f64,
    mean_gap_ms: f64,
    p50_gap_ms: f64,
    p90_gap_ms: f64,
    p99_gap_ms: f64,
    max_gap_ms: f64,
    /// Fraction of the offered span during which the time since the last offer
    /// is at least `rtp::cc::STANDOFF_WINDOW`: the window the stand-off's claim
    /// needs to be due.
    quiet_ge_standoff: f64,
    /// The same fraction at one round trip (2 x the one-way p50): R1's
    /// `lane_idle` rate criterion is `offered_pps < 1 / control_rtt`, i.e. the
    /// lane has nothing in flight for at least one control interval.
    quiet_ge_rtt: f64,
    rtt_proxy_ms: f64,
}

fn offer_gap_percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)]
}

fn measure_offer_shape(run: &ArmRun) -> OfferShape {
    let sends: Vec<f64> = run
        .timeline
        .iter()
        .map(|(arrival, latency)| arrival - latency / 1000.0)
        .collect();
    let gaps: Vec<f64> = sends.windows(2).map(|w| (w[1] - w[0]) * 1000.0).collect();
    let mut sorted = gaps.clone();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let span_s = sends
        .last()
        .zip(sends.first())
        .map(|(last, first)| last - first)
        .unwrap_or(0.0);
    let quiet = |threshold_ms: f64| -> f64 {
        if span_s <= 0.0 {
            return f64::NAN;
        }
        gaps.iter()
            .map(|gap| (gap - threshold_ms).max(0.0))
            .sum::<f64>()
            / (span_s * 1000.0)
    };
    let rtt_proxy_ms = 2.0 * run.summary.p50;
    OfferShape {
        offers: sends.len(),
        sent: run.summary.sent,
        received: run.summary.received,
        window_s: run.window.as_secs_f64(),
        span_s,
        mean_gap_ms: if gaps.is_empty() {
            f64::NAN
        } else {
            gaps.iter().sum::<f64>() / gaps.len() as f64
        },
        p50_gap_ms: offer_gap_percentile(&sorted, 0.50),
        p90_gap_ms: offer_gap_percentile(&sorted, 0.90),
        p99_gap_ms: offer_gap_percentile(&sorted, 0.99),
        max_gap_ms: sorted.last().copied().unwrap_or(f64::NAN),
        quiet_ge_standoff: quiet(rtp::cc::STANDOFF_WINDOW.as_secs_f64() * 1000.0),
        quiet_ge_rtt: quiet(rtt_proxy_ms),
        rtt_proxy_ms,
    }
}

/// The Minecraft topology's offer-gap probe: runs the arm's own shape (one
/// block at its real `300 B / 20 ms` cadence) and a **sensitivity control** at a
/// `2000 ms` cadence, which is above `STANDOFF_WINDOW`, so the quiet-fraction
/// instrument is shown to be able to read a gap when one exists. Neither block
/// asserts a product property: the deliverable is the measured distribution,
/// which is what decides whether the Minecraft shape can host (A) and what
/// "resumption" means for (B).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "opt-in shape probe: measures the Minecraft arm's interactive offer-gap distribution \
            (and a 2000 ms sensitivity control); run with --ignored --nocapture"]
async fn mc_nic_offer_gap_distribution() {
    // Fault hook: an override cadence (ms) for the first block.  A cadence
    // above the window (`MC_GAP_CADENCE_MS=30000`) makes the block offer
    // nothing, so the shape assertion below fires from the measurement path --
    // the vacuity demonstration for "the offer series was measured".
    let cadence_ms: u64 = std::env::var("MC_GAP_CADENCE_MS")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(20);
    let mut spec = mc_nic_arm("mc_gap", None);
    spec.cadence = Duration::from_millis(cadence_ms);
    let run = run_arm(spec.clone()).await;
    print_arm(&run);
    let shape = measure_offer_shape(&run);
    eprintln!(
        "[mc-gaps] cadence 300 B / {cadence_ms} ms  offers {} of {} sent ({} received) over a \
         {:.1} s window (offered span {:.2} s)\n\
         [mc-gaps] gap ms  mean {:.2}  p50 {:.2}  p90 {:.2}  p99 {:.2}  max {:.2}\n\
         [mc-gaps] quiet  >= STANDOFF_WINDOW (1500 ms) {:.4} of the span   \
         >= 1 RTT (proxy {:.1} ms) {:.4} of the span",
        shape.offers,
        shape.sent,
        shape.received,
        shape.window_s,
        shape.span_s,
        shape.mean_gap_ms,
        shape.p50_gap_ms,
        shape.p90_gap_ms,
        shape.p99_gap_ms,
        shape.max_gap_ms,
        shape.quiet_ge_standoff,
        shape.rtt_proxy_ms,
        shape.quiet_ge_rtt,
    );
    // Sensitivity control: the same topology with a cadence above the stand-off
    // window, so a gap genuinely exists and the quiet-fraction instrument must
    // read it. A metric that cannot separate these two blocks is not a measure
    // of the shape.
    spec.name = "mc_gap_slow";
    spec.cadence = Duration::from_millis(2000);
    let slow = run_arm(spec).await;
    print_arm(&slow);
    let slow_shape = measure_offer_shape(&slow);
    eprintln!(
        "[mc-gaps] sensitivity control 300 B / 2000 ms  offers {} of {} sent  \
         gap ms p50 {:.2} p90 {:.2} max {:.2}  quiet >= STANDOFF_WINDOW {:.4}  \
         quiet >= 1 RTT {:.4}",
        slow_shape.offers,
        slow_shape.sent,
        slow_shape.p50_gap_ms,
        slow_shape.p90_gap_ms,
        slow_shape.max_gap_ms,
        slow_shape.quiet_ge_standoff,
        slow_shape.quiet_ge_rtt,
    );
    assert!(
        shape.offers > 0 && shape.offers as u64 + 1 >= shape.sent,
        "[mc-gaps] the Minecraft cadence block offered {} message(s) ({} sent): the timeline the \
         gap distribution is recovered from is empty or truncated, so the shape is unmeasured",
        shape.offers,
        shape.sent,
    );
    assert!(
        slow_shape.quiet_ge_standoff > shape.quiet_ge_standoff + 0.1,
        "[mc-gaps] the 2000 ms sensitivity control's quiet-of-the-stand-off-window fraction \
         {:.4} does not exceed the 20 ms block's {:.4} by more than 0.1: the quiet-fraction \
         instrument cannot tell a gap from no gap, so its reading about the Minecraft shape is \
         not evidence",
        slow_shape.quiet_ge_standoff,
        shape.quiet_ge_standoff,
    );
}
