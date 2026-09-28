//! Diff a mandate-check run's per-arm measurements against a committed baseline.
//!
//! `tools/mandate-check` records what each perf *arm* measured — its sample
//! count, its distribution statistics, its delivery and wire counters, its
//! measured windows and the coverage cells it is declared to exercise — so that
//! a change which shortens a perf test can be *shown* coverage-neutral instead
//! of argued so. This module is the other half of that instrument: it compares a
//! fresh run's report with the committed baseline
//! (`tools/mandate-baseline.json`) and reports, per arm, exactly which
//! quantities moved. The `mandate-compare` subcommand of `netem-tools` is the
//! command-line face of it; `perf-history` reuses [`coverage_verdict`] so the
//! coverage/claim axes have one implementation rather than two.
//!
//! ## What it compares, and from whom
//!
//! Every arm of every producer a report covers is compared, because every arm's
//! `id` is unique across the producers of a run (a section is an arm-id
//! namespace, so two producers cannot share one) and every arm carries the
//! `producer` field that printed it. The verdict block names both runs'
//! producers, so a verdict can be read as covering them — and a producer whose
//! arms the candidate did not record at all is not a green verdict but a
//! coverage regression, one absent arm at a time. A producer added since the
//! baseline is reported as new arms, not as a regression: coverage grew.
//!
//! ## The two kinds of movement
//!
//! They are not the same kind of claim and must not be read as one.
//!
//! - **A coverage regression** is a quantity whose movement means the arm no
//!   longer covers what the baseline covered: the arm is gone, its sample count
//!   fell, a delivery or wire counter fell, a measurement window shrank, a
//!   statistic the assertions read stopped being measured, its delivery ratio
//!   fell past its tolerance, or a coverage cell the baseline exercised is
//!   exercised by no arm any more. These are failures (exit `4`): a shortened
//!   test that halves an arm's samples has moved its p99's statistical power,
//!   and a p99 that is still inside its bound does not say otherwise.
//! - **A value change** is a statistics move — a latency percentile, a goodput
//!   rate, a share. On a shared host these move run to run, so by default they
//!   are *reported* and the comparison stays green; with `--fail-on-value-drift`
//!   a move past `--value-tolerance` is a failure (exit `5`). The tolerance is
//!   deliberately loose (50 %, the same relative tolerance `netem-tools check-gate`
//!   applies to a declared cost) because the point of the default is to be
//!   usable on a loaded host.
//!
//! Counts are compared with a **50 % relative tolerance** rather than exactly,
//! because a window-driven arm's counts vary between runs of the same binary on
//! a contended host: across two recorded full runs of the unchanged tree the
//! request/response arm's sample count moved 819 -> 421 (-48.6 %) and 514 -> 882
//! (+71.6 %), and a hostile arm's wire bytes moved -10.7 %, while the cadence
//! and per-flow arms moved under 4 %. A fall of half or more is a regression; a
//! smaller fall is reported as a move. **The measured window is what carries the
//! sharp shortening signal** and is compared at 1 %, so a window reduced from
//! 12 s to 4 s is a regression whoever reports the sample counts. Everything the
//! comparison reads comes from the two reports; nothing is re-measured here.
//!
//! That relative tolerance is only meaningful where the baseline value is
//! load-bearing, and whether a counted quantity is *load-bearing for an arm* is
//! a question about the arm's own **declaration**, not about magnitude: the
//! arms' coverage cells state the property, the dimensions and the values each
//! arm exercises, and a cell that names the lane a counter measures is that
//! arm's claim to be compared on it. So the comparison derives the answer
//! ([`cell_claim`], [`arm_claim`], [`COUNTER_LANES`]) and prints the rule with its
//! verdict; a claiming cell is compared **with no floor at all**, so a small
//! counter is a tooth exactly like a large one.
//!
//! The three ways a cell can speak about a counter, and what each does:
//!
//! - **claimed** — the cell names the counter's lane as the arm's own
//!   (`lane=bulk`) or offers a workload on it (`load=` or `bulk=`, any value but
//!   `none`). Compared, with no floor: a fall past the tolerance **or** a
//!   disappearance is a coverage regression.
//! - **idle** — the cell declares the lane carries no workload (`load=none` or
//!   `bulk=none`, the way the M4 arms declare the bulk lane connected but never
//!   opened). *Reported and never compared*, and that is the intended behaviour
//!   rather than an accident of magnitude: the arm's own declaration says the
//!   counter is not the coverage the arm measures.
//! - **unstated** — the cell names neither. The declaration does not decide and
//!   the comparison does not guess: the pair is recorded in the diff's
//!   `claim_gaps` (with what therefore did decide it: the key's measured floor,
//!   or the plain count tolerance where the key has no floor), and only here the
//!   **measured floor** (`COUNT_FLOORS_BYTES`) is what keeps a residue among
//!   them from failing. The floor's value and derivation are unchanged; what
//!   narrowed is the population it applies to, from every arm to the arm x
//!   counter pairs no cell claims or disclaims.
//!
//! What the declaration can and cannot express is stated with [`COUNTER_LANES`]
//! and the blocker recorded there: the grammar admits two positive forms
//! (`lane=<the counter's lane>`, and `load=`/`bulk=` with a value that is not
//! `none`) and both are read, while `shape=` (the *interactive* lane's load
//! shape) and `rate=`/`burst=` (a rate regime, not a lane) are not, and
//! `lane=dual` is silent about the bulk lane — the two arms whose cells say it
//! need opposite outcomes, which is the specific evidence that keeps the floor
//! at all.
//!
//! **The detection limit**, stated so a green diff is not read as more than it
//! is: this comparison sees an arm that disappeared, a load-bearing sample count
//! or delivery/wire counter that fell by half or more, a load-bearing counter
//! that stopped being measured, a measured window that shrank by more than 1 %,
//! a statistic the assertions read that stopped being measured, the delivery
//! ratio falling past its tolerance, a coverage cell no arm covers any more, and
//! a mandate that vanished. It does **not** see an arm that keeps its sample
//! count and its counters while its impairment was quietly weakened — a 2 % loss
//! arm retuned to 1 % measures the same shape under a milder regime. That is a
//! change to a frozen perf-test setting, and the guard against it is the
//! setting's immutability and reading the arm against its declared coverage cell
//! in `tools/mandate-arms.json`, not this comparison. Nor does it see a
//! shortening that leaves the window in place, keeps at least half the samples
//! and drops no cell. And an arm whose cell *declares a lane idle* has that
//! lane's counters reported and never compared, deliberately, so a real workload
//! on a lane the declaration calls idle is not caught here: the accuracy of the
//! declaration is trusted, exactly as it is for the impairment it names. That is
//! the price of deciding relevance from the declaration; the *unstated* pairs
//! that used to be decided by magnitude are the ones this rule takes away from
//! it, and they are the ones it names.
//!
//! A false positive it used to report, and what the declaration does about it.
//! The counts once had no absolute floor, so one small enough that half of it is
//! a handful of datagrams crossed the 50 % tolerance on an **unchanged** tree:
//! on the `M1/lone_tail`/`M2/lone_tail` arms of two real full runs of the
//! unchanged tree, `bulk_wire_bytes` read 1920 in one and 785 in the next
//! (-59 %), red-flagging two arms that drive no bulk workload — the few kilobytes
//! are the fixture's incidental bulk-lane traffic, not the coverage the arm
//! exists to measure. A magnitude floor removed the false positive but could not
//! tell a residue from a small real workload, which is a *relevance* question: it
//! is the arm's declared cells that answer it, and they are what this comparison
//! reads now ([`cell_claim`], [`arm_claim`]). On those two arms the cell's
//! `lane=dual` leaves the bulk lane unstated, so they keep the floor and the
//! wobble stays *visible* in the arm's line while the verdict stays green — and
//! the pair is printed and written in `claim_gaps`, so the hole is named rather
//! than implied. A cell that *claims* the lane (`lane=bulk`, or a `load` on it)
//! is compared with no floor at all, so the residue arm's silence is doing real
//! work: the same 1920 -> 785 pair fails on a claiming cell, and every claim the
//! declarations make is printed with the verdict.
//!
//! ## What it refuses
//!
//! A comparison between runs that are not comparable is refused (exit `2`)
//! rather than reported as agreement: a candidate whose schema predates the
//! per-arm record, a baseline or candidate with no arms at all, a candidate
//! whose `--quick` flag differs from the baseline's, or a declared coverage cell
//! whose syntax the gate checker rejects. An empty comparison is never a pass.
//!
//! ## What it writes
//!
//! The verdict block on stdout, and with `--json-out` the same diff as JSON: the
//! two reports, the tolerances, every arm compared with its changes, the coverage
//! regressions, the value moves and the exit code.
//!
//! ## Exit codes
//!
//! - `0` — every arm the baseline covered is still covered, with no coverage
//!   regression and no value move past the tolerance.
//! - `2` — the comparison could not be made.
//! - `4` — at least one coverage regression.
//! - `5` — no coverage regression, and (only with `--fail-on-value-drift`) a
//!   value move past the tolerance.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::json::{self, Json};
use super::mandate_check::PRODUCERS_DECLARATION_NAME;

pub const DEFAULT_BASELINE_NAME: &str = "mandate-baseline.json";
pub const DEFAULT_REPORT_NAME: &str = "mandate-check.json";
// The flags' defaults are public because the `netem-tools` binary's clap
// definition names them (`default_value_t`), so the CLI and the comparison
// cannot drift apart on what an omitted flag means.
pub const DEFAULT_COUNT_TOLERANCE: f64 = 0.50;
pub const DEFAULT_WINDOW_TOLERANCE: f64 = 0.01;
pub const DEFAULT_DELIVERY_TOLERANCE: f64 = 0.005;
pub const DEFAULT_VALUE_TOLERANCE: f64 = 0.5;
const MINIMUM_SCHEMA: u32 = 3;
// How many arms and claim gaps the printed block lists before it summarises the
// rest, so a large run cannot bury its verdict in a wall of lines.
const MAX_ARM_LINES: usize = 200;
const MAX_GAP_LINES: usize = 24;

pub const EXIT_OK: i32 = 0;
pub const EXIT_UNCOMPARABLE: i32 = 2;
pub const EXIT_COVERAGE_REGRESSION: i32 = 4;
pub const EXIT_VALUE_DRIFT: i32 = 5;

// The quantities a coverage regression is decided on, per arm. A key present in
// the baseline and absent in the candidate is a regression for the same reason
// a fall is: the arm stopped measuring it.
const COVERAGE_COUNTER_KEYS: &[&str] = &[
    "sent",
    "received",
    "wire_bytes",
    "bulk_wire_bytes",
    "bulk_sink_bytes",
    "offered_bytes",
    "delivered_bytes",
    "forwarded_bytes",
];
const COVERAGE_WINDOW_KEYS: &[&str] = &["window_seconds", "elapsed_seconds"];
const COVERAGE_WALL_KEYS: &[&str] = &["wall_seconds"];
const VALUE_STAT_KEYS: &[&str] = &[
    "p50",
    "p90",
    "p99",
    "p999",
    "max",
    "min",
    "mean",
    "std",
    "over250",
    "fraction",
    "share",
    "imbalance",
    "min_share",
    "max_share",
    "ideal_share",
    "delivered_mib_s",
    "shaper_mib_s",
    "capacity_mib_s",
];
// The delivery ratio is a coverage quantity, not a statistic: a lane that
// delivered 1.000 and now delivers 0.980 has lost delivery, however static its
// p99 is. It is compared absolutely, with the slope the M2/M4 floors allow.
const DELIVERY_KEY: &str = "delivery";

// ─────────────────────── which counters are load-bearing ─────────────────────
//
// A counter's *fall* is coverage loss only when the arm's declaration claims the
// lane it measures. What a cell can say, and what each answer does to the
// comparison, is stated in the module docstring: a claim keeps the tooth with no
// floor, an idle declaration is reported and never compared, and an unstated
// lane keeps the measured floor and is named in `claim_gaps`.

/// The lane a compared counter measures, where it is not the arm's own lane.
/// Only keys that measure a *second* lane may appear: a key that measures the
/// arm's own lane cannot be disclaimed by a dimension about another lane, so
/// nothing of the sort is listed here. Every key absent from this map measures
/// the arm's own lane — the lane its cells name and its samples are taken on.
pub const COUNTER_LANES: &[(&str, &str)] =
    &[("bulk_wire_bytes", "bulk"), ("bulk_sink_bytes", "bulk")];

// The cell dimensions that speak about a lane, and the one value that declares
// it idle. `lane` names the lane the arm measures on; `load` and `bulk` declare
// the workload offered on the *second* (bulk) lane, and both are read, because
// the grammar's own cells express the bulk workload either way: `rtp_mux`'s gate
// rows write `load=bulk`/`load=bulk-burst`/`load=bulk-matched` and `load=none`,
// and its `hol_probe` rows write `bulk=none`/`bulk=shared`/`bulk=split-and-shared`.
// Two forms are deliberately *not* read: `shape=` names the **interactive**
// lane's load shape (the arm table these cells restate carries a separate `bulk`
// column), so reading `shape=cadence` as a bulk claim would make M4's
// `shape=cadence+load=none` a claim of a lane it never opens; and `rate=`/`burst=`
// name a rate regime that is not a lane at all —
// `conformance-reorder@impairment=reorder+rate=rate-limit` and
// `reorder-rate@impairment=reorder+rate=curve` carry them with no bulk lane in
// sight.
const LANE_DIMENSION: &str = "lane";
const LOAD_DIMENSIONS: &[&str] = &["load", "bulk"];
const IDLE_LOAD_VALUES: &[&str] = &["none"];

/// A cell that claims the counter's lane.
pub const CLAIMED: &str = "claimed";
/// A cell that declares the lane carries no workload.
pub const IDLE: &str = "idle";
/// A cell that names neither, so the declaration decides nothing.
pub const UNSTATED: &str = "unstated";

// How one arm's cells combine into its claim: a claim wins wherever any declared
// cell makes it (a tooth is never dropped because a second cell was silent), a
// silent cell is preferred to one that declares the lane idle, and only an arm
// whose every cell says the lane is idle is left uncompared.
const CLAIM_PRECEDENCE: &[&str] = &[CLAIMED, UNSTATED, IDLE];

// `<dimension>=<value>`, the per-dimension shape `netem-tools check-gate`'s
// `CELL_DIMENSION_RE` admits. That checker is the grammar's authority and has
// already rejected a malformed cell before a claim is read; this repeats the
// shape rather than importing the checker into every claim call.

/// The **measured floor** of a counted quantity, in bytes, applied to a pair the
/// cells leave unstated — and only there: a claiming cell is compared with no
/// floor, and an idle cell is not compared at all. The floor is an absolute
/// quantity because the noise is quantisation noise (a few datagrams): it
/// dominates a small counter and is invisible in a large one. Only byte counters
/// may be listed here.
///
/// Derived, not chosen, and unchanged by the narrowing above. Over every full-run
/// report recorded on disk, the two bulk-lane counters read 0..3525 B on the
/// `M1/lone_tail`/`M2/lone_tail` request-response arms — whose cells, under this
/// rule, leave the lane unstated — (56 observations across 15 runs, 6 of them of
/// one unchanged tree), and 2097152..8484001 B on every arm that drives the bulk
/// lane (112 observations) — a 595x gap. The floor is the largest power of two
/// inside that gap (geometric midpoint 85978 B), leaving an 18.6x margin below
/// the observed residue ceiling and a 32x margin above the smallest observed
/// real bulk workload. Re-deriving a tighter value would need a new measurement
/// of that spread; narrowing which pairs it applies to needs none, and that is
/// what this rule does.
///
/// It is kept, rather than replaced outright, because of one token the
/// declarations cannot distinguish: `lane=dual`. The `M1/clean` cell drives
/// 2 MiB / 3 s on the bulk lane and records `bulk_wire_bytes` 8482399, so reading
/// its cell as a non-claim would drop that counter out of the comparison and let
/// the arm lose its concurrent bulk load with a green verdict. The
/// `M1/lone_tail` cell runs with the bulk lane idle and records `bulk_wire_bytes`
/// 1920 and then 785 between two runs of the unchanged tree, so reading *its*
/// cell as a claim restores that false positive.
pub const COUNT_FLOORS_BYTES: &[(&str, u64)] =
    &[("bulk_wire_bytes", 65536), ("bulk_sink_bytes", 65536)];

/// A failure that must surface as a non-zero exit, naming the problem.
#[derive(Debug)]
pub struct MandateCompareError(pub String);

impl std::fmt::Display for MandateCompareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

type Result<T> = std::result::Result<T, MandateCompareError>;

fn err<T>(message: impl Into<String>) -> Result<T> {
    Err(MandateCompareError(message.into()))
}

/// The declared measured floor of one counted quantity, 0 when none.
///
/// A floor is consulted only where the arm's cells leave the counter's lane
/// unstated (`UNSTATED`); a claiming cell is compared with none, and an idle
/// cell is not compared at all.
pub fn floor_bytes(key: &str) -> u64 {
    COUNT_FLOORS_BYTES
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, floor)| *floor)
        .unwrap_or(0)
}

fn counter_lane(key: &str) -> Option<&'static str> {
    COUNTER_LANES
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, lane)| *lane)
}

/// A JSON scalar as the comparison and the printed block carry it.
#[derive(Debug, Clone)]
enum Scalar {
    Null,
    Num(Num),
    Text(String),
}

impl Scalar {
    fn from_json(value: &Json) -> Scalar {
        match value {
            Json::Null => Scalar::Null,
            Json::Int(number) => Scalar::Num(Num::int(*number)),
            Json::Float(number) => Scalar::Num(Num::float(*number)),
            Json::Str(text) => Scalar::Text(text.clone()),
            Json::Bool(flag) => Scalar::Text(if *flag { "True" } else { "False" }.to_string()),
            other => Scalar::Text(format!("{other:?}")),
        }
    }

    /// Python's `str()` of the value: `None`, the bare text, or the number with
    /// its integer/float identity intact.
    fn display(&self) -> String {
        match self {
            Scalar::Null => "None".to_string(),
            Scalar::Num(number) => number.display(),
            Scalar::Text(text) => text.clone(),
        }
    }

    fn number(&self) -> Option<f64> {
        match self {
            Scalar::Num(number) => Some(number.value),
            _ => None,
        }
    }

    /// Python's `==` over scalars: ints and floats compare by value.
    fn eq_scalar(&self, other: &Scalar) -> bool {
        match (self.number(), other.number()) {
            (Some(left), Some(right)) => left == right,
            _ => match (self, other) {
                (Scalar::Null, Scalar::Null) => true,
                (Scalar::Text(left), Scalar::Text(right)) => left == right,
                _ => false,
            },
        }
    }
}

/// A number that remembers whether it was written as an integer, so `126000`
/// prints as `126000` and `12.0` as `12.0`.
#[derive(Debug, Clone, Copy)]
struct Num {
    integral: bool,
    value: f64,
}

impl Num {
    fn int(value: i64) -> Num {
        Num {
            integral: true,
            value: value as f64,
        }
    }

    fn float(value: f64) -> Num {
        Num {
            integral: false,
            value,
        }
    }

    fn display(&self) -> String {
        if self.integral {
            (self.value as i64).to_string()
        } else {
            json::format_float(self.value)
        }
    }

    fn to_json(self) -> Json {
        if self.integral {
            Json::Int(self.value as i64)
        } else {
            Json::Float(self.value)
        }
    }
}

/// The candidate's relative change from the baseline, or `None`.
fn relative_change(baseline: &Scalar, candidate: &Scalar) -> Option<f64> {
    let old = baseline.number()?;
    if old == 0.0 {
        return None;
    }
    let new = candidate.number()?;
    Some((new - old) / old)
}

/// Python's `str()` of a JSON value, for the report path in an error.
fn json_display(value: &Json) -> String {
    match value {
        Json::Str(text) => format!("'{text}'"),
        other => Scalar::from_json(other).display(),
    }
}

/// One `mandate-check.json` as a dict, or a named failure.
struct Report {
    path: PathBuf,
    payload: Json,
    arms: BTreeMap<String, Json>,
}

impl Report {
    fn quick(&self) -> bool {
        self.payload.get("quick").is_some_and(Json::truthy)
    }
}

fn resolve_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    fs::canonicalize(&absolute).unwrap_or(absolute)
}

fn load_report(path: &Path, role: &str) -> Result<Report> {
    let resolved = resolve_path(path);
    if !resolved.is_file() {
        return err(format!(
            "the {role} report {} does not exist",
            resolved.display()
        ));
    }
    let text = fs::read_to_string(&resolved).map_err(|error| {
        MandateCompareError(format!(
            "the {role} report {} cannot be read: {error}",
            resolved.display()
        ))
    })?;
    let payload = json::parse(&text).map_err(|error| {
        MandateCompareError(format!(
            "the {role} report {} cannot be read: {error}",
            resolved.display()
        ))
    })?;
    if !matches!(payload, Json::Object(_)) {
        return err(format!(
            "the {role} report {} is not a JSON object",
            resolved.display()
        ));
    }
    let schema = payload.get("schema");
    let version = schema.and_then(schema_version);
    let Some(version) = version else {
        let rendered = schema
            .map(json_display)
            .unwrap_or_else(|| "None".to_string());
        return err(format!(
            "the {role} report {} declares schema {rendered}, not a \
             'mandate-check/<version>' this command can read",
            resolved.display()
        ));
    };
    if version < MINIMUM_SCHEMA {
        // Python interpolates the schema bare here (and `!r` in the
        // unknown-schema branch above), so this one carries no quotes.
        let rendered = schema.and_then(Json::as_str).unwrap_or_default();
        return err(format!(
            "the {role} report {} is schema {rendered}, which predates the \
             per-arm record (mandate-check/{MINIMUM_SCHEMA}); re-record it with \
             tools/mandate-check before comparing",
            resolved.display()
        ));
    }
    let arms = payload.get("arms").and_then(Json::as_array);
    match arms {
        Some(arms) if !arms.is_empty() => {
            let index = arm_index(arms)?;
            Ok(Report {
                path: resolved,
                payload,
                arms: index,
            })
        }
        _ => err(format!(
            "the {role} report {} carries no arm records, so there is nothing to \
             compare; a report with no arms cannot certify coverage",
            resolved.display()
        )),
    }
}

/// `mandate-check/<version>`, or `None` for anything else.
fn schema_version(schema: &Json) -> Option<u32> {
    let text = schema.as_str()?;
    text.strip_prefix("mandate-check/")?.parse::<u32>().ok()
}

/// `id -> arm record`, refusing a repeated id (which cannot be diffed).
fn arm_index(arms: &[Json]) -> Result<BTreeMap<String, Json>> {
    let mut index = BTreeMap::new();
    for arm in arms {
        let id = arm
            .as_object()
            .and_then(|map| map.get("id"))
            .and_then(Json::as_str);
        let Some(id) = id else {
            return err("an arm record has no id, so it cannot be compared");
        };
        if index.contains_key(id) {
            return err(format!(
                "the arm '{id}' is recorded twice; a reader cannot tell which \
                 record the comparison should read"
            ));
        }
        index.insert(id.to_string(), arm.clone());
    }
    Ok(index)
}

fn numbers(mapping: Option<&Json>) -> BTreeMap<String, Scalar> {
    let mut out = BTreeMap::new();
    if let Some(Json::Object(map)) = mapping {
        for (key, value) in map {
            if value.is_number() {
                out.insert(key.clone(), Scalar::from_json(value));
            }
        }
    }
    out
}

/// `dimension -> value` for one coverage cell, or `None` when ambiguous.
///
/// A cell stating one dimension twice says two things about it and the reader
/// cannot tell which a claim should read, so it yields nothing rather than one
/// of the two values (the arm's claim is then unstated, which is recorded).
fn cell_dimensions(cell: &str) -> Option<BTreeMap<String, String>> {
    let (_, dimensions) = cell.split_once('@')?;
    let mut named = BTreeMap::new();
    for part in dimensions.split('+') {
        let (name, value) = parse_dimension_part(part)?;
        if named.contains_key(name) {
            return None;
        }
        named.insert(name.to_string(), value.to_string());
    }
    Some(named)
}

/// `[A-Za-z][A-Za-z0-9_-]*=[^=,+\s]+`, the full-match shape of one cell part.
fn parse_dimension_part(part: &str) -> Option<(&str, &str)> {
    let (name, value) = part.split_once('=')?;
    let mut name_chars = name.chars();
    let first = name_chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if !name_chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return None;
    }
    if value.is_empty() {
        return None;
    }
    if value
        .chars()
        .any(|c| c == '=' || c == ',' || c == '+' || c.is_whitespace())
    {
        return None;
    }
    Some((name, value))
}

/// How one cell speaks about the lane `key` measures, `(claim, why)`.
///
/// `claimed` when the cell says the arm measures on that lane (it names the
/// lane as the arm's own) or offers a workload on it (a `load`/`bulk` dimension
/// whose value is not `none`); `idle` when it declares that lane carries no
/// workload; `unstated` when it names neither, which is not a claim and not a
/// disclaimer. The `why` is the cell's own words, so a verdict that rests on the
/// answer names it.
fn cell_claim(cell: &str, key: &str) -> (&'static str, String) {
    let lane = counter_lane(key);
    let Some(dimensions) = cell_dimensions(cell) else {
        return (
            UNSTATED,
            "the cell states no readable dimension (or states one twice)".to_string(),
        );
    };
    let named = dimensions.get(LANE_DIMENSION);
    let Some(lane) = lane else {
        // The key measures the arm's own lane — the lane its cells name and its
        // samples are taken on. `load`/`bulk` are about the *other* lane and
        // cannot disclaim it.
        return match named {
            None => (UNSTATED, "the cell names no lane dimension".to_string()),
            Some(named) => (
                CLAIMED,
                format!("the cell names the arm's own lane ({LANE_DIMENSION}={named})"),
            ),
        };
    };
    if named.map(String::as_str) == Some(lane) {
        return (
            CLAIMED,
            format!("the cell names the counter's lane as the arm's own ({LANE_DIMENSION}={lane})"),
        );
    }
    let declared: Vec<(&str, &String)> = LOAD_DIMENSIONS
        .iter()
        .filter_map(|name| dimensions.get(*name).map(|value| (*name, value)))
        .collect();
    for (name, value) in &declared {
        if !IDLE_LOAD_VALUES.contains(&value.as_str()) {
            return (
                CLAIMED,
                format!("the cell offers a workload on the {lane} lane ({name}={value})"),
            );
        }
    }
    if let Some((name, value)) = declared.first() {
        return (
            IDLE,
            format!("the cell declares the {lane} lane idle ({name}={value})"),
        );
    }
    match named {
        None => (
            UNSTATED,
            format!("the cell names neither the {lane} lane nor a load on it"),
        ),
        Some(named) => (
            UNSTATED,
            format!("the cell names {LANE_DIMENSION}={named} and no load on the {lane} lane"),
        ),
    }
}

/// `(claim, why, gap)`: what one arm's declared cells say about `key`.
///
/// The arm's cells combine by `CLAIM_PRECEDENCE`, so a claim any declared cell
/// makes keeps the tooth. `why` is the decided cell's own reason, without the
/// cell text (the cells are in the arm's record and in every gap this files).
/// `gap` is true when the arm's cells leave the lane unstated: the declaration
/// decides nothing for that pair, and the pair is filed so that what *did*
/// decide it (a floor, or the plain count tolerance where the key has no floor)
/// is visible rather than implicit.
fn arm_claim(arm: &Json, key: &str) -> (&'static str, String, bool) {
    let cells: Vec<String> = arm
        .get("cells")
        .and_then(Json::as_array)
        .map(|cells| {
            cells
                .iter()
                .filter_map(|cell| cell.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    if cells.is_empty() {
        return (
            UNSTATED,
            "the arm declares no coverage cell".to_string(),
            true,
        );
    }
    let claims: Vec<(&'static str, String)> =
        cells.iter().map(|cell| cell_claim(cell, key)).collect();
    for wanted in CLAIM_PRECEDENCE {
        if let Some((_, why)) = claims.iter().find(|(claim, _)| claim == wanted) {
            return (wanted, why.clone(), *wanted == UNSTATED);
        }
    }
    // Every cell answers one of the three; the precedence is exhaustive.
    unreachable!("no claim outcome for {key}: {claims:?}")
}

/// What an *unstated* pair's verdict rests on, the declaration deciding nothing.
///
/// The key's measured floor when it has one and the baseline sits inside it; the
/// 50 % count tolerance otherwise; and for a key with no floor at all, that same
/// tolerance — which is where a claimed pair would be too, so the gap weakens
/// nothing.
fn decided_by(key: &str, baseline: &Scalar) -> &'static str {
    let floor = floor_bytes(key);
    if floor == 0 {
        return "no-floor";
    }
    if baseline.number().is_some_and(|value| value < floor as f64) {
        "floor"
    } else {
        "tolerance"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Regression,
    Move,
}

fn signed_percent(change: f64) -> String {
    format!("{:+.1}%", change * 100.0)
}

fn tolerance_percent(tolerance: f64) -> String {
    format!("{:.0}%", tolerance * 100.0)
}

/// One counted quantity as `(outcome, text)`.
///
/// The fall is compared with `<=` so that a shortening which takes exactly half
/// an arm's samples — the case the instrument exists for — is a regression, and
/// the tolerance is the run-to-run spread the comparison is not allowed to read
/// as coverage loss.
fn compare_counter(
    key: &str,
    baseline: &Scalar,
    candidate: Option<&Scalar>,
    tolerance: f64,
    claim: &str,
    floor: u64,
    why: &str,
) -> (Option<Outcome>, String) {
    let baseline_text = baseline.display();
    let measured = candidate.filter(|value| !matches!(value, Scalar::Null));
    if claim == IDLE {
        let Some(candidate) = measured else {
            return (
                Some(Outcome::Move),
                format!(
                    "{key} {baseline_text} -> not measured [the arm's cells declare \
                     the lane idle ({why}): reported, never compared]"
                ),
            );
        };
        if baseline.eq_scalar(candidate) {
            return (None, String::new());
        }
        let change = relative_change(baseline, candidate);
        let text = format!("{key} {baseline_text} -> {}", candidate.display())
            + &change
                .map(|change| format!(" ({})", signed_percent(change)))
                .unwrap_or_default();
        return (
            Some(Outcome::Move),
            format!(
                "{text} [the arm's cells declare the lane idle ({why}): reported, \
                 never compared]"
            ),
        );
    }
    let inside = claim == UNSTATED
        && floor != 0
        && baseline.number().is_some_and(|value| value < floor as f64);
    let Some(candidate) = measured else {
        if inside {
            return (
                Some(Outcome::Move),
                format!(
                    "{key} {baseline_text} -> not measured [the arm's cells leave the \
                     lane unstated ({why}), and the baseline sits inside the measured \
                     {floor}-byte floor: absent, not a regression]"
                ),
            );
        }
        return (
            Some(Outcome::Regression),
            format!("{key} {baseline_text} -> not measured"),
        );
    };
    let change = relative_change(baseline, candidate);
    let Some(change) = change else {
        return if baseline.eq_scalar(candidate) {
            (None, String::new())
        } else {
            (
                Some(Outcome::Move),
                format!("{key} {baseline_text} -> {}", candidate.display()),
            )
        };
    };
    if change <= -tolerance {
        if inside {
            return (
                Some(Outcome::Move),
                format!(
                    "{key} {baseline_text} -> {} ({}), past the {} tolerance but inside \
                     the measured {floor}-byte floor, which applies because the arm's \
                     cells leave the lane unstated ({why}): reported, not a regression",
                    candidate.display(),
                    signed_percent(change),
                    tolerance_percent(tolerance)
                ),
            );
        }
        let claimed_note = if claim == CLAIMED && floor_bytes(key) > 0 {
            format!(" [the arm's cells claim the lane ({why}), so no floor applies]")
        } else {
            String::new()
        };
        return (
            Some(Outcome::Regression),
            format!(
                "{key} {baseline_text} -> {} ({}), past the {} tolerance{claimed_note}",
                candidate.display(),
                signed_percent(change),
                tolerance_percent(tolerance)
            ),
        );
    }
    if !baseline.eq_scalar(candidate) {
        return (
            Some(Outcome::Move),
            format!(
                "{key} {baseline_text} -> {} ({})",
                candidate.display(),
                signed_percent(change)
            ),
        );
    }
    (None, String::new())
}

/// One statistic's movement as `(outcome, text)`.
///
/// A statistic the baseline measured and the candidate did not is a coverage
/// regression: the assertion that reads it has nothing to read. A statistic
/// that moved is a *value* change — reported always, and a failure only when the
/// caller asks for strictness.
fn compare_stat(
    key: &str,
    baseline: &Scalar,
    candidate: Option<&Scalar>,
    tolerance: f64,
) -> StatOutcome {
    let baseline_text = baseline.display();
    let measured = candidate.filter(|value| !matches!(value, Scalar::Null));
    let Some(candidate) = measured else {
        return StatOutcome::regression(format!("{key} {baseline_text} -> not measured"));
    };
    if baseline.eq_scalar(candidate) {
        return StatOutcome::nothing();
    }
    let change = relative_change(baseline, candidate);
    let text = format!("{key} {baseline_text} -> {}", candidate.display())
        + &change
            .map(|change| format!(" ({})", signed_percent(change)))
            .unwrap_or_default();
    if change.is_some_and(|change| change.abs() > tolerance) {
        return StatOutcome::drift(format!(
            "{text} [past the {} value tolerance]",
            tolerance_percent(tolerance)
        ));
    }
    StatOutcome::moved(text)
}

enum StatOutcome {
    Nothing,
    Moved(String),
    Drift(String),
    Regression(String),
}

impl StatOutcome {
    fn nothing() -> StatOutcome {
        StatOutcome::Nothing
    }

    fn moved(text: String) -> StatOutcome {
        StatOutcome::Moved(text)
    }

    fn drift(text: String) -> StatOutcome {
        StatOutcome::Drift(text)
    }

    fn regression(text: String) -> StatOutcome {
        StatOutcome::Regression(text)
    }
}

/// The delivery ratio: a fall is coverage loss, a rise is a value move.
fn compare_delivery(
    baseline: &Scalar,
    candidate: Option<&Scalar>,
    tolerance: f64,
) -> (Option<Outcome>, String) {
    let baseline_text = baseline.display();
    let measured = candidate.filter(|value| !matches!(value, Scalar::Null));
    let Some(candidate) = measured else {
        return (
            Some(Outcome::Regression),
            format!("delivery {baseline_text} -> not measured"),
        );
    };
    if baseline.eq_scalar(candidate) {
        return (None, String::new());
    }
    let change = candidate.number().unwrap_or_default() - baseline.number().unwrap_or_default();
    let text = format!(
        "delivery {baseline_text} -> {} ({change:+.3})",
        candidate.display()
    );
    if change < -tolerance {
        return (
            Some(Outcome::Regression),
            format!("{text}, past the {tolerance:.3} delivery tolerance"),
        );
    }
    (Some(Outcome::Move), text)
}

#[derive(Debug, Clone)]
struct Gap {
    arm: String,
    key: String,
    baseline: Scalar,
    cells: Vec<String>,
    why: String,
    floor_bytes: u64,
    decided_by: &'static str,
}

#[derive(Default)]
struct ArmChanges {
    regressions: Vec<String>,
    drifts: Vec<String>,
    moves: Vec<String>,
    gaps: Vec<Gap>,
}

/// A present, non-null JSON value as a scalar, or `None` for a missing key or a
/// JSON null. Python's `arm.get("sample_count")` returns `None` for both, and
/// the sample-count comparison is skipped entirely on a null baseline.
fn scalar_value(value: &Json) -> Option<Scalar> {
    match value {
        Json::Null => None,
        other => Some(Scalar::from_json(other)),
    }
}

/// One arm's changes, split into coverage regressions and value moves.
///
/// The claim read for every counter is the *baseline* arm's — the declaration
/// under test — and a candidate that drops the cell making a claim is caught by
/// the cell-coverage comparison, which fails when a declared cell is exercised
/// by no arm any more.
fn compare_arm(
    baseline_arm: &Json,
    candidate_arm: Option<&Json>,
    tolerances: &Tolerances,
) -> ArmChanges {
    let mut changes = ArmChanges::default();
    let baseline_samples = baseline_arm.get("sample_count").and_then(scalar_value);
    let candidate_samples = candidate_arm
        .and_then(|arm| arm.get("sample_count"))
        .and_then(scalar_value);
    if let Some(baseline_samples) = &baseline_samples {
        record(
            &mut changes,
            compare_counter(
                "sample_count",
                baseline_samples,
                candidate_samples.as_ref(),
                tolerances.count,
                CLAIMED,
                0,
                "",
            ),
        );
    }
    let baseline_counters = numbers(baseline_arm.get("counters"));
    let candidate_counters = numbers(candidate_arm.and_then(|arm| arm.get("counters")));
    for key in COVERAGE_COUNTER_KEYS {
        let Some(baseline_value) = baseline_counters.get(*key) else {
            continue;
        };
        let (claim, why, gap) = arm_claim(baseline_arm, key);
        if gap {
            changes.gaps.push(Gap {
                arm: baseline_arm
                    .get("id")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_string(),
                key: (*key).to_string(),
                baseline: baseline_value.clone(),
                cells: baseline_arm
                    .get("cells")
                    .and_then(Json::as_array)
                    .map(|cells| {
                        cells
                            .iter()
                            .filter_map(|cell| cell.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
                why: why.clone(),
                floor_bytes: floor_bytes(key),
                decided_by: decided_by(key, baseline_value),
            });
        }
        let candidate_value = candidate_counters.get(*key);
        record(
            &mut changes,
            compare_counter(
                key,
                baseline_value,
                candidate_value,
                tolerances.count,
                claim,
                if claim == UNSTATED {
                    floor_bytes(key)
                } else {
                    0
                },
                &why,
            ),
        );
    }
    let baseline_windows = numbers(baseline_arm.get("windows"));
    let candidate_windows = numbers(candidate_arm.and_then(|arm| arm.get("windows")));
    // The measured geometry (`window_seconds`, `elapsed_seconds`) is the one
    // deterministic shortening signal, so it is compared tightly; the observed
    // wall-clock includes fixture setup and teardown and varies like the other
    // counts.
    for key in COVERAGE_WINDOW_KEYS {
        let Some(baseline_value) = baseline_windows.get(*key) else {
            continue;
        };
        record(
            &mut changes,
            compare_counter(
                key,
                baseline_value,
                candidate_windows.get(*key),
                tolerances.window,
                CLAIMED,
                0,
                "",
            ),
        );
    }
    for key in COVERAGE_WALL_KEYS {
        let Some(baseline_value) = baseline_windows.get(*key) else {
            continue;
        };
        record(
            &mut changes,
            compare_counter(
                key,
                baseline_value,
                candidate_windows.get(*key),
                tolerances.count,
                CLAIMED,
                0,
                "",
            ),
        );
    }
    let baseline_stats = numbers(baseline_arm.get("stats"));
    let candidate_stats = numbers(candidate_arm.and_then(|arm| arm.get("stats")));
    for key in VALUE_STAT_KEYS {
        let Some(baseline_value) = baseline_stats.get(*key) else {
            continue;
        };
        match compare_stat(
            key,
            baseline_value,
            candidate_stats.get(*key),
            tolerances.value,
        ) {
            StatOutcome::Nothing => {}
            StatOutcome::Moved(text) => changes.moves.push(text),
            StatOutcome::Drift(text) => changes.drifts.push(text),
            StatOutcome::Regression(text) => changes.regressions.push(text),
        }
    }
    if let Some(baseline_value) = baseline_stats.get(DELIVERY_KEY) {
        let (outcome, text) = compare_delivery(
            baseline_value,
            candidate_stats.get(DELIVERY_KEY),
            tolerances.delivery,
        );
        if outcome == Some(Outcome::Regression) {
            changes.regressions.push(text);
        } else if outcome.is_some() {
            changes.moves.push(text);
        }
    }
    changes
}

fn record(changes: &mut ArmChanges, comparison: (Option<Outcome>, String)) {
    match comparison.0 {
        Some(Outcome::Regression) => changes.regressions.push(comparison.1),
        Some(Outcome::Move) => changes.moves.push(comparison.1),
        None => {}
    }
}

#[derive(Debug, Clone)]
struct CellEntry {
    cell: String,
    covered_by: Vec<String>,
    regression: bool,
}

/// Every cell the baseline exercised must still be exercised by an arm.
///
/// The cells are the declared coverage the arms carry; a cell whose every arm
/// has been removed is the coverage the shortening dropped, however green the
/// remaining arms are.
fn compare_cells(
    baseline: &Report,
    candidate: &Report,
    problems: &mut Vec<String>,
) -> Vec<CellEntry> {
    let baseline_cells = cells_for(baseline);
    let candidate_cells = cells_for(candidate);
    for (role, report) in [("baseline", baseline), ("candidate", candidate)] {
        for (cell, covered_by) in cells_for(report) {
            if let Some(problem) = cell_problem(&cell) {
                problems.push(format!(
                    "the {role} declares the coverage cell '{cell}' (covered by {}), \
                     which is not <property>@<dimension>=<value>: {problem}",
                    covered_by.join(", ")
                ));
            }
        }
    }
    baseline_cells
        .iter()
        .map(|(cell, covered_by)| CellEntry {
            cell: cell.clone(),
            covered_by: covered_by.clone(),
            regression: !candidate_cells.contains_key(cell),
        })
        .collect()
}

/// `cell -> sorted arm ids` over a report's arms.
fn cells_for(report: &Report) -> BTreeMap<String, Vec<String>> {
    let mut covered: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (id, arm) in &report.arms {
        if let Some(cells) = arm.get("cells").and_then(Json::as_array) {
            for cell in cells {
                if let Some(cell) = cell.as_str() {
                    covered
                        .entry(cell.to_string())
                        .or_default()
                        .push(id.clone());
                }
            }
        }
    }
    for ids in covered.values_mut() {
        ids.sort();
    }
    covered
}

/// Why `cell` is not `<property>@<dimension>=<value>[+...]`, or `None`.
///
/// `netem-tools check-gate`'s `cell_problem` is the grammar's authority; this
/// repeats its shape so the Rust comparison can validate a report on its own.
fn cell_problem(cell: &str) -> Option<String> {
    let (property, dimensions) = match cell.split_once('@') {
        Some((property, dimensions)) if !dimensions.is_empty() => (property, dimensions),
        _ => return Some("no '@<dimension>=<value>' part".to_string()),
    };
    if !is_property_name(property) {
        return Some(format!(
            "property '{property}' is not a name ([A-Za-z][A-Za-z0-9_.-]*)"
        ));
    }
    for part in dimensions.split('+') {
        if parse_dimension_part(part).is_none() {
            return Some(format!("dimension '{part}' is not '<dimension>=<value>'"));
        }
    }
    None
}

/// `[A-Za-z][A-Za-z0-9_.-]*`, the property-name shape.
fn is_property_name(name: &str) -> bool {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    first.is_ascii_alphabetic()
        && characters.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

#[derive(Debug, Clone, Copy)]
struct Tolerances {
    count: f64,
    window: f64,
    delivery: f64,
    value: f64,
}

#[derive(Debug, Clone)]
struct Summary {
    path: String,
    schema: String,
    quick: bool,
    arms: usize,
    samples: Scalar,
    revision: Option<String>,
    producers: Vec<String>,
}

#[derive(Debug, Clone)]
struct Problem {
    arm: Option<String>,
    changes: Vec<String>,
}

#[derive(Debug, Clone)]
struct ArmEntry {
    id: String,
    mandate: Option<String>,
    missing: bool,
    sample_count: Scalar,
    candidate_sample_count: Scalar,
    regressions: Vec<String>,
    drifts: Vec<String>,
    moves: Vec<String>,
    gaps: Vec<Gap>,
}

/// The whole diff: what the verdict block prints and what `--json-out` writes.
#[derive(Debug, Clone)]
pub struct Diff {
    baseline: String,
    candidate: String,
    tolerances: Tolerances,
    claim_gaps: Vec<Gap>,
    baseline_summary: Summary,
    candidate_summary: Summary,
    arms: Vec<ArmEntry>,
    new_arms: Vec<String>,
    cells: Vec<CellEntry>,
    regressions: Vec<Problem>,
    value_drifts: Vec<Problem>,
    exit_code: i32,
}

fn summary_of(report: &Report) -> Summary {
    let payload = &report.payload;
    let schema = payload
        .get("schema")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();
    Summary {
        path: report.path.display().to_string(),
        schema,
        quick: report.quick(),
        arms: report.arms.len(),
        samples: sum_samples(report),
        revision: payload
            .get("rtp_mux")
            .and_then(|rtp_mux| rtp_mux.get("revision"))
            .and_then(Json::as_str)
            .filter(|revision| !revision.is_empty())
            .map(str::to_string),
        producers: producers_of(payload, &report.arms),
    }
}

/// Python's `sum(sample_count or 0)`: an integer sum stays an integer, and an
/// absent or null sample count contributes nothing.
fn sum_samples(report: &Report) -> Scalar {
    let mut total = 0.0f64;
    let mut integral = true;
    for arm in report.arms.values() {
        match arm.get("sample_count") {
            Some(Json::Int(value)) => total += *value as f64,
            Some(Json::Float(value)) => {
                total += value;
                integral = false;
            }
            _ => {}
        }
    }
    Scalar::Num(if integral {
        Num::int(total as i64)
    } else {
        Num::float(total)
    })
}

/// The producer ids a report covers, in its own order.
///
/// A `mandate-check/5` report names the producers it declared and selected, so
/// the comparison can say which producers its verdict covers. A `/3` or `/4`
/// report has no producer record at all (it predates the second producer), so
/// the ids are read from the arms' own `producer` field — and when even that is
/// absent the list is empty, which the verdict block prints as `unnamed` rather
/// than as a producer this comparison invented.
fn producers_of(payload: &Json, arms: &BTreeMap<String, Json>) -> Vec<String> {
    if let Some(declared) = payload.get("producers").and_then(Json::as_object) {
        if let Some(selected) = payload.get("producers_selected").and_then(Json::as_array)
            && !selected.is_empty()
        {
            return selected
                .iter()
                .map(|entry| match entry {
                    Json::Str(text) => text.clone(),
                    other => json_display(other),
                })
                .collect();
        }
        return declared.keys().cloned().collect();
    }
    let mut producers: Vec<String> = arms
        .values()
        .filter_map(|arm| arm.get("producer").and_then(Json::as_str))
        .map(str::to_string)
        .collect();
    producers.sort();
    producers.dedup();
    producers
}

/// The comparison's resolved options.
///
/// The flag *surface* is the binary's — `netem-tools`'s `mandate-compare`
/// subcommand is a clap `derive` struct that converts into this one, so the
/// library carries no parser and a consumer that never asks for the `cli`
/// feature never builds one. Every field is public for that conversion.
#[derive(Debug, Clone)]
pub struct Args {
    pub report: Option<PathBuf>,
    pub baseline: Option<PathBuf>,
    pub count_tolerance: f64,
    pub window_tolerance: f64,
    pub delivery_tolerance: f64,
    pub value_tolerance: f64,
    pub fail_on_value_drift: bool,
    pub json_out: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Args {
        Args {
            report: None,
            baseline: None,
            count_tolerance: DEFAULT_COUNT_TOLERANCE,
            window_tolerance: DEFAULT_WINDOW_TOLERANCE,
            delivery_tolerance: DEFAULT_DELIVERY_TOLERANCE,
            value_tolerance: DEFAULT_VALUE_TOLERANCE,
            fail_on_value_drift: false,
            json_out: None,
        }
    }
}

/// The whole diff, as a dict, or a [`MandateCompareError`].
fn compare(args: &Args) -> Result<Diff> {
    let mut problems = Vec::new();
    let baseline = load_report(
        args.baseline
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_BASELINE_NAME)),
        "baseline",
    )?;
    let candidate = load_report(
        args.report
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_REPORT_NAME)),
        "candidate",
    )?;
    let tolerances = Tolerances {
        count: args.count_tolerance,
        window: args.window_tolerance,
        delivery: args.delivery_tolerance,
        value: args.value_tolerance,
    };
    for (name, value) in [
        ("count", tolerances.count),
        ("window", tolerances.window),
        ("delivery", tolerances.delivery),
        ("value", tolerances.value),
    ] {
        if value < 0.0 {
            return err(format!("--{name}-tolerance must not be negative"));
        }
    }
    if baseline.quick() != candidate.quick() {
        return err(format!(
            "the baseline was recorded with {} and the candidate with {}, so their \
             sample counts and windows measure different sets; record the candidate \
             the same way as the baseline",
            quick_phrase(baseline.quick()),
            quick_phrase(candidate.quick())
        ));
    }
    let baseline_cells = compare_cells(&baseline, &candidate, &mut problems);
    if !problems.is_empty() {
        return err(problems.join("; "));
    }

    let mut arms = Vec::new();
    for (arm_id, baseline_arm) in &baseline.arms {
        let candidate_arm = candidate.arms.get(arm_id);
        let (regressions, drifts, moves, gaps) = match candidate_arm {
            None => (
                vec!["the arm is in the baseline and absent from the candidate".to_string()],
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            Some(candidate_arm) => {
                let changes = compare_arm(baseline_arm, Some(candidate_arm), &tolerances);
                (
                    changes.regressions,
                    changes.drifts,
                    changes.moves,
                    changes.gaps,
                )
            }
        };
        arms.push(ArmEntry {
            id: arm_id.clone(),
            mandate: baseline_arm
                .get("mandate")
                .and_then(Json::as_str)
                .map(str::to_string),
            missing: candidate_arm.is_none(),
            sample_count: baseline_arm
                .get("sample_count")
                .map(Scalar::from_json)
                .unwrap_or(Scalar::Null),
            candidate_sample_count: candidate_arm
                .and_then(|arm| arm.get("sample_count"))
                .map(Scalar::from_json)
                .unwrap_or(Scalar::Null),
            regressions,
            drifts,
            moves,
            gaps,
        });
    }
    let new_arms: Vec<String> = candidate
        .arms
        .keys()
        .filter(|id| !baseline.arms.contains_key(*id))
        .cloned()
        .collect();
    let mut regressions: Vec<Problem> = arms
        .iter()
        .filter(|arm| !arm.regressions.is_empty())
        .map(|arm| Problem {
            arm: Some(arm.id.clone()),
            changes: arm.regressions.clone(),
        })
        .collect();
    for cell in &baseline_cells {
        if cell.regression {
            regressions.push(Problem {
                arm: None,
                changes: vec![format!(
                    "the coverage cell '{}' was exercised by {} and is exercised by no arm now",
                    cell.cell,
                    cell.covered_by.join(", ")
                )],
            });
        }
    }
    let baseline_mandates = mandate_keys(&baseline.payload);
    let candidate_mandates = mandate_keys(&candidate.payload);
    for mandate in &baseline_mandates {
        if !candidate_mandates.contains(mandate) {
            regressions.push(Problem {
                arm: None,
                changes: vec![format!(
                    "the mandate {mandate} is in the baseline and not in the candidate"
                )],
            });
        }
    }
    let value_drifts: Vec<Problem> = arms
        .iter()
        .filter(|arm| !arm.drifts.is_empty())
        .map(|arm| Problem {
            arm: Some(arm.id.clone()),
            changes: arm.drifts.clone(),
        })
        .collect();
    let claim_gaps: Vec<Gap> = arms.iter().flat_map(|arm| arm.gaps.clone()).collect();
    Ok(Diff {
        baseline: baseline.path.display().to_string(),
        candidate: candidate.path.display().to_string(),
        tolerances,
        claim_gaps,
        baseline_summary: summary_of(&baseline),
        candidate_summary: summary_of(&candidate),
        arms,
        new_arms,
        cells: baseline_cells,
        regressions,
        value_drifts,
        exit_code: EXIT_OK,
    })
}

fn quick_phrase(quick: bool) -> &'static str {
    if quick { "--quick" } else { "the full windows" }
}

fn mandate_keys(payload: &Json) -> Vec<String> {
    payload
        .get("mandates")
        .and_then(Json::as_object)
        .map(|mandates| mandates.keys().cloned().collect())
        .unwrap_or_default()
}

/// The short 'what moved' tail of an arm line.
fn quantities(entry: &ArmEntry) -> String {
    let mut parts = entry.moves.clone();
    parts.extend(
        entry
            .drifts
            .iter()
            .map(|text| format!("{text} [value drift]")),
    );
    parts.join("; ")
}

/// The verdict block, exactly as the Python tool printed it.
pub fn verdict_lines(diff: &Diff) -> Vec<String> {
    let mut lines = vec![
        format!("mandate-compare: baseline {}", diff.baseline),
        format!("                 candidate {}", diff.candidate),
    ];
    for (role, summary) in [
        ("baseline", &diff.baseline_summary),
        ("candidate", &diff.candidate_summary),
    ] {
        lines.push(format!(
            "  {role:>9}: schema={} quick={} arms={} samples={} revision={}",
            summary.schema,
            if summary.quick { "yes" } else { "no" },
            summary.arms,
            summary.samples.display(),
            summary.revision.as_deref().unwrap_or("unresolved")
        ));
        let producers = if summary.producers.is_empty() {
            "unnamed".to_string()
        } else {
            summary.producers.join(", ")
        };
        lines.push(format!(
            "  {:>9}  producers: {producers} ({} covered)",
            "",
            summary.producers.len()
        ));
    }
    lines.push(format!(
        "  tolerances: count {:.0}%, window {:.1}%, delivery {:.3}, value {:.0}%",
        diff.tolerances.count * 100.0,
        diff.tolerances.window * 100.0,
        diff.tolerances.delivery,
        diff.tolerances.value * 100.0
    ));
    let mut lanes: Vec<&str> = COUNTER_LANES.iter().map(|(name, _)| *name).collect();
    lanes.sort_unstable();
    lines.push(
        "   claims: a counter is load-bearing for an arm when the arm's declared cells \
         claim the lane it measures"
            .to_string(),
    );
    lines.push(format!(
        "           claimed by {}; idle (reported, never compared) by {}; unstated \
         otherwise. Iterated over an arm's cells: {} over an arm's cells",
        claim_rule_claimed(),
        claim_rule_idle(),
        CLAIM_PRECEDENCE.join(" > ")
    ));
    lines.push(format!(
        "           the bulk-lane keys are {}; every other compared counter measures \
         the arm's own lane",
        lanes.join(", ")
    ));
    if !COUNT_FLOORS_BYTES.is_empty() {
        let mut floors: Vec<(&str, u64)> = COUNT_FLOORS_BYTES.to_vec();
        floors.sort_unstable();
        lines.push(format!(
            "    floors: {} (bytes) — applied only where the arm's cells leave the lane \
             unstated; a claiming cell is compared with no floor, an idle one not at all",
            floors
                .iter()
                .map(|(key, value)| format!("{key} {value}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let gaps = &diff.claim_gaps;
    let on_floor = gaps.iter().filter(|gap| gap.decided_by == "floor").count();
    lines.push(format!(
        "      gaps: {} unstated arm x counter pair(s) — the declaration decides nothing \
         for these; {on_floor} of them sit inside their floor, where that (not the \
         declaration) keeps a residue from failing",
        gaps.len()
    ));
    for gap in gaps.iter().take(MAX_GAP_LINES) {
        let note = match gap.decided_by {
            "floor" => format!(
                "inside the {}-byte floor: reported, not failed",
                gap.floor_bytes
            ),
            "tolerance" => {
                "above its floor: compared by magnitude, not by the declaration".to_string()
            }
            _ => "no floor on this key: compared exactly as a claimed counter".to_string(),
        };
        lines.push(format!(
            "            {} {} = {} [{note}; {}]",
            gap.arm,
            gap.key,
            gap.baseline.display(),
            gap.why
        ));
    }
    if gaps.len() > MAX_GAP_LINES {
        lines.push(format!(
            "            ... {} more gap(s)",
            gaps.len() - MAX_GAP_LINES
        ));
    }
    lines.push("arms:".to_string());
    for entry in diff.arms.iter().take(MAX_ARM_LINES) {
        let status = if entry.regressions.is_empty() {
            "ok  "
        } else {
            "LOSS"
        };
        let samples = entry.sample_count.display();
        if entry.missing {
            lines.push(format!(
                "  {status} {:<24} {samples} sample(s) -> absent: coverage regression",
                entry.id
            ));
            continue;
        }
        let moved = quantities(entry);
        let verdict = if !entry.regressions.is_empty() {
            entry.regressions.join("; ")
        } else if !moved.is_empty() {
            moved
        } else {
            "nothing moved".to_string()
        };
        lines.push(format!(
            "  {status} {:<24} samples {samples} -> {}: {verdict}",
            entry.id,
            entry.candidate_sample_count.display()
        ));
    }
    if diff.arms.len() > MAX_ARM_LINES {
        lines.push(format!(
            "  ... {} more arm(s)",
            diff.arms.len() - MAX_ARM_LINES
        ));
    }
    let regressed_cells: Vec<&CellEntry> =
        diff.cells.iter().filter(|cell| cell.regression).collect();
    lines.push(format!(
        "cells: {} declared cell(s) exercised, {} no longer covered",
        diff.cells.len(),
        regressed_cells.len()
    ));
    for cell in &regressed_cells {
        lines.push(format!(
            "  LOSS {} (was {})",
            cell.cell,
            cell.covered_by.join(", ")
        ));
    }
    if !diff.new_arms.is_empty() {
        lines.push(format!(
            "new arms (reported, not a regression): {}",
            diff.new_arms.join(", ")
        ));
    }
    let regressions: usize = diff
        .regressions
        .iter()
        .map(|entry| entry.changes.len())
        .sum();
    let drifts: usize = diff
        .value_drifts
        .iter()
        .map(|entry| entry.changes.len())
        .sum();
    lines.push(format!(
        "verdict: {}  exit={}  coverage regression(s)={regressions}  value drift(s)={drifts}",
        if regressions > 0 {
            "COVERAGE-LOSS"
        } else {
            "OK"
        },
        diff.exit_code
    ));
    for entry in &diff.regressions {
        for change in &entry.changes {
            lines.push(format!("problem: {change}"));
        }
    }
    lines
}

fn claim_rule_claimed() -> String {
    format!(
        "{LANE_DIMENSION}=<the counter's lane> (the lane is the arm's own) or \
         {}=<value other than {}> (the arm offers a workload on that lane)",
        LOAD_DIMENSIONS.join(" or "),
        IDLE_LOAD_VALUES.join(", ")
    )
}

fn claim_rule_idle() -> String {
    format!(
        "{}=<{}> (the cell declares that lane carries no workload)",
        LOAD_DIMENSIONS.join(" or "),
        IDLE_LOAD_VALUES.join(", ")
    )
}

fn diff_to_json(diff: &Diff) -> Json {
    let mut root = BTreeMap::new();
    root.insert("baseline".to_string(), Json::Str(diff.baseline.clone()));
    root.insert("candidate".to_string(), Json::Str(diff.candidate.clone()));
    let mut tolerances = BTreeMap::new();
    tolerances.insert("count".to_string(), Json::Float(diff.tolerances.count));
    tolerances.insert("window".to_string(), Json::Float(diff.tolerances.window));
    tolerances.insert(
        "delivery".to_string(),
        Json::Float(diff.tolerances.delivery),
    );
    tolerances.insert("value".to_string(), Json::Float(diff.tolerances.value));
    root.insert("tolerances".to_string(), Json::Object(tolerances));
    let mut floors = BTreeMap::new();
    for (key, value) in COUNT_FLOORS_BYTES {
        floors.insert((*key).to_string(), Json::Int(*value as i64));
    }
    root.insert("count_floors".to_string(), Json::Object(floors));
    let mut lanes = BTreeMap::new();
    for (key, lane) in COUNTER_LANES {
        lanes.insert((*key).to_string(), Json::Str((*lane).to_string()));
    }
    let mut rule = BTreeMap::new();
    rule.insert("counter_lanes".to_string(), Json::Object(lanes));
    rule.insert("claimed".to_string(), Json::Str(claim_rule_claimed()));
    rule.insert("idle".to_string(), Json::Str(claim_rule_idle()));
    rule.insert(
        "unstated".to_string(),
        Json::Str(
            "neither; the pair is reported in claim_gaps and its counter is compared \
             under the key's measured floor"
                .to_string(),
        ),
    );
    rule.insert(
        "combine".to_string(),
        Json::Str(format!(
            "{} over an arm's cells",
            CLAIM_PRECEDENCE.join(" > ")
        )),
    );
    root.insert("claim_rule".to_string(), Json::Object(rule));
    root.insert(
        "claim_gaps".to_string(),
        Json::Array(diff.claim_gaps.iter().map(gap_to_json).collect()),
    );
    root.insert(
        "baseline_summary".to_string(),
        summary_to_json(&diff.baseline_summary),
    );
    root.insert(
        "candidate_summary".to_string(),
        summary_to_json(&diff.candidate_summary),
    );
    root.insert(
        "arms".to_string(),
        Json::Array(diff.arms.iter().map(arm_to_json).collect()),
    );
    root.insert(
        "new_arms".to_string(),
        Json::Array(
            diff.new_arms
                .iter()
                .map(|arm| Json::Str(arm.clone()))
                .collect(),
        ),
    );
    root.insert(
        "cells".to_string(),
        Json::Array(
            diff.cells
                .iter()
                .map(|cell| {
                    let mut entry = BTreeMap::new();
                    entry.insert("cell".to_string(), Json::Str(cell.cell.clone()));
                    entry.insert(
                        "covered_by".to_string(),
                        Json::Array(
                            cell.covered_by
                                .iter()
                                .map(|id| Json::Str(id.clone()))
                                .collect(),
                        ),
                    );
                    entry.insert("regression".to_string(), Json::Bool(cell.regression));
                    Json::Object(entry)
                })
                .collect(),
        ),
    );
    root.insert(
        "regressions".to_string(),
        Json::Array(diff.regressions.iter().map(problem_to_json).collect()),
    );
    root.insert(
        "value_drifts".to_string(),
        Json::Array(diff.value_drifts.iter().map(problem_to_json).collect()),
    );
    root.insert("exit_code".to_string(), Json::Int(diff.exit_code as i64));
    Json::Object(root)
}

fn gap_to_json(gap: &Gap) -> Json {
    let mut entry = BTreeMap::new();
    entry.insert("arm".to_string(), Json::Str(gap.arm.clone()));
    entry.insert("key".to_string(), Json::Str(gap.key.clone()));
    entry.insert("baseline".to_string(), json_of_scalar(&gap.baseline));
    entry.insert(
        "cells".to_string(),
        Json::Array(
            gap.cells
                .iter()
                .map(|cell| Json::Str(cell.clone()))
                .collect(),
        ),
    );
    entry.insert("why".to_string(), Json::Str(gap.why.clone()));
    entry.insert("floor_bytes".to_string(), Json::Int(gap.floor_bytes as i64));
    entry.insert(
        "decided_by".to_string(),
        Json::Str(gap.decided_by.to_string()),
    );
    Json::Object(entry)
}

/// The JSON value a scalar carries (the comparison only ever files numeric
/// counters as a gap's baseline, and sample counts as a scalar).
fn json_of_scalar(scalar: &Scalar) -> Json {
    match scalar {
        Scalar::Num(number) => number.to_json(),
        Scalar::Null => Json::Null,
        Scalar::Text(text) => Json::Str(text.clone()),
    }
}

fn summary_to_json(summary: &Summary) -> Json {
    let mut entry = BTreeMap::new();
    entry.insert("path".to_string(), Json::Str(summary.path.clone()));
    entry.insert("schema".to_string(), Json::Str(summary.schema.clone()));
    entry.insert("quick".to_string(), Json::Bool(summary.quick));
    entry.insert("arms".to_string(), Json::Int(summary.arms as i64));
    entry.insert("samples".to_string(), json_of_scalar(&summary.samples));
    entry.insert(
        "revision".to_string(),
        summary
            .revision
            .clone()
            .map(Json::Str)
            .unwrap_or(Json::Null),
    );
    entry.insert(
        "producers".to_string(),
        Json::Array(
            summary
                .producers
                .iter()
                .map(|producer| Json::Str(producer.clone()))
                .collect(),
        ),
    );
    Json::Object(entry)
}

fn arm_to_json(arm: &ArmEntry) -> Json {
    let mut entry = BTreeMap::new();
    entry.insert("id".to_string(), Json::Str(arm.id.clone()));
    entry.insert(
        "mandate".to_string(),
        arm.mandate.clone().map(Json::Str).unwrap_or(Json::Null),
    );
    entry.insert("missing".to_string(), Json::Bool(arm.missing));
    entry.insert(
        "sample_count".to_string(),
        json_of_scalar(&arm.sample_count),
    );
    entry.insert(
        "candidate_sample_count".to_string(),
        json_of_scalar(&arm.candidate_sample_count),
    );
    entry.insert(
        "regressions".to_string(),
        Json::Array(
            arm.regressions
                .iter()
                .map(|text| Json::Str(text.clone()))
                .collect(),
        ),
    );
    entry.insert(
        "drifts".to_string(),
        Json::Array(
            arm.drifts
                .iter()
                .map(|text| Json::Str(text.clone()))
                .collect(),
        ),
    );
    entry.insert(
        "moves".to_string(),
        Json::Array(
            arm.moves
                .iter()
                .map(|text| Json::Str(text.clone()))
                .collect(),
        ),
    );
    entry.insert(
        "gaps".to_string(),
        Json::Array(arm.gaps.iter().map(gap_to_json).collect()),
    );
    Json::Object(entry)
}

fn problem_to_json(problem: &Problem) -> Json {
    let mut entry = BTreeMap::new();
    entry.insert(
        "arm".to_string(),
        problem.arm.clone().map(Json::Str).unwrap_or(Json::Null),
    );
    entry.insert(
        "changes".to_string(),
        Json::Array(
            problem
                .changes
                .iter()
                .map(|text| Json::Str(text.clone()))
                .collect(),
        ),
    );
    Json::Object(entry)
}

/// The `mandate-compare` subcommand's body: run the comparison for
/// already-parsed [`Args`], print the verdict block and (with `--json-out`)
/// write the diff.
///
/// The flags are parsed by the `netem-tools` binary with clap; this is the
/// library entry point that binary calls, so the comparison has one
/// implementation whether it is reached from the command line or from
/// [`coverage_verdict`].
pub fn main(args: Args) -> i32 {
    let stdout = io::stdout();
    let stderr = io::stderr();
    run(args, &mut stdout.lock(), &mut stderr.lock())
}

/// The subcommand's body, writing to the given sinks so a test can read what a
/// run would print instead of inferring it from the files it wrote.
fn run(mut args: Args, out: &mut impl Write, err: &mut impl Write) -> i32 {
    args.report = Some(
        args.report
            .unwrap_or_else(|| PathBuf::from(DEFAULT_REPORT_NAME)),
    );
    args.baseline = Some(args.baseline.unwrap_or_else(default_baseline));
    let mut diff = match compare(&args) {
        Ok(diff) => diff,
        Err(error) => {
            let _ = writeln!(err, "mandate-compare: error: {error}");
            return EXIT_UNCOMPARABLE;
        }
    };
    if !diff.regressions.is_empty() {
        diff.exit_code = EXIT_COVERAGE_REGRESSION;
    } else if !diff.value_drifts.is_empty() && args.fail_on_value_drift {
        diff.exit_code = EXIT_VALUE_DRIFT;
    }
    for line in verdict_lines(&diff) {
        let _ = writeln!(out, "{line}");
    }
    if let Some(path) = &args.json_out {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            let _ = fs::create_dir_all(parent);
        }
        let mut text = json::to_string(&diff_to_json(&diff));
        text.push('\n');
        if let Err(error) = fs::write(path, text) {
            let _ = writeln!(
                err,
                "mandate-compare: error: cannot write {}: {error}",
                path.display()
            );
            return EXIT_UNCOMPARABLE;
        }
        let _ = writeln!(out, "diff:     {}", resolve_path(path).display());
    }
    diff.exit_code
}

/// The committed baseline this tool defaults to. It belongs to the crate that
/// owns the mandate — the producer whose registry entry declares one — and is
/// resolved against that producer's own checkout, so this tool names no crate.
fn default_baseline() -> PathBuf {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut problems = Vec::new();
    if let Some(entries) = crate::tools::mandate_check::producers::load_producer_declaration(
        &workspace.join("tools").join(PRODUCERS_DECLARATION_NAME),
        &mut problems,
    ) {
        for entry in entries {
            if let Some(baseline) = &entry.baseline {
                return crate::tools::mandate_check::producers::producer_checkout(&entry)
                    .join(baseline);
            }
        }
    }
    workspace.join("tools").join(DEFAULT_BASELINE_NAME)
}

/// The coverage/claim verdict for two reports, for `perf-history`'s reuse.
///
/// This is the same comparison the subcommand runs, returned as the lines the
/// subcommand would print (or the single error line), so the two callers cannot
/// disagree about the semantics.
pub fn coverage_verdict(candidate_report: &Path, baseline_report: &Path) -> Vec<String> {
    let args = Args {
        report: Some(candidate_report.to_path_buf()),
        baseline: Some(baseline_report.to_path_buf()),
        ..Args::default()
    };
    match compare(&args) {
        Ok(mut diff) => {
            if !diff.regressions.is_empty() {
                diff.exit_code = EXIT_COVERAGE_REGRESSION;
            } else if !diff.value_drifts.is_empty() && args.fail_on_value_drift {
                diff.exit_code = EXIT_VALUE_DRIFT;
            }
            verdict_lines(&diff)
        }
        Err(error) => vec![format!("mandate-compare: error: {error}")],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    const M1_CELL: &str = "M1@impairment=clean+lane=dual+metric=p99";
    const M4_CELL: &str = "M4@lane=dual+flows=4+metric=per-flow-share";
    const PROBE_CELL: &str = "probe-forwarding@metric=throughput+layer=netem-runner";
    // The recorded arms whose bulk-lane counters the floor was derived from: the
    // `M1/lone_tail` cell drives no bulk load (its residue read 1920 B and then
    // 785 B between two runs of the unchanged tree), and the `M1/clean` cell
    // drives 2 MiB / 3 s of it. Both say `lane=dual`, which is why the
    // declaration cannot tell them apart and the floor survives for that pair.
    const LONE_TAIL_CELL: &str = "M1@impairment=gilbert-elliott-5-8+jitter=100ms+lane=dual+shape=request-response+depth=1+flows=1+scale=256B+metric=p99";
    const CLEAN_CELL: &str = "M1@impairment=loss2pct-iid+latency=25ms+jitter=5ms+lane=dual+shape=cadence+flows=1+scale=256B+metric=p99";
    // A hypothetical future arm whose cell *does* claim the bulk lane, by each of
    // the forms the grammar is used to express one.
    const CLAIMING_CELLS: &[(&str, &str)] = &[
        (
            "lane=bulk",
            "M3@lane=bulk+rate=8MiBps+burst=2MiB+period=3s+reps=3+metric=capacity-fraction",
        ),
        ("load=bulk", "M1@lane=dual+load=bulk+metric=p99"),
        (
            "bulk=shared",
            "hol@rate=400kbps+loss=iid1+bulk=shared+metric=p99",
        ),
    ];
    // Cells that declare the bulk lane idle, the two forms the producers use.
    const IDLE_CELLS: &[(&str, &str)] = &[
        (
            "load=none",
            "M4@impairment=clean+lane=dual+flows=4+shape=cadence+load=none+metric=per-flow-share",
        ),
        (
            "bulk=none",
            "hol@rate=400kbps+loss=iid1+bulk=none+metric=p99",
        ),
    ];
    const IDLE_CELL: &str =
        "M4@impairment=clean+lane=dual+flows=4+shape=cadence+load=none+metric=per-flow-share";

    static TOOL_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn int(value: i64) -> Json {
        Json::Int(value)
    }

    fn float(value: f64) -> Json {
        Json::Float(value)
    }

    fn text(value: &str) -> Json {
        Json::Str(value.to_string())
    }

    fn object(pairs: Vec<(&str, Json)>) -> Json {
        Json::Object(
            pairs
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        )
    }

    fn obj_mut(value: &mut Json) -> &mut BTreeMap<String, Json> {
        match value {
            Json::Object(map) => map,
            _ => panic!("not an object"),
        }
    }

    fn array_mut(value: &mut Json) -> &mut Vec<Json> {
        match value {
            Json::Array(items) => items,
            _ => panic!("not an array"),
        }
    }

    fn get_mut<'a>(value: &'a mut Json, key: &str) -> &'a mut Json {
        obj_mut(value)
            .get_mut(key)
            .unwrap_or_else(|| panic!("no key {key}"))
    }

    fn set(value: &mut Json, key: &str, new_value: Json) {
        obj_mut(value).insert(key.to_string(), new_value);
    }

    fn delete(value: &mut Json, key: &str) {
        obj_mut(value).remove(key);
    }

    fn arm_at(report: &mut Json, index: usize) -> &mut Json {
        &mut array_mut(get_mut(report, "arms"))[index]
    }

    fn arm_with_id<'a>(report: &'a mut Json, id: &str) -> &'a mut Json {
        array_mut(get_mut(report, "arms"))
            .iter_mut()
            .find(|arm| arm.get("id").and_then(Json::as_str) == Some(id))
            .unwrap_or_else(|| panic!("no arm {id}"))
    }

    fn set_in(arm: &mut Json, section: &str, key: &str, new_value: Json) {
        set(get_mut(arm, section), key, new_value);
    }

    fn delete_in(arm: &mut Json, section: &str, key: &str) {
        delete(get_mut(arm, section), key);
    }

    /// One arm record in the shape `tools/mandate-check` writes.
    #[derive(Clone)]
    struct ArmSpec {
        id: String,
        sample_count: Json,
        wire: i64,
        window: f64,
        wall: f64,
        p50: Json,
        p99: Json,
        p999: Json,
        delivery: f64,
        cells: Vec<String>,
        counters: Vec<(String, Json)>,
        producer: Option<String>,
    }

    impl ArmSpec {
        fn new(id: &str) -> ArmSpec {
            ArmSpec {
                id: id.to_string(),
                sample_count: int(2400),
                wire: 126000,
                window: 12.0,
                wall: 12.4,
                p50: float(25.0),
                p99: float(90.0),
                p999: float(98.0),
                delivery: 1.0,
                cells: vec![M1_CELL.to_string()],
                counters: Vec::new(),
                producer: None,
            }
        }

        fn sample(mut self, value: Json) -> ArmSpec {
            self.sample_count = value;
            self
        }

        fn wire(mut self, value: i64) -> ArmSpec {
            self.wire = value;
            self
        }

        fn p99(mut self, value: Json) -> ArmSpec {
            self.p99 = value;
            self
        }

        fn cells(mut self, cells: &[&str]) -> ArmSpec {
            self.cells = cells.iter().map(|cell| cell.to_string()).collect();
            self
        }

        fn producer(mut self, producer: &str) -> ArmSpec {
            self.producer = Some(producer.to_string());
            self
        }

        fn build(self) -> Json {
            let (mandate, label) = self.id.split_once('/').unwrap_or((&self.id, ""));
            let mut counters: BTreeMap<String, Json> = [
                ("sent".to_string(), self.sample_count.clone()),
                ("received".to_string(), self.sample_count.clone()),
                ("wire_bytes".to_string(), int(self.wire)),
            ]
            .into_iter()
            .collect();
            for (key, value) in self.counters {
                counters.insert(key, value);
            }
            let stats: BTreeMap<String, Json> = [
                ("p50".to_string(), self.p50),
                ("p99".to_string(), self.p99),
                ("p999".to_string(), self.p999),
                ("over250".to_string(), int(0)),
                ("delivery".to_string(), float(self.delivery)),
            ]
            .into_iter()
            .collect();
            let mut record = object(vec![
                ("id", text(&self.id)),
                ("mandate", text(mandate)),
                ("label", text(label)),
                ("dialect", text("kv")),
                ("sample_count", self.sample_count.clone()),
                ("stats", Json::Object(stats)),
                ("counters", Json::Object(counters)),
                (
                    "windows",
                    object(vec![
                        ("window_seconds", float(self.window)),
                        ("wall_seconds", float(self.wall)),
                    ]),
                ),
                (
                    "cells",
                    Json::Array(self.cells.iter().map(|cell| text(cell)).collect()),
                ),
                ("values", object(vec![])),
                ("raw_line", text(&format!("[mandate-smoke {label}] ..."))),
            ]);
            if let Some(producer) = self.producer {
                set(&mut record, "producer", text(&producer));
            }
            record
        }
    }

    /// One arm carrying exactly the counters a case is about.
    ///
    /// `sent`/`received`/`wire_bytes` are always present, so `counters` names the
    /// quantities under test and a key left out of it is the *absence* of that key
    /// rather than the absence of the whole record.
    fn arm_with(id: &str, cells: &[&str], counters: Vec<(&str, Json)>) -> Json {
        let mut spec = ArmSpec::new(id).cells(cells);
        spec.counters = counters
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        spec.build()
    }

    /// One `mandate-check.json` fixture.
    fn report_full(arms: Vec<Json>, quick: bool, schema: &str, mandates: &[&str]) -> Json {
        let mandate_map: BTreeMap<String, Json> = mandates
            .iter()
            .map(|mandate| {
                (
                    (*mandate).to_string(),
                    object(vec![
                        ("declared", Json::Bool(true)),
                        ("verdict", text("PASS")),
                        ("values", object(vec![])),
                    ]),
                )
            })
            .collect();
        let named: BTreeSet<String> = arms
            .iter()
            .filter_map(|arm| arm.get("producer").and_then(Json::as_str))
            .map(str::to_string)
            .collect();
        let mut payload = object(vec![
            ("schema", text(schema)),
            ("ok", Json::Bool(true)),
            ("exit_code", int(0)),
            ("verdict", text("PASS")),
            ("quick", Json::Bool(quick)),
            (
                "rtp_mux",
                object(vec![
                    ("revision", text(&"0".repeat(40))),
                    ("change_id", Json::Null),
                    ("revision_source", text("jj")),
                ]),
            ),
            ("mandates", Json::Object(mandate_map)),
            ("arms", Json::Array(arms)),
        ]);
        if !named.is_empty() {
            let producers: BTreeMap<String, Json> = named
                .iter()
                .map(|producer| {
                    (
                        producer.clone(),
                        object(vec![("id", text(producer)), ("selected", Json::Bool(true))]),
                    )
                })
                .collect();
            set(&mut payload, "producers", Json::Object(producers));
            set(
                &mut payload,
                "producers_declared",
                Json::Array(named.iter().map(|name| text(name)).collect()),
            );
            set(
                &mut payload,
                "producers_selected",
                Json::Array(named.iter().map(|name| text(name)).collect()),
            );
        }
        payload
    }

    fn report(arms: Vec<Json>) -> Json {
        report_full(arms, false, "mandate-check/3", &["M1", "M2", "M3", "M4"])
    }

    fn baseline_report() -> Json {
        report(vec![
            ArmSpec::new("M1/clean").build(),
            ArmSpec::new("M1/hostile")
                .sample(int(2500))
                .p99(float(245.1))
                .wire(43210)
                .build(),
            ArmSpec::new("M4/m4/clean")
                .sample(Json::Null)
                .p99(Json::Null)
                .cells(&[M4_CELL])
                .build(),
        ])
    }

    fn two_producer_report(probes: bool) -> Json {
        let mut arms = vec![
            ArmSpec::new("M1/clean").producer("rtp_mux").build(),
            ArmSpec::new("M1/hostile")
                .sample(int(2500))
                .p99(float(245.1))
                .wire(43210)
                .producer("rtp_mux")
                .build(),
            ArmSpec::new("M4/m4/clean")
                .sample(Json::Null)
                .p99(Json::Null)
                .cells(&[M4_CELL])
                .producer("rtp_mux")
                .build(),
        ];
        if probes {
            let mut probe = ArmSpec::new("probe/forwarding")
                .sample(int(200000))
                .wire(0)
                .cells(&[PROBE_CELL])
                .producer("netem_test")
                .build();
            // A probe measures no window, so its record carries none.
            set(&mut probe, "windows", object(vec![]));
            arms.push(probe);
        }
        report(arms)
    }

    struct Tool {
        root: PathBuf,
    }

    impl Tool {
        fn new() -> Tool {
            let counter = TOOL_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "mandate-compare-rs-{}-{counter}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("temp dir");
            Tool { root }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.root.join(name)
        }

        fn write(&self, name: &str, value: &Json) -> PathBuf {
            let path = self.path(name);
            fs::write(&path, json::to_string(value)).expect("write fixture");
            path
        }

        fn run_paths(
            &self,
            candidate: &Path,
            baseline: &Path,
            extra: &[&str],
        ) -> (i32, String, String) {
            let args = Args {
                report: Some(candidate.to_path_buf()),
                baseline: Some(baseline.to_path_buf()),
                ..options_from_extra(extra)
            };
            let mut out = Vec::new();
            let mut err = Vec::new();
            let code = run(args, &mut out, &mut err);
            (
                code,
                String::from_utf8_lossy(&out).into_owned(),
                String::from_utf8_lossy(&err).into_owned(),
            )
        }

        fn run(
            &self,
            candidate: &Json,
            extra: &[&str],
            baseline: Option<&Json>,
        ) -> (i32, String, String) {
            let candidate_path = self.write("candidate.json", candidate);
            let baseline_path = match baseline {
                Some(payload) => self.write("baseline.json", payload),
                None => self.write("baseline.json", &baseline_report()),
            };
            self.run_paths(&candidate_path, &baseline_path, extra)
        }

        fn run_against(
            &self,
            candidate: &Json,
            baseline_path: &Path,
            extra: &[&str],
        ) -> (i32, String, String) {
            let candidate_path = self.write("candidate.json", candidate);
            self.run_paths(&candidate_path, baseline_path, extra)
        }

        /// A baseline of one lone arm, written to disk, and its path.
        fn one_arm(
            &self,
            id: &str,
            cells: &[&str],
            counters: Vec<(&str, Json)>,
            name: &str,
        ) -> PathBuf {
            self.write(name, &report(vec![arm_with(id, cells, counters)]))
        }

        fn reject(
            &self,
            candidate: &Json,
            fragment: &str,
            extra: &[&str],
            baseline: Option<&Json>,
        ) {
            let (code, stdout, stderr) = self.run(candidate, extra, baseline);
            assert_eq!(code, EXIT_UNCOMPARABLE, "{stdout}{stderr}");
            assert!(
                format!("{stdout}{stderr}").contains(fragment),
                "expected {fragment:?} in {stdout}{stderr}"
            );
        }
    }

    /// The options a fixture run sets, from the argument tokens the tests name.
    ///
    /// This is fixture plumbing, not a command-line parser: the flag surface is
    /// the `netem-tools` binary's, pinned black-box by
    /// `tools/test_netem_tools.py` and by the bin's own clap tests. An unknown
    /// token panics rather than being silently dropped, so a fixture can never
    /// quietly stop exercising what it names.
    fn options_from_extra(extra: &[&str]) -> Args {
        let mut args = Args::default();
        let mut index = 0;
        while index < extra.len() {
            match extra[index] {
                "--fail-on-value-drift" => args.fail_on_value_drift = true,
                "--json-out" => {
                    index += 1;
                    args.json_out = Some(PathBuf::from(extra[index]));
                }
                "--count-tolerance" => {
                    index += 1;
                    args.count_tolerance = extra[index]
                        .parse()
                        .unwrap_or_else(|_| panic!("not a number: {}", extra[index]));
                }
                other => panic!("the fixture names no option {other:?}"),
            }
            index += 1;
        }
        args
    }

    fn jget<'a>(value: &'a Json, path: &[&str]) -> &'a Json {
        let mut current = value;
        for key in path {
            current = current
                .get(key)
                .unwrap_or_else(|| panic!("no key {key} in {current:?}"));
        }
        current
    }

    // -- two producers in one comparison ------------------------------------

    #[test]
    fn two_producers_are_compared_and_both_are_named() {
        let tool = Tool::new();
        let base = tool.write("two-producer.json", &two_producer_report(true));
        let (code, stdout, stderr) = tool.run_against(&two_producer_report(true), &base, &[]);
        assert_eq!(code, 0, "{stdout}{stderr}");
        assert!(stdout.contains("producers: netem_test, rtp_mux (2 covered)"));
        assert!(stdout.contains("ok   probe/forwarding"));
    }

    #[test]
    fn a_producer_the_candidate_dropped_is_a_coverage_regression() {
        let tool = Tool::new();
        let base = tool.write("two-producer.json", &two_producer_report(true));
        let (code, stdout, _) = tool.run_against(&two_producer_report(false), &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION);
        assert!(stdout.contains("probe/forwarding"));
        assert!(stdout.contains("absent: coverage regression"));
        assert!(stdout.contains("producers: rtp_mux (1 covered)"));
    }

    #[test]
    fn a_second_producers_halved_sample_count_is_a_coverage_regression() {
        let tool = Tool::new();
        let base = tool.write("two-producer.json", &two_producer_report(true));
        let mut candidate = two_producer_report(true);
        let probe = arm_with_id(&mut candidate, "probe/forwarding");
        set(probe, "sample_count", int(100000));
        set_in(probe, "counters", "received", int(100000));
        set_in(probe, "counters", "sent", int(100000));
        let (code, stdout, _) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION);
        assert!(stdout.contains("sample_count 200000 -> 100000"));
    }

    #[test]
    fn a_report_without_producer_records_names_the_arms_producers() {
        let tool = Tool::new();
        let base = tool.write("two-producer.json", &two_producer_report(true));
        let mut candidate = two_producer_report(true);
        for key in ["producers", "producers_declared", "producers_selected"] {
            delete(&mut candidate, key);
        }
        let (code, stdout, _) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, 0);
        assert!(stdout.contains("producers: netem_test, rtp_mux (2 covered)"));
    }

    #[test]
    fn a_report_with_no_producer_information_is_named_unnamed() {
        let tool = Tool::new();
        let (code, stdout, _) = tool.run(&baseline_report(), &[], None);
        assert_eq!(code, 0);
        assert!(stdout.contains("producers: unnamed (0 covered)"));
    }

    // -- the honest cases ---------------------------------------------------

    #[test]
    fn an_unchanged_run_is_green_and_says_nothing_moved() {
        let tool = Tool::new();
        let (code, stdout, stderr) = tool.run(&baseline_report(), &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("verdict: OK  exit=0"));
        assert!(stdout.contains("nothing moved"));
        assert!(stdout.contains("coverage regression(s)=0"));
        assert!(stdout.contains("cells: 2 declared cell(s) exercised, 0 no longer covered"));
    }

    #[test]
    fn a_schema_four_candidate_still_compares_against_a_three_baseline() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set(&mut candidate, "schema", text("mandate-check/4"));
        set(
            get_mut(&mut candidate, "rtp_mux"),
            "tree_id",
            text(&"a".repeat(40)),
        );
        set(
            get_mut(&mut candidate, "rtp_mux"),
            "tree_id_source",
            text("jj"),
        );
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("verdict: OK  exit=0"));
        assert!(stdout.contains("schema=mandate-check/3"));
        assert!(stdout.contains("schema=mandate-check/4"));
    }

    #[test]
    fn a_statistic_move_is_reported_and_bounded_not_failed() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(arm_at(&mut candidate, 0), "stats", "p99", float(150.0));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("p99 90.0 -> 150.0 (+66.7%) [past the 50% value tolerance]"));
        assert!(stdout.contains("value drift(s)=1"));
        assert!(stdout.contains("[value drift]"));
    }

    #[test]
    fn a_statistic_move_fails_only_when_strictness_is_asked_for() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(arm_at(&mut candidate, 0), "stats", "p99", float(150.0));
        let (code, _, stderr) = tool.run(&candidate, &["--fail-on-value-drift"], None);
        assert_eq!(code, EXIT_VALUE_DRIFT, "{stderr}");
        let (_, stdout, _) = tool.run(&candidate, &["--fail-on-value-drift"], None);
        assert!(stdout.contains("exit=5"));
    }

    #[test]
    fn a_small_statistic_move_is_reported_without_drifting() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(arm_at(&mut candidate, 0), "stats", "p99", float(100.0));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("p99 90.0 -> 100.0"));
        assert!(!stdout.contains("[value drift]"));
        assert!(stdout.contains("value drift(s)=0"));
    }

    #[test]
    fn a_sample_count_inside_the_tolerance_is_a_reported_move() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set(arm_at(&mut candidate, 0), "sample_count", int(2440));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("sample_count 2400 -> 2440 (+1.7%)"));
        assert!(stdout.contains("coverage regression(s)=0"));
    }

    #[test]
    fn a_new_arm_is_reported_and_is_not_a_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        array_mut(get_mut(&mut candidate, "arms")).push(ArmSpec::new("M2/clean").build());
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("new arms (reported, not a regression): M2/clean"));
    }

    #[test]
    fn json_out_writes_the_diff() {
        let tool = Tool::new();
        let out = tool.path("diff.json");
        let (code, _, stderr) = tool.run(
            &baseline_report(),
            &["--json-out", &out.display().to_string()],
            None,
        );
        assert_eq!(code, 0, "{stderr}");
        let diff = json::parse(&fs::read_to_string(&out).expect("diff written")).expect("parses");
        assert_eq!(jget(&diff, &["exit_code"]), &int(0));
        assert_eq!(jget(&diff, &["arms"]).as_array().expect("arms").len(), 3);
        assert_eq!(
            jget(&diff, &["tolerances", "count"]),
            &float(DEFAULT_COUNT_TOLERANCE)
        );
        assert_eq!(
            jget(&diff, &["tolerances", "window"]),
            &float(DEFAULT_WINDOW_TOLERANCE)
        );
    }

    // -- every coverage regression: non-zero, and naming the quantity -------

    #[test]
    fn a_dropped_arm_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        array_mut(get_mut(&mut candidate, "arms"))
            .retain(|arm| arm.get("id").and_then(Json::as_str) != Some("M1/hostile"));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("LOSS M1/hostile"));
        assert!(stdout.contains("the arm is in the baseline and absent from the candidate"));
        assert!(stdout.contains("verdict: COVERAGE-LOSS  exit=4"));
    }

    #[test]
    fn a_fallen_sample_count_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set(arm_at(&mut candidate, 0), "sample_count", int(1200));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        // Exactly half is a regression: it is the shortening the instrument
        // exists to catch, so the boundary belongs on the failing side.
        assert!(stdout.contains("sample_count 2400 -> 1200 (-50.0%), past the 50% tolerance"));
    }

    #[test]
    fn a_count_fall_inside_the_tolerance_is_a_reported_move() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        let arm = arm_at(&mut candidate, 0);
        set(arm, "sample_count", int(2000));
        set_in(arm, "counters", "wire_bytes", int(100000));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("sample_count 2400 -> 2000 (-16.7%)"));
        assert!(stdout.contains("wire_bytes 126000 -> 100000 (-20.6%)"));
        assert!(stdout.contains("coverage regression(s)=0"));
    }

    #[test]
    fn a_shrunk_window_is_a_coverage_regression_at_a_tight_tolerance() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(
            arm_at(&mut candidate, 0),
            "windows",
            "window_seconds",
            float(4.0),
        );
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("window_seconds 12.0 -> 4.0 (-66.7%)"));
        assert!(stdout.contains("past the 1% tolerance"));
    }

    #[test]
    fn a_window_move_inside_the_tight_tolerance_is_not_a_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(
            arm_at(&mut candidate, 0),
            "windows",
            "window_seconds",
            float(11.95),
        );
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("coverage regression(s)=0"));
    }

    #[test]
    fn a_fallen_counter_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(
            arm_at(&mut candidate, 0),
            "counters",
            "wire_bytes",
            int(50000),
        );
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("wire_bytes 126000 -> 50000 (-60.3%)"));
    }

    // -- the claim rule: a counter the cells claim is a tooth, with no floor -

    #[test]
    fn a_claiming_cell_below_the_floor_is_compared_and_fails() {
        // The future-arm case that motivated the rule: the cell claims the bulk
        // lane, so a 40 000-byte workload is a tooth exactly like 8 MiB — a
        // magnitude floor would have swallowed this fall.
        let tool = Tool::new();
        for (form, cell) in CLAIMING_CELLS {
            let base = tool.one_arm(
                "M1/clean",
                &[cell],
                vec![("bulk_wire_bytes", int(40000))],
                &format!("claim-{form}.json"),
            );
            let candidate = report(vec![arm_with(
                "M1/clean",
                &[cell],
                vec![("bulk_wire_bytes", int(20000))],
            )]);
            let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
            assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{form}: {stdout}{stderr}");
            assert!(
                stdout.contains("bulk_wire_bytes 40000 -> 20000 (-50.0%)"),
                "{form}"
            );
            assert!(stdout.contains("no floor applies"), "{form}");
            assert!(!stdout.contains("reported, not a regression"), "{form}");
        }
    }

    #[test]
    fn a_claiming_cell_whose_counter_vanishes_fails() {
        // The tooth the magnitude band removed, restored for a claiming arm.
        let tool = Tool::new();
        let cell = CLAIMING_CELLS[0].1;
        let base = tool.one_arm(
            "M1/clean",
            &[cell],
            vec![("bulk_sink_bytes", int(40000))],
            "claim-absent.json",
        );
        let candidate = report(vec![arm_with("M1/clean", &[cell], vec![])]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_sink_bytes 40000 -> not measured"));
    }

    #[test]
    fn the_vacuity_pair_a_non_claiming_cell_whose_counter_vanishes_is_ok() {
        // Stated as intended behaviour rather than left as an accident: the
        // cell declares the lane idle, so the counter is not the coverage the
        // arm measures and its disappearance is reported, never compared.
        let tool = Tool::new();
        for (form, cell) in IDLE_CELLS {
            let base = tool.one_arm(
                "M4/m4/clean",
                &[cell],
                vec![("bulk_sink_bytes", int(40000))],
                &format!("idle-absent-{form}.json"),
            );
            let candidate = report(vec![arm_with("M4/m4/clean", &[cell], vec![])]);
            let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
            assert_eq!(code, 0, "{form}: {stdout}{stderr}");
            assert!(
                stdout.contains("bulk_sink_bytes 40000 -> not measured"),
                "{form}"
            );
            assert!(stdout.contains("declare the lane idle"), "{form}");
            assert!(stdout.contains("reported, never compared"), "{form}");
            assert!(stdout.contains("coverage regression(s)=0"), "{form}");
        }
    }

    #[test]
    fn an_idle_cell_whose_counter_fell_is_reported_not_failed() {
        let tool = Tool::new();
        let base = tool.one_arm(
            "M4/m4/clean",
            &[IDLE_CELL],
            vec![("bulk_wire_bytes", int(40000))],
            "idle-fell.json",
        );
        let candidate = report(vec![arm_with(
            "M4/m4/clean",
            &[IDLE_CELL],
            vec![("bulk_wire_bytes", int(20000))],
        )]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, 0, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_wire_bytes 40000 -> 20000 (-50.0%)"));
        assert!(stdout.contains("reported, never compared"));
    }

    #[test]
    fn the_recorded_residue_pair_stays_reported_not_failed() {
        // The false positive this rule had to preserve the fix for, on the same
        // recorded pair: the request-response arm's cell names `lane=dual` and
        // no bulk load, so the declaration is silent, the pair is a gap, and the
        // 59 % wobble in the residue is visible and green.
        let tool = Tool::new();
        let base = tool.one_arm(
            "M1/lone_tail",
            &[LONE_TAIL_CELL],
            vec![("bulk_wire_bytes", int(1920))],
            "residue.json",
        );
        let candidate = report(vec![arm_with(
            "M1/lone_tail",
            &[LONE_TAIL_CELL],
            vec![("bulk_wire_bytes", int(785))],
        )]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, 0, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_wire_bytes 1920 -> 785 (-59.1%)"));
        assert!(stdout.contains("reported, not a regression"));
        assert!(stdout.contains("leave the lane unstated"));
        assert!(stdout.contains("coverage regression(s)=0"));
        assert!(stdout.contains("verdict: OK  exit=0"));
        assert!(stdout.contains("M1/lone_tail bulk_wire_bytes = 1920"));
    }

    #[test]
    fn the_same_recorded_pair_on_a_claiming_cell_would_fail() {
        // The pair *is* a tooth once a cell claims the lane, which is what makes
        // the residue's silence the thing that rescues it.
        let tool = Tool::new();
        let cell = CLAIMING_CELLS[1].1;
        let base = tool.one_arm(
            "M1/lone_tail",
            &[cell],
            vec![("bulk_wire_bytes", int(1920))],
            "residue-claimed.json",
        );
        let candidate = report(vec![arm_with(
            "M1/lone_tail",
            &[cell],
            vec![("bulk_wire_bytes", int(785))],
        )]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_wire_bytes 1920 -> 785 (-59.1%)"));
        assert!(!stdout.contains("reported, not a regression"));
    }

    #[test]
    fn a_silent_cell_above_its_floor_is_still_a_coverage_regression() {
        // The other arm the same token blocks: `M1/clean`'s cell is silent too,
        // and its 8 MiB counter has to stay a tooth.
        let tool = Tool::new();
        let base = tool.one_arm(
            "M1/clean",
            &[CLEAN_CELL],
            vec![("bulk_wire_bytes", int(8482399))],
            "silent-above.json",
        );
        let candidate = report(vec![arm_with(
            "M1/clean",
            &[CLEAN_CELL],
            vec![("bulk_wire_bytes", int(4241199))],
        )]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_wire_bytes 8482399 -> 4241199 (-50.0%)"));
        assert!(!stdout.contains("reported, not a regression"));
        // ... and its absence is still one, because the baseline is above the
        // floor the declaration left as the only thing that could decide.
        let candidate = report(vec![arm_with("M1/clean", &[CLEAN_CELL], vec![])]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_wire_bytes 8482399 -> not measured"));
    }

    #[test]
    fn a_claiming_cell_at_and_below_the_floor_is_compared() {
        // A floor is not consulted for a claimed counter: at exactly the floor
        // and one byte under it both keys are teeth, because the cell, not the
        // magnitude, decided.
        let tool = Tool::new();
        let cell = CLAIMING_CELLS[1].1;
        let base = tool.one_arm(
            "M1/clean",
            &[cell],
            vec![
                ("bulk_wire_bytes", int(65536)),
                ("bulk_sink_bytes", int(65535)),
            ],
            "floor-edge.json",
        );
        let candidate = report(vec![arm_with(
            "M1/clean",
            &[cell],
            vec![
                ("bulk_wire_bytes", int(32768)),
                ("bulk_sink_bytes", int(32767)),
            ],
        )]);
        let (code, stdout, stderr) = tool.run_against(&candidate, &base, &[]);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stdout}{stderr}");
        assert!(stdout.contains("bulk_wire_bytes 65536 -> 32768 (-50.0%)"));
        assert!(stdout.contains("bulk_sink_bytes 65535 -> 32767 (-50.0%)"));
    }

    #[test]
    fn the_claim_rule_is_derived_from_the_cells_and_ignores_shape() {
        // `shape` names the interactive lane's load shape and `rate`/`burst` name
        // a rate regime, so neither is read as a bulk claim: the same
        // `shape=cadence` is claimed-or-idle by the load dimension alone.
        for (form, cell) in CLAIMING_CELLS {
            assert_eq!(cell_claim(cell, "bulk_wire_bytes").0, CLAIMED, "{form}");
        }
        for (form, cell) in IDLE_CELLS {
            assert_eq!(cell_claim(cell, "bulk_sink_bytes").0, IDLE, "{form}");
        }
        assert_eq!(cell_claim(LONE_TAIL_CELL, "bulk_wire_bytes").0, UNSTATED);
        assert_eq!(cell_claim(CLEAN_CELL, "bulk_wire_bytes").0, UNSTATED);
        // A rate regime is not a lane: the conformance and reorder rows carry
        // `rate=` with no bulk lane, and reading it as one would claim the lane.
        for cell in [
            "conformance-reorder@impairment=reorder+rate=rate-limit",
            "reorder-rate@impairment=reorder+rate=curve+metric=p99",
        ] {
            assert_eq!(cell_claim(cell, "bulk_wire_bytes").0, UNSTATED, "{cell}");
        }
        // `load`/`bulk` are about the bulk lane, so they never disclaim the
        // arm's own lane — and a cell that names no lane at all leaves that lane
        // unstated rather than claimed (the `hol` cells carry `rate`/`loss`).
        for key in [
            "sent",
            "received",
            "wire_bytes",
            "offered_bytes",
            "delivered_bytes",
        ] {
            for (form, cell) in CLAIMING_CELLS {
                let expected = if cell
                    .split_once('@')
                    .is_some_and(|(_, dimensions)| dimensions.contains(LANE_DIMENSION))
                {
                    CLAIMED
                } else {
                    UNSTATED
                };
                assert_eq!(cell_claim(cell, key).0, expected, "{form}/{key}");
            }
            for (form, cell) in IDLE_CELLS {
                let expected = if cell
                    .split_once('@')
                    .is_some_and(|(_, dimensions)| dimensions.contains(LANE_DIMENSION))
                {
                    CLAIMED
                } else {
                    UNSTATED
                };
                assert_eq!(cell_claim(cell, key).0, expected, "{form}/{key}");
            }
            assert_eq!(counter_lane(key), None, "{key}");
        }
    }

    #[test]
    fn a_claiming_cell_outranks_a_silent_or_idle_one_on_the_same_arm() {
        // An arm's cells combine by the declared precedence, so a claim any
        // declared cell makes keeps the tooth.
        let arm_record = object(vec![(
            "cells",
            Json::Array(vec![
                text(IDLE_CELL),
                text(LONE_TAIL_CELL),
                text(CLAIMING_CELLS[1].1),
            ]),
        )]);
        let (claim, why, gap) = arm_claim(&arm_record, "bulk_wire_bytes");
        assert_eq!(claim, CLAIMED);
        assert!(why.contains("load=bulk"));
        assert!(!gap);
        let arm_record = object(vec![(
            "cells",
            Json::Array(vec![text(IDLE_CELL), text(LONE_TAIL_CELL)]),
        )]);
        let (claim, _why, gap) = arm_claim(&arm_record, "bulk_wire_bytes");
        assert_eq!(claim, UNSTATED, "silence outranks an idle cell");
        assert!(gap);
        let arm_record = object(vec![("cells", Json::Array(vec![text(IDLE_CELL)]))]);
        assert_eq!(arm_claim(&arm_record, "bulk_wire_bytes").0, IDLE);
    }

    #[test]
    fn a_cell_stating_one_dimension_twice_is_unstated() {
        assert_eq!(
            cell_claim("M1@lane=dual+load=bulk+load=none", "bulk_wire_bytes").0,
            UNSTATED
        );
    }

    #[test]
    fn a_claim_on_one_load_dimension_wins_over_an_idle_on_the_other() {
        // Two dimensions can speak, and the claim wins: a tooth is not dropped
        // by a second dimension saying the idle thing.
        assert_eq!(
            cell_claim("hol@lane=dual+load=none+bulk=shared", "bulk_wire_bytes").0,
            CLAIMED
        );
        assert_eq!(
            cell_claim("hol@lane=dual+load=bulk+bulk=none", "bulk_wire_bytes").0,
            CLAIMED
        );
        assert_eq!(
            cell_claim("hol@lane=dual+load=none+bulk=none", "bulk_wire_bytes").0,
            IDLE
        );
    }

    #[test]
    fn the_claim_gaps_are_printed_and_written_to_the_diff() {
        let tool = Tool::new();
        let base = tool.one_arm(
            "M1/lone_tail",
            &[LONE_TAIL_CELL],
            vec![("bulk_wire_bytes", int(1920))],
            "gap-write.json",
        );
        let candidate = report(vec![arm_with(
            "M1/lone_tail",
            &[LONE_TAIL_CELL],
            vec![("bulk_wire_bytes", int(785))],
        )]);
        let out = tool.path("gaps.json");
        let (code, stdout, stderr) = tool.run_against(
            &candidate,
            &base,
            &["--json-out", &out.display().to_string()],
        );
        assert_eq!(code, 0, "{stdout}{stderr}");
        assert!(stdout.contains("gaps: 1 unstated arm x counter pair(s)"));
        let diff = json::parse(&fs::read_to_string(&out).expect("diff written")).expect("parses");
        let gaps = jget(&diff, &["claim_gaps"]).as_array().expect("gaps");
        assert_eq!(gaps.len(), 1);
        assert_eq!(jget(&gaps[0], &["key"]), &text("bulk_wire_bytes"));
        assert_eq!(jget(&gaps[0], &["arm"]), &text("M1/lone_tail"));
        assert_eq!(jget(&gaps[0], &["baseline"]), &int(1920));
        assert_eq!(jget(&gaps[0], &["decided_by"]), &text("floor"));
        assert_eq!(
            jget(&diff, &["claim_rule", "counter_lanes", "bulk_wire_bytes"]),
            &text("bulk")
        );
    }

    #[test]
    fn a_gap_above_its_floor_is_recorded_as_magnitude_decided() {
        // The gap is recorded whatever the magnitude: the *declaration* decided
        // nothing here either, it is the count that stands above the floor.
        let tool = Tool::new();
        let base = tool.one_arm(
            "M1/clean",
            &[CLEAN_CELL],
            vec![("bulk_wire_bytes", int(8482399))],
            "gap-above.json",
        );
        let candidate = report(vec![arm_with(
            "M1/clean",
            &[CLEAN_CELL],
            vec![("bulk_wire_bytes", int(8482399))],
        )]);
        let out = tool.path("gap-above-diff.json");
        let (code, stdout, stderr) = tool.run_against(
            &candidate,
            &base,
            &["--json-out", &out.display().to_string()],
        );
        assert_eq!(code, 0, "{stdout}{stderr}");
        let diff = json::parse(&fs::read_to_string(&out).expect("diff written")).expect("parses");
        assert_eq!(
            jget(
                &jget(&diff, &["claim_gaps"]).as_array().expect("gaps")[0],
                &["decided_by"]
            ),
            &text("tolerance")
        );
        assert!(stdout.contains("above its floor: compared by magnitude, not by the declaration"));
    }

    #[test]
    fn a_gap_on_a_key_with_no_floor_is_recorded_as_compared_anyway() {
        // A cell that names no lane at all leaves its own-lane counters
        // unstated too. Nothing hangs on it — the key has no floor, so the pair
        // is compared exactly as a claimed one — and the gap says so instead of
        // implying a weakness that is not there.
        let tool = Tool::new();
        let base = tool.one_arm(
            "probe/forwarding",
            &[PROBE_CELL],
            vec![],
            "gap-no-floor.json",
        );
        let candidate = report(vec![arm_with("probe/forwarding", &[PROBE_CELL], vec![])]);
        let out = tool.path("gap-no-floor-diff.json");
        let (code, stdout, stderr) = tool.run_against(
            &candidate,
            &base,
            &["--json-out", &out.display().to_string()],
        );
        assert_eq!(code, 0, "{stdout}{stderr}");
        let diff = json::parse(&fs::read_to_string(&out).expect("diff written")).expect("parses");
        let gaps = jget(&diff, &["claim_gaps"]).as_array().expect("gaps");
        let keys: Vec<String> = gaps
            .iter()
            .map(|gap| jget(gap, &["key"]).as_str().expect("key").to_string())
            .collect();
        assert_eq!(keys, vec!["sent", "received", "wire_bytes"]);
        let decided: BTreeSet<String> = gaps
            .iter()
            .map(|gap| {
                jget(gap, &["decided_by"])
                    .as_str()
                    .expect("decided")
                    .to_string()
            })
            .collect();
        assert_eq!(decided, BTreeSet::from(["no-floor".to_string()]));
        assert!(stdout.contains("the cell names no lane dimension"));
        assert!(stdout.contains("compared exactly as a claimed counter"));
    }

    #[test]
    fn the_floors_are_printed_and_written_to_the_diff() {
        let tool = Tool::new();
        let out = tool.path("floors.json");
        let (code, stdout, stderr) = tool.run(
            &baseline_report(),
            &["--json-out", &out.display().to_string()],
            None,
        );
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("floors: bulk_sink_bytes 65536, bulk_wire_bytes 65536"));
        assert!(stdout.contains("applied only where the arm's cells leave the lane unstated"));
        let diff = json::parse(&fs::read_to_string(&out).expect("diff written")).expect("parses");
        let floors = jget(&diff, &["count_floors"]);
        assert_eq!(jget(floors, &["bulk_wire_bytes"]), &int(65536));
        assert_eq!(jget(floors, &["bulk_sink_bytes"]), &int(65536));
    }

    #[test]
    fn a_counter_without_a_floor_is_compared_at_the_count_tolerance() {
        // Only the two bulk-lane byte counters carry a floor; every other
        // counter keeps the plain 50 % criterion however small it is.
        assert_eq!(floor_bytes("received"), 0);
        assert_eq!(floor_bytes("wire_bytes"), 0);
        assert!(floor_bytes("bulk_wire_bytes") > 0);
    }

    #[test]
    fn every_floored_key_is_a_bulk_lane_byte_counter() {
        // A floor is a byte quantity, and it is only ever consulted for a lane
        // the cells can claim or disclaim: the bulk lane.
        for (key, _) in COUNT_FLOORS_BYTES {
            assert!(key.ends_with("_bytes"), "{key}");
            assert!(COVERAGE_COUNTER_KEYS.contains(key), "{key}");
            assert_eq!(counter_lane(key), Some("bulk"), "{key}");
        }
        for (key, lane) in COUNTER_LANES {
            assert!(COVERAGE_COUNTER_KEYS.contains(key), "{key}");
            assert!(!lane.is_empty(), "{key}");
        }
    }

    #[test]
    fn a_counter_that_stopped_being_measured_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        delete_in(arm_at(&mut candidate, 0), "counters", "wire_bytes");
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("wire_bytes 126000 -> not measured"));
    }

    #[test]
    fn a_statistic_that_stopped_being_measured_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        delete_in(arm_at(&mut candidate, 0), "stats", "p999");
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("p999 98.0 -> not measured"));
    }

    #[test]
    fn a_fallen_delivery_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set_in(arm_at(&mut candidate, 0), "stats", "delivery", float(0.98));
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("delivery 1.0 -> 0.98 (-0.020)"));
        assert!(stdout.contains("past the 0.005 delivery tolerance"));
    }

    #[test]
    fn a_cell_no_arm_covers_any_more_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        for arm in array_mut(get_mut(&mut candidate, "arms")) {
            set(
                arm,
                "cells",
                Json::Array(vec![text("M2@impairment=clean+metric=offer-and-latency")]),
            );
        }
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("no longer covered"));
        assert!(stdout.contains(M1_CELL));
        assert!(stdout.contains("is exercised by no arm now"));
    }

    #[test]
    fn a_missing_mandate_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        delete(get_mut(&mut candidate, "mandates"), "M3");
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("the mandate M3 is in the baseline and not in the candidate"));
    }

    #[test]
    fn a_missing_sample_count_is_a_coverage_regression() {
        let tool = Tool::new();
        let mut candidate = baseline_report();
        set(arm_at(&mut candidate, 0), "sample_count", Json::Null);
        let (code, stdout, stderr) = tool.run(&candidate, &[], None);
        assert_eq!(code, EXIT_COVERAGE_REGRESSION, "{stderr}");
        assert!(stdout.contains("sample_count 2400 -> not measured"));
    }

    // -- every refusal: exit 2, and naming the problem -----------------------

    #[test]
    fn a_quick_mismatch_is_refused_rather_than_compared() {
        let tool = Tool::new();
        tool.reject(
            &report_full(
                baseline_report()
                    .get("arms")
                    .and_then(Json::as_array)
                    .expect("arms")
                    .to_vec(),
                true,
                "mandate-check/3",
                &["M1", "M2", "M3", "M4"],
            ),
            "record the candidate the same way as the baseline",
            &[],
            None,
        );
    }

    #[test]
    fn a_baseline_that_predates_the_per_arm_record_is_refused() {
        let tool = Tool::new();
        let mut old = report_full(
            baseline_report()
                .get("arms")
                .and_then(Json::as_array)
                .expect("arms")
                .to_vec(),
            false,
            "mandate-check/2",
            &["M1", "M2", "M3", "M4"],
        );
        set(&mut old, "arms", Json::Array(vec![]));
        tool.reject(
            &baseline_report(),
            "predates the per-arm record",
            &[],
            Some(&old),
        );
    }

    #[test]
    fn a_report_with_no_arms_is_refused() {
        let tool = Tool::new();
        tool.reject(
            &baseline_report(),
            "carries no arm records",
            &[],
            Some(&report_full(vec![], false, "mandate-check/3", &["M1"])),
        );
    }

    #[test]
    fn an_unknown_schema_is_refused() {
        let tool = Tool::new();
        tool.reject(
            &baseline_report(),
            "not a 'mandate-check/<version>'",
            &[],
            Some(&report_full(
                baseline_report()
                    .get("arms")
                    .and_then(Json::as_array)
                    .expect("arms")
                    .to_vec(),
                false,
                "something/1",
                &["M1", "M2", "M3", "M4"],
            )),
        );
    }

    #[test]
    fn a_repeated_arm_id_is_refused() {
        let tool = Tool::new();
        let mut duplicated = baseline_report();
        array_mut(get_mut(&mut duplicated, "arms")).push(ArmSpec::new("M1/clean").build());
        tool.reject(&duplicated, "recorded twice", &[], None);
    }

    #[test]
    fn a_malformed_declared_cell_is_refused() {
        let tool = Tool::new();
        let mut broken = baseline_report();
        set(
            arm_at(&mut broken, 0),
            "cells",
            Json::Array(vec![text("M1")]),
        );
        tool.reject(&broken, "not <property>@<dimension>=<value>", &[], None);
    }

    #[test]
    fn a_missing_report_is_refused() {
        let tool = Tool::new();
        let (code, _, stderr) = tool.run_paths(
            &tool.path("absent.json"),
            &tool.path("also-absent.json"),
            &[],
        );
        assert_eq!(code, 2);
        assert!(stderr.contains("does not exist"));
    }

    #[test]
    fn a_negative_tolerance_is_refused() {
        let tool = Tool::new();
        tool.reject(
            &baseline_report(),
            "must not be negative",
            &["--count-tolerance", "-1"],
            None,
        );
    }

    /// The baseline belongs to the crate that owns the mandate, not to this
    /// tool. The old assertion — that it sat under this crate's `tools/` — is
    /// the one this replaces: if the resolution reverted to the tool's own
    /// directory the `!ends_with(tools/...)` clause fails.
    #[test]
    fn the_default_baseline_belongs_to_the_mandate_owner() {
        let baseline = default_baseline();
        assert!(
            !baseline.ends_with(Path::new("tools").join(DEFAULT_BASELINE_NAME)),
            "the baseline is still resolved from this tool's own directory: {}",
            baseline.display()
        );
        assert!(
            baseline
                .file_name()
                .is_some_and(|name| name == DEFAULT_BASELINE_NAME),
            "{}",
            baseline.display()
        );
        assert!(
            baseline.is_file(),
            "the committed baseline is missing at {}",
            baseline.display()
        );
    }

    #[test]
    fn the_committed_baseline_is_comparable_with_itself() {
        // The real committed baseline must satisfy the comparison's own
        // preconditions; a baseline this command refuses would make the
        // instrument unusable the day it lands.
        let committed = default_baseline();
        if !committed.is_file() {
            return;
        }
        let tool = Tool::new();
        let (code, stdout, stderr) = tool.run_paths(&committed, &committed, &[]);
        assert_eq!(code, 0, "{stdout}{stderr}");
        assert!(stdout.contains("verdict: OK  exit=0"));
    }
}
