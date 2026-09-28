//! `perf-history` — archive every perf run forever, and compare each run to the
//! previous one, reporting the degradation figure per arm and metric.
//!
//! Why this exists
//! ---------------
//!
//! A battery run is evidence only until the next run overwrites it, and a run
//! directory under a temp root is swept. That has cost this workspace the
//! ability to answer "is this worse than last time?" more than once — once
//! because the numbers lived only in a log, once because the previous run's
//! directory was deleted with a workspace. So:
//!
//! * **Every run is archived**, artifacts and all, under a durable root outside
//!   every repo and every temp tree. The archive is the record.
//! * **Every run is compared to the previous archived run**, and the comparison
//!   is written as `vs-prev.md` beside the run *and* into the archive.
//! * **An M1 degradation fails the run** (exit 6). M1 is the interactive tail
//!   and the standing priority: a run whose interactive tail is worse than the
//!   previous run's is a rejection, not a number to weigh. A *guard* bounds what
//!   a window can observe; it does not license a rise.
//!
//! How the M1 band is derived
//! --------------------------
//!
//! A **fixed percentage** band cannot bound a quantity whose own run-to-run
//! spread exceeds it, and this archive is the proof. Over the eleven runs on
//! record `lone_tail p90` spans **37.1-57.8 ms**, so the pair `37.1 -> 55.0`
//! (+48 %) is *inside* the arm's own spread while a settled 40 % band rejects
//! it: a good build rejected, and the quick-revert runbook this mechanism arms
//! firing on noise. `clean max` (30.5-94.7) and `hostile p50` (0.6-16.9 against
//! a p90 of 81-122) have the same shape. Seven of the archived runs fired the
//! fixed-band rule, and **every one of those firings was false** (the table is
//! in `PERF_INFRA.md`, "The perf-history M1 band").
//!
//! The band is therefore drawn from the archive, the way
//! `mandate_smoke::m1_interactive_tail_latency` draws its deployed baseline. For
//! an `(arm, metric)`:
//!
//! ```text
//! measured = mean + 4 sample standard deviations   over the archived runs
//! settled  = previous x (1 + settled_band(arm, metric))
//! bound    = max(settled, measured)                the wider of the two binds
//! ```
//!
//! and a rise is a regression only when it clears **all** of the bound, the
//! previous run's value, and the absolute floor [`MINIMUM_DELTA_MS`] -- a
//! relative test near zero is meaningless in the units the product promises
//! (`hostile p50` moves `0.6 -> 16.9` ms, a 28x ratio on a median that is not the
//! promise). The measured half is trusted only from [`MIN_SPREAD_SAMPLES`]
//! archived runs; below that the bound is the settled band alone **and the
//! report says so** (`spread insufficient: n/8`), because a rule that silently
//! falls back to a fixed 10 % is how this defect happened.
//!
//! `max` is the deliberate direction: a bound that is too narrow is the defect
//! being fixed, and the measured half exists to *absorb* a quantity whose spread
//! exceeds the settled band. The residual is stated rather than hidden -- where
//! the archive's spread is wide (the impaired arms) the bound is correspondingly
//! wide, so only a rise past the healthy population's own spread rejects, and
//! the rate-shaped `over250` guard in `mandate_smoke` is what bounds the regime
//! in between. Three false rejections (`lone_tail max`, `lone_tail p999`, and a
//! `hostile p50` printed as `+54.2%`) already retired those order statistics
//! from enforcement; they are reported with the same comparison and a
//! `REPORTED` verdict.
//!
//! The comparison is two-layered on purpose. The per-metric table below carries
//! the **degradation figure** — absolute and percent, per arm, per metric —
//! because that is the number a reader needs and a boolean cannot hold it. The
//! multi-axis coverage/claim verdict is the crate's own `mandate_compare` module
//! (the `mandate-compare` subcommand's implementation), called directly rather
//! than reimplemented or shelled out to, so its semantics cannot drift from
//! these two callers disagreeing.
//!
//! Usage
//! -----
//!
//! ```text
//! cargo run -p rtp_mux --features perf --bin perf-history -- <run-dir> [--label L]
//!     [--baseline <run-dir>] [--no-archive] [--only-compare] [--no-ab]
//! ```
//!
//! `$PERF_ARCHIVE_DIR` relocates the archive (default `.net-perf-history` in the
//! working directory, self-ignoring); `$PERF_BASELINE_DIR` compares against a
//! chosen run instead of the previous one (that is how a run is compared against
//! what is deployed, when the previous run is itself suspect).

use clap::Parser;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rtp_mux::tools::json::{self, Json};
use rtp_mux::tools::mandate_compare;

/// The report a completed run writes, and the log its raw lines land in.
const REPORT_NAME: &str = "mandate-check.json";
const LOG_NAME: &str = "mandate-smoke.log";
/// The run-level summary this bin writes beside `vs-prev.md`.
const SUMMARY_NAME: &str = "summary.md";
/// The rendered panels directory the summary names by absolute path.
const PLOTS_DIRNAME: &str = "plots";

/// Where runs are archived: `.net-perf-history` in the working directory, so
/// each repo carries its own history beside the work it describes. The
/// directory writes a `*` `.gitignore` for itself (the idiom the workspace's
/// `local/` uses), because an archive that lands inside a repo must not become a
/// working-copy change.
const DEFAULT_ARCHIVE_DIR: &str = ".net-perf-history";

/// Exit status for a run whose interactive tail regressed. Distinct from 1
/// (usage) and 2 (a run that could not be archived or compared) so a caller can
/// tell "worse than last time" from "could not tell".
const EXIT_M1_DEGRADATION: i32 = 6;

/// Exit status for a run that owes an artifact it did not produce: no panels, a
/// panel path that does not resolve, or a text artifact that was not written.
/// An evidence failure is never a pass, so it has its own status.
const EXIT_EVIDENCE_FAILURE: i32 = 7;

/// Metrics compared per arm, in the order a reader wants them. Latency
/// percentiles are the product's promise and lead; delivery follows because a
/// latency win can be bought by starving the lane that would have delivered.
const LATENCY_METRICS: &[&str] = &["p50", "p90", "p99", "p999", "max"];
const OTHER_METRICS: &[&str] = &["over250", "delivery"];

/// The arms whose tail *is* M1: the interactive lane, clean and impaired. Matched
/// exactly, on the *arm* part of `<producer>/<arm>` — a substring test admits
/// `mandate-smoke/m4/hostile flow A`, which is an M4 four-flow arm whose tail is
/// not the interactive lane, and would reject a run for the wrong metric.
const M1_ARM_KEYS: &[&str] = &["clean", "hostile", "lone_tail", "lone tail"];

/// How much movement is noise rather than degradation, per arm and metric, as a
/// fraction. Not invented: the shipped lone-tail measurement reports a p99
/// coefficient of variation of 0.067 across ten runs while its *maximum* varies
/// by 7.3x, so a percentile and a single order statistic cannot share a band. A
/// 10 % band absorbs ordinary run-to-run movement on a percentile; a maximum
/// gets a much wider one because it is one sample's tail.
///
/// The **impaired** arms need a wider percentile band than the clean one, and
/// that is measured, not asserted: across the archived runs the hostile p99 has
/// read 122.2, 127.2, 187.2 and 211.7 ms -- a 1.7x spread at the same settings.
/// A 10 % band there rejects a good run roughly half the time, which is a
/// rejection nobody can act on. A clean-arm percentile is tight (cv 0.067), so it
/// keeps the narrow band; a per-arm band is the price of a rejection that means
/// something.
///
/// This is now the **settled** band: the *floor* of the rule and its fallback,
/// never the whole rule. [`measured_spread`] may only widen it (the effective
/// bound is the wider of the two), so a settled band that is too narrow cannot
/// by itself reject a run -- which is the defect this file was fixed for.
fn settled_band(metric: &str, arm: &str) -> f64 {
    let impaired = arm.contains("hostile") || arm.contains("lone");
    match metric {
        // Every percentile on an impaired arm shares the measured spread; the
        // first fix widened only p99/p999 and left p90 at 10 %, which rejected a
        // run for `lone_tail p90 44.8 -> 52.3 (+16.7%)` -- the same variance.
        "p50" | "p90" | "p99" => {
            if impaired {
                0.40
            } else {
                0.10
            }
        }
        "p999" => {
            if impaired {
                0.50
            } else {
                0.20
            }
        }
        "max" => 0.50,
        _ => 0.10,
    }
}

/// The smallest latency change that counts as a regression, in milliseconds. A
/// percentage alone is meaningless near zero: the hostile arm's median is ~2 ms
/// (most of its samples are fast, its tail is not), so a 1.3 ms wobble printed as
/// "+54.2%" and rejected a run that was, in the units the product promises, fine.
/// A change must clear **both** the band and this floor, which is 2 % of the
/// 250 ms ceiling the product promises.
const MINIMUM_DELTA_MS: f64 = 5.0;

/// The sample margin a measured band is drawn at: `mean + 4 sample standard
/// deviations`. Not invented in this file: it is the margin
/// `mandate_smoke::m1_interactive_tail_latency` settled on for the deployed
/// baseline's limit (after `mean + 3sd` over six reps flaked twice in twenty
/// healthy runs), and 4 is RFC 6298's `K`, the variance margin this transport's
/// own RTO uses.
const SPREAD_SIGMA: f64 = 4.0;

/// The fewest archived runs a measured band may be drawn from, **excluding the
/// candidate**. Below it the bound is the settled band alone and the report
/// prints `spread insufficient: n/8` beside it, so the fallback is visible
/// rather than silent.
///
/// 8 is above the six reps `mandate_smoke` recorded as having flaked (the
/// measured half is drawn from a population whose composition changes as the
/// archive grows, so it needs strictly more samples than the count already
/// shown too few), and it is the smallest count at which the sample standard
/// deviation's own relative standard error `1/sqrt(2(n-1))` falls below 27 %
/// (26.7 % at n = 8) -- i.e. the 4-sigma margin is known to better than a
/// quarter of its own width. It is also the smallest count this archive can
/// supply: the eleven runs on record give a candidate n = 10.
const MIN_SPREAD_SAMPLES: usize = 8;

/// A metric where a *fall* is the degradation (delivery is the only one).
fn worse_when_lower(metric: &str) -> bool {
    metric == "delivery"
}

#[derive(Debug)]
struct Failure(String);

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

type Result<T> = std::result::Result<T, Failure>;

fn fail<T>(message: impl Into<String>) -> Result<T> {
    Err(Failure(message.into()))
}

// ─────────────────────────────── archiving ───────────────────────────────────

fn archive_dir() -> PathBuf {
    match env::var("PERF_ARCHIVE_DIR") {
        Ok(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => PathBuf::from(DEFAULT_ARCHIVE_DIR),
    }
}

fn now_stamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // UTC, formatted without a date library: days since the epoch, then the
    // civil date from the standard algorithm.
    let days = (seconds / 86_400) as i64;
    let secs_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// The revision the report names, short, if it names one.
fn revision_of(report: &Path) -> String {
    let Ok(text) = fs::read_to_string(report) else {
        return String::new();
    };
    for key in ["\"revision\"", "\"rev\"", "\"commit\"", "\"head\""] {
        if let Some(index) = text.find(key) {
            let rest = &text[index + key.len()..];
            if let Some(colon) = rest.find(':') {
                let after = &rest[colon + 1..];
                let span = after
                    .find('"')
                    .and_then(|start| after[start + 1..].find('"').map(|end| (start, end)));
                if let Some((start, end)) = span {
                    let value: String = after[start + 1..start + 1 + end]
                        .chars()
                        .filter(|c| c.is_ascii_alphanumeric())
                        .take(12)
                        .collect();
                    if !value.is_empty() {
                        return value;
                    }
                }
            }
        }
    }
    String::new()
}

/// What an archive keeps: everything a reader needs to re-derive a verdict
/// without re-running. The rendered panels are included deliberately — a run
/// whose panels were only summarised was not read, so the panels are the
/// artifact, not a by-product.
fn archive_entry(run_dir: &Path, label: &str, archive_root: &Path) -> Result<PathBuf> {
    let base = archive_root.join(label);
    fs::create_dir_all(&base)
        .map_err(|e| Failure(format!("cannot create {}: {e}", base.display())))?;
    // Self-ignoring at the archive *root*, so one `*` covers every label and an
    // archive inside a repo stays out of the working copy.
    let ignore = archive_root.join(".gitignore");
    if !ignore.exists() {
        let _ = fs::write(&ignore, "*\n");
    }

    let report = run_dir.join(REPORT_NAME);
    if !report.is_file() {
        return fail(format!(
            "{} has no {REPORT_NAME}, so it is not a completed run and there is nothing to archive",
            run_dir.display()
        ));
    }
    let revision = revision_of(&report);
    let stamp = now_stamp();
    let mut name = if revision.is_empty() {
        stamp.clone()
    } else {
        format!("{stamp}-{revision}")
    };
    let mut entry = base.join(&name);
    let mut suffix = 1;
    while entry.exists() {
        suffix += 1;
        name = format!("{stamp}-{suffix}");
        entry = base.join(&name);
    }
    fs::create_dir_all(&entry)
        .map_err(|e| Failure(format!("cannot create {}: {e}", entry.display())))?;

    let mut copied = 0usize;
    for item in fs::read_dir(run_dir).map_err(|e| Failure(format!("cannot read run dir: {e}")))? {
        let item = item.map_err(|e| Failure(format!("cannot read run entry: {e}")))?;
        let path = item.path();
        if !path.is_file() {
            continue;
        }
        let keep = matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("json") | Some("log") | Some("csv")
        );
        if keep {
            copy(&path, &entry.join(item.file_name()))?;
            copied += 1;
        }
    }
    let plots = run_dir.join("plots");
    if plots.is_dir() {
        let out = entry.join("plots");
        fs::create_dir_all(&out)
            .map_err(|e| Failure(format!("cannot create {}: {e}", out.display())))?;
        for plot in fs::read_dir(&plots).map_err(|e| Failure(format!("cannot read plots: {e}")))? {
            let plot = plot.map_err(|e| Failure(format!("cannot read plot entry: {e}")))?;
            if plot.path().is_file() {
                copy(&plot.path(), &out.join(plot.file_name()))?;
                copied += 1;
            }
        }
    }
    if copied == 0 {
        return fail(format!(
            "{} held no artifacts to archive, which means the run wrote none",
            run_dir.display()
        ));
    }

    fs::write(base.join("latest"), format!("{name}\n"))
        .map_err(|e| Failure(format!("cannot write latest pointer: {e}")))?;
    Ok(entry)
}

fn copy(from: &Path, to: &Path) -> Result<()> {
    fs::copy(from, to).map_err(|e| Failure(format!("cannot copy {}: {e}", from.display())))?;
    Ok(())
}

/// Every archived entry for `label`, oldest first.
fn archived_runs(archive_root: &Path, label: &str) -> Vec<PathBuf> {
    let base = archive_root.join(label);
    let Ok(items) = fs::read_dir(&base) else {
        return Vec::new();
    };
    let mut runs: Vec<PathBuf> = items
        .filter_map(|item| item.ok())
        .map(|item| item.path())
        .filter(|path| path.is_dir() && path.join(REPORT_NAME).is_file())
        .collect();
    runs.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    runs
}

/// The newest archived run that is not the candidate.
fn previous_run(archive_root: &Path, label: &str, candidate: &Path) -> Option<PathBuf> {
    let candidate = candidate
        .canonicalize()
        .unwrap_or_else(|_| candidate.to_path_buf());
    // `retain` + `pop`, not `filter(..).last()`/`next_back()`: clippy flags both
    // spellings of "the final element" on a Vec, and the newest run is simply the
    // last element of a list already sorted by name.
    let mut runs = archived_runs(archive_root, label);
    runs.retain(|path| path.canonicalize().map(|p| p != candidate).unwrap_or(true));
    runs.pop()
}

fn baseline_from_env() -> Option<PathBuf> {
    match env::var("PERF_BASELINE_DIR") {
        Ok(value) if !value.trim().is_empty() => Some(PathBuf::from(value)),
        _ => None,
    }
}

// ────────────── the population a measured band is drawn from ────────────────

/// One `(arm, metric)` reading the archive holds, with the code revision of the
/// run that produced it. Kept flat so the band derivation is a pure function
/// over it and can be exercised without a filesystem.
#[derive(Debug, Clone)]
struct Observation {
    revision: String,
    arm: String,
    metric: String,
    value: f64,
}

/// The measured spread of one `(arm, metric)` over the archived runs, and the
/// bound it licenses.
#[derive(Debug, Clone)]
struct Spread {
    samples: usize,
    mean: f64,
    sd: f64,
    range_min: f64,
    range_max: f64,
    /// The number of distinct code revisions the series pooled. Printed because
    /// the pool is the whole archive series, not one revision (see
    /// [`observations`]), so contamination by a code change must be visible to
    /// the reader rather than inferred.
    revisions: usize,
}

impl Spread {
    /// `mean + 4 sd`: the upper prediction bound a rise must clear.
    fn upper(&self) -> f64 {
        self.mean + SPREAD_SIGMA * self.sd
    }

    /// `mean - 4 sd`: the lower prediction bound a fall must clear.
    fn lower(&self) -> f64 {
        self.mean - SPREAD_SIGMA * self.sd
    }

    /// One `(samples, mean, sd, range, revisions)` line, for the report.
    fn describe(&self) -> String {
        format!(
            "n={} mean={:.2} sd={:.2} range={:.1}..{:.1} over {} revision(s)",
            self.samples, self.mean, self.sd, self.range_min, self.range_max, self.revisions
        )
    }
}

/// The band for one `(arm, metric)`: the wider of the archived runs' measured
/// spread and the settled relative band, in the metric's own units.
///
/// The two halves are kept separately because the report owes the derivation,
/// not just the verdict: [`Band::basis`] names which half bound, and
/// [`Band::measured`] is the population behind it (or the count that was too
/// small to be used).
#[derive(Debug, Clone)]
struct Band {
    /// True when a *fall* is the degradation (delivery is the only one).
    lower_is_worse: bool,
    /// The settled band in metric units, against the previous run's value.
    settled: f64,
    /// The measured prediction bound, when the archive supplied enough runs.
    measured: Option<f64>,
    /// The population behind `measured`, and the count that failed the
    /// requirement when it did not.
    spread: Option<Spread>,
    /// The count of archived runs seen for this `(arm, metric)`, whether or not
    /// it met [`MIN_SPREAD_SAMPLES`]. Zero means the archive held none.
    available: usize,
}

impl Band {
    /// The bound a worse value must cross.
    fn bound(&self) -> f64 {
        match self.measured {
            None => self.settled,
            Some(measured) if self.lower_is_worse => self.settled.min(measured),
            Some(measured) => self.settled.max(measured),
        }
    }

    /// Which half set the bound: `measured`, `settled`, or -- with too few
    /// archived runs -- `settled` with the shortfall named.
    fn basis(&self) -> &'static str {
        match self.measured {
            None if self.available == 0 => "settled (no archived spread)",
            None => "settled (insufficient spread)",
            Some(measured) if self.lower_is_worse && measured <= self.settled => "measured",
            Some(measured) if !self.lower_is_worse && measured >= self.settled => "measured",
            Some(_) => "settled",
        }
    }
}

/// Read every archived run's per-arm measurements into the observation list a
/// measured band is drawn from, excluding the candidate's own entry.
///
/// The population is **the whole archive series**, not the candidate's revision.
/// That is a measured decision, not a preference: across the eleven runs on
/// record the transport revision changes every one or two runs (six distinct
/// revisions), so a revision-scoped series holds at most three runs -- below
/// [`MIN_SPREAD_SAMPLES`] -- and would fall back to the settled band on every
/// run of this archive, *reintroducing* the false-positive defect the measured
/// band exists to remove. What is shared across the series is the arm's own
/// declaration (impairment, latency, jitter, seed, window, cadence, lane shape,
/// scale), which is frozen by the test source; that is the "same code" the
/// measured quantity belongs to. The residual is named and printed: the spread
/// carries the number of revisions it pooled, so a transport change that moves
/// an arm shows up as a wider population rather than as an invisible one.
///
/// A run that cannot be read is **named and skipped** rather than fatal, and the
/// caller prints the skips: a corrupt entry must not take the tool down, and a
/// skip nobody can see is a silent hole in the population.
fn observations(
    archive_root: &Path,
    label: &str,
    candidate: &Path,
) -> (Vec<Observation>, Vec<String>) {
    let candidate = candidate
        .canonicalize()
        .unwrap_or_else(|_| candidate.to_path_buf());
    let mut observations = Vec::new();
    let mut skipped = Vec::new();
    for run in archived_runs(archive_root, label) {
        if run
            .canonicalize()
            .map(|path| path == candidate)
            .unwrap_or(false)
        {
            continue;
        }
        let revision = run_revision(&run.join(REPORT_NAME));
        match read_arms(&run) {
            Ok(arms) => {
                for (arm, metrics) in arms {
                    for (metric, value) in metrics {
                        observations.push(Observation {
                            revision: revision.clone(),
                            arm: arm.clone(),
                            metric,
                            value,
                        });
                    }
                }
            }
            Err(Failure(message)) => skipped.push(format!("{}: {message}", run.display())),
        }
    }
    (observations, skipped)
}

/// The code revision whose behaviour the arms measure: the report's
/// `rtp_mux.revision` when it records one, else the run's own recorded revision.
fn run_revision(report: &Path) -> String {
    if let Some(revision) = transport_revision(report) {
        return revision;
    }
    revision_of(report)
}

fn transport_revision(report: &Path) -> Option<String> {
    let text = fs::read_to_string(report).ok()?;
    let payload = json::parse(&text).ok()?;
    payload
        .get("rtp_mux")?
        .get("revision")?
        .as_str()
        .map(str::to_string)
}

/// The measured spread of `(arm, metric)` over the archive, or `None` when the
/// archive holds fewer than [`MIN_SPREAD_SAMPLES`] readings of it.
fn measured_spread(
    observations: &[Observation],
    arm: &str,
    metric: &str,
) -> (Option<Spread>, usize) {
    let series: Vec<&Observation> = observations
        .iter()
        .filter(|observation| observation.arm == arm && observation.metric == metric)
        .collect();
    let available = series.len();
    if available < MIN_SPREAD_SAMPLES {
        return (None, available);
    }
    let mut values: Vec<f64> = series.iter().map(|observation| observation.value).collect();
    let revisions = series
        .iter()
        .map(|observation| observation.revision.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let samples = values.len();
    let mean = values.iter().sum::<f64>() / samples as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean) * (value - mean))
        .sum::<f64>()
        / (samples as f64 - 1.0);
    let spread = Spread {
        samples,
        mean,
        sd: variance.sqrt(),
        range_min: values[0],
        range_max: values[samples - 1],
        revisions,
    };
    (Some(spread), available)
}

/// The band for one `(arm, metric)`: the wider of the archived spread and the
/// settled relative band, in the metric's own units.
fn band_for(previous: f64, metric: &str, arm: &str, observations: &[Observation]) -> Band {
    let lower_is_worse = worse_when_lower(metric);
    let settled_band = settled_band(metric, arm);
    let settled = if lower_is_worse {
        previous * (1.0 - settled_band)
    } else {
        previous * (1.0 + settled_band)
    };
    let (spread, available) = measured_spread(observations, arm, metric);
    let measured = spread.as_ref().map(|spread| {
        if lower_is_worse {
            spread.lower()
        } else {
            spread.upper()
        }
    });
    Band {
        lower_is_worse,
        settled,
        measured,
        spread,
        available,
    }
}

// ────────────── reading the per-arm metrics out of a run ─────────────────────

/// A line that names an arm and carries its numbers: `[producer arm] k=v …`.
/// Parsed from text rather than from the report's JSON so the comparison keeps
/// working across schema bumps — which is the property a permanent record needs.
fn parse_arm_lines(text: &str, into: &mut BTreeMap<String, BTreeMap<String, f64>>) {
    for line in text.lines() {
        let line = line.trim();
        let Some(open) = line.strip_prefix('[') else {
            continue;
        };
        let Some(close) = open.find(']') else {
            continue;
        };
        let header = &open[..close];
        let mut parts = header.split_whitespace();
        let Some(producer) = parts.next() else {
            continue;
        };
        let arm = parts.collect::<Vec<_>>().join(" ");
        if arm.is_empty() {
            continue;
        }
        let body = &open[close + 1..];
        let mut values: BTreeMap<String, f64> = BTreeMap::new();
        // The raw producer line writes `p99= 26.5` and the report quotes it
        // verbatim, while the report's own typed fields write `"p99": 26.5,`.
        // Both put whitespace after the separator, so a token splitter sees
        // `p99=` with no value and silently measures nothing. Scan instead:
        // a key, optional space, a separator, optional space, a number.
        let bytes = body.as_bytes();
        let mut i = 0usize;
        while i < bytes.len() {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if i == start {
                i += 1;
                continue;
            }
            let key = &body[start..i];
            while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }
            if i >= bytes.len() || (bytes[i] != b'=' && bytes[i] != b':') {
                continue;
            }
            i += 1;
            while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }
            let value_start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_digit() || bytes[i] == b'.' || bytes[i] == b'-')
            {
                i += 1;
            }
            let parsed = if i > value_start {
                body[value_start..i].parse::<f64>().ok()
            } else {
                None
            };
            if let Some(value) = parsed {
                values.entry(key.to_string()).or_insert(value);
            }
        }
        // `delivery=1.000` arrives as `delivery` because the scan stops at the
        // space the producer writes after `=`; nothing to repair, the key is
        // already right. A lane that delivered nothing would write `0`.
        if values.is_empty() {
            continue;
        }
        into.entry(format!("{producer}/{arm}"))
            .or_default()
            .extend(values);
    }
}

/// The per-arm metrics of a run directory or a report file.
fn read_arms(path: &Path) -> Result<BTreeMap<String, BTreeMap<String, f64>>> {
    let report = if path.is_dir() {
        path.join(REPORT_NAME)
    } else {
        path.to_path_buf()
    };
    let text = fs::read_to_string(&report)
        .map_err(|e| Failure(format!("cannot read {}: {e}", report.display())))?;
    let mut arms = BTreeMap::new();
    parse_arm_lines(&text, &mut arms);
    let log = report.with_file_name(LOG_NAME);
    if let Ok(log_text) = fs::read_to_string(&log) {
        parse_arm_lines(&log_text, &mut arms);
    }
    if arms.is_empty() {
        return fail(format!(
            "{} records no per-arm measurement lines, so there is nothing to compare — a run that produced no arms is not a baseline",
            report.display()
        ));
    }
    Ok(arms)
}

// ─────────────────── the table: the degradation figure ───────────────────────

#[derive(Debug, Clone)]
struct Row {
    arm: String,
    metric: String,
    baseline: Option<f64>,
    candidate: Option<f64>,
    percent: f64,
    /// The bound a worse value must cross, in the metric's own units: the wider
    /// of the archived spread and the settled band ([`band_for`]).
    bound: f64,
    /// The settled band in the same units, so the two halves of the rule are
    /// both on the record rather than only the one that bound.
    settled: f64,
    /// Which half set the bound, or the shortfall when the archive was too thin.
    basis: &'static str,
    /// The archived population behind the measured half, when there is one.
    spread: Option<Spread>,
    /// How many archived readings of this `(arm, metric)` the archive held,
    /// whether or not they met [`MIN_SPREAD_SAMPLES`].
    available: usize,
    worse: bool,
    missing: bool,
}

fn metric_rows(
    baseline: &BTreeMap<String, BTreeMap<String, f64>>,
    candidate: &BTreeMap<String, BTreeMap<String, f64>>,
    observations: &[Observation],
) -> Vec<Row> {
    let mut rows = Vec::new();
    for (arm, base) in baseline {
        let Some(cand) = candidate.get(arm) else {
            rows.push(Row {
                arm: arm.clone(),
                metric: "*".to_string(),
                baseline: None,
                candidate: None,
                percent: 0.0,
                bound: 0.0,
                settled: 0.0,
                basis: "none",
                spread: None,
                available: 0,
                worse: true,
                missing: true,
            });
            continue;
        };
        for metric in LATENCY_METRICS.iter().chain(OTHER_METRICS) {
            let (old, new) = (base.get(*metric), cand.get(*metric));
            if old.is_none() && new.is_none() {
                continue;
            }
            let Some((old, new)) = old.zip(new) else {
                rows.push(Row {
                    arm: arm.clone(),
                    metric: metric.to_string(),
                    baseline: old.copied(),
                    candidate: new.copied(),
                    percent: 0.0,
                    bound: 0.0,
                    settled: 0.0,
                    basis: "none",
                    spread: None,
                    available: 0,
                    worse: true,
                    missing: true,
                });
                continue;
            };
            let (old, new) = (*old, *new);
            let band = band_for(old, metric, arm, observations);
            let bound = band.bound();
            let percent = if old != 0.0 {
                (new - old) / old.abs() * 100.0
            } else {
                0.0
            };
            // A latency metric must clear the floor *and* the bound: the bound
            // catches a real move on a large value, the floor stops a meaningless
            // ratio on a tiny one (`hostile p50` moves 0.6 -> 16.9 ms, a 28x
            // ratio on a median that is not the product's promise).
            let floor_ms = if LATENCY_METRICS.contains(metric) {
                MINIMUM_DELTA_MS
            } else {
                0.0
            };
            let worse = if worse_when_lower(metric) {
                new < old && new < bound && (old - new) > floor_ms
            } else {
                new > old && new > bound && (new - old) > floor_ms
            };
            rows.push(Row {
                arm: arm.clone(),
                metric: metric.to_string(),
                baseline: Some(old),
                candidate: Some(new),
                percent,
                bound,
                settled: band.settled,
                basis: band.basis(),
                spread: band.spread,
                available: band.available,
                worse,
                missing: false,
            });
        }
    }
    rows
}

/// The metrics whose rise rejects a run. **Only the percentiles that are stable
/// enough to mean something**: `p50`, `p90`, `p99`. `p999` and `max` are
/// **reported, not enforced** -- both are order statistics whose value is set by
/// one sample's tail, and the lone arm measures them accordingly (ten runs of its
/// maximum spanned 256.0-1863.7 ms, a 7.28x spread at a coefficient of variation
/// of 0.63). A `p999` over ~2400 samples is one sample in a thousand, which is
/// not a rate; a `p99` over the same samples is two dozen, which is. Enforcing an
/// order statistic rejects good runs at random -- it has done so twice here, once
/// on `lone_tail max` and once on `lone_tail p999 240.4 -> 390.3 (+62.4%)`.
const M1_REJECTION_METRICS: &[&str] = &["p50", "p90", "p99"];

/// The rows that are an M1 regression: an interactive arm's latency rose.
fn m1_degradations(rows: &[Row]) -> Vec<&Row> {
    rows.iter()
        .filter(|row| row.worse)
        .filter(|row| M1_REJECTION_METRICS.contains(&row.metric.as_str()))
        .filter(|row| {
            // The arm part of `<producer>/<arm>`, which must itself carry no
            // further `/`: that is what separates the interactive arms from the
            // per-flow four-flow arms.
            let arm = row
                .arm
                .split_once('/')
                .map(|(_, arm)| arm)
                .unwrap_or(&row.arm);
            !arm.contains('/') && M1_ARM_KEYS.contains(&arm)
        })
        .collect()
}

fn figure(row: &Row) -> String {
    if row.missing {
        return "the measurement stopped (arm or metric absent from one side)".to_string();
    }
    match (row.baseline, row.candidate) {
        (Some(old), Some(new)) => format!("{old:.4} -> {new:.4}  ({:+.1}%)", row.percent),
        _ => "(unavailable)".to_string(),
    }
}

/// The bound a row's worse direction had to clear, and which half set it.
fn band_cell(row: &Row) -> String {
    if row.missing {
        return "-".to_string();
    }
    format!("{:.2} ({})", row.bound, row.basis)
}

/// The archived population behind a row's measured half, or the shortfall that
/// kept it on the settled band. This is the derivation, not the verdict: it is
/// what lets the next reader check the bound instead of taking it on trust.
fn spread_cell(row: &Row) -> String {
    match &row.spread {
        Some(spread) => spread.describe(),
        None => format!(
            "insufficient: {} of {MIN_SPREAD_SAMPLES} archived runs",
            row.available
        ),
    }
}

/// The **bands** section: one line per arm and metric with the settled band, the
/// archived spread behind the measured half, and the bound they produced. Every
/// number the rule used is on this table, so the rule can be audited from the
/// run rather than re-derived from the source.
fn bands_section(rows: &[Row], observations: usize, skipped: &[String]) -> String {
    let mut out = String::new();
    out.push_str("## bands (the derivation)\n\n");
    out.push_str(&format!(
        "M1 band = `max(settled, measured)` where `measured` is `mean + 4 sample standard \
         deviations` over the archived runs of that arm and metric ([`SPREAD_SIGMA`], \
         [`MIN_SPREAD_SAMPLES`]); the wider half binds. Population: {observations} archived \
         reading(s).\n\n"
    ));
    out.push_str("| arm | metric | settled bound | archived spread | bound | basis |\n|---|---|---|---|---|---|\n");
    for row in rows {
        if row.missing {
            continue;
        }
        out.push_str(&format!(
            "| `{}` | {} | {:.2} | {} | {:.2} | {} |\n",
            row.arm,
            row.metric,
            row.settled,
            spread_cell(row),
            row.bound,
            row.basis
        ));
    }
    out.push('\n');
    if !skipped.is_empty() {
        out.push_str(&format!(
            "**{} archived run(s) could not be read and were skipped**:\n\n",
            skipped.len()
        ));
        for skip in skipped {
            out.push_str(&format!("- {skip}\n"));
        }
        out.push('\n');
    }
    out
}

fn vs_prev_markdown(
    rows: &[Row],
    baseline_name: &str,
    candidate_name: &str,
    ab_lines: &[String],
    observations: usize,
    skipped: &[String],
) -> String {
    let degradations = m1_degradations(rows);
    let mut out = String::new();
    out.push_str("# vs-prev\n\n");
    out.push_str(&format!("baseline:  `{baseline_name}`\n"));
    out.push_str(&format!("candidate: `{candidate_name}`\n\n"));
    if degradations.is_empty() {
        out.push_str(
            "## M1: no degradation\n\nNo interactive arm's latency rose past the wider of its\narchived spread and its settled band.\n\n",
        );
    } else {
        out.push_str("## M1 DEGRADATION - this run is rejected\n\n");
        out.push_str("M1 is the interactive tail and the standing priority. Any one of these is a\nrejection, not a trade to weigh:\n\n");
        out.push_str("| arm | metric | figure | bound | basis |\n|---|---|---|---|---|\n");
        for row in &degradations {
            out.push_str(&format!(
                "| `{}` | {} | {} | {:.2} | {} |\n",
                row.arm,
                row.metric,
                figure(row),
                row.bound,
                row.basis
            ));
        }
        out.push('\n');
    }
    let worse: Vec<&Row> = rows.iter().filter(|row| row.worse).collect();
    out.push_str("## metrics that moved worse\n\n");
    if worse.is_empty() {
        out.push_str("No arm or metric moved past its bound in the worse direction.\n\n");
    } else {
        out.push_str("| arm | metric | figure | bound | basis |\n|---|---|---|---|---|\n");
        for row in worse {
            out.push_str(&format!(
                "| `{}` | {} | {} | {:.2} | {} |\n",
                row.arm,
                row.metric,
                figure(row),
                row.bound,
                row.basis
            ));
        }
        out.push('\n');
    }
    out.push_str(
        "## every metric\n\n| arm | metric | figure | bound | basis |\n|---|---|---|---|---|\n",
    );
    for row in rows {
        if row.metric == "*" {
            continue;
        }
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            row.arm,
            row.metric,
            figure(row),
            band_cell(row),
            row.basis
        ));
    }
    out.push('\n');
    out.push_str(&bands_section(rows, observations, skipped));
    if !ab_lines.is_empty() {
        out.push_str("\n## coverage and claim verdict (netem-tools mandate-compare)\n\n```\n");
        for line in ab_lines {
            out.push_str(line);
            out.push('\n');
        }
        out.push_str("```\n");
    }
    out
}

// ────────────── the coverage/claim verdict, one implementation ───────────────

/// The full coverage/claim verdict for the two reports, from the crate's own
/// `mandate_compare` module. The per-metric table above answers "by how much";
/// this answers the multi-axis coverage and claim question, and it is the same
/// comparison the `netem-tools mandate-compare` subcommand runs — one
/// implementation, so the two callers cannot disagree about the semantics.
fn ab_verdict(baseline: &Path, candidate: &Path) -> Vec<String> {
    mandate_compare::coverage_verdict(&candidate.join(REPORT_NAME), &baseline.join(REPORT_NAME))
}

// ─────────────────── the three owed artifacts ────────────────────────────────
//
// Every perf run owes **panels**, a run-level **summary** and a **vs-prev**
// verdict, and the runner prints the two text artifacts to stdout rather than
// leaving them to be opened. Same reasoning as the forced per-panel summary: an
// artifact a reader must go and open is one a reader will skip, and "the numbers
// were in a file" is how a regression gets read as a pass. The paths a summary
// names are **absolute and canonicalized as of the run**, because every
// path mis-resolution in this workspace has been a reader computing a location
// instead of being told one.

/// One rendered panel: its SVG, and the PNG and forced summary beside it when
/// the run produced them. Every path is absolute and canonicalized, checked to
/// resolve before it is written into a summary.
#[derive(Default)]
struct Panel {
    stem: String,
    svg: Option<PathBuf>,
    png: Option<PathBuf>,
    summary: Option<PathBuf>,
}

fn canonical(path: &Path) -> Result<PathBuf> {
    path.canonicalize()
        .map_err(|e| Failure(format!("{} does not resolve: {e}", path.display())))
}

/// Every panel a run rendered, with its absolute paths, or an evidence failure.
///
/// A panel is its SVG; a PNG or a `plots/<panel>.summary.txt` attaches to it.
/// A stem with no SVG is refused — the SVG is the panel, and a summary pointing
/// a reader at a PNG with no panel beside it is pointing at a raster of nothing.
fn plot_files(run_dir: &Path) -> Result<Vec<Panel>> {
    let plots = run_dir.join(PLOTS_DIRNAME);
    if !plots.is_dir() {
        return fail(format!(
            "{} has no {PLOTS_DIRNAME}/ directory, so the run rendered no panels",
            run_dir.display()
        ));
    }
    let mut panels: BTreeMap<String, Panel> = BTreeMap::new();
    for entry in fs::read_dir(&plots)
        .map_err(|e| Failure(format!("cannot read {}: {e}", plots.display())))?
    {
        let entry = entry.map_err(|e| Failure(format!("cannot read plot entry: {e}")))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(stem) = name.strip_suffix(".svg") {
            panels.entry(stem.to_string()).or_default().svg = Some(canonical(&path)?);
        } else if let Some(stem) = name.strip_suffix(".png") {
            panels.entry(stem.to_string()).or_default().png = Some(canonical(&path)?);
        } else if let Some(stem) = name.strip_suffix(".summary.txt") {
            panels.entry(stem.to_string()).or_default().summary = Some(canonical(&path)?);
        }
    }
    for (stem, panel) in panels.iter_mut() {
        panel.stem = stem.clone();
    }
    let panels: Vec<Panel> = panels.into_values().collect();
    if panels.is_empty() {
        return fail(format!(
            "{} holds no rendered panel, so the run owes evidence it did not produce",
            plots.display()
        ));
    }
    for panel in &panels {
        if panel.svg.is_none() {
            return fail(format!(
                "the panel {} has no SVG (only a raster or a summary), so its \
                 absolute path would name no panel",
                panel.stem
            ));
        }
    }
    Ok(panels)
}

/// `(verdict, exit, revision)` from a run's report, "unreadable" when it cannot
/// be read. Reported rather than refused: a summary still has the arms and the
/// panels, and hiding them behind a parse error helps nobody.
fn report_verdict(report: &Path) -> (String, String, String) {
    let unreadable = (
        "unreadable".to_string(),
        "?".to_string(),
        "unresolved".to_string(),
    );
    let Ok(text) = fs::read_to_string(report) else {
        return unreadable;
    };
    let Ok(payload) = json::parse(&text) else {
        return unreadable;
    };
    let verdict = payload
        .get("verdict")
        .and_then(Json::as_str)
        .unwrap_or("unknown")
        .to_string();
    let exit = payload
        .get("exit_code")
        .map(|value| match value {
            Json::Int(number) => number.to_string(),
            other => json::to_string(other),
        })
        .unwrap_or_else(|| "?".to_string());
    let revision = payload
        .get("rtp_mux")
        .and_then(|rtp_mux| rtp_mux.get("revision"))
        .and_then(Json::as_str)
        .unwrap_or("unresolved")
        .to_string();
    (verdict, exit, revision)
}

/// The mandate verdicts a report recorded, in the report's own order.
fn mandate_verdicts(report: &Path) -> Vec<(String, String)> {
    let Ok(text) = fs::read_to_string(report) else {
        return Vec::new();
    };
    let Ok(payload) = json::parse(&text) else {
        return Vec::new();
    };
    let Some(mandates) = payload.get("mandates").and_then(Json::as_object) else {
        return Vec::new();
    };
    mandates
        .iter()
        .map(|(name, entry)| {
            (
                name.clone(),
                entry
                    .get("verdict")
                    .and_then(Json::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
            )
        })
        .collect()
}

/// One row per arm with its headline figures from the baseline comparison.
fn arm_figure_table(rows: &[Row]) -> String {
    let mut order: Vec<String> = Vec::new();
    let mut by_arm: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut worse: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        if !by_arm.contains_key(&row.arm) {
            order.push(row.arm.clone());
        }
        if row.metric != "*" {
            by_arm
                .entry(row.arm.clone())
                .or_default()
                .insert(row.metric.clone(), figure(row));
        }
        if row.worse {
            worse
                .entry(row.arm.clone())
                .or_default()
                .push(row.metric.clone());
        }
    }
    let mut out =
        String::from("| arm | p50 | p99 | delivery | moved worse |\n|---|---|---|---|---|\n");
    for arm in &order {
        let cell = |key: &str| {
            by_arm
                .get(arm)
                .and_then(|metrics| metrics.get(key))
                .cloned()
                .unwrap_or_else(|| "n/a".to_string())
        };
        let moved = worse
            .get(arm)
            .map(|metrics| metrics.join(", "))
            .filter(|text| !text.is_empty())
            .unwrap_or_else(|| "-".to_string());
        out.push_str(&format!(
            "| `{arm}` | {} | {} | {} | {moved} |\n",
            cell("p50"),
            cell("p99"),
            cell("delivery")
        ));
    }
    out
}

/// The run's own summary: its verdicts, its per-arm figures against the
/// baseline, its M1 outcome, and every panel by absolute path.
fn summary_markdown(
    run_dir: &Path,
    report: &Path,
    baseline_label: &str,
    rows: &[Row],
    m1: &[&Row],
    panels: &[Panel],
) -> String {
    let (verdict, exit, revision) = report_verdict(report);
    let mut out = format!(
        "# run summary\n\nrun: `{}`\nbaseline: `{baseline_label}`\n",
        run_dir.display()
    );
    out.push_str(&format!(
        "verdict: {verdict}  exit: {exit}  revision: {revision}\n\n"
    ));
    out.push_str("## mandates\n\n");
    let mandates = mandate_verdicts(report);
    if mandates.is_empty() {
        out.push_str("the report records no mandate verdicts.\n\n");
    } else {
        out.push_str("| mandate | verdict |\n|---|---|\n");
        for (mandate, verdict) in &mandates {
            out.push_str(&format!("| {mandate} | {verdict} |\n"));
        }
        out.push('\n');
    }
    out.push_str("## M1\n\n");
    if m1.is_empty() {
        out.push_str(
            "no degradation: no interactive arm's latency rose past the wider of its\narchived spread and its settled band.\n\n",
        );
    } else {
        out.push_str("**DEGRADATION** - this run is rejected.\n\n| arm | metric | figure | bound | basis |\n|---|---|---|---|---|\n");
        for row in m1 {
            out.push_str(&format!(
                "| `{}` | {} | {} | {:.2} | {} |\n",
                row.arm,
                row.metric,
                figure(row),
                row.bound,
                row.basis
            ));
        }
        out.push('\n');
    }
    out.push_str("## per-arm figures vs the baseline\n\n");
    out.push_str(&arm_figure_table(rows));
    out.push_str("\n## bands (the derivation)\n\n");
    out.push_str(&format!(
        "The M1 bound is `max(settled, mean + {SPREAD_SIGMA:.0} sample standard deviations over \
the archived runs)`, trusted from {MIN_SPREAD_SAMPLES} archived runs and falling back to the \
settled band below that; the full table is in `vs-prev.md`.\n\n"
    ));
    out.push_str("\n## panels (absolute paths as of this run)\n\n");
    out.push_str("| panel | svg | png | summary |\n|---|---|---|---|\n");
    for panel in panels {
        let display = |path: &Option<PathBuf>| match path {
            Some(path) => format!("`{}`", path.display()),
            None => "-".to_string(),
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            panel.stem,
            display(&panel.svg),
            display(&panel.png),
            display(&panel.summary)
        ));
    }
    out
}

// ─────────────────────────────── the runner ──────────────────────────────────

/// The runner's resolved options. The command line is parsed by [`Cli`] and
/// converted into this, so the runner itself never sees a flag string.
struct Args {
    run: PathBuf,
    baseline: Option<PathBuf>,
    label: String,
    archive: bool,
    only_compare: bool,
    no_ab: bool,
    /// The archive root. Carried in the resolved options rather than read from
    /// the environment at every use, so a caller (a test, or a future
    /// `--archive-dir` flag) can point the tool at a chosen archive without
    /// mutating process state.
    archive_root: PathBuf,
}

// The command line, parsed with clap's `derive` API. The rationale for the
// feature gate is stated in the module doc; only the flags and their help
// belong in the user-facing help, so no doc comment sits on this struct.
#[derive(Debug, clap::Parser)]
#[command(
    name = "perf-history",
    about = "archive every perf run forever, and compare each run to the previous one"
)]
struct Cli {
    /// the run directory (or the `mandate-check.json` inside it) to archive and compare
    #[arg(value_name = "run-dir", default_value = ".")]
    run: PathBuf,
    /// compare against this run instead of the previous archived one
    #[arg(long, value_name = "dir")]
    baseline: Option<PathBuf>,
    /// the archive series (default: mandate-check)
    #[arg(long, value_name = "name", default_value = "mandate-check")]
    label: String,
    /// compare without archiving this run
    #[arg(long)]
    no_archive: bool,
    /// exit 0 even on an M1 degradation (report-only)
    #[arg(long)]
    only_compare: bool,
    /// skip the coverage/claim verdict for this run
    #[arg(long)]
    no_ab: bool,
}

impl From<Cli> for Args {
    fn from(cli: Cli) -> Args {
        Args {
            run: cli.run,
            baseline: cli.baseline,
            label: cli.label,
            archive: !cli.no_archive,
            only_compare: cli.only_compare,
            no_ab: cli.no_ab,
            archive_root: archive_dir(),
        }
    }
}

fn run(args: Args, out: &mut impl Write) -> Result<i32> {
    let mut run_dir = args.run.clone();
    if run_dir.is_file() {
        run_dir = run_dir
            .parent()
            .map(|parent| parent.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
    }
    let run_dir = run_dir
        .canonicalize()
        .map_err(|e| Failure(format!("cannot resolve {}: {e}", run_dir.display())))?;

    let candidate = if args.archive {
        archive_entry(&run_dir, &args.label, &args.archive_root)?
    } else {
        run_dir.clone()
    };

    let baseline = match args.baseline.clone().or_else(baseline_from_env) {
        Some(path) => {
            if path.is_file() {
                path.parent().map(|p| p.to_path_buf()).unwrap_or(path)
            } else {
                path
            }
        }
        None => previous_run(&args.archive_root, &args.label, &candidate).unwrap_or_default(),
    };
    let first_run = baseline.as_os_str().is_empty();
    let baseline_label = if first_run {
        "(none - this run is the first archived one)".to_string()
    } else {
        baseline.display().to_string()
    };

    // The panels are the artifact the summary points at. A run that produced
    // none, or whose summary would name a path that does not resolve, is an
    // evidence failure and never a pass.
    let panels = match plot_files(&run_dir) {
        Ok(panels) => panels,
        Err(Failure(message)) => {
            eprintln!("perf-history: EVIDENCE FAILURE - {message}");
            return Ok(EXIT_EVIDENCE_FAILURE);
        }
    };

    // The population the measured half of every band is drawn from: the archived
    // runs of this series, the candidate excluded. Read once, so every row's
    // band is derived from the same set (and so the archive is walked once
    // rather than once per arm and metric).
    let (observations, skipped) = if first_run {
        (Vec::new(), Vec::new())
    } else {
        observations(&args.archive_root, &args.label, &candidate)
    };

    let (rows, ab_lines) = if first_run {
        (Vec::new(), Vec::new())
    } else {
        let rows = metric_rows(
            &read_arms(&baseline)?,
            &read_arms(&candidate)?,
            &observations,
        );
        let ab = if args.no_ab {
            Vec::new()
        } else {
            ab_verdict(&baseline, &candidate)
        };
        (rows, ab)
    };
    let m1 = m1_degradations(&rows);

    let vs_prev = vs_prev_markdown(
        &rows,
        &baseline_label,
        &candidate.display().to_string(),
        &ab_lines,
        observations.len(),
        &skipped,
    );
    let vs_prev_path = run_dir.join("vs-prev.md");
    fs::write(&vs_prev_path, &vs_prev)
        .map_err(|e| Failure(format!("cannot write vs-prev.md: {e}")))?;

    let summary = summary_markdown(
        &run_dir,
        &run_dir.join(REPORT_NAME),
        &baseline_label,
        &rows,
        &m1,
        &panels,
    );
    let summary_path = run_dir.join(SUMMARY_NAME);
    fs::write(&summary_path, &summary)
        .map_err(|e| Failure(format!("cannot write {SUMMARY_NAME}: {e}")))?;

    // Keep the archived copy's layout beside the panels it names, so the summary
    // and its panels stay together wherever the run is read from: the archived
    // summary carries the *archive's* absolute panel paths, not the run
    // directory's (which a temp tree sweeps).
    if candidate != run_dir {
        copy(&vs_prev_path, &candidate.join("vs-prev.md"))?;
        let archive_panels = plot_files(&candidate)?;
        let archive_summary = summary_markdown(
            &candidate,
            &candidate.join(REPORT_NAME),
            &baseline_label,
            &rows,
            &m1,
            &archive_panels,
        );
        fs::write(candidate.join(SUMMARY_NAME), &archive_summary)
            .map_err(|e| Failure(format!("cannot write the archived {SUMMARY_NAME}: {e}")))?;
    }

    // The run's own output carries both text artifacts verbatim, so nobody has
    // to open a file to learn what the run says.
    write!(out, "{summary}").map_err(|e| Failure(format!("cannot write stdout: {e}")))?;
    write!(out, "\n{vs_prev}").map_err(|e| Failure(format!("cannot write stdout: {e}")))?;
    writeln!(out, "summary:  {}", summary_path.display())
        .map_err(|e| Failure(format!("cannot write stdout: {e}")))?;
    writeln!(out, "vs-prev:  {}", vs_prev_path.display())
        .map_err(|e| Failure(format!("cannot write stdout: {e}")))?;
    if first_run {
        writeln!(
            out,
            "perf-history: archived {} as the first run; no previous run to compare",
            run_dir.display()
        )
        .map_err(|e| Failure(format!("cannot write stdout: {e}")))?;
    }

    if !m1.is_empty() {
        eprintln!(
            "perf-history: M1 DEGRADATION - {} interactive metric(s) worse than the baseline, so this run is rejected",
            m1.len()
        );
        for row in &m1 {
            eprintln!(
                "  {} {}: {} against the {} bound {:.2}",
                row.arm,
                row.metric,
                figure(row),
                row.basis,
                row.bound
            );
        }
        if !args.only_compare {
            return Ok(EXIT_M1_DEGRADATION);
        }
    }
    Ok(0)
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            // The hand-rolled exit codes are preserved rather than clap's: a
            // usage error exits 1 (clap would use 2), and help exits 0 on
            // stdout (clap's `print` sends help and version to stdout, every
            // other kind to stderr).
            let _ = error.print();
            std::process::exit(if error.use_stderr() { 1 } else { 0 });
        }
    };
    let args: Args = cli.into();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    match run(args, &mut out) {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("perf-history: error: {error}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_run_dir() -> PathBuf {
        let counter = RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("perf-history-rs-{}-{counter}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join(PLOTS_DIRNAME)).expect("create run dir");
        dir
    }

    /// A minimal but complete run: a report, a log with one arm line, and one
    /// rendered panel with its forced summary.
    fn write_minimal_run(dir: &Path) {
        fs::write(
            dir.join(REPORT_NAME),
            r#"{"schema":"mandate-check/5","verdict":"PASS","exit_code":0,
                 "mandates":{"M1":{"verdict":"PASS"},"M2":{"verdict":"PASS"}},
                 "rtp_mux":{"revision":"abc123"},"arms":[]}"#,
        )
        .expect("report");
        fs::write(
            dir.join(LOG_NAME),
            "[mandate-smoke clean] p99= 26.5 delivery= 1.000\n",
        )
        .expect("log");
        fs::write(dir.join(PLOTS_DIRNAME).join("M1-latency.svg"), "<svg/>").expect("svg");
        fs::write(
            dir.join(PLOTS_DIRNAME).join("M1-latency.summary.txt"),
            "summary| panel M1-latency\n",
        )
        .expect("panel summary");
    }

    fn args_for(dir: &Path) -> Args {
        Args {
            run: dir.to_path_buf(),
            // A baseline equal to the run keeps the test independent of the
            // environment and of the archive; the first-run path is exercised by
            // the missing-panels case below.
            baseline: Some(dir.to_path_buf()),
            label: "test-series".to_string(),
            archive: false,
            only_compare: false,
            no_ab: true,
            archive_root: dir.join("archive"),
        }
    }

    #[test]
    fn the_runner_prints_the_summary_and_vs_prev_and_names_every_panel_absolutely() {
        let dir = temp_run_dir();
        write_minimal_run(&dir);
        let mut out = Vec::new();
        let code = run(args_for(&dir), &mut out).expect("run");
        assert_eq!(code, 0);
        let printed = String::from_utf8(out).expect("utf8");
        let summary = fs::read_to_string(dir.join(SUMMARY_NAME)).expect("summary written");
        let vs_prev = fs::read_to_string(dir.join("vs-prev.md")).expect("vs-prev written");
        assert!(
            printed.contains(&summary),
            "the summary body must be printed verbatim, not only written"
        );
        assert!(
            printed.contains(&vs_prev),
            "the vs-prev body must be printed verbatim, not only written"
        );
        // The summary states the run's verdicts and its M1 outcome.
        assert!(summary.contains("verdict: PASS"), "{summary}");
        assert!(summary.contains("| M1 | PASS |"), "{summary}");
        assert!(summary.contains("no degradation"), "{summary}");
        // Every panel is named by an absolute path that resolves as of the run.
        let svg = dir
            .join(PLOTS_DIRNAME)
            .join("M1-latency.svg")
            .canonicalize()
            .expect("canonical svg");
        let text = svg.to_str().expect("utf8 path");
        assert!(
            summary.contains(text),
            "the summary must name the panel by its absolute path {text}"
        );
        assert!(
            printed.contains(text),
            "the printed output must carry the absolute panel path {text}"
        );
        assert!(svg.is_file(), "the named path must resolve");
    }

    #[test]
    fn a_run_without_panels_is_an_evidence_failure() {
        let dir = temp_run_dir();
        write_minimal_run(&dir);
        fs::remove_dir_all(dir.join(PLOTS_DIRNAME)).expect("remove plots");
        let mut out = Vec::new();
        let code = run(args_for(&dir), &mut out).expect("run");
        assert_eq!(
            code, EXIT_EVIDENCE_FAILURE,
            "a run that rendered no panel must not pass"
        );
    }

    fn parse(argv: &[&str]) -> Cli {
        let mut full = vec!["perf-history"];
        full.extend_from_slice(argv);
        Cli::try_parse_from(full).expect("the arguments parse")
    }

    // ── the measured band, against the archive's own readings ────────────────
    //
    // The three arrays are the **real** per-arm values of the eleven archived
    // runs in `.net-perf-history/mandate-check` (2026-09-27), in archive order.
    // They are the population the band is drawn from, and the reason a fixed
    // percentage cannot bound these quantities: `lone_tail p90` spans
    // 37.1-57.8 ms, so the accounted pair `37.1 -> 55.0` (+48 %) is *inside* the
    // arm's own spread while a settled 40 % band rejects it.
    const ARCHIVED_LONE_P90: [f64; 11] = [
        45.7, 45.7, 39.3, 38.6, 49.3, 57.8, 44.8, 52.3, 44.3, 37.1, 55.0,
    ];
    const ARCHIVED_LONE_P99: [f64; 11] = [
        161.4, 161.4, 174.8, 155.1, 189.3, 165.5, 155.0, 165.8, 169.3, 178.7, 165.0,
    ];
    const ARCHIVED_HOSTILE_P99: [f64; 11] = [
        138.1, 138.1, 123.9, 221.8, 150.8, 221.1, 203.2, 124.1, 122.5, 136.3, 125.1,
    ];

    fn archived_observations(count: usize) -> Vec<Observation> {
        let mut observations = Vec::new();
        for index in 0..count {
            // Six distinct revisions, as the real archive has: the population is
            // the series, not one revision, and the count is only reported.
            let revision = format!("rev{}", index % 6);
            for (arm, metric, value) in [
                ("mandate-smoke/lone_tail", "p90", ARCHIVED_LONE_P90[index]),
                ("mandate-smoke/lone_tail", "p99", ARCHIVED_LONE_P99[index]),
                ("mandate-smoke/hostile", "p99", ARCHIVED_HOSTILE_P99[index]),
            ] {
                observations.push(Observation {
                    revision: revision.clone(),
                    arm: arm.to_string(),
                    metric: metric.to_string(),
                    value,
                });
            }
        }
        observations
    }

    /// The population the candidate sees: the ten runs archived before it. The
    /// candidate's own entry is excluded, exactly as `observations` excludes the
    /// candidate's directory, so the fixture matches the tool.
    fn archived_population() -> Vec<Observation> {
        archived_observations(10)
    }

    type Arms = BTreeMap<String, BTreeMap<String, f64>>;

    fn arms(readings: &[(&str, &str, f64)]) -> Arms {
        let mut arms: Arms = BTreeMap::new();
        for (arm, metric, value) in readings {
            arms.entry(arm.to_string())
                .or_default()
                .insert(metric.to_string(), *value);
        }
        arms
    }

    /// The false positive the unit that found this defect watched fire: the
    /// battery PASSed 4/4 while the wrapper exited 6 on `lone_tail p90`.
    #[test]
    fn the_measured_band_absorbs_the_archived_false_positive() {
        let observations = archived_population();
        let baseline = arms(&[("mandate-smoke/lone_tail", "p90", 37.1)]);
        let candidate = arms(&[("mandate-smoke/lone_tail", "p90", 55.0)]);
        let rows = metric_rows(&baseline, &candidate, &observations);
        assert_eq!(rows.len(), 1, "one arm, one metric: {rows:?}");
        let row = &rows[0];
        let spread = row.spread.as_ref().expect("the archive supplies a spread");
        assert_eq!(spread.samples, 10, "the candidate's own entry is excluded");
        assert_eq!(spread.revisions, 6);
        assert!(
            row.bound > 55.0,
            "the bound must cover the observed healthy maximum 57.8 and this 55.0: {}",
            row.bound
        );
        assert_eq!(row.basis, "measured", "the measured half must bind here");
        assert!(
            !row.worse,
            "`lone_tail p90 37.1 -> 55.0` is inside the arm's own spread and must not be a regression"
        );
        assert!(
            m1_degradations(&rows).is_empty(),
            "the recorded false positive must not reject the run"
        );
    }

    /// The other side of the vacuity: a **genuine** degradation still rejects.
    /// The constructed value is the documented +300 ms one-way fault on the
    /// hostile arm (`rtp_mux/GATE.md`: `hostile p99` 1891.4 ms against a
    /// 166.7 ms deployed median), laid on a real 125.1 ms reading.
    #[test]
    fn a_genuine_degradation_still_rejects_and_names_its_bound() {
        let observations = archived_population();
        let baseline = arms(&[("mandate-smoke/hostile", "p99", 125.1)]);
        let candidate = arms(&[("mandate-smoke/hostile", "p99", 1891.4)]);
        let rows = metric_rows(&baseline, &candidate, &observations);
        let degradations = m1_degradations(&rows);
        assert_eq!(degradations.len(), 1, "one M1 degradation: {rows:?}");
        let row = degradations[0];
        assert!(row.worse);
        assert!(
            row.bound < 1000.0,
            "the bound must be a bound, not the observation: {}",
            row.bound
        );
        let named = format!(
            "{} {}: {} against the {} bound {:.2}",
            row.arm,
            row.metric,
            figure(row),
            row.basis,
            row.bound
        );
        for expected in [
            "mandate-smoke/hostile",
            "p99",
            "125.1000 -> 1891.4000",
            "bound",
        ] {
            assert!(named.contains(expected), "{named:?} must name {expected:?}");
        }
    }

    /// A modest, plausible rise on a tight quantity also rejects -- the rule is
    /// not only catching catastrophes. `lone_tail p99 165.0 -> 247.5` is +50 %,
    /// past the settled 40 % band on the arm's own p99.
    #[test]
    fn a_settled_band_rise_on_a_tight_quantity_still_rejects() {
        let observations = archived_population();
        let baseline = arms(&[("mandate-smoke/lone_tail", "p99", 165.0)]);
        let candidate = arms(&[("mandate-smoke/lone_tail", "p99", 247.5)]);
        let rows = metric_rows(&baseline, &candidate, &observations);
        assert_eq!(m1_degradations(&rows).len(), 1, "{rows:?}");
        assert_eq!(rows[0].basis, "settled");
    }

    /// The sample requirement: below [`MIN_SPREAD_SAMPLES`] the bound is the
    /// settled band and the report says so, rather than silently substituting a
    /// number the reader cannot see.
    #[test]
    fn a_thin_archive_falls_back_to_the_settled_band_and_says_so() {
        let thin: Vec<Observation> = archived_population()
            .into_iter()
            .filter(|observation| observation.metric == "p90")
            .take(7)
            .collect();
        assert_eq!(thin.len(), 7);
        let band = band_for(37.1, "p90", "mandate-smoke/lone_tail", &thin);
        assert!(band.spread.is_none(), "7 readings must not be trusted");
        assert_eq!(band.available, 7);
        assert_eq!(band.basis(), "settled (insufficient spread)");
        assert_eq!(band.bound(), 37.1 * 1.4);
        // The same arm and metric at the requirement is measured, not settled.
        let fat: Vec<Observation> = archived_population()
            .into_iter()
            .filter(|observation| observation.metric == "p90")
            .take(8)
            .collect();
        let band = band_for(37.1, "p90", "mandate-smoke/lone_tail", &fat);
        assert_eq!(band.available, 8);
        assert_eq!(band.basis(), "measured");
        assert!(band.bound() > band.settled);
    }

    /// The absolute floor: a relative test near zero is meaningless in the units
    /// the product promises. `hostile p50 2.4 -> 3.7` is +54 %, past the settled
    /// 40 % band, but 1.3 ms is not a change in the promise -- and on a thin
    /// archive (no measured half) the band alone would reject it.
    #[test]
    fn the_absolute_floor_stops_a_meaningless_ratio_near_zero() {
        let thin: Vec<Observation> = archived_population()
            .into_iter()
            .filter(|observation| {
                observation.arm == "mandate-smoke/hostile" && observation.metric == "p99"
            })
            .take(3)
            .collect();
        let baseline = arms(&[("mandate-smoke/hostile", "p50", 2.4)]);
        let candidate = arms(&[("mandate-smoke/hostile", "p50", 3.7)]);
        let rows = metric_rows(&baseline, &candidate, &thin);
        assert!(
            rows[0].spread.is_none(),
            "no measured half on a thin archive"
        );
        assert!(
            (rows[0].bound - 2.4 * 1.4).abs() < 1e-9,
            "the bound is the settled band alone: {}",
            rows[0].bound
        );
        assert!(
            !rows[0].worse,
            "1.3 ms on a 2.4 ms median is not a change in the product's promise"
        );
        // The floor is not blanket immunity: a real move past it still rejects.
        let candidate = arms(&[("mandate-smoke/hostile", "p50", 9.0)]);
        let rows = metric_rows(&baseline, &candidate, &thin);
        assert!(rows[0].worse, "6.6 ms past the band and the floor is worse");
    }

    /// Delivery is the one metric where a *fall* is the degradation; the
    /// direction must survive the band rewrite, or the rule inverts for it.
    #[test]
    fn delivery_still_needs_a_fall_past_its_band() {
        let observations: Vec<Observation> = (0..11)
            .map(|index| Observation {
                revision: format!("rev{index}"),
                arm: "mandate-smoke/clean".to_string(),
                metric: "delivery".to_string(),
                value: 1.0,
            })
            .collect();
        let baseline = arms(&[("mandate-smoke/clean", "delivery", 1.0)]);
        let shallow = arms(&[("mandate-smoke/clean", "delivery", 0.999)]);
        let rows = metric_rows(&baseline, &shallow, &observations);
        assert!(!rows[0].worse, "0.1 % off 1.000 is not a delivery failure");
        let deep = arms(&[("mandate-smoke/clean", "delivery", 0.85)]);
        let rows = metric_rows(&baseline, &deep, &observations);
        assert!(rows[0].worse, "a 15 % delivery fall is worse");
    }

    // ── the exit codes, end to end, on a synthetic archive ───────────────────

    /// Write one archived run: a report naming a revision and a log carrying the
    /// arm lines. The archive walk (`archived_runs`, `observations`) is what is
    /// under test, so this goes through the same `REPORT_NAME`/`LOG_NAME` pair a
    /// real archive entry has.
    fn write_run_files(dir: &Path, lone_p90: f64, lone_p99: f64, hostile_p99: f64) {
        fs::create_dir_all(dir).expect("run dir");
        fs::write(
            dir.join(REPORT_NAME),
            r#"{"revision":"aaaa","rtp_mux":{"revision":"aaaa"}}"#,
        )
        .expect("report");
        fs::write(
            dir.join(LOG_NAME),
            format!(
                "[mandate-smoke lone_tail] p50= 0.2 p90= {lone_p90} p99= {lone_p99} delivery= 1.000\n\
                 [mandate-smoke hostile  ] p50= 1.4 p90= 83.7 p99= {hostile_p99} delivery= 1.000\n"
            ),
        )
        .expect("log");
    }

    /// The eleven archived readings, written as an archive on disk, so the CLI's
    /// own exit code is exercised rather than only the decision function.
    fn write_archive(root: &Path) {
        for index in 0..11 {
            write_run_files(
                &root.join("mandate-check").join(format!("2026{index:02}")),
                ARCHIVED_LONE_P90[index],
                ARCHIVED_LONE_P99[index],
                ARCHIVED_HOSTILE_P99[index],
            );
        }
    }

    /// A candidate run directory: the same report and log, plus the panel and
    /// its forced summary the run owes. It lives **outside** the archive, so the
    /// population never contains the candidate.
    fn write_candidate_run(dir: &Path, lone_p90: f64, lone_p99: f64, hostile_p99: f64) {
        write_run_files(dir, lone_p90, lone_p99, hostile_p99);
        fs::create_dir_all(dir.join(PLOTS_DIRNAME)).expect("plots dir");
        fs::write(dir.join(PLOTS_DIRNAME).join("M1-latency.svg"), "<svg/>").expect("svg");
        fs::write(
            dir.join(PLOTS_DIRNAME).join("M1-latency.summary.txt"),
            "summary| panel M1-latency\n",
        )
        .expect("panel summary");
    }

    fn candidate_args(dir: &Path, root: &Path, baseline: &str) -> Args {
        Args {
            run: dir.to_path_buf(),
            baseline: Some(root.join("mandate-check").join(baseline)),
            label: "mandate-check".to_string(),
            archive: false,
            only_compare: false,
            no_ab: true,
            archive_root: root.to_path_buf(),
        }
    }

    #[test]
    fn the_cli_exits_0_on_the_archived_false_positive_and_6_on_a_genuine_degradation() {
        let root = temp_run_dir().join("archive");
        write_archive(&root);

        // (a) the recorded false positive: 37.1 -> 55.0 on `lone_tail p90`.
        let dir = temp_run_dir();
        write_candidate_run(&dir, 55.0, 165.0, 125.1);
        let mut out = Vec::new();
        let code = run(candidate_args(&dir, &root, "202609"), &mut out).expect("run");
        assert_eq!(
            code,
            0,
            "the archive's own spread covers 55.0, so the run must pass: {}",
            String::from_utf8_lossy(&out)
        );
        let vs_prev = fs::read_to_string(dir.join("vs-prev.md")).expect("vs-prev");
        assert!(vs_prev.contains("## M1: no degradation"), "{vs_prev}");
        assert!(
            vs_prev.contains("37.1000 -> 55.0000") && vs_prev.contains("(measured)"),
            "the derivation must be on the record: {vs_prev}"
        );

        // (b) a genuine degradation: the +300 ms fault's `hostile p99`.
        let dir = temp_run_dir();
        write_candidate_run(&dir, 55.0, 165.0, 1891.4);
        let mut out = Vec::new();
        let code = run(candidate_args(&dir, &root, "202610"), &mut out).expect("run");
        assert_eq!(
            code, EXIT_M1_DEGRADATION,
            "a genuine degraded run must still be rejected"
        );
        let printed = String::from_utf8(out).expect("utf8");
        for expected in ["mandate-smoke/hostile", "125.1000 -> 1891.4000", "314.83"] {
            assert!(
                printed.contains(expected),
                "the rejection must name {expected:?}: {printed}"
            );
        }
    }

    #[test]
    fn an_omitted_flag_takes_the_hand_rolled_default() {
        let args: Args = parse(&[]).into();
        assert_eq!(args.run, PathBuf::from("."));
        assert_eq!(args.baseline, None);
        assert_eq!(args.label, "mandate-check");
        assert!(args.archive);
        assert!(!args.only_compare);
        assert!(!args.no_ab);
    }

    #[test]
    fn every_flag_binds_to_its_option() {
        let args: Args = parse(&[
            "runs/one",
            "--baseline",
            "runs/base",
            "--label",
            "series",
            "--no-archive",
            "--only-compare",
            "--no-ab",
        ])
        .into();
        assert_eq!(args.run, PathBuf::from("runs/one"));
        assert_eq!(args.baseline, Some(PathBuf::from("runs/base")));
        assert_eq!(args.label, "series");
        assert!(!args.archive);
        assert!(args.only_compare);
        assert!(args.no_ab);
    }

    #[test]
    fn an_unknown_flag_is_a_usage_error() {
        assert!(Cli::try_parse_from(["perf-history", "--bogus"]).is_err());
    }
}
