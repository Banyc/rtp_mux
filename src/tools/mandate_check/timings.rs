//! The run's wall-clock, observed on the child's output stream.
//!
//! A test's duration is libtest's own `--report-time` stamp, which libtest
//! takes around that test's own execution and which therefore stays that
//! test's own while the target's tests run concurrently. A mandate's duration
//! cannot come from libtest — its `MANDATE` line is printed from inside the
//! test that measured the section — so it stays the bracket between two
//! `MANDATE` lines and says so. A bracket this report's own two-decimal
//! resolution renders as `0.00` is reported **absent**, with the note that says
//! why: `0.00s` and "not measured" are the same figure to a reader.

use std::sync::OnceLock;

use crate::tools::pyre::Regex;

use super::value::{py_round, py_str};
use super::{
    DURATION_DECIMALS, DURATION_SOURCE_HARNESS, DURATION_SOURCE_STREAM_BRACKET, TEST_STATES,
    TIMING_ARGS, TIMING_ENV, TIMING_ENV_VALUE, TIMING_METHOD, TOTAL_FIT_FLOOR_SECONDS,
    TOTAL_FIT_TOLERANCE,
};

fn mandate_timing_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^MANDATE (?P<mandate>M[0-9]+) ", false).expect("compiles"))
}

fn test_result_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(r"^test (?P<name>\S+) \.\.\. ?(?P<tail>.*)$", false).expect("compiles")
    })
}

fn test_total_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^test result: .*finished in (?:(?P<minutes>[0-9]+)m )?(?P<seconds>[0-9]+(?:\.[0-9]+)?)s$",
            false,
        )
        .expect("compiles")
    })
}

fn test_stamp_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^(?P<state>ok|FAILED)(?:,.*)?\s+<(?P<seconds>[0-9]+(?:\.[0-9]+)?)s>$",
            false,
        )
        .expect("compiles")
    })
}

fn test_progress_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^has been running for over ", false).expect("compiles"))
}

/// One test's observed completion.
#[derive(Debug, Clone, PartialEq)]
pub struct TestTiming {
    pub target: String,
    pub name: String,
    pub state: String,
    pub started_at_seconds: f64,
    pub finished_at_seconds: f64,
    pub duration_seconds: Option<f64>,
    pub duration_source: Option<String>,
    pub producer: Option<String>,
}

/// One verdict section's bracket.
#[derive(Debug, Clone, PartialEq)]
pub struct MandateTiming {
    pub mandate: String,
    pub finished_at_seconds: f64,
    pub duration_seconds: Option<f64>,
    pub duration_source: Option<String>,
    pub duration_note: Option<String>,
    pub producer: Option<String>,
}

/// One target's own recorded total, and whether its stamps fit it.
#[derive(Debug, Clone, PartialEq)]
pub struct TargetTiming {
    pub target: String,
    pub total_seconds: Option<f64>,
    pub total_source: Option<String>,
    pub serial: bool,
    pub tests: usize,
    pub ran: usize,
    pub stamped: usize,
    pub sum_seconds: f64,
    pub max_seconds: Option<f64>,
    pub overlap_factor: Option<f64>,
    pub fits: Option<bool>,
    pub note: Option<String>,
    pub producer: Option<String>,
}

/// One producer's timeline, from its own child's start.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Timings {
    pub tests: Vec<TestTiming>,
    pub mandates: Vec<MandateTiming>,
    pub targets: Vec<TargetTiming>,
    pub problems: Vec<String>,
}

/// The run's per-test and per-mandate wall-clock, from the line timeline.
pub fn derive_timings(events: &[(f64, String)], target: &str, serial: bool) -> Timings {
    let mut tests: Vec<TestTiming> = Vec::new();
    let mut mandates: Vec<MandateTiming> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    let mut previous_completion = 0.0_f64;
    let mut previous_mandate = 0.0_f64;
    let mut previous_mandate_id: Option<String> = None;
    let mut total_seconds: Option<f64> = None;
    let mut awaiting: Option<String> = None;

    for (seconds, raw) in events.iter() {
        let seconds = *seconds;
        let line = raw.trim().to_string();
        if let Some(found) = mandate_timing_re().search(&line) {
            let mandate = found.named("mandate").unwrap_or_default();
            let label = match &previous_mandate_id {
                Some(previous) => format!("MANDATE {previous}'s line"),
                None => "the child's start".to_string(),
            };
            mandates.push(mandate_bracket(&mandate, seconds, previous_mandate, &label));
            previous_mandate = seconds;
            previous_mandate_id = Some(mandate);
            continue;
        }
        if let Some(found) = test_total_re().search(&line) {
            let minutes: i64 = found
                .named("minutes")
                .and_then(|text| text.parse().ok())
                .unwrap_or(0);
            let secs: f64 = found
                .named("seconds")
                .and_then(|text| text.parse().ok())
                .unwrap_or(0.0);
            total_seconds = Some(py_round(minutes as f64 * 60.0 + secs, 3));
            continue;
        }
        if let Some(found) = test_result_re().search(&line) {
            let tail = found.named("tail").unwrap_or_default().trim().to_string();
            let state = result_state(&tail);
            let Some(state) = state else {
                if test_progress_re().is_match(&tail) {
                    continue;
                }
                awaiting = Some(found.named("name").unwrap_or_default());
                continue;
            };
            awaiting = None;
            record(
                &mut tests,
                &mut previous_completion,
                target,
                &found.named("name").unwrap_or_default(),
                &state,
                seconds,
                stamp_seconds(&tail),
            );
            continue;
        }
        if let Some(name) = awaiting.clone()
            && let Some(state) = result_state(&line)
        {
            {
                record(
                    &mut tests,
                    &mut previous_completion,
                    target,
                    &name,
                    &state,
                    seconds,
                    stamp_seconds(&line),
                );
                awaiting = None;
            }
        }
    }
    let fitted = target_fit(target, &tests, total_seconds, serial, &mut problems);
    Timings {
        tests,
        mandates,
        targets: vec![fitted],
        problems,
    }
}

#[allow(clippy::too_many_arguments)]
fn record(
    tests: &mut Vec<TestTiming>,
    previous_completion: &mut f64,
    target: &str,
    name: &str,
    state: &str,
    seconds: f64,
    stamp: Option<f64>,
) {
    let mut entry = TestTiming {
        target: target.to_string(),
        name: name.to_string(),
        state: state.to_string(),
        started_at_seconds: *previous_completion,
        finished_at_seconds: seconds,
        duration_seconds: None,
        duration_source: None,
        producer: None,
    };
    if state != "ignored" {
        if let Some(stamp) = stamp {
            entry.duration_seconds = Some(py_round(stamp, 3));
            entry.duration_source = Some(DURATION_SOURCE_HARNESS.to_string());
        }
        *previous_completion = seconds;
    }
    tests.push(entry);
}

/// The libtest state a result line's tail names, or `None`.
fn result_state(tail: &str) -> Option<String> {
    TEST_STATES
        .iter()
        .find(|candidate| {
            tail == **candidate
                || tail.starts_with(&format!("{candidate} "))
                || tail.starts_with(&format!("{candidate},"))
        })
        .map(|candidate| (*candidate).to_string())
}

/// The harness's own time for a result, or `None` when it carries none.
fn stamp_seconds(text: &str) -> Option<f64> {
    let found = test_stamp_re().search(text.trim())?;
    found.named("seconds").and_then(|value| value.parse().ok())
}

/// Whether the report's own precision would print `seconds` as `0.00`.
pub fn renders_as_zero(seconds: f64) -> bool {
    let text = format!("{seconds:.width$}", width = DURATION_DECIMALS);
    text.parse::<f64>() == Ok(0.0)
}

/// One mandate's bracket, or the reason it is not a duration.
fn mandate_bracket(
    mandate: &str,
    seconds: f64,
    previous_seconds: f64,
    previous_label: &str,
) -> MandateTiming {
    let bracket = py_round((seconds - previous_seconds).max(0.0), 3);
    let mut entry = MandateTiming {
        mandate: mandate.to_string(),
        finished_at_seconds: seconds,
        duration_seconds: Some(bracket),
        duration_source: Some(DURATION_SOURCE_STREAM_BRACKET.to_string()),
        duration_note: None,
        producer: None,
    };
    if !renders_as_zero(bracket) {
        return entry;
    }
    entry.duration_seconds = None;
    entry.duration_note = Some(format!(
        "empty bracket: this MANDATE line was printed {bracket:.3}s after \
         {previous_label}, so the interval between the two verdict lines is \
         not a duration this section spent -- at the report's \
         {DURATION_DECIMALS}-decimal resolution it is zero, which is what \
         'unmeasured' looks like too. The bracket is a section's own \
         wall-clock only while one section runs inside it, and two sections \
         that read one shared arm measurement print their lines in the same \
         instant: the second closes an interval holding no run of its own, \
         and the run it asserts on is timed inside the bracket of the \
         section that measured it"
    ));
    entry
}

/// One target's own-recorded total, and whether its stamps fit it.
fn target_fit(
    target: &str,
    tests: &[TestTiming],
    total_seconds: Option<f64>,
    serial: bool,
    problems: &mut Vec<String>,
) -> TargetTiming {
    let ran: Vec<&TestTiming> = tests
        .iter()
        .filter(|entry| entry.state != "ignored")
        .collect();
    let stamped: Vec<&TestTiming> = tests
        .iter()
        .filter(|entry| entry.duration_source.as_deref() == Some(DURATION_SOURCE_HARNESS))
        .collect();
    let durations: Vec<f64> = stamped
        .iter()
        .map(|entry| entry.duration_seconds.unwrap_or(0.0))
        .collect();
    // Python's `sum([])` is the *integer* zero, and Rust's `Sum for f64`
    // starts from `-0.0` (the identity for IEEE addition), so the empty sum is
    // spelled in Python's terms here rather than in the iterator's.
    let summed = py_round(
        if durations.is_empty() {
            0.0
        } else {
            durations.iter().sum::<f64>()
        },
        3,
    );
    let largest = durations
        .iter()
        .copied()
        .fold(None, |best: Option<f64>, value| {
            Some(match best {
                Some(current) if current >= value => current,
                _ => value,
            })
        });
    let mut record = TargetTiming {
        target: target.to_string(),
        total_seconds,
        total_source: total_seconds.map(|_| "libtest-finished-in".to_string()),
        serial,
        tests: tests.len(),
        ran: ran.len(),
        stamped: stamped.len(),
        sum_seconds: summed,
        max_seconds: largest,
        overlap_factor: match total_seconds {
            Some(total) if !stamped.is_empty() && total > 0.0 => Some(py_round(summed / total, 3)),
            _ => None,
        },
        fits: None,
        note: None,
        producer: None,
    };
    if !ran.is_empty() && stamped.is_empty() {
        record.note = Some("no-test-carried-a-libtest-stamp".to_string());
        problems.push(format!(
            "the {target} target ran {} test(s) but not one of its results \
             carries libtest's own per-test stamp, so no test's duration could \
             be taken. The command ran it with '{}' and {TIMING_ENV}={TIMING_ENV_VALUE}; \
             a libtest that refuses those, or a --format that prints no stamp, \
             leaves every duration null rather than bracketed from the stream",
            ran.len(),
            TIMING_ARGS.join(" ")
        ));
        return record;
    }
    let Some(total) = total_seconds else {
        record.note = Some(
            if tests.is_empty() {
                "no-tests-ran"
            } else {
                "no-libtest-total-line"
            }
            .to_string(),
        );
        return record;
    };
    let slack = TOTAL_FIT_FLOOR_SECONDS.max(total * TOTAL_FIT_TOLERANCE);
    let oversized: Vec<&&TestTiming> = stamped
        .iter()
        .filter(|entry| entry.duration_seconds.unwrap_or(0.0) > total + slack)
        .collect();
    if !oversized.is_empty() {
        // Python's `max` keeps the first maximum, so a tie names the earlier
        // test's line rather than the later one's.
        let mut worst = oversized[0];
        for entry in oversized.iter().skip(1) {
            if entry.duration_seconds.unwrap_or(0.0) > worst.duration_seconds.unwrap_or(0.0) {
                worst = entry;
            }
        }
        record.fits = Some(false);
        let extra = if oversized.len() > 1 {
            format!("; {} stamp(s) exceed it", oversized.len())
        } else {
            String::new()
        };
        problems.push(format!(
            "{} reports {:.3}s, which cannot fit the {target} target's own \
             total of {total:.2}s (libtest's 'finished in'), so it is not that \
             test's own time{extra}",
            worst.name,
            worst.duration_seconds.unwrap_or(0.0)
        ));
        return record;
    }
    if serial && summed > total + slack {
        record.fits = Some(false);
        problems.push(format!(
            "the {target} target's stamped per-test times sum to {summed:.3}s \
             but the target ran serially (--test-threads=1) and finished in \
             {total:.2}s, so at least one stamp is not its own test's time"
        ));
        return record;
    }
    record.fits = Some(true);
    if !stamped.is_empty() && stamped.len() < ran.len() {
        record.note = Some(format!(
            "{} of {} test(s) carried no stamp and have a null duration",
            ran.len() - stamped.len(),
            ran.len()
        ));
    }
    record
}

/// Whether a producer's own arguments serialise its tests.
pub fn declared_serial(test_args: &[String]) -> bool {
    for (index, token) in test_args.iter().enumerate() {
        let value = if let Some(rest) = token.strip_prefix("--test-threads=") {
            rest.to_string()
        } else if token == "--test-threads" {
            test_args.get(index + 1).cloned().unwrap_or_default()
        } else {
            continue;
        };
        return value.trim() == "1";
    }
    false
}

/// The `timings.method` value, which the report states so a reader knows what
/// was measured.
pub fn method() -> &'static str {
    TIMING_METHOD
}

/// A mandate's reported duration must be one, or absent and explained.
pub fn mandate_duration_problems(
    mandates: &std::collections::BTreeMap<String, super::MandateReport>,
    verdicts: &[String],
) -> Vec<String> {
    let mut problems = Vec::new();
    for mandate in verdicts {
        let Some(record) = mandates.get(mandate) else {
            continue;
        };
        if record.raw_line.is_none() {
            continue;
        }
        let duration = record.duration_seconds;
        let note = record.duration_note.as_ref();
        let Some(duration) = duration else {
            if note.is_none() {
                problems.push(format!(
                    "{mandate}: its MANDATE line printed but the record carries \
                     neither a duration nor a note saying why it has none, so a \
                     reader cannot tell an unmeasured section from one whose \
                     bracket was dropped; an absent duration must say why"
                ));
            }
            continue;
        };
        if renders_as_zero(duration) {
            problems.push(format!(
                "{mandate}: reports a duration of {duration:.3}s, which the \
                 report's {DURATION_DECIMALS}-decimal resolution prints as \
                 0.00s -- a figure no reader can tell from 'not measured'. A \
                 bracket that resolves to zero must be reported as absent (with \
                 the note that says why), never as a duration"
            ));
        }
    }
    problems
}

/// A value's spelling in a message, so a mismatch names what it compared.
pub fn named(value: &crate::tools::json::Json) -> String {
    py_str(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(lines: &[(f64, &str)]) -> Vec<(f64, String)> {
        lines
            .iter()
            .map(|(seconds, line)| (*seconds, line.to_string()))
            .collect()
    }

    #[test]
    fn each_duration_comes_from_its_own_stamp() {
        let timings = derive_timings(
            &events(&[
                (0.0, "running 2 tests"),
                (0.10, "test a ... ok <1.234s>"),
                (9.90, "test b ... ok <0.205s>"),
                (
                    10.0,
                    "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 10.00s",
                ),
            ]),
            "mandate_smoke",
            false,
        );
        assert_eq!(timings.tests.len(), 2);
        assert_eq!(timings.tests[0].duration_seconds, Some(1.234));
        assert_eq!(
            timings.tests[1].duration_source.as_deref(),
            Some(DURATION_SOURCE_HARNESS)
        );
        // The bracket is not used: the second test's arrival is at 9.9 but its
        // own time is 0.205s.
        assert_eq!(timings.tests[1].duration_seconds, Some(0.205));
        assert_eq!(timings.tests[1].started_at_seconds, 0.10);
        assert_eq!(timings.targets[0].total_seconds, Some(10.0));
        assert_eq!(timings.targets[0].fits, Some(true));
        assert!(timings.problems.is_empty(), "{:?}", timings.problems);
    }

    #[test]
    fn a_stamp_less_result_is_marked_and_never_bracketed() {
        let timings = derive_timings(
            &events(&[
                (0.0, "running 1 test"),
                (0.10, "test a ... ok"),
                (
                    9.90,
                    "test result: ok. 1 passed; 0 failed; 0 ignored; finished in 9.90s",
                ),
            ]),
            "mandate_smoke",
            false,
        );
        assert_eq!(timings.tests[0].duration_seconds, None);
        assert_eq!(timings.tests[0].duration_source, None);
        assert_eq!(timings.targets[0].stamped, 0);
        assert_eq!(timings.targets[0].ran, 1);
        assert_eq!(
            timings.targets[0].note.as_deref(),
            Some("no-test-carried-a-libtest-stamp")
        );
        assert_eq!(timings.problems.len(), 1);
        assert!(timings.problems[0].contains("not one of its results carries"));
    }

    #[test]
    fn a_completion_split_by_the_tests_own_output_is_read_from_the_state_line() {
        let timings = derive_timings(
            &events(&[
                (0.0, "test probe ... probe: direct=3.157 Mpps"),
                (0.01, "ok <0.010s>"),
                (
                    0.02,
                    "test result: ok. 1 passed; 0 failed; finished in 0.02s",
                ),
            ]),
            "lib",
            false,
        );
        assert_eq!(timings.tests.len(), 1);
        assert_eq!(timings.tests[0].name, "probe");
        assert_eq!(timings.tests[0].duration_seconds, Some(0.010));
    }

    #[test]
    fn a_progress_note_is_not_a_completion() {
        let timings = derive_timings(
            &events(&[
                (0.0, "test slow ... has been running for over 60 seconds"),
                (0.5, "test slow ... ok <61.0s>"),
                (
                    1.0,
                    "test result: ok. 1 passed; 0 failed; finished in 61.00s",
                ),
            ]),
            "t",
            false,
        );
        assert_eq!(timings.tests.len(), 1);
        assert_eq!(timings.tests[0].duration_seconds, Some(61.0));
    }

    #[test]
    fn a_bracket_that_prints_as_zero_is_reported_unmeasured_and_says_why() {
        let timings = derive_timings(
            &events(&[
                (0.0, "MANDATE M1 PASS p99=1"),
                (10.0, "MANDATE M2 PASS clean_delivery=1.0"),
                (10.002, "MANDATE M4 PASS flows=4"),
            ]),
            "t",
            false,
        );
        assert_eq!(timings.mandates.len(), 3);
        // The first line arrives at the child's start, so its own bracket is
        // zero: this line is the one a reader cannot tell from unmeasured.
        assert_eq!(timings.mandates[0].duration_seconds, None);
        assert!(
            timings.mandates[0]
                .duration_note
                .as_deref()
                .expect("a note")
                .starts_with("empty bracket:")
        );
        assert_eq!(timings.mandates[1].duration_seconds, Some(10.0));
        assert_eq!(timings.mandates[2].duration_seconds, None);
        assert!(timings.mandates[2].duration_note.is_some());
    }

    #[test]
    fn a_serial_targets_stamps_must_sum_to_no_more_than_its_total() {
        let timings = derive_timings(
            &events(&[
                (0.0, "test a ... ok <6.0s>"),
                (6.0, "test b ... ok <6.0s>"),
                (12.0, "test result: ok. 2 passed; finished in 10.00s"),
            ]),
            "t",
            true,
        );
        assert_eq!(timings.targets[0].fits, Some(false));
        assert_eq!(timings.problems.len(), 1);
        assert!(timings.problems[0].contains("ran serially"));
    }

    #[test]
    fn a_concurrent_targets_overlap_is_recorded_rather_than_refused() {
        let timings = derive_timings(
            &events(&[
                (0.0, "test a ... ok <6.0s>"),
                (6.0, "test b ... ok <6.0s>"),
                (12.0, "test result: ok. 2 passed; finished in 10.00s"),
            ]),
            "t",
            false,
        );
        assert_eq!(timings.targets[0].fits, Some(true));
        assert_eq!(timings.targets[0].overlap_factor, Some(1.2));
        assert!(timings.problems.is_empty(), "{:?}", timings.problems);
    }

    #[test]
    fn a_stamp_over_the_targets_total_is_refused() {
        let timings = derive_timings(
            &events(&[
                (0.0, "test a ... ok <86.163s>"),
                (1.0, "test result: ok. 1 passed; finished in 10.00s"),
            ]),
            "t",
            false,
        );
        assert_eq!(timings.targets[0].fits, Some(false));
        assert_eq!(timings.problems.len(), 1);
        assert!(timings.problems[0].contains("cannot fit the t target's own total"));
    }

    #[test]
    fn a_serial_producer_is_read_from_its_declared_arguments() {
        assert!(declared_serial(&["--test-threads=1".to_string()]));
        assert!(declared_serial(&[
            "--test-threads".to_string(),
            "1".to_string()
        ]));
        assert!(!declared_serial(&["--nocapture".to_string()]));
        assert!(!declared_serial(&["--test-threads=2".to_string()]));
    }

    #[test]
    fn the_zero_render_rule_asks_the_reports_own_precision() {
        assert!(renders_as_zero(0.002));
        assert!(!renders_as_zero(0.01));
        assert!(renders_as_zero(0.0));
    }

    /// A note does not buy a printed zero: the reader still sees `0.00s`.
    #[test]
    fn a_duration_that_prints_as_zero_is_refused_even_with_a_note() {
        let mut mandates = std::collections::BTreeMap::new();
        let mut record = super::super::MandateReport::new("rtp_mux");
        record.raw_line = Some("MANDATE M1 PASS p99=1".to_string());
        record.duration_seconds = Some(0.0);
        record.duration_source = Some(DURATION_SOURCE_STREAM_BRACKET.to_string());
        record.duration_note = Some("the arms were measured under M2's bracket".to_string());
        mandates.insert("M1".to_string(), record);
        let problems = mandate_duration_problems(&mandates, &["M1".to_string()]);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("0.00s"), "{problems:?}");
        // The record the runner writes for a zero bracket is the green state on
        // the same rule: absent, with the note that says why.
        let mut record = super::super::MandateReport::new("rtp_mux");
        record.raw_line = Some("MANDATE M1 PASS p99=1".to_string());
        record.duration_note = Some("empty bracket: ...".to_string());
        mandates.insert("M1".to_string(), record);
        assert_eq!(
            mandate_duration_problems(&mandates, &["M1".to_string()]),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_mandate_timing_with_no_note_and_no_duration_is_refused() {
        let mut mandates = std::collections::BTreeMap::new();
        mandates.insert(
            "M1".to_string(),
            super::super::MandateReport {
                producer: "rtp_mux".to_string(),
                declared: true,
                verdict: Some("PASS".to_string()),
                values: Default::default(),
                raw_line: Some("MANDATE M1 PASS p99=1".to_string()),
                plots: Vec::new(),
                series_counts: Vec::new(),
                panels: 0,
                panel_summaries: Vec::new(),
                censoring_arms: Vec::new(),
                finished_at_seconds: None,
                duration_seconds: None,
                duration_source: None,
                duration_note: None,
            },
        );
        let problems = mandate_duration_problems(&mandates, &["M1".to_string()]);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("neither a duration nor a note"));
    }
}
