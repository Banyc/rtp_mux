//! The documented counts: a number written into prose that a command already
//! determines is either *verified* against the source that determines it
//! (`DOC_COUNTS`) or *derived* -- the sentence names the command that prints it
//! (`DOC_DERIVED`).
//!
//! A count whose sentence or source is gone is a problem, not a skip: the
//! alternative is a check that cannot fail once the number it guarded has been
//! deleted.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::perf::{PerfRow, check_perf_relations, parse_perf_budgets, parse_perf_design};
use super::{Session, fmt_float, partition};
use crate::tools::json::Json;
use crate::tools::pyjson::repr_str;
use crate::tools::pyre::{Flags, Regex};

/// A prose count may state 0..12 as digits or as a numeral word.
const NUMERAL: &str = r"(?:[0-9]+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)";

/// The value of a numeral word; `third`..`fifth` are the ordinals the
/// producer-count sentence uses.
fn numeral_words() -> &'static BTreeMap<&'static str, i64> {
    static ONCE: std::sync::OnceLock<BTreeMap<&'static str, i64>> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let mut words: BTreeMap<&'static str, i64> = BTreeMap::new();
        for (value, word) in [
            "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
            "eleven", "twelve",
        ]
        .iter()
        .enumerate()
        {
            words.insert(*word, value as i64);
        }
        words.insert("third", 3);
        words.insert("fourth", 4);
        words.insert("fifth", 5);
        words
    })
}

/// One number in prose that a command already determines.
struct DocCount {
    label: &'static str,
    docs: &'static [&'static str],
    pattern: Regex,
    keys: &'static [&'static str],
    authority: &'static str,
}

/// One inventory claim whose tally was replaced by the command.
struct DocDerived {
    label: &'static str,
    doc: &'static str,
    marker: Regex,
    until: Regex,
    pointer: &'static str,
    forbidden: &'static [(&'static str, &'static str)],
}

fn regex(pattern: &str, flags: Flags) -> Regex {
    Regex::new_with_flags(pattern, flags).unwrap_or_else(|error| panic!("{pattern}: {error}"))
}

fn ignore_case(pattern: &str) -> Regex {
    regex(
        pattern,
        Flags {
            dotall: false,
            multiline: false,
            ignorecase: true,
        },
    )
}

fn multiline(pattern: &str) -> Regex {
    regex(
        pattern,
        Flags {
            dotall: false,
            multiline: true,
            ignorecase: false,
        },
    )
}

/// The verified prose counts.
fn doc_counts() -> &'static Vec<DocCount> {
    static ONCE: std::sync::OnceLock<Vec<DocCount>> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        vec![
            DocCount {
                label: "producers declared",
                docs: &["tools/PERF_INFRA.md", "tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(r"\b({NUMERAL}) producers are declared")),
                keys: &["producers"],
                authority: "the producers[] array of tools/mandate-producers.json",
            },
            DocCount {
                label: "the ordinal after the declared producers",
                docs: &["tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(r"\bA (third|fourth|fifth) producer is a registry entry"),
                keys: &["next_producer"],
                authority: "one past the producers[] array of tools/mandate-producers.json",
            },
            DocCount {
                label: "evidence files per run",
                docs: &["tools/PERF_INFRA.md", "tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(
                    r"(?:the|all|its) ({NUMERAL}) (?:expected )?evidence\s+files"
                )),
                keys: &["evidence_files"],
                authority: "two files (<id>.json, <id>.csv) per verdict section of the rtp_mux \
                            producer in tools/mandate-producers.json",
            },
            DocCount {
                label: "MANDATE lines per run",
                docs: &["tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(r"all ({NUMERAL}) `MANDATE` lines")),
                keys: &["verdicts"],
                authority: "the verdicts of the rtp_mux producer in tools/mandate-producers.json",
            },
            DocCount {
                label: "panels and plot files of a mandate-check run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(
                    r"\(({NUMERAL}) panels, ({NUMERAL}) SVG\+PNG plot\s+files\)"
                )),
                keys: &["baseline_panels", "baseline_plot_files"],
                authority: "the summed mandates[*].panels of tools/mandate-baseline.json, and two \
                            files per panel (SVG + PNG)",
            },
            DocCount {
                label: "mandates and verified panels of the baseline run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(
                    r"passed all ({NUMERAL}) mandates with ({NUMERAL})\s+verified SVG panels"
                )),
                keys: &["verdicts", "baseline_panels"],
                authority: "the verdicts of the rtp_mux producer in tools/mandate-producers.json, \
                            and the summed mandates[*].panels of tools/mandate-baseline.json",
            },
            DocCount {
                label: "arms recorded in the baseline run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(
                    r"recorded \*\*({NUMERAL}) arms from both producers\*\*"
                )),
                keys: &["arms_total"],
                authority: "len(arms) of tools/mandate-baseline.json",
            },
            DocCount {
                label: "rtp_mux arms in the baseline run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(r"\*\*: ({NUMERAL}) for\s+`rtp_mux`")),
                keys: &["arms_rtp_mux"],
                authority: "the arms of tools/mandate-baseline.json whose producer is rtp_mux",
            },
            DocCount {
                label: "per-mandate arm counts in the baseline run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(
                    r"\(({NUMERAL}) M1, ({NUMERAL}) M2, ({NUMERAL}) M3 reps and ({NUMERAL}) M4 arms\)"
                )),
                keys: &["arms_M1", "arms_M2", "arms_M3", "arms_M4"],
                authority: "the arms of tools/mandate-baseline.json grouped by mandate",
            },
            DocCount {
                label: "probes recorded in the baseline run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(r"and the ({NUMERAL}) probes\b")),
                keys: &["baseline_probes"],
                authority: "the arms of tools/mandate-baseline.json whose producer is netem_test",
            },
            DocCount {
                label: "probe arms in the probe section",
                docs: &["tools/PERF_INFRA.md", "tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(
                    r"\b({NUMERAL})(?:\s+[A-Za-z-]+){{0,3}}\s+arms in the `probe` section"
                )),
                keys: &["arms_probe"],
                authority: "the probe/<arm> keys of tools/mandate-arms.json",
            },
            DocCount {
                label: "perf-tier probes of the harness",
                docs: &["tools/PERF_INFRA.md", "tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(r"\b({NUMERAL}) perf-tier probes")),
                keys: &["arms_probe"],
                authority: "the probe/<arm> keys of tools/mandate-arms.json",
            },
            DocCount {
                label: "probe-* rows of the harness declaration",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(&format!(r"their ({NUMERAL}) `probe-\*` rows")),
                keys: &["harness_probe_rows"],
                authority: "the gate-perf-design rows of tests/GATE.md whose cells are in the \
                            probe-* namespace",
            },
            DocCount {
                label: "duration of the baseline run",
                docs: &["tools/PERF_INFRA.md"],
                pattern: ignore_case(r"The run took \*\*([0-9.]+) s\*\*"),
                keys: &["baseline_duration"],
                authority: "duration_seconds of tools/mandate-baseline.json",
            },
            DocCount {
                label: "producers a two-producer case runs",
                docs: &["tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(r"Its ({NUMERAL})-producer cases")),
                keys: &["producers"],
                authority: "the producers[] array of tools/mandate-producers.json",
            },
            DocCount {
                label: "counted floors applied to an unstated lane",
                docs: &["tools/MANDATE_SMOKE.md"],
                pattern: ignore_case(&format!(r"the ({NUMERAL}) bulk-lane byte counters")),
                keys: &["count_floor_counters"],
                authority: "len(COUNT_FLOORS_BYTES) in netem-test/src/tools/mandate_compare.rs",
            },
            DocCount {
                label: "reference families of the rtp_mux draft",
                docs: &["tools/PERF_PENDING_rtp_mux.md"],
                pattern: ignore_case(&format!(
                    r"splits into \*\*({NUMERAL}) reference families\*\* \(\*\*({NUMERAL}) named plus the default\*\*\)"
                )),
                keys: &["draft_families", "draft_named_families"],
                authority: "the baseline/baseline.<family> lines of the gate-budgets block of \
                            tools/PERF_PENDING_rtp_mux.md (one family per reference row, plus the \
                            default)",
            },
            DocCount {
                label: "relation counts of the rtp_mux draft",
                docs: &["tools/PERF_PENDING_rtp_mux.md"],
                pattern: ignore_case(&format!(
                    r"the ({NUMERAL}) rows are \*\*({NUMERAL}) orthogonal\*\*, \*\*({NUMERAL}) composite\*\*, \*\*({NUMERAL})\s+re-measurement\*\* and \*\*({NUMERAL}) baseline\*\*"
                )),
                keys: &[
                    "draft_rows",
                    "draft_orthogonal",
                    "draft_composite",
                    "draft_re_measurement",
                    "draft_baseline_rows",
                ],
                authority: "the gate-perf-relations summary the checker derives from the draft's \
                            own blocks, with its deliberately-unmeasured `TBD` costs substituted \
                            (a relation is derived from the cells, never from the cost)",
            },
            DocCount {
                label: "cost sums of the rtp_mux draft",
                docs: &["tools/PERF_PENDING_rtp_mux.md"],
                pattern: ignore_case(&format!(
                    r"declare ({NUMERAL}) s in `default` \(the four `mandate_smoke` rows and the constitution\s+gate\) and ({NUMERAL}) s in `perf` \(({NUMERAL}) rows, ({NUMERAL}) of them still `TBD`\)"
                )),
                keys: &[
                    "draft_cost_default_rounded",
                    "draft_cost_perf_rounded",
                    "draft_rows_perf",
                    "draft_unmeasured_perf",
                ],
                authority: "the summed nominal costs, row count and `TBD` count of the draft's \
                            gate-perf-design rows per tier (the default figure to the whole \
                            second, as the sentence states it)",
            },
            DocCount {
                label: "measurement targets of the rtp_mux draft",
                docs: &["tools/PERF_PENDING_rtp_mux.md"],
                pattern: ignore_case(&format!(r"cover\s+the ({NUMERAL}) measurement targets")),
                keys: &["draft_targets"],
                authority: "the distinct targets of the draft's gate-perf-design rows, with its \
                            deliberately-unmeasured `TBD` costs substituted so every row parses",
            },
            DocCount {
                label: "every declared reference, harness plus draft",
                docs: &["tools/PERF_PENDING_rtp_mux.md"],
                pattern: ignore_case(&format!(r"Every\s+declared reference \(all ({NUMERAL})\)")),
                keys: &["references_total"],
                authority: "the reference rows of tests/GATE.md plus those of \
                            tools/PERF_PENDING_rtp_mux.md",
            },
            DocCount {
                label: "baselines and namespaces of the harness declaration",
                docs: &["tests/GATE.md"],
                pattern: ignore_case(&format!(
                    r"The harness declares ({NUMERAL}) baselines, one per measurement family, and ({NUMERAL})\s+namespaces"
                )),
                keys: &["harness_families", "harness_namespaces"],
                authority: "the baseline/baseline.<family> lines and the members.<family> lines of \
                            the gate-budgets block of tests/GATE.md",
            },
            DocCount {
                label: "netem_scenarios rows beside the default baseline",
                docs: &["tests/GATE.md"],
                pattern: ignore_case(&format!(
                    r"the ({NUMERAL}) other `netem_scenarios` rows are stated against it"
                )),
                keys: &["harness_netem_rows"],
                authority: "the netem_scenarios:: rows of the gate-perf-design block of \
                            tests/GATE.md, less their baseline",
            },
            DocCount {
                label: "declared relation counts of the harness declaration",
                docs: &["tests/GATE.md"],
                pattern: ignore_case(&format!(
                    r"Declared: \*\*({NUMERAL}) orthogonal\*\* rows, \*\*({NUMERAL}) composite\*\* rows and \*\*({NUMERAL})\s+re-measurement\*\*, plus the ({NUMERAL}) baseline rows"
                )),
                keys: &[
                    "harness_orthogonal",
                    "harness_composite",
                    "harness_re_measurement",
                    "harness_baseline_rows",
                ],
                authority: "the gate-perf-relations summary the checker derives from the \
                            gate-perf-design and gate-budgets blocks of tests/GATE.md",
            },
            DocCount {
                label: "probe rows other than the probe family's reference",
                docs: &["tests/GATE.md"],
                pattern: ignore_case(&format!(
                    r"and the ({NUMERAL}) probes\s+differ from the probe cell"
                )),
                keys: &["harness_other_probe_rows"],
                authority: "the probe-* rows of tests/GATE.md, less the probe family's reference row",
            },
        ]
    })
}

/// The inventory claims whose tally was replaced by the command.
fn doc_derived() -> &'static Vec<DocDerived> {
    static ONCE: std::sync::OnceLock<Vec<DocDerived>> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        vec![
            DocDerived {
                label: "the rtp opt-in inventory",
                doc: "tools/PERF_INFRA.md",
                marker: multiline(r"^- \*\*`rtp`\*\*"),
                until: multiline(r"\n\*\*`proxy`\*\*"),
                pointer: "crates/rtp/tools/check-ignored.py",
                forbidden: &[
                    (
                        r"[0-9]+\s+`#\[ignore\]`d\s+tests",
                        "a transcribed total of #[ignore]d tests",
                    ),
                    (
                        r"`(?:perf-lane|probe|standard|full|perf)`\s+[0-9]+",
                        "a transcribed per-tier tally",
                    ),
                    (
                        r"[0-9]+\s+in-crate opt-ins",
                        "a transcribed in-crate opt-in count",
                    ),
                    (
                        r"[0-9]+\s+of\s+[0-9]+\s+probes?\s+record(?:ed)?",
                        "a transcribed probe-selfcheck count",
                    ),
                ],
            },
            DocDerived {
                label: "the mux perf-test tier tally",
                doc: "tools/PERF_INFRA.md",
                marker: multiline(r"^- \*\*`mux`\*\*"),
                until: multiline(r"\n\nThe checker's treatment"),
                pointer: "--crate ../mux",
                forbidden: &[
                    (
                        r"\b(?:one|two|[0-9]+)\s+`standard`-tier scenario",
                        "a transcribed tier tally",
                    ),
                    (r"\bno\s+`perf`-tier scenario", "a transcribed tier tally"),
                ],
            },
            DocDerived {
                label: "the rtp probe-selfcheck tally",
                doc: "tools/PERF_INFRA.md",
                marker: multiline(r"^\*\*Read report-only output"),
                until: multiline(r"\n\n"),
                pointer: "crates/rtp/tools/check-ignored.py",
                forbidden: &[
                    (
                        r"[0-9]+\s+of\s+[0-9]+\s+probes?\s+record(?:ed)?",
                        "a transcribed probe-selfcheck count",
                    ),
                    (
                        r"`(?:perf-lane|probe|standard|full|perf)`\s+[0-9]+",
                        "a transcribed per-tier tally",
                    ),
                ],
            },
        ]
    })
}

/// The value of a prose count, written as digits or as a numeral word.
fn doc_number(text: &str) -> Option<f64> {
    let word = text.trim().to_lowercase();
    if let Some(value) = numeral_words().get(word.as_str()) {
        return Some(*value as f64);
    }
    super::perf::py_float(&word)
}

/// Load a declaration the documented counts are derived from.
fn doc_json(root: &Path, relpath: &str, problems: &mut Vec<String>) -> Option<Json> {
    let path = root.join(relpath);
    let Ok(text) = std::fs::read_to_string(&path) else {
        problems.push(format!(
            "DOC COUNT: cannot derive: {relpath} does not exist"
        ));
        return None;
    };
    match crate::tools::json::parse(&text) {
        Ok(value) => Some(value),
        Err(error) => {
            problems.push(format!(
                "DOC COUNT: cannot derive: {relpath} is not JSON ({error})"
            ));
            None
        }
    }
}

/// Rows, families, the relation tally and the cost sums of one block set.
fn gate_block_counts(
    text: &str,
    source: &str,
    prefix: &str,
    problems: &mut Vec<String>,
    null_cost: Option<(&str, &str)>,
) -> BTreeMap<String, f64> {
    let mut values: BTreeMap<String, f64> = BTreeMap::new();
    let design = super::Layout::fenced_block(text, "gate-perf-design");
    let budgets_text = super::Layout::fenced_block(text, "gate-budgets");
    let (Some(design), Some(budgets_text)) = (design, budgets_text) else {
        problems.push(format!(
            "DOC COUNT: cannot derive: {source} has no gate-perf-design/gate-budgets block"
        ));
        return values;
    };
    let mut unmeasured: BTreeMap<String, i64> = BTreeMap::new();
    let design = if let Some((placeholder, replacement)) = null_cost {
        for line in design.lines() {
            if !line.contains(placeholder) {
                continue;
            }
            let (_, separator, rest) = partition(line, "=");
            if separator.is_empty() {
                continue;
            }
            let tier = rest.split('|').next().unwrap_or("").trim().to_string();
            *unmeasured.entry(tier).or_insert(0) += 1;
        }
        let pattern = format!(r"\b{}\b", placeholder);
        regex(&pattern, Flags::default()).replace_all(&design, replacement)
    } else {
        design
    };
    let rows = parse_perf_design(&design, &mut Vec::new());
    let budgets = parse_perf_budgets(&budgets_text, &mut Vec::new());
    values.insert(format!("{prefix}_rows"), rows.len() as f64);
    let targets: BTreeSet<String> = rows
        .iter()
        .map(|row| row.name.split("::").next().unwrap_or("").to_string())
        .collect();
    values.insert(format!("{prefix}_targets"), targets.len() as f64);
    let tiers: BTreeSet<String> = rows.iter().map(|row| row.tier.clone()).collect();
    for tier in &tiers {
        let tier_rows: Vec<&PerfRow> = rows.iter().filter(|row| &row.tier == tier).collect();
        let total: f64 = tier_rows.iter().map(|row| row.cost).sum();
        values.insert(format!("{prefix}_cost_{tier}"), total);
        values.insert(
            format!("{prefix}_cost_{tier}_rounded"),
            total.round_ties_even(),
        );
        values.insert(format!("{prefix}_rows_{tier}"), tier_rows.len() as f64);
        values.insert(
            format!("{prefix}_unmeasured_{tier}"),
            *unmeasured.get(tier).unwrap_or(&0) as f64,
        );
    }
    values.insert(
        format!("{prefix}_families"),
        (1 + budgets.named.len()) as f64,
    );
    values.insert(
        format!("{prefix}_named_families"),
        budgets.named.len() as f64,
    );
    values.insert(format!("{prefix}_namespaces"), budgets.members.len() as f64);
    let summary = check_perf_relations(&rows, &budgets, &mut Vec::new());
    let relations = summary
        .iter()
        .find(|line| line.contains("gate-perf-relations:"));
    let Some(relations) = relations else {
        problems.push(format!(
            "DOC COUNT: cannot derive: the checker printed no gate-perf-relations summary for \
             {source}"
        ));
        return values;
    };
    let pattern = regex(
        r"gate-perf-relations: ([0-9]+) orthogonal, ([0-9]+) composite, ([0-9]+) re-measurement, ([0-9]+) baseline of",
        Flags::default(),
    );
    let Some(found) = pattern.search(relations) else {
        problems.push(format!(
            "DOC COUNT: cannot derive: the checker printed no gate-perf-relations summary for \
             {source}"
        ));
        return values;
    };
    for (index, key) in ["orthogonal", "composite", "re_measurement", "baseline_rows"]
        .iter()
        .enumerate()
    {
        let value = found
            .group(index + 1)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0);
        values.insert(format!("{prefix}_{key}"), value);
    }
    values
}

/// The counts the harness's own `gate-*` blocks determine.
fn harness_gate_counts(root: &Path, problems: &mut Vec<String>) -> BTreeMap<String, f64> {
    let gate_path = root.join("tests").join("GATE.md");
    let Ok(text) = std::fs::read_to_string(&gate_path) else {
        problems.push("DOC COUNT: cannot derive: tests/GATE.md does not exist".to_string());
        return BTreeMap::new();
    };
    let design = super::Layout::fenced_block(&text, "gate-perf-design");
    let budgets_text = super::Layout::fenced_block(&text, "gate-budgets");
    let (Some(design), Some(budgets_text)) = (design, budgets_text) else {
        problems.push(
            "DOC COUNT: cannot derive: tests/GATE.md has no gate-perf-design/gate-budgets block"
                .to_string(),
        );
        return BTreeMap::new();
    };
    let mut inner: Vec<String> = Vec::new();
    let rows = parse_perf_design(&design, &mut inner);
    let budgets = parse_perf_budgets(&budgets_text, &mut inner);
    if !inner.is_empty() {
        problems.push(
            "note: the documented harness counts are not derived this run: the \
             gate-perf-design/gate-budgets blocks do not parse (see the perf declaration problems \
             above)"
                .to_string(),
        );
        return BTreeMap::new();
    }
    let mut values = gate_block_counts(&text, "tests/GATE.md", "harness", problems, None);
    let netem_rows = rows
        .iter()
        .filter(|row| {
            row.name.starts_with("netem_scenarios::")
                && row
                    .relation
                    .as_ref()
                    .is_some_and(|relation| relation.family.is_none())
        })
        .count();
    values.insert(
        "harness_netem_rows".to_string(),
        netem_rows.saturating_sub(1) as f64,
    );
    let probe_rows: Vec<&PerfRow> = rows
        .iter()
        .filter(|row| row.cells.iter().any(|cell| cell.starts_with("probe-")))
        .collect();
    values.insert("harness_probe_rows".to_string(), probe_rows.len() as f64);
    let probe_reference = budgets.named.get("probe");
    values.insert(
        "harness_other_probe_rows".to_string(),
        probe_rows
            .iter()
            .filter(|row| Some(&row.name) != probe_reference)
            .count() as f64,
    );
    values
}

/// The counts the `rtp_mux` draft declaration's own blocks determine.
fn draft_gate_counts(root: &Path, problems: &mut Vec<String>) -> BTreeMap<String, f64> {
    let path = root.join("tools").join("PERF_PENDING_rtp_mux.md");
    let Ok(text) = std::fs::read_to_string(&path) else {
        problems.push(
            "DOC COUNT: cannot derive: tools/PERF_PENDING_rtp_mux.md does not exist; if the draft \
             was applied and deleted, drop its entries from DOC_COUNTS in the Rust checker"
                .to_string(),
        );
        return BTreeMap::new();
    };
    gate_block_counts(
        &text,
        "tools/PERF_PENDING_rtp_mux.md",
        "draft",
        problems,
        Some(("TBD", "0")),
    )
}

/// `len(COUNT_FLOORS_BYTES)` from the Rust tool that owns the rule.
fn count_floor_counters(root: &Path, problems: &mut Vec<String>) -> BTreeMap<String, f64> {
    let path = root
        .join("netem-test")
        .join("src")
        .join("tools")
        .join("mandate_compare.rs");
    if !path.is_file() {
        problems.push(format!(
            "DOC COUNT: cannot derive: {} does not exist, so COUNT_FLOORS_BYTES has no declared \
             source",
            path.display()
        ));
        return BTreeMap::new();
    }
    let Ok(text) = std::fs::read_to_string(&path) else {
        problems.push(format!(
            "DOC COUNT: cannot derive: {} cannot be read",
            path.display()
        ));
        return BTreeMap::new();
    };
    let Some(start) = text.find("const COUNT_FLOORS_BYTES") else {
        problems.push(format!(
            "DOC COUNT: cannot derive: {} declares no COUNT_FLOORS_BYTES const array",
            path.display()
        ));
        return BTreeMap::new();
    };
    let Some(end) = text[start..].find("];") else {
        problems.push(format!(
            "DOC COUNT: cannot derive: {} declares no COUNT_FLOORS_BYTES const array",
            path.display()
        ));
        return BTreeMap::new();
    };
    let block = &text[start..start + end];
    let floors = regex(r#"\(\s*"[^"]+"\s*,\s*[0-9_]+\s*\)"#, Flags::default()).find_iter(block);
    if floors.is_empty() {
        problems.push(format!(
            "DOC COUNT: cannot derive: {}'s COUNT_FLOORS_BYTES declares no (\"<key>\", <bytes>) \
             entry",
            path.display()
        ));
        return BTreeMap::new();
    }
    let mut values = BTreeMap::new();
    values.insert("count_floor_counters".to_string(), floors.len() as f64);
    values
}

/// Every value the verified prose counts are checked against.
fn doc_count_values(root: &Path, problems: &mut Vec<String>) -> BTreeMap<String, f64> {
    let mut values: BTreeMap<String, f64> = BTreeMap::new();
    let producers = doc_json(root, "tools/mandate-producers.json", problems);
    let arms = doc_json(root, "tools/mandate-arms.json", problems);
    let baseline = doc_json(root, "tools/mandate-baseline.json", problems);

    if let Some(producers) = &producers {
        let entries: Vec<Json> = producers
            .get("producers")
            .and_then(Json::as_array)
            .map(|items| items.to_vec())
            .unwrap_or_default();
        values.insert("producers".to_string(), entries.len() as f64);
        values.insert("next_producer".to_string(), (entries.len() + 1) as f64);
        let verdicts: Vec<&Json> = entries
            .iter()
            .filter(|entry| entry.get("id").and_then(Json::as_str) == Some("rtp_mux"))
            .collect();
        if verdicts.is_empty() {
            problems.push(
                "DOC COUNT: cannot derive: tools/mandate-producers.json declares no rtp_mux \
                 producer, so its verdict sections are unknown"
                    .to_string(),
            );
        } else {
            let count = verdicts[0]
                .get("verdicts")
                .and_then(Json::as_array)
                .map(|items| items.len())
                .unwrap_or(0) as f64;
            values.insert("verdicts".to_string(), count);
            values.insert("evidence_files".to_string(), 2.0 * count);
        }
    }

    if let Some(arms) = &arms {
        let cells = arms.get("cells");
        let count = cells
            .and_then(Json::as_object)
            .map(|map| map.keys().filter(|key| key.starts_with("probe/")).count())
            .unwrap_or(0);
        values.insert("arms_probe".to_string(), count as f64);
    }

    if let Some(baseline) = &baseline {
        let recorded: Vec<Json> = baseline
            .get("arms")
            .and_then(Json::as_array)
            .map(|items| items.to_vec())
            .unwrap_or_default();
        values.insert("baseline_arms".to_string(), recorded.len() as f64);
        values.insert("arms_total".to_string(), recorded.len() as f64);
        values.insert(
            "arms_rtp_mux".to_string(),
            recorded
                .iter()
                .filter(|arm| arm.get("producer").and_then(Json::as_str) == Some("rtp_mux"))
                .count() as f64,
        );
        values.insert(
            "baseline_probes".to_string(),
            recorded
                .iter()
                .filter(|arm| arm.get("producer").and_then(Json::as_str) == Some("netem_test"))
                .count() as f64,
        );
        let mut by_mandate: BTreeMap<String, i64> = BTreeMap::new();
        for arm in &recorded {
            let mandate = arm
                .get("mandate")
                .and_then(Json::as_str)
                .unwrap_or("None")
                .to_string();
            *by_mandate.entry(mandate).or_insert(0) += 1;
        }
        for mandate in ["M1", "M2", "M3", "M4"] {
            values.insert(
                format!("arms_{mandate}"),
                *by_mandate.get(mandate).unwrap_or(&0) as f64,
            );
        }
        let mandates = baseline.get("mandates");
        let panels: i64 = mandates
            .and_then(Json::as_object)
            .map(|map| {
                map.values()
                    .map(|record| py_int_value(record.get("panels")))
                    .sum()
            })
            .unwrap_or(0);
        values.insert("baseline_panels".to_string(), panels as f64);
        values.insert("baseline_plot_files".to_string(), (2 * panels) as f64);
        if let Some(duration) = baseline.get("duration_seconds") {
            match duration {
                Json::Int(_) | Json::Float(_) => {
                    values.insert(
                        "baseline_duration".to_string(),
                        duration.as_f64().unwrap_or(0.0),
                    );
                }
                // Python's `isinstance(True, int)` is true, so a boolean
                // `duration_seconds` is `1.0` to the check that quotes it.
                Json::Bool(value) => {
                    values.insert(
                        "baseline_duration".to_string(),
                        if *value { 1.0 } else { 0.0 },
                    );
                }
                _ => {}
            }
        }
    }

    values.extend(harness_gate_counts(root, problems));
    let draft = draft_gate_counts(root, problems);
    if let (Some(draft_families), Some(harness_families)) =
        (draft.get("draft_families"), values.get("harness_families"))
    {
        values.insert(
            "references_total".to_string(),
            draft_families + harness_families,
        );
    }
    values.extend(draft);
    values.extend(count_floor_counters(root, problems));
    values
}

/// Python's `int(value or 0)`: an absent, null, zero, empty or boolean value
/// resolves the way the report's own arithmetic reads it.
fn py_int_value(value: Option<&Json>) -> i64 {
    match value {
        None | Some(Json::Null) => 0,
        Some(Json::Bool(value)) => i64::from(*value),
        Some(Json::Int(value)) => *value,
        Some(Json::Float(value)) => *value as i64,
        Some(Json::Str(text)) => text.trim().parse::<i64>().unwrap_or(0),
        Some(Json::Array(items)) => i64::from(!items.is_empty()),
        Some(Json::Object(_)) => 1,
    }
}

/// Verify every documented count that a command determines.
pub fn check_doc_counts(root: &Path) -> (Vec<String>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    let values = doc_count_values(root, &mut problems);
    let mut checked = 0usize;
    for entry in doc_counts() {
        for relpath in entry.docs {
            let path = root.join(relpath);
            let Ok(text) = std::fs::read_to_string(&path) else {
                problems.push(format!("DOC COUNT: {relpath} does not exist"));
                continue;
            };
            let matches = entry.pattern.find_iter(&text);
            if matches.is_empty() {
                problems.push(format!(
                    "DOC COUNT: {relpath} no longer states {}; a verified count that has left the \
                     prose cannot be checked - restore the sentence or drop this entry from \
                     DOC_COUNTS in the Rust checker",
                    repr_str(entry.label)
                ));
                continue;
            }
            for found in matches {
                for (index, key) in entry.keys.iter().enumerate() {
                    let written = found.group(index + 1).unwrap_or_default();
                    let Some(given) = doc_number(&written) else {
                        problems.push(format!(
                            "DOC COUNT: {relpath}: {} reads {}, which is not a number",
                            repr_str(entry.label),
                            repr_str(&written)
                        ));
                        continue;
                    };
                    let Some(derived) = values.get(*key) else {
                        problems.push(format!(
                            "DOC COUNT: {relpath}: {} cannot be checked: nothing determined a \
                             value for {} ({})",
                            repr_str(entry.label),
                            repr_str(key),
                            entry.authority
                        ));
                        continue;
                    };
                    checked += 1;
                    if (given - derived).abs() > super::DOC_COUNT_TOLERANCE {
                        problems.push(format!(
                            "DOC COUNT: {relpath}: {} says {}, but {} is what determines it ({}); \
                             update the document, or the declaration if the change is intended",
                            repr_str(entry.label),
                            repr_str(&written),
                            fmt_float(*derived, "g"),
                            entry.authority
                        ));
                    }
                }
            }
        }
    }
    for entry in doc_derived() {
        let path = root.join(entry.doc);
        let Ok(text) = std::fs::read_to_string(&path) else {
            problems.push(format!("DOC COUNT: {} does not exist", entry.doc));
            continue;
        };
        let Some(marker) = entry.marker.search(&text) else {
            problems.push(format!(
                "DOC COUNT: {} no longer has the sentence {} was derived into; restore it or drop \
                 this entry from DOC_DERIVED in the Rust checker",
                entry.doc,
                repr_str(entry.label)
            ));
            continue;
        };
        // Match offsets are character offsets; the region must be sliced in the
        // same coordinate system or a non-ASCII character before it shifts the
        // window (and can land mid-UTF-8).
        let chars: Vec<char> = text.chars().collect();
        let tail: String = chars[marker.end..].iter().collect();
        let stop = match entry.until.search(&tail) {
            Some(found) => marker.end + found.start,
            None => chars.len(),
        };
        let region: String = chars[marker.start..stop].iter().collect();
        let region = region.as_str();
        if !region.contains(entry.pointer) {
            problems.push(format!(
                "DOC COUNT: {}: {} no longer names {}; a derived count whose command is not named \
                 is unreadable - point the sentence at the command that prints it",
                entry.doc,
                repr_str(entry.label),
                repr_str(entry.pointer)
            ));
        }
        for (pattern, why) in entry.forbidden {
            let transcribed = regex(pattern, Flags::default()).search(region);
            if let Some(transcribed) = transcribed {
                problems.push(format!(
                    "DOC COUNT: {}: {} has a transcription back: {} is {why}, and the number is \
                     already printed by {}",
                    entry.doc,
                    repr_str(entry.label),
                    repr_str(&transcribed.whole()),
                    repr_str(entry.pointer)
                ));
            }
        }
    }
    let docs: BTreeSet<&str> = doc_counts()
        .iter()
        .flat_map(|entry| entry.docs.iter().copied())
        .collect();
    let summary = vec![format!(
        "  gate-doc-counts: {checked} verified count(s) across {} doc(s), {} derived inventory \
         claim(s) pinned to their checker",
        docs.len(),
        doc_derived().len()
    )];
    (problems, summary)
}

impl Session<'_> {
    /// Verify the documented counts of the harness repository.
    pub fn check_doc_counts(&self) -> (Vec<String>, Vec<String>) {
        check_doc_counts(&self.layout.root)
    }
}
