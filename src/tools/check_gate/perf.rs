//! The perf-test dual mandate: the declared tiers, budgets, baselines,
//! relations, cell-name namespaces and coverage gaps, and the drift check
//! against a fresh `mandate-check.json`.
//!
//! Every failure names the row or the line and what to write instead, so a
//! composite arm cannot pass as an orthogonal one and a scenario cannot be
//! filed under a tier whose budget it does not fit.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::scan::partition;
use super::{
    DEFAULT_DRIFT_FLOOR_SECONDS, DEFAULT_DRIFT_TOLERANCE, LIB_TARGET, PERF_TIERS, R,
    RELATION_KINDS, Session, cell_dimension_re, cell_key_re, cell_property_re, fmt_float,
    membership_prefix_re, relation_re, sort_families, sorted,
};
use crate::tools::json::Json;
use crate::tools::pyjson::{repr_float, repr_str};

/// One `gate-perf-design` row: a perf test, its tier, cost and coverage.
#[derive(Debug, Clone)]
pub struct PerfRow {
    pub name: String,
    pub tier: String,
    pub cost: f64,
    pub cells: Vec<String>,
    pub relation: Option<Relation>,
}

/// A row's declared relation to a `gate-budgets` baseline row.
#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub kind: String,
    pub keys: Vec<String>,
    pub reason: String,
    pub family: Option<String>,
}

/// The `gate-budgets` block: one budget per tier, plus the baselines.
#[derive(Debug, Clone)]
pub struct PerfBudgets {
    pub tiers: BTreeMap<String, f64>,
    pub baseline: Option<String>,
    pub drift: f64,
    pub drift_floor_seconds: f64,
    pub named: BTreeMap<String, String>,
    pub members: BTreeMap<String, String>,
}

impl Default for PerfBudgets {
    fn default() -> Self {
        PerfBudgets {
            tiers: BTreeMap::new(),
            baseline: None,
            drift: DEFAULT_DRIFT_TOLERANCE,
            drift_floor_seconds: DEFAULT_DRIFT_FLOOR_SECONDS,
            named: BTreeMap::new(),
            members: BTreeMap::new(),
        }
    }
}

/// One `gate-coverage-gaps` line: an uncovered cell and why it is empty.
#[derive(Debug, Clone)]
pub struct PerfGap {
    pub cell: String,
    pub reason: String,
}

/// The non-empty, non-comment lines of a perf block with their numbers.
pub fn perf_lines(block: &str) -> Vec<(usize, String)> {
    block
        .lines()
        .enumerate()
        .map(|(index, raw)| (index + 1, raw.trim().to_string()))
        .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

/// Why `cell` is not `<property>@<dimension>=<value>[+...]`, or `None`.
pub fn cell_problem(cell: &str) -> Option<String> {
    let (property_name, _, dimensions) = partition(cell, "@");
    if dimensions.is_empty() {
        return Some("no '@<dimension>=<value>' part".to_string());
    }
    if !cell_property_re().is_match(&property_name) {
        return Some(format!(
            "property {} is not a name ([A-Za-z][A-Za-z0-9_.-]*)",
            repr_str(&property_name)
        ));
    }
    for part in dimensions.split('+') {
        if !cell_dimension_re().is_match(part) {
            return Some(format!(
                "dimension {} is not '<dimension>=<value>'",
                repr_str(part)
            ));
        }
    }
    None
}

/// The relation text a diagnostic tells the author to write.
pub fn relation_display(kind: &str, family: Option<&str>) -> String {
    match family {
        Some(family) if !family.is_empty() => format!("{kind}@{family}"),
        _ => kind.to_string(),
    }
}

/// Parse a row's relation field, or say exactly what to write instead.
pub fn parse_relation(text: &str) -> (Option<Relation>, Option<String>) {
    let text = text.trim();
    let not_a_relation = format!(
        "relation {} is not a relation this grammar knows; write `baseline`, `orthogonal`, \
         `composite(<dimension>[,<dimension>...])` or `re-measurement(<reason>)`, each optionally \
         followed by `@<baseline-family>` to state it against a named baseline",
        repr_str(text)
    );
    let Some(found) = relation_re().search(text) else {
        let (kind, opened, _) = partition(text, "(");
        if !opened.is_empty() && !text.ends_with(')') {
            return (
                None,
                Some(format!(
                    "relation {} is missing its closing ')'; write {}(...)",
                    repr_str(text),
                    kind.trim()
                )),
            );
        }
        return (None, Some(not_a_relation));
    };
    let kind = found.named("kind").unwrap_or_default();
    let argument = found.named("argument");
    let family = found.named("family");
    if !RELATION_KINDS.contains(&kind.as_str()) {
        return (None, Some(not_a_relation));
    }
    let Some(argument) = argument else {
        if kind == "baseline" || kind == "orthogonal" {
            return (
                Some(Relation {
                    kind,
                    keys: Vec::new(),
                    reason: String::new(),
                    family,
                }),
                None,
            );
        }
        let tail = if kind == "composite" {
            "`composite(<dimension>[,<dimension>...])` naming the dimensions the row's cells vary"
        } else {
            "`re-measurement(<reason>)` naming why the row repeats the baseline's cell"
        };
        return (
            None,
            Some(format!(
                "relation {} gives no argument; write {tail}",
                repr_str(text)
            )),
        );
    };
    let inner = argument.trim().to_string();
    if kind == "baseline" || kind == "orthogonal" {
        return (
            None,
            Some(format!(
                "relation {} takes no argument; write `{}`",
                repr_str(text),
                relation_display(&kind, family.as_deref())
            )),
        );
    }
    if kind == "re-measurement" {
        if inner.is_empty() {
            return (
                None,
                Some(
                    "`re-measurement(<reason>)` names no reason; say why the row deliberately \
                     repeats the baseline's cell (a second tier, a stability re-run, a seed sweep)"
                        .to_string(),
                ),
            );
        }
        if inner.contains(',') || inner.contains('(') || inner.contains(')') || inner.contains('@')
        {
            return (
                None,
                Some(format!(
                    "re-measurement reason {} contains ',', a parenthesis or '@'; write one reason \
                     token (e.g. `re-measurement(full-tier-rerun)@<family>`)",
                    repr_str(&inner)
                )),
            );
        }
        return (
            Some(Relation {
                kind,
                keys: Vec::new(),
                reason: inner,
                family,
            }),
            None,
        );
    }
    let mut keys: Vec<String> = Vec::new();
    for part in inner.split(',') {
        let key = part.trim();
        if key.is_empty() {
            continue;
        }
        if !cell_key_re().is_match(key) {
            return (
                None,
                Some(format!(
                    "composite dimension {} is not a name ([A-Za-z][A-Za-z0-9_-]*)",
                    repr_str(key)
                )),
            );
        }
        if keys.iter().any(|existing| existing == key) {
            return (
                None,
                Some(format!(
                    "composite(...) names the dimension {} twice",
                    repr_str(key)
                )),
            );
        }
        keys.push(key.to_string());
    }
    if keys.len() < 2 {
        return (
            None,
            Some(
                "`composite(...)` must name at least two dimensions; a row that varies exactly one \
                 dimension from the baseline is `orthogonal`"
                    .to_string(),
            ),
        );
    }
    (
        Some(Relation {
            kind,
            keys,
            reason: String::new(),
            family,
        }),
        None,
    )
}

/// Parse the `gate-perf-design` rows, naming every malformed one.
pub fn parse_perf_design(text: &str, problems: &mut Vec<String>) -> Vec<PerfRow> {
    let mut rows: Vec<PerfRow> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (number, line) in perf_lines(text) {
        let (name, separator, rest) = partition(&line, " = ");
        if separator.is_empty() {
            problems.push(format!(
                "gate-perf-design line {number}: {} is not '<target>::<test> = <tier> | \
                 <nominal_cost_s> | <relation> | <coverage>'",
                repr_str(&line)
            ));
            continue;
        }
        let name = name.trim().to_string();
        let parts: Vec<String> = rest
            .split('|')
            .map(|part| part.trim().to_string())
            .collect();
        if parts.len() != 3 && parts.len() != 4 {
            problems.push(format!(
                "gate-perf-design row {name}: expected '<tier> | <cost_s> | <relation> | \
                 <coverage>', found {} field(s); a row without its relation to the baseline is \
                 not a declaration",
                parts.len()
            ));
            continue;
        }
        let (tier, cost_text, relation_text, coverage_text) = if parts.len() == 4 {
            (
                parts[0].clone(),
                parts[1].clone(),
                Some(parts[2].clone()),
                parts[3].clone(),
            )
        } else {
            (parts[0].clone(), parts[1].clone(), None, parts[2].clone())
        };
        let mut relation: Option<Relation> = None;
        if let Some(relation_text) = &relation_text {
            let (parsed, reason) = parse_relation(relation_text);
            if let Some(reason) = reason {
                problems.push(format!("gate-perf-design row {name}: {reason}"));
            } else {
                relation = parsed;
            }
        }
        if !PERF_TIERS.contains(&tier.as_str()) {
            problems.push(format!(
                "gate-perf-design row {name}: unknown tier {} (one of {})",
                repr_str(&tier),
                sorted(PERF_TIERS.iter().copied()).join(", ")
            ));
            continue;
        }
        let Some(cost) = py_float(&cost_text) else {
            problems.push(format!(
                "gate-perf-design row {name}: nominal cost {} is not a number of seconds",
                repr_str(&cost_text)
            ));
            continue;
        };
        if cost < 0.0 {
            problems.push(format!(
                "gate-perf-design row {name}: nominal cost {} is negative",
                repr_float(cost)
            ));
            continue;
        }
        let cells: Vec<String> = coverage_text
            .split(',')
            .map(|cell| cell.trim().to_string())
            .filter(|cell| !cell.is_empty())
            .collect();
        if cells.is_empty() {
            problems.push(format!(
                "gate-perf-design row {name}: no coverage cell; a perf test must name the cells it \
                 covers, and a cell it does not cover belongs in gate-coverage-gaps with a reason"
            ));
        }
        for cell in &cells {
            if let Some(reason) = cell_problem(cell) {
                problems.push(format!(
                    "gate-perf-design row {name}: coverage cell {} is malformed: {reason}",
                    repr_str(cell)
                ));
            }
        }
        if seen.contains(&name) {
            problems.push(format!("gate-perf-design: duplicate row {name}"));
        }
        seen.insert(name.clone());
        rows.push(PerfRow {
            name,
            tier,
            cost,
            cells,
            relation,
        });
    }
    rows
}

/// Parse `gate-budgets`: `<tier> = <budget_s>` plus the baseline families.
pub fn parse_perf_budgets(text: &str, problems: &mut Vec<String>) -> PerfBudgets {
    let mut budgets = PerfBudgets::default();
    for (number, line) in perf_lines(text) {
        let (key, separator, value) = partition(&line, " = ");
        let key = key.trim().to_string();
        let value = value.trim().to_string();
        if separator.is_empty() {
            problems.push(format!(
                "gate-budgets line {number}: {} is not '<tier> = <budget_s>'",
                repr_str(&line)
            ));
            continue;
        }
        if key == "baseline" {
            if value.is_empty() {
                problems.push("gate-budgets: the baseline line names no row".to_string());
                continue;
            }
            budgets.baseline = Some(value);
            continue;
        }
        if let Some(family) = key.strip_prefix("baseline.") {
            let family = family.trim().to_string();
            if !cell_key_re().is_match(&family) {
                problems.push(format!(
                    "gate-budgets line {number}: {} does not name a baseline family; write \
                     `baseline.<family> = <row>` with a family name ([A-Za-z][A-Za-z0-9_-]*)",
                    repr_str(&key)
                ));
                continue;
            }
            if value.is_empty() {
                problems.push(format!("gate-budgets: baseline.{family} names no row"));
                continue;
            }
            if budgets.named.contains_key(&family) {
                problems.push(format!(
                    "gate-budgets: duplicate baseline family {}",
                    repr_str(&family)
                ));
                continue;
            }
            budgets.named.insert(family, value);
            continue;
        }
        if key == "members" {
            problems.push(format!(
                "gate-budgets line {number}: {} names no family, and the default family's \
                 namespace is the residual (every cell name no `members.<family>` claims), so it \
                 needs no declaration; write `members.<family> = <prefix>` for each named family",
                repr_str(&key)
            ));
            continue;
        }
        if let Some(family) = key.strip_prefix("members.") {
            let family = family.trim().to_string();
            if !cell_key_re().is_match(&family) {
                problems.push(format!(
                    "gate-budgets line {number}: {} does not name a family; write \
                     `members.<family> = <prefix>` with a family name ([A-Za-z][A-Za-z0-9_-]*)",
                    repr_str(&key)
                ));
                continue;
            }
            if !membership_prefix_re().is_match(&value) {
                problems.push(format!(
                    "gate-budgets line {number}: members.{family} = {} does not name a cell-name \
                     namespace; write a cell name ([A-Za-z][A-Za-z0-9_.-]*), optionally followed \
                     by '*' to make it the prefix its family's cell names start with",
                    repr_str(&value)
                ));
                continue;
            }
            if budgets.members.contains_key(&family) {
                problems.push(format!(
                    "gate-budgets: duplicate members line for family {}",
                    repr_str(&family)
                ));
                continue;
            }
            budgets.members.insert(family, value);
            continue;
        }
        if key == "drift" || key == "drift_floor_s" {
            let Some(parsed) = py_float(&value) else {
                problems.push(format!(
                    "gate-budgets: {key} {} is not a number",
                    repr_str(&value)
                ));
                continue;
            };
            if parsed < 0.0 {
                problems.push(format!(
                    "gate-budgets: {key} {} is negative",
                    repr_float(parsed)
                ));
                continue;
            }
            if key == "drift" {
                budgets.drift = parsed;
            } else {
                budgets.drift_floor_seconds = parsed;
            }
            continue;
        }
        if !PERF_TIERS.contains(&key.as_str()) {
            problems.push(format!(
                "gate-budgets line {number}: {} is neither a tier nor one of \
                 baseline/drift/drift_floor_s (nor a `baseline.<family>`, `members.<family>` line)",
                repr_str(&key)
            ));
            continue;
        }
        if budgets.tiers.contains_key(&key) {
            problems.push(format!("gate-budgets: duplicate budget for tier {key}"));
            continue;
        }
        let Some(budget) = py_float(&value) else {
            problems.push(format!(
                "gate-budgets: tier {key} budget {} is not a number",
                repr_str(&value)
            ));
            continue;
        };
        if budget < 0.0 {
            problems.push(format!(
                "gate-budgets: tier {key} budget {} is negative",
                repr_float(budget)
            ));
            continue;
        }
        budgets.tiers.insert(key, budget);
    }
    budgets
}

/// Parse the `gate-coverage-gaps` lines: `<cell> = <reason>`.
pub fn parse_perf_gaps(text: &str, problems: &mut Vec<String>) -> Vec<PerfGap> {
    let mut gaps: Vec<PerfGap> = Vec::new();
    for (number, line) in perf_lines(text) {
        let (mut cell, separator, mut reason) = partition(&line, " = ");
        cell = cell.trim().to_string();
        reason = reason.trim().to_string();
        if separator.is_empty() {
            if line.ends_with('=') {
                cell = line[..line.len() - 1].trim().to_string();
                reason = String::new();
            } else {
                problems.push(format!(
                    "gate-coverage-gaps line {number}: {} is not '<cell> = <reason>'",
                    repr_str(&line)
                ));
                continue;
            }
        }
        if cell.is_empty() {
            problems.push(format!(
                "gate-coverage-gaps line {number}: the gap names no cell"
            ));
            continue;
        }
        if reason.is_empty() {
            problems.push(format!(
                "gate-coverage-gaps: cell {} records no reason; a cell may be knowingly empty but \
                 never silently empty",
                repr_str(&cell)
            ));
            continue;
        }
        if let Some(problem) = cell_problem(&cell) {
            problems.push(format!(
                "gate-coverage-gaps: cell {} is malformed: {problem}",
                repr_str(&cell)
            ));
            continue;
        }
        gaps.push(PerfGap { cell, reason });
    }
    gaps
}

/// `dimension -> value` for one cell, naming a dimension stated twice.
fn cell_state(cell: &str, ambiguous: &mut Vec<String>) -> BTreeMap<String, String> {
    if !cell.contains('@') {
        return BTreeMap::new();
    }
    let dimensions = cell.split_once('@').map(|(_, rest)| rest).unwrap_or("");
    let mut state: BTreeMap<String, String> = BTreeMap::new();
    for part in dimensions.split('+') {
        let (key, value) = match part.split_once('=') {
            Some((key, value)) => (key.to_string(), value.to_string()),
            None => (part.to_string(), String::new()),
        };
        if let Some(existing) = state.get(&key)
            && existing != &value
        {
            {
                ambiguous.push(format!(
                    "the cell {} states {} as both {} and {}",
                    repr_str(cell),
                    repr_str(&key),
                    repr_str(existing),
                    repr_str(&value)
                ));
                continue;
            }
        }
        state.insert(key, value);
    }
    state
}

/// Verify each row's declared relation against the dimensions its cells vary.
pub fn check_perf_relations(
    rows: &[PerfRow],
    budgets: &PerfBudgets,
    problems: &mut Vec<String>,
) -> Vec<String> {
    let mut summary: Vec<String> = Vec::new();
    let Some(baseline_name) = budgets.baseline.clone() else {
        return summary;
    };
    let by_name: BTreeMap<&str, &PerfRow> =
        rows.iter().map(|row| (row.name.as_str(), row)).collect();
    if !by_name.contains_key(baseline_name.as_str()) {
        return summary;
    }

    let mut families: Vec<(Option<String>, String)> = vec![(None, baseline_name.clone())];
    families.extend(
        budgets
            .named
            .iter()
            .map(|(family, name)| (Some(family.clone()), name.clone())),
    );

    let mut reference_of: BTreeMap<String, BTreeSet<Option<String>>> = BTreeMap::new();
    let mut base_state: BTreeMap<Option<String>, BTreeMap<String, String>> = BTreeMap::new();
    for (family, name) in &families {
        let Some(row) = by_name.get(name.as_str()) else {
            if family.is_some() {
                problems.push(format!(
                    "gate-budgets: baseline.{} names {}, which is not a gate-perf-design row, so \
                     the rows stated against it are stated against nothing",
                    family.clone().unwrap_or_default(),
                    repr_str(name)
                ));
            }
            continue;
        };
        reference_of
            .entry(name.clone())
            .or_default()
            .insert(family.clone());
        let mut conflicts: Vec<String> = Vec::new();
        let mut state: BTreeMap<String, String> = BTreeMap::new();
        for cell in &row.cells {
            for (key, value) in cell_state(cell, &mut conflicts) {
                state.insert(key, value);
            }
        }
        if !conflicts.is_empty() {
            problems.push(format!(
                "gate-perf-design baseline row {name}: its cells are not one point, so no row's \
                 relation to it can be determined ({}); state the baseline once per dimension, \
                 with one value each",
                conflicts.join("; ")
            ));
            continue;
        }
        base_state.insert(family.clone(), state);
    }
    for (name, references) in &reference_of {
        if references.len() > 1 {
            let labels: Vec<String> = references
                .iter()
                .map(|family| match family {
                    None => "the default baseline".to_string(),
                    Some(family) => format!("baseline.{family}"),
                })
                .collect();
            problems.push(format!(
                "gate-budgets: {} is the reference row of {}; a row carries one relation, so give \
                 each family its own reference row",
                repr_str(name),
                labels.join(", ")
            ));
        }
    }

    let against_phrase = |family: &Option<String>| match family {
        None => "the baseline".to_string(),
        Some(family) => format!("baseline {}", repr_str(family)),
    };

    let mut varied_by_row: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut counts: BTreeMap<String, i64> = RELATION_KINDS
        .iter()
        .map(|kind| (kind.to_string(), 0i64))
        .collect();
    let mut family_counts: BTreeMap<Option<String>, BTreeMap<String, i64>> = BTreeMap::new();
    for row in rows {
        let relation = row.relation.as_ref();
        let family = relation.and_then(|relation| relation.family.clone());
        if let Some(family) = &family
            && !budgets.named.contains_key(family)
        {
            {
                problems.push(format!(
                    "gate-perf-design row {}: it names the baseline family {}, which gate-budgets \
                     does not declare; declare `baseline.{family} = <row>` or state the row \
                     against the default baseline ({baseline_name})",
                    row.name,
                    repr_str(family)
                ));
                continue;
            }
        }
        if !base_state.contains_key(&family) {
            continue;
        }
        let against = against_phrase(&family);
        let referenced = families
            .iter()
            .find(|(candidate, _)| candidate == &family)
            .map(|(_, name)| name.clone())
            .unwrap_or_default();
        let mut conflicted: Vec<String> = Vec::new();
        let mut varied: BTreeSet<String> = BTreeSet::new();
        for cell in &row.cells {
            let state = cell_state(cell, &mut conflicted);
            for (key, value) in state {
                let base = &base_state[&family];
                if base.get(&key) != Some(&value) {
                    varied.insert(key);
                }
            }
        }
        let derived: Vec<String> = varied.into_iter().collect();
        varied_by_row.insert(row.name.clone(), derived.clone());
        if !conflicted.is_empty() {
            problems.push(format!(
                "gate-perf-design row {}: its relation to {against} cannot be determined ({}), so \
                 the dimensions it varies are ambiguous; state each dimension once, with one value \
                 (split the row if it covers two points)",
                row.name,
                conflicted.join("; ")
            ));
            continue;
        }
        let wanted = relation_display(
            &if derived.len() == 1 {
                "orthogonal".to_string()
            } else if !derived.is_empty() {
                format!("composite({})", derived.join(","))
            } else {
                "re-measurement(<reason>)".to_string()
            },
            family.as_deref(),
        );

        let mut bump = |kind: &str, family: &Option<String>| {
            *counts.entry(kind.to_string()).or_insert(0) += 1;
            let tally = family_counts.entry(family.clone()).or_default();
            *tally.entry(kind.to_string()).or_insert(0) += 1;
        };

        if reference_of
            .get(&row.name)
            .is_some_and(|references| references.contains(&family))
        {
            if relation.is_some_and(|relation| relation.kind == "baseline") {
                bump("baseline", &family);
                continue;
            }
            if reference_of.get(&row.name).map(BTreeSet::len) == Some(1) {
                let label = relation_display("baseline", family.as_deref());
                if family.is_none() {
                    problems.push(format!(
                        "gate-perf-design row {}: it is the gate-budgets baseline, so its relation \
                         is the reference every other row is stated against; write `{label}`",
                        row.name
                    ));
                } else {
                    problems.push(format!(
                        "gate-perf-design row {}: it is the reference row of baseline.{} \
                         ({referenced}), so its relation is what every other row in that family is \
                         stated against; write `{label}`",
                        row.name,
                        family.clone().unwrap_or_default()
                    ));
                }
            }
            continue;
        }
        let Some(relation) = relation else {
            problems.push(format!(
                "gate-perf-design row {}: it declares no relation to {against} {referenced}; its \
                 cells vary {} dimension(s), so write `{wanted}`",
                row.name,
                derived.len()
            ));
            continue;
        };
        if relation.kind == "baseline" {
            if family.is_none() && reference_of.contains_key(&row.name) {
                let only = reference_of[&row.name].iter().next().cloned().flatten();
                let named = match &only {
                    None => "the default baseline".to_string(),
                    Some(only) => format!(
                        "baseline.{only} ({})",
                        families
                            .iter()
                            .find(|(candidate, _)| candidate == &Some(only.clone()))
                            .map(|(_, name)| name.clone())
                            .unwrap_or_default()
                    ),
                };
                problems.push(format!(
                    "gate-perf-design row {}: it is the reference row of {named}, so its relation \
                     is what every other row in that family is stated against; write `{}`",
                    row.name,
                    relation_display("baseline", only.as_deref())
                ));
                continue;
            }
            if family.is_none() {
                problems.push(format!(
                    "gate-perf-design row {}: it is labelled `baseline`, but the baseline is \
                     {baseline_name}; a row's cells vary {} dimension(s) from it, so write \
                     `{wanted}`",
                    row.name,
                    derived.len()
                ));
            } else {
                let named_family = family.clone().unwrap_or_default();
                problems.push(format!(
                    "gate-perf-design row {}: it is labelled `baseline@{named_family}`, but \
                     baseline.{named_family} is {}, so a baseline label names only the family \
                     whose reference row the row is; its cells vary {} dimension(s) from \
                     {against} {referenced}, so write `{wanted}`",
                    row.name,
                    budgets
                        .named
                        .get(&named_family)
                        .cloned()
                        .unwrap_or_default(),
                    derived.len()
                ));
            }
            continue;
        }
        let mut problem: Option<String> = None;
        if derived.is_empty() && relation.kind != "re-measurement" {
            problem = Some(format!(
                "gate-perf-design row {}: its cells name no dimension that differs from {against} \
                 {referenced} (every dimension it names repeats the baseline's value), so it is a \
                 deliberate repeat and must say why; write `re-measurement(<reason>)` (e.g. a \
                 second tier or a stability re-run), not `{}`",
                row.name, relation.kind
            ));
        } else if derived.len() == 1 && relation.kind != "orthogonal" {
            problem = Some(format!(
                "gate-perf-design row {}: it varies exactly one dimension from {against} ({}), so \
                 write `{}`, not `{}`",
                row.name,
                derived[0],
                relation_display("orthogonal", family.as_deref()),
                relation.kind
            ));
        } else if derived.len() > 1 && relation.kind == "re-measurement" {
            problem = Some(format!(
                "gate-perf-design row {}: it is labelled a re-measurement, but its cells vary {} \
                 dimension(s) from {against} ({}), so write `{wanted}`",
                row.name,
                derived.len(),
                derived.join(", ")
            ));
        } else if derived.len() > 1 && relation.kind != "composite" {
            problem = Some(format!(
                "gate-perf-design row {}: its cells vary {} dimension(s) from {against} ({}), so \
                 write `{wanted}`",
                row.name,
                derived.len(),
                derived.join(", ")
            ));
        } else if relation.kind == "composite"
            && relation.keys.iter().cloned().collect::<BTreeSet<_>>()
                != derived.iter().cloned().collect::<BTreeSet<_>>()
        {
            problem = Some(format!(
                "gate-perf-design row {}: it is labelled composite({}){}, but its cells vary {}; \
                 name exactly the dimensions the cells vary, or fix the cells",
                row.name,
                relation.keys.join(","),
                relation_display("", family.as_deref()),
                if derived.is_empty() {
                    "nothing".to_string()
                } else {
                    derived.join(", ")
                }
            ));
        }
        if let Some(problem) = problem {
            problems.push(problem);
            continue;
        }
        bump(&relation.kind, &family);
    }

    for (family, name) in sort_families(&families) {
        if !base_state.contains_key(&family) {
            continue;
        }
        let used = rows.iter().any(|row| {
            row.name != *name
                && row
                    .relation
                    .as_ref()
                    .is_some_and(|relation| relation.family == family)
        });
        if used {
            continue;
        }
        let label = match &family {
            None => format!("baseline = {name}"),
            Some(family) => format!("baseline.{family} = {name}"),
        };
        problems.push(format!(
            "gate-budgets: {label} is declared but no row states a relation against it; a baseline \
             no row uses is a stale reference - remove the line, or state the row that belongs to \
             the family"
        ));
    }

    let order = ["orthogonal", "composite", "re-measurement", "baseline"];
    let totals: Vec<String> = order
        .iter()
        .map(|kind| format!("{} {kind}", counts.get(*kind).copied().unwrap_or(0)))
        .collect();
    let totals = totals.join(", ");
    if budgets.named.is_empty() {
        summary.push(format!(
            "  gate-perf-relations: {totals} of {} row(s), stated against {baseline_name}",
            rows.len()
        ));
    } else {
        summary.push(format!(
            "  gate-perf-relations: {totals} of {} row(s) across {} baseline(s)",
            rows.len(),
            families.len()
        ));
        for (family, name) in sort_families(&families) {
            if !base_state.contains_key(&family) {
                continue;
            }
            let tally = family_counts.get(&family).cloned().unwrap_or_default();
            let label = match &family {
                None => format!("default({name})"),
                Some(family) => format!("{family}({name})"),
            };
            summary.push(format!(
                "  gate-perf-family: {label} {} orthogonal, {} composite, {} re-measurement, {} baseline",
                tally.get("orthogonal").copied().unwrap_or(0),
                tally.get("composite").copied().unwrap_or(0),
                tally.get("re-measurement").copied().unwrap_or(0),
                tally.get("baseline").copied().unwrap_or(0)
            ));
        }
    }
    for row in rows {
        if let Some(relation) = &row.relation
            && relation.kind == "composite"
        {
            {
                let reference = match &relation.family {
                    None => budgets.baseline.clone(),
                    Some(family) => budgets.named.get(family).cloned(),
                };
                summary.push(format!(
                    "  gate-perf-composite: {} varies {} against {}",
                    row.name,
                    varied_by_row
                        .get(&row.name)
                        .cloned()
                        .unwrap_or_default()
                        .join(", "),
                    reference.unwrap_or_default()
                ));
            }
        }
    }
    summary
}

/// A cell's property name: the part before its `@<dimension>=<value>`.
pub fn cell_name(cell: &str) -> String {
    cell.split_once('@')
        .map(|(head, _)| head)
        .unwrap_or(cell)
        .to_string()
}

/// Whether a cell name falls in a `members.<family>` namespace.
pub fn prefix_matches(pattern: &str, name: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        name.starts_with(prefix)
    } else {
        name == pattern
    }
}

/// The narrowest prefix namespace covering `names`, or '' if none does.
pub fn prefix_hint(names: &[String]) -> String {
    let Some(first) = names.first() else {
        return String::new();
    };
    let mut prefix = first.clone();
    for name in &names[1..] {
        let mut index = 0;
        let limit = prefix.chars().count().min(name.chars().count());
        let prefix_chars: Vec<char> = prefix.chars().collect();
        let name_chars: Vec<char> = name.chars().collect();
        while index < limit && prefix_chars[index] == name_chars[index] {
            index += 1;
        }
        prefix = prefix_chars[..index].iter().collect();
    }
    if prefix.is_empty() {
        return String::new();
    }
    let unique: BTreeSet<&String> = names.iter().collect();
    if unique.len() == 1 {
        prefix
    } else {
        format!("{prefix}*")
    }
}

/// Verify each row's cells name the family its relation states against.
pub fn check_perf_membership(
    rows: &[PerfRow],
    budgets: &PerfBudgets,
    problems: &mut Vec<String>,
) -> Vec<String> {
    let mut summary: Vec<String> = Vec::new();
    let Some(baseline_name) = budgets.baseline.clone() else {
        return summary;
    };
    let by_name: BTreeMap<&str, &PerfRow> =
        rows.iter().map(|row| (row.name.as_str(), row)).collect();

    let mut families: Vec<(Option<String>, String)> = vec![(None, baseline_name.clone())];
    families.extend(
        budgets
            .named
            .iter()
            .map(|(family, name)| (Some(family.clone()), name.clone())),
    );

    let mut all_names: BTreeSet<String> = BTreeSet::new();
    for row in rows {
        for cell in &row.cells {
            all_names.insert(cell_name(cell));
        }
    }
    let all_names: Vec<String> = all_names.into_iter().collect();

    let mut family_rows: BTreeMap<Option<String>, Vec<&PerfRow>> = BTreeMap::new();
    for row in rows {
        let family = row
            .relation
            .as_ref()
            .and_then(|relation| relation.family.clone());
        family_rows.entry(family).or_default().push(row);
    }

    let names_of = |family: &Option<String>| -> Vec<String> {
        family_rows
            .get(family)
            .map(|rows| {
                rows.iter()
                    .flat_map(|row| row.cells.iter().map(|cell| cell_name(cell)))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    };

    let write_members = |family: &Option<String>| -> String {
        let label = match family {
            None => "members".to_string(),
            Some(family) => format!("members.{family}"),
        };
        let hint = prefix_hint(&names_of(family));
        if hint.is_empty() {
            format!("`{label} = <prefix>`")
        } else {
            format!("`{label} = {hint}`")
        }
    };

    for family in budgets.named.keys() {
        if budgets.members.contains_key(family) {
            continue;
        }
        let names = names_of(&Some(family.clone()));
        let hint = prefix_hint(&names);
        let wanted = if hint.is_empty() {
            format!(
                "a shared cell name (`members.<family> = <prefix>`), because its rows' cells are \
                 named {} and share no prefix",
                names.join(", ")
            )
        } else {
            format!("`members.{family} = {hint}`")
        };
        problems.push(format!(
            "gate-budgets: baseline.{family} declares a family whose cell-name namespace is not \
             declared; write {wanted} so a row's cells decide whether it belongs to the family"
        ));
    }
    for family in budgets.members.keys() {
        if !budgets.named.contains_key(family) {
            problems.push(format!(
                "gate-budgets: members.{family} = {} declares the cell-name namespace of a family \
                 gate-budgets does not declare; declare `baseline.{family} = <row>` or remove the \
                 line",
                budgets.members[family]
            ));
        }
    }
    let mut claimed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (family, pattern) in &budgets.members {
        for name in &all_names {
            if prefix_matches(pattern, name) {
                claimed
                    .entry(name.clone())
                    .or_default()
                    .push(family.clone());
            }
        }
    }
    for (family, pattern) in &budgets.members {
        let matches: Vec<&String> = all_names
            .iter()
            .filter(|name| prefix_matches(pattern, name))
            .collect();
        if matches.is_empty() {
            problems.push(format!(
                "gate-budgets: members.{family} = {pattern} matches no row's cell name, so it \
                 declares a namespace nothing occupies; write the prefix the family's cells carry \
                 ({}) or remove the line",
                if names_of(&Some(family.clone())).is_empty() {
                    "none".to_string()
                } else {
                    names_of(&Some(family.clone())).join(", ")
                }
            ));
            continue;
        }
        if family_rows
            .get(&Some(family.clone()))
            .map(Vec::len)
            .unwrap_or(0)
            == 0
        {
            continue;
        }
        let outside: Vec<String> = names_of(&Some(family.clone()))
            .into_iter()
            .filter(|name| !prefix_matches(pattern, name))
            .collect();
        if !outside.is_empty() {
            problems.push(format!(
                "gate-budgets: members.{family} = {pattern} does not cover the family's own cells \
                 ({}); a family's namespace is where its rows live, so write {}",
                outside.join(", "),
                write_members(&Some(family.clone()))
            ));
        }
    }
    for (name, owners) in &claimed {
        if owners.len() < 2 {
            continue;
        }
        let declared: Vec<String> = owners
            .iter()
            .map(|family| format!("members.{family} = {}", budgets.members[family]))
            .collect();
        problems.push(format!(
            "gate-budgets: the cell name {} is claimed by {} families ({}); a cell name belongs to \
             exactly one family, so narrow one namespace until the cell names are disjoint",
            repr_str(name),
            owners.len(),
            declared.join(", ")
        ));
    }

    for (family, name) in sort_families(&families) {
        let Some(row) = by_name.get(name.as_str()) else {
            continue;
        };
        let pattern = match &family {
            None => None,
            Some(family) => budgets.members.get(family),
        };
        let Some(pattern) = pattern else {
            continue;
        };
        let outside: Vec<String> = row
            .cells
            .iter()
            .map(|cell| cell_name(cell))
            .filter(|name| !prefix_matches(pattern, name))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if !outside.is_empty() {
            problems.push(format!(
                "gate-perf-design row {} is the reference of baseline.{}, but its cells are named \
                 {}, which members.{} = {pattern} does not claim; the family's namespace must \
                 contain its own reference, so write {}",
                row.name,
                family.clone().unwrap_or_default(),
                outside.join(", "),
                family.clone().unwrap_or_default(),
                write_members(&family)
            ));
        }
    }

    let owner_phrase = |owners: &[String]| -> String {
        owners
            .iter()
            .map(|family| {
                format!(
                    "family {} (members.{family} = {})",
                    repr_str(family),
                    budgets.members[family]
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut misfiled = 0;
    for row in rows {
        let family = row
            .relation
            .as_ref()
            .and_then(|relation| relation.family.clone());
        let names: Vec<String> = row
            .cells
            .iter()
            .map(|cell| cell_name(cell))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut owners: Vec<String> = Vec::new();
        for name in &names {
            for owner in claimed.get(name).map(Vec::as_slice).unwrap_or(&[]) {
                if !owners.contains(owner) {
                    owners.push(owner.clone());
                }
            }
        }
        let pattern = match &family {
            None => None,
            Some(family) => budgets.members.get(family),
        };
        if family.is_some() && pattern.is_none() {
            continue;
        }
        let pattern = pattern.cloned().unwrap_or_default();
        if family.is_some() && names.iter().any(|name| !prefix_matches(&pattern, name)) {
            misfiled += 1;
            let outside: Vec<&String> = names
                .iter()
                .filter(|name| !prefix_matches(&pattern, name))
                .collect();
            let hint = if owners.len() == 1 {
                format!(
                    "; those cells belong to {}, so state the row against `@{}` or move the name \
                     out of that namespace",
                    owner_phrase(&owners),
                    owners[0]
                )
            } else if owners.len() > 1 {
                format!(
                    ", and {} families claim them ({}), so one namespace must be narrowed before \
                     the row can be filed",
                    owners.len(),
                    owner_phrase(&owners)
                )
            } else {
                format!(
                    "; declare it as this family's own cell name: {}",
                    write_members(&family)
                )
            };
            problems.push(format!(
                "gate-perf-design row {}: its cells are named {}, which members.{} = {pattern} \
                 does not claim, so the row's cells and the family it names disagree{hint}",
                row.name,
                outside
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                family.clone().unwrap_or_default()
            ));
            continue;
        }
        if owners.len() > 1 {
            misfiled += 1;
            problems.push(format!(
                "gate-perf-design row {}: its cells are named {}, which {} families claim ({}); a \
                 cell name belongs to one family, so the row cannot be filed by its cells until \
                 one namespace is narrowed",
                row.name,
                names.join(", "),
                owners.len(),
                owner_phrase(&owners)
            ));
            continue;
        }
        if !owners.is_empty() && family.as_ref() != Some(&owners[0]) {
            misfiled += 1;
            if family.is_none() {
                problems.push(format!(
                    "gate-perf-design row {} names no family, but its cells are named {}, which \
                     belong to {}; a row whose cells occupy a family's namespace must state \
                     against it - write `@{}`",
                    row.name,
                    names.join(", "),
                    owner_phrase(&owners),
                    owners[0]
                ));
            } else {
                problems.push(format!(
                    "gate-perf-design row {}: its cells are named {}, which belong to {} rather \
                     than family {}; state the row against `@{}` or move the name out of that \
                     namespace",
                    row.name,
                    names.join(", "),
                    owner_phrase(&owners),
                    repr_str(&family.clone().unwrap_or_default()),
                    owners[0]
                ));
            }
        }
    }

    if budgets.members.is_empty() {
        return summary;
    }
    let residual: Vec<String> = all_names
        .iter()
        .filter(|name| !claimed.contains_key(*name))
        .cloned()
        .collect();
    summary.push(format!(
        "  gate-perf-membership: {} cell-name namespace(s) declared, {} cell name(s) claimed, {} \
         row(s) stated outside the namespace of the family they name",
        budgets.members.len(),
        claimed.len(),
        misfiled
    ));
    for family in budgets.members.keys() {
        let rows_in = family_rows
            .get(&Some(family.clone()))
            .map(Vec::len)
            .unwrap_or(0);
        summary.push(format!(
            "  gate-perf-namespace: {family}({}) {rows_in} row(s), cells: {}",
            budgets.members[family],
            names_of(&Some(family.clone())).join(", ")
        ));
    }
    if !residual.is_empty() {
        summary.push(format!(
            "  gate-perf-namespace: default(residual) {} row(s), cells: {}",
            family_rows.get(&None).map(Vec::len).unwrap_or(0),
            residual.join(", ")
        ));
    }
    summary
}

/// The per-test timings of a `mandate-check.json`, or `None` with a problem.
pub fn read_mandate_report(path: &Path, problems: &mut Vec<String>) -> Option<Json> {
    if !path.is_file() {
        problems.push(format!(
            "the mandate-check report {} does not exist; pass --mandate-check-json a report this \
             run produced, or omit it to skip the drift comparison",
            path.display()
        ));
        return None;
    }
    let Ok(text) = std::fs::read_to_string(path) else {
        problems.push(format!(
            "the mandate-check report {} cannot be read",
            path.display()
        ));
        return None;
    };
    let Ok(payload) = crate::tools::json::parse(&text) else {
        problems.push(format!(
            "the mandate-check report {} cannot be read: invalid JSON",
            path.display()
        ));
        return None;
    };
    if payload.as_object().is_none() {
        problems.push(format!(
            "the mandate-check report {} is not a JSON object, so its schema and timings cannot be \
             read",
            path.display()
        ));
        return None;
    }
    let schema = payload.get("schema");
    let is_report = schema
        .and_then(Json::as_str)
        .is_some_and(|schema| schema.starts_with("mandate-check/"));
    if !is_report {
        problems.push(format!(
            "the mandate-check report {} declares schema {}, not a mandate-check report this \
             checker can read",
            path.display(),
            json_repr(schema)
        ));
        return None;
    }
    Some(payload)
}

/// Python's `repr()` of a JSON value, for a diagnostic.
pub fn json_repr(value: Option<&Json>) -> String {
    match value {
        None | Some(Json::Null) => "None".to_string(),
        Some(Json::Bool(true)) => "True".to_string(),
        Some(Json::Bool(false)) => "False".to_string(),
        Some(Json::Int(number)) => number.to_string(),
        Some(Json::Float(number)) => repr_float(*number),
        Some(Json::Str(text)) => repr_str(text),
        Some(Json::Array(items)) => {
            let parts: Vec<String> = items.iter().map(|item| json_repr(Some(item))).collect();
            format!("[{}]", parts.join(", "))
        }
        Some(Json::Object(map)) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(key, item)| format!("{}: {}", repr_str(key), json_repr(Some(item))))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

/// `<target>::<test> -> measured seconds` from a report's per-test timings.
pub fn report_timings(report: &Json) -> BTreeMap<String, f64> {
    let Some(timings) = report.get("timings") else {
        return BTreeMap::new();
    };
    if timings.as_object().is_none() {
        return BTreeMap::new();
    }
    let mut measured: BTreeMap<String, f64> = BTreeMap::new();
    let entries = timings.get("tests").and_then(Json::as_array).unwrap_or(&[]);
    for entry in entries {
        if entry.as_object().is_none() {
            continue;
        }
        let (Some(name), Some(target)) = (
            entry.get("name").and_then(Json::as_str),
            entry.get("target").and_then(Json::as_str),
        ) else {
            continue;
        };
        let duration = entry.get("duration_seconds");
        let number = match duration {
            Some(Json::Bool(_)) | None => continue,
            Some(value) if value.is_number() => value.as_f64().unwrap_or(0.0),
            _ => continue,
        };
        measured.insert(format!("{target}::{name}"), number);
    }
    measured
}

impl Session<'_> {
    /// The tier the compiled test set puts `name` in, or `None` if unknown.
    pub fn listing_tier(
        &mut self,
        name: &str,
        manifest: &BTreeMap<String, String>,
    ) -> R<Option<String>> {
        let (target, _, _) = partition(name, "::");
        let default = self.listed_scenarios(&target, false)?;
        let ignored = self.listed_scenarios(&target, true)?;
        let default: BTreeSet<String> = default.difference(&ignored).cloned().collect();
        if target == LIB_TARGET {
            if default.contains(name) {
                return Ok(Some(
                    manifest
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| "default".to_string()),
                ));
            }
            if ignored.contains(name) {
                return Ok(Some(
                    manifest
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| "perf".to_string()),
                ));
            }
            return Ok(None);
        }
        if default.contains(name) {
            return Ok(Some("default".to_string()));
        }
        if let Some(tier) = manifest.get(name) {
            return Ok(Some(tier.clone()));
        }
        Ok(None)
    }

    /// Check the perf-design/budgets/coverage-gaps blocks of this `GATE.md`.
    pub fn check_perf_gate(
        &mut self,
        manifest: &BTreeMap<String, String>,
        report_path: Option<&Path>,
    ) -> R<(Vec<String>, Vec<String>, Vec<String>)> {
        let mut problems: Vec<String> = Vec::new();
        let mut notes: Vec<String> = Vec::new();
        let design_block = self.layout.manifest_block("gate-perf-design");
        let budgets_block = self.layout.manifest_block("gate-budgets");
        let gaps_block = self.layout.manifest_block("gate-coverage-gaps");
        let perf_tier: Vec<String> = sorted(
            manifest
                .iter()
                .filter(|(_, tier)| tier.as_str() == "perf")
                .map(|(name, _)| name.clone()),
        );
        let Some(design_block) = design_block else {
            if budgets_block.is_some() || gaps_block.is_some() {
                problems.push(
                    "gate-perf-design is missing while gate-budgets/gate-coverage-gaps is present: \
                     the perf declaration must be complete or absent"
                        .to_string(),
                );
            } else if !perf_tier.is_empty() {
                notes.push(format!(
                    "note: {} declares {} perf-tier scenario(s) but no ```gate-perf-design block; \
                     its perf-test time/coverage declaration is PENDING (advisory, not a failure)",
                    self.layout.package,
                    perf_tier.len()
                ));
            }
            return Ok((problems, Vec::new(), notes));
        };
        for (name, block) in [
            ("gate-budgets", budgets_block.as_ref()),
            ("gate-coverage-gaps", gaps_block.as_ref()),
        ] {
            if block.is_none() {
                problems.push(format!(
                    "gate-perf-design is present without a ```{name} block"
                ));
            }
        }
        let rows = parse_perf_design(&design_block, &mut problems);
        let budgets = parse_perf_budgets(budgets_block.as_deref().unwrap_or(""), &mut problems);
        let gaps = parse_perf_gaps(gaps_block.as_deref().unwrap_or(""), &mut problems);
        if !rows.is_empty() && budgets.baseline.is_none() {
            problems.push(
                "gate-budgets declares no 'baseline = <row>' line; every design row's coverage \
                 must be stated relative to a named baseline row"
                    .to_string(),
            );
        }
        if rows.is_empty() {
            if gaps.is_empty() {
                problems.push(
                    "gate-perf-design declares no row and gate-coverage-gaps records no gap: a \
                     zero-row declaration must still say what is not covered; state at least one \
                     `<cell> = <reason>` line, or declare a row"
                        .to_string(),
                );
            }
            if let Some(baseline) = &budgets.baseline {
                problems.push(format!(
                    "gate-budgets declares baseline {} while gate-perf-design declares no row; \
                     with zero rows the declaration is the gap-only form and states against \
                     nothing, so remove the baseline line (or declare the row it names)",
                    repr_str(baseline)
                ));
            }
        }
        let relation_summary = check_perf_relations(&rows, &budgets, &mut problems);
        let membership_summary = check_perf_membership(&rows, &budgets, &mut problems);

        for row in &rows {
            let (target, _, _) = partition(&row.name, "::");
            let actual = self.listing_tier(&row.name, manifest)?;
            let Some(actual) = actual else {
                problems.push(format!(
                    "gate-perf-design row {}: the {} target does not report this test (an unknown \
                     target or an unknown test is a failure)",
                    row.name,
                    repr_str(&target)
                ));
                continue;
            };
            if actual != row.tier {
                problems.push(format!(
                    "gate-perf-design row {} declares tier {} but the test set puts it in {}",
                    row.name,
                    repr_str(&row.tier),
                    repr_str(&actual)
                ));
            }
        }

        let mut by_tier: BTreeMap<String, Vec<&PerfRow>> = BTreeMap::new();
        for row in &rows {
            by_tier.entry(row.tier.clone()).or_default().push(row);
        }
        for (tier, tier_rows) in &by_tier {
            let total: f64 = tier_rows.iter().map(|row| row.cost).sum();
            match budgets.tiers.get(tier) {
                None => problems.push(format!(
                    "gate-perf-design uses the {tier} tier but gate-budgets declares no budget for \
                     it; {} row(s) totalling {}s cannot be paid for",
                    tier_rows.len(),
                    fmt_float(total, ".2f")
                )),
                Some(budget) if total > *budget => {
                    let names: Vec<String> = tier_rows.iter().map(|row| row.name.clone()).collect();
                    problems.push(format!(
                        "gate-perf-design declares {}s in the {tier} tier, over its {}s budget \
                         ({}); retier a test, lower a cost, or raise the budget as a declared \
                         change",
                        fmt_float(total, ".2f"),
                        fmt_float(*budget, ".2f"),
                        names.join(", ")
                    ));
                }
                _ => {}
            }
        }
        if !rows.is_empty()
            && let Some(baseline) = &budgets.baseline
            && !rows.iter().any(|row| &row.name == baseline)
        {
            problems.push(format!(
                "gate-budgets: baseline {} is not a gate-perf-design row, so the rows' coverage \
                 is stated against nothing",
                repr_str(baseline)
            ));
        }

        if let Some(report_path) = report_path {
            let report = read_mandate_report(report_path, &mut problems);
            if let Some(report) = report {
                let stale = match (
                    std::fs::metadata(report_path).and_then(|meta| meta.modified()),
                    std::fs::metadata(&self.layout.manifest).and_then(|meta| meta.modified()),
                ) {
                    (Ok(report_time), Ok(manifest_time)) => report_time < manifest_time,
                    _ => false,
                };
                if stale {
                    notes.push(format!(
                        "note: {} predates {}; its per-test timings are stale and the drift \
                         comparison is skipped",
                        report_path.display(),
                        self.layout.manifest.display()
                    ));
                } else {
                    let measured = report_timings(&report);
                    let mut compared = 0;
                    for row in &rows {
                        let Some(seconds) = measured.get(&row.name).copied() else {
                            continue;
                        };
                        compared += 1;
                        let delta = seconds - row.cost;
                        let relative = if row.cost > 0.0 {
                            delta / row.cost
                        } else {
                            f64::INFINITY
                        };
                        if delta.abs() > budgets.drift_floor_seconds
                            && relative.abs() > budgets.drift
                        {
                            problems.push(format!(
                                "measured/declared drift for {}: declared {}s, measured {}s \
                                 ({}, tolerance {}, floor {}s)",
                                row.name,
                                fmt_float(row.cost, ".2f"),
                                fmt_float(seconds, ".2f"),
                                fmt_float(relative, "+.0%"),
                                fmt_float(budgets.drift, ".0%"),
                                fmt_float(budgets.drift_floor_seconds, ".1f")
                            ));
                        }
                        if let Some(budget) = budgets.tiers.get(&row.tier)
                            && seconds > *budget
                        {
                            {
                                problems.push(format!(
                                    "{} measured {}s, over its {} tier budget {}s",
                                    row.name,
                                    fmt_float(seconds, ".2f"),
                                    row.tier,
                                    fmt_float(*budget, ".2f")
                                ));
                            }
                        }
                    }
                    if measured.is_empty() {
                        notes.push(format!(
                            "note: {} carries no per-test timings (schema {}); the declared costs \
                             are not drift-checked",
                            report_path.display(),
                            json_repr(report.get("schema"))
                        ));
                    } else {
                        notes.push(format!(
                            "note: drift compared {compared} of {} declared row(s) against {} \
                             (tolerance {}, floor {}s)",
                            rows.len(),
                            report_path.display(),
                            fmt_float(budgets.drift, ".0%"),
                            fmt_float(budgets.drift_floor_seconds, ".1f")
                        ));
                    }
                }
            }
        }

        let cells: usize = rows.iter().map(|row| row.cells.len()).sum();
        let named = if budgets.named.is_empty() {
            String::new()
        } else {
            format!(
                " (+{} named: {})",
                budgets.named.len(),
                budgets.named.keys().cloned().collect::<Vec<_>>().join(", ")
            )
        };
        let baseline = budgets
            .baseline
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "unset".to_string());
        let mut summary = vec![format!(
            "  gate-perf-design: {} perf test row(s), {cells} coverage cell(s), {} gap(s), \
             baseline {baseline}{named}",
            rows.len(),
            gaps.len()
        )];
        summary.extend(relation_summary);
        summary.extend(membership_summary);
        for (tier, tier_rows) in &by_tier {
            let total: f64 = tier_rows.iter().map(|row| row.cost).sum();
            match budgets.tiers.get(tier) {
                Some(budget) => summary.push(format!(
                    "  gate-budgets: {tier} {}/{}s",
                    fmt_float(total, ".2f"),
                    fmt_float(*budget, ".2f")
                )),
                None => summary.push(format!(
                    "  gate-budgets: {tier} {}/no budget",
                    fmt_float(total, ".2f")
                )),
            }
        }
        Ok((problems, summary, notes))
    }
}

/// Python's `float(text)`, for the decimal literals the grammar accepts.
pub fn py_float(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let (sign, rest) = match lower.strip_prefix('-') {
        Some(rest) => (-1.0, rest),
        None => (1.0, lower.strip_prefix('+').unwrap_or(&lower)),
    };
    match rest {
        "inf" | "infinity" => return Some(sign * f64::INFINITY),
        "nan" => return Some(f64::NAN),
        _ => {}
    }
    // Python accepts underscores between digits; fold them out only where they
    // sit between two digits.
    let chars: Vec<char> = rest.chars().collect();
    let mut cleaned = String::new();
    for (index, character) in chars.iter().enumerate() {
        if *character == '_' {
            let before = index > 0 && chars[index - 1].is_ascii_digit();
            let after = index + 1 < chars.len() && chars[index + 1].is_ascii_digit();
            if before && after {
                continue;
            }
            return None;
        }
        cleaned.push(*character);
    }
    cleaned.parse::<f64>().ok().map(|value| sign * value)
}

/// A `gate-perf-design` row, re-exported so callers need only this module.
pub type Row = PerfRow;
