//! The producer contract's lines: verdicts, per-arm measurements, and the M1
//! instrument readings.
//!
//! Every guard here is a vacuity guard. A line that claims to be a verdict but
//! does not match the grammar is a failure rather than a line to skip; an arm
//! line that can never be attributed, a section with no arm line, an arm with
//! no declared coverage cell, and a run with no arm measurement at all are all
//! problems, so an arm set that quietly empties is a failure rather than a
//! report with an empty `arms` list.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::OnceLock;

use crate::tools::json::{self, Json};
use crate::tools::pyjson::{py_list, repr_str};
use crate::tools::pyre::Regex;

use super::value::{Ordered, coerce_measure, coerce_value, py_splitlines};
use super::{
    ARM_COUNTER_KEYS, ARM_STAT_KEYS, ARM_WINDOW_KEYS, ARMS_DECLARATION_NAME,
    ARMS_DECLARATION_SCHEMA, VALUE_RE_SOURCE,
};

/// Python's `repr()` of a possibly-absent name, which the refusals print.
pub fn repr(value: Option<&str>) -> String {
    match value {
        Some(text) => repr_str(text),
        None => "None".to_string(),
    }
}

fn mandate_line_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^MANDATE (?P<mandate>[A-Za-z][A-Za-z0-9_]*) (?P<verdict>PASS|FAIL)(?:[ \t]+(?P<values>.*?))?[ \t]*$",
            false,
        )
        .expect("compiles")
    })
}

fn value_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(VALUE_RE_SOURCE, false).expect("compiles"))
}

fn arm_line_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^\[mandate-smoke (?P<label>[^\]]*)\](?:[ \t]+(?P<body>.*?))?[ \t]*$",
            false,
        )
        .expect("compiles")
    })
}

fn arm_token_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=[ \t]*(?P<value>[^\s=]+)",
            false,
        )
        .expect("compiles")
    })
}

fn arm_bulk_rep_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^delivered (?P<delivered_mib_s>[0-9.]+) MiB/s over (?P<elapsed_seconds>[0-9.]+)s, shaper forwarded (?P<shaper_mib_s>[0-9.]+) MiB/s, capacity (?P<capacity_mib_s>[0-9.]+) MiB/s, fraction (?P<fraction>[0-9.]+) \((?P<delivered_bytes>[0-9]+) / (?P<forwarded_bytes>[0-9]+) bytes\)$",
            false,
        )
        .expect("compiles")
    })
}

fn censoring_row_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^\[(?P<instrument>[A-Za-z0-9_.-]+)\] arm=(?P<arm>\S+)[ \t]+(?P<body>.*?)[ \t]*$",
            false,
        )
        .expect("compiles")
    })
}

/// One `MANDATE` line's parsed record.
#[derive(Debug, Clone, PartialEq)]
pub struct MandateRecord {
    pub verdict: String,
    pub values: Ordered,
    pub raw_line: String,
}

/// Parse the `MANDATE` lines into `({id: record}, [problems])`.
///
/// Every line whose first token is `MANDATE` must match the contract grammar
/// exactly; anything else that starts with `MANDATE` is reported as a problem
/// rather than skipped. `ids` are the verdict sections the producing crate
/// declares, so a line naming a section the producer does not declare is a
/// named failure rather than an accepted verdict.
pub fn parse_mandate_lines(
    text: &str,
    ids: &[String],
) -> (BTreeMap<String, MandateRecord>, Vec<String>) {
    let mut records = BTreeMap::new();
    let mut problems = Vec::new();
    for (index, raw_line) in py_splitlines(text).iter().enumerate() {
        let line_number = index + 1;
        let line = raw_line.trim_end().to_string();
        if line != "MANDATE" && !line.starts_with("MANDATE ") {
            continue;
        }
        let Some(found) = mandate_line_re().search(&line) else {
            problems.push(format!(
                "line {line_number}: {} starts with MANDATE but does not match \
                 the contract grammar 'MANDATE <ID> <PASS|FAIL> <key>=<value> ...'",
                repr_str(&line)
            ));
            continue;
        };
        let mandate = found.named("mandate").unwrap_or_default();
        if !ids.contains(&mandate) {
            let because = if ids.is_empty() {
                "names a section, but the producer that printed it declares no verdict section"
                    .to_string()
            } else {
                format!("is not one of {}", ids.join(", "))
            };
            problems.push(format!(
                "line {line_number}: mandate {} {because}",
                repr_str(&mandate)
            ));
            continue;
        }
        if records.contains_key(&mandate) {
            problems.push(format!(
                "line {line_number}: mandate {mandate} is declared twice; a reader \
                 cannot tell which verdict is the run's"
            ));
            continue;
        }
        let values_text = found.named("values").unwrap_or_default();
        let (values, value_problems) = parse_values(&mandate, &values_text, line_number);
        problems.extend(value_problems);
        records.insert(
            mandate,
            MandateRecord {
                verdict: found.named("verdict").unwrap_or_default(),
                values,
                raw_line: line,
            },
        );
    }
    (records, problems)
}

fn parse_values(mandate: &str, values_text: &str, line_number: usize) -> (Ordered, Vec<String>) {
    let mut values = Ordered::default();
    let mut problems = Vec::new();
    for token in values_text.split_whitespace() {
        let Some(found) = value_re().search(token) else {
            problems.push(format!(
                "line {line_number}: {mandate} measurement {} is not a \
                 <key>=<value> token",
                repr_str(token)
            ));
            continue;
        };
        let key = found.named("key").unwrap_or_default();
        if values.contains_key(&key) {
            problems.push(format!(
                "line {line_number}: {mandate} measurement {} is repeated",
                repr_str(&key)
            ));
            continue;
        }
        values.insert(key, coerce_value(&found.named("value").unwrap_or_default()));
    }
    if values.is_empty() {
        problems.push(format!(
            "line {line_number}: {mandate} reports a verdict without a single \
             key=value measurement; a verdict that measures nothing cannot be checked"
        ));
    }
    (values, problems)
}

/// One `[mandate-smoke <arm>]` line, as the arm record the report carries.
///
/// `note` marks a line that matches neither documented shape: prose the
/// command does not depend on, kept visible in the report rather than dropped.
#[derive(Debug, Clone, PartialEq)]
pub struct Arm {
    pub id: Option<String>,
    pub mandate: Option<String>,
    pub label: String,
    pub dialect: Option<String>,
    pub sample_count: Option<i64>,
    pub stats: BTreeMap<String, Json>,
    pub counters: BTreeMap<String, Json>,
    pub windows: BTreeMap<String, Json>,
    pub cells: Vec<String>,
    pub values: Ordered,
    pub raw_line: String,
    pub repeated_keys: Vec<String>,
    pub note: bool,
    pub body: String,
    pub producer: Option<String>,
}

impl Arm {
    fn note_entry(label: String, body: String) -> Arm {
        Arm {
            id: None,
            mandate: None,
            label,
            dialect: None,
            sample_count: None,
            stats: BTreeMap::new(),
            counters: BTreeMap::new(),
            windows: BTreeMap::new(),
            cells: Vec::new(),
            values: Ordered::default(),
            raw_line: String::new(),
            repeated_keys: Vec::new(),
            note: true,
            body,
            producer: None,
        }
    }

    /// The record as the report writes it.
    pub fn to_json(&self) -> Json {
        let mut map = BTreeMap::new();
        map.insert("id".to_string(), json::opt_str(self.id.as_deref()));
        map.insert(
            "mandate".to_string(),
            json::opt_str(self.mandate.as_deref()),
        );
        map.insert("label".to_string(), Json::Str(self.label.clone()));
        map.insert(
            "dialect".to_string(),
            json::opt_str(self.dialect.as_deref()),
        );
        map.insert(
            "sample_count".to_string(),
            self.sample_count.map_or(Json::Null, Json::Int),
        );
        map.insert("stats".to_string(), json::object(&self.stats));
        map.insert("counters".to_string(), json::object(&self.counters));
        map.insert("windows".to_string(), json::object(&self.windows));
        map.insert(
            "cells".to_string(),
            Json::Array(self.cells.iter().cloned().map(Json::Str).collect()),
        );
        map.insert("values".to_string(), self.values.to_json());
        map.insert("raw_line".to_string(), Json::Str(self.raw_line.clone()));
        if !self.repeated_keys.is_empty() {
            map.insert(
                "repeated_keys".to_string(),
                Json::Array(self.repeated_keys.iter().cloned().map(Json::Str).collect()),
            );
        }
        if let Some(producer) = &self.producer {
            map.insert("producer".to_string(), Json::Str(producer.clone()));
        }
        Json::Object(map)
    }

    /// The prose note's record: the fields a reader is shown.
    pub fn note_json(&self) -> Json {
        let mut map = BTreeMap::new();
        map.insert(
            "mandate".to_string(),
            json::opt_str(self.mandate.as_deref()),
        );
        map.insert("label".to_string(), Json::Str(self.label.clone()));
        map.insert("body".to_string(), Json::Str(self.body.clone()));
        Json::Object(map)
    }
}

/// One `[mandate-smoke <arm>]` line as an arm record, or `None` when the line
/// is not an arm line at all.
pub fn parse_arm_line(line: &str) -> Option<Arm> {
    let found = arm_line_re().search(line)?;
    let label = found.named("label").unwrap_or_default().trim().to_string();
    let body = found.named("body").unwrap_or_default().trim().to_string();
    if label.is_empty() {
        return Some(Arm::note_entry(label, body));
    }
    let mut repeated = Vec::new();
    let (values, dialect) = match arm_bulk_rep_re().search(&body) {
        Some(bulk) if bulk.whole().chars().count() == body.chars().count() => {
            let mut values = Ordered::default();
            for key in [
                "delivered_mib_s",
                "elapsed_seconds",
                "shaper_mib_s",
                "capacity_mib_s",
                "fraction",
                "delivered_bytes",
                "forwarded_bytes",
            ] {
                if let Some(text) = bulk.named(key) {
                    values.insert(key.to_string(), coerce_value(&text));
                }
            }
            (values, Some("bulk-rep".to_string()))
        }
        _ => {
            let mut values = Ordered::default();
            for token in arm_token_re().find_iter(&body) {
                let key = token.named("key").unwrap_or_default();
                let raw = token.named("value").unwrap_or_default();
                if values.contains_key(&key) {
                    repeated.push(key);
                    continue;
                }
                values.insert(key, coerce_measure(&raw).0);
            }
            if values.is_empty() {
                return Some(Arm::note_entry(label, body));
            }
            (values, Some("kv".to_string()))
        }
    };
    let mut record = arm_record(&label, &body, dialect, values);
    if !repeated.is_empty() {
        repeated.sort();
        repeated.dedup();
        record.repeated_keys = repeated;
    }
    Some(record)
}

/// The normalised arm record for a parsed arm line.
fn arm_record(label: &str, body: &str, dialect: Option<String>, values: Ordered) -> Arm {
    let mut stats = BTreeMap::new();
    let mut counters = BTreeMap::new();
    let mut windows = BTreeMap::new();
    for (key, value) in values.iter() {
        if ARM_STAT_KEYS.contains(&key.as_str()) {
            stats.insert(key.clone(), value.clone());
        }
        if let Some(named) = ARM_COUNTER_KEYS
            .iter()
            .find(|(source, _)| source == key)
            .map(|(_, named)| *named)
        {
            counters.insert(named.to_string(), value.clone());
        }
        if let Some(named) = ARM_WINDOW_KEYS
            .iter()
            .find(|(source, _)| source == key)
            .map(|(_, named)| *named)
        {
            windows.insert(named.to_string(), value.clone());
        }
    }
    let sample_count = counters.get("received").and_then(|value| match value {
        Json::Bool(_) => None,
        Json::Int(number) => Some(*number),
        Json::Float(number) => Some(*number as i64),
        _ => None,
    });
    Arm {
        id: None,
        mandate: None,
        label: label.to_string(),
        dialect,
        sample_count,
        stats,
        counters,
        windows,
        cells: Vec::new(),
        values,
        raw_line: if body.is_empty() {
            format!("[mandate-smoke {label}]")
        } else {
            format!("[mandate-smoke {label}] {body}")
        },
        repeated_keys: Vec::new(),
        note: false,
        body: body.to_string(),
        producer: None,
    }
}

/// The run's arms, attributed to the section each one belongs to.
///
/// Every `[mandate-smoke ...]` line is a candidate. An arm line whose body
/// carries `section=<id>` is attributed by that token, and its id is
/// `<id>/<label>`; any other arm line belongs to the section named by the
/// `MANDATE` line that next arrives. Returns `(arms, notes)` with `arms`
/// sorted by id.
pub fn parse_arm_lines(
    events: &[(f64, String)],
    problems: &mut Vec<String>,
) -> (Vec<Arm>, Vec<Arm>) {
    let mut arms: Vec<Arm> = Vec::new();
    let mut notes: Vec<Arm> = Vec::new();
    let mut pending: Vec<Arm> = Vec::new();

    for (_seconds, raw) in events {
        let line = raw.trim_end().to_string();
        if line != "MANDATE" && line.starts_with("MANDATE ") {
            if let Some(mandate) = mandate_line_re()
                .search(&line)
                .and_then(|found| found.named("mandate"))
            {
                flush(&mut pending, &mandate, &mut arms, &mut notes);
            }
            continue;
        }
        let Some(parsed) = parse_arm_line(&line) else {
            continue;
        };
        let section: Option<String> = if parsed.note {
            None
        } else {
            parsed
                .values
                .get("section")
                .and_then(Json::as_str)
                .map(str::to_string)
        };
        match section {
            Some(section) => {
                let mut entry = parsed;
                entry.mandate = Some(section.clone());
                entry.id = Some(format!("{section}/{}", entry.label));
                arms.push(entry);
            }
            None => pending.push(parsed),
        }
    }
    for entry in pending.iter() {
        if entry.note {
            notes.push(entry.clone());
            continue;
        }
        problems.push(format!(
            "the arm line {} follows the last 'MANDATE' line and carries no \
             'section=' token, so it cannot be attributed to a mandate or a section; \
             print the arm's line before its section's MANDATE line, or name the \
             section in the line itself",
            repr_str(&entry.raw_line)
        ));
    }
    arms.sort_by(|left, right| left.id.cmp(&right.id));
    (arms, notes)
}

fn flush(pending: &mut Vec<Arm>, section: &str, arms: &mut Vec<Arm>, notes: &mut Vec<Arm>) {
    for entry in pending.iter() {
        if entry.note {
            let mut note = entry.clone();
            note.mandate = Some(section.to_string());
            notes.push(note);
            continue;
        }
        let mut entry = entry.clone();
        entry.mandate = Some(section.to_string());
        entry.id = Some(format!("{section}/{}", entry.label));
        arms.push(entry);
    }
    pending.clear();
}

/// One producer's per-arm readings, keyed by arm, from its instrument rows.
pub fn parse_censoring_rows(
    events: &[(f64, String)],
    instrument: &str,
) -> (BTreeMap<String, BTreeMap<String, Json>>, Vec<String>) {
    let mut rows: BTreeMap<String, BTreeMap<String, Json>> = BTreeMap::new();
    let mut problems = Vec::new();
    for (_seconds, line) in events {
        let Some(found) = censoring_row_re().search(line) else {
            continue;
        };
        if found.named("instrument").as_deref() != Some(instrument) {
            continue;
        }
        let arm = found.named("arm").unwrap_or_default();
        let mut values = BTreeMap::new();
        for token in arm_token_re().find_iter(&found.named("body").unwrap_or_default()) {
            values.insert(
                token.named("key").unwrap_or_default(),
                coerce_measure(&token.named("value").unwrap_or_default()).0,
            );
        }
        if values.is_empty() {
            problems.push(format!(
                "[{instrument}] arm={arm} carries no <key>=<value> measurement, so \
                 it is a reading that reads nothing"
            ));
            continue;
        }
        if rows.contains_key(&arm) {
            problems.push(format!(
                "[{instrument}] arm={arm} is printed twice; two readings of one arm \
                 are two measurements under one name"
            ));
            continue;
        }
        rows.insert(arm, values);
    }
    (rows, problems)
}

/// The declared cells for an arm id: the longest matching prefix.
///
/// The match ends at a word boundary: the character after the prefix must be
/// neither alphanumeric nor `_`/`-`, so `M4/m4/clean` covers `M4/m4/clean flow
/// A` while `M1/clean` does not cover `M1/cleanup`.
pub fn declared_cells(arm_id: &str, cells: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let chars: Vec<char> = arm_id.chars().collect();
    let mut best: Option<&String> = None;
    for key in cells.keys() {
        if !arm_id.starts_with(key.as_str()) {
            continue;
        }
        let key_len = key.chars().count();
        if chars.len() > key_len {
            let next = chars[key_len];
            if next.is_alphanumeric() || next == '_' || next == '-' {
                continue;
            }
        }
        if best.is_none_or(|current| key_len > current.chars().count()) {
            best = Some(key);
        }
    }
    match best {
        Some(key) => {
            let mut out = cells[key].clone();
            out.sort();
            out
        }
        None => Vec::new(),
    }
}

/// The arm coverage declaration, or a named failure.
/// The `(mandate, instrument)` a declaration names for its censoring reading:
/// the panel that cannot distinguish a peak-and-return from a truncated climb
/// needs a machine verdict, and only the crate that owns the mandate knows which
/// instrument prints it.
pub fn declared_censoring(payload: &Json) -> Option<(String, String)> {
    let object = payload.get("censoring").and_then(Json::as_object)?;
    let mandate = object.get("mandate").and_then(Json::as_str)?.to_string();
    let instrument = object.get("instrument").and_then(Json::as_str)?.to_string();
    Some((mandate, instrument))
}

/// The mandate ids the owning crate declares, in declaration order. Empty only
/// for a declaration [`load_arm_declaration`] has already refused.
pub fn declared_mandates(payload: &Json) -> Vec<String> {
    payload
        .get("mandates")
        .and_then(Json::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub fn load_arm_declaration(path: &Path, problems: &mut Vec<String>) -> Option<Json> {
    if !path.is_file() {
        problems.push(format!(
            "the arm coverage declaration {} does not exist, so no arm's coverage \
             cells can be recorded; it travels with this command",
            path.display()
        ));
        return None;
    }
    let payload = match json::parse_document(path) {
        Ok(value) => value,
        Err(error) => {
            problems.push(format!(
                "the arm coverage declaration {} cannot be read: {error}",
                path.display()
            ));
            return None;
        }
    };
    if payload.as_object().is_none() {
        problems.push(format!(
            "the arm coverage declaration {} is not a JSON object",
            path.display()
        ));
        return None;
    }
    let schema = payload.get("schema").and_then(Json::as_str);
    if schema != Some(ARMS_DECLARATION_SCHEMA) {
        problems.push(format!(
            "the arm coverage declaration {} declares schema {}, not {}",
            path.display(),
            repr(schema),
            repr_str(ARMS_DECLARATION_SCHEMA)
        ));
        return None;
    }
    let Some(cells) = payload.get("cells").and_then(Json::as_object) else {
        problems.push(format!(
            "the arm coverage declaration {} declares no cells, so no arm can claim \
             a covered cell",
            path.display()
        ));
        return None;
    };
    if cells.is_empty() {
        problems.push(format!(
            "the arm coverage declaration {} declares no cells, so no arm can claim \
             a covered cell",
            path.display()
        ));
        return None;
    }
    // The mandate id set travels with the crate that owns the mandate, never as
    // a constant in this tool: an unknown id is refused here rather than
    // silently unattributed later.
    let mandates_well_formed = payload
        .get("mandates")
        .and_then(Json::as_array)
        .is_some_and(|items| {
            !items.is_empty()
                && items
                    .iter()
                    .all(|token| token.as_str().is_some_and(|text| !text.is_empty()))
        });
    if !mandates_well_formed {
        problems.push(format!(
            "the arm coverage declaration {} declares no `mandates` list, so this tool \
             would have to guess the mandate id set it belongs to the crate that owns \
             the mandate to state",
            path.display()
        ));
        return None;
    }
    let censoring_well_formed = payload
        .get("censoring")
        .and_then(Json::as_object)
        .is_some_and(|object| {
            ["mandate", "instrument"].iter().all(|key| {
                object
                    .get(*key)
                    .and_then(Json::as_str)
                    .is_some_and(|text| !text.is_empty())
            })
        });
    if !censoring_well_formed {
        problems.push(format!(
            "the arm coverage declaration {} declares no `censoring` {{mandate, instrument}}, \
             so this tool would have to guess which instrument prints the reading the panel \
             that cannot show a failure needs",
            path.display()
        ));
        return None;
    }
    for (key, value) in cells {
        if key.is_empty() {
            problems.push(format!(
                "the arm coverage declaration {} has a non-string key",
                path.display()
            ));
            return None;
        }
        let listed = value.as_array();
        let well_formed = listed.is_some_and(|items| {
            !items.is_empty()
                && items
                    .iter()
                    .all(|cell| cell.as_str().is_some_and(|text| !text.is_empty()))
        });
        if !well_formed {
            problems.push(format!(
                "the arm coverage declaration {} gives {} no cell; a cell may be \
                 knowingly empty but never silently empty",
                path.display(),
                repr_str(key)
            ));
            return None;
        }
    }
    Some(payload)
}

/// The declared cell map out of a loaded declaration.
pub fn declared_cell_map(declaration: &Json) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    if let Some(cells) = declaration.get("cells").and_then(Json::as_object) {
        for (key, value) in cells {
            let listed = value
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|cell| cell.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            out.insert(key.clone(), listed);
        }
    }
    out
}

/// Attach each arm's declared coverage cells, failing on an undeclared arm.
pub fn stamp_arm_coverage(
    arms: &mut [Arm],
    declaration: Option<&Json>,
    problems: &mut Vec<String>,
) {
    let Some(declaration) = declaration else {
        return;
    };
    let cells = declared_cell_map(declaration);
    for arm in arms.iter_mut() {
        let id = arm.id.clone().unwrap_or_default();
        arm.cells = declared_cells(&id, &cells);
        if arm.cells.is_empty() {
            problems.push(format!(
                "the arm {} covers no declared cell; add it to the arm coverage \
                 declaration ({ARMS_DECLARATION_NAME}), because a cell may be \
                 knowingly empty but never silently empty",
                repr_str(&id)
            ));
        }
    }
}

/// Require every section's arms to have been measured, or say which not.
pub fn check_arm_coverage(
    arms: &[Arm],
    _notes: &[Arm],
    problems: &mut Vec<String>,
    sections: &[String],
) {
    let measured: Vec<&String> = arms.iter().filter_map(|arm| arm.mandate.as_ref()).collect();
    for section in sections {
        if !measured.contains(&section) {
            problems.push(format!(
                "{section}: no '[mandate-smoke <arm>]' measurement line was \
                 attributed to it, so its arms were never measured"
            ));
        }
    }
    if arms.is_empty() {
        problems.push(
            "the run printed no '[mandate-smoke <arm>] ...' arm measurement at all, \
             so no arm's coverage was recorded; an instrument that returns nothing \
             has deleted the coverage it exists to provide"
                .to_string(),
        );
    }
}

/// Every arm's section must be one the producing crate declares.
pub fn check_arm_sections(
    arms: &[Arm],
    sections: &[String],
    producer: &str,
    problems: &mut Vec<String>,
) {
    for arm in arms {
        let mandate = arm.mandate.clone().unwrap_or_default();
        if !sections.contains(&mandate) {
            problems.push(format!(
                "the arm {} is attributed to the section {}, which the \
                 {producer} producer does not declare (its sections are {}); declare \
                 the section in {} or attribute the arm to one of the declared ones",
                repr(arm.id.as_deref()),
                repr(Some(&mandate)),
                sections.join(", "),
                super::PRODUCERS_DECLARATION_NAME
            ));
        }
    }
}

/// One line per producer and section: its arms and their sample count.
pub fn arm_summary(arms: &[Arm], notes: &[Arm], producers: &Json) -> Vec<String> {
    let mut lines = Vec::new();
    let mut produced: Vec<String> = arms.iter().filter_map(|arm| arm.producer.clone()).collect();
    produced.sort();
    produced.dedup();
    for producer in &produced {
        let record = producers.get(producer).cloned().unwrap_or(Json::Null);
        let prefix = if produced.len() > 1 {
            format!("{producer} ")
        } else {
            String::new()
        };
        let sections: Vec<String> = record
            .get("sections")
            .and_then(Json::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        for section in &sections {
            let of_section: Vec<&Arm> = arms
                .iter()
                .filter(|arm| {
                    arm.producer.as_deref() == Some(producer.as_str())
                        && arm.mandate.as_deref() == Some(section.as_str())
                })
                .collect();
            if of_section.is_empty() {
                lines.push(format!("  {prefix}{section} arms: none measured"));
                continue;
            }
            let known: Vec<i64> = of_section
                .iter()
                .filter_map(|arm| arm.sample_count)
                .collect();
            let samples = if known.is_empty() {
                "no per-sample count".to_string()
            } else {
                format!("{} sample(s)", known.iter().sum::<i64>())
            };
            let labels: Vec<String> = of_section.iter().map(|arm| arm.label.clone()).collect();
            lines.push(format!(
                "  {prefix}{section} arms: {} ({}), {samples}",
                of_section.len(),
                labels.join(", ")
            ));
        }
    }
    if !notes.is_empty() {
        lines.push(format!(
            "  arm notes (prose, not compared): {}",
            notes.len()
        ));
    }
    lines
}

/// A Python `repr()` of a sorted list of names, which the `M1` censoring
/// mismatch prints.
pub fn py_sorted(items: &[String]) -> String {
    let mut sorted = items.to_vec();
    sorted.sort();
    py_list(&sorted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> Vec<String> {
        ["M1", "M2", "M3", "M4"]
            .iter()
            .map(|id| id.to_string())
            .collect()
    }

    fn events(lines: &[&str]) -> Vec<(f64, String)> {
        lines
            .iter()
            .enumerate()
            .map(|(index, line)| (index as f64, line.to_string()))
            .collect()
    }

    #[test]
    fn the_documented_shape_parses_into_typed_values() {
        let (records, problems) = parse_mandate_lines(
            "MANDATE M1 PASS p99=31.5 ceiling=250.0 over250=0 name=Clear",
            &ids(),
        );
        assert!(problems.is_empty(), "{problems:?}");
        let record = records.get("M1").expect("M1 is parsed");
        assert_eq!(record.verdict, "PASS");
        assert_eq!(record.values.get("p99"), Some(&Json::Float(31.5)));
        assert_eq!(record.values.get("ceiling"), Some(&Json::Float(250.0)));
        assert_eq!(record.values.get("over250"), Some(&Json::Int(0)));
        assert_eq!(
            record.values.get("name"),
            Some(&Json::Str("Clear".to_string()))
        );
    }

    #[test]
    fn a_repeated_key_and_an_unparsable_token_are_refused() {
        let (records, problems) = parse_mandate_lines("MANDATE M1 PASS p99=1 p99=2 oops", &ids());
        assert_eq!(records.len(), 1);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems[0].contains("measurement 'p99' is repeated"),
            "{problems:?}"
        );
        assert!(
            problems[1].contains("measurement 'oops' is not a <key>=<value> token"),
            "{problems:?}"
        );
    }

    #[test]
    fn a_mandate_outside_the_producers_verdicts_is_named() {
        let (records, problems) = parse_mandate_lines("MANDATE M9 PASS p99=1", &ids());
        assert!(records.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(
            problems[0].contains("is not one of M1, M2, M3, M4"),
            "{problems:?}"
        );
    }

    #[test]
    fn a_line_that_claims_to_be_a_verdict_but_is_not_is_a_problem() {
        let (records, problems) = parse_mandate_lines("MANDATE M1 MAYBE p99=1", &ids());
        assert!(records.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("does not match the contract grammar"));
    }

    #[test]
    fn a_verdict_without_a_measurement_is_refused() {
        let (records, problems) = parse_mandate_lines("MANDATE M1 PASS", &ids());
        assert_eq!(records.len(), 1);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("without a single key=value measurement"));
    }

    #[test]
    fn an_arm_line_is_normalised_into_stats_counters_and_windows() {
        let arm = parse_arm_line(
            "[mandate-smoke clean] sent=  800 recv=  800 wire=   12345B wall=12.3s p99=  89.0",
        )
        .expect("an arm line");
        assert_eq!(arm.dialect.as_deref(), Some("kv"));
        assert_eq!(arm.sample_count, Some(800));
        assert_eq!(arm.counters.get("wire_bytes"), Some(&Json::Int(12345)));
        assert_eq!(arm.windows.get("wall_seconds"), Some(&Json::Float(12.3)));
        assert_eq!(arm.stats.get("p99"), Some(&Json::Float(89.0)));
    }

    #[test]
    fn a_repeated_key_on_an_arm_line_is_recorded_rather_than_overwritten() {
        let arm = parse_arm_line("[mandate-smoke clean] recv=1 recv=2").expect("an arm line");
        assert_eq!(arm.repeated_keys, vec!["recv".to_string()]);
        assert_eq!(arm.values.get("recv"), Some(&Json::Int(1)));
    }

    #[test]
    fn the_bulk_rep_line_is_its_own_dialect() {
        let arm = parse_arm_line(
            "[mandate-smoke m3/rep1] delivered 0.963 MiB/s over 2.0004s, shaper forwarded \
             0.972 MiB/s, capacity 1.000 MiB/s, fraction 0.963 (820148 / 992240 bytes)",
        )
        .expect("a bulk rep line");
        assert_eq!(arm.dialect.as_deref(), Some("bulk-rep"));
        assert_eq!(arm.stats.get("fraction"), Some(&Json::Float(0.963)));
        assert_eq!(
            arm.counters.get("delivered_bytes"),
            Some(&Json::Int(820148))
        );
        assert_eq!(
            arm.windows.get("elapsed_seconds"),
            Some(&Json::Float(2.0004))
        );
        assert_eq!(
            arm.counters.get("forwarded_bytes"),
            Some(&Json::Int(992240))
        );
    }

    #[test]
    fn a_prose_arm_line_is_a_note_rather_than_an_arm() {
        let note = parse_arm_line("[mandate-smoke clean] three flows in flight").expect("a line");
        assert!(note.note);
        assert!(note.values.is_empty());
        assert!(parse_arm_line("plain test output").is_none());
    }

    #[test]
    fn arms_are_attributed_by_the_mandate_line_that_follows() {
        let mut problems = Vec::new();
        let (arms, notes) = parse_arm_lines(
            &events(&[
                "[mandate-smoke clean] recv=800",
                "MANDATE M1 PASS p99=1",
                "[mandate-smoke probe] section=probe recv=21",
            ]),
            &mut problems,
        );
        assert!(notes.is_empty());
        assert_eq!(arms.len(), 2);
        assert_eq!(arms[0].id.as_deref(), Some("M1/clean"));
        assert_eq!(arms[1].id.as_deref(), Some("probe/probe"));
        assert!(problems.is_empty());
    }

    #[test]
    fn an_arm_line_after_the_last_mandate_line_cannot_be_attributed() {
        let mut problems = Vec::new();
        let (arms, _notes) = parse_arm_lines(
            &events(&["MANDATE M1 PASS p99=1", "[mandate-smoke clean] recv=800"]),
            &mut problems,
        );
        assert!(arms.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("carries no 'section=' token"));
    }

    #[test]
    fn the_longest_declared_prefix_wins_and_a_word_boundary_ends_it() {
        let mut cells = BTreeMap::new();
        cells.insert("M1/clean".to_string(), vec!["arm".to_string()]);
        cells.insert("M1".to_string(), vec!["mandate".to_string()]);
        cells.insert("M1/cleanup".to_string(), vec!["other".to_string()]);
        assert_eq!(declared_cells("M1/clean", &cells), vec!["arm".to_string()]);
        assert_eq!(
            declared_cells("M1/hostile", &cells),
            vec!["mandate".to_string()]
        );
        assert_eq!(
            declared_cells("M1/cleanup", &cells),
            vec!["other".to_string()]
        );
        assert_eq!(
            declared_cells("M1/clean/flow-A", &cells),
            vec!["arm".to_string()]
        );
    }

    /// A path unique within the process: a clock-derived name can be shared by
    /// two tests, and a test reading another's declaration passes for a reason
    /// that is not its own.
    fn declaration_file(label: &str, text: &str) -> std::path::PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mandate-arms-{label}-{}-{unique}.json",
            std::process::id()
        ));
        std::fs::write(&path, text).expect("write");
        path
    }

    #[test]
    fn a_missing_arm_declaration_is_refused() {
        let path = std::env::temp_dir().join("mandate-arms-absent.json");
        let _ = std::fs::remove_file(&path);
        let mut problems = Vec::new();
        assert!(load_arm_declaration(&path, &mut problems).is_none());
        assert!(problems[0].contains("does not exist"), "{problems:?}");
        assert!(
            problems[0].contains("arm coverage declaration"),
            "{problems:?}"
        );
    }

    #[test]
    fn a_malformed_arm_declaration_is_refused_and_a_good_one_is_not() {
        let malformed = [
            ("not an object", "[]"),
            (
                "wrong schema",
                r#"{"schema": "mandate-arms/2", "mandates": ["M1"], "cells": {"M1/clean": ["M1@x=1"]}}"#,
            ),
            (
                "no mandates",
                r#"{"schema": "mandate-arms/1", "cells": {"M1/clean": ["M1@x=1"]}}"#,
            ),
            (
                "no cells",
                r#"{"schema": "mandate-arms/1", "mandates": ["M1"], "cells": {}}"#,
            ),
            (
                "empty cell list",
                r#"{"schema": "mandate-arms/1", "mandates": ["M1"], "cells": {"M1/clean": []}}"#,
            ),
            (
                "non-string cell",
                r#"{"schema": "mandate-arms/1", "mandates": ["M1"], "cells": {"M1/clean": [7]}}"#,
            ),
        ];
        for (label, text) in malformed {
            let path = declaration_file(label, text);
            let mut problems = Vec::new();
            assert!(
                load_arm_declaration(&path, &mut problems).is_none(),
                "{label} must be refused"
            );
            assert!(!problems.is_empty(), "{label} must say why");
            let _ = std::fs::remove_file(path);
        }
        let good = declaration_file(
            "good",
            r#"{"schema": "mandate-arms/1", "mandates": ["M1"], "censoring": {"mandate": "M1", "instrument": "m1-censoring"}, "cells": {"M1/clean": ["M1@x=1"]}}"#,
        );
        let mut problems = Vec::new();
        let loaded = load_arm_declaration(&good, &mut problems);
        assert!(loaded.is_some(), "{problems:?}");
        assert!(problems.is_empty(), "{problems:?}");
        let loaded = loaded.expect("loaded");
        let cells = declared_cell_map(&loaded);
        assert_eq!(cells["M1/clean"], vec!["M1@x=1".to_string()]);
        let _ = std::fs::remove_file(good);
    }

    #[test]
    fn a_duplicated_censoring_reading_for_one_arm_is_refused() {
        let (_rows, problems) = parse_censoring_rows(
            &events(&[
                "[m1-censoring] arm=impaired verdict=Clear room=2000.0",
                "[m1-censoring] arm=impaired verdict=Rung room=1.0",
            ]),
            "m1-censoring",
        );
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("is printed twice"));
    }

    #[test]
    fn a_censoring_row_that_reads_nothing_is_refused() {
        let (rows, problems) = parse_censoring_rows(
            &events(&["[m1-censoring] arm=impaired quiet"]),
            "m1-censoring",
        );
        assert!(rows.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("carries no <key>=<value> measurement"));
    }

    #[test]
    fn an_arm_with_no_declared_cell_is_refused_and_a_declared_one_is_not() {
        let mut cells = BTreeMap::new();
        cells.insert("M1/clean".to_string(), vec!["M1@x=1".to_string()]);
        let mut declaration = BTreeMap::new();
        declaration.insert(
            "cells".to_string(),
            json::object_map(
                cells
                    .iter()
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            Json::Array(value.iter().cloned().map(Json::Str).collect()),
                        )
                    })
                    .collect(),
            ),
        );
        let declaration = Json::Object(declaration);
        let mut arms = vec![
            parse_arm_line("[mandate-smoke clean] recv=1").expect("a line"),
            parse_arm_line("[mandate-smoke hostile] recv=1").expect("a line"),
        ];
        arms[0].id = Some("M1/clean".to_string());
        arms[1].id = Some("M1/hostile".to_string());
        let mut problems = Vec::new();
        stamp_arm_coverage(&mut arms, Some(&declaration), &mut problems);
        assert_eq!(arms[0].cells, vec!["M1@x=1".to_string()]);
        assert!(arms[1].cells.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("covers no declared cell"));
    }
}
