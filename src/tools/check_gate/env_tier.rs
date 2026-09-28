//! The env-scaled opt-in surface: a tier that is not `#[ignore]`d at all but
//! scaled by environment variables, which every other block is blind to.
//!
//! Detection is two-sided, which is what makes it an artifact rather than a
//! guess: a name counts when a script in the crate *names* it and the crate's
//! Rust sources read it from the process environment. The declaration is then
//! enforced in both directions.

use std::collections::{BTreeMap, BTreeSet};

use super::scan::{crate_scripts, script_env_names};
use super::{
    Session, env_name_re, env_tier_load_bound_re, env_tier_load_count_re, env_tier_load_wall_re,
    partition, toolchain_env_re,
};
use crate::tools::pyjson::repr_str;
use crate::tools::pyre::Regex;

/// `<runner>` states how a surface is run: a script path relative to the crate
/// root, or this marker when nothing but the invocation runs it.
pub const ENV_TIER_NO_RUNNER: &str = "-";

/// A surface's `<load>` field: the shape its cost was measured under.
#[derive(Debug, Clone)]
pub struct EnvTierLoad {
    pub factors: Vec<(String, i64)>,
    pub expression: String,
    pub total: i64,
    pub wall: f64,
    pub bound: String,
}

impl EnvTierLoad {
    pub fn describe(&self) -> String {
        let bound = if self.bound.is_empty() {
            String::new()
        } else {
            format!(", bound {}", self.bound)
        };
        format!(
            "{} = {}, {}s{bound}",
            self.expression,
            self.total,
            super::fmt_float(self.wall, "g")
        )
    }
}

/// One env-scaled opt-in surface: its variables, its runner and its cells.
#[derive(Debug, Clone)]
pub struct EnvTier {
    pub name: String,
    pub variables: Vec<String>,
    pub runner: String,
    pub measures: String,
    pub cells: Vec<String>,
    pub load: Option<EnvTierLoad>,
}

// -- the load expression -----------------------------------------------------

/// The count a load's `total` expression yields, and the names it reads.
pub fn load_expression(
    expression: &str,
    sizes: &BTreeMap<String, i64>,
) -> Option<(i64, BTreeSet<String>)> {
    let chars: Vec<char> = expression.chars().collect();
    let mut parser = LoadParser {
        chars: &chars,
        pos: 0,
        sizes,
        names: BTreeSet::new(),
    };
    let value = parser.expression()?;
    if parser.pos != parser.chars.len() {
        return None;
    }
    Some((value, parser.names))
}

struct LoadParser<'a> {
    chars: &'a [char],
    pos: usize,
    sizes: &'a BTreeMap<String, i64>,
    names: BTreeSet<String>,
}

impl LoadParser<'_> {
    fn skip_space(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.skip_space();
        self.chars.get(self.pos).copied()
    }

    fn expression(&mut self) -> Option<i64> {
        let mut value = self.term()?;
        while self.peek() == Some('+') {
            self.pos += 1;
            value = value.checked_add(self.term()?)?;
        }
        Some(value)
    }

    fn term(&mut self) -> Option<i64> {
        let mut value = self.factor()?;
        while self.peek() == Some('*') {
            self.pos += 1;
            value = value.checked_mul(self.factor()?)?;
        }
        Some(value)
    }

    fn factor(&mut self) -> Option<i64> {
        match self.peek()? {
            '(' => {
                self.pos += 1;
                let value = self.expression()?;
                if self.peek() != Some(')') {
                    return None;
                }
                self.pos += 1;
                Some(value)
            }
            character if character.is_ascii_digit() => {
                let mut digits = String::new();
                let mut previous_digit = false;
                while self.pos < self.chars.len() {
                    let character = self.chars[self.pos];
                    if character.is_ascii_digit() {
                        digits.push(character);
                        previous_digit = true;
                        self.pos += 1;
                    } else if character == '_' && previous_digit {
                        previous_digit = false;
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                digits.parse::<i64>().ok()
            }
            character if character.is_ascii_alphabetic() || character == '_' => {
                let mut name = String::new();
                while self.pos < self.chars.len() {
                    let character = self.chars[self.pos];
                    if character.is_ascii_alphanumeric() || character == '_' {
                        name.push(character);
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                self.names.insert(name.clone());
                self.sizes.get(&name).copied()
            }
            _ => None,
        }
    }
}

/// Parse a surface's `<load>` field, or name what is wrong with it.
pub fn parse_env_tier_load(
    text: &str,
    name: &str,
    variables: &[String],
    problems: &mut Vec<String>,
) -> Option<EnvTierLoad> {
    const RESERVED: [&str; 3] = ["total", "wall", "bound"];
    let mut problem = false;
    let mut counts: Vec<(String, String)> = Vec::new();
    for item in text
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (key, separator, value) = partition(item, "=");
        let key = key.trim().to_string();
        let value = value.trim().to_string();
        if separator.is_empty() || key.is_empty() || value.is_empty() {
            problems.push(format!(
                "gate-env-tier surface {name}: load {} is not '<key>=<value>'",
                repr_str(item)
            ));
            problem = true;
            continue;
        }
        if counts.iter().any(|(existing, _)| existing == &key) {
            problems.push(format!(
                "gate-env-tier surface {name}: load {key} named more than once"
            ));
            problem = true;
            continue;
        }
        counts.push((key, value));
    }
    let mut factors: Vec<(String, i64)> = Vec::new();
    for (key, value) in &counts {
        if RESERVED.contains(&key.as_str()) {
            continue;
        }
        if !variables.iter().any(|variable| variable == key) {
            problems.push(format!(
                "gate-env-tier surface {name}: load key {} is neither {} nor a variable of the \
                 surface ({}); a load shape is sized by the surface's own variables",
                repr_str(key),
                RESERVED.join(", "),
                variables.join(", ")
            ));
            problem = true;
            continue;
        }
        if !env_tier_load_count_re().is_match(value) || value.parse::<i64>().unwrap_or(0) < 1 {
            problems.push(format!(
                "gate-env-tier surface {name}: load {key}={value} is not a positive count of that \
                 variable"
            ));
            problem = true;
            continue;
        }
        factors.push((key.clone(), value.parse::<i64>().unwrap_or(0)));
    }
    if factors.is_empty() {
        problems.push(format!(
            "gate-env-tier surface {name}: its load names no variable of the surface; a shape is \
             the surface's own knobs at the values the measurement sized them to"
        ));
        problem = true;
    }
    if problem {
        return None;
    }
    let total_text = counts
        .iter()
        .find(|(key, _)| key == "total")
        .map(|(_, value)| value.clone());
    let Some(total_text) = total_text else {
        problems.push(format!(
            "gate-env-tier surface {name}: its load states no total; the count the shape yields is \
             what a cost is stated against"
        ));
        return None;
    };
    let sizes: BTreeMap<String, i64> = factors.iter().cloned().collect();
    let Some((total, named)) = load_expression(&total_text, &sizes) else {
        problems.push(format!(
            "gate-env-tier surface {name}: load total={total_text} is not arithmetic over the \
             named variables and integer literals ('5*SOAK_DIALERS*SOAK_ITERATIONS+SOAK_DIALERS*10')"
        ));
        return None;
    };
    if named.is_empty() {
        problems.push(format!(
            "gate-env-tier surface {name}: load total={total_text} yields a count from no variable \
             of the shape; the count a shape yields is derived from its own sizes"
        ));
        return None;
    }
    if total < 1 {
        problems.push(format!(
            "gate-env-tier surface {name}: load total={total_text} is not a positive count"
        ));
        return None;
    }
    let wall_text = counts
        .iter()
        .find(|(key, _)| key == "wall")
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| "None".to_string());
    let wall_match = env_tier_load_wall_re().match_at(&wall_text);
    let wall = wall_match
        .as_ref()
        .and_then(|found| found.group(1))
        .and_then(|value| value.parse::<f64>().ok());
    let Some(wall) = wall.filter(|wall| *wall > 0.0) else {
        problems.push(format!(
            "gate-env-tier surface {name}: load wall={wall_text} is not a positive duration in \
             seconds ('1.97s'); a load shape with no measured wall clock is not a cost"
        ));
        return None;
    };
    let bound = counts
        .iter()
        .find(|(key, _)| key == "bound")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    if !bound.is_empty() {
        let bound_match = env_tier_load_bound_re().match_at(&bound);
        let value = bound_match
            .as_ref()
            .and_then(|found| found.group(1))
            .and_then(|value| value.parse::<f64>().ok());
        if value.is_none_or(|value| value <= 0.0) {
            problems.push(format!(
                "gate-env-tier surface {name}: load bound={bound} is not a positive rate \
                 ('1.8e-4/dial'); a bound is stated per what it bounds"
            ));
            return None;
        }
    }
    Some(EnvTierLoad {
        factors,
        expression: total_text,
        total,
        wall,
        bound,
    })
}

/// Parse `gate-env-tier` rows.
pub fn parse_env_tier(text: &str, problems: &mut Vec<String>) -> Vec<EnvTier> {
    let mut surfaces: Vec<EnvTier> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (number, line) in super::perf::perf_lines(text) {
        let (name, separator, rest) = partition(&line, " = ");
        let name = name.trim().to_string();
        let rest = rest.trim().to_string();
        if separator.is_empty() {
            problems.push(format!(
                "gate-env-tier line {number}: {} is not '<name> = <vars> | <runner> | <measures> \
                 | <cells>'",
                repr_str(&line)
            ));
            continue;
        }
        if !super::cell_key_re().is_match(&name) {
            problems.push(format!(
                "gate-env-tier line {number}: {} does not name a surface; write a name \
                 ([A-Za-z][A-Za-z0-9_-]*)",
                repr_str(&name)
            ));
            continue;
        }
        if seen.contains(&name) {
            problems.push(format!(
                "gate-env-tier: duplicate surface {}",
                repr_str(&name)
            ));
            continue;
        }
        let fields: Vec<String> = rest
            .split('|')
            .map(|field| field.trim().to_string())
            .collect();
        if fields.len() != 4 && fields.len() != 5 {
            problems.push(format!(
                "gate-env-tier surface {name}: expected '<vars> | <runner> | <measures> | <cells> \
                 [| <load>]', got {} field(s)",
                fields.len()
            ));
            continue;
        }
        let variables_text = fields[0].clone();
        let runner = fields[1].clone();
        let measures = fields[2].clone();
        let cells_text = fields[3].clone();
        let load_text = fields.get(4).cloned();
        let mut problem = false;
        let variables: Vec<String> = variables_text
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect();
        if variables.is_empty() {
            problems.push(format!(
                "gate-env-tier surface {name}: no variable named; a surface is scaled by at least \
                 one environment variable"
            ));
            problem = true;
        }
        for variable in &variables {
            if !env_name_re().is_match(variable) {
                problems.push(format!(
                    "gate-env-tier surface {name}: {} is not an environment variable name \
                     ([A-Z][A-Z0-9_]*)",
                    repr_str(variable)
                ));
                problem = true;
            }
        }
        let duplicates: Vec<String> = variables
            .iter()
            .filter(|variable| variables.iter().filter(|other| other == variable).count() > 1)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if !duplicates.is_empty() {
            problems.push(format!(
                "gate-env-tier surface {name}: {} named more than once",
                duplicates.join(", ")
            ));
            problem = true;
        }
        if runner.is_empty() {
            problems.push(format!(
                "gate-env-tier surface {name}: no runner field; state the script that runs the \
                 surface, or {} for a surface no script runs, because how a surface is run is part \
                 of the record",
                repr_str(ENV_TIER_NO_RUNNER)
            ));
            problem = true;
        }
        if measures.is_empty() {
            problems.push(format!(
                "gate-env-tier surface {name}: says nothing about what it measures; a surface with \
                 no measured quantity is not a declaration"
            ));
            problem = true;
        }
        let cells: Vec<String> = cells_text
            .split(',')
            .map(str::trim)
            .filter(|cell| !cell.is_empty())
            .map(str::to_string)
            .collect();
        if cells.is_empty() {
            problems.push(format!(
                "gate-env-tier surface {name}: covers no cell; state what the surface measures in \
                 the coverage vocabulary"
            ));
            problem = true;
        }
        for cell in &cells {
            if let Some(issue) = super::perf::cell_problem(cell) {
                problems.push(format!(
                    "gate-env-tier surface {name}: cell {} is malformed: {issue}",
                    repr_str(cell)
                ));
                problem = true;
            }
        }
        let mut load = None;
        if let Some(load_text) = &load_text {
            load = parse_env_tier_load(load_text, &name, &variables, problems);
            if load.is_none() {
                problem = true;
            }
        }
        if problem {
            continue;
        }
        seen.insert(name.clone());
        surfaces.push(EnvTier {
            name,
            variables,
            runner,
            measures,
            cells,
            load,
        });
    }
    surfaces
}

// -- the Rust sources that read the environment ------------------------------

/// `ENV_NAME -> {source}` for the names this crate's sources read from the env.
pub fn rust_env_read_names(root: &std::path::Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut sources: Vec<(std::path::PathBuf, Vec<char>, Vec<char>)> = Vec::new();
    let mut forwarders: Vec<Forwarder> = Vec::new();
    for path in super::scan::rust_sources(root) {
        let Some(raw) = super::scan::read_scanned(&path) else {
            continue;
        };
        let code_text = strip_rust_comments(&raw, false);
        let quiet_text = strip_rust_comments(&raw, true);
        let code: Vec<char> = code_text.chars().collect();
        let quiet: Vec<char> = quiet_text.chars().collect();
        for found in fn_re().find_iter(&quiet_text) {
            let Some(name) = found.group(1) else { continue };
            let Some(open) = quiet_text[found.end..].find('{') else {
                continue;
            };
            let start = found.end + open;
            let end = brace_end_chars(&quiet, start);
            let params = parameter_names(&call_arguments(&quiet, found.end - 1));
            forwarders.push(Forwarder {
                name,
                params,
                code_body: code[start..end].to_vec(),
                quiet_body: quiet[start..end].to_vec(),
                path: path.clone(),
            });
        }
        for found in closure_re().find_iter(&quiet_text) {
            let Some(name) = found.group(1) else { continue };
            let Some(close) = quiet_text[found.end..].find('|') else {
                continue;
            };
            let close = found.end + close;
            if let Some((code_body, quiet_body)) = closure_body(&code, &quiet, close + 1) {
                let params = parameter_names(&quiet[found.end..close]);
                forwarders.push(Forwarder {
                    name,
                    params,
                    code_body,
                    quiet_body,
                    path: path.clone(),
                });
            }
        }
        sources.push((path, code, quiet));
    }
    let mut readers: BTreeSet<String> = forwarders
        .iter()
        .filter(|forwarder| {
            env_read_re().is_match(&forwarder.quiet_body.iter().collect::<String>())
        })
        .map(|forwarder| forwarder.name.clone())
        .collect();
    let mut changed = true;
    while changed {
        changed = false;
        for forwarder in &forwarders {
            if readers.contains(&forwarder.name) {
                continue;
            }
            let body: String = forwarder.quiet_body.iter().collect();
            let calls_reader = call_re()
                .find_iter(&body)
                .iter()
                .filter_map(|found| found.group(1))
                .any(|call| readers.contains(&call));
            if calls_reader {
                readers.insert(forwarder.name.clone());
                changed = true;
            }
        }
    }
    let keys = forwarder_key_positions(&forwarders, &readers);
    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (path, code, quiet) in &sources {
        let code_text: String = code.iter().collect();
        let quiet_text: String = quiet.iter().collect();
        for found_literal in env_literal_re().find_iter(&code_text) {
            if !env_read_re().match_from(&quiet_text, found_literal.start) {
                continue;
            }
            if let Some(literal) = found_literal.group(1) {
                remember_env_read(&mut found, &literal, path, root);
            }
        }
        let mut aliases: BTreeMap<String, String> = BTreeMap::new();
        for found_alias in const_str_re().find_iter(&code_text) {
            let (Some(alias), Some(literal)) = (found_alias.group(1), found_alias.group(2)) else {
                continue;
            };
            let Some(literal_start) = found_alias.group_span(2).map(|(start, _)| start) else {
                continue;
            };
            let end = literal_start.saturating_sub(1);
            if code[found_alias.start..end] == quiet[found_alias.start..end] {
                aliases.insert(alias, literal);
            }
        }
        for found_ident in env_ident_re().find_iter(&quiet_text) {
            if let Some(literal) = found_ident
                .group(1)
                .and_then(|identifier| aliases.get(&identifier).cloned())
            {
                remember_env_read(&mut found, &literal, path, root);
            }
        }
    }
    for forwarder in &forwarders {
        let quiet_body: String = forwarder.quiet_body.iter().collect();
        for found_call in call_re().find_iter(&quiet_body) {
            let Some(callee) = found_call.group(1) else {
                continue;
            };
            if !readers.contains(&callee) {
                continue;
            }
            let positions = keys.positions(&callee, &forwarder.path);
            if positions.is_empty() {
                continue;
            }
            let spans = argument_spans(&forwarder.quiet_body, found_call.end - 1);
            for index in positions {
                if index >= spans.len() {
                    continue;
                }
                let (start, end) = spans[index];
                let slice: String = forwarder.code_body[start..end].iter().collect();
                for literal in string_literal_re().find_iter(&slice) {
                    if let Some(literal) = literal.group(1) {
                        remember_env_read(&mut found, &literal, &forwarder.path, root);
                    }
                }
            }
        }
    }
    found
}

struct Forwarder {
    name: String,
    params: Vec<String>,
    code_body: Vec<char>,
    quiet_body: Vec<char>,
    path: std::path::PathBuf,
}

fn fn_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"\b(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:const\s+)?fn\s+([A-Za-z0-9_]+)\s*(?:<[^>]*>)?\s*\(",
            false,
        )
        .expect("compiles")
    })
}

fn closure_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"\blet\s+(?:mut\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*(?::[^=;]*)?=\s*(?:move\s*)?\|",
            false,
        )
        .expect("compiles")
    })
}

fn const_str_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            "\\b(?:pub(?:\\s*\\([^)]*\\))?\\s+)?(?:const|static)\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*:\\s*&(?:'static\\s+)?str\\s*=\\s*\"([^\"\\n]*)\"",
            false,
        )
        .expect("compiles")
    })
}

fn env_read_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"env::var(?:_os)?\s*\(", false).expect("compiles"))
}

fn env_literal_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r#"env::var(?:_os)?\s*\(\s*"([A-Za-z_][A-Za-z0-9_]*)""#,
            false,
        )
        .expect("compiles")
    })
}

fn env_ident_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(r"env::var(?:_os)?\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)", false).expect("compiles")
    })
}

fn call_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)\s*\(",
            false,
        )
        .expect("compiles")
    })
}

fn rust_ident_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"[A-Za-z_][A-Za-z0-9_]*", false).expect("compiles"))
}

fn string_literal_re() -> &'static Regex {
    static ONCE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r#""([^"\n]*)""#, false).expect("compiles"))
}

/// The key positions of every crate-local forwarder.
struct KeyPositions {
    by_file: BTreeMap<(std::path::PathBuf, String), BTreeSet<usize>>,
    by_name: BTreeMap<String, BTreeSet<usize>>,
}

impl KeyPositions {
    fn positions(&self, callee: &str, path: &std::path::Path) -> BTreeSet<usize> {
        if let Some(local) = self.by_file.get(&(path.to_path_buf(), callee.to_string())) {
            return local.clone();
        }
        self.by_name.get(callee).cloned().unwrap_or_default()
    }
}

fn forwarder_key_positions(forwarders: &[Forwarder], readers: &BTreeSet<String>) -> KeyPositions {
    let mut by_file: BTreeMap<(std::path::PathBuf, String), BTreeSet<usize>> = BTreeMap::new();
    for forwarder in forwarders {
        let slot = by_file
            .entry((forwarder.path.clone(), forwarder.name.clone()))
            .or_default();
        let quiet_body: String = forwarder.quiet_body.iter().collect();
        for read in env_read_re().find_iter(&quiet_body) {
            let arguments = call_arguments(&forwarder.quiet_body, read.end - 1);
            if let Some(position) = parameter_position(&arguments, &forwarder.params) {
                slot.insert(position);
            }
        }
    }
    let mut by_name: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
    let mut changed = true;
    while changed {
        changed = false;
        by_name = BTreeMap::new();
        for ((_, name), positions) in &by_file {
            by_name
                .entry(name.clone())
                .or_default()
                .extend(positions.iter().copied());
        }
        for forwarder in forwarders {
            let quiet_body: String = forwarder.quiet_body.iter().collect();
            let slot = by_file
                .entry((forwarder.path.clone(), forwarder.name.clone()))
                .or_default()
                .clone();
            let mut slot = slot;
            for found in call_re().find_iter(&quiet_body) {
                let Some(callee) = found.group(1) else {
                    continue;
                };
                if !readers.contains(&callee) {
                    continue;
                }
                let callee_positions = by_file
                    .get(&(forwarder.path.clone(), callee.clone()))
                    .cloned()
                    .or_else(|| by_name.get(&callee).cloned())
                    .unwrap_or_default();
                if callee_positions.is_empty() {
                    continue;
                }
                let spans = argument_spans(&forwarder.quiet_body, found.end - 1);
                for index in callee_positions {
                    if index >= spans.len() {
                        continue;
                    }
                    let (start, end) = spans[index];
                    let argument: Vec<char> = forwarder.quiet_body[start..end].to_vec();
                    if let Some(position) = parameter_position(&argument, &forwarder.params)
                        && slot.insert(position)
                    {
                        changed = true;
                    }
                }
            }
            by_file.insert((forwarder.path.clone(), forwarder.name.clone()), slot);
        }
    }
    KeyPositions { by_file, by_name }
}

/// A forwarder's parameter names, positionally.
fn parameter_names(text: &[char]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for item in split_top_level(text) {
        let item: String = item.iter().collect();
        let mut head = item.split(':').next().unwrap_or("").trim().to_string();
        if let Some(rest) = head.strip_prefix("mut ") {
            head = rest.trim().to_string();
        }
        if head == "_" || rust_ident_re().full_match(&head).is_none() {
            names.push(String::new());
        } else {
            names.push(head);
        }
    }
    names
}

/// The index of the parameter `argument` names, or `None`.
fn parameter_position(argument: &[char], params: &[String]) -> Option<usize> {
    let mut name: String = argument.iter().collect();
    name = name.trim().to_string();
    while name.starts_with('&') {
        name = name[1..].trim_start().to_string();
    }
    for (index, param) in params.iter().enumerate() {
        if !param.is_empty() && param == &name {
            return Some(index);
        }
    }
    None
}

/// The `(start, end)` of each top-level argument of the call at `open_index`.
fn argument_spans(text: &[char], open_index: usize) -> Vec<(usize, usize)> {
    let mut depth = 0i32;
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut start = open_index + 1;
    let mut index = open_index;
    while index < text.len() {
        let character = text[index];
        if matches!(character, '(' | '[' | '{') {
            depth += 1;
        } else if matches!(character, ')' | ']' | '}') {
            depth -= 1;
            if depth == 0 {
                if index > start {
                    spans.push((start, index));
                }
                break;
            }
        } else if character == ',' && depth == 1 {
            spans.push((start, index));
            start = index + 1;
        }
        index += 1;
    }
    spans
}

/// The comma-separated pieces of `text` at bracket depth 0.
fn split_top_level(text: &[char]) -> Vec<Vec<char>> {
    let mut pieces: Vec<Vec<char>> = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (index, character) in text.iter().enumerate() {
        match character {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                pieces.push(text[start..index].to_vec());
                start = index + 1;
            }
            _ => {}
        }
    }
    pieces.push(text[start..].to_vec());
    pieces
        .into_iter()
        .filter(|piece| piece.iter().any(|character| !character.is_whitespace()))
        .collect()
}

/// Both views of the body whose closure header ends at `header_end`.
fn closure_body(
    code: &[char],
    quiet: &[char],
    header_end: usize,
) -> Option<(Vec<char>, Vec<char>)> {
    let brace = find_char(quiet, '{', header_end);
    let semi = find_char(quiet, ';', header_end);
    if let Some(brace) = brace
        && semi.is_none_or(|semi| brace < semi)
    {
        let end = brace_end_chars(quiet, brace);
        return Some((code[brace..end].to_vec(), quiet[brace..end].to_vec()));
    }
    let semi = semi?;
    Some((
        code[header_end..semi].to_vec(),
        quiet[header_end..semi].to_vec(),
    ))
}

fn find_char(chars: &[char], needle: char, from: usize) -> Option<usize> {
    chars[from..]
        .iter()
        .position(|character| *character == needle)
        .map(|offset| from + offset)
}

/// The text inside the parentheses whose `(` is at `open_index`.
fn call_arguments(chars: &[char], open_index: usize) -> Vec<char> {
    let mut depth = 0i32;
    let mut index = open_index;
    while index < chars.len() {
        let character = chars[index];
        if character == '(' {
            depth += 1;
        } else if character == ')' {
            depth -= 1;
            if depth == 0 {
                return chars[open_index + 1..index].to_vec();
            }
        }
        index += 1;
    }
    Vec::new()
}

/// The index of the `}` closing the `{` at `open_index`.
fn brace_end_chars(chars: &[char], open_index: usize) -> usize {
    let mut depth = 0i32;
    let mut index = open_index;
    while index < chars.len() {
        match chars[index] {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return index;
                }
            }
            _ => {}
        }
        index += 1;
    }
    chars.len().saturating_sub(1)
}

/// Record `literal` as a name `path` reads, if it is one to record.
fn remember_env_read(
    found: &mut BTreeMap<String, BTreeSet<String>>,
    literal: &str,
    path: &std::path::Path,
    root: &std::path::Path,
) {
    if !env_name_re().is_match(literal) || toolchain_env_re().is_match(literal) {
        return;
    }
    let relative = path
        .strip_prefix(root)
        .map(|relative| relative.display().to_string())
        .unwrap_or_else(|_| path.display().to_string());
    found
        .entry(literal.to_string())
        .or_default()
        .insert(relative);
}

/// The source text with every comment blanked to spaces, length preserved.
pub fn strip_rust_comments(text: &str, blank_literals: bool) -> String {
    let mut out: Vec<char> = text.chars().collect();
    let chars: Vec<char> = out.clone();
    let size = chars.len();
    let mut index = 0usize;
    while index < size {
        if starts_with(&chars, index, "//") {
            let end = find_char(&chars, '\n', index).unwrap_or(size);
            blank(&chars, &mut out, index, end);
            index = end;
            continue;
        }
        if starts_with(&chars, index, "/*") {
            let end = block_comment_end(&chars, index);
            blank(&chars, &mut out, index, end);
            index = end;
            continue;
        }
        if chars[index] == '"' {
            let end = string_end(&chars, index);
            if blank_literals {
                blank(&chars, &mut out, index, end);
            }
            index = end;
            continue;
        }
        if (chars[index] == 'r' || chars[index] == 'b')
            && let Some(raw_end) = raw_string_end(&chars, index)
        {
            if blank_literals {
                blank(&chars, &mut out, index, raw_end);
            }
            index = raw_end;
            continue;
        }
        if chars[index] == '\''
            && let Some(char_end) = char_literal_end(&chars, index)
        {
            if blank_literals {
                blank(&chars, &mut out, index, char_end);
            }
            index = char_end;
            continue;
        }
        index += 1;
    }
    out.into_iter().collect()
}

fn starts_with(chars: &[char], index: usize, needle: &str) -> bool {
    let needle: Vec<char> = needle.chars().collect();
    chars.len() >= index + needle.len() && chars[index..index + needle.len()] == needle[..]
}

/// Blank `chars[start:end]` in `out`, keeping its newlines in place.
fn blank(chars: &[char], out: &mut [char], start: usize, end: usize) {
    for position in start..end {
        if chars[position] != '\n' {
            out[position] = ' ';
        }
    }
}

/// The index just past the block comment whose `/*` is at `start`.
fn block_comment_end(chars: &[char], start: usize) -> usize {
    let mut depth = 0i32;
    let mut index = start;
    let size = chars.len();
    while index < size {
        if starts_with(chars, index, "/*") {
            depth += 1;
            index += 2;
            continue;
        }
        if starts_with(chars, index, "*/") {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return index;
            }
            continue;
        }
        index += 1;
    }
    size
}

/// The index just past the ordinary string literal whose `"` is at `start`.
fn string_end(chars: &[char], start: usize) -> usize {
    let mut index = start + 1;
    let size = chars.len();
    while index < size {
        if chars[index] == '\\' {
            index += 2;
            continue;
        }
        if chars[index] == '"' {
            return index + 1;
        }
        index += 1;
    }
    size
}

/// The index just past the raw string literal starting at `start`.
fn raw_string_end(chars: &[char], start: usize) -> Option<usize> {
    let mut index = start;
    if starts_with(chars, index, "br") || starts_with(chars, index, "rb") {
        index += 2;
    } else if chars.get(index) == Some(&'r') {
        index += 1;
    } else {
        return None;
    }
    let mut hashes = 0usize;
    while index < chars.len() && chars[index] == '#' {
        hashes += 1;
        index += 1;
    }
    if index >= chars.len() || chars[index] != '"' {
        return None;
    }
    let closing: Vec<char> = std::iter::once('"')
        .chain(std::iter::repeat_n('#', hashes))
        .collect();
    let mut position = index + 1;
    while position + closing.len() <= chars.len() {
        if chars[position..position + closing.len()] == closing[..] {
            return Some(position + closing.len());
        }
        position += 1;
    }
    Some(chars.len())
}

/// The index just past the character literal whose `'` is at `start`.
fn char_literal_end(chars: &[char], start: usize) -> Option<usize> {
    if chars.get(start + 1) == Some(&'\'') {
        return Some(start + 2);
    }
    if chars.get(start + 1) == Some(&'\\') {
        let mut index = start + 2;
        while index < chars.len() && chars[index] != '\'' {
            index += 1;
        }
        return Some(index + 1);
    }
    if start + 2 < chars.len() && chars[start + 2] == '\'' {
        return Some(start + 3);
    }
    None
}

impl Session<'_> {
    /// Check the ```gate-env-tier block, and detect a surface no block declares.
    pub fn check_env_tier(&mut self, problems: &mut Vec<String>) -> (Vec<String>, Vec<String>) {
        let mut notes: Vec<String> = Vec::new();
        let root = self.layout.root.clone();
        let rust_sources = rust_env_read_names(&root);
        let mut detected: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut script_names: BTreeMap<std::path::PathBuf, BTreeSet<String>> = BTreeMap::new();
        for script in crate_scripts(&root) {
            let names = script_env_names(&script);
            for name in &names {
                if rust_sources.contains_key(name) {
                    let relative = script
                        .strip_prefix(&root)
                        .map(|relative| relative.display().to_string())
                        .unwrap_or_else(|_| script.display().to_string());
                    detected.entry(name.clone()).or_default().insert(relative);
                }
            }
            script_names.insert(script, names);
        }
        let scriptless: Vec<String> = rust_sources
            .keys()
            .filter(|name| !detected.contains_key(*name))
            .cloned()
            .collect();
        let Some(block) = self.layout.manifest_block("gate-env-tier") else {
            let mut undeclared: BTreeSet<String> = detected.keys().cloned().collect();
            undeclared.extend(rust_sources.keys().cloned());
            if !undeclared.is_empty() {
                let scripts: Vec<String> = detected
                    .values()
                    .flat_map(|names| names.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let readers: Vec<String> = undeclared
                    .iter()
                    .flat_map(|name| rust_sources.get(name).cloned().unwrap_or_default())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let named = if scripts.is_empty() {
                    "named by no script and ".to_string()
                } else {
                    format!("named by {} and ", scripts.join(", "))
                };
                notes.push(format!(
                    "note: env-scaled opt-in surface undeclared: {} ({named}read by {}); a \
                     ```gate-env-tier block names the variables, the runner and what it measures \
                     (advisory, because a GATE.md written before the block existed cannot be \
                     failed for a line the grammar did not have)",
                    undeclared.into_iter().collect::<Vec<_>>().join(", "),
                    readers.join(", ")
                ));
            }
            return (Vec::new(), notes);
        };
        let surfaces = parse_env_tier(&block, problems);
        let declared: BTreeSet<String> = surfaces
            .iter()
            .flat_map(|surface| surface.variables.iter().cloned())
            .collect();
        for (variable, scripts) in &detected {
            if !declared.contains(variable) {
                problems.push(format!(
                    "gate-env-tier: {variable} is set by {} and read by this crate's sources, so \
                     it scales an opt-in tier, but no declared surface names it; add it to the \
                     surface's variable list (a surface the declaration omits is exactly the one \
                     nothing else can see)",
                    scripts.iter().cloned().collect::<Vec<_>>().join(", ")
                ));
            }
        }
        for variable in &scriptless {
            if !declared.contains(variable) {
                problems.push(format!(
                    "gate-env-tier: {variable} is read by {} and set by no script of this crate, \
                     so it scales an opt-in tier in-process, but no declared surface names it; add \
                     it to a surface's variable list (a name no script sets is the half the \
                     scripts cannot show, so the declaration is the only record of it)",
                    rust_sources[variable]
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        let available: BTreeSet<String> = rust_sources.keys().cloned().collect();
        for surface in &surfaces {
            let runnerless = surface.runner == ENV_TIER_NO_RUNNER;
            let mut setters: BTreeMap<String, Vec<String>> = BTreeMap::new();
            if runnerless {
                for (path, names) in &script_names {
                    let overlap: Vec<String> = names
                        .iter()
                        .filter(|name| surface.variables.contains(name))
                        .cloned()
                        .collect();
                    if !overlap.is_empty() {
                        let relative = path
                            .strip_prefix(&root)
                            .map(|relative| relative.display().to_string())
                            .unwrap_or_else(|_| path.display().to_string());
                        setters.insert(relative, overlap);
                    }
                }
            } else if !root.join(&surface.runner).is_file() {
                problems.push(format!(
                    "gate-env-tier surface {}: runner {} is not a file under {}; a surface is run \
                     by something, and that something is part of the record",
                    surface.name,
                    repr_str(&surface.runner),
                    root.display()
                ));
                continue;
            }
            let variables: BTreeSet<String> = surface.variables.iter().cloned().collect();
            let unread: Vec<String> = variables.difference(&available).cloned().collect();
            if !unread.is_empty() {
                problems.push(format!(
                    "gate-env-tier surface {}: {} is passed to no env-reading function of this \
                     crate; a declared variable the crate never reads is a stale declaration",
                    surface.name,
                    unread.join(", ")
                ));
            }
            if runnerless {
                if !setters.is_empty() {
                    let detail: Vec<String> = setters
                        .iter()
                        .map(|(script, names)| format!("{script} sets {}", names.join(", ")))
                        .collect();
                    problems.push(format!(
                        "gate-env-tier surface {}: {} states that no script runs it, but {}; a \
                         surface whose variables a script sets has that script as its runner, so \
                         name it",
                        surface.name,
                        repr_str(ENV_TIER_NO_RUNNER),
                        detail.join("; ")
                    ));
                }
                continue;
            }
            let runner_path = root.join(&surface.runner);
            let runner_sets: BTreeSet<String> = match script_names.get(&runner_path) {
                Some(names) => names.clone(),
                None => script_env_names(&runner_path),
            };
            if let Some(load) = &surface.load {
                let outside: Vec<String> = load
                    .factors
                    .iter()
                    .map(|(key, _)| key.clone())
                    .filter(|key| !runner_sets.contains(key))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if !outside.is_empty() {
                    problems.push(format!(
                        "gate-env-tier surface {}: its load sizes {}, which the runner {} does \
                         not set; the recorded shape must be the shape the runner runs",
                        surface.name,
                        outside.join(", "),
                        repr_str(&surface.runner)
                    ));
                }
            }
            let unset: Vec<String> = variables.difference(&runner_sets).cloned().collect();
            if unset.len() == variables.len() {
                problems.push(format!(
                    "gate-env-tier surface {}: runner {} names none of the declared variables \
                     ({}); the runner and the surface must be the same instrument",
                    surface.name,
                    repr_str(&surface.runner),
                    surface.variables.join(", ")
                ));
            }
            let missing: Vec<String> = runner_sets
                .intersection(&available)
                .filter(|name| !variables.contains(*name))
                .cloned()
                .collect();
            if !missing.is_empty() {
                problems.push(format!(
                    "gate-env-tier surface {}: runner {} sets {}, which this crate's sources read, \
                     and the surface does not name them; every variable of the surface must be \
                     declared",
                    surface.name,
                    repr_str(&surface.runner),
                    missing.join(", ")
                ));
            }
        }
        let variables: Vec<String> = surfaces
            .iter()
            .flat_map(|surface| surface.variables.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let cells: usize = surfaces.iter().map(|surface| surface.cells.len()).sum();
        let mut summary = vec![format!(
            "  gate-env-tier: {} env-scaled surface(s), {} variable(s), {cells} coverage cell(s)",
            surfaces.len(),
            variables.len()
        )];
        for surface in &surfaces {
            let runner = if surface.runner == ENV_TIER_NO_RUNNER {
                "no script runner".to_string()
            } else {
                surface.runner.clone()
            };
            summary.push(format!(
                "  gate-env-tier-surface: {} ({runner}) {} - {}",
                surface.name,
                surface.variables.join(", "),
                surface.measures
            ));
            if let Some(load) = &surface.load {
                summary.push(format!(
                    "  gate-env-tier-load: {} {}",
                    surface.name,
                    load.describe()
                ));
            }
        }
        (summary, notes)
    }
}
