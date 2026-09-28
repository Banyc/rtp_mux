//! The producer registry, the checkouts it resolves to, and the run directory.
//!
//! The registry is the runner's own file: it declares which test targets are
//! producers and what each owes the runner, so a missing or malformed one is a
//! failure. No flag names a crate — which crates exist is the registry's
//! business, and the runner names none of them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::tools::json::{self, Json};
use crate::tools::pyre::Regex;

use super::lines::repr;
use super::{
    LOG_NAME, OPTIONAL_PRODUCER_KEYS, PLOTS_DIRNAME, PRIMARY_PRODUCER, PRODUCER_KEYS,
    PRODUCERS_DECLARATION_SCHEMA, REPORT_NAME, REVISION_TIMEOUT_SECONDS,
};

/// One `producers[]` entry, validated.
#[derive(Debug, Clone, PartialEq)]
pub struct Producer {
    pub id: String,
    pub package: String,
    pub target: String,
    pub source: String,
    pub default_path: String,
    pub cargo_args: Vec<String>,
    pub test_args: Vec<String>,
    pub sections: Vec<String>,
    pub verdicts: Vec<String>,
    pub log: String,
    /// The arm coverage declaration this producer owns, if any. Resolved
    /// against the producer's own checkout, never this tool's directory.
    pub declaration: Option<String>,
    /// The baseline report this producer owns, if any.
    pub baseline: Option<String>,
}

/// One producer's record in the report.
#[derive(Debug, Clone, PartialEq)]
pub struct ProducerRecord {
    pub id: String,
    pub package: String,
    pub target: String,
    pub source: String,
    pub default_path: String,
    pub selected: bool,
    pub path: Option<String>,
    pub sections: Vec<String>,
    pub verdicts: Vec<String>,
    pub evidence: bool,
    pub log: String,
    pub command: Option<Vec<String>>,
    pub revision: Option<String>,
    pub change_id: Option<String>,
    pub revision_source: Option<String>,
    pub tree_id: Option<String>,
    pub tree_id_source: Option<String>,
    pub run: RunRecord,
    pub arms: usize,
}

/// What a producer's invocation did.
#[derive(Debug, Clone, PartialEq)]
pub struct RunRecord {
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub log: String,
}

impl ProducerRecord {
    pub fn to_json(&self) -> Json {
        let mut map = BTreeMap::new();
        map.insert("id".to_string(), Json::Str(self.id.clone()));
        map.insert("package".to_string(), Json::Str(self.package.clone()));
        map.insert("target".to_string(), Json::Str(self.target.clone()));
        map.insert("source".to_string(), Json::Str(self.source.clone()));
        map.insert(
            "default_path".to_string(),
            Json::Str(self.default_path.clone()),
        );
        map.insert("selected".to_string(), Json::Bool(self.selected));
        map.insert("path".to_string(), json::opt_str(self.path.as_deref()));
        map.insert("sections".to_string(), json::str_list(&self.sections));
        map.insert("verdicts".to_string(), json::str_list(&self.verdicts));
        map.insert("evidence".to_string(), Json::Bool(self.evidence));
        map.insert("log".to_string(), Json::Str(self.log.clone()));
        map.insert(
            "command".to_string(),
            match &self.command {
                Some(tokens) => json::str_list(tokens),
                None => Json::Null,
            },
        );
        map.insert(
            "revision".to_string(),
            json::opt_str(self.revision.as_deref()),
        );
        map.insert(
            "change_id".to_string(),
            json::opt_str(self.change_id.as_deref()),
        );
        map.insert(
            "revision_source".to_string(),
            json::opt_str(self.revision_source.as_deref()),
        );
        map.insert(
            "tree_id".to_string(),
            json::opt_str(self.tree_id.as_deref()),
        );
        map.insert(
            "tree_id_source".to_string(),
            json::opt_str(self.tree_id_source.as_deref()),
        );
        let mut run = BTreeMap::new();
        run.insert(
            "exit_code".to_string(),
            self.run
                .exit_code
                .map_or(Json::Null, |code| Json::Int(code as i64)),
        );
        run.insert("timed_out".to_string(), Json::Bool(self.run.timed_out));
        run.insert("log".to_string(), Json::Str(self.run.log.clone()));
        map.insert("run".to_string(), Json::Object(run));
        map.insert("arms".to_string(), Json::Int(self.arms as i64));
        Json::Object(map)
    }
}

/// The registry, or a named failure.
pub fn load_producer_declaration(path: &Path, problems: &mut Vec<String>) -> Option<Vec<Producer>> {
    fn refuse(path: &Path, problems: &mut Vec<String>, message: String) -> Option<Vec<Producer>> {
        problems.push(format!(
            "the producer declaration {}: {message}",
            path.display()
        ));
        None
    }
    if !path.is_file() {
        problems.push(format!(
            "the producer declaration {} does not exist, so no producer's arms \
             can be recorded; it travels with this command",
            path.display()
        ));
        return None;
    }
    let payload = match json::parse_document(path) {
        Ok(value) => value,
        Err(error) => {
            problems.push(format!(
                "the producer declaration {} cannot be read: {error}",
                path.display()
            ));
            return None;
        }
    };
    if payload.as_object().is_none() {
        problems.push(format!(
            "the producer declaration {} is not a JSON object",
            path.display()
        ));
        return None;
    }
    let schema = payload.get("schema").and_then(Json::as_str);
    if schema != Some(PRODUCERS_DECLARATION_SCHEMA) {
        problems.push(format!(
            "the producer declaration {} declares schema {}, not {}",
            path.display(),
            repr(schema),
            crate::tools::pyjson::repr_str(PRODUCERS_DECLARATION_SCHEMA)
        ));
        return None;
    }
    let Some(entries) = payload.get("producers").and_then(Json::as_array) else {
        let message = format!(
            "the producer declaration {} declares no producers, so there is nothing \
             to run",
            path.display()
        );
        problems.push(message);
        return None;
    };
    if entries.is_empty() {
        problems.push(format!(
            "the producer declaration {} declares no producers, so there is nothing \
             to run",
            path.display()
        ));
        return None;
    }
    let mut seen: Vec<String> = Vec::new();
    let mut declared: Vec<Producer> = Vec::new();
    for entry in entries {
        if let Some(message) = producer_problem(entry, &seen) {
            return refuse(path, problems, message);
        }
        let producer = producer_from_json(entry).expect("validated above");
        seen.push(producer.id.clone());
        declared.push(producer);
    }
    // A section is an arm-id namespace, so two producers may not share one.
    let mut owners: BTreeMap<String, String> = BTreeMap::new();
    for entry in &declared {
        for section in &entry.sections {
            if let Some(owner) = owners.get(section) {
                return refuse(
                    path,
                    problems,
                    format!(
                        "the section {} is declared by both {} and {}; a section is \
                         an arm-id namespace, so it must have one owner",
                        repr(Some(section)),
                        repr(Some(owner)),
                        repr(Some(&entry.id))
                    ),
                );
            }
            owners.insert(section.clone(), entry.id.clone());
        }
    }
    if !seen.iter().any(|id| id == PRIMARY_PRODUCER) {
        return refuse(
            path,
            problems,
            format!(
                "does not declare the {} producer, whose record the report keeps \
                 under the keys a mandate-check/4 reader reads",
                repr(Some(PRIMARY_PRODUCER))
            ),
        );
    }
    Some(declared)
}

/// One registry entry's first problem, or `None` when it is well formed.
fn producer_problem(entry: &Json, seen: &[String]) -> Option<String> {
    let Some(object) = entry.as_object() else {
        return Some("an entry is not a JSON object".to_string());
    };
    let id = object.get("id").and_then(Json::as_str);
    let missing: Vec<&str> = PRODUCER_KEYS
        .iter()
        .copied()
        .filter(|key| !object.contains_key(*key))
        .collect();
    if !missing.is_empty() {
        return Some(format!(
            "the entry {} is missing {}",
            repr(id),
            missing.join(", ")
        ));
    }
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !PRODUCER_KEYS.contains(key) && !OPTIONAL_PRODUCER_KEYS.contains(key))
        .collect();
    if !unknown.is_empty() {
        return Some(format!(
            "the entry {} carries unknown key(s) {}",
            repr(id),
            unknown.join(", ")
        ));
    }
    let Some(id) = id else {
        return Some("an entry's id is not a non-empty string".to_string());
    };
    if id.is_empty() {
        return Some("an entry's id is not a non-empty string".to_string());
    }
    if seen.iter().any(|other| other == id) {
        return Some(format!("the producer {} is declared twice", repr(Some(id))));
    }
    for key in ["package", "target", "source", "default_path", "log"] {
        let value = object.get(key).and_then(Json::as_str);
        if value.is_none_or(str::is_empty) {
            return Some(format!(
                "the producer {} gives {} no string",
                repr(Some(id)),
                repr(Some(key))
            ));
        }
    }
    for key in ["cargo_args", "test_args"] {
        let some_strings = object
            .get(key)
            .and_then(Json::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .all(|token| token.as_str().is_some_and(|text| !text.is_empty()))
            });
        if !some_strings {
            return Some(format!(
                "the producer {} gives {} no list of tokens",
                repr(Some(id)),
                repr(Some(key))
            ));
        }
    }
    for key in ["sections", "verdicts"] {
        let some_strings = object
            .get(key)
            .and_then(Json::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .all(|token| token.as_str().is_some_and(|text| !text.is_empty()))
            });
        if !some_strings {
            return Some(format!(
                "the producer {} gives {} no list of sections",
                repr(Some(id)),
                repr(Some(key))
            ));
        }
    }
    let sections = string_list(object.get("sections"));
    if sections.is_empty() {
        return Some(format!(
            "the producer {} declares no section, so no arm it prints can be \
             attributed",
            repr(Some(id))
        ));
    }
    let mut sorted = sections.clone();
    sorted.sort();
    sorted.dedup();
    if sorted.len() != sections.len() {
        return Some(format!(
            "the producer {} declares a section twice",
            repr(Some(id))
        ));
    }
    let verdicts = string_list(object.get("verdicts"));
    let outside: Vec<&str> = verdicts
        .iter()
        .map(String::as_str)
        .filter(|section| !sections.iter().any(|candidate| candidate == section))
        .collect();
    if !outside.is_empty() {
        return Some(format!(
            "the producer {} declares the verdict section(s) {} it does not list \
             among its sections",
            repr(Some(id)),
            outside.join(", ")
        ));
    }
    None
}

fn string_list(value: Option<&Json>) -> Vec<String> {
    value
        .and_then(Json::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn producer_from_json(entry: &Json) -> Option<Producer> {
    let text = |key: &str| entry.get(key).and_then(Json::as_str).map(str::to_string);
    Some(Producer {
        id: text("id")?,
        package: text("package")?,
        target: text("target")?,
        source: text("source")?,
        default_path: text("default_path")?,
        cargo_args: string_list(entry.get("cargo_args")),
        test_args: string_list(entry.get("test_args")),
        sections: string_list(entry.get("sections")),
        verdicts: string_list(entry.get("verdicts")),
        log: text("log")?,
        declaration: entry
            .get("declaration")
            .and_then(Json::as_str)
            .map(str::to_string),
        baseline: entry
            .get("baseline")
            .and_then(Json::as_str)
            .map(str::to_string),
    })
}

/// A producer's declared default checkout, resolved against the workspace.
pub fn producer_checkout(producer: &Producer) -> PathBuf {
    let declared = expanduser(&producer.default_path);
    if declared.is_absolute() {
        return absolute(&declared);
    }
    absolute(&workspace_root().join(declared))
}

/// The crate directory this binary was built from. `tools/` — the runner's own
/// declarations — lives inside it, so everything this tool owns resolves from
/// here rather than from a sibling harness's workspace.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The directory the runner's own declarations live in.
pub fn tools_dir() -> PathBuf {
    workspace_root().join("tools")
}

/// The absolute form of a path, resolved the way Python's `Path.resolve()`
/// resolved it: symlinks in the components that exist are followed (so a
/// `/var/folders/...` scratch path is reported as `/private/var/folders/...`
/// on macOS), and a path that does not exist yet keeps its lexical remainder.
pub fn absolute(path: &Path) -> PathBuf {
    if let Ok(resolved) = std::fs::canonicalize(path) {
        return resolved;
    }
    let mut remainder: Vec<std::ffi::OsString> = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        if let Some(name) = current.file_name() {
            remainder.push(name.to_os_string());
        }
        let Some(parent) = current.parent() else {
            break;
        };
        if parent.as_os_str().is_empty() {
            break;
        }
        if let Ok(resolved) = std::fs::canonicalize(parent) {
            let mut out = resolved;
            for part in remainder.iter().rev() {
                out.push(part);
            }
            return out;
        }
        current = parent.to_path_buf();
    }
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// `~` expansion, the way Python's `Path.expanduser()` did it.
fn expanduser(text: &str) -> PathBuf {
    if (text == "~" || text.starts_with("~/"))
        && let Some(home) = std::env::var_os("HOME")
    {
        let rest = text.trim_start_matches('~').trim_start_matches('/');
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(text)
}

/// `(checkout, None)`, or `(None, problem)` naming what is missing.
/// The checkout a producer resolves to, override first: `--producer-path
/// `<id>=<path>` names another tree, otherwise the registry's declared path.
/// This is the *path* alone — [`resolve_producer`] adds the source check — so a
/// caller that only needs to find a file in the checkout (the arm declaration,
/// say) can do so before the producers are validated.
pub fn producer_checkout_with(producer: &Producer, override_path: Option<&str>) -> PathBuf {
    match override_path {
        Some(path) => absolute(&expanduser(path)),
        None => producer_checkout(producer),
    }
}

pub fn resolve_producer(
    producer: &Producer,
    override_path: Option<&str>,
) -> (Option<PathBuf>, Option<String>) {
    let crate_dir = producer_checkout_with(producer, override_path);
    if !crate_dir.join("Cargo.toml").is_file() {
        return (
            None,
            Some(format!(
                "the {} checkout {} has no Cargo.toml, so its {} target cannot be \
                 built from it",
                producer.id,
                crate_dir.display(),
                repr(Some(&producer.target))
            )),
        );
    }
    let source = crate_dir.join(&producer.source);
    if !source.is_file() {
        return (
            None,
            Some(format!(
                "the {} producer's source {} does not exist: the {} target must live \
                 in {}, or --producer-path {}=<path> names the wrong checkout",
                producer.id,
                source.display(),
                repr(Some(&producer.target)),
                source.parent().unwrap_or(Path::new("")).display(),
                producer.id
            )),
        );
    }
    (Some(crate_dir), None)
}

/// `(commit_id, change_id, source)` for the checkout, or `(None, None, None)`.
pub fn resolve_revision(crate_dir: &Path) -> (Option<String>, Option<String>, Option<String>) {
    if let Some(jj) = which("jj") {
        let result = capture_with_timeout(
            &[
                jj.display().to_string(),
                "--no-pager".to_string(),
                "log".to_string(),
                "-r".to_string(),
                "@".to_string(),
                "--no-graph".to_string(),
                "-T".to_string(),
                "commit_id ++ \"\\n\" ++ change_id".to_string(),
            ],
            crate_dir,
            REVISION_TIMEOUT_SECONDS,
        );
        if result.0 == Some(0) {
            let lines: Vec<String> = result
                .1
                .lines()
                .map(|line| line.trim().to_string())
                .filter(|line| !line.is_empty())
                .collect();
            if lines.first().is_some_and(|line| is_commit_id(line)) {
                let change_id = lines.get(1).filter(|line| is_change_id(line)).cloned();
                return (Some(lines[0].clone()), change_id, Some("jj".to_string()));
            }
        }
    }
    if let Some(git) = which("git") {
        let result = capture_with_timeout(
            &[
                git.display().to_string(),
                "-C".to_string(),
                crate_dir.display().to_string(),
                "rev-parse".to_string(),
                "HEAD".to_string(),
            ],
            crate_dir,
            REVISION_TIMEOUT_SECONDS,
        );
        if result.0 == Some(0) {
            let revision = result.1.trim().to_string();
            if is_commit_id(&revision) {
                return (Some(revision), None, Some("git".to_string()));
            }
        }
    }
    (None, None, None)
}

/// `(tree_id, source)` for a resolved commit, or `(None, None)`.
pub fn resolve_tree_id(
    crate_dir: &Path,
    revision: Option<&str>,
    revision_source: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(revision) = revision else {
        return (None, None);
    };
    if let (Some(jj), Some("jj")) = (which("jj"), revision_source) {
        {
            let result = capture_with_timeout(
                &[
                    jj.display().to_string(),
                    "--no-pager".to_string(),
                    "debug".to_string(),
                    "object".to_string(),
                    "commit".to_string(),
                    "--ignore-working-copy".to_string(),
                    revision.to_string(),
                ],
                crate_dir,
                REVISION_TIMEOUT_SECONDS,
            );
            if result.0 == Some(0)
                && let Some(tree) = jj_root_tree_re()
                    .search(&result.1)
                    .and_then(|found| found.named("tree"))
            {
                return (Some(tree), Some("jj".to_string()));
            }
        }
    }
    if let Some(git) = which("git") {
        let result = capture_with_timeout(
            &[
                git.display().to_string(),
                "-C".to_string(),
                crate_dir.display().to_string(),
                "rev-parse".to_string(),
                format!("{revision}^{{tree}}"),
            ],
            crate_dir,
            REVISION_TIMEOUT_SECONDS,
        );
        if result.0 == Some(0) {
            let tree = result.1.trim().to_string();
            if is_commit_id(&tree) {
                return (Some(tree), Some("git".to_string()));
            }
        }
    }
    (None, None)
}

fn jj_root_tree_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r#"root_tree:\s*Resolved\(\s*TreeId\(\s*"(?P<tree>[0-9a-f]{40})""#,
            true,
        )
        .expect("compiles")
    })
}

fn is_commit_id(text: &str) -> bool {
    text.len() == 40
        && text
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_change_id(text: &str) -> bool {
    text.len() >= 10 && text.chars().all(|character| character.is_ascii_lowercase())
}

/// Create the run directory and clear what an earlier run left in it.
pub fn prepare_output_dir(
    out_dir: &Path,
    log_names: &[String],
    mandate_ids: &[String],
) -> Result<(), String> {
    let plots = out_dir.join(PLOTS_DIRNAME);
    if plots.is_file() {
        return Err(format!(
            "the plots path {} is a file, so the panel directory cannot be created; \
             --dir must name a directory this command may write",
            plots.display()
        ));
    }
    if let Err(error) = std::fs::create_dir_all(out_dir) {
        return Err(format!(
            "--dir {} cannot be created: {error}",
            out_dir.display()
        ));
    }
    let ours = out_dir.join(REPORT_NAME).is_file() || out_dir.join(LOG_NAME).is_file();
    let holds_anything = std::fs::read_dir(out_dir)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false);
    if !ours && holds_anything {
        return Err(format!(
            "--dir {} already holds files and no earlier run of this command \
             ({REPORT_NAME} or {LOG_NAME}), so it is not this command's directory to \
             clear; pass an empty --dir",
            out_dir.display()
        ));
    }
    for name in std::iter::once(REPORT_NAME.to_string())
        .chain(std::iter::once(LOG_NAME.to_string()))
        .chain(log_names.iter().cloned())
    {
        let stale = out_dir.join(name);
        if stale.is_file() {
            let _ = std::fs::remove_file(stale);
        }
    }
    for mandate in mandate_ids {
        for suffix in [".json", ".csv"] {
            let stale = out_dir.join(format!("{mandate}{suffix}"));
            if stale.is_file() {
                let _ = std::fs::remove_file(stale);
            }
        }
    }
    if plots.is_dir() {
        let _ = std::fs::remove_dir_all(plots);
    }
    Ok(())
}

/// A fresh directory beneath `$TMPDIR`, or a named failure.
pub fn default_out_dir() -> Result<PathBuf, String> {
    let safe_root = std::env::var("TMPDIR")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let mut last_error = String::new();
    for attempt in 0..64_u64 {
        let name = temp_name(attempt);
        let candidate = safe_root.join(name);
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) => last_error = error.to_string(),
        }
    }
    Err(format!(
        "a run directory cannot be created beneath {}: {last_error}; pass --dir to \
         name one this command may write",
        safe_root.display()
    ))
}

/// Eight characters from the alphabet Python's `mkdtemp` draws from.
fn temp_name(attempt: u64) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_";
    let mut state = std::process::id() as u64
        ^ attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.subsec_nanos() as u64)
            .unwrap_or(0);
    let mut suffix = String::new();
    for _ in 0..8 {
        // xorshift64*
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let value = state.wrapping_mul(0x2545_f491_4f6c_dd1d);
        suffix.push(ALPHABET[(value >> 33) as usize % ALPHABET.len()] as char);
    }
    format!("mandate-check-{suffix}")
}

/// One producer's contract invocation: its declared argv, no filter added.
pub fn producer_command(cargo: &Path, producer: &Producer) -> Vec<String> {
    let mut out = vec![cargo.display().to_string()];
    out.extend(producer.cargo_args.iter().cloned());
    out.push("--".to_string());
    out.extend(super::TIMING_ARGS.iter().map(|token| token.to_string()));
    out.extend(producer.test_args.iter().cloned());
    out
}

/// One producer's record: what it is, where it lives, what it printed.
pub fn producer_record(producer: &Producer, out_dir: &Path) -> ProducerRecord {
    ProducerRecord {
        id: producer.id.clone(),
        package: producer.package.clone(),
        target: producer.target.clone(),
        source: producer.source.clone(),
        default_path: producer.default_path.clone(),
        selected: false,
        path: None,
        sections: producer.sections.clone(),
        verdicts: producer.verdicts.clone(),
        // Derived, not declared: a producer that prints a verdict line owes its
        // evidence, so a registry entry cannot declare the guard away.
        evidence: !producer.verdicts.is_empty(),
        log: out_dir.join(&producer.log).display().to_string(),
        command: None,
        revision: None,
        change_id: None,
        revision_source: None,
        tree_id: None,
        tree_id_source: None,
        run: RunRecord {
            exit_code: None,
            timed_out: false,
            log: out_dir.join(&producer.log).display().to_string(),
        },
        arms: 0,
    }
}

/// The per-producer checkout paths the CLI names.
pub fn producer_overrides(tokens: &[String]) -> (BTreeMap<String, String>, Vec<String>) {
    let mut overrides = BTreeMap::new();
    let mut problems = Vec::new();
    for token in tokens {
        let mut parts = token.splitn(2, '=');
        let producer = parts.next().unwrap_or_default();
        let path = parts.next();
        match path {
            Some(path) if !producer.is_empty() && !path.is_empty() => {
                overrides.insert(producer.to_string(), path.to_string());
            }
            _ => problems.push(format!(
                "--producer-path {} is not <id>=<path>",
                crate::tools::pyjson::repr_str(token)
            )),
        }
    }
    (overrides, problems)
}

/// The producers to run: those named, or every declared producer.
pub fn select_producers(
    declared: &[Producer],
    requested: &[String],
    problems: &mut Vec<String>,
) -> Vec<Producer> {
    if requested.is_empty() {
        return declared.to_vec();
    }
    let mut selected: Vec<Producer> = Vec::new();
    for name in requested {
        match declared.iter().find(|entry| entry.id == *name) {
            Some(found) => {
                if !selected.iter().any(|entry| entry.id == found.id) {
                    selected.push(found.clone());
                }
            }
            None => {
                let known: Vec<String> = declared.iter().map(|entry| entry.id.clone()).collect();
                problems.push(format!(
                    "--producer {} is not one of {}",
                    crate::tools::pyjson::repr_str(name),
                    known.join(", ")
                ));
            }
        }
    }
    selected
}

/// The path of an executable, the way `shutil.which` found one.
pub fn which(program: &str) -> Option<PathBuf> {
    let candidate = Path::new(program);
    if program.contains('/') {
        return is_executable(candidate).then(|| candidate.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(program);
        if is_executable(&candidate) {
            return Some(candidate);
        }
    }
    None
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

/// A child's exit status and stdout, `None` on a failure or a timeout.
pub fn capture_with_timeout(command: &[String], cwd: &Path, timeout: f64) -> (Option<i32>, String) {
    super::exec::capture_with_timeout(command, cwd, timeout)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(producers: &str) -> String {
        format!("{{\"schema\": \"mandate-producers/1\", \"producers\": [{producers}]}}")
    }

    /// One well-formed entry whose section is its own id, so two entries do not
    /// collide on the namespace the duplicate-owner rule guards.
    fn entry(id: &str, extra: &str) -> String {
        entry_with(id, id, extra)
    }

    fn entry_with(id: &str, section: &str, extra: &str) -> String {
        format!(
            "{{\"id\": \"{id}\", \"package\": \"{id}\", \"target\": \"t\", \
             \"source\": \"src/lib.rs\", \"default_path\": \".\", \
             \"cargo_args\": [\"test\"], \"test_args\": [], \
             \"sections\": [\"{section}\"], \"verdicts\": [], \"log\": \"l.log\"{extra}}}"
        )
    }

    /// A path unique within the process. A clock-derived name is not: two
    /// tests in the same nanosecond would share one file, and one test reading
    /// another's registry would pass or fail for a reason that is not its own.
    fn unique_path(dir: &Path, stem: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        dir.join(format!("{stem}-{}-{unique}.json", std::process::id()))
    }

    fn load(text: &str) -> (Option<Vec<Producer>>, Vec<String>) {
        let dir = std::env::temp_dir().join(format!("mandate-producers-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = unique_path(&dir, "p");
        std::fs::write(&path, text).expect("write");
        let mut problems = Vec::new();
        let loaded = load_producer_declaration(&path, &mut problems);
        let _ = std::fs::remove_file(path);
        (loaded, problems)
    }

    #[test]
    fn a_registry_entry_cannot_declare_its_evidence_away() {
        // `evidence` is derived from the verdicts, so naming it is an unknown
        // key rather than a way to drop the obligation.
        let text = registry(&format!(
            "{},{}",
            entry("rtp_mux", ""),
            entry_with("other", "S2", ", \"evidence\": false")
        ));
        let (loaded, problems) = load(&text);
        assert!(loaded.is_none());
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("unknown key(s) evidence")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_section_declared_by_two_producers_is_refused() {
        // A section is an arm-id namespace: the same id from two producers
        // would record two different measurements under one name.
        let text = registry(&format!(
            "{},{}",
            entry("rtp_mux", ""),
            entry_with("other", "rtp_mux", "")
        ));
        let (loaded, problems) = load(&text);
        assert!(loaded.is_none(), "the shared section must be refused");
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("is declared by both")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_registry_without_the_primary_producer_is_refused() {
        let text = registry(&entry("other", ""));
        let (loaded, problems) = load(&text);
        assert!(loaded.is_none());
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("does not declare the 'rtp_mux' producer")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_well_formed_registry_parses() {
        let text = registry(&format!(
            "{},{}",
            entry("rtp_mux", ""),
            entry_with("other", "S2", "")
        ));
        let (loaded, problems) = load(&text);
        assert!(problems.is_empty(), "{problems:?}");
        let loaded = loaded.expect("parsed");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "rtp_mux");
        assert_eq!(loaded[1].sections, vec!["S2".to_string()]);
        assert!(loaded[1].verdicts.is_empty());
    }

    #[test]
    fn a_verdict_section_outside_the_declared_sections_is_refused() {
        let mut entry = entry_with("rtp_mux", "S1", "");
        entry = entry.replace("\"verdicts\": []", "\"verdicts\": [\"S2\"]");
        let (loaded, problems) = load(&registry(&entry));
        assert!(loaded.is_none());
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("does not list among its sections")),
            "{problems:?}"
        );
    }

    #[test]
    fn an_unresolvable_revision_or_tree_is_null_and_never_fabricated() {
        let dir = std::env::temp_dir().join(format!("mandate-tree-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        // No answer from jj or git: a directory outside any repository, and a
        // revision nothing can resolve. A fabricated tree id would make a
        // committed baseline name content it never built.
        assert_eq!(
            resolve_tree_id(&dir, Some("0".repeat(40).as_str()), Some("jj")),
            (None, None)
        );
        assert_eq!(
            resolve_tree_id(&dir, Some("0".repeat(40).as_str()), Some("git")),
            (None, None)
        );
        assert_eq!(resolve_tree_id(&dir, None, None), (None, None));
        let _ = std::fs::remove_dir_all(dir);
    }
}
