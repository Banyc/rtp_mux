//! The panel read record: the runner surfaces, in its own output, every panel
//! it rendered — by absolute path, with a per-panel statement of whether a
//! recorded read exists.
//!
//! `AGENTS.md` makes reading every rendered panel mandatory: the printed
//! `summary|` block is the *property* (axis, bounds, pixel positions, reading)
//! and the render is the *shape*, and a summary alone is not a read. What this
//! module makes **mechanically true** is only that an unread panel is **loud in
//! the run's output**: the run names every rendered panel by absolute path and
//! marks it `read` or `UNREAD`, so a panel that nobody opened is visible in the
//! artifact instead of silently absent from it.
//!
//! ## What this proves, and what it does not
//!
//! It is a **declaration**, not a verification. A direct read can only be issued
//! by the agent that reports the verdict; it cannot be delegated, and no tool can
//! verify that anyone looked at a panel. The runner receives the operator's
//! statement of which panels they opened (`--panels-read`,
//! `--panels-read-file`, `$MANDATE_PANELS_READ`, or a `panels-read.txt` beside
//! the run) and records it. The mechanism removes the **silent omission**, not
//! the lie: an operator who names panels they did not open defeats it, and that
//! is a limit of any mechanical check of another party's reading. The refusal
//! text says so.
//!
//! ## Backwards compatibility
//!
//! A run with **no** record keeps working: the archive and a CI run with no
//! operator must not become uncompilable. Such a run states
//! `PASS (panels unread: n)`, names every unread panel by path, and exits `0`
//! unless `--require-panels-read` is explicit, in which case it refuses and names
//! the unread panels. A record that is **present but incomplete** is always
//! refused, because there the operator made a claim the geometry contradicts —
//! that is not an absence, it is a false statement, and no run that *does* read
//! is weakened by it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::Args;
use crate::tools::json::{self, Json};

/// The env var a CI step or a shell may carry the record in.
pub const ENV_PANELS_READ: &str = "MANDATE_PANELS_READ";
/// The file the runner looks for beside the run when no record is passed.
pub const RECEIPT_FILENAME: &str = "panels-read.txt";
/// The one-line statement of what the per-panel marks do and do not prove.
pub const DECLARATION: &str = "the read mark is the operator's declaration about their own reading, not evidence \
     that a panel was read";

/// The panels an operator reports having opened, and where that claim came from.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Receipt {
    /// The normalized panel names the operator claims to have read.
    pub names: BTreeSet<String>,
    /// The sources the claim came from, for the report: `--panels-read`,
    /// `--panels-read-file <path>`, `$MANDATE_PANELS_READ`, or the default file.
    pub sources: Vec<String>,
    /// Whether any record was supplied at all.
    pub provided: bool,
    /// Whether `--require-panels-read` makes an absent record a refusal.
    pub strict: bool,
}

/// The panel name a plot path names: its file stem, without `.svg`/`.png`.
pub fn panel_name(path: &str) -> String {
    let base = Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string());
    for suffix in [".svg", ".png"] {
        if let Some(stem) = base.strip_suffix(suffix) {
            return stem.to_string();
        }
    }
    base
}

/// Every panel the run rendered, read out of the report's own plot list. The
/// map is panel name -> the absolute path of its SVG (the primary artifact),
/// falling back to the PNG when a run was rendered without rasterizing.
pub fn rendered_panels(plots: &[String]) -> BTreeMap<String, String> {
    let mut rendered: BTreeMap<String, String> = BTreeMap::new();
    for path in plots {
        if !(path.ends_with(".svg") || path.ends_with(".png")) {
            continue;
        }
        let name = panel_name(path);
        let replace = match rendered.get(&name) {
            None => true,
            Some(existing) => path.ends_with(".svg") && !existing.ends_with(".svg"),
        };
        if replace {
            rendered.insert(name, path.clone());
        }
    }
    rendered
}

/// Split one record item into names: comma- and whitespace-separated, so
/// `--panels-read M1-cdf,M2-latency` and a one-per-line file both work.
fn split_names(raw: &str) -> Vec<String> {
    raw.split(|character: char| character == ',' || character.is_whitespace())
        .filter(|name| !name.is_empty())
        .map(panel_name)
        .collect()
}

/// The names a record file holds: one per line, `#` comments and blank lines
/// ignored.
fn read_names_file(path: &Path) -> Result<Vec<String>, String> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "the panel read-receipt file {} cannot be read: {error}",
            path.display()
        )
    })?;
    let mut names = Vec::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        names.extend(split_names(line));
    }
    Ok(names)
}

/// Collect a record from its four possible sources. The default file is only
/// consulted when neither a flag nor the env var supplied one, so an explicit
/// record is never silently extended by a stale file.
fn collect(
    explicit: &[String],
    file: Option<&Path>,
    env: Option<&str>,
    default_file: Option<&Path>,
    strict: bool,
) -> (Receipt, Vec<String>) {
    let mut receipt = Receipt {
        strict,
        ..Receipt::default()
    };
    let mut problems = Vec::new();
    if !explicit.is_empty() {
        receipt.provided = true;
        receipt.sources.push("--panels-read".to_string());
        for item in explicit {
            receipt.names.extend(split_names(item));
        }
    }
    if let Some(path) = file {
        receipt.provided = true;
        receipt
            .sources
            .push(format!("--panels-read-file {}", path.display()));
        match read_names_file(path) {
            Ok(names) => receipt.names.extend(names),
            Err(problem) => problems.push(problem),
        }
    }
    if let Some(raw) = env.filter(|raw| !raw.trim().is_empty()) {
        receipt.provided = true;
        receipt.sources.push(format!("${ENV_PANELS_READ}"));
        receipt.names.extend(split_names(raw));
    }
    if !receipt.provided
        && let Some(path) = default_file.filter(|path| path.is_file())
    {
        receipt.provided = true;
        receipt.sources.push(path.display().to_string());
        match read_names_file(path) {
            Ok(names) => receipt.names.extend(names),
            Err(problem) => problems.push(problem),
        }
    }
    (receipt, problems)
}

/// Resolve the record from the flags, the env var and the default file. A file
/// the tool cannot read is returned as a problem, not swallowed: the caller
/// turns it into an evidence failure.
pub fn resolve(args: &Args, out_dir: &Path) -> (Receipt, Vec<String>) {
    let env = std::env::var_os(ENV_PANELS_READ).map(|raw| raw.to_string_lossy().into_owned());
    let default_file: PathBuf = out_dir.join(RECEIPT_FILENAME);
    collect(
        &args.panels_read,
        args.panels_read_file.as_deref(),
        env.as_deref(),
        Some(&default_file),
        args.require_panels_read,
    )
}

/// The JSON record, and the refusals a record leaves.
pub fn evaluate(receipt: &Receipt, rendered: &BTreeMap<String, String>) -> (Json, Vec<String>) {
    let read: Vec<String> = rendered
        .keys()
        .filter(|name| receipt.names.contains(*name))
        .cloned()
        .collect();
    let unread: Vec<String> = rendered
        .keys()
        .filter(|name| !receipt.names.contains(*name))
        .cloned()
        .collect();
    let unknown: Vec<String> = receipt
        .names
        .iter()
        .filter(|name| !rendered.contains_key(*name))
        .cloned()
        .collect();
    let mut problems = Vec::new();
    if !unknown.is_empty() {
        problems.push(format!(
            "the panel read record names {} panel(s) this run did not render ({}); a \
             name that matches no rendered panel is a hole, not a reading, so the \
             declaration cannot be true as given",
            unknown.len(),
            py_list(&unknown)
        ));
    }
    if receipt.provided && !unread.is_empty() {
        problems.push(format!(
            "the panel read record covers {} of the {} rendered panel(s) and leaves \
             {} unread ({}); the run refuses PASS because an unread panel is not \
             evidence, however green the verdict line",
            read.len(),
            rendered.len(),
            unread.len(),
            py_list(&unread)
        ));
    }
    if !receipt.provided && receipt.strict && !rendered.is_empty() {
        problems.push(format!(
            "--require-panels-read was given and no record is present (no \
             --panels-read, no --panels-read-file, no ${ENV_PANELS_READ}, no \
             {RECEIPT_FILENAME} beside the run), so the {} rendered panel(s) ({}) are \
             unread; the run refuses PASS",
            rendered.len(),
            py_list(&unread)
        ));
    }
    let note = note(receipt, rendered, &read, &unread);
    let panels: Vec<Json> = rendered
        .iter()
        .map(|(name, path)| {
            let mut entry = BTreeMap::new();
            entry.insert("name".to_string(), Json::Str(name.clone()));
            entry.insert("path".to_string(), Json::Str(path.clone()));
            entry.insert("read".to_string(), Json::Bool(receipt.names.contains(name)));
            Json::Object(entry)
        })
        .collect();
    let mut map = BTreeMap::new();
    map.insert(
        "declaration".to_string(),
        Json::Str(DECLARATION.to_string()),
    );
    map.insert("provided".to_string(), Json::Bool(receipt.provided));
    map.insert("strict".to_string(), Json::Bool(receipt.strict));
    map.insert("sources".to_string(), json::str_list(&receipt.sources));
    map.insert("panels".to_string(), Json::Array(panels));
    map.insert("read".to_string(), json::str_list(&read));
    map.insert("unread".to_string(), json::str_list(&unread));
    map.insert("unknown".to_string(), json::str_list(&unknown));
    map.insert("note".to_string(), Json::Str(note));
    (Json::Object(map), problems)
}

/// The per-panel lines the run prints: every rendered panel by absolute path,
/// marked `read` or `UNREAD`.
pub fn panel_lines(record: &Json) -> Vec<String> {
    record
        .get("panels")
        .and_then(Json::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let path = entry
                        .get("path")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let read = matches!(entry.get("read"), Some(Json::Bool(true)));
                    format!("panel: {}  {}", path, if read { "read" } else { "UNREAD" })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The one-line statement the run prints for the record.
fn note(
    receipt: &Receipt,
    rendered: &BTreeMap<String, String>,
    read: &[String],
    unread: &[String],
) -> String {
    if !receipt.provided {
        let strict = if receipt.strict {
            " --require-panels-read was given, so this run refuses"
        } else {
            "; pass --require-panels-read to refuse a run with no record"
        };
        return format!(
            "panels: {} rendered, 0 read (no record: no --panels-read, no \
             --panels-read-file, no ${} and no {RECEIPT_FILENAME} beside the run) -- \
             PASS (panels unread: {}){strict}; {DECLARATION}",
            rendered.len(),
            ENV_PANELS_READ,
            unread.len(),
        );
    }
    let sources = receipt.sources.join(", ");
    if unread.is_empty() {
        format!(
            "panels: {} rendered, {} read (record: {sources}); {DECLARATION}",
            rendered.len(),
            read.len(),
        )
    } else {
        format!(
            "panels: {} rendered, {} read, {} unread ({}) (record: {sources})",
            rendered.len(),
            read.len(),
            unread.len(),
            py_list(unread),
        )
    }
}

/// The clause the verdict summary line carries.
pub fn verdict_clause(record: &Json) -> String {
    let len = |key: &str| {
        record
            .get(key)
            .and_then(Json::as_array)
            .map(|items| items.len())
            .unwrap_or(0)
    };
    let rendered = record
        .get("panels")
        .and_then(Json::as_array)
        .map(|items| items.len())
        .unwrap_or_else(|| len("read") + len("unread"));
    let (read, unread) = (len("read"), len("unread"));
    if unread == 0 {
        format!("panels read: {read}/{rendered}")
    } else {
        format!("panels unread: {unread}")
    }
}

fn py_list(items: &[String]) -> String {
    items
        .iter()
        .map(|item| crate::tools::pyjson::repr_str(item))
        .collect::<Vec<String>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(names: &[&str]) -> BTreeMap<String, String> {
        names
            .iter()
            .map(|name| ((*name).to_string(), format!("/tmp/run/plots/{name}.svg")))
            .collect()
    }

    fn receipt(names: &[&str]) -> Receipt {
        Receipt {
            names: names.iter().map(|name| (*name).to_string()).collect(),
            sources: vec!["--panels-read".to_string()],
            provided: true,
            strict: false,
        }
    }

    #[test]
    fn a_complete_record_covers_every_rendered_panel() {
        let (record, problems) = evaluate(
            &receipt(&["M1-cdf", "M2-latency"]),
            &rendered(&["M1-cdf", "M2-latency"]),
        );
        assert!(problems.is_empty(), "{problems:?}");
        assert_eq!(verdict_clause(&record), "panels read: 2/2");
    }

    #[test]
    fn an_unread_panel_is_surfaced_by_path_and_refused_when_a_record_is_given() {
        // Vacuity: the *only* difference from the passing case above is the
        // record's coverage, and it is that coverage the refusal guards. Drop
        // `M2-latency` and the run must mark it UNREAD by absolute path and
        // redden with the tool's own message naming it.
        let (record, problems) =
            evaluate(&receipt(&["M1-cdf"]), &rendered(&["M1-cdf", "M2-latency"]));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("'M2-latency'"), "{problems:?}");
        assert!(problems[0].contains("refuses PASS"), "{problems:?}");
        assert_eq!(verdict_clause(&record), "panels unread: 1");
        let lines = panel_lines(&record);
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(
            lines
                .iter()
                .any(|line| line == "panel: /tmp/run/plots/M1-cdf.svg  read"),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line == "panel: /tmp/run/plots/M2-latency.svg  UNREAD"),
            "{lines:?}"
        );
    }

    #[test]
    fn a_record_naming_a_panel_the_run_did_not_render_is_refused() {
        let (_, problems) = evaluate(&receipt(&["M1-cdf", "M9-phantom"]), &rendered(&["M1-cdf"]));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("'M9-phantom'"), "{problems:?}");
        assert!(problems[0].contains("hole, not a reading"), "{problems:?}");
    }

    #[test]
    fn a_run_with_no_record_states_the_unread_count_and_stays_ok() {
        let empty = Receipt::default();
        let (record, problems) = evaluate(&empty, &rendered(&["M1-cdf", "M2-latency"]));
        assert!(problems.is_empty(), "{problems:?}");
        assert_eq!(verdict_clause(&record), "panels unread: 2");
        let note = record.get("note").and_then(Json::as_str).unwrap_or("");
        assert!(note.contains("panels unread: 2"), "{note}");
        assert!(note.contains("no record"), "{note}");
        // The per-panel lines state the declaration, so a reader of the run
        // cannot mistake the marks for a verification.
        assert!(
            note.contains("not evidence that a panel was read"),
            "{note}"
        );
    }

    #[test]
    fn a_strict_run_with_no_record_refuses_and_names_every_panel() {
        let strict = Receipt {
            strict: true,
            ..Receipt::default()
        };
        let (_, problems) = evaluate(&strict, &rendered(&["M1-cdf", "M2-latency"]));
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("'M1-cdf'"), "{problems:?}");
        assert!(problems[0].contains("'M2-latency'"), "{problems:?}");
        assert!(
            problems[0].contains("--require-panels-read"),
            "{problems:?}"
        );
    }

    #[test]
    fn a_record_file_the_tool_cannot_read_is_a_problem_not_an_absent_record() {
        let missing = std::env::temp_dir().join("receipt-that-does-not-exist.txt");
        let (receipt, problems) = collect(&[], Some(&missing), None, None, false);
        assert!(
            receipt.provided,
            "a named file is a record even when unreadable"
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("cannot be read"), "{problems:?}");
    }

    #[test]
    fn a_panel_name_is_a_stem_without_its_raster_extension() {
        assert_eq!(panel_name("/tmp/run/plots/M2-latency.svg"), "M2-latency");
        assert_eq!(panel_name("plots/M2-latency.png"), "M2-latency");
        assert_eq!(panel_name("M2-latency"), "M2-latency");
    }

    #[test]
    fn rendered_panels_prefer_the_svg_and_deduplicate_the_png() {
        let plots = vec![
            "/tmp/r/plots/M1-cdf.png".to_string(),
            "/tmp/r/plots/M1-cdf.svg".to_string(),
            "/tmp/r/plots/M2-latency.svg".to_string(),
        ];
        let map = rendered_panels(&plots);
        assert_eq!(map.len(), 2, "{map:?}");
        assert_eq!(map["M1-cdf"], "/tmp/r/plots/M1-cdf.svg");
        assert_eq!(map["M2-latency"], "/tmp/r/plots/M2-latency.svg");
    }

    #[test]
    fn a_record_file_reads_one_name_per_line_and_ignores_comments() {
        let dir = std::env::temp_dir().join(format!("receipt-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("the temp dir");
        let path = dir.join("panels-read.txt");
        std::fs::write(
            &path,
            "# the panels I opened\nM1-cdf\n\nM2-latency  # the tail one\n",
        )
        .expect("the record file");
        let names = read_names_file(&path).expect("the file reads");
        assert_eq!(names, vec!["M1-cdf", "M2-latency"]);
        // Vacuity: a file the tool cannot read is an error, not an empty record
        // (an absent record is lenient, a broken one is not).
        assert!(read_names_file(&dir.join("nope.txt")).is_err());
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!dir.is_dir());
    }
}
