//! `netem-tools check-gate` — the scenario gate checker, ported from
//! `tools/check-gate.py`.
//!
//! `cargo test` silently skips every `#[ignore]`d scenario, so the set of
//! opt-in scenarios and their tiers is recorded in a crate's `GATE.md`. This
//! subcommand re-derives that set from the compiled test binaries and exits
//! non-zero when the manifest and reality disagree, so a scenario can never be
//! added, removed, or re-ignored without the gate documentation being updated.
//!
//! It enforces, in one run: the `gate-manifest` / `gate-default-required` /
//! `gate-asserting` blocks; the report-only `perf` tier (a `perf` scenario's own
//! body, and the crate-local helpers its call graph reaches, may hold no
//! assertion the `gate-perf-guard-helpers` block does not record); the
//! `gate-perf-design` / `gate-budgets` / `gate-coverage-gaps` declaration with
//! its baseline families, derived relations and cell-name membership; the
//! `gate-env-tier` surfaces and the Rust sources that read them; the
//! `gate-lane-roles` block against `perf_loop.lane_classification` (harness
//! mode only); and the documented counts that a command already determines
//! (harness mode only).
//!
//! The port keeps the Python checker's decisions, its diagnostics' wording and
//! its exit codes: a difference in any of the three is a defect, not a
//! refactor. The differential taken against a fresh `python3 tools/check-gate.py`
//! run over every crate's real invocation -- before the Python twin was deleted
//! with this port -- is what held that, and the port's own Rust test targets
//! hold the refusals it exercised.

pub mod doc_counts;
pub mod env_tier;
pub mod perf;
pub mod scan;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use crate::tools::pyformat;
use crate::tools::pyjson::repr_str;
use crate::tools::pyre::Regex;

// -- the checker's vocabulary ------------------------------------------------

/// The three opt-in tiers a `gate-manifest` line may name.
pub const TIERS: [&str; 3] = ["standard", "full", "perf"];
/// The tiers a perf design row may name: the opt-in tiers plus the always-run
/// default tier.
pub const PERF_TIERS: [&str; 4] = ["standard", "full", "perf", "default"];
/// The relation a `gate-perf-design` row declares to a `gate-budgets` baseline.
pub const RELATION_KINDS: [&str; 4] = ["baseline", "orthogonal", "composite", "re-measurement"];
/// The roles a perf-loop lane may play.
pub const LANE_ROLES: [&str; 2] = ["verdict", "diagnostic"];
/// The reserved perf-design target naming a package's `--lib` test target.
pub const LIB_TARGET: &str = "lib";
/// The default drift tolerance (relative) and the absolute floor below which a
/// difference is not reported.
pub const DEFAULT_DRIFT_TOLERANCE: f64 = 0.5;
pub const DEFAULT_DRIFT_FLOOR_SECONDS: f64 = 2.0;
/// The report `mandate-check` writes, read for the drift comparison by default.
pub const DEFAULT_REPORT_NAME: &str = "mandate-check.json";
/// The tiers whose scenarios assert a gate property.
pub const ASSERTING_TIERS: [&str; 2] = ["standard", "full"];
/// Never scanned: build output and version-control metadata.
pub const SKIP_DIRS: [&str; 5] = ["target", ".git", ".jj", "node_modules", ".pytest_cache"];
/// A file larger than this is data, not a script or a source to read literals from.
pub const MAX_SCANNED_BYTES: u64 = 2_000_000;

/// `<property>@<dimension>=<value>[+...]`, the coverage-cell grammar.
pub fn cell_property_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9_.-]*$", false).expect("compiles"))
}

pub fn cell_dimension_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9_-]*=[^=,+\s]+$", false).expect("compiles"))
}

/// A cargo package name, as a `gate-lib-package` declaration must state one.
pub fn package_name_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9_-]*$", false).expect("compiles"))
}

/// A name a dimension or a baseline family carries.
pub fn cell_key_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9_-]*$", false).expect("compiles"))
}

/// An environment variable name.
pub fn env_name_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[A-Z][A-Z0-9_]{2,}$", false).expect("compiles"))
}

/// A name the toolchain sets for every build and every test process.
pub fn toolchain_env_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^CARGO(?:$|_)", false).expect("compiles"))
}

/// A whole string literal's contents.
pub fn string_literal_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r#""([^"\n]*)""#, false).expect("compiles"))
}

/// A family's cell-name namespace: a cell name, optionally `*`-suffixed.
pub fn membership_prefix_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9_.-]*\*?$", false).expect("compiles"))
}

/// A numeric literal in a load shape's `total` expression.
pub fn env_tier_load_count_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| Regex::new(r"^[0-9]+$", false).expect("compiles"))
}

/// `<seconds>s` in a load shape's `wall`.
pub fn env_tier_load_wall_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(r"^([0-9]+(?:\.[0-9]+)?(?:[eE][-+]?[0-9]+)?)s$", false).expect("compiles")
    })
}

/// `<rate>/<unit>` in a load shape's `bound`.
pub fn env_tier_load_bound_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^([0-9]+(?:\.[0-9]+)?(?:[eE][-+]?[0-9]+)?)/([A-Za-z][A-Za-z0-9_-]*)$",
            false,
        )
        .expect("compiles")
    })
}

/// `<kind>[(<args>)][@<family>]`, a perf row's relation field.
pub fn relation_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"^(?P<kind>[A-Za-z][A-Za-z0-9_-]*)(?:\((?P<argument>[^()@]*)\))?(?:@(?P<family>[A-Za-z][A-Za-z0-9_-]*))?$",
            false,
        )
        .expect("compiles")
    })
}

/// Python's `str.partition(separator)`.
pub use scan::partition;

/// The drift tolerance of a documented count: a relative difference past it is
/// a failure. (The same 5 % the Python checker applies.)
pub const DOC_COUNT_TOLERANCE: f64 = 0.05;

/// A fatal error: the process prints the accumulated stderr and exits 1,
/// exactly as `sys.exit(<message>)` does.
#[derive(Debug)]
pub struct Fatal;

/// The checker's internal result.
pub type R<T> = Result<T, Fatal>;

/// What a run produced: the two streams and the exit code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub stdout: String,
    pub stderr: String,
    pub exit: i32,
}

// -- the crate's gate layout -------------------------------------------------

/// One crate's gate layout: where cargo runs and where the scenarios live.
#[derive(Debug, Clone)]
pub struct Layout {
    /// The crate workspace root (`cargo test -p <package>`'s cwd).
    pub root: PathBuf,
    /// The package whose test binaries are enumerated.
    pub package: String,
    /// The scenario target directory.
    pub dir: PathBuf,
    /// The `GATE.md` that records the tiers.
    pub manifest: PathBuf,
    /// The shared parent of the sibling crate checkouts (`crates/`).
    pub crates_root: PathBuf,
    /// Harness mode additionally enables the lane-role and doc-count checks.
    pub is_harness: bool,
    /// The manifest's own text, read once.
    pub manifest_text: String,
    /// The package whose `--lib` target the reserved `lib` entries name.
    pub lib_package: String,
    /// The package root of the `--lib` target.
    pub lib_root: PathBuf,
}

impl Layout {
    /// The checked crate's own layer kit, when it is read from the checked root.
    pub fn own_kit_dir(&self) -> Option<PathBuf> {
        if self.package == "tests" {
            return None;
        }
        let own = self.root.join("src").join("testkit");
        own.is_dir().then_some(own)
    }

    /// `(kit dir, crate-qualified module prefix)` pairs scanned per target.
    pub fn kit_source_dirs(&self) -> Vec<(PathBuf, String)> {
        let mut dirs: Vec<(PathBuf, String)> = Vec::new();
        if let Some(own) = self.own_kit_dir() {
            dirs.push((own.clone(), format!("{}::testkit", self.package)));
        }
        let mut seen: BTreeSet<PathBuf> = dirs.iter().map(|(dir, _)| dir.clone()).collect();
        for (kit_dir, prefix) in [
            (
                self.crates_root.join("rtp").join("src").join("testkit"),
                "rtp::testkit",
            ),
            (
                self.crates_root.join("mux").join("src").join("testkit"),
                "mux::testkit",
            ),
            (
                self.crates_root.join("rtp_mux").join("src").join("testkit"),
                "rtp_mux::testkit",
            ),
            (
                self.crates_root
                    .join("netem_test")
                    .join("netem-test")
                    .join("src")
                    .join("kit"),
                "netem_test::kit",
            ),
        ] {
            if seen.insert(kit_dir.clone()) {
                dirs.push((kit_dir, prefix.to_string()));
            }
        }
        dirs
    }

    /// The body of the ```name fenced block in a text, or `None`.
    pub fn fenced_block(text: &str, name: &str) -> Option<String> {
        let needle = format!("```{name}\n");
        let start = text.find(&needle)? + needle.len();
        let end = text[start..].find("```")?;
        Some(text[start..start + end].to_string())
    }

    /// The body of the ```name fenced block of this crate's manifest.
    pub fn manifest_block(&self, name: &str) -> Option<String> {
        Layout::fenced_block(&self.manifest_text, name)
    }
}

/// The default layout: the netem_test `tests` package, rooted at `repo`.
pub fn harness_layout(repo: &Path) -> Layout {
    Layout {
        root: repo.to_path_buf(),
        package: "tests".to_string(),
        dir: repo.join("tests").join("tests"),
        manifest: repo.join("tests").join("GATE.md"),
        crates_root: repo.parent().unwrap_or(repo).to_path_buf(),
        is_harness: true,
        manifest_text: String::new(),
        lib_package: String::new(),
        lib_root: repo.to_path_buf(),
    }
}

/// The package a manifest declares for the reserved `lib` target, if any.
pub fn declared_lib_package(manifest_text: &str) -> Result<Option<String>, String> {
    let Some(block) = Layout::fenced_block(manifest_text, "gate-lib-package") else {
        return Ok(None);
    };
    let names: Vec<String> = block
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect();
    if names.len() != 1 {
        return Err(format!(
            "the ```gate-lib-package block must name exactly one package, found {}",
            names.len()
        ));
    }
    let name = &names[0];
    if !package_name_re().is_match(name) {
        return Err(format!(
            "gate-lib-package names {}, which is not a cargo package name",
            repr_str(name)
        ));
    }
    Ok(Some(name.clone()))
}

// -- cargo, behind a seam so the ported fixtures can stand one in -------------

/// A cargo invocation that failed, with the message the checker exits on.
#[derive(Debug, Clone)]
pub struct CargoFailure {
    pub stderr: String,
    pub message: String,
}

/// The two cargo invocations the checker makes.
pub trait Cargo {
    /// `cargo metadata --format-version 1 --no-deps` in `root`.
    fn metadata(&self, root: &Path) -> Result<String, CargoFailure>;
    /// `cargo test -p <package> (--lib | --test <target>) -- --list [--ignored]`.
    fn list(
        &self,
        root: &Path,
        package: &str,
        target: Option<&str>,
        ignored: bool,
    ) -> Result<String, CargoFailure>;
}

/// The real cargo on `PATH`.
pub struct SystemCargo;

impl Cargo for SystemCargo {
    fn metadata(&self, root: &Path) -> Result<String, CargoFailure> {
        let argv = ["metadata", "--format-version", "1", "--no-deps"];
        let output = Command::new("cargo").args(argv).current_dir(root).output();
        let (stdout, stderr, code) = match output {
            Ok(output) => (
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
                output.status.code().unwrap_or(1),
            ),
            Err(error) => (String::new(), format!("{error}\n"), 1),
        };
        if code != 0 {
            return Err(CargoFailure {
                stderr,
                message: format!("cargo {} failed in {}", argv.join(" "), root.display()),
            });
        }
        Ok(stdout)
    }

    fn list(
        &self,
        root: &Path,
        package: &str,
        target: Option<&str>,
        ignored: bool,
    ) -> Result<String, CargoFailure> {
        let mut argv: Vec<String> = vec!["test".into(), "-p".into(), package.to_string()];
        let where_clause: Vec<String> = match target {
            None => vec!["--lib".into()],
            Some(target) => vec!["--test".into(), target.to_string()],
        };
        argv.extend(where_clause.iter().cloned());
        argv.push("--".into());
        argv.push("--list".into());
        if ignored {
            argv.push("--ignored".into());
        }
        let output = Command::new("cargo").args(&argv).current_dir(root).output();
        let (stdout, stderr, code) = match output {
            Ok(output) => (
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
                output.status.code().unwrap_or(1),
            ),
            Err(error) => (String::new(), format!("{error}\n"), 1),
        };
        if code != 0 {
            let mode = if ignored {
                " --list --ignored"
            } else {
                " --list"
            };
            return Err(CargoFailure {
                stderr,
                message: format!(
                    "cargo test -p {package} {}{mode} failed",
                    where_clause.join(" ")
                ),
            });
        }
        Ok(stdout)
    }
}

// -- the session -------------------------------------------------------------

/// One checker run's state: the layout, the cargo seam, the two streams, the
/// failing-block symptoms, and the cached test listings.
pub struct Session<'a> {
    pub layout: Layout,
    pub cargo: &'a dyn Cargo,
    pub out: String,
    pub err: String,
    pub bad: bool,
    pub symptoms: Vec<String>,
    listings: HashMap<(String, bool), BTreeSet<String>>,
    package_targets: Option<BTreeMap<String, PathBuf>>,
}

impl<'a> Session<'a> {
    pub fn new(layout: Layout, cargo: &'a dyn Cargo) -> Session<'a> {
        Session {
            layout,
            cargo,
            out: String::new(),
            err: String::new(),
            bad: false,
            symptoms: Vec::new(),
            listings: HashMap::new(),
            package_targets: None,
        }
    }

    pub fn print(&mut self, line: impl AsRef<str>) {
        self.out.push_str(line.as_ref());
        self.out.push('\n');
    }

    /// `sys.exit(<message>)`: append it to stderr and fail the run.
    pub fn exit(&mut self, message: impl AsRef<str>) -> Fatal {
        self.err.push_str(message.as_ref());
        self.err.push('\n');
        Fatal
    }

    /// Record a failing block's symptom, once.
    pub fn fail(&mut self, symptom: impl Into<String>) {
        self.bad = true;
        let symptom = symptom.into();
        if !self.symptoms.contains(&symptom) {
            self.symptoms.push(symptom);
        }
    }

    // -- manifest blocks ----------------------------------------------------

    pub fn manifest_entries(&mut self) -> R<BTreeMap<String, String>> {
        let Some(block) = self.layout.manifest_block("gate-manifest") else {
            let manifest = self.layout.manifest.display().to_string();
            return Err(self.exit(format!("{manifest}: no ```gate-manifest block found")));
        };
        let mut entries: BTreeMap<String, String> = BTreeMap::new();
        for raw in block.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, _, tier) = partition(line, " = ");
            let name = name.trim().to_string();
            let tier = tier.trim().to_string();
            if !TIERS.contains(&tier.as_str()) {
                let manifest = self.layout.manifest.display().to_string();
                return Err(self.exit(format!(
                    "{manifest}: {name} has unknown tier {}",
                    repr_str(&tier)
                )));
            }
            if entries.contains_key(&name) {
                let manifest = self.layout.manifest.display().to_string();
                return Err(self.exit(format!("{manifest}: duplicate entry {name}")));
            }
            entries.insert(name, tier);
        }
        Ok(entries)
    }

    pub fn required_default_entries(&self) -> Vec<String> {
        match self.layout.manifest_block("gate-default-required") {
            None => Vec::new(),
            Some(block) => block
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty() && !line.starts_with('#'))
                .map(str::to_string)
                .collect(),
        }
    }

    pub fn asserting_entries(&mut self) -> R<Vec<String>> {
        let Some(block) = self.layout.manifest_block("gate-asserting") else {
            let manifest = self.layout.manifest.display().to_string();
            return Err(self.exit(format!("{manifest}: no ```gate-asserting block found")));
        };
        Ok(block
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect())
    }

    pub fn recorded_perf_guard_helpers(&mut self) -> R<BTreeMap<String, i64>> {
        let Some(block) = self.layout.manifest_block("gate-perf-guard-helpers") else {
            let manifest = self.layout.manifest.display().to_string();
            return Err(self.exit(format!(
                "{manifest}: no ```gate-perf-guard-helpers block found"
            )));
        };
        let mut recorded: BTreeMap<String, i64> = BTreeMap::new();
        for raw in block.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (identity, _, count) = partition(line, " = ");
            let identity = identity.trim().to_string();
            let count = count.trim();
            let Some(parsed) = py_int(count) else {
                let manifest = self.layout.manifest.display().to_string();
                return Err(self.exit(format!(
                    "{manifest}: malformed perf guard helper entry: {}",
                    repr_str(line)
                )));
            };
            if recorded.contains_key(&identity) {
                let manifest = self.layout.manifest.display().to_string();
                return Err(self.exit(format!(
                    "{manifest}: duplicate perf guard helper {identity}"
                )));
            }
            recorded.insert(identity, parsed);
        }
        Ok(recorded)
    }

    // -- cargo-derived test lists ------------------------------------------

    /// The `<target>::<test>` names cargo reports for one test target.
    pub fn listed_test_names(
        &mut self,
        target: &str,
        ignored: bool,
        lib: bool,
        package: Option<&str>,
    ) -> R<BTreeSet<String>> {
        let package = package.unwrap_or(&self.layout.package).to_string();
        let key = (format!("{target}\u{1}{package}"), ignored);
        if let Some(found) = self.listings.get(&key) {
            return Ok(found.clone());
        }
        let target_arg = if lib { None } else { Some(target) };
        let stdout = match self
            .cargo
            .list(&self.layout.root, &package, target_arg, ignored)
        {
            Ok(stdout) => stdout,
            Err(failure) => {
                self.err.push_str(&failure.stderr);
                return Err(self.exit(failure.message));
            }
        };
        let mut found: BTreeSet<String> = BTreeSet::new();
        for line in stdout.lines() {
            let Some(name) = line.strip_suffix(": test") else {
                continue;
            };
            if name.contains("::support::") || name.starts_with("support::") {
                continue;
            }
            found.insert(format!("{target}::{name}"));
        }
        self.listings.insert(key, found.clone());
        Ok(found)
    }

    /// A target's test names, resolving the reserved `lib` name to `--lib`.
    pub fn listed_scenarios(&mut self, target: &str, ignored: bool) -> R<BTreeSet<String>> {
        let lib = target == LIB_TARGET;
        let package = if lib {
            Some(self.layout.lib_package.clone())
        } else {
            None
        };
        self.listed_test_names(target, ignored, lib, package.as_deref())
    }

    pub fn ignored_scenarios(&mut self, target: &str) -> R<BTreeSet<String>> {
        self.listed_scenarios(target, true)
    }

    /// `{target name: source path}` for the checked package's test targets.
    pub fn package_test_targets(&mut self) -> R<BTreeMap<String, PathBuf>> {
        if let Some(targets) = &self.package_targets {
            return Ok(targets.clone());
        }
        let stdout = match self.cargo.metadata(&self.layout.root) {
            Ok(stdout) => stdout,
            Err(failure) => {
                self.err.push_str(&failure.stderr);
                return Err(self.exit(failure.message));
            }
        };
        let argv = "metadata --format-version 1 --no-deps";
        let Ok(parsed) = crate::tools::json::parse(&stdout) else {
            return Err(self.exit(format!("cargo {argv} printed no package list")));
        };
        let Some(packages) = parsed.get("packages").and_then(|value| value.as_array()) else {
            return Err(self.exit(format!("cargo {argv} printed no package list")));
        };
        for package in packages {
            if package.get("name").and_then(|value| value.as_str()) != Some(&self.layout.package) {
                continue;
            }
            let mut targets: BTreeMap<String, PathBuf> = BTreeMap::new();
            let Some(list) = package.get("targets").and_then(|value| value.as_array()) else {
                continue;
            };
            for target in list {
                let kinds = target
                    .get("kind")
                    .and_then(|value| value.as_array())
                    .map(|items| items.to_vec())
                    .unwrap_or_default();
                if !kinds.iter().any(|kind| kind.as_str() == Some("test")) {
                    continue;
                }
                let (Some(name), Some(src_path)) = (
                    target.get("name").and_then(|value| value.as_str()),
                    target.get("src_path").and_then(|value| value.as_str()),
                ) else {
                    continue;
                };
                targets.insert(name.to_string(), PathBuf::from(src_path));
            }
            self.package_targets = Some(targets.clone());
            return Ok(targets);
        }
        Err(self.exit(format!(
            "cargo {argv} does not report package {} in {}",
            self.layout.package,
            self.layout.root.display()
        )))
    }

    /// The scenario directory to derive the ignored set from.
    pub fn scenario_dir_resolution(&mut self) -> R<(Option<PathBuf>, Vec<String>, Vec<String>)> {
        let listed: BTreeSet<String> = match std::fs::read_dir(&self.layout.dir) {
            Ok(entries) => entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
                .filter_map(|path| {
                    path.file_stem()
                        .map(|stem| stem.to_string_lossy().to_string())
                })
                .collect(),
            Err(_) => BTreeSet::new(),
        };
        let targets = self.package_test_targets()?;
        let names: BTreeSet<String> = targets.keys().cloned().collect();
        if names.is_empty() || (!listed.is_empty() && names.difference(&listed).next().is_none()) {
            return Ok((None, Vec::new(), Vec::new()));
        }
        let missing: Vec<String> = names.difference(&listed).cloned().collect();
        if !listed.is_empty() {
            return Ok((
                None,
                Vec::new(),
                vec![format!(
                    "SCENARIO DIRECTORY {} holds {} test target(s) but package {} also compiles {}; \
                     name the package's scenario directory",
                    self.layout.dir.display(),
                    listed.len(),
                    self.layout.package,
                    missing.join(", ")
                )],
            ));
        }
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        for path in targets.values() {
            if let Some(parent) = path.parent() {
                dirs.insert(parent.to_path_buf());
            }
        }
        let dirs: Vec<PathBuf> = dirs.into_iter().collect();
        if dirs.len() != 1 {
            let shown: Vec<String> = dirs.iter().map(|dir| dir.display().to_string()).collect();
            return Ok((
                None,
                Vec::new(),
                vec![format!(
                    "SCENARIO DIRECTORY {} holds no `*.rs` test target and package {} compiles \
                     its targets under {} directories ({}); name the package's scenario directory",
                    self.layout.dir.display(),
                    self.layout.package,
                    dirs.len(),
                    shown.join(", ")
                )],
            ));
        }
        let note = format!(
            "note: scenario directory {} holds no `*.rs` test target; package {} compiles its \
             targets under {}, resolved from cargo's own target list",
            self.layout.dir.display(),
            self.layout.package,
            dirs[0].display()
        );
        Ok((Some(dirs[0].clone()), vec![note], Vec::new()))
    }

    // -- lane roles --------------------------------------------------------

    /// Cross-check the documented perf-loop lane roles against the classifier.
    pub fn check_lane_roles(&mut self) -> R<(BTreeMap<String, String>, Vec<String>)> {
        let (known, diagnostic) = match self.perf_loop_lanes() {
            Ok(found) => found,
            Err(message) => return Err(self.exit(message)),
        };
        let documented = self.lane_role_entries()?;
        let mut errors: Vec<String> = Vec::new();
        let documented_set: BTreeSet<String> = documented.keys().cloned().collect();
        let known_set: BTreeSet<String> = known.iter().cloned().collect();
        for lane in documented_set.difference(&known_set) {
            errors.push(format!(
                "gate-lane-roles names unknown --link-profile lane: {lane}"
            ));
        }
        for lane in known_set.difference(&documented_set) {
            errors.push(format!(
                "--link-profile lane missing from gate-lane-roles: {lane}"
            ));
        }
        for lane in known_set.intersection(&documented_set) {
            let expected = if diagnostic.contains(lane) {
                "diagnostic"
            } else {
                "verdict"
            };
            if documented[lane] != expected {
                errors.push(format!(
                    "lane role mismatch for {lane}: GATE.md says {}, \
                     perf_loop.lane_classification says {}",
                    repr_str(&documented[lane]),
                    repr_str(expected)
                ));
            }
        }
        if documented.get("hostile").map(String::as_str) != Some("diagnostic") {
            errors.push(
                "the hostile lane must be declared diagnostic-only: it returned \
                 not_ready (within_run_phase_not_stable) in all 70 recorded runs"
                    .to_string(),
            );
        }
        if !documented.values().any(|role| role == "verdict") {
            errors.push("no verdict lane is declared".to_string());
        }
        Ok((documented, errors))
    }

    fn lane_role_entries(&mut self) -> R<BTreeMap<String, String>> {
        let Some(block) = self.layout.manifest_block("gate-lane-roles") else {
            let manifest = self.layout.manifest.display().to_string();
            return Err(self.exit(format!("{manifest}: no ```gate-lane-roles block found")));
        };
        let mut roles: BTreeMap<String, String> = BTreeMap::new();
        for raw in block.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (lane, _, role) = partition(line, " = ");
            let lane = lane.trim().to_string();
            let role = role.trim().to_string();
            if !LANE_ROLES.contains(&role.as_str()) {
                let manifest = self.layout.manifest.display().to_string();
                return Err(self.exit(format!(
                    "{manifest}: lane {lane} has unknown role {}",
                    repr_str(&role)
                )));
            }
            if roles.contains_key(&lane) {
                let manifest = self.layout.manifest.display().to_string();
                return Err(self.exit(format!("{manifest}: duplicate lane entry {lane}")));
            }
            roles.insert(lane, role);
        }
        Ok(roles)
    }

    /// `(LINK_PROFILES, DIAGNOSTIC_LANES)` read out of `tools/perf_loop.py`.
    ///
    /// `perf_loop.py` stays the authority for a lane's role -- it is the
    /// function that stamps `link_role` into a run's `run.json` -- so the port
    /// reads the two declarations rather than restating them. The
    /// classifier's own one-line shape is checked too: if it stops being
    /// `profile in DIAGNOSTIC_LANES`, this refuses rather than deriving a role
    /// from a function it no longer understands.
    fn perf_loop_lanes(&self) -> Result<(Vec<String>, Vec<String>), String> {
        let path = self.layout.root.join("tools").join("perf_loop.py");
        let text = std::fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let known = extract_string_tuple(&text, "LINK_PROFILES").ok_or_else(|| {
            format!(
                "{} no longer declares a LINK_PROFILES tuple, so the lane roles cannot be derived",
                path.display()
            )
        })?;
        let diagnostic = extract_string_tuple(&text, "DIAGNOSTIC_LANES").ok_or_else(|| {
            format!(
                "{} no longer declares a DIAGNOSTIC_LANES tuple, so the lane roles cannot be derived",
                path.display()
            )
        })?;
        let expected = r#"return "diagnostic" if profile in DIAGNOSTIC_LANES else "verdict""#;
        if !text.contains(expected) {
            return Err(format!(
                "{}'s lane_classification is no longer the one-line `profile in \
                 DIAGNOSTIC_LANES` form this checker derives a lane's role from; \
                 update the Rust port rather than reading a stale role",
                path.display()
            ));
        }
        Ok((known, diagnostic))
    }
}

/// The first `name = (...)` tuple's string literals in a source text.
fn extract_string_tuple(text: &str, name: &str) -> Option<Vec<String>> {
    let start = text.find(&format!("{name} = ("))?;
    let open = text[start..].find('(')? + start;
    let close = text[open..].find(')')? + open;
    let body = &text[open + 1..close];
    let mut items: Vec<String> = Vec::new();
    for found in string_literal_re().find_iter(body) {
        if let Some(literal) = found.group(1) {
            items.push(literal);
        }
    }
    if items.is_empty() { None } else { Some(items) }
}

/// Python's `int(text)` for a decimal literal: whitespace and `_` between
/// digits are accepted, anything else is not.
pub fn py_int(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (sign, digits) = match trimmed.as_bytes()[0] {
        b'+' => (1i64, &trimmed[1..]),
        b'-' => (-1i64, &trimmed[1..]),
        _ => (1i64, trimmed),
    };
    if digits.is_empty() {
        return None;
    }
    let mut cleaned = String::new();
    let mut previous_digit = false;
    for character in digits.chars() {
        if character.is_ascii_digit() {
            cleaned.push(character);
            previous_digit = true;
        } else if character == '_' && previous_digit {
            previous_digit = false;
            cleaned.push(character);
        } else {
            return None;
        }
    }
    if !previous_digit {
        return None;
    }
    cleaned
        .replace('_', "")
        .parse::<i64>()
        .ok()
        .map(|value| sign * value)
}

/// Python's `str` rendering of an integer (`f"{n}"`).
pub fn number(value: i64) -> String {
    value.to_string()
}

/// Python's `format(value, spec)` for a float; a formatting failure is a
/// programming error, so it panics rather than printing something else.
pub fn fmt_float(value: f64, spec: &str) -> String {
    pyformat::format_float(value, spec).unwrap_or_else(|error| panic!("{spec}: {error}"))
}

/// Python's `sorted(...)` of a set of strings.
pub fn sorted<T: Ord>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut out: Vec<T> = items.into_iter().collect();
    out.sort();
    out
}

/// Set difference as a sorted `Vec`.
pub fn difference<T: Ord + Clone>(
    left: impl IntoIterator<Item = T>,
    right: impl IntoIterator<Item = T>,
) -> Vec<T> {
    let right: BTreeSet<T> = right.into_iter().collect();
    let mut out: Vec<T> = left
        .into_iter()
        .filter(|item| !right.contains(item))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// A `HashSet` read as a sorted `Vec`, for a diagnostic.
pub fn sorted_set<T: Ord + Clone>(items: &HashSet<T>) -> Vec<T> {
    sorted(items.iter().cloned())
}

/// The default family first, then the named families by name, matching the
/// Python checker's `(family is not None, family or "")` sort key.
pub fn sort_families(families: &[(Option<String>, String)]) -> Vec<(Option<String>, String)> {
    let mut out = families.to_vec();
    out.sort_by(|left, right| {
        (left.0.is_some(), left.0.clone().unwrap_or_default())
            .cmp(&(right.0.is_some(), right.0.clone().unwrap_or_default()))
    });
    out
}

// -- the command-line face and the run --------------------------------------

/// The flags of `check-gate`, converted from the binary's own clap struct.
#[derive(Debug, Clone, Default)]
pub struct Args {
    /// `--crate <ROOT> <PACKAGE> <DIR> <GATE_MD>`, or `None` for harness mode.
    pub crate_spec: Option<(PathBuf, String, PathBuf, PathBuf)>,
    /// `--mandate-check-json <PATH>`.
    pub mandate_check_json: Option<PathBuf>,
}

/// The absolute form of a path, canonicalised when it exists.
///
/// Python's `Path.resolve()` resolves symlinks, and its diagnostics name the
/// resolved path (`/private/var/...` on macOS for a `/var/...` root), so a
/// message that named the unresolved one would differ byte for byte. A path
/// that does not exist yet has nothing to resolve, and falls back to the
/// lexical absolute form.
fn absolute(path: &Path) -> PathBuf {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// Build the layout a run checks, or say why the arguments cannot name one.
pub fn build_layout(args: &Args) -> Result<Layout, String> {
    if let Some((root, package, dir, manifest)) = &args.crate_spec {
        let root = absolute(root);
        let dir = if dir.is_absolute() {
            dir.clone()
        } else {
            root.join(dir)
        };
        let manifest = if manifest.is_absolute() {
            manifest.clone()
        } else {
            root.join(manifest)
        };
        if !dir.is_dir() {
            return Err(format!(
                "scenario directory {} does not exist",
                dir.display()
            ));
        }
        if !manifest.is_file() {
            return Err(format!("manifest {} does not exist", manifest.display()));
        }
        let crates_root = absolute(root.parent().unwrap_or(&root));
        let manifest_text = std::fs::read_to_string(&manifest)
            .map_err(|error| format!("{} cannot be read: {error}", manifest.display()))?;
        let declared = declared_lib_package(&manifest_text)
            .map_err(|reason| format!("{}: {reason}", manifest.display()))?;
        let lib_package = declared.clone().unwrap_or_else(|| package.clone());
        let lib_root = if lib_package == *package {
            root.clone()
        } else {
            let member = root.join(&lib_package);
            if member.is_dir() {
                member
            } else {
                crates_root.join(&lib_package)
            }
        };
        return Ok(Layout {
            root,
            package: package.clone(),
            dir,
            manifest,
            crates_root,
            is_harness: false,
            manifest_text,
            lib_package,
            lib_root,
        });
    }
    let cwd = std::env::current_dir().map_err(|error| format!("cannot read cwd: {error}"))?;
    let root = absolute(&cwd);
    let manifest = root.join("tests").join("GATE.md");
    let manifest_text = std::fs::read_to_string(&manifest)
        .map_err(|error| format!("{} cannot be read: {error}", manifest.display()))?;
    let declared = declared_lib_package(&manifest_text)
        .map_err(|reason| format!("{}: {reason}", manifest.display()))?;
    let package = "tests".to_string();
    let lib_package = declared.unwrap_or_else(|| "netem-test".to_string());
    let lib_root = if lib_package == package {
        root.clone()
    } else {
        root.join(&lib_package)
    };
    let mut layout = harness_layout(&root);
    layout.manifest_text = manifest_text;
    layout.lib_package = lib_package;
    layout.lib_root = lib_root;
    Ok(layout)
}

/// Run the checker under a cargo seam, returning what it wrote and its exit.
pub fn run(mut session: Session<'_>, report_path: Option<PathBuf>) -> Outcome {
    match run_inner(&mut session, report_path) {
        Ok(()) => Outcome {
            stdout: session.out,
            stderr: session.err,
            exit: 0,
        },
        Err(Fatal) => Outcome {
            stdout: session.out,
            stderr: session.err,
            exit: 1,
        },
    }
}

fn run_inner(session: &mut Session<'_>, report_path: Option<PathBuf>) -> R<()> {
    let (resolved, notes, dir_problems) = session.scenario_dir_resolution()?;
    if let Some(dir) = resolved {
        session.layout.dir = dir;
    }
    for note in notes {
        session.print(note);
    }

    let manifest = session.manifest_entries()?;
    let mut targets: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&session.layout.dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "rs")
                && let Some(stem) = path.file_stem()
            {
                targets.push(stem.to_string_lossy().to_string());
            }
        }
    }
    targets.sort();
    let mut actual: BTreeSet<String> = BTreeSet::new();
    for target in &targets {
        actual.extend(session.ignored_scenarios(target)?);
    }
    let actual_lib = session.ignored_scenarios(LIB_TARGET)?;

    let package = session.layout.package.clone();
    let manifest_path = session.layout.manifest.display().to_string();
    for problem in dir_problems {
        session.print(problem);
        session.fail(format!(
            "the SCENARIO DIRECTORY line(s) above name a directory that is not the one \
             {package}'s test targets live in; pass it with --crate <ROOT> <PACKAGE> <DIR> \
             <GATE_MD>"
        ));
    }
    let manifest_names: BTreeSet<String> = manifest.keys().cloned().collect();
    let missing = difference(actual.iter().cloned(), manifest_names.iter().cloned());
    let stale = difference(
        manifest_names.iter().cloned(),
        actual
            .iter()
            .cloned()
            .chain(actual_lib.iter().cloned())
            .collect::<BTreeSet<String>>(),
    );
    if !missing.is_empty() || !stale.is_empty() {
        for name in &missing {
            session.print(format!("UNCLASSIFIED ignored scenario: {name}"));
        }
        for name in &stale {
            session.print(format!("STALE manifest entry (no longer ignored): {name}"));
        }
        session.fail(format!(
            "manifest has {} entries, binaries report {} ignored scenarios; update {manifest_path}",
            manifest.len(),
            actual.len()
        ));
    }

    let design_names: BTreeSet<String> = perf::parse_perf_design(
        &session
            .layout
            .manifest_block("gate-perf-design")
            .unwrap_or_default(),
        &mut Vec::new(),
    )
    .iter()
    .map(|row| row.name.clone())
    .collect();
    let undeclared_lib = difference(
        actual_lib.iter().cloned(),
        manifest_names
            .iter()
            .cloned()
            .chain(design_names.iter().cloned())
            .collect::<BTreeSet<String>>(),
    );
    for name in &undeclared_lib {
        session.print(format!(
            "note: unclassified ignored lib scenario {name}; a lib opt-in is recorded as a \
             `lib::<module>::<test> = <tier>` line of the ```gate-manifest block or as a \
             ```gate-perf-design row naming the reserved `lib` target (advisory, because the lib \
             target is a separate opt-in surface and a declaration written before it was nameable \
             must not fail for omitting one)"
        ));
    }

    let required = session.required_default_entries();
    for entry in &required {
        let (target, _, name) = partition(entry, "::");
        if target.is_empty() || name.is_empty() {
            session.print(format!("MALFORMED gate-default-required entry: {entry}"));
            session.fail(format!(
                "a gate-default-required entry is malformed; it names a scenario as \
                 `<target>::<test>` -- update {manifest_path}"
            ));
            continue;
        }
        let default = session.listed_scenarios(&target, false)?;
        let ignored = session.listed_scenarios(&target, true)?;
        let default: BTreeSet<String> = default.difference(&ignored).cloned().collect();
        if !default.contains(entry) {
            session.print(format!(
                "REQUIRED default scenario is not in the default tier (re-ignored or removed?): \
                 {entry}"
            ));
            session.fail(format!(
                "a scenario the gate marks as required no longer runs in the default tier; re-tier \
                 it or update {manifest_path}"
            ));
        }
    }

    let expected_asserting: BTreeSet<String> = manifest
        .iter()
        .filter(|(_, tier)| ASSERTING_TIERS.contains(&tier.as_str()))
        .map(|(name, _)| name.clone())
        .chain(required.iter().cloned())
        .collect();
    let recorded_asserting = session.asserting_entries()?;
    let recorded_set: BTreeSet<String> = recorded_asserting.iter().cloned().collect();
    if recorded_set.len() != recorded_asserting.len() {
        session.print("DUPLICATE entry in gate-asserting");
        session.fail(format!(
            "the ```gate-asserting block lists a scenario twice; update {manifest_path}"
        ));
    }
    for name in difference(
        expected_asserting.iter().cloned(),
        recorded_set.iter().cloned(),
    ) {
        session.print(format!(
            "ASSERTING scenario missing from gate-asserting: {name}"
        ));
        session.fail(format!(
            "a scenario that asserts a gate is missing from the ```gate-asserting block; update \
             {manifest_path}"
        ));
    }
    for name in difference(
        recorded_set.iter().cloned(),
        expected_asserting.iter().cloned(),
    ) {
        session.print(format!(
            "gate-asserting entry is not an asserting scenario (perf-tier or unknown): {name}"
        ));
        session.fail(format!(
            "the ```gate-asserting block lists a scenario that asserts nothing (perf-tier or \
             unknown); update {manifest_path}"
        ));
    }

    let mut manifest_by_name = manifest.clone();
    for (name, tier) in std::mem::take(&mut manifest_by_name) {
        if tier != "perf" {
            continue;
        }
        let (target, _, test) = partition(&name, "::");
        let bodies = scan::test_bodies(&target, &session.layout);
        if scan::body_asserts(&target, &test, &bodies) {
            let tokens: Vec<String> = found_tokens_of(&bodies, &target, &test);
            session.print(format!(
                "ASSERTING scenario in report-only perf tier (re-tier to standard/full/default \
                 or make it report-only): {name} [file {}, token(s): {}]",
                scan::target_source_label(&target, &session.layout),
                tokens.join(", ")
            ));
            session.fail(format!(
                "a scenario filed under the report-only `perf` tier asserts in its own body; make \
                 it report-only or re-tier it in {manifest_path}"
            ));
        }
    }

    let (derived_helpers, helper_tokens, unlocatable) =
        scan::helper_scan(&manifest, &session.layout);
    for name in &unlocatable {
        session.print(format!(
            "PERF scenario body not found in source (macro-generated or moved?): {name}"
        ));
        session.fail(format!(
            "a `perf` scenario in the manifest has no body in the source it names; update \
             {manifest_path}"
        ));
    }
    let recorded_helpers = session.recorded_perf_guard_helpers()?;
    for identity in difference(
        derived_helpers.keys().cloned(),
        recorded_helpers.keys().cloned(),
    ) {
        let count = derived_helpers.get(&identity).copied().unwrap_or(0);
        let tokens = helper_tokens.get(&identity).cloned().unwrap_or_default();
        session.print(format!(
            "PERF scenario reaches asserting helper not recorded as report-only: {identity} \
             ({count} assertion token(s): {})",
            tokens.join(", ")
        ));
        session.fail(format!(
            "a helper reachable from a `perf` scenario asserts without being recorded as a \
             report-only guard; update {manifest_path}"
        ));
    }
    for identity in difference(
        recorded_helpers.keys().cloned(),
        derived_helpers.keys().cloned(),
    ) {
        session.print(format!(
            "recorded perf guard helper is not reachable from any perf scenario (stale entry?): \
             {identity}"
        ));
        session.fail(format!(
            "a helper recorded as a report-only perf guard is no longer reachable from any `perf` \
             scenario; update {manifest_path}"
        ));
    }
    for identity in derived_helpers.keys() {
        if !recorded_helpers.contains_key(identity) {
            continue;
        }
        if derived_helpers[identity] != recorded_helpers[identity] as usize {
            session.print(format!(
                "perf guard helper assertion count changed for {identity}: recorded {}, found {}",
                recorded_helpers[identity], derived_helpers[identity]
            ));
            session.fail(format!(
                "the assertion count of the report-only perf guard {identity} changed; update \
                 {manifest_path}"
            ));
        }
    }

    let lane_roles = if session.layout.is_harness {
        let (roles, errors) = session.check_lane_roles()?;
        for error in errors {
            session.print(error);
            session.fail(format!(
                "the ```gate-lane-roles block does not match perf_loop.lane_classification; \
                 update it in {manifest_path}"
            ));
        }
        roles
    } else {
        BTreeMap::new()
    };

    let report = report_path.or_else(|| {
        let default = session.layout.root.join(DEFAULT_REPORT_NAME);
        default.is_file().then_some(default)
    });
    let (perf_problems, perf_summary, perf_notes) =
        session.check_perf_gate(&manifest, report.as_deref())?;
    for note in perf_notes {
        session.print(note);
    }
    for problem in &perf_problems {
        session.print(format!("PERF DECLARATION: {problem}"));
    }
    if !perf_problems.is_empty() {
        session.fail(format!(
            "the PERF DECLARATION line(s) above do not match the gate manifest or the measured \
             timings; the perf tests and their declarations are checked against {manifest_path}"
        ));
    }

    let mut env_problems: Vec<String> = Vec::new();
    let (env_summary, env_notes) = session.check_env_tier(&mut env_problems);
    for note in env_notes {
        session.print(note);
    }
    for problem in &env_problems {
        session.print(format!("ENV TIER: {problem}"));
    }
    if !env_problems.is_empty() {
        session.fail(format!(
            "the ENV TIER line(s) above name an env-scaled opt-in surface that does not match the \
             source; the surface is declared in the ```gate-env-tier block of {manifest_path}"
        ));
    }

    let doc_summary = if session.layout.is_harness {
        let (problems, summary) = session.check_doc_counts();
        for problem in &problems {
            session.print(problem);
        }
        if !problems.is_empty() {
            session.fail(
                "the DOC COUNT line(s) above name a documented count that no longer matches the \
                 command that determines it; update the document that states it",
            );
        }
        summary
    } else {
        Vec::new()
    };

    if session.bad {
        session.err.push('\n');
        for symptom in &session.symptoms {
            session.err.push_str(symptom);
            session.err.push('\n');
        }
        return Err(Fatal);
    }

    session.print(format!(
        "gate manifest OK: {} ignored scenarios classified",
        actual.len()
    ));
    let mut by_tier: BTreeMap<String, i64> = BTreeMap::new();
    for tier in manifest.values() {
        *by_tier.entry(tier.clone()).or_insert(0) += 1;
    }
    for (tier, count) in &by_tier {
        session.print(format!("  {tier}: {count}"));
    }
    if !actual_lib.is_empty() {
        session.print(format!(
            "  lib target: {} ignored scenario(s), {} named in gate-manifest or a \
             gate-perf-design row",
            actual_lib.len(),
            actual_lib.len() - undeclared_lib.len()
        ));
    }
    session.print(format!(
        "  default-required: {} asserting scenario(s) present",
        required.len()
    ));
    session.print(format!(
        "  gate-asserting: {} asserting scenario(s) recorded",
        expected_asserting.len()
    ));
    session.print(format!(
        "  gate-perf-guard-helpers: {} asserting helper(s) reachable from the perf tier",
        derived_helpers.len()
    ));
    if session.layout.is_harness {
        let diagnostic = lane_roles
            .values()
            .filter(|role| role.as_str() == "diagnostic")
            .count();
        session.print(format!(
            "  gate-lane-roles: {} perf-loop lane(s), {diagnostic} diagnostic-only",
            lane_roles.len()
        ));
    }
    for line in perf_summary {
        session.print(line);
    }
    for line in env_summary {
        session.print(line);
    }
    for line in doc_summary {
        session.print(line);
    }
    Ok(())
}

/// The assertion tokens of a report-only scenario's own body, sorted.
fn found_tokens_of(bodies: &BTreeMap<String, String>, target: &str, test: &str) -> Vec<String> {
    let body = bodies
        .get(&scan::lib_bare_name(target, test))
        .cloned()
        .unwrap_or_default();
    sorted(
        scan::found_tokens(&body)
            .into_iter()
            .collect::<BTreeSet<_>>(),
    )
}

/// The binary's entry point for `check-gate`: prints both streams and returns
/// the exit code.
pub fn main(args: Args) -> i32 {
    let layout = match build_layout(&args) {
        Ok(layout) => layout,
        Err(message) => {
            eprintln!("{message}");
            return 1;
        }
    };
    let cargo = SystemCargo;
    let session = Session::new(layout, &cargo);
    let outcome = run(session, args.mandate_check_json);
    print!("{}", outcome.stdout);
    eprint!("{}", outcome.stderr);
    outcome.exit
}
