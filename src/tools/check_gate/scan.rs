//! The gate checker's source scanners: what a scenario's own body is, which
//! crate-local helpers it can reach, and where a test name lives.
//!
//! Every scan here is regex plus brace counting, not a Rust parser, and it is
//! shared between the direct body scan and the crate-local call graph so both
//! agree on what a function body is. The port keeps the ported checker's
//! *decisions* and its *spelling*: a diagnostic that names a file, a function
//! or a token names the same one here.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use super::{LIB_TARGET, Layout, SKIP_DIRS};
use crate::tools::pyre::{Flags, Regex};

// -- the patterns, spelled as the Python checker spells them -----------------

/// `debug_assert_ne!|debug_assert_eq!|debug_assert!|assert_ne!|assert_eq!|assert!|panic!|unreachable!`
pub fn assertion_tokens() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| regex(r"(debug_assert_ne!|debug_assert_eq!|debug_assert!|assert_ne!|assert_eq!|assert!|panic!|unreachable!)"))
}

/// `\b(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:const\s+)?fn\s+([A-Za-z0-9_]+)\s*(?:<[^>]*>)?\s*\(`
fn fn_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"\b(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:const\s+)?fn\s+([A-Za-z0-9_]+)\s*(?:<[^>]*>)?\s*\(",
            false,
        )
        .expect("the function pattern compiles")
    })
}

/// `([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)\s*\(`
fn call_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new(
            r"([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)\s*\(",
            false,
        )
        .expect("the call pattern compiles")
    })
}

/// `^\s*(?:pub(?:\([^)]*\))?\s+)?use\s+(.+?);` with `re.S | re.M`.
fn use_re() -> &'static Regex {
    static ONCE: OnceLock<Regex> = OnceLock::new();
    ONCE.get_or_init(|| {
        Regex::new_with_flags(
            r"^\s*(?:pub(?:\([^)]*\))?\s+)?use\s+(.+?);",
            Flags {
                dotall: true,
                multiline: true,
                ignorecase: false,
            },
        )
        .expect("the use pattern compiles")
    })
}

fn regex(pattern: &str) -> Regex {
    Regex::new(pattern, false).unwrap_or_else(|error| panic!("{pattern}: {error}"))
}

// -- function bodies ---------------------------------------------------------

/// Map each `fn NAME` in `text` to its brace-balanced body (first definition
/// wins, matching `bodies.setdefault`).
pub fn function_bodies(text: &str) -> BTreeMap<String, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut bodies: BTreeMap<String, String> = BTreeMap::new();
    for found in fn_re().find_iter(text) {
        let Some(name) = found.group(1) else { continue };
        let Some(open) = chars[found.end..].iter().position(|c| *c == '{') else {
            continue;
        };
        let start = found.end + open;
        let end = brace_end(&chars, start);
        bodies
            .entry(name)
            .or_insert_with(|| chars[start..=end].iter().collect());
    }
    bodies
}

/// A top-level `fn NAME` of a target's sources, with its body.
#[derive(Debug, Clone)]
pub struct SourceFunction {
    pub identity: String,
    pub module: String,
    pub name: String,
    pub body: String,
}

/// The body a target test's source declares, keyed by the `fn` name a `lib`
/// entry states as its last segment.
pub fn test_bodies(target: &str, layout: &Layout) -> BTreeMap<String, String> {
    if target == LIB_TARGET {
        let mut bodies: BTreeMap<String, String> = BTreeMap::new();
        for path in lib_source_files(layout) {
            if let Ok(text) = std::fs::read_to_string(&path) {
                for (name, body) in function_bodies(&text) {
                    bodies.insert(name, body);
                }
            }
        }
        return bodies;
    }
    let mut bodies = BTreeMap::new();
    if let Ok(text) = std::fs::read_to_string(layout.dir.join(format!("{target}.rs"))) {
        bodies = function_bodies(&text);
    }
    bodies
}

/// The assertion tokens in `body`, in source order.
pub fn found_tokens(body: &str) -> Vec<String> {
    assertion_tokens()
        .find_iter(body)
        .into_iter()
        .filter_map(|found| found.group(1))
        .collect()
}

/// A target test name's bare `fn` name: its last `::` segment for `lib`.
pub fn lib_bare_name(target: &str, test: &str) -> String {
    if target == LIB_TARGET {
        test.rsplit("::").next().unwrap_or(test).to_string()
    } else {
        test.to_string()
    }
}

/// True when the test function's own body contains an assertion token.
pub fn body_asserts(target: &str, name: &str, bodies: &BTreeMap<String, String>) -> bool {
    match bodies.get(&lib_bare_name(target, name)) {
        Some(body) => !body.is_empty() && assertion_tokens().is_match(body),
        None => false,
    }
}

/// The source a target's tests live in, for a diagnostic.
pub fn target_source_label(target: &str, layout: &Layout) -> String {
    if target == LIB_TARGET {
        return layout.lib_root.join("src").display().to_string();
    }
    layout
        .dir
        .join(format!("{target}.rs"))
        .display()
        .to_string()
}

// -- source identity and module paths ----------------------------------------

/// Rust module path of a source file, or `None` when it is not scanned.
pub fn source_module(path: &Path, layout: &Layout) -> Option<String> {
    match path.strip_prefix(&layout.dir) {
        Ok(rest) => {
            let parts: Vec<_> = rest.components().collect();
            if parts.len() == 1 {
                Some(String::new())
            } else {
                None
            }
        }
        Err(_) => {
            for (base, prefix) in layout.kit_source_dirs() {
                let Ok(relative) = path.strip_prefix(&base) else {
                    continue;
                };
                let parts: Vec<_> = relative.components().collect();
                if parts.len() != 1 {
                    return None;
                }
                let name = parts[0].as_os_str().to_string_lossy();
                let stem = name.strip_suffix(".rs").unwrap_or(&name);
                return Some(if stem == "mod" {
                    prefix
                } else {
                    format!("{prefix}::{stem}")
                });
            }
            if is_lib_source(path, layout) {
                return Some(lib_module_name(path, layout));
            }
            None
        }
    }
}

/// Whether `path` is a `.rs` file of the package's `--lib` target tree.
pub fn is_lib_source(path: &Path, layout: &Layout) -> bool {
    let root = layout.lib_root.join("src");
    match path.strip_prefix(&root) {
        Ok(relative) => {
            let parts: Vec<_> = relative.components().collect();
            !parts.is_empty()
                && parts
                    .last()
                    .is_some_and(|part| part.as_os_str().to_string_lossy().ends_with(".rs"))
        }
        Err(_) => false,
    }
}

/// The Rust module path of a lib source file, from its own path.
fn lib_module_name(path: &Path, layout: &Layout) -> String {
    let root = layout.lib_root.join("src");
    let relative = path.strip_prefix(&root).expect("a lib source is under it");
    let parts: Vec<String> = relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy().to_string())
        .collect();
    let name = parts.last().expect("a file has a name");
    let stem = name.strip_suffix(".rs").unwrap_or(name);
    let directory = &parts[..parts.len() - 1];
    let mut segments: Vec<String> = directory.to_vec();
    if stem != "lib" && stem != "mod" {
        segments.push(stem.to_string());
    }
    segments.join("::")
}

/// Every `.rs` file shipped into the package's `--lib` test target.
pub fn lib_source_files(layout: &Layout) -> Vec<PathBuf> {
    let root = layout.lib_root.join("src");
    if !root.is_dir() {
        return Vec::new();
    }
    let mut found = Vec::new();
    collect_recursive(&root, &mut found);
    found.retain(|path| path.extension().is_some_and(|ext| ext == "rs"));
    found.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    found
}

/// Every file under `root`, depth-first, in the order Python's `Path.rglob`
/// yields (a directory's entries as the OS lists them, then each subdirectory).
fn collect_recursive(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut directories: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        out.push(path.clone());
        if path.is_dir() {
            directories.push(path);
        }
    }
    directories.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    for directory in directories {
        collect_recursive(&directory, out);
    }
}

/// The identity of the lib source function `test` defines, if exactly one.
pub fn lib_test_identity(test: &str, layout: &Layout) -> Option<String> {
    let name = lib_bare_name(LIB_TARGET, test);
    let mut found: Vec<String> = Vec::new();
    for path in lib_source_files(layout) {
        for function in parse_functions(&path, layout) {
            if function.name == name {
                found.push(function.identity);
            }
        }
    }
    if found.len() == 1 {
        Some(found.remove(0))
    } else {
        None
    }
}

/// Stable identity prefix for a scanned source file.
pub fn source_identity(path: &Path, layout: &Layout) -> String {
    if layout.is_harness
        && let Ok(relative) = path.strip_prefix(layout.root.join(&layout.package))
    {
        return relative.display().to_string();
    }
    if path.parent() == Some(layout.dir.as_path()) {
        return path
            .strip_prefix(&layout.root)
            .map(|relative| relative.display().to_string())
            .unwrap_or_else(|_| path.display().to_string());
    }
    if let Some(own) = layout.own_kit_dir()
        && path.parent() == Some(own.as_path())
    {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        return format!("{}/src/testkit/{name}", layout.package);
    }
    if is_lib_source(path, layout) {
        return path
            .strip_prefix(&layout.root)
            .map(|relative| relative.display().to_string())
            .unwrap_or_else(|_| path.display().to_string());
    }
    path.strip_prefix(&layout.crates_root)
        .map(|relative| relative.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

/// Crate-local functions in `path` with brace-balanced bodies.
pub fn parse_functions(path: &Path, layout: &Layout) -> Vec<SourceFunction> {
    let Some(module) = source_module(path, layout) else {
        return Vec::new();
    };
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut seen: HashSet<String> = HashSet::new();
    let mut functions = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    for found in fn_re().find_iter(&text) {
        let Some(name) = found.group(1) else { continue };
        let Some(open) = chars[found.end..].iter().position(|c| *c == '{') else {
            continue;
        };
        let start = found.end + open;
        let end = brace_end(&chars, start);
        let identity = format!("{}::{name}", source_identity(path, layout));
        if !seen.insert(identity.clone()) {
            continue;
        }
        functions.push(SourceFunction {
            identity,
            module: module.clone(),
            name,
            body: chars[start..=end].iter().collect(),
        });
    }
    functions
}

// -- imports and the call graph ----------------------------------------------

/// Bare `use` first segments that name external crate roots in the scanned
/// sources.
const EXTERNAL_ROOT_STEMS: [&str; 6] = ["netem_test", "rtp", "mux", "rtp_mux", "tokio", "std"];

/// Resolve a use-path prefix to a crate-root-absolute module path.
pub fn normalize_module(base: &str, current_module: &str) -> String {
    let segments: Vec<&str> = base.split("::").filter(|part| !part.is_empty()).collect();
    if segments.is_empty() {
        return current_module.to_string();
    }
    if segments[0] == "crate" {
        return segments[1..].join("::");
    }
    if EXTERNAL_ROOT_STEMS.contains(&segments[0]) {
        return segments.join("::");
    }
    let mut current: Vec<&str> = if current_module.is_empty() {
        Vec::new()
    } else {
        current_module.split("::").collect()
    };
    let mut index = 0;
    while index < segments.len() && (segments[index] == "super" || segments[index] == "self") {
        if segments[index] == "super" && !current.is_empty() {
            current.pop();
        }
        index += 1;
    }
    let mut combined: Vec<&str> = current;
    combined.extend_from_slice(&segments[index..]);
    combined.join("::")
}

/// Map each imported bare name to its module, plus `super::*`-style globs.
pub fn parse_imports(text: &str, current_module: &str) -> (BTreeMap<String, String>, Vec<String>) {
    let mut imports: BTreeMap<String, String> = BTreeMap::new();
    let mut globs: Vec<String> = Vec::new();
    for found in use_re().find_iter(text) {
        let Some(statement) = found.group(1) else {
            continue;
        };
        let statement = statement.trim();
        let (inner, mut base_module): (String, String) = if statement.contains('{') {
            let (base, rest) = statement.split_once('{').expect("it contains one");
            let inner = rest.rsplit_once('}').map(|(head, _)| head).unwrap_or(rest);
            (
                inner.to_string(),
                normalize_module(base.trim(), current_module),
            )
        } else {
            (statement.to_string(), String::new())
        };
        for item in inner.split(',') {
            let item = item.split(" as ").next().unwrap_or("").trim();
            if item.is_empty() {
                continue;
            }
            if item == "*" || item.ends_with("::*") {
                if base_module.is_empty() {
                    let stem = item[..item.len() - 3].trim();
                    base_module = normalize_module(stem, current_module);
                }
                if !base_module.is_empty() {
                    globs.push(base_module.clone());
                }
                continue;
            }
            if !base_module.is_empty() {
                imports.insert(item.to_string(), base_module.clone());
                continue;
            }
            let segments: Vec<&str> = item.split("::").collect();
            if segments.len() < 2 {
                continue;
            }
            let name = segments[segments.len() - 1];
            let module =
                normalize_module(&segments[..segments.len() - 1].join("::"), current_module);
            imports.insert(name.to_string(), module);
        }
    }
    (imports, globs)
}

/// Source files compiled into the `<dir>/<target>.rs` integration target,
/// including the kit sources behind the direct imports.
pub fn target_source_files(target: &str, layout: &Layout) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = if target == LIB_TARGET {
        lib_source_files(layout)
    } else {
        vec![layout.dir.join(format!("{target}.rs"))]
    };
    for (kit_dir, _) in layout.kit_source_dirs() {
        if let Ok(entries) = std::fs::read_dir(&kit_dir) {
            let mut kit: Vec<PathBuf> = entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
                .collect();
            kit.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
            files.extend(kit);
        }
    }
    let mut seen: BTreeSet<PathBuf> = BTreeSet::new();
    let mut unique = Vec::new();
    for path in files {
        if seen.contains(&path) || !path.exists() {
            continue;
        }
        seen.insert(path.clone());
        unique.push(path);
    }
    unique
}

/// One integration target's crate-local functions.
pub fn target_functions(paths: &[PathBuf], layout: &Layout) -> Vec<SourceFunction> {
    let mut functions = Vec::new();
    for path in paths {
        functions.extend(parse_functions(path, layout));
    }
    functions
}

/// Crate-local call graph for one integration target.
pub struct TargetGraph {
    pub functions: BTreeMap<String, SourceFunction>,
    by_name: BTreeMap<String, Vec<String>>,
    by_module_name: BTreeMap<(String, String), Vec<String>>,
    imports: HashMap<String, BTreeMap<String, String>>,
    globs: HashMap<String, Vec<String>>,
}

impl TargetGraph {
    pub fn new(functions: Vec<SourceFunction>, paths: &[PathBuf], layout: &Layout) -> Self {
        let mut graph = TargetGraph {
            functions: BTreeMap::new(),
            by_name: BTreeMap::new(),
            by_module_name: BTreeMap::new(),
            imports: HashMap::new(),
            globs: HashMap::new(),
        };
        for function in functions {
            graph
                .by_name
                .entry(function.name.clone())
                .or_default()
                .push(function.identity.clone());
            graph
                .by_module_name
                .entry((function.module.clone(), function.name.clone()))
                .or_default()
                .push(function.identity.clone());
            graph.functions.insert(function.identity.clone(), function);
        }
        for path in paths {
            let Some(module) = source_module(path, layout) else {
                continue;
            };
            let Ok(text) = std::fs::read_to_string(path) else {
                continue;
            };
            let (imports, globs) = parse_imports(&text, &module);
            graph
                .imports
                .entry(module.clone())
                .or_default()
                .extend(imports);
            graph.globs.entry(module).or_default().extend(globs);
        }
        graph
    }

    /// Candidate identities for a call written as `path(` inside `module`.
    pub fn resolve(&self, module: &str, path: &str) -> Vec<String> {
        let name = path.rsplit("::").next().unwrap_or(path);
        if path.contains("::") {
            let head = path.rsplit_once("::").map(|(head, _)| head).unwrap_or("");
            let target_module = normalize_module(head, module);
            let found = self
                .by_module_name
                .get(&(target_module.clone(), name.to_string()));
            if let Some(found) = found {
                return found.clone();
            }
            let found = self.through_views(&target_module, name, &mut BTreeSet::new());
            if !found.is_empty() {
                return found;
            }
        }
        if let Some(found) = self
            .by_module_name
            .get(&(module.to_string(), name.to_string()))
        {
            return found.clone();
        }
        if let Some(imported) = self.imports.get(module).and_then(|map| map.get(name)) {
            let found = self
                .by_module_name
                .get(&(imported.clone(), name.to_string()))
                .cloned()
                .unwrap_or_default();
            if !found.is_empty() {
                return found;
            }
            let found = self.through_views(imported, name, &mut BTreeSet::new());
            if !found.is_empty() {
                return found;
            }
        }
        for glob in self.globs.get(module).map(Vec::as_slice).unwrap_or(&[]) {
            if let Some(found) = self.by_module_name.get(&(glob.clone(), name.to_string())) {
                return found.clone();
            }
            let found = self.through_views(glob, name, &mut BTreeSet::new());
            if !found.is_empty() {
                return found;
            }
        }
        self.by_name.get(name).cloned().unwrap_or_default()
    }

    /// Resolve `name` visible in `module` through its re-export views.
    fn through_views(&self, module: &str, name: &str, seen: &mut BTreeSet<String>) -> Vec<String> {
        if !seen.insert(module.to_string()) {
            return Vec::new();
        }
        if let Some(imported) = self.imports.get(module).and_then(|map| map.get(name)) {
            let found = self
                .by_module_name
                .get(&(imported.clone(), name.to_string()))
                .cloned()
                .unwrap_or_default();
            if !found.is_empty() {
                return found;
            }
            let found = self.through_views(imported, name, seen);
            if !found.is_empty() {
                return found;
            }
        }
        for glob in self.globs.get(module).map(Vec::as_slice).unwrap_or(&[]) {
            if let Some(found) = self.by_module_name.get(&(glob.clone(), name.to_string())) {
                return found.clone();
            }
            let found = self.through_views(glob, name, seen);
            if !found.is_empty() {
                return found;
            }
        }
        Vec::new()
    }

    /// Every crate-local function reachable from `seeds`.
    pub fn reachable(&self, seeds: &[String]) -> BTreeSet<String> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = seeds.to_vec();
        while let Some(identity) = stack.pop() {
            let Some(function) = self.functions.get(&identity) else {
                continue;
            };
            if !seen.insert(identity.clone()) {
                continue;
            }
            for found in call_re().find_iter(&function.body) {
                let Some(call) = found.group(1) else { continue };
                for callee in self.resolve(&function.module, &call) {
                    if !seen.contains(&callee) {
                        stack.push(callee);
                    }
                }
            }
        }
        seen
    }
}

/// Asserting crate functions reachable from the perf tier, with token counts.
pub type HelperScan = (
    BTreeMap<String, usize>,
    BTreeMap<String, Vec<String>>,
    Vec<String>,
);

pub fn helper_scan(manifest: &BTreeMap<String, String>, layout: &Layout) -> HelperScan {
    let mut perf_by_target: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, tier) in manifest {
        if tier == "perf" {
            let (target, _, test) = partition(name, "::");
            perf_by_target
                .entry(target.to_string())
                .or_default()
                .push(test.to_string());
        }
    }
    let mut reachable: BTreeMap<String, usize> = BTreeMap::new();
    let mut tokens: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut unlocatable: Vec<String> = Vec::new();
    for (target, tests) in perf_by_target {
        let paths = target_source_files(&target, layout);
        let graph = TargetGraph::new(target_functions(&paths, layout), &paths, layout);
        let mut seeds: Vec<String> = Vec::new();
        for test in &tests {
            let identity = if target == LIB_TARGET {
                lib_test_identity(test, layout)
            } else {
                Some(format!(
                    "{}::{test}",
                    source_identity(&layout.dir.join(format!("{target}.rs")), layout)
                ))
            };
            match identity {
                Some(identity) if graph.functions.contains_key(&identity) => seeds.push(identity),
                _ => unlocatable.push(format!("{target}::{test}")),
            }
        }
        let seed_set: BTreeSet<String> = seeds.iter().cloned().collect();
        for identity in graph.reachable(&seeds) {
            if seed_set.contains(&identity) {
                continue;
            }
            let body = &graph.functions[&identity].body;
            let count = found_tokens(body).len();
            if count > 0 {
                let entry = reachable.entry(identity.clone()).or_insert(0);
                *entry = (*entry).max(count);
                let found = found_tokens(body);
                if found.len() > tokens.get(&identity).map(Vec::len).unwrap_or(0) {
                    tokens.insert(identity, found);
                }
            }
        }
    }
    (reachable, tokens, unlocatable)
}

// -- small shared scanners ---------------------------------------------------

/// The index of the `}` closing the `{` at `open_index` (character offsets).
pub fn brace_end(chars: &[char], open_index: usize) -> usize {
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

/// Python's `str.partition(separator)`: `(head, separator, tail)`.
pub fn partition(text: &str, separator: &str) -> (String, String, String) {
    match text.find(separator) {
        Some(index) => (
            text[..index].to_string(),
            separator.to_string(),
            text[index + separator.len()..].to_string(),
        ),
        None => (text.to_string(), String::new(), String::new()),
    }
}

/// The crate's own scripts: the only place a runner can set a variable.
pub fn crate_scripts(root: &Path) -> Vec<PathBuf> {
    const RUNNER_SUFFIXES: [&str; 9] = [
        "py", "nu", "sh", "bash", "command", "bat", "ps1", "js", "ts",
    ];
    let mut all = Vec::new();
    collect_recursive(root, &mut all);
    let mut scripts: Vec<PathBuf> = all
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .is_some_and(|ext| RUNNER_SUFFIXES.contains(&ext.to_string_lossy().as_ref()))
                && !path
                    .strip_prefix(root)
                    .map(|relative| {
                        relative.components().any(|part| {
                            SKIP_DIRS.contains(&part.as_os_str().to_string_lossy().as_ref())
                        })
                    })
                    .unwrap_or(false)
        })
        .collect();
    scripts.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    scripts
}

/// The environment-variable-shaped string literals a script names.
pub fn script_env_names(script: &Path) -> BTreeSet<String> {
    let Some(text) = read_scanned(script) else {
        return BTreeSet::new();
    };
    let mut names = BTreeSet::new();
    for found in super::string_literal_re().find_iter(&text) {
        if let Some(literal) = found.group(1)
            && super::env_name_re().is_match(&literal)
        {
            names.insert(literal);
        }
    }
    names
}

/// A file's text, or `None` when it is too large to be a scanned source.
pub fn read_scanned(path: &Path) -> Option<String> {
    let metadata = std::fs::metadata(path).ok()?;
    if metadata.len() > super::MAX_SCANNED_BYTES {
        return None;
    }
    std::fs::read_to_string(path).ok()
}

/// Every `.rs` file under `root`, depth-first, skipping version-control and
/// build directories.
pub fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut all = Vec::new();
    collect_recursive(root, &mut all);
    let mut sources: Vec<PathBuf> = all
        .into_iter()
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "rs")
                && !path
                    .strip_prefix(root)
                    .map(|relative| {
                        relative.components().any(|part| {
                            SKIP_DIRS.contains(&part.as_os_str().to_string_lossy().as_ref())
                        })
                    })
                    .unwrap_or(false)
        })
        .collect();
    sources.sort_by(|a, b| a.as_os_str().cmp(b.as_os_str()));
    sources
}
