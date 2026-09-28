//! Running the producers and the panel plotter.
//!
//! The runner shells out twice: to the producers (a `cargo test` invocation per
//! registry entry) and to `netem-tools mandate-plot`, whose `--json` summary it
//! parses. Both are bounded rather than trusted: a child that hangs is an
//! evidence failure to report, not a run to wait on forever.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::tools::json::{self, Json};
use crate::tools::pyjson::repr_str;
#[cfg(test)]
use std::collections::BTreeMap;

use super::producers::{absolute, workspace_root};
use super::value::py_round;
use super::{
    OUT_DIR_ENV, PLC, PLOT_BUILD_COMMAND, PLOT_TIMEOUT_SECONDS, PLOT_TOOL, PLOTS_DIRNAME,
    QUICK_ENV, TIMING_ENV, TIMING_ENV_VALUE,
};

/// What one producer's invocation did, and its output timeline.
#[derive(Debug, Clone)]
pub struct RunResult {
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub output: String,
    pub events: Vec<(f64, String)>,
}

/// Run one producer, returning its output, how it ended and its timeline.
pub fn run_producer(
    command: &[String],
    crate_dir: &Path,
    out_dir: &Path,
    quick: bool,
    timeout: f64,
) -> std::io::Result<RunResult> {
    let program = &command[0];
    let argv = &command[1..];
    // The child's stderr is merged into its stdout at the OS level, which is
    // what Python's `stderr=subprocess.STDOUT` did: a producer may print an arm
    // line on either stream, and the order the two arrive in is part of what
    // the runner attributes arms by. `exec` replaces the shell, so the child
    // the runner sees is the declared program with the declared argv.
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg("exec \"$0\" \"$@\" 2>&1")
        .arg(program)
        .args(argv)
        .current_dir(crate_dir)
        .env(OUT_DIR_ENV, out_dir)
        .env(TIMING_ENV, TIMING_ENV_VALUE)
        .env_remove(QUICK_ENV)
        .envs(if quick {
            vec![(QUICK_ENV, "1")]
        } else {
            vec![]
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .process_group_safe()
        .spawn()?;
    let started = Instant::now();
    let stdout = child.stdout.take().expect("stdout is piped");
    let (sender, receiver) = mpsc::channel::<(f64, String)>();
    let reader = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            match reader.read_until(b'\n', &mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let seconds = py_round(started.elapsed().as_secs_f64(), 3);
            let mut line = String::from_utf8_lossy(&buffer).into_owned();
            if line.ends_with('\n') {
                line.pop();
            }
            if line.ends_with('\r') {
                line.pop();
            }
            if sender.send((seconds, line)).is_err() {
                break;
            }
        }
    });
    let timed_out = wait_with_timeout(&mut child, timeout);
    let _ = reader.join();
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    let mut output = events
        .iter()
        .map(|(_seconds, line)| line.as_str())
        .collect::<Vec<&str>>()
        .join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    Ok(RunResult {
        exit_code: exit_code(&mut child),
        timed_out,
        output,
        events,
    })
}

/// `process_group(0)`, as the one call the child needs from the Unix process
/// API: the child gets its own group so a timeout kills the test binary and not
/// just the cargo that spawned it.
#[cfg(unix)]
trait ProcessGroupSafe {
    fn process_group_safe(&mut self) -> &mut Self;
}

#[cfg(unix)]
impl ProcessGroupSafe for Command {
    fn process_group_safe(&mut self) -> &mut Self {
        use std::os::unix::process::CommandExt;
        self.process_group(0)
    }
}

#[cfg(not(unix))]
trait ProcessGroupSafe {
    fn process_group_safe(&mut self) -> &mut Self;
}

#[cfg(not(unix))]
impl ProcessGroupSafe for Command {
    fn process_group_safe(&mut self) -> &mut Self {
        self
    }
}

/// Wait for the child, killing its whole process group on the deadline.
fn wait_with_timeout(child: &mut Child, timeout: f64) -> bool {
    let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(0.0));
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return false,
            Ok(None) => {}
            Err(_) => return false,
        }
        if Instant::now() >= deadline {
            kill_process_group(child);
            let _ = child.wait();
            return true;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

/// Python's `process.returncode`: a signalled child reports its signal negated.
fn exit_code(child: &mut Child) -> Option<i32> {
    let status = child.try_wait().ok().flatten()?;
    if let Some(code) = status.code() {
        return Some(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal().map(|signal| -signal)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Kill the child's process group, falling back to the child itself.
fn kill_process_group(child: &mut Child) {
    let pid = child.id();
    let killed = Command::new("/bin/kill")
        .args(["-9", &format!("-{pid}")])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if !killed {
        let _ = child.kill();
    }
}

/// A child's exit status and stdout, `None` on a failure or a timeout.
pub fn capture_with_timeout(command: &[String], cwd: &Path, timeout: f64) -> (Option<i32>, String) {
    let Some((program, argv)) = command.split_first() else {
        return (None, String::new());
    };
    let mut child = match Command::new(program)
        .args(argv)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group_safe()
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return (None, String::new()),
    };
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let reader = thread::spawn(move || {
        let mut text = String::new();
        let _ = stdout.read_to_string(&mut text);
        text
    });
    let timed_out = wait_with_timeout(&mut child, timeout);
    let text = reader.join().unwrap_or_default();
    if timed_out {
        return (None, String::new());
    }
    (exit_code(&mut child), text)
}

/// The built `netem-tools`, or a failure naming the command that builds it.
pub fn plot_binary() -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(directory) = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(std::path::Path::to_path_buf))
    {
        candidates.push(directory.join(PLOT_TOOL));
    }
    let root = workspace_root();
    candidates.push(root.join("target/release").join(PLOT_TOOL));
    candidates.push(root.join("target/debug").join(PLOT_TOOL));
    candidates.push(root.join("netem-test/target/release").join(PLOT_TOOL));
    candidates.push(root.join("netem-test/target/debug").join(PLOT_TOOL));
    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(format!(
        "the panel plotter {} is not built, so no panel can be rendered; build it \
         with `{PLOT_BUILD_COMMAND}` from {}",
        repr_str(PLOT_TOOL),
        root.display()
    ))
}

/// The plotter's own error sentence, out of its `mandate_plot: error:` line.
pub fn plot_error(stderr: &str) -> String {
    let text = stderr.trim();
    let prefix = "mandate_plot: error: ";
    if let Some(rest) = text.strip_prefix(prefix) {
        return rest.to_string();
    }
    if text.is_empty() {
        "the plotter exited non-zero without a message".to_string()
    } else {
        text.to_string()
    }
}

/// One child's captured output.
pub struct Captured {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

/// Run one child, capturing both streams on their own threads.
pub fn capture_both(command: &[String], timeout: f64) -> Result<Captured, std::io::Error> {
    let Some((program, argv)) = command.split_first() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty command",
        ));
    };
    let mut child = Command::new(program)
        .args(argv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group_safe()
        .spawn()?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let out_reader = read_thread(stdout);
    let err_reader = read_thread(stderr);
    let timed_out = wait_with_timeout(&mut child, timeout);
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    Ok(Captured {
        exit_code: exit_code(&mut child),
        stdout,
        stderr,
        timed_out,
    })
}

fn read_thread(mut stream: impl Read + Send + 'static) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut text = String::new();
        let _ = stream.read_to_string(&mut text);
        text
    })
}

/// Render one mandate's declared panels, returning `(summary, problems)`.
pub fn render_mandate(
    mandate: &str,
    out_dir: &Path,
    rasterize: bool,
    browser: Option<&str>,
    run_values: Option<&str>,
    run_censoring: Option<&str>,
    fault: Option<&str>,
) -> (Option<Json>, Vec<String>) {
    let declaration = out_dir.join(format!("{mandate}.json"));
    let data = out_dir.join(format!("{mandate}.csv"));
    let mut problems = Vec::new();
    for (path, kind) in [(&declaration, "declaration"), (&data, "data CSV")] {
        if !path.is_file() {
            problems.push(format!(
                "{mandate}: {kind} {} was not written by the smoke set (the producing \
                 test writes <mandate>.json and <mandate>.csv into ${OUT_DIR_ENV}={})",
                path.display(),
                out_dir.display()
            ));
        } else if path.metadata().map(|meta| meta.len()).unwrap_or(0) == 0 {
            problems.push(format!("{mandate}: {kind} {} is empty", path.display()));
        }
    }
    if !problems.is_empty() {
        return (None, problems);
    }
    let plotter = match plot_binary() {
        Ok(path) => path,
        Err(message) => return (None, vec![format!("{mandate}: {message}")]),
    };
    let mut command = vec![
        plotter.display().to_string(),
        "mandate-plot".to_string(),
        declaration.display().to_string(),
        "--out".to_string(),
        out_dir.join(PLOTS_DIRNAME).display().to_string(),
        "--json".to_string(),
    ];
    if !rasterize {
        command.push("--no-rasterize".to_string());
    }
    if let Some(browser) = browser {
        command.push("--browser".to_string());
        command.push(browser.to_string());
    }
    if let Some(values) = run_values {
        command.push("--run-values".to_string());
        command.push(values.to_string());
    }
    if let Some(censoring) = run_censoring {
        command.push("--run-censoring".to_string());
        command.push(censoring.to_string());
    }
    if let Some(fault) = fault {
        command.push("--fault".to_string());
        command.push(fault.to_string());
    }
    let captured = match capture_both(&command, PLOT_TIMEOUT_SECONDS) {
        Ok(captured) => captured,
        Err(error) => {
            return (
                None,
                vec![format!(
                    "{mandate}: the panel plotter could not be run: {error}"
                )],
            );
        }
    };
    if captured.timed_out {
        return (
            None,
            vec![format!(
                "{mandate}: the panel plotter did not finish within \
                 {PLOT_TIMEOUT_SECONDS:.0}s and was killed"
            )],
        );
    }
    if captured.exit_code != Some(0) {
        return (
            None,
            vec![format!("{mandate}: {}", plot_error(&captured.stderr))],
        );
    }
    match json::parse(&captured.stdout) {
        Ok(summary) => (Some(summary), Vec::new()),
        Err(error) => (
            None,
            vec![format!(
                "{mandate}: the panel plotter's --json summary could not be read: {error}"
            )],
        ),
    }
}

/// Every written panel must exist, be non-empty, carry series geometry, and
/// carry the summary of what it drew.
pub fn verify_plots(mandate: &str, summary: &Json) -> Vec<String> {
    let mut problems = Vec::new();
    let counts: Vec<i64> = summary
        .get("series_counts")
        .and_then(Json::as_array)
        .map(|items| items.iter().filter_map(as_int).collect())
        .unwrap_or_default();
    let panels = summary.get("panels").and_then(as_int);
    let svg = string_list(summary.get("svg"));
    if svg.is_empty() {
        problems.push(format!("{mandate}: rendering declared no panel"));
    }
    if Some(counts.len() as i64) != panels {
        problems.push(format!(
            "{mandate}: {} panel series count(s) for {} declared panel(s)",
            counts.len(),
            panels.unwrap_or(0)
        ));
    }
    for count in &counts {
        if *count <= 0 {
            problems.push(format!(
                "{mandate}: a rendered panel carries no series data"
            ));
        }
    }
    let summaries = summary
        .get("summaries")
        .and_then(Json::as_array)
        .map(<[Json]>::to_vec)
        .unwrap_or_default();
    if Some(summaries.len() as i64) != panels {
        problems.push(format!(
            "{mandate}: {} panel summary(ies) for {} declared panel(s); every panel \
             owes a summary of what it drew, so a run without one cannot report PASS",
            summaries.len(),
            panels.unwrap_or(0)
        ));
    }
    for document in &summaries {
        let named = document.get("panel").and_then(Json::as_str);
        let block = document.get("block").and_then(Json::as_str);
        if named.is_none_or(str::is_empty) || block.is_none_or(str::is_empty) {
            problems.push(format!(
                "{mandate}: a panel summary names no panel or carries no readable block"
            ));
        }
    }
    let mut written: Vec<String> = svg;
    written.extend(string_list(summary.get("png")));
    for path in written {
        let file = Path::new(&path);
        let present = file
            .metadata()
            .map(|meta| meta.is_file() && meta.len() > 0)
            .unwrap_or(false);
        if !present {
            problems.push(format!(
                "{mandate}: the plot {} is missing or empty",
                file.display()
            ));
        }
        let sidecar = file.with_extension("summary.txt");
        let sidecar_present = sidecar
            .metadata()
            .map(|meta| meta.is_file() && meta.len() > 0)
            .unwrap_or(false);
        if !sidecar_present {
            problems.push(format!(
                "{mandate}: the panel {} has no {} beside it, so its summary is not in \
                 the run's evidence",
                file.display(),
                sidecar
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_default()
            ));
        }
    }
    problems
}

fn as_int(value: &Json) -> Option<i64> {
    match value {
        Json::Int(number) => Some(*number),
        _ => None,
    }
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

/// The last `lines` non-empty lines of a child's output.
pub fn log_tail(text: &str, lines: usize) -> Vec<String> {
    let stripped: Vec<String> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_string)
        .collect();
    let start = stripped.len().saturating_sub(lines);
    stripped[start..].to_vec()
}

/// An absolute form of the run directory, for the report.
pub fn resolve_out_dir(path: &Path) -> PathBuf {
    absolute(path)
}

/// The literal a mis-shaped `--producer-path` is refused with.
pub const fn placeholder() -> &'static str {
    PLC
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "mandate-exec-{}-{label}-{unique}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create");
        dir
    }

    /// A summary of one panel whose SVG and sidecar are on disk.
    fn summary_for(svg: &Path) -> Json {
        let mut object = BTreeMap::new();
        object.insert("panels".to_string(), Json::Int(1));
        object.insert("series_counts".to_string(), Json::Array(vec![Json::Int(1)]));
        object.insert(
            "svg".to_string(),
            Json::Array(vec![Json::Str(svg.display().to_string())]),
        );
        object.insert("png".to_string(), Json::Array(Vec::new()));
        let mut panel = BTreeMap::new();
        panel.insert("panel".to_string(), Json::Str("latency".to_string()));
        panel.insert("block".to_string(), Json::Str("panel latency".to_string()));
        object.insert(
            "summaries".to_string(),
            Json::Array(vec![Json::Object(panel)]),
        );
        Json::Object(object)
    }

    #[test]
    fn a_panel_without_its_summary_is_an_evidence_failure() {
        let dir = temp("summary");
        let svg = dir.join("M1-latency.svg");
        std::fs::write(&svg, "<svg/>").expect("svg");
        std::fs::write(dir.join("M1-latency.summary.txt"), "panel latency").expect("sidecar");
        let mut summary = summary_for(&svg);
        // A summary-less panel cannot report PASS, because then the only
        // statement of what a panel shows is the render.
        if let Some(object) = summary.as_object_mut() {
            object.insert("summaries".to_string(), Json::Array(Vec::new()));
        }
        let problems = verify_plots("M1", &summary);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("panel summary")),
            "{problems:?}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_panel_without_its_summary_sidecar_is_an_evidence_failure() {
        let dir = temp("sidecar");
        let svg = dir.join("M1-latency.svg");
        std::fs::write(&svg, "<svg/>").expect("svg");
        let summary = summary_for(&svg);
        let problems = verify_plots("M1", &summary);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("has no M1-latency.summary.txt beside it")),
            "{problems:?}"
        );
        // The sidecar present is the green state on the same rule.
        std::fs::write(dir.join("M1-latency.summary.txt"), "panel latency").expect("sidecar");
        assert_eq!(verify_plots("M1", &summary), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_panel_that_reached_the_disk_empty_is_an_evidence_failure() {
        let dir = temp("empty");
        let svg = dir.join("M1-latency.svg");
        std::fs::write(&svg, "").expect("svg");
        std::fs::write(dir.join("M1-latency.summary.txt"), "panel latency").expect("sidecar");
        let summary = summary_for(&svg);
        let problems = verify_plots("M1", &summary);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("missing or empty")),
            "{problems:?}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_panel_with_no_series_is_an_evidence_failure() {
        let dir = temp("noseries");
        let svg = dir.join("M1-latency.svg");
        std::fs::write(&svg, "<svg/>").expect("svg");
        std::fs::write(dir.join("M1-latency.summary.txt"), "panel latency").expect("sidecar");
        let mut summary = summary_for(&svg);
        if let Some(object) = summary.as_object_mut() {
            object.insert("series_counts".to_string(), Json::Array(vec![Json::Int(0)]));
        }
        let problems = verify_plots("M1", &summary);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("no series data")),
            "{problems:?}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_log_tail_keeps_the_last_non_empty_lines() {
        let text = "a\n\nb\n\nc\n";
        assert_eq!(log_tail(text, 2), vec!["b".to_string(), "c".to_string()]);
        assert_eq!(
            log_tail(text, 20),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(log_tail("", 20).is_empty());
    }
}
