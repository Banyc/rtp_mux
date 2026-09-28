//! The wrapper's history step: archive the run and compare it to the previous
//! one, then let an M1 degradation fail the invocation.
//!
//! The battery's own verdict is the first word: this step *adds* a rejection, it
//! never explains one away. The `perf-history` bin does the archive and the
//! comparison (it reuses the `mandate-compare` module in-process), so the runner
//! invokes it rather than re-implementing it. A bin that is not built is a
//! warning, not a silent skip: the numbers simply do not survive the temp tree,
//! and the run's own verdict still stands.

use std::path::{Path, PathBuf};
use std::process::Command;

use super::producers::workspace_root;

/// The `perf-history` binary, wherever the build put it.
fn find_binary() -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(directory) = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(std::path::Path::to_path_buf))
    {
        candidates.push(directory.join("perf-history"));
    }
    let root = workspace_root();
    candidates.push(root.join("target/release/perf-history"));
    candidates.push(root.join("netem-test/target/release/perf-history"));
    candidates.into_iter().find(|candidate| candidate.is_file())
}

/// Archive, compare and report. Returns the exit status this step implies.
pub fn history(run_dir: &Path, label: Option<&str>) -> i32 {
    let Some(binary) = find_binary() else {
        eprintln!(
            "mandate-check: warning: perf-history is not built, so this run was not \
             archived and not compared to the previous one (build it: cargo build \
             --release -p rtp_mux --features perf --bin perf-history)"
        );
        return 0;
    };
    let mut command = Command::new(&binary);
    command.arg(run_dir);
    if let Some(label) = label {
        command.arg("--label").arg(label);
    }
    match command.status() {
        Ok(status) => status.code().unwrap_or(0),
        Err(error) => {
            eprintln!("mandate-check: warning: could not run perf-history: {error}");
            0
        }
    }
}
