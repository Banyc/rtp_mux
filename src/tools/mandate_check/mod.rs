//! `mandate-check` — run the tri-mandate performance smoke set and keep proof.
//!
//! This is the runner everything hangs off. It runs the producers the registry
//! declares (`tools/mandate-producers.json`), renders each mandate's panels
//! through the `netem-tools mandate-plot` subcommand, prints one verdict line
//! per mandate, and writes `mandate-check.json` into the run directory so a
//! reader can verify from a machine, not from prose, that the mandated checks
//! ran and what they measured.
//!
//! The producer contract it enforces — the target and its invocation, the
//! evidence files, the `MANDATE` verdict grammar, `--quick`, and the per-arm
//! measurement lines — is stated as the form a crate's author follows in
//! `tools/MANDATE_SMOKE.md`. The runner's own docstring is the ported one; the
//! ports keep their twin's wording so a diagnostic a reader has seen stays the
//! diagnostic they see.
//!
//! ## The producers
//!
//! A run records the arms of every **producer** it selects. `--producer <id>`
//! selects one or more, and with none named **every** declared producer runs.
//! `--producer-path <id>=<path>` points one producer at another checkout;
//! no flag names a crate, because which crates exist is the registry's business.
//!
//! ## What it writes
//!
//! Into `--dir` (default: a fresh directory beneath `$TMPDIR`): each producer's
//! log, the verified panels under `plots/` (each with its mandatory
//! `.summary.txt` sidecar), and `mandate-check.json`. The panel summaries are
//! also printed by the run, prefixed `summary|`, so the run's own output states
//! every panel's reading.
//!
//! ## Exit codes
//!
//! - `0` — every mandate `PASS`, every series and plot present.
//! - `2` — the command could not do its job, or the evidence is incomplete.
//! - `3` — the evidence is complete and at least one mandate reports `FAIL`.

pub mod delivery;
pub mod exec;
pub mod history;
pub mod lines;
pub mod producers;
pub mod timings;
pub mod value;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::tools::json::{self, Json};
use crate::tools::pyjson::repr_str;

use lines::{Arm, MandateRecord};
use producers::{Producer, ProducerRecord, RunRecord};
use timings::Timings;
use value::py_str;

/// The producer the registry marks primary.
pub const PRIMARY_PRODUCER: &str = "rtp_mux";
pub const OUT_DIR_ENV: &str = "MANDATE_CHECK_DIR";
pub const QUICK_ENV: &str = "MANDATE_SMOKE_QUICK";
pub const FAULT_ENV: &str = "MANDATE_SMOKE_FAULT";
pub const DEFAULT_TIMEOUT_SECONDS: f64 = 900.0;
pub const DEFAULT_CARGO: &str = "cargo";
pub const REPORT_NAME: &str = "mandate-check.json";
pub const LOG_NAME: &str = "mandate-smoke.log";
pub const PLOTS_DIRNAME: &str = "plots";
pub const REPORT_SCHEMA: &str = "mandate-check/10";
pub const ARMS_DECLARATION_NAME: &str = "mandate-arms.json";
pub const ARMS_DECLARATION_SCHEMA: &str = "mandate-arms/1";
pub const PRODUCERS_DECLARATION_NAME: &str = "mandate-producers.json";
pub const PRODUCERS_DECLARATION_SCHEMA: &str = "mandate-producers/1";
/// The keys a registry entry may carry, in the order a missing-key message
/// names them.
pub const PRODUCER_KEYS: [&str; 10] = [
    "id",
    "package",
    "target",
    "source",
    "default_path",
    "cargo_args",
    "test_args",
    "sections",
    "verdicts",
    "log",
];
/// Keys a registry entry may carry *besides* [`PRODUCER_KEYS`]: the declarations
/// a producer owns. The arm coverage declaration and its mandate id set are the
/// mandate owner's, so the owner names them here rather than the tool assuming
/// a path.
pub const OPTIONAL_PRODUCER_KEYS: [&str; 2] = ["declaration", "baseline"];
pub const REVISION_TIMEOUT_SECONDS: f64 = 30.0;
pub const LOG_TAIL_LINES: usize = 20;

pub const EXIT_OK: i32 = 0;
pub const EXIT_EVIDENCE_FAILURE: i32 = 2;
pub const EXIT_MANDATE_FAILURE: i32 = 3;

pub const VALUE_RE_SOURCE: &str = r"^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\S+)$";
pub const TEST_STATES: [&str; 3] = ["ok", "FAILED", "ignored"];

pub const TIMING_ARGS: [&str; 3] = ["-Z", "unstable-options", "--report-time"];
pub const TIMING_ENV: &str = "RUSTC_BOOTSTRAP";
pub const TIMING_ENV_VALUE: &str = "1";

pub const DURATION_SOURCE_HARNESS: &str = "libtest-report-time";
pub const DURATION_SOURCE_STREAM_BRACKET: &str = "stream-bracket-of-mandate-lines";
pub const DURATION_DECIMALS: usize = 2;
pub const TOTAL_FIT_TOLERANCE: f64 = 0.01;
pub const TOTAL_FIT_FLOOR_SECONDS: f64 = 0.1;

pub const ARM_STAT_KEYS: [&str; 19] = [
    "p50",
    "p90",
    "p99",
    "p999",
    "max",
    "min",
    "mean",
    "std",
    "over250",
    "delivery",
    "fraction",
    "share",
    "imbalance",
    "min_share",
    "max_share",
    "ideal_share",
    "delivered_mib_s",
    "shaper_mib_s",
    "capacity_mib_s",
];
pub const ARM_COUNTER_KEYS: [(&str, &str); 15] = [
    ("sent", "sent"),
    ("recv", "received"),
    ("received", "received"),
    ("wire", "wire_bytes"),
    ("wire_bytes", "wire_bytes"),
    ("bulk_sink", "bulk_sink_bytes"),
    ("bulk_sink_bytes", "bulk_sink_bytes"),
    ("bulk_wire", "bulk_wire_bytes"),
    ("bulk_wire_bytes", "bulk_wire_bytes"),
    ("forwarded", "forwarded_bytes"),
    ("forwarded_bytes", "forwarded_bytes"),
    ("delivered", "delivered_bytes"),
    ("delivered_bytes", "delivered_bytes"),
    ("offered", "offered_bytes"),
    ("offered_bytes", "offered_bytes"),
];
pub const ARM_WINDOW_KEYS: [(&str, &str); 7] = [
    ("window", "window_seconds"),
    ("window_s", "window_seconds"),
    ("wall", "wall_seconds"),
    ("wall_s", "wall_seconds"),
    ("elapsed", "elapsed_seconds"),
    ("elapsed_seconds", "elapsed_seconds"),
    ("measured_s", "measured_seconds"),
];

pub const DELIVERY_KEY: &str = "delivery";
pub const DELIVERY_FLOOR_KEY: &str = "delivery_floor";
pub const DELIVERY_OFFERED_COUNTER: &str = "sent";
pub const DELIVERY_RECEIVED_COUNTER: &str = "received";
pub const DELIVERY_PRINT_STEP: f64 = 1e-3;

pub const PLOT_TOOL: &str = "netem-tools";
pub const PLOT_BUILD_COMMAND: &str =
    "cargo build --release -p rtp_mux --features perf --bin netem-tools";
pub const PLOT_TIMEOUT_SECONDS: f64 = 1800.0;

/// A literal no caller reads; kept so a dead placeholder cannot be mistaken for
/// a message.
pub const PLC: &str = "";

/// What the report records about how a duration was measured.
pub const TIMING_METHOD: &str = "libtest-per-test-stamp: a test's duration is the time libtest itself \
    reports for that test (the child is run with -Z unstable-options --report-time, so its result \
    reads 'test <name> ... ok <1.234s>'), which libtest takes around that test's own execution and \
    which therefore stays that test's own while the target's tests run concurrently. It is not a \
    line's position in the stream, which under libtest's default parallelism is an artefact of the \
    interleaving. A test whose result carries no stamp has a null duration and duration_source, and \
    is never bracketed. A mandate's duration is still the bracket between its neighbouring MANDATE \
    lines and is marked with that source, because the MANDATE line is printed from inside the test \
    that measured the section and no per-section stamp exists, and a bracket this report's own \
    resolution renders as '0.00s' is reported as absent with the note that says why rather than as \
    a duration";

/// The flags of `mandate-check`.
#[derive(Debug, Clone)]
pub struct Args {
    pub producer: Vec<String>,
    pub producer_path: Vec<String>,
    pub dir: Option<PathBuf>,
    pub quick: bool,
    pub timeout: f64,
    pub cargo: String,
    pub browser: Option<String>,
    /// `--no-rasterize` inverts this.
    pub rasterize: bool,
    pub fault: Option<String>,
    /// The wrapper's own flag: skip the archive/compare step.
    pub no_history: bool,
    pub history_label: Option<String>,
}

impl Default for Args {
    fn default() -> Args {
        Args {
            producer: Vec::new(),
            producer_path: Vec::new(),
            dir: None,
            quick: false,
            timeout: DEFAULT_TIMEOUT_SECONDS,
            cargo: DEFAULT_CARGO.to_string(),
            browser: None,
            rasterize: true,
            fault: None,
            no_history: false,
            history_label: None,
        }
    }
}

/// One verdict section's record in the report.
#[derive(Debug, Clone, PartialEq)]
pub struct MandateReport {
    pub producer: String,
    pub declared: bool,
    pub verdict: Option<String>,
    pub values: value::Ordered,
    pub raw_line: Option<String>,
    pub plots: Vec<String>,
    pub series_counts: Vec<i64>,
    pub panels: i64,
    pub panel_summaries: Vec<Json>,
    pub censoring_arms: Vec<String>,
    pub finished_at_seconds: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub duration_source: Option<String>,
    pub duration_note: Option<String>,
}

impl MandateReport {
    fn new(producer: &str) -> MandateReport {
        MandateReport {
            producer: producer.to_string(),
            declared: false,
            verdict: None,
            values: value::Ordered::default(),
            raw_line: None,
            plots: Vec::new(),
            series_counts: Vec::new(),
            panels: 0,
            panel_summaries: Vec::new(),
            censoring_arms: Vec::new(),
            finished_at_seconds: None,
            duration_seconds: None,
            duration_source: None,
            duration_note: None,
        }
    }

    fn to_json(&self) -> Json {
        let mut map = BTreeMap::new();
        map.insert("producer".to_string(), Json::Str(self.producer.clone()));
        map.insert("declared".to_string(), Json::Bool(self.declared));
        map.insert(
            "verdict".to_string(),
            json::opt_str(self.verdict.as_deref()),
        );
        map.insert("values".to_string(), self.values.to_json());
        map.insert(
            "raw_line".to_string(),
            json::opt_str(self.raw_line.as_deref()),
        );
        map.insert("plots".to_string(), json::str_list(&self.plots));
        map.insert(
            "series_counts".to_string(),
            Json::Array(self.series_counts.iter().copied().map(Json::Int).collect()),
        );
        map.insert("panels".to_string(), Json::Int(self.panels));
        map.insert(
            "panel_summaries".to_string(),
            Json::Array(self.panel_summaries.clone()),
        );
        map.insert(
            "censoring_arms".to_string(),
            json::str_list(&self.censoring_arms),
        );
        map.insert(
            "finished_at_seconds".to_string(),
            json::opt_float(self.finished_at_seconds),
        );
        map.insert(
            "duration_seconds".to_string(),
            json::opt_float(self.duration_seconds),
        );
        map.insert(
            "duration_source".to_string(),
            json::opt_str(self.duration_source.as_deref()),
        );
        map.insert(
            "duration_note".to_string(),
            json::opt_str(self.duration_note.as_deref()),
        );
        Json::Object(map)
    }
}

/// One producer's per-arm instrument readings.
#[derive(Debug, Clone, PartialEq)]
pub struct CensoringRecord {
    pub instrument: String,
    pub mandate: Option<String>,
    pub arms: BTreeMap<String, BTreeMap<String, Json>>,
}

impl CensoringRecord {
    fn to_json(&self) -> Json {
        let mut arms = BTreeMap::new();
        for (arm, readings) in &self.arms {
            arms.insert(arm.clone(), json::object(readings));
        }
        let mut map = BTreeMap::new();
        map.insert("instrument".to_string(), Json::Str(self.instrument.clone()));
        map.insert(
            "mandate".to_string(),
            json::opt_str(self.mandate.as_deref()),
        );
        map.insert("arms".to_string(), Json::Object(arms));
        Json::Object(map)
    }
}

/// Where the arm coverage declaration was read from and what it declared.
#[derive(Debug, Clone, PartialEq)]
pub struct ArmDeclarationRecord {
    pub path: String,
    pub schema: String,
    pub source: Option<String>,
    pub declared_cells: usize,
}

impl ArmDeclarationRecord {
    fn to_json(&self) -> Json {
        let mut map = BTreeMap::new();
        map.insert("path".to_string(), Json::Str(self.path.clone()));
        map.insert("schema".to_string(), Json::Str(self.schema.clone()));
        map.insert("source".to_string(), json::opt_str(self.source.as_deref()));
        map.insert(
            "declared_cells".to_string(),
            Json::Int(self.declared_cells as i64),
        );
        Json::Object(map)
    }
}

/// The run's report.
#[derive(Debug, Clone)]
pub struct Report {
    pub ok: bool,
    pub exit_code: Option<i32>,
    pub verdict: Option<String>,
    pub started_at: String,
    pub duration_seconds: Option<f64>,
    pub timeout_seconds: f64,
    pub quick: bool,
    pub producers_declared: Vec<String>,
    pub producers_selected: Vec<String>,
    pub producers: BTreeMap<String, ProducerRecord>,
    pub command: Option<Vec<String>>,
    pub cwd: Option<String>,
    pub out_dir: String,
    pub report: String,
    pub primary: Option<Json>,
    pub smoke: Json,
    pub mandates: BTreeMap<String, MandateReport>,
    pub mandate_order: Vec<String>,
    pub section_order: Vec<String>,
    pub timings: Timings,
    pub arms: Vec<Arm>,
    pub arm_notes: Vec<Arm>,
    pub censoring: BTreeMap<String, CensoringRecord>,
    pub delivery_granularity: BTreeMap<String, delivery::DeliveryReading>,
    pub arm_declaration: ArmDeclarationRecord,
    pub problems: Vec<String>,
}

impl Report {
    pub fn to_json(&self) -> Json {
        let mut map = BTreeMap::new();
        map.insert("schema".to_string(), Json::Str(REPORT_SCHEMA.to_string()));
        map.insert("ok".to_string(), Json::Bool(self.ok));
        map.insert(
            "exit_code".to_string(),
            self.exit_code
                .map_or(Json::Null, |code| Json::Int(code as i64)),
        );
        map.insert(
            "verdict".to_string(),
            json::opt_str(self.verdict.as_deref()),
        );
        map.insert("started_at".to_string(), Json::Str(self.started_at.clone()));
        map.insert(
            "duration_seconds".to_string(),
            json::opt_float(self.duration_seconds),
        );
        map.insert(
            "timeout_seconds".to_string(),
            Json::Float(self.timeout_seconds),
        );
        map.insert("quick".to_string(), Json::Bool(self.quick));
        map.insert(
            "producers_declared".to_string(),
            json::str_list(&self.producers_declared),
        );
        map.insert(
            "producers_selected".to_string(),
            json::str_list(&self.producers_selected),
        );
        let mut producers = BTreeMap::new();
        for (id, record) in &self.producers {
            producers.insert(id.clone(), record.to_json());
        }
        map.insert("producers".to_string(), Json::Object(producers));
        map.insert(
            "command".to_string(),
            match &self.command {
                Some(tokens) => json::str_list(tokens),
                None => Json::Null,
            },
        );
        map.insert("cwd".to_string(), json::opt_str(self.cwd.as_deref()));
        map.insert("out_dir".to_string(), Json::Str(self.out_dir.clone()));
        map.insert("report".to_string(), Json::Str(self.report.clone()));
        map.insert(
            "rtp_mux".to_string(),
            self.primary.clone().unwrap_or(Json::Null),
        );
        map.insert("smoke".to_string(), self.smoke.clone());
        let mut mandates = BTreeMap::new();
        for (id, record) in &self.mandates {
            mandates.insert(id.clone(), record.to_json());
        }
        map.insert("mandates".to_string(), Json::Object(mandates));
        map.insert(
            "mandate_order".to_string(),
            json::str_list(&self.mandate_order),
        );
        map.insert(
            "section_order".to_string(),
            json::str_list(&self.section_order),
        );
        map.insert("timings".to_string(), timings_to_json(&self.timings));
        map.insert(
            "arms".to_string(),
            Json::Array(self.arms.iter().map(Arm::to_json).collect()),
        );
        map.insert(
            "arm_notes".to_string(),
            Json::Array(self.arm_notes.iter().map(Arm::note_json).collect()),
        );
        let mut censoring = BTreeMap::new();
        for (id, record) in &self.censoring {
            censoring.insert(id.clone(), record.to_json());
        }
        map.insert("censoring".to_string(), Json::Object(censoring));
        let mut granularity = BTreeMap::new();
        for (id, reading) in &self.delivery_granularity {
            granularity.insert(id.clone(), delivery_to_json(reading));
        }
        map.insert(
            "delivery_granularity".to_string(),
            Json::Object(granularity),
        );
        map.insert(
            "arm_declaration".to_string(),
            self.arm_declaration.to_json(),
        );
        map.insert("problems".to_string(), json::str_list(&self.problems));
        Json::Object(map)
    }
}

fn timings_to_json(timings: &Timings) -> Json {
    let tests = timings
        .tests
        .iter()
        .map(|entry| {
            let mut map = BTreeMap::new();
            map.insert("target".to_string(), Json::Str(entry.target.clone()));
            map.insert("name".to_string(), Json::Str(entry.name.clone()));
            map.insert("state".to_string(), Json::Str(entry.state.clone()));
            map.insert(
                "started_at_seconds".to_string(),
                Json::Float(entry.started_at_seconds),
            );
            map.insert(
                "finished_at_seconds".to_string(),
                Json::Float(entry.finished_at_seconds),
            );
            map.insert(
                "duration_seconds".to_string(),
                json::opt_float(entry.duration_seconds),
            );
            map.insert(
                "duration_source".to_string(),
                json::opt_str(entry.duration_source.as_deref()),
            );
            map.insert(
                "producer".to_string(),
                json::opt_str(entry.producer.as_deref()),
            );
            Json::Object(map)
        })
        .collect();
    let mandates = timings
        .mandates
        .iter()
        .map(|entry| {
            let mut map = BTreeMap::new();
            map.insert("mandate".to_string(), Json::Str(entry.mandate.clone()));
            map.insert(
                "finished_at_seconds".to_string(),
                Json::Float(entry.finished_at_seconds),
            );
            map.insert(
                "duration_seconds".to_string(),
                json::opt_float(entry.duration_seconds),
            );
            map.insert(
                "duration_source".to_string(),
                json::opt_str(entry.duration_source.as_deref()),
            );
            map.insert(
                "duration_note".to_string(),
                json::opt_str(entry.duration_note.as_deref()),
            );
            map.insert(
                "producer".to_string(),
                json::opt_str(entry.producer.as_deref()),
            );
            Json::Object(map)
        })
        .collect();
    let targets = timings
        .targets
        .iter()
        .map(|entry| {
            let mut map = BTreeMap::new();
            map.insert("target".to_string(), Json::Str(entry.target.clone()));
            map.insert(
                "total_seconds".to_string(),
                json::opt_float(entry.total_seconds),
            );
            map.insert(
                "total_source".to_string(),
                json::opt_str(entry.total_source.as_deref()),
            );
            map.insert("serial".to_string(), Json::Bool(entry.serial));
            map.insert("tests".to_string(), Json::Int(entry.tests as i64));
            map.insert("ran".to_string(), Json::Int(entry.ran as i64));
            map.insert("stamped".to_string(), Json::Int(entry.stamped as i64));
            map.insert(
                "sum_seconds".to_string(),
                // Python's `round(sum([]), 3)` is the integer zero, and its own
                // JSON writes `0`; a stamped target's sum is a float.
                if entry.stamped == 0 {
                    Json::Int(0)
                } else {
                    Json::Float(entry.sum_seconds)
                },
            );
            map.insert(
                "max_seconds".to_string(),
                json::opt_float(entry.max_seconds),
            );
            map.insert(
                "overlap_factor".to_string(),
                json::opt_float(entry.overlap_factor),
            );
            map.insert(
                "fits".to_string(),
                match entry.fits {
                    Some(true) => Json::Bool(true),
                    Some(false) => Json::Bool(false),
                    None => Json::Null,
                },
            );
            map.insert("note".to_string(), json::opt_str(entry.note.as_deref()));
            map.insert(
                "producer".to_string(),
                json::opt_str(entry.producer.as_deref()),
            );
            Json::Object(map)
        })
        .collect();
    let mut map = BTreeMap::new();
    map.insert("method".to_string(), Json::Str(TIMING_METHOD.to_string()));
    map.insert(
        "origin".to_string(),
        Json::Str("smoke-child-start".to_string()),
    );
    map.insert("tests".to_string(), Json::Array(tests));
    map.insert("mandates".to_string(), Json::Array(mandates));
    map.insert("targets".to_string(), Json::Array(targets));
    Json::Object(map)
}

fn delivery_to_json(reading: &delivery::DeliveryReading) -> Json {
    let mut map = BTreeMap::new();
    map.insert("floor".to_string(), reading.floor.clone());
    map.insert("offered_min".to_string(), Json::Int(reading.offered_min));
    map.insert("budget_units".to_string(), Json::Int(reading.budget_units));
    map.insert(
        "min_failing_units".to_string(),
        Json::Int(reading.min_failing_units),
    );
    map.insert(
        "units_short_max".to_string(),
        Json::Int(reading.units_short_max),
    );
    map.insert("block_ms".to_string(), json::opt_float(reading.block_ms));
    let arms = reading
        .arms
        .iter()
        .map(|arm| {
            let mut entry = BTreeMap::new();
            entry.insert("id".to_string(), Json::Str(arm.id.clone()));
            entry.insert("offered".to_string(), Json::Int(arm.offered));
            entry.insert("received".to_string(), Json::Int(arm.received));
            entry.insert("units_short".to_string(), Json::Int(arm.units_short));
            entry.insert("budget_units".to_string(), Json::Int(arm.budget_units));
            entry.insert(
                "window_seconds".to_string(),
                json::opt_float(arm.window_seconds),
            );
            Json::Object(entry)
        })
        .collect();
    map.insert("arms".to_string(), Json::Array(arms));
    Json::Object(map)
}

/// The empty report: every declared producer, and every selected one's grid.
pub fn build_report(
    args: &Args,
    out_dir: &Path,
    declared: &[Producer],
    selected: &[Producer],
) -> Report {
    let mut producers = BTreeMap::new();
    for entry in declared {
        producers.insert(entry.id.clone(), producers::producer_record(entry, out_dir));
    }
    let mut mandates = BTreeMap::new();
    let mut mandate_order = Vec::new();
    let mut section_order = Vec::new();
    for entry in declared {
        for section in &entry.sections {
            section_order.push(section.clone());
        }
        if !selected.iter().any(|chosen| chosen.id == entry.id) {
            continue;
        }
        for mandate in &entry.verdicts {
            mandate_order.push(mandate.clone());
            mandates.insert(mandate.clone(), MandateReport::new(&entry.id));
        }
    }
    let mut smoke = BTreeMap::new();
    smoke.insert("exit_code".to_string(), Json::Null);
    smoke.insert("timed_out".to_string(), Json::Bool(false));
    smoke.insert(
        "log".to_string(),
        Json::Str(out_dir.join(LOG_NAME).display().to_string()),
    );
    smoke.insert("producer".to_string(), Json::Null);
    Report {
        ok: false,
        exit_code: None,
        verdict: None,
        started_at: now_iso8601(),
        duration_seconds: None,
        timeout_seconds: args.timeout,
        quick: args.quick,
        producers_declared: declared.iter().map(|entry| entry.id.clone()).collect(),
        producers_selected: selected.iter().map(|entry| entry.id.clone()).collect(),
        producers,
        command: None,
        cwd: None,
        out_dir: out_dir.display().to_string(),
        report: out_dir.join(REPORT_NAME).display().to_string(),
        primary: None,
        smoke: Json::Object(smoke),
        mandates,
        mandate_order,
        section_order,
        timings: Timings::default(),
        arms: Vec::new(),
        arm_notes: Vec::new(),
        censoring: BTreeMap::new(),
        delivery_granularity: BTreeMap::new(),
        arm_declaration: ArmDeclarationRecord {
            path: producers::tools_dir()
                .join(ARMS_DECLARATION_NAME)
                .display()
                .to_string(),
            schema: ARMS_DECLARATION_SCHEMA.to_string(),
            source: None,
            declared_cells: 0,
        },
        problems: Vec::new(),
    }
}

/// Write the report beside the run's evidence.
pub fn write_report(out_dir: &Path, report: &Report) -> std::io::Result<PathBuf> {
    let path = out_dir.join(REPORT_NAME);
    let text = format!("{}\n", json::to_string(&report.to_json()));
    std::fs::write(&path, text)?;
    Ok(path)
}

/// Record one producer's per-arm measurements, or name why they are missing.
pub fn apply_arm_records(
    report: &mut Report,
    events: &[(f64, String)],
    declaration: Option<&Json>,
    producer: &Producer,
    problems: &mut Vec<String>,
) -> Vec<Arm> {
    if report.arm_declaration.source.is_none()
        && let Some(declaration) = declaration
    {
        {
            report.arm_declaration.source = declaration
                .get("source")
                .and_then(Json::as_str)
                .map(str::to_string);
            report.arm_declaration.declared_cells = declaration
                .get("cells")
                .and_then(Json::as_object)
                .map(|cells| {
                    cells
                        .values()
                        .map(|value| value.as_array().map(<[Json]>::len).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0);
        }
    }
    let (mut arms, notes) = lines::parse_arm_lines(events, problems);
    for arm in arms.iter_mut() {
        arm.producer = Some(producer.id.clone());
    }
    lines::stamp_arm_coverage(&mut arms, declaration, problems);
    lines::check_arm_sections(&arms, &producer.sections, &producer.id, problems);
    lines::check_arm_coverage(&arms, &notes, problems, &producer.sections);
    report.arms.extend(arms.clone());
    report.arm_notes.extend(notes);
    arms
}

/// Attach one producer's measured wall-clock to its mandates and the report.
pub fn apply_mandate_timings(report: &mut Report, timings: &Timings, producer: &Producer) {
    let by_mandate: BTreeMap<&str, &timings::MandateTiming> = timings
        .mandates
        .iter()
        .map(|entry| (entry.mandate.as_str(), entry))
        .collect();
    for entry in &timings.tests {
        let mut entry = entry.clone();
        entry.producer = Some(producer.id.clone());
        report.timings.tests.push(entry);
    }
    for entry in &timings.mandates {
        let mut entry = entry.clone();
        entry.producer = Some(producer.id.clone());
        report.timings.mandates.push(entry);
    }
    for entry in &timings.targets {
        let mut entry = entry.clone();
        entry.producer = Some(producer.id.clone());
        report.timings.targets.push(entry);
    }
    for mandate in &producer.verdicts {
        let Some(entry) = by_mandate.get(mandate.as_str()) else {
            continue;
        };
        let Some(record) = report.mandates.get_mut(mandate) else {
            continue;
        };
        record.finished_at_seconds = Some(entry.finished_at_seconds);
        record.duration_seconds = entry.duration_seconds;
        record.duration_source = entry.duration_source.clone();
        record.duration_note = entry.duration_note.clone();
    }
}

/// Parse and verify one producer's run, filling the report. Exit code back.
pub fn evaluate_producer(
    args: &Args,
    producer: &Producer,
    out_dir: &Path,
    report: &mut Report,
    run: &exec::RunResult,
    declaration: Option<&Json>,
    censoring: Option<&(String, String)>,
) -> i32 {
    let log_path = out_dir.join(&producer.log);
    let log_text = log_path.display().to_string();
    if let Err(error) = std::fs::write(&log_path, &run.output) {
        eprintln!("mandate-check: error: {}: {log_text}: {error}", producer.id);
        return EXIT_EVIDENCE_FAILURE;
    }
    if let Some(record) = report.producers.get_mut(&producer.id) {
        record.run = RunRecord {
            exit_code: run.exit_code,
            timed_out: run.timed_out,
            log: log_text.clone(),
        };
    }
    let timings = timings::derive_timings(
        &run.events,
        &producer.target,
        timings::declared_serial(&producer.test_args),
    );
    apply_mandate_timings(report, &timings, producer);
    let mut problems: Vec<String> = timings.problems.clone();
    let arms = apply_arm_records(report, &run.events, declaration, producer, &mut problems);
    if let Some(record) = report.producers.get_mut(&producer.id) {
        record.arms = arms.len();
    }
    if let Some((mandate, instrument)) =
        censoring.filter(|(mandate, _)| producer.verdicts.contains(mandate))
    {
        let (rows, mut censoring_problems) = lines::parse_censoring_rows(&run.events, instrument);
        problems.append(&mut censoring_problems);
        if rows.is_empty() {
            problems.push(format!(
                "{mandate}: the smoke set printed no '[{instrument}] arm=...' reading, \
                 so the {mandate} latency panel has no machine verdict to state. A peak \
                 that returned and a climb cut off by the window's end are the same \
                 pixels, so that panel would be drawn for a failure it cannot show"
            ));
        }
        report.censoring.insert(
            producer.id.clone(),
            CensoringRecord {
                instrument: instrument.clone(),
                mandate: Some(mandate.clone()),
                arms: rows.clone(),
            },
        );
    }
    if run.timed_out {
        problems.push(format!(
            "the test target did not finish within {:.0}s and was killed; its \
             partial output is {log_text}",
            args.timeout
        ));
    } else if run.exit_code != Some(0) {
        problems.push(format!(
            "the test target exited {} (compile or test failure); its output is \
             {log_text}",
            run.exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "None".to_string())
        ));
    }

    let (records, parse_problems) = lines::parse_mandate_lines(&run.output, &producer.verdicts);
    problems.extend(parse_problems);
    for mandate in &producer.verdicts {
        if !records.contains_key(mandate) {
            problems.push(format!(
                "{mandate}: the smoke set printed no 'MANDATE {mandate} <PASS|FAIL> \
                 ...' line, so this mandate was never measured"
            ));
        }
    }

    for mandate in &producer.verdicts {
        let parsed: Option<MandateRecord> = records.get(mandate).cloned();
        if let Some(parsed) = &parsed
            && let Some(section) = report.mandates.get_mut(mandate)
        {
            {
                section.declared = true;
                section.verdict = Some(parsed.verdict.clone());
                section.values = parsed.values.clone();
                section.raw_line = Some(parsed.raw_line.clone());
            }
        }
        let run_censoring = if Some(mandate.as_str()) == censoring.map(|(m, _)| m.as_str()) {
            report
                .censoring
                .get(&producer.id)
                .map(censoring_arms_json)
                .map(|arms| json::to_compact(&arms))
        } else {
            None
        };
        let (summary, render_problems) = exec::render_mandate(
            mandate,
            out_dir,
            args.rasterize,
            args.browser.as_deref(),
            parsed
                .as_ref()
                .map(|record| record.values.to_compact())
                .as_deref(),
            run_censoring.as_deref(),
            args.fault.as_deref(),
        );
        if let Some(summary) = &summary {
            let stated: Vec<String> = summary
                .get("censoring")
                .and_then(Json::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let run_arms: Vec<String> = report
                .censoring
                .get(&producer.id)
                .map(|record| record.arms.keys().cloned().collect())
                .unwrap_or_default();
            let mut stated_sorted = stated.clone();
            stated_sorted.sort();
            let mut run_sorted = run_arms.clone();
            run_sorted.sort();
            if let Some(section) = report.mandates.get_mut(mandate) {
                section.censoring_arms = stated.clone();
                section.plots = string_list(summary.get("svg"));
                section.plots.extend(string_list(summary.get("png")));
                section.series_counts = summary
                    .get("series_counts")
                    .and_then(Json::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| match item {
                                Json::Int(number) => Some(*number),
                                _ => None,
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                section.panels = summary
                    .get("panels")
                    .and_then(|value| match value {
                        Json::Int(number) => Some(*number),
                        _ => None,
                    })
                    .unwrap_or(0);
                section.panel_summaries = summary
                    .get("summaries")
                    .and_then(Json::as_array)
                    .map(<[Json]>::to_vec)
                    .unwrap_or_default();
            }
            if Some(mandate.as_str()) == censoring.map(|(m, _)| m.as_str())
                && stated_sorted != run_sorted
            {
                problems.push(format!(
                    "{mandate}: the plotter stated {} of the run's {} per-arm reading(s) \
                     ({}), so at least one arm's own verdict is not on the panel",
                    stated.len(),
                    run_arms.len(),
                    lines::py_sorted(&stated)
                ));
            }
            problems.extend(exec::verify_plots(mandate, summary));
        }
        problems.extend(render_problems);
    }
    problems.extend(timings::mandate_duration_problems(
        &report.mandates,
        &producer.verdicts,
    ));

    report.problems.extend(
        problems
            .iter()
            .map(|problem| format!("{}: {problem}", producer.id)),
    );
    if !problems.is_empty() {
        return EXIT_EVIDENCE_FAILURE;
    }
    if producer.verdicts.iter().any(|mandate| {
        report
            .mandates
            .get(mandate)
            .and_then(|r| r.verdict.clone())
            .as_deref()
            == Some("FAIL")
    }) {
        return EXIT_MANDATE_FAILURE;
    }
    EXIT_OK
}

fn censoring_arms_json(record: &CensoringRecord) -> Json {
    let mut arms = BTreeMap::new();
    for (arm, readings) in &record.arms {
        arms.insert(arm.clone(), json::object(readings));
    }
    Json::Object(arms)
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

/// The human- and machine-readable block printed on every exit path.
pub fn verdict_block(report: &Report) -> Vec<String> {
    let mut lines = vec![format!(
        "mandate-check: {} producer(s) run of {} declared",
        report.producers_selected.len(),
        report.producers_declared.len()
    )];
    for producer in &report.producers_selected {
        let Some(record) = report.producers.get(producer) else {
            continue;
        };
        lines.push(format!(
            "producer: {producer}  {}:{}",
            record.package, record.target
        ));
        lines.push(format!(
            "  checkout: {}",
            record
                .path
                .clone()
                .unwrap_or_else(|| "unresolved".to_string())
        ));
        lines.push(format!(
            "  revision: {} ({})",
            record
                .revision
                .clone()
                .unwrap_or_else(|| "unresolved".to_string()),
            record
                .revision_source
                .clone()
                .unwrap_or_else(|| "no jj or git".to_string())
        ));
        lines.push(format!(
            "  tree:     {} ({})",
            record
                .tree_id
                .clone()
                .unwrap_or_else(|| "unresolved".to_string()),
            record
                .tree_id_source
                .clone()
                .unwrap_or_else(|| "no jj or git".to_string())
        ));
        lines.push(format!(
            "  command:  {}",
            record.command.clone().unwrap_or_default().join(" ")
        ));
        lines.push(format!("  log:      {}", record.log));
        lines.push(format!(
            "  exit:     {}{}   arms: {}",
            record
                .run
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "None".to_string()),
            if record.run.timed_out {
                " (timed out)"
            } else {
                ""
            },
            record.arms
        ));
    }
    let unselected: Vec<&String> = report
        .producers_declared
        .iter()
        .filter(|entry| !report.producers_selected.contains(entry))
        .collect();
    if !unselected.is_empty() {
        lines.push(format!(
            "  not selected: {}",
            unselected
                .iter()
                .map(|entry| entry.as_str())
                .collect::<Vec<&str>>()
                .join(", ")
        ));
    }
    lines.push(format!("  output:   {}", report.out_dir));
    lines.push(format!(
        "  quick:    {}   timeout: {:.0}s",
        if report.quick { "yes" } else { "no" },
        report.timeout_seconds
    ));
    for mandate in &report.mandate_order {
        let Some(record) = report.mandates.get(mandate) else {
            continue;
        };
        let verdict = record
            .verdict
            .clone()
            .unwrap_or_else(|| "MISSING".to_string());
        let measured = record
            .values
            .iter()
            .map(|(key, value)| format!("{key}={}", py_str(value)))
            .collect::<Vec<String>>()
            .join(" ");
        lines.push(
            format!("{mandate} {verdict}  {measured}")
                .trim_end()
                .to_string(),
        );
        let duration = record.duration_seconds;
        let note = record.duration_note.as_deref();
        if let Some(duration) = duration {
            lines.push(format!(
                "  duration: {duration:.width$}s ({})",
                record
                    .duration_source
                    .clone()
                    .unwrap_or_else(|| "unstated".to_string()),
                width = DURATION_DECIMALS
            ));
        } else if let Some(note) = note {
            lines.push(format!("  duration: unmeasured ({note})"));
        } else if record.raw_line.is_some() {
            lines.push(
                "  duration: unmeasured (its MANDATE line printed but the record \
                 carries neither a bracket nor a reason)"
                    .to_string(),
            );
        }
        for path in &record.plots {
            lines.push(format!("  plot: {path}"));
        }
        for document in &record.panel_summaries {
            let block = document
                .get("block")
                .and_then(Json::as_str)
                .unwrap_or_default();
            for line in block.split('\n') {
                lines.push(format!("  summary| {line}"));
            }
        }
    }
    for mandate in &report.mandate_order {
        let Some(reading) = report.delivery_granularity.get(mandate) else {
            continue;
        };
        let mut breach = format!("{} counted unit(s)", reading.min_failing_units);
        if let Some(block_ms) = reading.block_ms {
            breach.push_str(&format!(
                " of this run's smallest offer ({block_ms:.1}ms at {} units per window)",
                reading.offered_min
            ));
        }
        lines.push(format!(
            "delivery: {mandate} floor={} offered_min={} budget_units={} \
             units_short_max={}  -> a breach of the floor is {breach}, not the \
             ratio's third decimal",
            py_str(&reading.floor),
            reading.offered_min,
            reading.budget_units,
            reading.units_short_max
        ));
    }
    for (producer_id, entry) in &report.censoring {
        let readings = entry
            .arms
            .iter()
            .map(|(arm, reading)| {
                format!(
                    "{arm}={}",
                    reading
                        .get("verdict")
                        .map(py_str)
                        .unwrap_or_else(|| "None".to_string())
                )
            })
            .collect::<Vec<String>>()
            .join(", ");
        lines.push(format!(
            "instrument: {producer_id}  {} [{}]  {} arm(s) read{}{} (stated on the \
             mandate's line panel)",
            entry.instrument,
            entry.mandate.clone().unwrap_or_else(|| "None".to_string()),
            entry.arms.len(),
            if readings.is_empty() { "" } else { ": " },
            readings
        ));
    }
    for entry in &report.timings.targets {
        let total = entry.total_seconds;
        let fits = entry.fits;
        let mut shape = String::new();
        if let Some(total) = total {
            if entry.serial {
                shape = format!(", {:.2}s of {total:.2}s total (serial)", entry.sum_seconds);
            } else {
                let overlap = match entry.overlap_factor {
                    Some(factor) => format!("{factor:.2}x overlap"),
                    None => "overlap unmeasured".to_string(),
                };
                shape = format!(
                    ", {:.2}s of {total:.2}s total ({overlap}: {} concurrent test(s))",
                    entry.sum_seconds, entry.stamped
                );
            }
        }
        let fit = match fits {
            Some(true) => "yes",
            Some(false) => "NO",
            None => "unchecked",
        };
        lines.push(format!(
            "timings: {}:{}  {}/{} test(s) stamped by libtest{shape}  fit={fit}{}",
            entry.producer.clone().unwrap_or_else(|| "None".to_string()),
            entry.target,
            entry.stamped,
            entry.ran,
            match &entry.note {
                Some(note) => format!("  note: {note}"),
                None => String::new(),
            }
        ));
    }
    let produced: Vec<String> = {
        let mut list: Vec<String> = report
            .arms
            .iter()
            .filter_map(|arm| arm.producer.clone())
            .collect();
        list.sort();
        list.dedup();
        list
    };
    lines.push(format!(
        "arms: {} measured{}",
        report.arms.len(),
        if produced.len() > 1 {
            format!(" ({})", produced.join(", "))
        } else {
            String::new()
        }
    ));
    let mut producers_json = BTreeMap::new();
    for (id, record) in &report.producers {
        producers_json.insert(id.clone(), record.to_json());
    }
    lines.extend(lines::arm_summary(
        &report.arms,
        &report.arm_notes,
        &Json::Object(producers_json),
    ));
    if let Some(source) = &report.arm_declaration.source {
        let _ = source;
        lines.push(format!(
            "  cells: {} declared in {}",
            report.arm_declaration.declared_cells,
            Path::new(&report.arm_declaration.path)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default()
        ));
    }
    let duration = report.duration_seconds;
    let order = &report.mandate_order;
    let passed = order
        .iter()
        .filter(|mandate| {
            report
                .mandates
                .get(*mandate)
                .and_then(|record| record.verdict.clone())
                .as_deref()
                == Some("PASS")
        })
        .count();
    let panels: usize = order
        .iter()
        .map(|mandate| {
            report
                .mandates
                .get(mandate)
                .map(|record| record.plots.len())
                .unwrap_or(0)
        })
        .sum();
    let summary = if report.exit_code == Some(EXIT_EVIDENCE_FAILURE) {
        format!(
            "evidence incomplete ({passed}/{} mandate line(s) said PASS, {panels} plot(s))",
            order.len()
        )
    } else {
        format!(
            "{passed}/{} mandate(s) passed, {panels} plot(s)",
            order.len()
        )
    };
    lines.push(format!(
        "verdict: {}  exit={}  {summary}{}",
        if report.ok { "PASS" } else { "FAIL" },
        report
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "None".to_string()),
        match duration {
            Some(duration) => format!(", {duration:.1}s"),
            None => String::new(),
        }
    ));
    for problem in &report.problems {
        lines.push(format!("problem: {problem}"));
    }
    lines.push(format!("report:  {}", report.report));
    lines
}

/// The battery: the runner's own work, without the wrapper's history step.
pub fn battery(args: &Args) -> (i32, Option<PathBuf>) {
    if args.timeout <= 0.0 {
        eprintln!("mandate-check: error: --timeout must be positive");
        return (EXIT_EVIDENCE_FAILURE, None);
    }
    let started = Instant::now();
    let out_dir = match &args.dir {
        Some(dir) => producers::absolute(&expand_tilde(dir)),
        None => match producers::default_out_dir() {
            Ok(dir) => dir,
            Err(error) => {
                eprintln!("mandate-check: error: {error}");
                return (EXIT_EVIDENCE_FAILURE, None);
            }
        },
    };
    let mut declaration_problems = Vec::new();
    let declaration = producers::load_producer_declaration(
        &producers::tools_dir().join(PRODUCERS_DECLARATION_NAME),
        &mut declaration_problems,
    );
    let logs: Vec<String> = match &declaration {
        Some(entries) => {
            let listed: Vec<String> = entries.iter().map(|entry| entry.log.clone()).collect();
            if listed.is_empty() {
                vec![LOG_NAME.to_string()]
            } else {
                listed
            }
        }
        None => vec![LOG_NAME.to_string()],
    };
    let declined = |problems: &[String]| {
        eprintln!("mandate-check: error: {}", problems.join("; "));
        (EXIT_EVIDENCE_FAILURE, None)
    };
    if !declaration_problems.is_empty() {
        return declined(&declaration_problems);
    }
    let declaration = declaration.expect("no problems means the registry parsed");
    let (overrides, override_problems) = producers::producer_overrides(&args.producer_path);
    if !override_problems.is_empty() {
        return declined(&override_problems);
    }
    let mut selection_problems = Vec::new();
    let selected =
        producers::select_producers(&declaration, &args.producer, &mut selection_problems);
    if !selection_problems.is_empty() {
        return declined(&selection_problems);
    }
    if selected.is_empty() {
        return declined(&["no producer selected".to_string()]);
    }
    // The arm declaration and its mandate id set belong to the crate that owns
    // the mandate: the producer whose lines print verdicts. This tool resolves
    // the file from that producer's own checkout — the override first — and
    // reads the ids it declares, so no mandate id is a constant here. Loading it
    // before the producers are validated is deliberate: the run directory is
    // then cleared even by a run that is refused, because a report left behind
    // by an earlier run would be compared as this run's measurement.
    let mut arm_problems = Vec::new();
    let arm_path = match selected
        .iter()
        .position(|entry| entry.declaration.is_some())
    {
        Some(index) => producers::producer_checkout_with(
            &selected[index],
            overrides.get(&selected[index].id).map(String::as_str),
        )
        .join(
            selected[index]
                .declaration
                .as_deref()
                .unwrap_or(ARMS_DECLARATION_NAME),
        ),
        None => producers::tools_dir().join(ARMS_DECLARATION_NAME),
    };
    let arm_declaration = lines::load_arm_declaration(&arm_path, &mut arm_problems);
    let mandate_ids: Vec<String> = arm_declaration
        .as_ref()
        .map(lines::declared_mandates)
        .unwrap_or_default();
    let declared_censoring = arm_declaration.as_ref().and_then(lines::declared_censoring);
    if let Err(error) = producers::prepare_output_dir(&out_dir, &logs, &mandate_ids) {
        eprintln!("mandate-check: error: {error}");
        return (EXIT_EVIDENCE_FAILURE, None);
    }
    let mut resolved = Vec::new();
    for entry in &selected {
        let (crate_dir, problem) =
            producers::resolve_producer(entry, overrides.get(&entry.id).map(String::as_str));
        match (crate_dir, problem) {
            (Some(crate_dir), None) => resolved.push(crate_dir),
            (None, Some(problem)) => {
                eprintln!("mandate-check: error: {problem}");
                return (EXIT_EVIDENCE_FAILURE, None);
            }
            _ => unreachable!("resolve_producer returns one of the two"),
        }
    }
    // The producers' own checkouts are validated first: a `--producer-path`
    // naming nothing is reported as the bad path it is, not as the arm
    // declaration that cannot be found under it.
    if !arm_problems.is_empty() {
        for problem in &arm_problems {
            eprintln!("mandate-check: error: {problem}");
        }
        return (EXIT_EVIDENCE_FAILURE, None);
    }
    let cargo = match producers::which(&args.cargo) {
        Some(path) => path,
        None => {
            eprintln!(
                "mandate-check: error: the cargo executable {} was not found on PATH, \
                 so no producer can be built",
                repr_str(&args.cargo)
            );
            return (EXIT_EVIDENCE_FAILURE, None);
        }
    };

    let mut report = build_report(args, &out_dir, &declaration, &selected);
    let mut codes: Vec<i32> = Vec::new();
    let mut runs: Vec<exec::RunResult> = Vec::new();
    for (index, entry) in selected.iter().enumerate() {
        let crate_dir = &resolved[index];
        let (revision, change_id, revision_source) = producers::resolve_revision(crate_dir);
        let (tree_id, tree_id_source) =
            producers::resolve_tree_id(crate_dir, revision.as_deref(), revision_source.as_deref());
        let command = producers::producer_command(&cargo, entry);
        if let Some(record) = report.producers.get_mut(&entry.id) {
            record.selected = true;
            record.path = Some(crate_dir.display().to_string());
            record.command = Some(command.clone());
            record.revision = revision;
            record.change_id = change_id;
            record.revision_source = revision_source;
            record.tree_id = tree_id;
            record.tree_id_source = tree_id_source;
        }
        let run = match exec::run_producer(&command, crate_dir, &out_dir, args.quick, args.timeout)
        {
            Ok(run) => run,
            Err(error) => {
                eprintln!("mandate-check: error: {}: {error}", entry.id);
                return (EXIT_EVIDENCE_FAILURE, None);
            }
        };
        codes.push(evaluate_producer(
            args,
            entry,
            &out_dir,
            &mut report,
            &run,
            arm_declaration.as_ref(),
            declared_censoring.as_ref(),
        ));
        runs.push(run);
    }

    let primary = report.producers.get(PRIMARY_PRODUCER).cloned();
    if primary.as_ref().is_some_and(|record| record.selected) {
        let record = primary.expect("checked");
        let mut identity = BTreeMap::new();
        identity.insert("path".to_string(), json::opt_str(record.path.as_deref()));
        identity.insert(
            "revision".to_string(),
            json::opt_str(record.revision.as_deref()),
        );
        identity.insert(
            "change_id".to_string(),
            json::opt_str(record.change_id.as_deref()),
        );
        identity.insert(
            "revision_source".to_string(),
            json::opt_str(record.revision_source.as_deref()),
        );
        identity.insert(
            "tree_id".to_string(),
            json::opt_str(record.tree_id.as_deref()),
        );
        identity.insert(
            "tree_id_source".to_string(),
            json::opt_str(record.tree_id_source.as_deref()),
        );
        report.primary = Some(Json::Object(identity));
        let mut smoke = BTreeMap::new();
        smoke.insert(
            "producer".to_string(),
            Json::Str(PRIMARY_PRODUCER.to_string()),
        );
        smoke.insert(
            "exit_code".to_string(),
            record
                .run
                .exit_code
                .map_or(Json::Null, |code| Json::Int(code as i64)),
        );
        smoke.insert("timed_out".to_string(), Json::Bool(record.run.timed_out));
        smoke.insert("log".to_string(), Json::Str(record.run.log.clone()));
        report.smoke = Json::Object(smoke);
    }
    if let Some(first) = report
        .producers_selected
        .first()
        .and_then(|id| report.producers.get(id))
    {
        report.command = first.command.clone();
        report.cwd = first.path.clone();
    }

    let (granularity, granularity_problems) =
        delivery::check_delivery_granularity(&report.arms, &report.mandate_order, &report.mandates);
    report.delivery_granularity = granularity;
    if !granularity_problems.is_empty() {
        report.problems.extend(granularity_problems);
        codes.push(EXIT_EVIDENCE_FAILURE);
    }
    let exit_code = if codes.contains(&EXIT_EVIDENCE_FAILURE) {
        EXIT_EVIDENCE_FAILURE
    } else if codes.contains(&EXIT_MANDATE_FAILURE) {
        EXIT_MANDATE_FAILURE
    } else {
        EXIT_OK
    };
    report.exit_code = Some(exit_code);
    report.ok = exit_code == EXIT_OK;
    report.verdict = Some(if report.ok { "PASS" } else { "FAIL" }.to_string());
    report.duration_seconds = Some(value::py_round(started.elapsed().as_secs_f64(), 3));
    if let Err(error) = write_report(&out_dir, &report) {
        eprintln!(
            "mandate-check: error: the report {} cannot be written: {error}",
            out_dir.join(REPORT_NAME).display()
        );
        return (EXIT_EVIDENCE_FAILURE, Some(out_dir));
    }
    for line in verdict_block(&report) {
        println!("{line}");
    }
    for (index, entry) in selected.iter().enumerate() {
        let Some(run) = runs.get(index) else {
            continue;
        };
        if !(run.timed_out || run.exit_code != Some(0)) {
            continue;
        }
        let tail = exec::log_tail(&run.output, LOG_TAIL_LINES);
        if tail.is_empty() {
            continue;
        }
        let log = report
            .producers
            .get(&entry.id)
            .map(|record| record.log.clone())
            .unwrap_or_default();
        eprintln!("--- last {} line(s) of {log} ---", tail.len());
        for line in tail {
            eprintln!("{line}");
        }
    }
    (exit_code, Some(out_dir))
}

/// The runner plus the wrapper's history step.
pub fn main(args: &Args) -> i32 {
    let (status, out_dir) = battery(args);
    if args.no_history {
        return status;
    }
    let Some(run_dir) = out_dir else {
        eprintln!(
            "mandate-check: warning: no run directory found, so this run was not \
             archived or compared"
        );
        return status;
    };
    let history_status = history::history(&run_dir, args.history_label.as_deref());
    if status != 0 {
        return status;
    }
    history_status
}

fn expand_tilde(path: &Path) -> PathBuf {
    let text = path.display().to_string();
    if (text == "~" || text.starts_with("~/"))
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(text.trim_start_matches('~').trim_start_matches('/'));
    }
    path.to_path_buf()
}

/// `datetime.now(timezone.utc).isoformat(timespec="seconds")`.
pub fn now_iso8601() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or(0);
    let days = (seconds / 86_400) as i64;
    let secs_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}+00:00",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// Howard Hinnant's `civil_from_days`.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_timestamp_is_pythons_isoformat_with_a_utc_offset() {
        let stamp = now_iso8601();
        assert_eq!(stamp.len(), 25, "{stamp}");
        assert!(stamp.ends_with("+00:00"), "{stamp}");
        assert_eq!(&stamp[4..5], "-");
        assert_eq!(&stamp[10..11], "T");
    }

    #[test]
    fn the_civil_date_algorithm_agrees_with_a_known_epoch() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_000), (2022, 1, 8));
    }
}
