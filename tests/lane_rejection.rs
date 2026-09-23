//! Pin the five server-side lane-rejection classifications at the live
//! `serve`/accept boundary.
//!
//! `serve` builds its `LaneRejectionLog` internally (in
//! `serve_with_handler`), so a test cannot reach into that log directly. The
//! observable side channel is the per-class metrics counter that
//! [`LaneRejectionLog::record`] increments (`src/lane_rejection.rs`); each
//! test below drives a real `RtpMuxServer::serve` on loopback with real
//! `rtp::udp::FrameDeliveryIo` client lanes and raw lane hellos, then
//! asserts the documented metric for the expected rejection class moved by
//! exactly one.
//!
//! The metric-name mapping itself is pinned separately by
//! `each_lane_rejection_class_reports_its_documented_metric_name`
//! (`src/lane_rejection.rs`). Together the two close the loop: mutating the
//! classification at any of the five call sites in `src/server.rs`
//! (`Capacity`, `HelloTimeout`, `HelloParse`, `ClassMismatch`, `GroupFull`)
//! makes exactly the test that owns that call site fail.

#![allow(clippy::disallowed_methods)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use mux::{GroupToken, LaneClass, PairingNonce};
use rtp_mux::{RtpMuxServer, RtpMuxServerConfig, SessionSpawner};
use tokio::io::AsyncWriteExt;

mod support;

use support::{TEST_TASK_QUEUE_BOUND, TestScope};

// The documented metric names from `src/lane_rejection.rs`; the tests below
// are the other half of that pin — each names the class that must land in
// the metric when the live serve boundary rejects a lane.
const METRIC_CAPACITY: &str = "stream.rtp_mux.capacity_rejected";
const METRIC_HELLO_TIMEOUT: &str = "stream.rtp_mux.hello_timeout";
const METRIC_HELLO_PARSE: &str = "stream.rtp_mux.hello_parse_error";
const METRIC_CLASS_MISMATCH: &str = "stream.rtp_mux.class_mismatch";
const METRIC_GROUP_FULL: &str = "stream.rtp_mux.group_full";
// Two auxiliary counters the tests synchronise on (not rejection classes).
const METRIC_RTP_ACCEPTS: &str = "stream.rtp_mux.rtp.accepts";
const METRIC_PAIRED: &str = "stream.rtp_mux.paired";

/// The hello frame length written by `mux::write_lane_hello`: 1 class byte +
/// 16-byte pairing nonce + 16-byte group token.
const HELLO_LEN: usize = 1 + 16 + 16;
/// The interactive lane's pending-slot budget per peer IP
/// (`crate::shared::MAX_PENDING_LANES_PER_PEER`), which this suite cannot
/// name directly because `shared` is `pub(crate)`.
const MAX_PENDING_LANES_PER_PEER: usize = 32;
/// A lane that never transmits is never accepted at the rtp layer (the
/// server-side accept completes only once the client's datagrams flow), so
/// the mux `HelloTimeout`/pending-slot paths are driven with a *partial*
/// hello: enough bytes to materialise the rtp session and land in
/// `read_lane_hello`'s `read_exact`, never enough to complete the 33-byte
/// hello.
const PARTIAL_HELLO_LEN: usize = 5;

// ---------------------------------------------------------------------------
// In-process metrics recorder
// ---------------------------------------------------------------------------

/// A minimal `metrics` recorder capturing the counter increments this suite
/// asserts on. The `metrics` crate keeps one process-global recorder and
/// caches each metric handle at first use, so the recorder is installed once
/// (before any server runs in this binary) and the counters accumulate for
/// the whole suite; every test snapshots before/after its own window.
#[derive(Default)]
struct RecordingRecorder {
    counters: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

impl RecordingRecorder {
    fn cell(&self, name: &str) -> Arc<AtomicU64> {
        let mut counters = self.counters.lock().unwrap();
        counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    fn value(&self, name: &str) -> u64 {
        let counters = self.counters.lock().unwrap();
        counters
            .get(name)
            .map(|cell| cell.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// Forwards `Counter::increment`/`absolute` onto the shared atomic cell.
struct AtomicCounter(Arc<AtomicU64>);

impl metrics::CounterFn for AtomicCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

impl metrics::Recorder for RecordingRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::new(AtomicCounter(self.cell(key.name()))))
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

fn recorder() -> &'static RecordingRecorder {
    static RECORDER: OnceLock<RecordingRecorder> = OnceLock::new();
    let recorder = RECORDER.get_or_init(RecordingRecorder::default);
    // The first test installs it; a repeat install is `AlreadySet` for the
    // same recorder and is ignored.
    let _ = metrics::set_global_recorder(recorder);
    recorder
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The tests share the per-IP pending-lane budget and the process-global
/// metrics recorder, so they run one at a time.
static SERIAL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn serialized() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// Interactive client lane, mirroring `lane_transport::connect_config(
/// LaneClass::Interactive, …)` as the connector's dial builds it: FEC on
/// with the interactive tuning, reordering frame delivery, shared
/// congestion intent, handshake off to match `spawn_server`.
fn interactive_config() -> rtp::udp::ConnectConfig<'static> {
    rtp::udp::ConnectConfig {
        handshake: false,
        fec: true,
        fec_tuning: rtp::FecTuning::interactive_prompt(),
        instream_group_fec: rtp::udp::AcceptConfig::default().instream_group_fec,
        frame_delivery: rtp::FrameMode::enabled_reordering(),
        congestion_lane: rtp::CongestionLane::Shared,
        ..rtp::udp::ConnectConfig::default()
    }
}

/// Bulk client lane, mirroring the connector's bulk dial: FEC-free, strict
/// frame delivery, dedicated congestion intent.
fn bulk_config() -> rtp::udp::ConnectConfig<'static> {
    rtp::udp::ConnectConfig {
        handshake: false,
        fec: false,
        frame_delivery: rtp::FrameMode::default(),
        congestion_lane: rtp::CongestionLane::Dedicated,
        ..rtp::udp::ConnectConfig::default()
    }
}

/// Bind a real handshake-free dual-lane server and run `serve` inside the
/// scope; returns the interactive and bulk listener addresses.
async fn spawn_server(scope: &mut TestScope) -> (SocketAddr, SocketAddr) {
    let server = RtpMuxServer::bind("127.0.0.1:0", RtpMuxServerConfig::default())
        .await
        .expect("bind the rejection-test server")
        .with_handshake(false);
    let interactive = server.listener().local_addr();
    let bulk = server.bulk_listener().local_addr();
    let submitter = scope.submitter(TEST_TASK_QUEUE_BOUND);
    let spawner = SessionSpawner::new({
        let submitter = submitter.clone();
        move |fut| {
            submitter.submit(fut);
        }
    });
    scope.spawn_required("lane-rejection serve loop", async move {
        let _ = server.serve(spawner, |_| {}).await;
    });
    (interactive, bulk)
}

/// A raw client lane: a real RTP session to the live server, bound on the
/// same IP with an ephemeral port.
async fn dial(
    server: SocketAddr,
    config: rtp::udp::ConnectConfig<'static>,
) -> rtp::udp::FrameDeliveryIo {
    let bind = SocketAddr::new(server.ip(), 0);
    rtp::udp::FrameDeliveryIo::connect(bind, server, config)
        .await
        .expect("the raw client lane must connect to the live server")
}

/// Poll the recorder until `name` reaches `at_least`. A timeout fails the
/// test with a message naming the metric and the rejection class it
/// represents, so a mutation that re-classifies a rejection under a
/// different class reddens with the owning metric's name in the panic.
async fn wait_for_metric(name: &str, at_least: u64, what: &str, cutoff: Duration) {
    let started = std::time::Instant::now();
    loop {
        let observed = recorder().value(name);
        if observed >= at_least {
            return;
        }
        assert!(
            started.elapsed() < cutoff,
            "timed out after {cutoff:?} waiting for {what}: the '{name}' metric stayed at {observed} (needed {at_least}), so the server-side lane rejection was not recorded under that class",
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// The five server-side lane-rejection classifications
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_that_sends_a_garbled_hello_is_recorded_as_hello_parse() {
    let _serial = serialized().await;
    let before = recorder().value(METRIC_HELLO_PARSE);
    let mut scope = TestScope::new();
    let (interactive, _bulk) = spawn_server(&mut scope).await;
    scope
        .run(async {
            let mut io = dial(interactive, interactive_config()).await;
            // A full-length hello whose first byte is not a lane-class byte
            // (Interactive = 0xD1, Bulk = 0xD2): `read_lane_hello` completes
            // the read and fails the class parse, so the lane is rejected as
            // HelloParse rather than timing out or mis-matching.
            io.write
                .write_all(&[0x00u8; HELLO_LEN])
                .await
                .expect("write the garbled hello");
            wait_for_metric(
                METRIC_HELLO_PARSE,
                before + 1,
                "the garbled-hello rejection",
                Duration::from_secs(10),
            )
            .await;
        })
        .await;
    assert_eq!(
        recorder().value(METRIC_HELLO_PARSE),
        before + 1,
        "a lane whose hello does not parse must be counted under '{METRIC_HELLO_PARSE}' exactly once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_that_declares_the_wrong_class_is_recorded_as_class_mismatch() {
    let _serial = serialized().await;
    let before = recorder().value(METRIC_CLASS_MISMATCH);
    let mut scope = TestScope::new();
    let (interactive, _bulk) = spawn_server(&mut scope).await;
    scope
        .run(async {
            let mut io = dial(interactive, interactive_config()).await;
            // A well-formed Bulk hello on the interactive lane: the class
            // parses, then fails the expected-class check.
            mux::write_lane_hello(
                &mut io.write,
                LaneClass::Bulk,
                PairingNonce::generate(),
                GroupToken::generate(),
            )
            .await
            .expect("write the mismatched-class hello");
            wait_for_metric(
                METRIC_CLASS_MISMATCH,
                before + 1,
                "the mismatched-class rejection",
                Duration::from_secs(10),
            )
            .await;
        })
        .await;
    assert_eq!(
        recorder().value(METRIC_CLASS_MISMATCH),
        before + 1,
        "a lane whose hello declares the wrong class must be counted under '{METRIC_CLASS_MISMATCH}' exactly once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_that_sends_no_hello_is_recorded_as_hello_timeout() {
    let _serial = serialized().await;
    let before = recorder().value(METRIC_HELLO_TIMEOUT);
    let mut scope = TestScope::new();
    let (interactive, _bulk) = spawn_server(&mut scope).await;
    scope
        .run(async {
            let mut io = dial(interactive, interactive_config()).await;
            // Send a hello that can never complete: the forbid bytes land the
            // lane inside `read_lane_hello`'s `read_exact`, where it stays
            // silent (no full hello) until the serve side's HELLO_DEADLINE
            // elapses and rejects it as HelloTimeout. A fully silent client
            // is never even accepted at the rtp layer, so this is the closest
            // the live boundary gets to "the hello never arrived".
            io.write
                .write_all(&[0x00u8; PARTIAL_HELLO_LEN])
                .await
                .expect("write the never-completing hello prefix");
            wait_for_metric(
                METRIC_HELLO_TIMEOUT,
                before + 1,
                "the silent-lane rejection (HELLO_DEADLINE elapsed)",
                Duration::from_secs(15),
            )
            .await;
            let _ = io;
        })
        .await;
    assert_eq!(
        recorder().value(METRIC_HELLO_TIMEOUT),
        before + 1,
        "a lane that sends no complete hello must be counted under '{METRIC_HELLO_TIMEOUT}' exactly once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_over_the_per_peer_pending_cap_is_recorded_as_capacity() {
    let _serial = serialized().await;
    let before = recorder().value(METRIC_CAPACITY);
    let mut scope = TestScope::new();
    let (interactive, _bulk) = spawn_server(&mut scope).await;
    scope
        .run(async {
            // Hold one live lane per pending slot of the per-IP budget: each
            // dial writes a hello *prefix* that can never complete, so its
            // rtp session is accepted and its admission permit stays held in
            // `read_lane_hello`'s `read_exact` until the hello deadline.
            let mut held: Vec<rtp::udp::FrameDeliveryIo> =
                Vec::with_capacity(MAX_PENDING_LANES_PER_PEER);
            for _ in 0..MAX_PENDING_LANES_PER_PEER {
                let mut io = dial(interactive, interactive_config()).await;
                io.write
                    .write_all(&[0x00u8; PARTIAL_HELLO_LEN])
                    .await
                    .expect("write the flood lane's never-completing hello prefix");
                held.push(io);
            }
            wait_for_metric(
                METRIC_RTP_ACCEPTS,
                MAX_PENDING_LANES_PER_PEER as u64,
                "all flood lanes to be admitted",
                Duration::from_secs(15),
            )
            .await;
            // The next lane from the same peer IP exceeds the per-peer cap
            // before its hello can even be read. It is driven with a single
            // raw datagram rather than `dial`: one rtp frame write is sent
            // with immediate retransmission armor (several datagrams for a
            // single write), and because a rejected lane's connection is torn
            // down, every later datagram from that source opens a fresh
            // connection and is rejected again.  A single datagram is what
            // makes this lane exactly one rejectable connection.
            let extra = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            extra.connect(interactive).await.unwrap();
            extra
                .send(&[0x00u8; PARTIAL_HELLO_LEN])
                .await
                .expect("send the 33rd lane's hello prefix");
            wait_for_metric(
                METRIC_CAPACITY,
                before + 1,
                "the over-capacity lane rejection",
                Duration::from_secs(10),
            )
            .await;
            drop((held, extra));
        })
        .await;
    assert_eq!(
        recorder().value(METRIC_CAPACITY),
        before + 1,
        "a lane over the per-peer pending cap must be counted under '{METRIC_CAPACITY}' exactly once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_presenting_a_full_group_token_is_recorded_as_group_full() {
    let _serial = serialized().await;
    let before = recorder().value(METRIC_GROUP_FULL);
    let mut scope = TestScope::new();
    let (interactive, bulk) = spawn_server(&mut scope).await;
    scope
        .run(async {
            let group = GroupToken::generate();
            // Two distinct dual-lane sessions on the same group token fill
            // the group (members = 2): each is a raw dial with its own nonce
            // and the correct lane classes, and pairing/join happen
            // server-side without any client mux.
            let mut sessions: Vec<rtp::udp::FrameDeliveryIo> = Vec::new();
            for _ in 0..2 {
                let nonce = PairingNonce::generate();
                let mut interactive_io = dial(interactive, interactive_config()).await;
                let mut bulk_io = dial(bulk, bulk_config()).await;
                mux::write_lane_hello(
                    &mut interactive_io.write,
                    LaneClass::Interactive,
                    nonce,
                    group,
                )
                .await
                .expect("write the first session's interactive hello");
                mux::write_lane_hello(&mut bulk_io.write, LaneClass::Bulk, nonce, group)
                    .await
                    .expect("write the first session's bulk hello");
                sessions.push(interactive_io);
                sessions.push(bulk_io);
            }
            wait_for_metric(
                METRIC_PAIRED,
                2,
                "both sessions to pair",
                Duration::from_secs(15),
            )
            .await;
            // `paired` fires just before the group join; give both joins a
            // beat to land before presenting the third lane.
            tokio::time::sleep(Duration::from_millis(150)).await;
            let mut third = dial(interactive, interactive_config()).await;
            mux::write_lane_hello(
                &mut third.write,
                LaneClass::Interactive,
                PairingNonce::generate(),
                group,
            )
            .await
            .expect("write the third lane's hello");
            wait_for_metric(
                METRIC_GROUP_FULL,
                before + 1,
                "the third lane's group-full rejection",
                Duration::from_secs(10),
            )
            .await;
            drop(sessions);
        })
        .await;
    assert_eq!(
        recorder().value(METRIC_GROUP_FULL),
        before + 1,
        "a lane presenting an already-full group token must be counted under '{METRIC_GROUP_FULL}' exactly once"
    );
}
