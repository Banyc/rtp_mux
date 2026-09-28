//! A **saturating downstream** bulk lane carrying a Minecraft-shaped
//! interactive lane, built on the crate's **public** entry points — the same
//! ones a real caller has — and asserting where the traffic actually landed
//! rather than what the arm asked for.
//!
//! ```sh
//! cargo test --release -p rtp_mux --test minecraft_contested -- \
//!     --ignored --nocapture --test-threads=1
//! ```
//!
//! # The public path this arm calls, and why these are the caller's entry
//! points
//!
//! | side | entry point | why it is the caller's |
//! | --- | --- | --- |
//! | server | [`RtpMuxServer::bind`] + [`RtpMuxServer::serve`] with a stream handler | the crate's only public server: `bind` binds the interactive/bulk listener pair and `serve` hands each accepted [`ServerStream`] to the caller's handler. There is no test-only server construction here. |
//! | client session | [`RtpMuxConnector::with_config`] with [`RtpMuxConnectorConfig::standard`] | the documented connector; `standard` carries the shipped lane policy ([`crate::shared::interactive_lane_fec_policy`], the bulk-lane FEC-off mapping, the shipped bulk-address derivation). |
//! | bulk destination | [`BulkAddrSelector`] returning the bulk lane's own proxy address | a deployed chain puts the two lanes behind *separate* proxies, so the connector's bulk destination is a caller-supplied selector, not the `port + 1` default. This is the same field the crate's own `standard` config uses for the default. |
//! | opening a stream | [`RtpMuxConnector::connect_stream`] (no lane argument) and [`RtpMuxConnector::connect_stream_with_lane`] | the two public ways to open a stream. `connect_stream` is the *default* path — it takes no lane, so the lane it opens on is the product's own decision, not the arm's label. |
//! | reading the lane back | [`ServerStream::source_lane`] | the product's own observable: the lane the accepted stream actually arrived on. |
//!
//! **Finding: `rtp_mux` has no shape-based lane classifier on its public
//! path.** The lane is a caller-declared [`LaneClass`] argument
//! ([`RtpMuxConnector::connect_stream_with_lane`], whose no-lane sibling
//! defaults to [`LaneClass::Interactive`]), and that is the entire public
//! decision surface. The one shape classifier in the stack —
//! `mux::DualStreamOpener::open_migrating`, which migrates a stream on a write
//! larger than `AUTO_BULK_THRESHOLD` — lives on `mux`'s
//! `DualStreamOpener`, which `rtp_mux` does not re-export. So there is nothing
//! on this crate's public path that could *classify* Minecraft-shaped traffic
//! and nothing for this arm to put that traffic through; what the arm can do,
//! and does, is hold the product to the lane its own default names and then
//! read the lane back. A misclassification by a future classifier would be
//! caught by the assertion below, but the classifier itself is not reachable
//! from here, which is a fact about where the lane decision is made today.
//!
//! # The lane assertion
//!
//! The server's handler records `(mode tag, [`ServerStream::source_lane`])` for
//! every accepted stream, and the arm asserts:
//!
//! * **every Minecraft-shaped stream is on [`LaneClass::Interactive`]** — the
//!   traffic the operator's client sends must stay interactive;
//! * **the bulk stream is on [`LaneClass::Bulk`]** — if both flows land on one
//!   lane the arm's whole claim is confounded, so the failure is loud rather
//!   than a scenario that quietly passes because the bulk never went near the
//!   interactive lane;
//! * every expected stream appears exactly once, so a stream that silently
//!   failed to open is not read as a clean run.
//!
//! # The shape being reproduced, and which numbers are assumptions
//!
//! The operator's live overlay reading (Minecraft 26.2 / Fabric, Hypixel via
//! BungeeCord to a Hygot server), recorded as **read-approximate** — its
//! millisecond labels are partly covered by on-screen chat. This arm reproduces
//! the *shape*.
//!
//! | shape | arm | source |
//! | --- | --- | --- |
//! | **one long-lived session carries everything** | one connector session per run; every stream opened once off it and carried for the run's whole window | operator |
//! | **≈1:40, downstream-dominated** (60 tx / 2352 rx) | a small steady upstream plus a large downstream; the bulk saturation is **downstream only** | operator. The arm prints its own offered ratio (≈43:1); the measured `1:40` is approximate, so the ratio is reproduced rather than the count. |
//! | **many small downstream datagrams** | 300 B frames every 20 ms (50/s) | operator ("almost every inbound datagram is a tiny entity/world-state update"; the trace sits in the low-KiB range) |
//! | **occasional large downstream bursts** | one 512 KiB frame every 5 s | operator ("1–2.6 MiB/s for chunk/entity bursts"; "multi-hundred-KB-to-MiB-scale"). The 5 s period is this arm's choice and is labelled an assumption. |
//! | **small steady upstream (input, movement, acks)** | 48 B every 25 ms, echoed by the server so the arm also has a round trip | operator |
//! | **ping floor ≈100 ms RTT** | 50 ms one-way each direction | operator ("99 ms min, ~129 ms avg") |
//! | **peaks ≈7× the floor** (≈299 ms avg spike, ≈751 ms max) | the reading the arm reports and bounds; not injected | operator |
//! | 2 % iid loss, 10 ms jitter, 1 MiB/s shared downstream capacity | the link | **assumptions.** The operator's trace fixes no loss, jitter or capacity; 2 % iid is this workspace's clean-arm convention, the capacity sits inside the operator's own 64 B/s–1.0 MiB/s axis, and jitter is deliberately small (10 ms) so any tail the arm reports is the **queue's**, not the link's. |
//!
//! # "Saturating" means the downstream shared queue is the limit
//!
//! The bulk lane is one stream on [`LaneClass::Bulk`] that the **server**
//! pushes back to back — it waits on no clock, so its offer is whatever the
//! transport's congestion window admits. Two measured facts, both asserted:
//! **A closed-loop sender cannot be measured offering more than the link
//! carries**, so "saturating" is not an offer-ratio claim: the RTP sender's
//! congestion window converges to the link rate, and its wire offer therefore
//! reads ≈1.0x the capacity whether the link is saturated or merely matched.
//! The arm instead asserts the chain that does distinguish the two:
//!
//! 1. **the demand is unbounded by construction** — the push loop has no clock
//!    and no sleep;
//! 2. **the demand was actually limited by the transport** — the writer spent
//!    at least [`SATURATION_WRITE_AWAIT_FRACTION`] of its window awaiting
//!    `write_all` backpressure;
//! 3. **the link was the constraint** — the shared shaper's serialization
//!    backlog was non-empty for at least [`SATURATION_BACKLOG_FRACTION`] of the
//!    window;
//! 4. **the link passed its capacity** — the aggregate downstream the shaper
//!    forwarded is at least [`SATURATION_PASSED_FRACTION`] of it.
//!
//! The bulk lane's own offered ratio is printed beside them, labelled
//! informational for exactly the reason above.
//!
//! # How this differs from the existing families
//!
//! **The direction is the difference.** Every shared-bottleneck arm in this
//! workspace saturates the **client→server** direction and measures the
//! interactive lane on that same direction: `dynamic_contested::dyn_game_sync_*`
//! (closest in load shape — a saturating bulk upload behind an interactive
//! cadence on one shared `BottleneckShaper`, but 200 B / 25 ms with a single
//! 8 KiB write at the start and an assertion that only some deltas arrived),
//! `contested_latency::contested_*`, and `hol_probe::hol_*` (whose
//! shared-bottleneck arms vary the *link* — cap400, rtt100 GE5/GE1, rtt40,
//! hostile — and the bulk's pace, never the direction). `mandate_smoke`'s arms
//! have no shared shaper at all. The cell this closes is
//! `saturating-bulk@direction=s2c`, and
//! [`mc_bulk_direction_decomposition`] measures the same interactive lane
//! against a downstream, an upstream and a symmetric saturating bulk, one
//! dimension apart, so the operator's hypothesis — that a change helping a
//! *symmetric* contest may do nothing here — is what the pair decides.
//!
//! # What the direction sweep measured, and the lever it refused
//!
//! One run per direction, one dimension apart, at load 5.0 on ten cores
//! (`MC_RUNS` unset, `MC_WINDOW_SECS` 20):
//!
//! | bulk direction | rtt p50 | p90 | p99 | max | burst p50 | shaper backlog | bulk delivered |
//! | --- | --- | --- | --- | --- | --- | --- | --- |
//! | **downstream** (this arm's shape) | 98.5 | 132.2 | 194.7 | 228.5 | 1277.7 | 0.772 | 0.778 |
//! | upstream | 97.0 | 138.3 | 168.6 | 228.2 | 1092.6 | 0.121 | 0.000 |
//! | both | 102.1 | 140.1 | 262.9 | 293.3 | 1519.7 | 0.653 | 0.665 |
//!
//! (ms for the latency columns; fractions for the last two.)
//!
//! **The operator's hypothesis is only half right, and the sweep says which
//! half.** Direction moves the *median* -- the small frames' p50 is 58.0 ms
//! downstream and 46.2 ms upstream, so the saturated downstream queue costs
//! about 12 ms there -- but it barely moves the *p99* (194.7 against 168.6).
//! The tail is therefore **not** mostly downstream queueing: an interactive
//! lane whose downstream link is idle still reads a 168.6 ms p99 and a 1092 ms
//! burst, because both are set by the lane's **own** cost at a 100 ms RTT -- a
//! 2 % iid loss realization's repair, and the 512 KiB burst's own serialization
//! plus reassembly floor (`512 KiB / 1 MiB/s = 512 ms`, measured 1092 ms
//! unloaded). The saturating bulk's whole contribution to this shape is
//! ~26 ms on the p99 and ~185 ms on the burst; the both-directions arm, which
//! doubles the demand on the same shaper, adds the remaining ~68 ms.
//!
//! **The one `rtp_mux`-owned lever over that contribution was measured and
//! refused.** The bulk lane's congestion intent
//! (`lane_transport::congestion_lane`) is the only caller-invisible policy this
//! crate owns that acts on the bulk connection's standing window, and
//! `LaneClass::Bulk => CongestionLane::Shared` was measured on this arm,
//! interleaved with the production value, two runs each at load 9.0-18.2:
//!
//! | bulk `congestion_lane` | rtt p50 | p90 | p99 | max | burst p50 |
//! | --- | --- | --- | --- | --- | --- |
//! | `Dedicated` (production) | 103.2 | 138.3 | 244.0 | 287.0 | 1511.9 |
//! | `Shared` | 600.4 | 694.2 | 714.4 | 1327.1 | 2404.3 |
//!
//! `Shared` is **6x worse at the median and 2.9x at the p99**, and its round
//! trips fell to 66 from 500 -- the lane was too slow to keep its own cadence.
//! The reason is in the tuning: `Shared` carries a `0.20` gentle probe gain
//! against `Dedicated`'s `0.02`, so it probes further past capacity every cycle
//! and leaves a deeper standing queue, while `Dedicated`'s `0.90` drain keeps
//! its own queue shallow. **`Dedicated` is the right value here even though its
//! documented reason is wrong for this deployment**: `lane_transport` justifies
//! it with "no competing traffic over its connection's queue", and in this
//! deployment the bulk connection's queue *is* the interactive lane's queue.
//! The value survives; the stated reason does not, and the measured numbers
//! above are what settles it. The tree was restored with `touch` and the
//! restored `lane_transport.rs` verified byte-identical to the pre-probe copy.
//!
//! # Composition, and what it does not cover
//!
//! A **stated composite**: the production shape needs the small cadence, the
//! bursts and the small upstream at once, so they ride separate streams on the
//! one interactive connection and the arm prints a series per stream. The
//! one-dimension arm (one shape under the same bulk) is not built; the series
//! split stands in for it.
//!
//! **One probe was attempted and refused as an instrument defect.** A
//! `jitter_tail` fault (700 ms of one-way jitter, the delay left alone, chosen
//! to raise the tail while every write stays fast) **passed** the tripwire it
//! was written to fail. The reason is the round-trip instrument and not the
//! tripwire: the client pairs each echo *arrival* with the most recent *send*,
//! so a reordered link's readings attribute one send's round trip to another's
//! arrival and average the tail away. The fault was therefore deleted rather
//! than kept as a green selector, and the limitation is recorded below.
//!
//! It does **not** cover: the four-flow split (M4's arms own it); a 15 s
//! *periodic keepalive* as a distinct shape (the round trip here runs at the
//! input cadence, 40/s); a client *uploading* chunk data; a hostile/GE loss
//! model; reordering or duplication; the deployed proxy chain (loopback only —
//! the arm places the two lanes behind separate netem proxies, which is what
//! the chain does, but it is not BungeeCord); byte-for-byte bulk payload
//! integrity (the arm counts frames and payload bytes structurally;
//! `bidirectional`/`duplex`/`xsession` own the byte-level claim); and a
//! **shape-based lane classifier**, which this crate does not expose — see the
//! finding above. If a classifier is added later, this arm's lane tap is where
//! its decision would be read.
//!
//! # The fault selectors (the arm's vacuity demonstrations)
//!
//! `MC_CONTESTED_FAULT` perturbs an arm's **input**, never its assertion:
//!
//! * `slow` — +250 ms one-way on both directions. Measured: the offer still
//!   completes (`600/600` and `3/3`) while the round trip's p99 reads
//!   `836.4 ms` against the `450 ms` tripwire, so it is the **tripwire's** own
//!   probe. At +600 ms the *same* fault fails the offer floor first
//!   (`484/600`) — a fixed delay eventually costs the schedule, which is why
//!   the shift is the one that separates the two clauses;
//! * `no_bulk` — the bulk stream is opened but its push is zero-length, so the
//!   saturation chain must fail (measured: `push_await_fraction 0.000`);
//! * `offer_cut` — the downstream cadence and the upstream input are offered a
//!   tenth of their schedule, so the offer floor must fail on the count;
//! * `bulk_interactive` — the bulk stream is opened on the **interactive**
//!   lane, so the lane assertion must fail naming the tag and the lane it
//!   landed on. That is the probe of the check the operator asked for.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::stats::percentile;
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{BottleneckShaper, NetemConfig, NetemPair};
use rtp::cc::CcSignalHub;
use rtp_mux::{
    BindSelector, BulkAddrSelector, ExplorerConfig, LaneClass, RtpMuxConnector,
    RtpMuxConnectorConfig, RtpMuxServer, RtpMuxServerConfig, ServerStream, SessionSpawner,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ───────────────────────────── the Minecraft shape ──────────────────────────
//
// Every constant is either the operator's reading or an assumption the module
// doc names as one.

/// The downstream small-frame cadence: 20 ms, 50 frames/second.
const SMALL_PERIOD: Duration = Duration::from_millis(20);

/// A small downstream frame.
const SMALL_FRAME_BYTES: usize = 300;

/// A chunk/entity burst, written as **one** frame so the mux decides the
/// fragmentation.
const BURST_FRAME_BYTES: usize = 512 * 1024;

/// The burst period — this arm's choice (see the module doc).
const BURST_PERIOD: Duration = Duration::from_secs(5);

/// The upstream input cadence: 40/s.
const INPUT_PERIOD: Duration = Duration::from_millis(25);

/// An upstream input packet.
const INPUT_FRAME_BYTES: usize = 48;

/// The saturating bulk push's write size.
const BULK_CHUNK_BYTES: usize = 16 * 1024;

/// The upstream bulk upload's write size.
const BULK_UPLOAD_CHUNK_BYTES: usize = 64 * 1024;

// ──────────────────────────────── the link ──────────────────────────────────

/// One-way delay: the operator's ping floor is ≈100 ms RTT.
const OWD: Duration = Duration::from_millis(50);

/// Jitter — **assumed**, kept small so any tail the arm reports is the queue's.
const JITTER: Duration = Duration::from_millis(10);

/// The independent loss threshold — this workspace's clean-arm convention.
const LOSS_PCT: u32 = 2;

/// The **shared downstream** capacity: 1 MiB/s.
const DOWN_RATE_BPS: u64 = 8 * 1024 * 1024;

/// The **shared upstream** capacity: the same rate with no bulk on it.
const UP_RATE_BPS: u64 = 8 * 1024 * 1024;

/// Bulk ramp before the measured window.
const RAMP: Duration = Duration::from_millis(1500);

/// The measured window.
const WINDOW: Duration = Duration::from_secs(20);

/// Grace after the window.
const GRACE: Duration = Duration::from_secs(3);

/// The shared downstream shaper must pass at least this fraction of its
/// configured capacity. A saturated link passes its capacity; an idle one does
/// not.
const SATURATION_PASSED_FRACTION: f64 = 0.8;

/// The shared downstream backlog must be non-empty for at least this fraction
/// of the sampled window: the standing queue that makes the link, rather than
/// the sender, the constraint.
const SATURATION_BACKLOG_FRACTION: f64 = 0.5;

/// The saturating push's writer must have spent at least this fraction of its
/// window awaiting transport backpressure. Its demand is unbounded *by
/// construction* (the loop has no clock and no sleep); this is the measurement
/// that the transport, not the arm, is what limited it.
const SATURATION_WRITE_AWAIT_FRACTION: f64 = 0.5;

/// Delivery floor for each interactive series.
const MC_DELIVERY_FLOOR: f64 = 0.99;

/// The offered schedule must be reached to within this tolerance.
const MC_OFFER_TOLERANCE: f64 = 0.02;

/// The bulk goodput floor, as a fraction of the shared capacity.
const MC_BULK_GOODPUT_FLOOR: f64 = 0.35;

/// Runs per arm. One before/after pair is a draw rather than a property.
const MC_RUNS: usize = 3;

/// Per-run wall-clock budget.
const MC_RUN_TIMEOUT: Duration = Duration::from_secs(75);

// ────────────────────────── the assertions' bounds ──────────────────────────
//
// The bounds and their derivation live with the arm's declaration in
// `rtp_mux/GATE.md`; the constants here carry the numbers that derivation
// produced.

/// The interactive round trip's p99 tripwire, in ms.
///
/// **Measured, then bounded**: the production policy's pooled p99 over four
/// runs on this host -- three in one arm invocation (229.1 ms) and one in the
/// direction sweep (194.7 ms) -- at `uptime` 1-minute load 4.0-18.2 on ten
/// cores. The tripwire is `450 ms`, ~1.8x the worst observation, so a change
/// that at least doubles this lane's tail fails here naming the value it saw.
/// A single run is a draw, not a property: the same revision's pooled p99 moved
/// 194.7-244.0 ms across those four runs.
const MC_RTT_P99_TRIPWIRE_MS: f64 = 450.0;

/// The loose ceiling printed beside the tripwire, so a breach inside the
/// tripwire is still visible as a number rather than as a pass.
const MC_RTT_P99_CEILING_MS: f64 = 750.0;

// ─────────────────────────── the mode protocol ──────────────────────────────

/// The mode tag a client writes first on every stream. The server's handler
/// reads it to learn what the caller wants on this stream.
const MODE_SHAPED: u8 = b'R';
const MODE_BURST: u8 = b'T';
const MODE_ECHO: u8 = b'E';
const MODE_BULK_DOWN: u8 = b'B';
const MODE_BULK_UP: u8 = b'S';

/// A downstream shaping request: `ramp | run | frame_bytes | period`, four
/// little-endian `u64`s. The shape is the arm's, so it travels with the arm.
#[derive(Clone, Copy, Debug)]
struct ShapeRequest {
    ramp: Duration,
    run_for: Duration,
    frame_bytes: usize,
    period: Duration,
}

impl ShapeRequest {
    const WIRE_BYTES: usize = 32;

    fn encode(&self) -> [u8; Self::WIRE_BYTES] {
        words([
            self.ramp.as_nanos() as u64,
            self.run_for.as_nanos() as u64,
            self.frame_bytes as u64,
            self.period.as_nanos() as u64,
        ])
    }
}

/// A bulk push/upload request: `ramp | run | chunk_bytes`.
#[derive(Clone, Copy, Debug)]
struct BulkRequest {
    ramp: Duration,
    run_for: Duration,
    chunk_bytes: usize,
}

impl BulkRequest {
    const WIRE_BYTES: usize = 24;

    fn encode(&self) -> [u8; Self::WIRE_BYTES] {
        let all = [
            self.ramp.as_nanos() as u64,
            self.run_for.as_nanos() as u64,
            self.chunk_bytes as u64,
            0,
            0,
            0,
        ];
        let mut out = [0u8; Self::WIRE_BYTES];
        for (i, word) in all.iter().take(3).enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&word.to_le_bytes());
        }
        out
    }
}

fn words(values: [u64; 4]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, v) in values.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&v.to_le_bytes());
    }
    out
}

fn read_u64(bytes: &[u8], index: usize) -> u64 {
    u64::from_le_bytes(bytes[index * 8..index * 8 + 8].try_into().unwrap())
}

/// A timestamped frame: `u32 len | payload | u64 send-timestamp-us`.
fn make_frame(msg_bytes: usize, fill: u8, base: Instant) -> Vec<u8> {
    assert!(msg_bytes >= 12, "frame needs a length and a stamp");
    let sent_us = base.elapsed().as_micros() as u64;
    let mut frame = Vec::with_capacity(msg_bytes);
    frame.extend_from_slice(&(msg_bytes as u32).to_le_bytes());
    frame.resize(msg_bytes - 8, fill);
    frame.extend_from_slice(&sent_us.to_le_bytes());
    frame
}

fn stamp(frame: &mut [u8], base: Instant) {
    let n = frame.len();
    let now = base.elapsed().as_micros() as u64;
    frame[n - 8..].copy_from_slice(&now.to_le_bytes());
}

/// Pull one whole frame off the front of `buf`.
fn take_frame(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len < 12 || buf.len() < len {
        return None;
    }
    let frame = buf[..len].to_vec();
    buf.drain(..len);
    Some(frame)
}

fn frame_one_way_ms(frame: &[u8], base: Instant) -> f64 {
    let n = frame.len();
    let mut tail = [0u8; 8];
    tail.copy_from_slice(&frame[n - 8..]);
    let sent_us = u64::from_le_bytes(tail);
    let now_us = base.elapsed().as_micros() as u64;
    now_us.saturating_sub(sent_us) as f64 / 1000.0
}

// ─────────────────────────── the server handler ─────────────────────────────

/// Where each accepted stream's tag landed, read from the product's own
/// [`ServerStream::source_lane`] rather than from the arm's request.
#[derive(Default)]
struct LaneTap {
    seen: Mutex<Vec<(u8, LaneClass)>>,
    /// The echo stream's reaching-leg one-way latencies, read on the server:
    /// the upstream half of the round trip, so the round trip can be attributed
    /// to a direction rather than only reported whole.
    up_one_way: Mutex<Vec<f64>>,
    /// The saturating push's own accounting: `(writes, await nanos)`. The loop
    /// has no clock, so its demand is unbounded; the await time is the
    /// measurement that the transport was what limited it.
    push_accounting: Mutex<(u64, u64)>,
}

impl LaneTap {
    fn record(&self, tag: u8, lane: LaneClass) {
        self.seen.lock().unwrap().push((tag, lane));
    }
}

/// The server side of one stream, dispatched by the mode tag the client wrote
/// first: the production server hands the caller a [`ServerStream`] and the
/// caller decides what to do with it.
async fn serve_stream(stream: ServerStream, tap: Arc<LaneTap>, base: Instant) {
    let lane = stream.source_lane();
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut tag = [0u8; 1];
    if reader.read_exact(&mut tag).await.is_err() {
        return;
    }
    tap.record(tag[0], lane);
    match tag[0] {
        MODE_ECHO => {
            // The client's input/movement/ack cadence, echoed: the round trip
            // the operator's ping measures.
            let mut buf: Vec<u8> = Vec::with_capacity(1 << 16);
            let mut chunk = vec![0u8; 16 * 1024];
            while let Ok(n) = reader.read(&mut chunk).await {
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                while let Some(frame) = take_frame(&mut buf) {
                    tap.up_one_way
                        .lock()
                        .unwrap()
                        .push(frame_one_way_ms(&frame, base));
                    if writer.write_all(&frame).await.is_err() {
                        return;
                    }
                }
            }
        }
        MODE_SHAPED | MODE_BURST => {
            let mut header = [0u8; ShapeRequest::WIRE_BYTES];
            if reader.read_exact(&mut header).await.is_err() {
                return;
            }
            let request = ShapeRequest {
                ramp: Duration::from_nanos(read_u64(&header, 0)),
                run_for: Duration::from_nanos(read_u64(&header, 1)),
                frame_bytes: read_u64(&header, 2) as usize,
                period: Duration::from_nanos(read_u64(&header, 3)),
            };
            push_shaped(&mut writer, request, base).await;
        }
        MODE_BULK_DOWN => {
            let mut header = [0u8; BulkRequest::WIRE_BYTES];
            if reader.read_exact(&mut header).await.is_err() {
                return;
            }
            let request = BulkRequest {
                ramp: Duration::from_nanos(read_u64(&header, 0)),
                run_for: Duration::from_nanos(read_u64(&header, 1)),
                chunk_bytes: read_u64(&header, 2) as usize,
            };
            let accounting = push_bulk(&mut writer, request, base).await;
            let cell = &tap.push_accounting;
            let mut guard = cell.lock().unwrap();
            *guard = (guard.0 + accounting.0, guard.1 + accounting.1);
        }
        MODE_BULK_UP => {
            // A sink: the client's upload is counted structurally so the run
            // can say whether anything arrived at all.
            let mut chunk = vec![0u8; 64 * 1024];
            while let Ok(n) = reader.read(&mut chunk).await {
                if n == 0 {
                    break;
                }
            }
        }
        _ => {}
    }
    let _ = writer.shutdown().await;
}

/// Write `frame_bytes`-sized timestamped frames every `period`, after `ramp`,
/// for `run_for`.
async fn push_shaped(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    request: ShapeRequest,
    base: Instant,
) {
    if request.frame_bytes < 12 {
        return;
    }
    if !request.ramp.is_zero() {
        tokio::time::sleep(request.ramp).await;
    }
    let start = Instant::now();
    let end = start + request.run_for;
    let mut frame = make_frame(request.frame_bytes, b'm', base);
    let mut next = start;
    loop {
        let now = Instant::now();
        if now >= end {
            break;
        }
        if next > now {
            tokio::time::sleep(next - now).await;
            continue;
        }
        stamp(&mut frame, base);
        if writer.write_all(&frame).await.is_err() {
            return;
        }
        next += request.period;
        if next <= now {
            next = now + request.period;
        }
    }
}

/// Write `chunk_bytes`-sized timestamped frames **back to back** — the
/// saturating push: it waits on no clock, so only the transport's own
/// backpressure can slow it.
async fn push_bulk(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    request: BulkRequest,
    base: Instant,
) -> (u64, u64) {
    if request.chunk_bytes < 12 {
        return (0, 0);
    }
    if !request.ramp.is_zero() {
        tokio::time::sleep(request.ramp).await;
    }
    let end = Instant::now() + request.run_for;
    let mut frame = make_frame(request.chunk_bytes, 0xA5, base);
    let mut writes = 0u64;
    let mut await_nanos = 0u64;
    while Instant::now() < end {
        stamp(&mut frame, base);
        let started = Instant::now();
        let result = writer.write_all(&frame).await;
        await_nanos += started.elapsed().as_nanos() as u64;
        if result.is_err() {
            break;
        }
        writes += 1;
    }
    (writes, await_nanos)
}

// ─────────────────────────── the result and reader ──────────────────────────

#[derive(Default)]
struct McRun {
    down_small: Vec<f64>,
    down_burst: Vec<f64>,
    up_one_way: Vec<f64>,
    rtt: Vec<f64>,
    small_due: u64,
    burst_due: u64,
    input_sent: u64,
    int_down_offered_bytes: u64,
    int_up_offered_bytes: u64,
    down_bulk_offered_bytes: u64,
    down_bulk_delivered_bytes: u64,
    /// The aggregate downstream the two lanes' shapers forwarded, plus the bulk
    /// push's own write accounting.
    down_passed_bytes: u64,
    push_writes: u64,
    push_await_nanos: u64,
    up_bulk_offered_bytes: u64,
    up_bulk_delivered_bytes: u64,
    down_backlog_nonzero: f64,
    up_backlog_nonzero: f64,
    lanes: Vec<(u8, LaneClass)>,
    active_secs: f64,
    wall: Duration,
}

impl McRun {
    fn down_capacity_bytes_per_sec() -> f64 {
        DOWN_RATE_BPS as f64 / 8.0
    }

    /// The aggregate downstream forwarded, as a fraction of the shared
    /// capacity. A saturated link passes its capacity.
    fn down_passed_fraction(&self) -> f64 {
        let capacity = McRun::down_capacity_bytes_per_sec() * self.active_secs;
        if capacity <= 0.0 {
            0.0
        } else {
            self.down_passed_bytes as f64 / capacity
        }
    }

    /// The fraction of the push's window it spent awaiting backpressure.
    fn push_await_fraction(&self) -> f64 {
        let wall = self.active_secs * 1e9;
        if wall <= 0.0 {
            0.0
        } else {
            self.push_await_nanos as f64 / wall
        }
    }

    fn down_bulk_goodput(&self) -> f64 {
        if self.active_secs <= 0.0 {
            0.0
        } else {
            self.down_bulk_delivered_bytes as f64 / self.active_secs
        }
    }

    fn down_offered_bps(&self) -> f64 {
        if self.active_secs <= 0.0 {
            0.0
        } else {
            self.down_bulk_offered_bytes as f64 * 8.0 / self.active_secs
        }
    }

    fn up_offered_bps(&self) -> f64 {
        if self.active_secs <= 0.0 {
            0.0
        } else {
            self.up_bulk_offered_bytes as f64 * 8.0 / self.active_secs
        }
    }
}

/// `(p50, p90, p99, max)` over a sample set. `percentile` takes a **sorted**
/// slice, so every reading goes through here rather than sorting at one call
/// site and not at another — an unsorted slice reads as a plausible-looking
/// number in a ladder that is not monotone, which is how one printed row read
/// `p50 88.0 p90 87.8`.
fn quantiles(samples: &[f64]) -> (f64, f64, f64, f64) {
    if samples.is_empty() {
        return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (
        percentile(&sorted, 0.50),
        percentile(&sorted, 0.90),
        percentile(&sorted, 0.99),
        sorted[sorted.len() - 1],
    )
}

fn series_row(label: &str, samples: &[f64]) -> String {
    if samples.is_empty() {
        return format!("[mc {label}] n=0 (no samples)");
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    format!(
        "[mc {label}] n={} p50={:.1} p90={:.1} p99={:.1} max={:.1} ms",
        sorted.len(),
        percentile(&sorted, 0.50),
        percentile(&sorted, 0.90),
        percentile(&sorted, 0.99),
        sorted[sorted.len() - 1],
    )
}

fn lane_name(lane: LaneClass) -> &'static str {
    match lane {
        LaneClass::Interactive => "interactive",
        LaneClass::Bulk => "bulk",
    }
}

fn print_run(dir: Direction, index: usize, run: &McRun, window: Duration) {
    let label = format!("mc-{}", dir.label());
    println!(
        "[{label} run {index}] wall={:.1}s active={:.1}s window={:.0}s",
        run.wall.as_secs_f64(),
        run.active_secs,
        window.as_secs_f64()
    );
    println!("{}", series_row("down-small", &run.down_small));
    println!("{}", series_row("down-burst", &run.down_burst));
    println!("{}", series_row("up-one-way", &run.up_one_way));
    println!("{}", series_row("round-trip", &run.rtt));
    let goodput = run.down_bulk_goodput();
    println!(
        "[{label} bulk run {index}] down_offered={:.3} MiB/s ({:.2}x down capacity, informational: \
         a closed-loop sender converges to the link rate) down_delivered={:.3} MiB/s \
         down_passed_fraction={:.3} down_backlog_nonzero={:.3} push_writes={} \
         push_await_fraction={:.3} up_offered={:.3} MiB/s",
        run.down_offered_bps() / 8.0 / (1024.0 * 1024.0),
        run.down_offered_bps() / DOWN_RATE_BPS as f64,
        goodput / (1024.0 * 1024.0),
        run.down_passed_fraction(),
        run.down_backlog_nonzero,
        run.push_writes,
        run.push_await_fraction(),
        run.up_offered_bps() / 8.0 / (1024.0 * 1024.0),
    );
    println!(
        "[{label} upstream run {index}] up_offered={:.3} MiB/s up_forwarded={} B \
         up_backlog_nonzero={:.3}",
        run.up_offered_bps() / 8.0 / (1024.0 * 1024.0),
        run.up_bulk_delivered_bytes,
        run.up_backlog_nonzero,
    );
    println!(
        "[{label} offer run {index}] down_small {}/{} | down_burst {}/{} | input {} | \
         interactive offered down {} B up {} B",
        run.down_small.len(),
        run.small_due,
        run.down_burst.len(),
        run.burst_due,
        run.input_sent,
        run.int_down_offered_bytes,
        run.int_up_offered_bytes,
    );
    println!(
        "[{label} asymmetry run {index}] interactive offered down {:.0} B/s : up {:.0} B/s = \
         {:.1}:1",
        run.int_down_offered_bytes as f64 / run.active_secs.max(1e-9),
        run.int_up_offered_bytes as f64 / run.active_secs.max(1e-9),
        if run.int_up_offered_bytes == 0 {
            f64::INFINITY
        } else {
            run.int_down_offered_bytes as f64 / run.int_up_offered_bytes as f64
        },
    );
    let lanes: Vec<String> = run
        .lanes
        .iter()
        .map(|(tag, lane)| format!("{}={}", *tag as char, lane_name(*lane)))
        .collect();
    println!(
        "[{label} lanes run {index}] {} (from ServerStream::source_lane)",
        lanes.join(" ")
    );
}

// ─────────────────────────────── the run body ───────────────────────────────

/// Which direction the saturating bulk load runs in. The interactive lane's
/// shape is identical in all three; only this differs, which is what makes the
/// direction table an attribution rather than a description.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    /// The operator's shape: the server pushes bulk downstream.
    Downstream,
    /// The mirror: the client uploads the bulk.
    Upstream,
    /// Both, at once.
    Symmetric,
}

impl Direction {
    fn label(self) -> &'static str {
        match self {
            Direction::Downstream => "downstream",
            Direction::Upstream => "upstream",
            Direction::Symmetric => "symmetric",
        }
    }

    fn pushes_down(self) -> bool {
        matches!(self, Direction::Downstream | Direction::Symmetric)
    }

    fn uploads_up(self) -> bool {
        matches!(self, Direction::Upstream | Direction::Symmetric)
    }
}

fn fault() -> Option<String> {
    let value = std::env::var("MC_CONTESTED_FAULT").ok()?;
    let value = value.trim().to_owned();
    if value.is_empty() { None } else { Some(value) }
}

fn runs() -> usize {
    std::env::var("MC_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MC_RUNS)
}

fn window() -> Duration {
    std::env::var("MC_WINDOW_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(WINDOW)
}

fn bind_selector() -> BindSelector {
    Arc::new(|addr: SocketAddr| match addr {
        SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
    })
}

/// The link profile: 50 ms one-way, 10 ms jitter, 2 % iid loss, and **no
/// per-pair rate** — the rate lives on the shared [`BottleneckShaper`], so the
/// arm can say which queue a sample waited in.
fn link_config(seed: u64, shift: Duration, jitter: Duration) -> NetemConfig {
    NetemConfig {
        rate: 0,
        loss: ((LOSS_PCT as f64 / 100.0) * u32::MAX as f64) as u32,
        latency: OWD + shift,
        jitter: JITTER + jitter,
        queue_limit_pkts: 4096,
        seed,
        ..NetemConfig::default()
    }
}

/// Read a stream to its end, returning every frame's one-way latency.
async fn read_latency_frames(
    read: &mut (impl tokio::io::AsyncRead + Unpin),
    base: Instant,
) -> Vec<f64> {
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut chunk = vec![0u8; 64 * 1024];
    let mut samples = Vec::new();
    while let Ok(n) = read.read(&mut chunk).await {
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        while let Some(frame) = take_frame(&mut buf) {
            samples.push(frame_one_way_ms(&frame, base));
        }
    }
    samples
}

/// Read a stream to its end, returning `(frames, payload bytes)`. Structural
/// only: the frame's own length field bounds each read, so a desynchronized
/// stream cannot be counted as payload.
async fn read_payload_bytes(read: &mut (impl tokio::io::AsyncRead + Unpin)) -> (u64, u64) {
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut chunk = vec![0u8; 64 * 1024];
    let mut frames = 0u64;
    let mut bytes = 0u64;
    while let Ok(n) = read.read(&mut chunk).await {
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        while let Some(frame) = take_frame(&mut buf) {
            frames += 1;
            bytes += (frame.len() - 12) as u64;
        }
    }
    (frames, bytes)
}

/// Read a stream to its end and count the bytes (the upload sink's peer side
/// counts nothing; the shaper's own counters carry the offered figure).
async fn drain(read: &mut (impl tokio::io::AsyncRead + Unpin)) {
    let mut chunk = vec![0u8; 64 * 1024];
    while let Ok(n) = read.read(&mut chunk).await {
        if n == 0 {
            break;
        }
    }
}

/// The input round trip: write a small frame, wait for its echo, record the
/// round trip. This is the operator's ping.
async fn offer_input_round_trips(
    write: &mut (impl tokio::io::AsyncWrite + Unpin),
    read: &mut (impl tokio::io::AsyncRead + Unpin),
    base: Instant,
    run_for: Duration,
    start_after: Duration,
    interval: Duration,
) -> (u64, Vec<f64>) {
    let start = Instant::now();
    if !start_after.is_zero() {
        tokio::time::sleep(start_after).await;
    }
    let mut deadline = start_after;
    let mut sent = 0u64;
    let mut rtts = Vec::new();
    let mut echoed = vec![0u8; INPUT_FRAME_BYTES];
    while start.elapsed() < run_for {
        while start.elapsed() < deadline {
            tokio::time::sleep(deadline.saturating_sub(start.elapsed())).await;
        }
        let frame = make_frame(INPUT_FRAME_BYTES, b'K', base);
        let sent_at = Instant::now();
        if write.write_all(&frame).await.is_err() {
            break;
        }
        sent += 1;
        let budget = (run_for + GRACE).saturating_sub(start.elapsed());
        match tokio::time::timeout(budget, read.read_exact(&mut echoed)).await {
            Ok(Ok(_)) => rtts.push(sent_at.elapsed().as_secs_f64() * 1000.0),
            _ => break,
        }
        deadline += interval;
    }
    (sent, rtts)
}

/// Write a cyclic payload back to back until the window's deadline.
async fn upload_bulk(
    write: &mut (impl tokio::io::AsyncWrite + Unpin),
    base: Instant,
    win: Duration,
) {
    let payload = cyclic_payload(64 * 1024 * 1024);
    let mut offset = 0usize;
    let deadline = RAMP + win;
    while base.elapsed() < deadline {
        let take = BULK_UPLOAD_CHUNK_BYTES.min(payload.len() - offset);
        match write.write(&payload[offset..offset + take]).await {
            Ok(0) | Err(_) => break,
            Ok(n) => offset = (offset + n) % payload.len(),
        }
    }
    // Close the stream: the server's sink ends on EOF, and the client's own
    // drain of the read half waits on that.
    let _ = write.shutdown().await;
}

fn fault_shift(fault: Option<&str>) -> Duration {
    if fault == Some("slow") {
        Duration::from_millis(250)
    } else {
        Duration::ZERO
    }
}

fn divisors(fault: Option<&str>) -> (u32, u32) {
    if fault == Some("offer_cut") {
        (10, 10)
    } else {
        (1, 1)
    }
}

fn fraction_used(nonzero: &AtomicU64, samples: &AtomicU64) -> f64 {
    let total = samples.load(Ordering::Relaxed);
    if total == 0 {
        0.0
    } else {
        nonzero.load(Ordering::Relaxed) as f64 / total as f64
    }
}

/// One run: one long-lived connector session, the production server behind two
/// netem proxies, and one saturating bulk direction. Every stream is opened
/// once off the session and carried for the run's whole window.
async fn run_mc(
    seed: u64,
    win: Duration,
    dir: Direction,
    fault: Option<&str>,
    cc_link: Option<CcSignalHub>,
) -> McRun {
    let shift = fault_shift(fault);
    let want_bulk = fault != Some("no_bulk");
    let bulk_wrong_lane = fault == Some("bulk_interactive");
    let (small_div, input_div) = divisors(fault);

    let base = Instant::now();
    let wall = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let outcome = tasks
        .run(async {
            // ── the production server, on the public path ─────────────────
            let server = RtpMuxServer::bind(
                "127.0.0.1:0",
                RtpMuxServerConfig {
                    cc_link: cc_link.clone(),
                    ..RtpMuxServerConfig::default()
                },
            )
                .await
                .unwrap();
            let int_server = server.listener().local_addr();
            let bulk_server = server.bulk_listener().local_addr();
            let tap = Arc::new(LaneTap::default());
            {
                let tap = Arc::clone(&tap);
                let task_tx = task_tx.clone();
                submit_test_task(
                    &task_tx.clone(),
                    Box::pin(async move {
                        let spawner = SessionSpawner::new({
                            let task_tx = task_tx.clone();
                            move |fut| submit_test_task(&task_tx, fut)
                        });
                        let _ = server
                            .serve(spawner, move |stream| {
                                let task_tx = task_tx.clone();
                                submit_test_task(
                                    &task_tx,
                                    Box::pin(serve_stream(stream, Arc::clone(&tap), base)),
                                );
                            })
                            .await;
                    }),
                );
            }

            // ── one shared bottleneck per direction, shared by both lanes ──
            let down_shaper = BottleneckShaper::new(DOWN_RATE_BPS, 0);
            let up_shaper = BottleneckShaper::new(UP_RATE_BPS, 0);
            let int_pair = NetemPair::spawn_shared(
                int_server,
                link_config(seed, shift, Duration::ZERO),
                link_config(seed + 1, shift, Duration::ZERO),
                Some(up_shaper.clone()),
                Some(down_shaper.clone()),
            )
            .unwrap();
            let bulk_pair = NetemPair::spawn_shared(
                bulk_server,
                link_config(seed + 10, shift, Duration::ZERO),
                link_config(seed + 11, shift, Duration::ZERO),
                Some(up_shaper.clone()),
                Some(down_shaper.clone()),
            )
            .unwrap();

            // ── the production connector, on the public path ──────────────
            let bulk_proxy_addr = bulk_pair.client_addr();
            let bulk_addr: BulkAddrSelector = Arc::new(move |_| Ok(bulk_proxy_addr));
            let (connector, driver) = RtpMuxConnector::with_config(RtpMuxConnectorConfig {
                bulk_addr,
                cc_link,
                explorer: ExplorerConfig {
                    enabled: false,
                    ..ExplorerConfig::default()
                },
                ..RtpMuxConnectorConfig::standard(bind_selector())
            });
            submit_test_task(&task_tx, Box::pin(driver));
            let int_proxy_addr = int_pair.client_addr();

            // ── the streams, opened once off the one session ──────────────
            // `connect_stream` takes no lane, so the lane it opens on is the
            // product's own decision; the tap is what checks it.
            let small = connector.connect_stream(int_proxy_addr).await.unwrap();
            let bursts = connector.connect_stream(int_proxy_addr).await.unwrap();
            let input = connector.connect_stream(int_proxy_addr).await.unwrap();
            let bulk_lane = if bulk_wrong_lane {
                LaneClass::Interactive
            } else {
                LaneClass::Bulk
            };
            let bulk = connector
                .connect_stream_with_lane(int_proxy_addr, bulk_lane)
                .await
                .unwrap();

            let (mut small_r, mut small_w) = tokio::io::split(small);
            let (mut burst_r, mut burst_w) = tokio::io::split(bursts);
            let (mut input_r, mut input_w) = tokio::io::split(input);
            let (bulk_r, mut bulk_w) = tokio::io::split(bulk);

            small_w.write_all(&[MODE_SHAPED]).await.unwrap();
            small_w
                .write_all(
                    &ShapeRequest {
                        ramp: RAMP,
                        run_for: win,
                        frame_bytes: SMALL_FRAME_BYTES,
                        period: SMALL_PERIOD * small_div,
                    }
                    .encode(),
                )
                .await
                .unwrap();
            burst_w.write_all(&[MODE_BURST]).await.unwrap();
            burst_w
                .write_all(
                    &ShapeRequest {
                        ramp: RAMP,
                        run_for: win,
                        frame_bytes: BURST_FRAME_BYTES,
                        period: BURST_PERIOD,
                    }
                    .encode(),
                )
                .await
                .unwrap();
            input_w.write_all(&[MODE_ECHO]).await.unwrap();

            // The bulk direction: a downstream push, an upstream upload, or
            // both. `Symmetric` needs two bulk-lane streams because the
            // server's push handler never reads and the upload sink never
            // writes.
            let bulk_request = BulkRequest {
                ramp: RAMP,
                run_for: win,
                chunk_bytes: BULK_CHUNK_BYTES,
            };
            let mut bulk_down_halves = None;
            let mut bulk_up_halves = None;
            {
                match dir {
                    Direction::Downstream => {
                        let request = if want_bulk {
                            bulk_request
                        } else {
                            BulkRequest {
                                run_for: Duration::ZERO,
                                ..bulk_request
                            }
                        };
                        bulk_w.write_all(&[MODE_BULK_DOWN]).await.unwrap();
                        bulk_w.write_all(&request.encode()).await.unwrap();
                        bulk_down_halves = Some((bulk_r, bulk_w));
                    }
                    Direction::Upstream => {
                        bulk_w.write_all(&[MODE_BULK_UP]).await.unwrap();
                        bulk_up_halves = Some((bulk_r, bulk_w));
                    }
                    Direction::Symmetric => {
                        let request = if want_bulk {
                            bulk_request
                        } else {
                            BulkRequest {
                                run_for: Duration::ZERO,
                                ..bulk_request
                            }
                        };
                        bulk_w.write_all(&[MODE_BULK_DOWN]).await.unwrap();
                        bulk_w.write_all(&request.encode()).await.unwrap();
                        bulk_down_halves = Some((bulk_r, bulk_w));
                        // The push handler never reads and the upload sink never
                        // writes, so the symmetric contest needs two streams.
                        let second = connector
                            .connect_stream_with_lane(int_proxy_addr, LaneClass::Bulk)
                            .await
                            .unwrap();
                        let (r2, mut w2) = tokio::io::split(second);
                        w2.write_all(&[MODE_BULK_UP]).await.unwrap();
                        bulk_up_halves = Some((r2, w2));
                    }
                }
            }

            // Backlog sampling: the measured evidence that the shaper, not the
            // sender, is what limits each direction's offer.
            let down_backlog = Arc::new(AtomicU64::new(0));
            let down_samples = Arc::new(AtomicU64::new(0));
            let up_backlog = Arc::new(AtomicU64::new(0));
            let up_samples = Arc::new(AtomicU64::new(0));
            for (shaper, nonzero, samples) in [
                (
                    down_shaper.clone(),
                    Arc::clone(&down_backlog),
                    Arc::clone(&down_samples),
                ),
                (
                    up_shaper.clone(),
                    Arc::clone(&up_backlog),
                    Arc::clone(&up_samples),
                ),
            ] {
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let deadline = RAMP + win;
                        while base.elapsed() < deadline {
                            if shaper.backlog_bytes(Instant::now()) > 0 {
                                nonzero.fetch_add(1, Ordering::Relaxed);
                            }
                            samples.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }),
                );
            }

            let small_read = read_latency_frames(&mut small_r, base);
            let burst_read = read_latency_frames(&mut burst_r, base);
            let input_flow = offer_input_round_trips(
                &mut input_w,
                &mut input_r,
                base,
                win,
                RAMP,
                INPUT_PERIOD * input_div,
            );
            let bulk_down_flow = async {
                let Some((mut r, w)) = bulk_down_halves else {
                    return 0u64;
                };
                let (_, bytes) = read_payload_bytes(&mut r).await;
                // Hold the write half until the push's window has closed, so
                // the stream is never closed from this side mid-push.
                drop(w);
                bytes
            };
            let bulk_up_flow = async {
                let Some((mut r, mut w)) = bulk_up_halves else {
                    return;
                };
                if want_bulk {
                    let park = drain(&mut r);
                    tokio::join!(park, upload_bulk(&mut w, base, win));
                } else {
                    drain(&mut r).await;
                }
            };

            let (down_small, down_burst, (input_sent, rtts), down_bulk_delivered, ()) =
                tokio::join!(small_read, burst_read, input_flow, bulk_down_flow, bulk_up_flow);

            tokio::time::sleep(GRACE).await;
            let int_down = int_pair.stats_s2c();
            let int_up = int_pair.stats_c2s();
            let bulk_down = bulk_pair.stats_s2c();
            let bulk_up = bulk_pair.stats_c2s();
            let active_secs = win.saturating_sub(RAMP).as_secs_f64();
            // The server's push runs `win` after its own ramp: it writes the
            // first frame immediately and then one per period while the clock is
            // still inside the window, so `ceil(win / period)` frames are owed.
            let ceil_div = |window: Duration, period: Duration| -> u64 {
                window.as_nanos().div_ceil(period.as_nanos()) as u64
            };
            // The *declared* offer is the tick; the fault's divisor slows the
            // request only. Dividing both sides would make the offer floor
            // unfalsifiable -- and it did: the first `offer_cut` probe passed.
            let small_due = ceil_div(win, SMALL_PERIOD);
            let burst_due = ceil_div(win, BURST_PERIOD);
            let mut lanes = tap.seen.lock().unwrap().clone();
            lanes.sort_by_key(|(tag, _)| *tag);
            let up_one_way = std::mem::take(&mut *tap.up_one_way.lock().unwrap());
            let (push_writes, push_await_nanos) = *tap.push_accounting.lock().unwrap();
            let run = McRun {
                down_small,
                down_burst,
                up_one_way,
                rtt: rtts,
                small_due,
                burst_due,
                input_sent,
                int_down_offered_bytes: int_down.received_bytes,
                int_up_offered_bytes: int_up.received_bytes,
                down_bulk_offered_bytes: if dir.pushes_down() {
                    bulk_down.received_bytes
                } else {
                    0
                },
                down_bulk_delivered_bytes: down_bulk_delivered,
                down_passed_bytes: int_down.forwarded_bytes + bulk_down.forwarded_bytes,
                push_writes,
                push_await_nanos,
                up_bulk_offered_bytes: if dir.uploads_up() {
                    bulk_up.received_bytes
                } else {
                    0
                },
                up_bulk_delivered_bytes: if dir.uploads_up() {
                    bulk_up.forwarded_bytes
                } else {
                    0
                },
                down_backlog_nonzero: fraction_used(&down_backlog, &down_samples),
                up_backlog_nonzero: fraction_used(&up_backlog, &up_samples),
                lanes,
                active_secs,
                wall: Duration::ZERO,
            };
            int_pair.stop();
            bulk_pair.stop();
            run
        })
        .await;
    let mut run = outcome;
    run.wall = wall.elapsed();
    run
}

// ──────────────────────────────── the arms ──────────────────────────────────

async fn run_arm(dir: Direction, win: Duration, count: usize) -> Vec<McRun> {
    let fault = fault();
    let mut arms = Vec::new();
    for index in 0..count {
        let seed = 900 + 17 * index as u64;
        let run = with_timeout(
            MC_RUN_TIMEOUT,
            "minecraft_contested run",
            run_mc(seed, win, dir, fault.as_deref(), None),
        )
        .await;
        print_run(dir, index, &run, win);
        arms.push(run);
    }
    arms
}

/// `runs`, pooled per series.
struct Pooled {
    down_small: Vec<f64>,
    down_burst: Vec<f64>,
    rtt: Vec<f64>,
    up_one_way: Vec<f64>,
    pooled: Vec<f64>,
    down_offered_ratio: f64,
    down_backlog: f64,
    passed_fraction: f64,
    push_await_fraction: f64,
    delivered_fraction: f64,
    small_seen: u64,
    small_due: u64,
    burst_seen: u64,
    burst_due: u64,
    input_seen: u64,
    input_due: u64,
    lanes: Vec<(u8, LaneClass)>,
}

fn pool(runs: &[McRun]) -> Pooled {
    let mut p = Pooled {
        down_small: runs.iter().flat_map(|r| r.down_small.clone()).collect(),
        down_burst: runs.iter().flat_map(|r| r.down_burst.clone()).collect(),
        rtt: runs.iter().flat_map(|r| r.rtt.clone()).collect(),
        up_one_way: runs.iter().flat_map(|r| r.up_one_way.clone()).collect(),
        pooled: Vec::new(),
        down_offered_ratio: runs
            .iter()
            .map(|r| r.down_offered_bps() / DOWN_RATE_BPS as f64)
            .fold(f64::INFINITY, f64::min),
        down_backlog: runs
            .iter()
            .map(|r| r.down_backlog_nonzero)
            .fold(f64::INFINITY, f64::min),
        passed_fraction: runs
            .iter()
            .map(|r| r.down_passed_fraction())
            .fold(f64::INFINITY, f64::min),
        push_await_fraction: runs
            .iter()
            .map(|r| r.push_await_fraction())
            .fold(f64::INFINITY, f64::min),
        delivered_fraction: 0.0,
        small_seen: runs.iter().map(|r| r.down_small.len() as u64).sum(),
        small_due: runs.iter().map(|r| r.small_due).sum(),
        burst_seen: runs.iter().map(|r| r.down_burst.len() as u64).sum(),
        burst_due: runs.iter().map(|r| r.burst_due).sum(),
        input_seen: runs.iter().map(|r| r.rtt.len() as u64).sum(),
        input_due: runs.iter().map(|r| r.input_sent).sum(),
        lanes: runs.iter().flat_map(|r| r.lanes.clone()).collect(),
    };
    let active: f64 = runs.iter().map(|r| r.active_secs).sum();
    let sink: u64 = runs.iter().map(|r| r.down_bulk_delivered_bytes).sum();
    p.delivered_fraction = if active > 0.0 {
        (sink as f64 / active) / McRun::down_capacity_bytes_per_sec()
    } else {
        0.0
    };
    let mut all: Vec<f64> = p
        .down_small
        .iter()
        .chain(p.down_burst.iter())
        .chain(p.up_one_way.iter())
        .chain(p.rtt.iter())
        .cloned()
        .collect();
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    p.pooled = all;
    p
}

fn print_pooled(dir: Direction, count: usize, win: Duration, p: &Pooled) {
    let label = format!("mc-{}", dir.label());
    println!(
        "[{label} pooled] runs={count} window={:.0}s pooled n={} p50={:.1} p90={:.1} p99={:.1} \
         max={:.1} ms",
        win.as_secs_f64(),
        p.pooled.len(),
        percentile(&p.pooled, 0.50),
        percentile(&p.pooled, 0.90),
        percentile(&p.pooled, 0.99),
        p.pooled.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
    );
    println!("{}", series_row("pooled-down-small", &p.down_small));
    println!("{}", series_row("pooled-down-burst", &p.down_burst));
    println!("{}", series_row("pooled-up-one-way", &p.up_one_way));
    println!("{}", series_row("pooled-round-trip", &p.rtt));
    println!(
        "[{label} pooled bulk] min_down_offered_ratio={:.2}x (informational) \
         min_down_backlog_nonzero={:.3} min_passed_fraction={:.3} \
         min_push_await_fraction={:.3} bulk_delivered_fraction={:.3} \
         tripwire={MC_RTT_P99_TRIPWIRE_MS} ceiling={MC_RTT_P99_CEILING_MS} ms",
        p.down_offered_ratio,
        p.down_backlog,
        p.passed_fraction,
        p.push_await_fraction,
        p.delivered_fraction,
    );
    println!(
        "[{label} pooled delivery] down_small {}/{} ({:.4}) down_burst {}/{} input {}/{} ({:.4})",
        p.small_seen,
        p.small_due,
        p.small_seen as f64 / p.small_due.max(1) as f64,
        p.burst_seen,
        p.burst_due,
        p.input_seen,
        p.input_due,
        p.input_seen as f64 / p.input_due.max(1) as f64,
    );
}

/// Every lane the server observed, one entry per accepted stream, so a reader
/// can see the decision rather than the arm's request.
fn print_lanes(dir: Direction, p: &Pooled) {
    let counts_for = |tag: u8| -> (usize, usize) {
        let lanes: Vec<LaneClass> = p
            .lanes
            .iter()
            .filter(|(t, _)| *t == tag)
            .map(|(_, l)| *l)
            .collect();
        (
            lanes
                .iter()
                .filter(|l| **l == LaneClass::Interactive)
                .count(),
            lanes.iter().filter(|l| **l == LaneClass::Bulk).count(),
        )
    };
    let mut row = Vec::new();
    for tag in [
        MODE_SHAPED,
        MODE_BURST,
        MODE_ECHO,
        MODE_BULK_DOWN,
        MODE_BULK_UP,
    ] {
        let (interactive, bulk) = counts_for(tag);
        row.push(format!(
            "{}:interactive={interactive},bulk={bulk}",
            tag as char
        ));
    }
    println!("[mc-{} lanes] {}", dir.label(), row.join(" | "));
}

/// The operator's shape: a saturating **downstream** bulk lane behind a
/// Minecraft-shaped interactive lane, over one long-lived session.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Minecraft-shaped interactive lane behind a saturating downstream bulk on one shared \
            bottleneck; three ~25 s runs (MC_RUNS), full tier; run with --ignored --nocapture \
            --test-threads=1 (see module header)"]
async fn mc_downstream_saturating_bulk() {
    let win = window();
    let count = runs();
    let arms = run_arm(Direction::Downstream, win, count).await;
    let p = pool(&arms);
    print_pooled(Direction::Downstream, count, win, &p);
    print_lanes(Direction::Downstream, &p);

    // ── where the traffic landed, from the product's own surface ───────────
    // "I asked for interactive" is not evidence: every Minecraft-shaped stream
    // must be *reported* on the interactive lane by `ServerStream::source_lane`
    // and the bulk stream must be reported on the bulk lane, or the arm's whole
    // claim is confounded.
    let count_on = |tag: u8| -> (usize, usize) {
        let lanes: Vec<LaneClass> = p
            .lanes
            .iter()
            .filter(|(t, _)| *t == tag)
            .map(|(_, l)| *l)
            .collect();
        (
            lanes
                .iter()
                .filter(|l| **l == LaneClass::Interactive)
                .count(),
            lanes.iter().filter(|l| **l == LaneClass::Bulk).count(),
        )
    };
    let expected_streams = count * 4;
    assert_eq!(
        p.lanes.len(),
        expected_streams,
        "[mc] the server accepted {} streams where {expected_streams} were opened; a stream that \
         never arrived is not a clean run (observed: {:?})",
        p.lanes.len(),
        p.lanes
            .iter()
            .map(|(t, l)| (*t as char, lane_name(*l)))
            .collect::<Vec<_>>(),
    );
    for tag in [MODE_SHAPED, MODE_BURST, MODE_ECHO] {
        let (interactive, bulk) = count_on(tag);
        assert_eq!(
            (interactive, bulk),
            (count, 0),
            "[mc] the Minecraft-shaped stream '{}' was observed on {} interactive and {} bulk \
             lanes ({} run(s)); the interactive lane is where this traffic must stay",
            tag as char,
            interactive,
            bulk,
            count,
        );
    }
    let (bulk_interactive, bulk_count) = count_on(MODE_BULK_DOWN);
    assert_eq!(
        (bulk_interactive, bulk_count),
        (0, count),
        "[mc] the saturating bulk stream was observed on {} interactive and {} bulk lanes ({} \
         run(s)); if the bulk load shares the interactive lane the arm is measuring itself",
        bulk_interactive,
        bulk_count,
        count,
    );

    // ── the instrument's own sanity ───────────────────────────────────────
    assert!(
        p.down_small.len() >= 30 && !p.down_burst.is_empty() && !p.rtt.is_empty(),
        "[mc] the arm produced no usable samples: down_small n={} down_burst n={} rtt n={}",
        p.down_small.len(),
        p.down_burst.len(),
        p.rtt.len(),
    );
    assert!(
        p.small_seen as f64 >= p.small_due as f64 * (1.0 - MC_OFFER_TOLERANCE),
        "[mc] the downstream small-frame offer was not carried out: {} frames of {} due ({:.4}, \
         tolerance {MC_OFFER_TOLERANCE})",
        p.small_seen,
        p.small_due,
        p.small_seen as f64 / p.small_due.max(1) as f64,
    );
    assert!(
        p.burst_seen as f64 >= p.burst_due as f64 * (1.0 - MC_OFFER_TOLERANCE),
        "[mc] the downstream burst offer was not carried out: {} frames of {} due",
        p.burst_seen,
        p.burst_due,
    );
    assert!(
        p.input_seen as f64 >= p.input_due as f64 * MC_DELIVERY_FLOOR,
        "[mc] the input round trip delivered {}/{} echoes, below the {MC_DELIVERY_FLOOR} floor",
        p.input_seen,
        p.input_due,
    );

    // ── the bulk lane is saturating, downstream ───────────────────────────
    // The chain a closed-loop sender supports: unbounded demand by
    // construction, a transport that actually blocked it, a standing queue at
    // the shared shaper, and a shaper passing its capacity. An offer ratio
    // cannot distinguish a saturated link from a matched one, so it is printed
    // and not asserted.
    assert!(
        p.push_await_fraction >= SATURATION_WRITE_AWAIT_FRACTION,
        "[mc] the saturating push was never blocked: it spent only {:.3} of its window awaiting \
         backpressure (floor {SATURATION_WRITE_AWAIT_FRACTION}), so the transport was not what \
         limited its unbounded demand",
        p.push_await_fraction,
    );
    assert!(
        p.down_backlog >= SATURATION_BACKLOG_FRACTION,
        "[mc] the shared downstream bottleneck was never the limit: its serialization backlog was \
         non-empty for only {:.3} of the window (floor {SATURATION_BACKLOG_FRACTION})",
        p.down_backlog,
    );
    assert!(
        p.passed_fraction >= SATURATION_PASSED_FRACTION,
        "[mc] the shared downstream link was not carrying its capacity: the shaper forwarded only \
         {:.3} of it (floor {SATURATION_PASSED_FRACTION})",
        p.passed_fraction,
    );
    assert!(
        p.delivered_fraction >= MC_BULK_GOODPUT_FLOOR,
        "[mc] the downstream bulk lane delivered {:.3}x of the shared capacity (floor \
         {MC_BULK_GOODPUT_FLOOR}) -- the interactive tail was bought by starving the bulk lane",
        p.delivered_fraction,
    );

    // ── M1: the interactive round trip's tail ─────────────────────────────
    let (floor, _, p99, max) = quantiles(&p.rtt);
    assert!(
        p99 <= MC_RTT_P99_TRIPWIRE_MS,
        "[mc] the interactive round trip's p99 regressed: {p99:.1} ms against the \
         {MC_RTT_P99_TRIPWIRE_MS} ms tripwire (ceiling {MC_RTT_P99_CEILING_MS} ms, max {max:.1} ms, \
         p50 {floor:.1} ms, peak/floor {:.2}x, n={})",
        max / floor.max(1e-9),
        p.rtt.len(),
    );
}

/// The direction dimension, one dimension per arm: the same interactive lane
/// behind a saturating bulk load going downstream, upstream, and both ways.
/// Report-only — it attributes, it does not gate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "saturating-bulk direction attribution (downstream / upstream / symmetric) behind the \
            Minecraft-shaped interactive lane; one ~25 s run each, full tier, report-only; run with \
            --ignored --nocapture --test-threads=1 (see module header)"]
async fn mc_bulk_direction_decomposition() {
    let win = window();
    for dir in [
        Direction::Downstream,
        Direction::Upstream,
        Direction::Symmetric,
    ] {
        let arms = run_arm(dir, win, 1).await;
        let p = pool(&arms);
        print_pooled(dir, 1, win, &p);
        print_lanes(dir, &p);
        // The arm's own instrument sanity. A report that ran, measured nothing
        // and printed zeros is the same defect as an assertion that cannot
        // fail, so each direction must have produced samples, observed its own
        // lane split, and -- when it pushes -- received bulk.
        assert!(
            !p.down_small.is_empty() && !p.down_burst.is_empty() && !p.rtt.is_empty(),
            "[mc-direction {}] the arm measured nothing: down_small n={} down_burst n={} rtt n={}",
            dir.label(),
            p.down_small.len(),
            p.down_burst.len(),
            p.rtt.len(),
        );
        let observed = p.lanes.len();
        assert_eq!(
            observed,
            4 + usize::from(dir.pushes_down() && dir.uploads_up()),
            "[mc-direction {}] the server accepted {observed} streams, not the 4 (or 5,              symmetric) the arm opened: {:?}",
            dir.label(),
            p.lanes
                .iter()
                .map(|(t, l)| (*t as char, lane_name(*l)))
                .collect::<Vec<_>>(),
        );
        for tag in [MODE_SHAPED, MODE_BURST, MODE_ECHO] {
            let interactive = p
                .lanes
                .iter()
                .filter(|(t, l)| *t == tag && *l == LaneClass::Interactive)
                .count();
            assert_eq!(
                interactive,
                1,
                "[mc-direction {}] the Minecraft-shaped stream '{}' was not observed on the                  interactive lane",
                dir.label(),
                tag as char,
            );
        }
        if dir.pushes_down() {
            let delivered: u64 = arms.iter().map(|r| r.down_bulk_delivered_bytes).sum();
            assert!(
                delivered > 0,
                "[mc-direction {}] the downstream bulk push delivered nothing",
                dir.label(),
            );
        }
        println!(
            "[mc-direction {}] min_down_offered_ratio={:.2}x min_down_backlog={:.3} \
             min_passed_fraction={:.3} min_push_await_fraction={:.3} \
             bulk_delivered_fraction={:.3} rtt p50/p90/p99/max={:.1}/{:.1}/{:.1}/{:.1} ms",
            dir.label(),
            p.down_offered_ratio,
            p.down_backlog,
            p.passed_fraction,
            p.push_await_fraction,
            p.delivered_fraction,
            quantiles(&p.rtt).0,
            quantiles(&p.rtt).1,
            quantiles(&p.rtt).2,
            quantiles(&p.rtt).3,
        );
    }
}

/// Does the egress path's cross-lane congestion signal help a *realistic*
/// Minecraft-shaped interactive lane behind a saturating downstream bulk
/// without capping bulk? Baseline and signal arms interleave run-by-run on the
/// same seeds, so host drift hits them alike.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Minecraft-shaped interactive lane behind a saturating downstream bulk; 2 arms x MC_RUNS \
            runs; run with --ignored --nocapture --test-threads=1"]
async fn mc_nic_cross_lane_signal() {
    let win = window();
    let count = runs();
    let fault = fault();
    let mut base = Vec::new();
    let mut signal = Vec::new();
    for index in 0..count {
        let seed = 900 + 17 * index as u64;
        let b = with_timeout(
            MC_RUN_TIMEOUT,
            "mc cc_link baseline",
            run_mc(seed, win, Direction::Downstream, fault.as_deref(), None),
        )
        .await;
        print_run(Direction::Downstream, index, &b, win);
        base.push(b);
        let scheduler = CcSignalHub::new();
        let n = with_timeout(
            MC_RUN_TIMEOUT,
            "mc cc_link signal",
            run_mc(
                seed,
                win,
                Direction::Downstream,
                fault.as_deref(),
                Some(scheduler),
            ),
        )
        .await;
        signal.push(n);
    }
    let pb = pool(&base);
    let pn = pool(&signal);
    let (b50, b90, b99, bmax) = quantiles(&pb.rtt);
    let (n50, n90, n99, nmax) = quantiles(&pn.rtt);
    eprintln!(
        "[cc_link-mc] interactive rtt   baseline p50 {b50:6.1} p90 {b90:6.1} p99 {b99:6.1} max {bmax:6.1}\n\
         [cc_link-mc]                   signal   p50 {n50:6.1} p90 {n90:6.1} p99 {n99:6.1} max {nmax:6.1}"
    );
    eprintln!(
        "[cc_link-mc] bulk delivered   baseline {:.3}  signal {:.3}",
        pb.delivered_fraction, pn.delivered_fraction
    );
    eprintln!(
        "[cc_link-mc] down backlog     baseline {:.3}  signal {:.3}",
        pb.down_backlog, pn.down_backlog
    );
    assert!(
        pn.delivered_fraction >= pb.delivered_fraction * 0.9,
        "the cross-lane signal capped bulk delivery ({:.3} vs {:.3})",
        pn.delivered_fraction,
        pb.delivered_fraction
    );
    assert!(
        n99 <= b99,
        "the cross-lane signal did not improve the interactive rtt p99 ({n99:.1} vs {b99:.1} ms)"
    );
}
