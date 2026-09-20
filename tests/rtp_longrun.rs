//! Long-run and multi-flow latency/throughput measurement arms for the
//! deployment dual-lane composition (`game client -TCP-> access-server ->
//! rtp_mux(rtp+mux) -> proxy-server -TCP-> game server`).
//!
//! The existing `rtp_mux_jitter` arms measure a 30-second window, which cannot
//! see slow drift (a leak or creep in the RTT floor / queue tolerance / send
//! rate / cwnd / queue occupancy), recurring stalls with a period longer than
//! the window, or per-flow unfairness that only shows up when several
//! interactive streams share the lane. This arm runs the production
//! interactive lane (frame fast-forward + prompt FEC) beside the bulk lane for
//! a multi-minute window and prints a CSV row per sampling interval:
//!
//! * `[longrun-iv]`   — aggregate interactive p50/p99/max, offered/forwarded
//!   interactive wire, bulk forwarded wire, and the sender's controller state
//!   (send rate, cwnd, in-flight, RTT floor, queue tolerance, persistent-queue
//!   age, drain/backoff/probe counters). The tail also carries the netem
//!   per-direction receive/drop counters and the sender's retransmission and
//!   FEC counters, so a latency tail can be attributed to loss repair rather
//!   than to queueing.
//! * `[longrun-stream]` — one row per interactive stream tag, so per-flow
//!   fairness (p50/p99/count) is visible.
//! * `[longrun-final]` — whole-run percentiles, delivery, and bulk goodput.
//! * `[longrun-repair]` — end-of-run FEC/retransmission breakdown with the
//!   repair reasons (RTO / RACK reorder window / evidence-gated fast loss)
//!   and the interactive lane's per-direction drop counts.
//!
//! Report-only: the assertions are loose sanity guards. The numbers are the
//! deliverable. Run with (release is expected):
//!
//! ```sh
//! cargo test --release -p rtp_mux --test rtp_longrun -- \
//!     --ignored --nocapture --test-threads=1
//! ```
//!
//! Environment knobs (all optional):
//! * `RTP_LONGRUN_SECS`     — measurement window, default 300.
//! * `RTP_LONGRUN_INTERVAL_SECS` — CSV sampling interval, default 10.
//! * `RTP_LONGRUN_STREAMS`  — interactive streams sharing the lane, default 1.
//! * `RTP_LONGRUN_LOSS_PCT` — independent per-packet loss, default 2. Set `0`
//!   to separate a multi-flow queueing tail from a loss-repair tail.
//! * `RTP_LONGRUN_LABEL`    — CSV label suffix, default `base`.
//!
//! Measured on the production link (25 ms one-way delay, 5 ms jitter, 2%
//! independent loss, bulk 1 MiB/s periodic-burst lane):
//!
//! * Single interactive stream, 10 min: delivery 1.0000, p50 22.5 ms, p99
//!   28.5 ms, max 36.5 ms. RTT floor / queue tolerance / send rate (128 pkt/s)
//!   / cwnd (48) are flat across all 120 intervals; the persistent-queue age
//!   and the drain / backoff / probe counters stay at zero. No drift or leak.
//! * Four interactive streams, 6 min: delivery 1.0000, p50 ~28 ms, p99 ~92 ms,
//!   max 166 ms. Every stream keeps a comparable p99 (67-79 ms; no
//!   starvation), the bulk lane's goodput is byte-identical to the single-flow
//!   arm (0.6739 MiB/s), and the p99 improves over the run (90 -> 78 ms) as FEC
//!   and fast start warm up — not a creep.
//! * Four / eight streams at 0% loss: after the t=0 rate-ramp transient the
//!   steady p99 is 30-35 ms with a flat RTT floor and a zero persistent-queue
//!   age, so there is no multi-flow queueing tail to fix.
//!
//! The multi-flow loss tail is therefore the RACK reorder-window repair floor
//! (`[longrun-repair]` shows the repairs are `reorder`, not evidence-gated
//! fast loss); lowering it needs added redundancy or a shorter reorder window
//! that would risk spurious retransmits, so it is not a no-wire-cost win.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mux::testkit::mux::send_timestamped_messages;
use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::stats::percentile;
use netem_test::kit::{TestScope, submit_test_task};
use netem_test::{Counters, NetemConfig, NetemPair};
use rtp_mux::testkit::dual::{
    LaneRtpConfig, dual_mux_client_connect_lane_rtp_via,
    spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via,
};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::MissedTickBehavior;

/// One-way delay applied to every packet in both directions.
const OWD: Duration = Duration::from_millis(25);
/// Uniform jitter around [`OWD`].
const JITTER: Duration = Duration::from_millis(5);
/// Interactive message size and cadence (a typical game ping).
const MSG_BYTES: usize = 256;
const CADENCE: Duration = Duration::from_millis(25);
/// Bottleneck rate for the bulk lane (1 MiB/s) and its periodic burst shape,
/// identical to the production `rtp_mux_jitter` dual-lane `both` arm.
const BULK_RATE_BPS: u64 = 1024 * 1024 * 8;
const BULK_BURST_BYTES: usize = 2 * 1024 * 1024;
const BULK_PERIOD: Duration = Duration::from_secs(3);
const BULK_RAMP: Duration = Duration::from_millis(1500);
/// Let stragglers arrive before the final counters are read.
const GRACE: Duration = Duration::from_secs(3);
/// Bounded queue for test-owned tasks (mirrors the sibling scenarios).
const TASK_QUEUE_BOUND: usize = netem_test::kit::TEST_TASK_QUEUE_BOUND;
/// Percent of independent per-packet loss scaled to the netem threshold. The
/// production lane runs 2%; the knob lets a diagnostic arm isolate loss-repair
/// latency from mux-queueing latency without touching the production preset.
fn loss_from_pct(pct: u64) -> u32 {
    (u32::MAX / 100) * (pct.min(100) as u32)
}
/// Distinct first-byte tags for the interactive streams sharing the lane.
const STREAM_TAGS: [u8; 8] = *b"LMNOPQRS";

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env_u64(key, default as u64) as usize
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// One impairment direction: fixed delay + jitter, an independent-loss
/// threshold (`0` = none), and an optional rate cap.
fn link(seed: u64, loss: u32, rate_bps: u64) -> NetemConfig {
    NetemConfig {
        latency: OWD,
        jitter: JITTER,
        rate: rate_bps,
        loss,
        seed,
        ..NetemConfig::default()
    }
}

/// The prompt-parity preset: force-flush each interactive data burst's open
/// FEC group at the burst tail, with a single parity symbol.
fn prompt_tuning() -> rtp::FecTuning {
    rtp::FecTuning {
        instream_flush: true,
        small_group_parity_count: 1,
    }
}

/// A periodic bulk burst: `burst_bytes` offered every `period`, as fast as the
/// transport accepts, for the duration of the run.
async fn periodic_burst(
    write: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
    burst_bytes: usize,
    period: Duration,
    ramp: Duration,
    run_for: Duration,
) -> u64 {
    let start = Instant::now();
    tokio::time::sleep(ramp).await;
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick is immediate; consume it so bursts start at `ramp`.
    interval.tick().await;

    let mut cursor = 0usize;
    let mut written = 0u64;
    loop {
        if start.elapsed() >= run_for {
            break;
        }
        let mut remaining = burst_bytes;
        while remaining > 0 {
            if start.elapsed() >= run_for {
                return written;
            }
            let avail = payload.len() - cursor;
            let take = remaining.min(avail);
            if write
                .write_all(&payload[cursor..cursor + take])
                .await
                .is_err()
            {
                return written;
            }
            cursor = (cursor + take) % payload.len();
            remaining -= take;
            written += take as u64;
        }
        interval.tick().await;
    }
    written
}

type SnapshotCell = Arc<Mutex<Option<rtp::metrics::MetricsSnapshot>>>;
type FecCell = Arc<Mutex<Option<rtp::metrics::MetricsFecCounters>>>;

/// An observer that keeps the latest full transport snapshot, sampled at most
/// every 100 ms so the long run is not perturbed, plus the last `Some` FEC
/// counter snapshot (a later snapshot may carry `None`).
fn snapshot_observer() -> (rtp::metrics::MetricsObserver, SnapshotCell, FecCell) {
    let cell: SnapshotCell = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&cell);
    let fec_cell: FecCell = Arc::new(Mutex::new(None));
    let fec_sink = Arc::clone(&fec_cell);
    let last_ms = Arc::new(AtomicU64::new(0));
    let observer = rtp::metrics::MetricsObserver::filtered(
        move |_event, elapsed| {
            let now = elapsed.as_millis() as u64;
            let previous = last_ms.load(Ordering::Relaxed);
            if now >= previous.saturating_add(100) {
                last_ms.store(now, Ordering::Relaxed);
                true
            } else {
                false
            }
        },
        move |observation| {
            if let Some(snapshot) = observation.snapshot {
                if let Some(fec) = snapshot.fec_counters {
                    *fec_sink.lock().unwrap() = Some(fec);
                }
                *sink.lock().unwrap() = Some(snapshot);
            }
        },
    );
    (observer, cell, fec_cell)
}

/// Whole-run result of one long-run arm.
struct LongRunReport {
    sent: u64,
    received: u64,
    samples: BTreeMap<u8, Vec<f64>>,
    int_counters: Counters,
    int_c2s: Counters,
    int_s2c: Counters,
    bulk_wire_bytes: u64,
    bulk_sink_bytes: u64,
    bulk_counters: Counters,
    fec: Option<rtp::metrics::MetricsFecCounters>,
    rtx: Option<rtp::metrics::MetricsRetransmissionCounters>,
}

impl LongRunReport {
    fn all_samples(&self) -> Vec<f64> {
        self.samples.values().flatten().copied().collect()
    }

    fn final_summary(&self) -> Summary {
        let mut all = self.all_samples();
        all.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Summary {
            count: all.len() as u64,
            p50: percentile(&all, 0.50),
            p90: percentile(&all, 0.90),
            p99: percentile(&all, 0.99),
            p999: percentile(&all, 0.999),
            max: all.last().copied().unwrap_or(0.0),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Summary {
    count: u64,
    p50: f64,
    p90: f64,
    p99: f64,
    p999: f64,
    max: f64,
}

fn print_stream_row(label: &str, iv: u64, tag: u8, samples: &mut [f64]) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    eprintln!(
        "[longrun-stream] {label},{iv},{tag},{count},{p50:.2},{p99:.2},{max:.2}",
        tag = tag as char,
        count = samples.len(),
        p50 = percentile(samples, 0.50),
        p99 = percentile(samples, 0.99),
        max = samples.last().copied().unwrap_or(0.0),
    );
}

#[allow(clippy::too_many_arguments)]
fn print_interval_row(
    label: &str,
    iv: u64,
    elapsed: Duration,
    summary: &Summary,
    int_counters: Counters,
    int_c2s: Counters,
    int_s2c: Counters,
    bulk_counters: Counters,
    snapshot: Option<rtp::metrics::MetricsSnapshot>,
) {
    let ms = |d: Option<Duration>| d.map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);
    let (rate, cwnd, inflight, floor, tol, persist, drains, backoffs, probes) = match snapshot {
        Some(s) => (
            s.send_rate_packets_per_second,
            s.congestion_window_packets,
            s.in_flight_packets,
            ms(s.congestion_rtt_floor),
            ms(s.congestion_queue_tolerance),
            s.congestion_persistent_queue_for
                .map(|d| d.as_secs_f64() * 1000.0)
                .unwrap_or(0.0),
            s.congestion_delay_drains,
            s.congestion_loss_backoffs,
            s.congestion_bandwidth_probe_increases,
        ),
        None => (0.0, 0, 0, 0.0, 0.0, 0.0, 0, 0, 0),
    };
    let rtx = snapshot
        .map(|s| s.retransmission_counters)
        .unwrap_or_default();
    let (fec_parity, fec_recovered) = snapshot
        .and_then(|s| s.fec_counters)
        .map(|f| (f.parity_sent, f.recovered_symbols))
        .unwrap_or((0, 0));
    let rx_pkts = snapshot.map(|s| s.received_packets).unwrap_or(0) as u64;
    eprintln!(
        "[longrun-iv] {label},{iv},{t:.1},{n},{p50:.2},{p90:.2},{p99:.2},{p999:.2},{max:.2},\
         {int_fwd},{int_fwd_bytes},{bulk_fwd},{bulk_fwd_bytes},{rate:.1},{cwnd},{inflight},\
         {floor:.2},{tol:.2},{persist:.1},{drains},{backoffs},{probes},\
         {int_recv},{int_drop},{bulk_drop},{rx_pkts},{rtx_att},{rtx_first},{rtx_rep},\
         {rtx_rto},{rtx_reorder},{rtx_fastloss},{rtx_probe},{fec_parity},{fec_recovered},\
         {c2s_recv},{c2s_drop},{s2c_recv},{s2c_drop}",
        t = elapsed.as_secs_f64(),
        n = summary.count,
        p50 = summary.p50,
        p90 = summary.p90,
        p99 = summary.p99,
        p999 = summary.p999,
        max = summary.max,
        int_fwd = int_counters.forwarded,
        int_fwd_bytes = int_counters.forwarded_bytes,
        bulk_fwd = bulk_counters.forwarded,
        bulk_fwd_bytes = bulk_counters.forwarded_bytes,
        int_recv = int_counters.received,
        int_drop = int_counters.dropped,
        bulk_drop = bulk_counters.dropped,
        rtx_att = rtx.attempts,
        rtx_first = rtx.first_attempts,
        rtx_rep = rtx.repeat_attempts,
        rtx_rto = rtx.rto_reason,
        rtx_reorder = rtx.reorder_reason,
        rtx_fastloss = rtx.fast_loss_reason,
        rtx_probe = rtx.tail_probes,
        c2s_recv = int_c2s.received,
        c2s_drop = int_c2s.dropped,
        s2c_recv = int_s2c.received,
        s2c_drop = int_s2c.dropped,
    );
}

/// Run one long-run arm: `streams` interactive mux streams sharing the
/// production interactive lane (frame fast-forward + prompt FEC, 2% loss) plus
/// one bulk stream on the separate strict byte-stream lane. Prints per-interval
/// CSV rows and returns the whole-run report.
async fn run_longrun(
    label: &str,
    streams: usize,
    run_for: Duration,
    interval: Duration,
) -> LongRunReport {
    assert!(
        streams > 0 && streams <= STREAM_TAGS.len(),
        "streams must be in 1..={}",
        STREAM_TAGS.len()
    );
    let loss = loss_from_pct(env_u64("RTP_LONGRUN_LOSS_PCT", 2));
    let int_rtp = LaneRtpConfig::frame_reordering(true, prompt_tuning());
    let bulk_rtp = LaneRtpConfig::byte_stream();
    let base = Instant::now();

    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (int_addr, bulk_addr, mut latencies, bulk_counter, _sink_streams) =
                spawn_dual_mux_latency_bulk_server_two_listeners_lane_rtp_via(
                    &task_tx, base, int_rtp, bulk_rtp,
                )
                .await
                .unwrap();
            let int_pair =
                NetemPair::spawn(int_addr, link(41, loss, 0), link(42, loss, 0)).unwrap();
            let bulk_pair = NetemPair::spawn(
                bulk_addr,
                link(43, loss, BULK_RATE_BPS),
                link(44, loss, BULK_RATE_BPS),
            )
            .unwrap();

            let (observer, snapshot_cell, fec_cell) = snapshot_observer();
            let (opener, _accepter) = dual_mux_client_connect_lane_rtp_via(
                &task_tx,
                int_pair.client_addr(),
                bulk_pair.client_addr(),
                int_rtp,
                bulk_rtp,
                Some(observer),
                None,
            )
            .await
            .unwrap();

            // Interactive streams, each with a distinct first-byte tag so the
            // per-stream rows can attribute latency.
            let sent = Arc::new(AtomicU64::new(0));
            for &tag in STREAM_TAGS.iter().take(streams) {
                let (mut read, mut write) = opener.open(mux::LaneClass::Interactive).await.unwrap();
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 8 * 1024];
                        while let Ok(n) = read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                        }
                    }),
                );
                let sent = Arc::clone(&sent);
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        if write.write_all(&[tag]).await.is_err() {
                            return;
                        }
                        let n = send_timestamped_messages(
                            &mut write, base, MSG_BYTES, CADENCE, run_for,
                        )
                        .await;
                        sent.fetch_add(n, Ordering::Relaxed);
                    }),
                );
            }

            // Bulk stream on the separate lane.
            let (mut bulk_read, mut bulk_write) = opener.open(mux::LaneClass::Bulk).await.unwrap();
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = bulk_read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let payload = cyclic_payload(BULK_BURST_BYTES);
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    if bulk_write.write_all(b"B").await.is_err() {
                        return;
                    }
                    periodic_burst(
                        &mut bulk_write,
                        &payload,
                        BULK_BURST_BYTES,
                        BULK_PERIOD,
                        BULK_RAMP,
                        run_for,
                    )
                    .await;
                }),
            );

            // Per-interval sampler: drains the latency channel continuously
            // (keeping the server's bounded channel from filling), buckets by
            // stream tag, and prints a CSV row per interval with the latest
            // controller snapshot and wire counters.
            let run_start = Instant::now();
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await; // consume the immediate first tick

            let mut all: BTreeMap<u8, Vec<f64>> = BTreeMap::new();
            let mut interval_index = 0u64;
            loop {
                let mut bucket: BTreeMap<u8, Vec<f64>> = BTreeMap::new();
                let deadline = tokio::time::Instant::now() + interval;
                loop {
                    tokio::select! {
                        maybe = latencies.recv() => {
                            match maybe {
                                Some((tag, lat)) => bucket.entry(tag).or_default().push(lat),
                                None => break,
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                }
                interval_index += 1;
                let elapsed = run_start.elapsed();
                let mut interval_samples: Vec<f64> = bucket.values().flatten().copied().collect();
                interval_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let summary = Summary {
                    count: interval_samples.len() as u64,
                    p50: percentile(&interval_samples, 0.50),
                    p90: percentile(&interval_samples, 0.90),
                    p99: percentile(&interval_samples, 0.99),
                    p999: percentile(&interval_samples, 0.999),
                    max: interval_samples.last().copied().unwrap_or(0.0),
                };
                print_interval_row(
                    label,
                    interval_index,
                    elapsed,
                    &summary,
                    int_pair.stats(),
                    int_pair.stats_c2s(),
                    int_pair.stats_s2c(),
                    bulk_pair.stats(),
                    *snapshot_cell.lock().unwrap(),
                );
                for (tag, samples) in bucket.iter_mut() {
                    print_stream_row(label, interval_index, *tag, samples);
                    all.entry(*tag).or_default().extend(samples.iter().copied());
                }
                if elapsed >= run_for {
                    break;
                }
            }

            tokio::time::sleep(GRACE).await;
            let int_counters = int_pair.stats();
            let int_c2s = int_pair.stats_c2s();
            let int_s2c = int_pair.stats_s2c();
            let bulk_counters = bulk_pair.stats();
            let bulk_wire_bytes = bulk_pair.stats_c2s().forwarded_bytes;
            let last_snapshot = *snapshot_cell.lock().unwrap();
            let last_fec = *fec_cell.lock().unwrap();
            let report = LongRunReport {
                sent: sent.load(Ordering::Relaxed),
                received: all.values().map(Vec::len).sum::<usize>() as u64,
                samples: all,
                int_counters,
                int_c2s,
                int_s2c,
                bulk_wire_bytes,
                bulk_sink_bytes: bulk_counter.load(Ordering::Relaxed),
                bulk_counters,
                fec: last_fec,
                rtx: last_snapshot.map(|s| s.retransmission_counters),
            };

            int_pair.stop();
            bulk_pair.stop();
            report
        })
        .await
}

/// Print the whole-run aggregate for one long-run arm.
fn print_final(label: &str, streams: usize, run_for: Duration, r: &LongRunReport) {
    let s = r.final_summary();
    let delivery = if r.sent == 0 {
        0.0
    } else {
        r.received as f64 / r.sent as f64
    };
    let bulk_mibps = r.bulk_wire_bytes as f64 / (1024.0 * 1024.0) / run_for.as_secs_f64();
    eprintln!(
        "[longrun-final] {label},{streams},{run_s},{sent},{recv},{delivery:.4},{p50:.2},{p90:.2},\
         {p99:.2},{p999:.2},{max:.2},int_fwd={int_fwd},int_fwd_bytes={int_bytes},\
         bulk_wire_bytes={bulk_wire},bulk_sink_bytes={bulk_sink},bulk_wire_MiBps={bulk_mibps:.4},\
         bulk_fwd={bulk_fwd}",
        run_s = run_for.as_secs(),
        sent = r.sent,
        recv = r.received,
        p50 = s.p50,
        p90 = s.p90,
        p99 = s.p99,
        p999 = s.p999,
        max = s.max,
        int_fwd = r.int_counters.forwarded,
        int_bytes = r.int_counters.forwarded_bytes,
        bulk_wire = r.bulk_wire_bytes,
        bulk_sink = r.bulk_sink_bytes,
        bulk_fwd = r.bulk_counters.forwarded,
    );
    for (tag, samples) in &r.samples {
        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eprintln!(
            "[longrun-final-stream] {label},{tag},{count},{p50:.2},{p99:.2},{max:.2}",
            tag = *tag as char,
            count = sorted.len(),
            p50 = percentile(&sorted, 0.50),
            p99 = percentile(&sorted, 0.99),
            max = sorted.last().copied().unwrap_or(0.0),
        );
    }
    if let Some(fec) = r.fec {
        eprintln!(
            "[longrun-repair] {label} fec parity_sent={} groups_flushed={} recovered={} \
             skipped_no_surplus={} skipped_burst_end={} skipped_loss_gate={} \
             skipped_no_spare_capacity={} rtx_attempts={} first={} repeat={} rto={} reorder={} \
             fast_loss={} tail_probes={} int_recv={} int_drop={} bulk_drop={} \
             c2s_recv={} c2s_drop={} s2c_recv={} s2c_drop={}",
            fec.parity_sent,
            fec.groups_flushed,
            fec.recovered_symbols,
            fec.groups_skipped_no_surplus_tokens,
            fec.groups_skipped_burst_end,
            fec.groups_skipped_loss_gate,
            fec.groups_skipped_no_spare_capacity,
            r.rtx.map(|r| r.attempts).unwrap_or(0),
            r.rtx.map(|r| r.first_attempts).unwrap_or(0),
            r.rtx.map(|r| r.repeat_attempts).unwrap_or(0),
            r.rtx.map(|r| r.rto_reason).unwrap_or(0),
            r.rtx.map(|r| r.reorder_reason).unwrap_or(0),
            r.rtx.map(|r| r.fast_loss_reason).unwrap_or(0),
            r.rtx.map(|r| r.tail_probes).unwrap_or(0),
            r.int_counters.received,
            r.int_counters.dropped,
            r.bulk_counters.dropped,
            r.int_c2s.received,
            r.int_c2s.dropped,
            r.int_s2c.received,
            r.int_s2c.dropped,
        );
    } else {
        eprintln!(
            "[longrun-repair] {label} fec=None rtx_attempts={} int_recv={} int_drop={} bulk_drop={} \
             c2s_recv={} c2s_drop={} s2c_recv={} s2c_drop={}",
            r.rtx.map(|r| r.attempts).unwrap_or(0),
            r.int_counters.received,
            r.int_counters.dropped,
            r.bulk_counters.dropped,
            r.int_c2s.received,
            r.int_c2s.dropped,
            r.int_s2c.received,
            r.int_s2c.dropped,
        );
    }
}

async fn run_arm(label: &str, streams: usize, run_for: Duration, interval: Duration) {
    let report = with_timeout(
        run_for + Duration::from_secs(120),
        label,
        run_longrun(label, streams, run_for, interval),
    )
    .await;
    assert!(
        report.received > 0,
        "{label}: no interactive latency samples; the lane stalled"
    );
    assert!(
        report.received as f64 / report.sent.max(1) as f64 > 0.99,
        "{label}: interactive delivery {}/{} below 0.99",
        report.received,
        report.sent
    );
    print_final(label, streams, run_for, &report);
}

/// Long-run single-flow arm: the production interactive lane plus the bulk
/// lane for a multi-minute window, sampled per interval. Report-only.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "long-run measurement (multi-minute, real time); run with --ignored --nocapture --test-threads=1"]
async fn longrun_duallane() {
    let run_for = Duration::from_secs(env_u64("RTP_LONGRUN_SECS", 300));
    let interval = Duration::from_secs(env_u64("RTP_LONGRUN_INTERVAL_SECS", 10).max(1));
    let label = env_string("RTP_LONGRUN_LABEL", "base");
    let streams = env_usize("RTP_LONGRUN_STREAMS", 1);
    run_arm(&label, streams, run_for, interval).await;
}

/// Multi-flow arm: several interactive streams sharing the production
/// interactive lane beside the bulk lane, so per-stream fairness is visible.
/// Report-only.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "multi-flow measurement (real time); run with --ignored --nocapture --test-threads=1"]
async fn multiflow_duallane() {
    let run_for = Duration::from_secs(env_u64("RTP_LONGRUN_SECS", 180));
    let interval = Duration::from_secs(env_u64("RTP_LONGRUN_INTERVAL_SECS", 10).max(1));
    let label = env_string("RTP_LONGRUN_LABEL", "multiflow");
    let streams = env_usize("RTP_LONGRUN_STREAMS", 4);
    run_arm(&label, streams, run_for, interval).await;
}
