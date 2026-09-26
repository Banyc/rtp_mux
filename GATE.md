# The rtp_mux validation gate

This file is the authoritative scope of the rtp_mux scenario gate. `cargo test
-p rtp_mux` silently skips every `#[ignore]`d scenario, so the gate is defined
in tiers and the `gate-manifest` block below names every opt-in scenario and
its tier. The manifest is machine-checked by the shared checker
(`netem_test/tools/check-gate.py`, parameterized per crate), which fails if a
scenario is added or removed without the manifest being updated, making an
unnoticed `#[ignore]` skip impossible.

Run the checker after adding, removing, or re-tiering any scenario. The
checker lives in `netem_test/tools/check-gate.py` (run from the crate
checkout via the sibling path; it resolves the sibling kit trees from the
shared crates root):

```sh
python3 ../netem_test/tools/check-gate.py \
  --crate . rtp_mux tests GATE.md
```

The rtp_mux scenarios were relocated here from the harness
(`netem_test/tests`) so that every rtp_mux performance invariant — in
particular the tri-mandate constitution below — is asserted by rtp_mux's own
test invocation; the harness keeps only the impairment instrument and points
at the owning crates (`netem_test/tests/README.md` is now a pointer table).
The layer kit (`rtp_mux::testkit`, behind the `testing` feature) holds the
dual-lane server/connector plumbing, the tagged-stream sink machinery, and the
transport-mediated `mux`-over-`rtp` scaffolding (`testkit::mux_over_rtp`: the
rtp listener/accept plumbing, the echo/connect/sink servers, the
timestamped-message latency sinks, the transient connects, and the loopback
bulk-goodput probe constants and floors); imports go only downward (rtp_mux
kit → mux kit / rtp kit / harness kit), so `netem-test` stays a leaf.

rtp_mux is also where the `rtp`+`mux` cooperation is measured: the layer
invariant keeps `mux` from knowing `rtp` and `rtp` from knowing `mux`, so any
scenario that needs both belongs here. That is why the whole `mux`-over-`rtp`
scenario set — `mux_over_rtp`, `mux_over_rtp_perf`, `rtp_and_mux`,
`mux_stream_fairness`, `mux_bulk_clean_stall`, `mux_ceiling_probe` (the
loopback bulk ceiling), and `hol_verify4` (the DualMux-v4 A/B report-only
probes) — lives here rather than in `mux`, alongside the two targets moved
here earlier: `contested_latency` (a sparse interactive ping stream and a bulk
upload sharing one `mux`-over-`rtp` connection through a `NetemPair`) and
`perf_probe` (the `tools/perf-loop` battery: time-boxed `mux`-over-`rtp`
goodput on impaired links, sparse-message latency, raw `rtp` 4 MiB echo
ceilings, and the two link-preset seeding tests). `tools/perf-loop` compiles
`perf_probe` from the frozen suite's exported **rtp_mux** component, so a
`--component-revision rtp_mux=<commit>` pin selects the probe that runs; the
`mux_ceiling_probe` target is the distinct loopback-ceiling instrument, so the
two do not compete for one Cargo target name.

## Performance

The operator's product constitution is **three mandates**, jointly the
acceptance criterion for every change to the interactive path — a change that
improves one mandate while violating another is a failure, not a win. rtp_mux
owns the production dual-lane topology (the interactive lane on its own RTP
connection, the bulk lane on a second, separate connection), so all three
mandates are asserted by **rtp_mux's own gates**; the full statement, with
each bound's derivation, is the module doc of
`rtp_mux/tests/dual_lane_mandates.rs` and is summarised here so a reader of
either sees the whole constitution:

1. **Low latency of the interactive lane** — the interactive lane's tail
   latency (p99, plus a spike bound) stays at its floor on the production
   dual-lane topology. Bound (derived): one-way delay floor 25 ms + a
   documented margin — the `250 ms` spike ceiling (the README's "zero >250 ms
   spikes" criterion, ~8× the measured ~29 ms p99 on the seeded `both` arm).
   Asserted by `rtp_mux_jitter::jitter_duallane_constitution_gate_p99`
   (`full` tier): median of three seeded runs against the ceiling plus a zero
   `>250 ms` spike count on every run. Wall-clock, so median-of-N, opt-in:
   ```sh
   cargo test --release -p rtp_mux --test rtp_mux_jitter -- \
       --ignored jitter_duallane_constitution_gate_p99 --nocapture --test-threads=1
   ```
2. **Reasonable goodput of the interactive lane** — the interactive lane
   delivers what it is offered (`delivery == 1.000`) **without inflating its
   own wire** to get there. Bound (derived): offered payload = the
   deterministic sent-message byte count; the lane's *aggregate*
   client→server wire forwarded by the impairment proxy must stay within `6×`
   of it (measured ~3.6× on the seeded `both` arm — RTP/mux framing +
   control + the repair traffic 2 % loss needs — so ~1.6× headroom, and the
   aggregate wire must not grow by more than ~+64 %). The budget does not
   bound one message's redundancy: a fully-armored lone interactive tail is
   `primary + 5 copies` = six datagrams carrying the same 256 B payload, so
   it alone costs at least the whole budget before framing. Asserted by
   `rtp_mux_jitter::jitter_duallane_constitution_gate` (**default tier** —
   both quantities are deterministic counts, and counts belong in the gate
   that always runs); it runs on every `cargo test -p rtp_mux`. The offered
   payload's byte-exact integrity is additionally asserted always-run at the
   mux layer beneath (mux's default tier, `mux/GATE.md`) and in rtp_mux's own
   default tests (`tests/bidirectional.rs`).
3. **High goodput of the bulk lane** — the bulk lane's goodput stays at a
   high fraction of the link's capacity on the same topology. Bound
   (derived): sink-delivered goodput ≥ `0.35 ×` the configured bulk-lane rate
   (8 Mbit/s shape, the `rtp_bufferbloat` `0.35 × capacity` precedent — the
   floor is deliberately slack so ordinary host noise never trips it while a
   change that at least halves the bulk lane's goodput fails). The gate's arm
   is the deployment's own bulk lane (`LaneRtpConfig::production_bulk`: the
   byte-stream, FEC-free transport carrying the congestion intent production
   maps from `LaneClass::Bulk` — `Dedicated`, not the stock `Shared` default
   a bare byte-stream lane gets), so the mandate measures the configuration
   the product ships; the measured band is ~0.96× of the shaped rate, so the
   floor leaves ~2.7× headroom. Asserted by
   `dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction`
   (`full` tier): median of three seeded runs. The goodput is the sink
   counter's delta across the offered window, sampled at both ends while the
   saturating pump still runs, so the reading is the rate the lane sustains
   under load: the delta keeps the pump's pre-window saturation phase out of
   it (the cumulative counter measured a spurious 1.88× on a 1.0 MiB/s cap),
   and the interval is the window itself rather than window + drain grace,
   which divided the window's bytes by an interval ~13 % longer in which the
   sender offers nothing and reported 0.86× where the lane sustains 0.96×.
   ```sh
   cargo test --release -p rtp_mux --test dual_lane_mandates -- \
       --ignored bulk_lane_goodput_stays_above_capacity_fraction --nocapture --test-threads=1
   ```

The interactive scaling boundary is additionally gated by
`hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery` (`full` tier):
four interactive streams on the same lane keep per-flow delivery ≥ 0.90 and
p50 ≤ 2.5× the solo reference, so the interactive lane's latency floor also
holds when several flows share it.

### The tri-mandate smoke set

The one command that measures all three mandates together and leaves
machine-checkable evidence is the `mandate_smoke` target; `tools/mandate-check`
(`crates/netem_test/tools/mandate-check`) runs it, renders one panel per
mandate, prints a verdict block and writes `mandate-check.json` and the
evidence files (`M1/M2/M3/M4.json`/`.csv`):

```sh
cargo test --release -p rtp_mux --test mandate_smoke -- --nocapture
cd crates/netem_test && tools/mandate-check --rtp-mux ../rtp_mux
```

It is a smoke set **alongside** the gates above, not a replacement: it neither
retunes nor re-arms them. All three mandates are measured on the production
dual-lane topology, over three arms in the shape the field sends. `clean` is
2 % iid loss with 25 ms one-way delay and 5 ms jitter and the production
2 MiB / 3 s bulk load — the arm the mandate bounds themselves are asserted on.
`hostile` adds the product's known bad regime: the four-state Gilbert-Elliot
model `gilbert_elliott_loss(5.0, 8.0)` plus ~100 ms jitter, same cadence and
bulk load. `lone_tail` rides the same hostile impairment with the
request/response shape (`depth` 1, one unacked data packet at a time) and no
bulk lane. Every window is `<= 15 s` and the interactive cadence is ~5 ms, so a
short run still carries thousands of samples per cadence arm; the run is ~3
minutes, and `MANDATE_SMOKE_QUICK=1` takes the shortest windows.

M1 asserts the mandate-1 bound (p99 `<= 250 ms` and zero samples `> 250 ms`) on
`clean`; M2 asserts the mandate-2 bound (`delivery == 1.000`, own-wire `<= 6x`)
on `clean`; M3 asserts the mandate-3 floor (`>= 0.35x` of the configured link
rate) as the within-run delivered/shaper-forwarded fraction, median of three.
Those bounds and their derivations are the ones stated above; the smoke set
does not restate them.

The `hostile` and `lone_tail` arms carry the product's **known, measured**
hostile defect — the 1 s `MIN_RTO` repair floor plus exponential backoff (the
field's GE lone-tail p99 1053–1542 ms, `rtx_rto` 13–38 and `rtx_repeat`
10–26, the slowest 60 s ladder 1535 / 2532 / 3544 / 5315 ms) — so asserting the
250 ms ceiling there would assert something currently false. They assert
**regression guards** derived from those measurements, in the same style as the
bounds above (measured X, bound Y, so a change that at least doubles it fails):

| quantity | measured | guard |
| --- | --- | --- |
| M1 hostile (GE cadence) p99 | 212–280 ms (12 s arm) | `900 ms` (~3×) |
| M1 hostile `> 250 ms` share | 0–2.75 % | `8 %` (~3×) |
| M1 lone-tail p99 | field 1053–1542 ms (60 s) | `3200 ms` (~2×) |
| M1 lone-tail p99.9 | 797–1636 ms (15 s arm); field ladder 5315 ms | `8000 ms` (~1.5× the field ladder) |
| M1 lone-tail `> 250 ms` share | field 2.7 %, smoke 0–0.7 % | `8 %` |
| M2 hostile own-wire | 4.63–4.81× | `10×` (~2×) |
| M2 lone-tail own-wire | 6.07–6.41× (15 s), field 6.22–7.17× | `14×` (~2×) |
| M2 hostile/lone delivery | 1.000 | `0.995` |

The smoke panels carry the mandate lines regardless: the M1 latency panel
draws the **250 ms ceiling**, the M2 wire panel draws the **6× budget** and the
M3 panels draw the **0.35× floor**, so a hostile or lone-tail breach is visible
in the evidence even when that arm's assertion is only a regression guard. The
assertion is a tripwire; the panel shows what moved. The M1 evidence also
draws a p99 CDF panel (the ceiling itself cannot be a horizontal line on a
latency CDF), and every `MANDATE` line prints p50/p90/p99/p999/max and the
`> 250 ms` sample count for all three arms.

**Redundancy monotonicity is NOT a mandate** — it was only ever a proxy for
these outcomes. FEC recovery parity may legitimately grow with loss; what must
not happen is the interactive lane's extra/armor packets inflating its own
delivered wire. Every gate above is vacuity-checked (break the bound — inject
latency, inflate the wire, drop a delivery, starve the bulk lane — and the
gate fails naming the mandate); the harness must not restate this
constitution.

### M4: the interactive lane's split across several flows

M1 and M2 each measure **one** interactive flow, so a mandate result obtained
by starving one of several flows sharing the interactive lane would pass all
three mandates. `mandate_smoke::m4_interactive_lane_fairness` closes that. The
arm is the M1/M2 `clean` interactive lane — the production
`LaneRtpConfig::frame_reordering(true, prompt)` lane, the same tagged-stream
sink (`spawn_tagged_stream_sink`, which buckets every sample by its stream's
first-byte tag exactly as the two-interactive battery does) and the same
`send_timestamped_messages` offer — carrying **four** interactive streams
(`M4_FLOWS`) instead of one, each offered the same 256 B payload at the same
~5 ms cadence, each attributed by its own tag. Every flow's delivered bytes and
latency are measured; the bulk lane is connected (the topology is the
production dual-lane one) but carries no stream, so the quantity measured is
the interactive lane's own split, which no other arm measures. A second arm
repeats the multi-flow offer on the M1/M2 `hostile` link.

Four quantities, asserted on both arms unless the row says otherwise:

| quantity | measured (M4's own runs) | bound (derived) |
| --- | --- | --- |
| per-flow delivery (`received_i / offered_i`) | 1.000 on every flow of both arms, 29 runs | `>= 0.995` (no starvation) |
| fair-share imbalance `max_i \|share_i − 1/N\| / (1/N)`, `share_i` = flow `i`'s share of the lane's delivered bytes | `0.46 %` (clean worst), `0.43 %` (hostile worst) | `1 %` (2.2× the worst measured, so a change that at least doubles the imbalance fails) |
| clean-arm p99 spread `max p99 / min p99` | `1.20×` (36 runs, both windows) | `2×` (1.67× the worst measured) |
| hostile-arm per-flow p99 | `<= 423 ms` | M1's hostile p99 regression guard (item 1's row above; not restated) |

The imbalance bound's derivation in full: one delivered frame is
`1 / (4 × 2064) = 0.012 %` of the lane, i.e. `0.048 %` of the equal share, so
the measured skew is a handful of frames of connection ramp at the window
edges; the `1 %` bound is 2.2× the worst of 29 runs and cannot be reached by
frame-edge ramp. The share statistic is blind to a scheduler bias that is
hidden by an idle lane (with equal offers and delivery at 1.000 every share
would be equal however the frames were ordered), which is why the clean arm
also asserts a **fair-latency** bound: no flow's p99 may exceed the best flow's
p99 by more than `2×`. That bound is deliberately not asserted on the hostile
arm, where the per-flow p99 differences are a GE loss realization rather than a
scheduler property (that arm measured a spread of up to `2.93×` across 10
runs); the hostile arm keeps M1's absolute guard instead.

M4 **reports** rather than asserts the absolute interactive ceiling: the
4-flow clean arm measures p99 179–231 ms — `0.72–0.92` of M1's ceiling — so an
absolute per-flow assertion there would sit within 1.1× of the arm's own
measurement and fire on host noise. M1 remains the authority for the ceiling,
the M4 latency panel draws it, and the `MANDATE M4` line prints
`clean_p50_max`/`clean_p99_max`/`hostile_p99_max`, so a multi-flow latency
regression is visible in the evidence and in the verdict line. The arm's own
cost is the queueing delay of four flows behind the shared interactive window
(p50 24 ms, p90 155 ms, p99 180 ms on the 12 s window where the one-flow arm
sits at the 25 ms floor): the split is fair, and the lane's latency budget
under 4× multiplexing is the finding that number carries.

M4 is **default tier** (not `#[ignore]`d): its asserted quantities are counts
and shares over a seeded link, the same class as the mandate-2 constitution
gate, and its ~31 s wall-clock belongs in the gate that always runs. The
measurement is part of the one command above — M4's evidence is `M4.json` plus
four panels (per-flow shares against the fair-share line, per-flow departure
from it against the ±bound, per-flow delivery against the floor, and per-flow
p50/p99 against M1's ceiling). The two vacuity demonstrations are
`MANDATE_SMOKE_FAULT=M4_starve` (flow 0 is offered the whole window and the
rest only its second half: the fair-share bound fails naming M4 at 0.70
imbalance) and `MANDATE_SMOKE_FAULT=M4_drop` (90 % loss on the clean link: the
per-flow delivery floor fails naming M4).

## Tiers

- **default** — not `#[ignore]`d, so a plain `cargo test -p rtp_mux` runs it.
  Every scenario here is seeded (deterministic impairment). The mandate-2
  constitution gate (`jitter_duallane_constitution_gate`) is a ~40 s
  wall-clock dual-lane run whose asserted quantities are deterministic
  counts, so it still belongs in the gate that always runs; the crate's own
  non-scenario targets are much faster. This is the gate that runs on every
  `cargo test`.
- **standard** — `#[ignore]`d, runs in well under a minute per target and
  asserts a correctness property (not just a measurement). Run with
  `cargo test -p rtp_mux -- --ignored --test-threads=1`.
- **full** — `#[ignore]`d, minutes per target; still asserts a property, but
  too slow for the default gate. Run the target explicitly.
- **perf** — `#[ignore]`d, report-only measurement or long-run tooling; these
  produce numbers (or feed `tools/perf-loop`), they do not assert a gate
  floor. A `perf` scenario must not contain an assertion in its own body;
  `check-gate.py` fails with the scenario name, its file, and the token if
  one does. It must also not reach an assertion through a helper: the checker
  derives the crate-local call-graph closure of every `perf` scenario and
  requires every asserting helper it reaches to be declared report-only in the
  `gate-perf-guard-helpers` block.

## Default tier (runs in `cargo test -p rtp_mux`)

The mandate-2 interactive constitution gate
(`rtp_mux_jitter::jitter_duallane_constitution_gate` — `delivery == 1.000`
and the own-wire budget, both deterministic counts over the seeded dual-lane
link), the two hol_probe FEC-wiring property tests
(`fec_gaming_treatment_has_bad_path_and_large_capacity_headroom` and
`fec_saturated_pair_keys_loss_to_the_same_rtp_sequence`), the moved
`mux`-over-`rtp` default scenarios (the clean and latency-only echo integrity
of `mux_over_rtp`, the reassigned `rtp`/`mux` smoke trio of `rtp_and_mux`, the
lossy smoke, the contended-delivery and the small-stream-while-bulk ordering
of `mux_over_rtp_perf`, and `mux_bulk_clean_stall`'s clean-link bulk progress
gate plus its teardown vacuity check), plus the crate's own non-scenario
targets (`bidirectional`, `duplex`, `explorer`, `lane_rejection`,
`session_stats`, `xsession`), which assert the offer-payload integrity the
constitution's mandate 2 depends on at the byte level. It also runs
`bind_race`, which asserts that concurrent ephemeral binds each come away
holding an adjacent interactive/bulk pair while the host's ephemeral ports are
occupied — the invariant the connector's derived bulk destination depends on.

The `gate-default-required` block names the asserting scenarios that must stay
in this tier; `check-gate.py` fails if one is re-`#[ignore]`d or removed.

```gate-default-required
hol_probe::fec_gaming_treatment_has_bad_path_and_large_capacity_headroom
hol_probe::fec_saturated_pair_keys_loss_to_the_same_rtp_sequence
perf_probe::controller_fat_pipe_has_only_fixed_shaping
perf_probe::deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss
rtp_mux_jitter::jitter_duallane_constitution_gate
mandate_smoke::m1_interactive_tail_latency
mandate_smoke::m2_interactive_delivery_and_wire
mandate_smoke::m3_bulk_goodput_fraction
mandate_smoke::m4_interactive_lane_fairness
mux_bulk_clean_stall::bounded_teardown_does_not_park_on_a_stuck_blocking_task
mux_bulk_clean_stall::clean_link_mux_bulk_completes_within_timeout
mux_over_rtp::mux_over_rtp_over_netem_clean_link_echoes
mux_over_rtp::mux_over_rtp_survives_netem_latency
mux_over_rtp_perf::mux_over_rtp_400kib_lossy_contended_perf
mux_over_rtp_perf::mux_over_rtp_lossy_perf_smoke
mux_over_rtp_perf::mux_over_rtp_small_stream_while_bulk_perf
rtp_and_mux::mux_over_rtp_over_netem_clean_link_echoes
rtp_and_mux::rtp_over_netem_clean_link_delivers_data
rtp_and_mux::rtp_over_netem_reliability_survives_mild_loss
```

## Opt-in manifest

Each line is `target::test_name = tier`. The set must equal the set of
non-`support` tests reported by `cargo test -p rtp_mux --test <target> --
--list --ignored`.

```gate-manifest
contested_latency::contested_capped_clean = full
contested_latency::contested_capped_jitter_loss = perf
contested_latency::contested_hostile = perf
dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction = full
dynamic_contested::dyn_dual_auto_big_first = full
dynamic_contested::dyn_dual_auto_big_first_migrating = full
dynamic_contested::dyn_dual_auto_per_message = full
dynamic_contested::dyn_dual_auto_small_first = full
dynamic_contested::dyn_dual_auto_small_first_migrating = full
dynamic_contested::dyn_dual_hint_static = full
dynamic_contested::dyn_dual_msg_channel = full
dynamic_contested::dyn_dual_msg_channel_ordered = full
dynamic_contested::dyn_game_sync_migrating = full
dynamic_contested::dyn_game_sync_single_mux = full
dynamic_contested::dyn_game_sync_sticky = full
dynamic_contested::dyn_single_mux = full
hol_probe::dual_lane_asym_frame_delivers_and_tears_down = full
hol_probe::hol_cap400_fec_solo = perf
hol_probe::hol_cap400_loss1_split_shared = perf
hol_probe::hol_cap400_shared = full
hol_probe::hol_cap400_shared_frame_delivery_diag = full
hol_probe::hol_cap400_solo = full
hol_probe::hol_hostile_shared = full
hol_probe::hol_hostile_shared_frame_delivery_diag = full
hol_probe::hol_hostile_solo = full
hol_probe::hol_hostile_split = full
hol_probe::hol_paced_bulk_median_p99_regression = full
hol_probe::hol_rtp_mux_fec_default_on_recovery = full
hol_probe::hol_rtt100_clean_shared = full
hol_probe::hol_rtt100_clean_shared_frame_delivery_diag = full
hol_probe::hol_rtt100_clean_solo = full
hol_probe::hol_rtt100_clean_split = full
hol_probe::hol_rtt100_ge1_loss1_shared = full
hol_probe::hol_rtt100_ge1_loss1_solo = full
hol_probe::hol_rtt100_ge1_loss1_split = full
hol_probe::hol_rtt100_ge1_shared_frame_delivery_diag = full
hol_probe::hol_rtt100_ge5_dual_lane_two_interactive_frame_diag = full
hol_probe::hol_rtt100_ge5_dual_lane_two_interactive_stock_diag = full
hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery = full
hol_probe::hol_rtt100_ge5_shared = full
hol_probe::hol_rtt100_ge5_shared_dual_lane = full
hol_probe::hol_rtt100_ge5_shared_dual_lane_asym_frame_diag = full
hol_probe::hol_rtt100_ge5_shared_dual_lane_frame_delivery = full
hol_probe::hol_rtt100_ge5_shared_frame_delivery = full
hol_probe::hol_rtt100_ge5_solo = full
hol_probe::hol_rtt100_ge5_split = full
hol_probe::hol_rtt100_ge5_two_interactive_frame_delivery = full
hol_probe::hol_rtt100_ge5_v2_shared = full
hol_probe::hol_rtt100_ge5_v2_solo = full
hol_probe::hol_rtt100_ge5_v3_shared = full
hol_probe::hol_rtt100_ge5_v3_solo = full
hol_probe::hol_rtt100_ge5_v3_split = full
hol_probe::hol_rtt40_ge1_loss1_shared = full
hol_probe::hol_rtt40_ge1_loss1_solo = full
hol_probe::hol_rtt40_ge1_loss1_split = full
hol_probe::hol_rtt40_ge1_shared = full
hol_probe::hol_rtt40_ge1_solo = full
hol_probe::hol_rtt40_ge1_split = full
rtp_longrun::longrun_duallane = full
rtp_longrun::multiflow_duallane = full
hol_verify4::v4_clean_muxbulk = perf
hol_verify4::v4_ge5_muxbulk = perf
mux_bulk_clean_stall::induced_stall_fires_the_watchdog = full
mux_bulk_clean_stall::slow_live_link_is_backpressure_not_a_stall = full
mux_ceiling_probe::probe_mux_echo_1mib_direct = standard
mux_ceiling_probe::probe_mux_echo_1mib_mss8k = standard
mux_ceiling_probe::probe_mux_sink_4mib_direct = standard
mux_ceiling_probe::probe_mux_sink_4mib_mss8k = standard
mux_over_rtp_perf::mux_over_rtp_400mib_hostile_perf = full
mux_stream_fairness::mux_stream_fairness_longrun = full
mux_stream_fairness::mux_stream_fairness_sweep = full
rtp_mux::rtp_mux_bidirectional_contention_offloads_both_transfers = full
rtp_mux::rtp_mux_clean_dual_lane_echoes_interactive_and_bulk_streams = full
rtp_mux::rtp_mux_explorer_relays_onto_better_path = full
rtp_mux::rtp_mux_recycle_migrates_live_streams = full
rtp_mux::rtp_mux_response_migration_offloads_download = full
rtp_mux::rtp_mux_survives_independent_impaired_lanes = full
rtp_mux_jitter::jitter_bulk_idle_restart_arm = perf
rtp_mux_jitter::jitter_burst_loss_arms = perf
rtp_mux_jitter::jitter_cellular_timeline_arms = perf
rtp_mux_jitter::jitter_decomposition = perf
rtp_mux_jitter::jitter_duallane_arms = perf
rtp_mux_jitter::jitter_duallane_constitution_gate_p99 = full
rtp_mux_jitter::jitter_fec_arms_2pct = perf
rtp_mux_jitter::jitter_fec_arms_6pct = perf
rtp_mux_jitter::jitter_frame_reorder_decomposition = perf
rtp_mux_jitter::jitter_frame_reorder_fec_arms = perf
rtp_mux_jitter::jitter_frame_reorder_fec_bulk_loss_reorder = perf
rtp_mux_jitter::jitter_interactive_bulk_and_loss = perf
rtp_mux_jitter::jitter_interactive_solo = perf
rtp_mux_jitter::jitter_interactive_with_bulk = perf
rtp_mux_jitter::jitter_interactive_with_loss = perf
rtp_mux_jitter::jitter_latency_dimension_arms = perf
rtp_mux_jitter::jitter_nonloss_impairments = perf
rtp_mux_jitter::jitter_reorder_direction = perf
rtp_mux_jitter::jitter_reorder_rate_curve = perf
rtp_mux_jitter::jitter_request_response_arms = perf
rtp_mux_jitter::jitter_shared_bottleneck_arms = perf
perf_probe::probe_hostile_goodput_30s = full
perf_probe::probe_hostile_message_latency = full
perf_probe::probe_rtp_echo_4mib_direct = standard
perf_probe::probe_rtp_echo_4mib_mss8k = standard
```

The `gate-asserting` block below records the report-only/asserting split. It
names every scenario that asserts a property (a gate): all `standard` and
`full` scenarios plus the default-tier assertions. The `perf` tier is
report-only by definition, so no `perf` scenario may appear here. The checker
derives the expected set from the manifest tiers plus `gate-default-required`
and fails if this block disagrees, and it also scans each `perf` scenario's
own body: a `perf` scenario containing `assert!`/`assert_eq!`/`assert_ne!`/
`panic!`/`unreachable!` (or the debug-only `debug_assert!`/`debug_assert_eq!`/
`debug_assert_ne!` forms) is an error, named with its file and the token found
(an asserting check filed under the report-only tier would never run).

```gate-asserting
contested_latency::contested_capped_clean
dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction
dynamic_contested::dyn_dual_auto_big_first
dynamic_contested::dyn_dual_auto_big_first_migrating
dynamic_contested::dyn_dual_auto_per_message
dynamic_contested::dyn_dual_auto_small_first
dynamic_contested::dyn_dual_auto_small_first_migrating
dynamic_contested::dyn_dual_hint_static
dynamic_contested::dyn_dual_msg_channel
dynamic_contested::dyn_dual_msg_channel_ordered
dynamic_contested::dyn_game_sync_migrating
dynamic_contested::dyn_game_sync_single_mux
dynamic_contested::dyn_game_sync_sticky
dynamic_contested::dyn_single_mux
hol_probe::dual_lane_asym_frame_delivers_and_tears_down
hol_probe::fec_gaming_treatment_has_bad_path_and_large_capacity_headroom
hol_probe::fec_saturated_pair_keys_loss_to_the_same_rtp_sequence
hol_probe::hol_cap400_shared
hol_probe::hol_cap400_shared_frame_delivery_diag
hol_probe::hol_cap400_solo
hol_probe::hol_hostile_shared
hol_probe::hol_hostile_shared_frame_delivery_diag
hol_probe::hol_hostile_solo
hol_probe::hol_hostile_split
hol_probe::hol_paced_bulk_median_p99_regression
hol_probe::hol_rtp_mux_fec_default_on_recovery
hol_probe::hol_rtt100_clean_shared
hol_probe::hol_rtt100_clean_shared_frame_delivery_diag
hol_probe::hol_rtt100_clean_solo
hol_probe::hol_rtt100_clean_split
hol_probe::hol_rtt100_ge1_loss1_shared
hol_probe::hol_rtt100_ge1_loss1_solo
hol_probe::hol_rtt100_ge1_loss1_split
hol_probe::hol_rtt100_ge1_shared_frame_delivery_diag
hol_probe::hol_rtt100_ge5_dual_lane_two_interactive_frame_diag
hol_probe::hol_rtt100_ge5_dual_lane_two_interactive_stock_diag
hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery
hol_probe::hol_rtt100_ge5_shared
hol_probe::hol_rtt100_ge5_shared_dual_lane
hol_probe::hol_rtt100_ge5_shared_dual_lane_asym_frame_diag
hol_probe::hol_rtt100_ge5_shared_dual_lane_frame_delivery
hol_probe::hol_rtt100_ge5_shared_frame_delivery
hol_probe::hol_rtt100_ge5_solo
hol_probe::hol_rtt100_ge5_split
hol_probe::hol_rtt100_ge5_two_interactive_frame_delivery
hol_probe::hol_rtt100_ge5_v2_shared
hol_probe::hol_rtt100_ge5_v2_solo
hol_probe::hol_rtt100_ge5_v3_shared
hol_probe::hol_rtt100_ge5_v3_solo
hol_probe::hol_rtt100_ge5_v3_split
hol_probe::hol_rtt40_ge1_loss1_shared
hol_probe::hol_rtt40_ge1_loss1_solo
hol_probe::hol_rtt40_ge1_loss1_split
hol_probe::hol_rtt40_ge1_shared
hol_probe::hol_rtt40_ge1_solo
hol_probe::hol_rtt40_ge1_split
rtp_longrun::longrun_duallane
rtp_longrun::multiflow_duallane
mux_bulk_clean_stall::bounded_teardown_does_not_park_on_a_stuck_blocking_task
mux_bulk_clean_stall::clean_link_mux_bulk_completes_within_timeout
mux_bulk_clean_stall::induced_stall_fires_the_watchdog
mux_bulk_clean_stall::slow_live_link_is_backpressure_not_a_stall
mux_ceiling_probe::probe_mux_echo_1mib_direct
mux_ceiling_probe::probe_mux_echo_1mib_mss8k
mux_ceiling_probe::probe_mux_sink_4mib_direct
mux_ceiling_probe::probe_mux_sink_4mib_mss8k
mux_over_rtp::mux_over_rtp_over_netem_clean_link_echoes
mux_over_rtp::mux_over_rtp_survives_netem_latency
mux_over_rtp_perf::mux_over_rtp_400kib_lossy_contended_perf
mux_over_rtp_perf::mux_over_rtp_400mib_hostile_perf
mux_over_rtp_perf::mux_over_rtp_lossy_perf_smoke
mux_over_rtp_perf::mux_over_rtp_small_stream_while_bulk_perf
mux_stream_fairness::mux_stream_fairness_longrun
mux_stream_fairness::mux_stream_fairness_sweep
rtp_and_mux::mux_over_rtp_over_netem_clean_link_echoes
rtp_and_mux::rtp_over_netem_clean_link_delivers_data
rtp_and_mux::rtp_over_netem_reliability_survives_mild_loss
rtp_mux::rtp_mux_bidirectional_contention_offloads_both_transfers
rtp_mux::rtp_mux_clean_dual_lane_echoes_interactive_and_bulk_streams
rtp_mux::rtp_mux_explorer_relays_onto_better_path
rtp_mux::rtp_mux_recycle_migrates_live_streams
rtp_mux::rtp_mux_response_migration_offloads_download
rtp_mux::rtp_mux_survives_independent_impaired_lanes
perf_probe::controller_fat_pipe_has_only_fixed_shaping
perf_probe::deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss
perf_probe::probe_hostile_goodput_30s
perf_probe::probe_hostile_message_latency
perf_probe::probe_rtp_echo_4mib_direct
perf_probe::probe_rtp_echo_4mib_mss8k
rtp_mux_jitter::jitter_duallane_constitution_gate
rtp_mux_jitter::jitter_duallane_constitution_gate_p99
mandate_smoke::m1_interactive_tail_latency
mandate_smoke::m2_interactive_delivery_and_wire
mandate_smoke::m3_bulk_goodput_fraction
mandate_smoke::m4_interactive_lane_fairness
```

## Perf-tier reach into asserting helpers

The direct-body scan only sees assertions in a `perf` scenario's own body, so
it would miss an assertion moved one call away into a helper. The
`gate-perf-guard-helpers` block below closes that hole as far as a regex-level
tool can. For every `perf` scenario the checker builds a crate-local call
graph (functions in `rtp_mux/tests/<target>.rs` and the kit sources it
reaches — the rtp_mux kit `rtp_mux/src/testkit/**` (including the
transport-mediated `mux_over_rtp` module), the mux kit `mux/src/testkit/**`,
the rtp kit `rtp/src/testkit/**` and the harness kit
`netem-test/src/kit/**`; a call is resolved against the caller file's `use`
declarations first, then the caller's own module, then a bare-name fallback)
and takes the transitive closure. Every asserting function the closure
reaches must be listed here as `RELATIVE_PATH::fn = assertion-token-count`.
The checker fails when a reachable asserting helper is unrecorded, when a
recorded helper's token count changes, or when a recorded helper is no longer
reachable.

Every entry is a report-only harness guard, not a gate: finalize/setup
helpers that abort on harness malfunction (`with_timeout`, `submit_test_task`,
`TestScope::run`/`spawn_required`, the `spawn_*_server_core` mux/rtp server
helpers, the dual-lane client-connect core
(`dual_mux_client_connect_lane_rtp_via`), `try_send_observation`), argument
validation (`percentile`, `gilbert_elliott_loss`), and the `rtp_mux_jitter`
report-only `assert_sane`/`assert_reportable` liveness floors. The
constitution outcomes they sanity-guard are asserted by the `full`-tier gates
that run them.

```gate-perf-guard-helpers
mux/src/testkit/mux.rs::mux_client_connect_core = 1
mux/src/testkit/mux.rs::mux_client_connect_frame_delivery_via = 1
netem_test/netem-test/src/kit/mod.rs::try_send_observation = 1
netem_test/netem-test/src/kit/payload.rs::with_timeout = 1
netem_test/netem-test/src/kit/presets.rs::gilbert_elliott_loss = 2
netem_test/netem-test/src/kit/stats.rs::percentile = 1
netem_test/netem-test/src/kit/task_scope.rs::run = 1
netem_test/netem-test/src/kit/task_scope.rs::spawn_required = 1
netem_test/netem-test/src/kit/task_scope.rs::submit_test_task = 2
netem_test/netem-test/src/kit/task_scope.rs::submit_test_task_required = 1
rtp/src/testkit/rtp.rs::send_timestamped_messages = 1
rtp/src/testkit/rtp.rs::spawn_rtp_byte_sink_server_core = 1
rtp_mux/src/testkit/dual.rs::dual_mux_client_connect_lane_rtp_via = 1
rtp_mux/src/testkit/mux_over_rtp.rs::spawn_mux_frame_delivery_latency_bulk_server_core = 1
rtp_mux/src/testkit/mux_over_rtp.rs::spawn_mux_over_rtp_server_core = 1
tests/rtp_mux_jitter.rs::assert_reportable = 2
tests/rtp_mux_jitter.rs::assert_sane = 2
```

## Opt-in targets outside this manifest

`check-gate.py` covers only the rtp_mux scenario targets. The crate's own
non-scenario targets (`bidirectional`, `duplex`, `explorer`, `lane_rejection`,
`session_stats`, `xsession` and the `support/**` plumbing) run in the default
tier and are not gated as scenarios. The harness crate has its own gate
(`netem_test/tests/GATE.md`) and keeps the perf-loop lane-role authority.