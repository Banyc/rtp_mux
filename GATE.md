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
   deterministic sent-message byte count; the client→server wire forwarded by
   the impairment proxy must stay within `6×` of it (measured ~3.7× on the
   seeded `both` arm, ~1.6× headroom, +50 % redundancy inflation still
   trips it). Asserted by
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
   the product ships; the measured band is ~0.87× of the shaped rate, so the
   floor leaves ~2.5× headroom. Asserted by
   `dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction`
   (`full` tier): median of three seeded runs; the sink counter is sampled
   as a window delta so the pre-window saturation phase cannot inflate the
   reading (a measured 1.88× inflation was fixed with the move).
   ```sh
   cargo test --release -p rtp_mux --test dual_lane_mandates -- \
       --ignored bulk_lane_goodput_stays_above_capacity_fraction --nocapture --test-threads=1
   ```

The interactive scaling boundary is additionally gated by
`hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery` (`full` tier):
four interactive streams on the same lane keep per-flow delivery ≥ 0.90 and
p50 ≤ 2.5× the solo reference, so the interactive lane's latency floor also
holds when several flows share it.

**Redundancy monotonicity is NOT a mandate** — it was only ever a proxy for
these outcomes. FEC recovery parity may legitimately grow with loss; what must
not happen is the interactive lane's extra/armor packets inflating its own
delivered wire. Every gate above is vacuity-checked (break the bound — inject
latency, inflate the wire, drop a delivery, starve the bulk lane — and the
gate fails naming the mandate); the harness must not restate this
constitution.

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