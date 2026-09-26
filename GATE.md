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
   it alone costs at least the whole budget before framing. The check is
   **two-sided**: the floor `wire >= offered` is asserted alongside the budget,
   because the messages whose delivery the same arm asserts must have crossed
   that path — a zeroed or unobserved wire counter reads as a pass against the
   upper bound alone. Asserted by
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

The interactive scaling boundary is gated by
`hol_probe::hol_rtt100_ge5_four_interactive_concurrent_frame_delivery`
(`full` tier): four interactive streams on the same lane, offered together on
one mux connection over one frame-delivery RTP link, keep per-flow delivery
≥ 0.90 and p50 ≤ 2.5× the solo reference, keep every flow's offering window
inside a common window for at least half its length, and keep every flow
within 15 % of its `run_for / cadence` offer schedule — so the interactive
lane's latency floor and its split across several flows both hold while the
flows are in flight together.

`hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery` (`full` tier) is
that row's baseline: the same four streams, lane, impairment, seed, message
shape and measurement over a **serialized** offer, four sweeps offered one
after another. Its per-flow numbers are therefore single-flow numbers
repeated, and no scheduler that starves a flow while another is active can
move them.

### Declared perf rows

The rows below declare twenty-five families in the `gate-perf-design` grammar:
`<row> = <tier> | <cost_s> | <relation> | <cell>[,<cell>…]`, each cell
`<property>@<dimension>=<value>[+…]`. The **default** family is the residual —
every cell name no `members.<family>` claims — and holds the pre-existing
`interactive-scaling` rows plus the two-flow rung declared alongside them; the
twenty-four named families (`constitution`, `contested`, `dual-lane`,
`establishment`, `fec`, `fec-instrument-sanity`, `frame-reorder`, `hol-cap400`,
`hol-fec`, `hol-frame`, `hol-hostile`, `hol-paced`, `hol-rtt100-clean`,
`hol-rtt100-ge5`, `hol-rtt40-ge1`, `hostile-probes`, `instrument-sanity`,
`interactive`, `latency-sweep`, `lone-tail`, `m3-bulk`, `mux-over-rtp`,
`non-loss-impairment`, `reorder`) each carry
their own baseline and their own cell-name namespace in `gate-budgets`, so a
row's cells decide whether it belongs to the family its relation names.
Keeping only the rows whose cells actually carry that name is what turns
family membership from a free label into a property of the row.

The last four of those namespaces are the ones a **cell-name collision** had
blocked, and the fix was a rename of the rows' cells, not of any arm:
`constitution` claims every `M*` name, and `M1`/`M2`/`M3` were also carried by
the interactive-cadence, bulk-goodput and latency-sweep arms, so those arms
could not be filed against the family they belong to. Their cells are now
`interactive-cadence@…`, `m3-bulk@…` and `latency-sweep@…`; the constitution
rows keep `M1`/`M2`, and `members.constitution = M*` therefore names one
family's cells and no other family's. No window, cadence, threshold, seed,
tier, `#[ignore]` reason, assertion or test body changed with those renames:
they are a change to this declaration's labels only. The collision is closed
for the rows declared here and not for every row that will ever be: `M*` is a
prefix namespace, so it claims any `M*` cell name any row carries, and the
pending declaration proposes `M*` cells for three rows this revision does not
declare (the `mandate_smoke` M2 and M4 arms and `jitter_burst_loss_arms`; the
fourth it named, `jitter_cellular_timeline_arms`, is declared below under the
`non-loss-impairment` family's own name instead). Those rows face the same
conflict on the day they are declared: either their family is `constitution` —
which they are not — or their cells are renamed the way these three were.

**The `hol` regime rename.** The largest collision left was the `hol_probe`
target's own: every one of its arms drew its cells from the same `hol*` space,
so the eight regimes the pending declaration proposes — the cap400 shaper, the
rtt100 GE5 burst-loss links, the rtt40 GE1 links, the rtt100 clean links, the
hostile real link, the frame-delivery layer, the paced-bulk regression and the
cap400 FEC arm — shared one name and no namespace could separate them; a
regime's *values*, not its cells, told them apart. Each regime now carries its
own cell name — `hol-cap400@…`, `hol-rtt100-ge5@…`, `hol-rtt40-ge1@…`,
`hol-rtt100-clean@…`, `hol-hostile@…`, `hol-frame@…`, `hol-paced@…`,
`hol-fec@…` — and names its whole link as one `impairment` value, the
convention the constitution rows already use (`impairment=clean2pct-iid`,
`impairment=owd25-iid2pct-iid6pct-ge5pct-jitter5-100-200ms`), so the
dimensions a row states beside it (`bulk`, `flows`, `seeds`, `layer`, `load`,
`cadence`, `report`, `metric`, `lanes`) are the axes it actually varies. No
window, cadence, threshold, seed, `#[ignore]` reason, assertion or test body
changed with the rename: only this declaration's labels moved, and the
seven rows of the target that stay undeclared carry the blockers recorded
below — a family name a second family claims, or a reference context no
sibling arm states.

Every member below is stated against the baseline of its own family, and the
label is the checker's derivation from the row's own cells rather than a
judgement call: of the 78 declared rows, 25 are a family's own reference,
28 vary exactly one dimension from it (`orthogonal`) and 25 vary several
(`composite(…)`), which name the dimensions they vary. No row is a
`re-measurement` — the two `hol_probe` seed-variant rows and their shared-bulk
siblings declare a `seeds` dimension instead, because their arms differ from
their reference in the loss realization and not only in the tier or the run
count, so the seed variation is visible as an axis rather than hidden as an
unstated repeat. A composite is a confounded arm — it cannot attribute a
failure on its own — so each is also recorded as an attribution gap below
rather than presented as an attributable member. The budget block covers the
rows declared here, not every row of each tier: the remaining rows are either
cell-mismatched or uncosted and are recorded as gaps below, so the per-tier
sum the checker reports is the declared subset.

```gate-perf-design
cold_connection::cold_connection_decomposition = standard | 28 | baseline@establishment | cold-connection@lanes=dual+handshake=on+impairment=clean+scale=owd20-and-owd96+metric=min-round-trips-to-stream
cold_connection::mux_lane_birth_is_one_round_trip = standard | 13 | orthogonal@establishment | cold-connection@lanes=dual+handshake=on+impairment=clean+scale=owd20-and-owd96+metric=mux-pairing-round-trips
hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery = full | 66 | baseline | interactive-scaling@flows=4+offer=sequential
hol_probe::hol_rtt100_ge5_four_interactive_concurrent_frame_delivery = full | 20 | orthogonal | interactive-scaling@flows=4+offer=concurrent
rtp_mux_jitter::jitter_duallane_constitution_gate = default | 40 | baseline@constitution | M2@lane=dual+shape=cadence+arm-set=clean-and-hostile+metric=own-wire-budget
rtp_mux_jitter::jitter_duallane_constitution_gate_p99 = full | 105 | orthogonal@constitution | M1@lane=dual+shape=cadence+arm-set=clean-and-hostile+metric=p99-median-of-3
rtp_mux_jitter::jitter_fec_arms_2pct = perf | 175 | baseline@fec | fec-tuning@impairment=loss2pct-iid+fec=off-stock-prompt+metric=parity-and-latency
rtp_mux_jitter::jitter_fec_arms_6pct = perf | 175 | orthogonal@fec | fec-tuning@impairment=loss6pct-iid+fec=off-stock-prompt+metric=parity-and-latency
rtp_mux_jitter::jitter_frame_reorder_fec_arms = perf | 210 | baseline@frame-reorder | frame-reorder-fec@layer=rtp-frame+reorder=fast-forward+fec=on+impairment=loss2pct-iid
rtp_mux_jitter::jitter_frame_reorder_fec_bulk_loss_reorder = perf | 140 | orthogonal@frame-reorder | frame-reorder-fec@layer=rtp-frame+reorder=fast-forward+fec=on+load=bulk+impairment=loss2pct-iid
rtp_mux_jitter::jitter_reorder_direction = perf | 70 | baseline@reorder | reorder-direction@impairment=reorder+direction=c2s-and-s2c+metric=p99
rtp_mux_jitter::jitter_reorder_rate_curve = perf | 140 | orthogonal@reorder | reorder-rate@impairment=reorder+rate=curve+metric=p99
rtp_mux_jitter::jitter_interactive_solo = perf | 35 | baseline@interactive | interactive-cadence@lane=interactive+flows=1+shape=cadence+loss=none+rate=none+load=none+metric=latency-percentiles
rtp_mux_jitter::jitter_interactive_with_loss = perf | 35 | orthogonal@interactive | interactive-cadence@lane=interactive+flows=1+shape=cadence+loss=2pct-iid+rate=none+load=none+metric=latency-percentiles
rtp_mux_jitter::jitter_interactive_with_bulk = perf | 65 | composite(rate,load)@interactive | interactive-cadence@lane=interactive+flows=1+shape=cadence+loss=none+rate=1MiBps+load=bulk-burst-2MiB-per-3s+metric=latency-percentiles
rtp_mux_jitter::jitter_interactive_bulk_and_loss = perf | 35 | composite(loss,rate,load)@interactive | interactive-cadence@lane=interactive+flows=1+shape=cadence+loss=2pct-iid+rate=1MiBps+load=bulk-burst-2MiB-per-3s+metric=latency-percentiles
mandate_smoke::m3_bulk_goodput_fraction = default | 108 | baseline@m3-bulk | m3-bulk@lane=bulk+rate=1MiBps+load=saturated+window=6s+metric=capacity-fraction
dual_lane_mandates::bulk_lane_goodput_stays_above_capacity_fraction = full | 45 | orthogonal@m3-bulk | m3-bulk@lane=bulk+rate=1MiBps+load=saturated+window=15s+metric=capacity-fraction
rtp_mux_jitter::jitter_bulk_idle_restart_arm = perf | 35 | composite(rate,load,window,metric)@m3-bulk | m3-bulk@lane=bulk+rate=2Mbps-c2s+load=idle-restart-bursts-512KiB+window=34s+metric=delivered-goodput
rtp_mux_jitter::jitter_latency_dimension_arms = perf | 385 | baseline@latency-sweep | latency-sweep@sweep=shared-capacity-ack-path-cellular+metric=latency-percentiles-and-goodput
rtp_mux_jitter::jitter_shared_bottleneck_arms = perf | 157 | orthogonal@latency-sweep | latency-sweep@sweep=shared-capacity-queue-depth+metric=latency-percentiles-and-goodput
rtp_mux_jitter::jitter_request_response_arms = perf | 1170 | baseline@lone-tail | lone-tail@lane=dual+shape=request-response+depth=1-and-2+impairment=owd25-iid2pct-iid6pct-ge5pct-jitter5-100-200ms+metric=p99-and-over250-share
mandate_smoke::m1_lone_tail_field_rtt = full | 20 | composite(depth,impairment)@lone-tail | lone-tail@lane=dual+shape=request-response+depth=1+impairment=owd100-ge5pct-jitter100ms+metric=p99-and-over250-share
mandate_smoke::m1_lone_tail_field_rtt_depth_sweep = full | 38 | orthogonal@lone-tail | lone-tail@lane=dual+shape=request-response+depth=1-and-2+impairment=owd100-ge5pct-jitter100ms+metric=p99-and-over250-share
hol_probe::hol_cap400_fec_solo = perf | 20 | baseline@hol-fec | hol-fec@impairment=cap400-loss1+fec=on+bulk=none+metric=p99
hol_probe::hol_cap400_loss1_split_shared = perf | 20 | composite(bulk,impairment,metric)@hol-cap400 | hol-cap400@impairment=cap400-loss1-shaper+bulk=split-shared+metric=p99
hol_probe::hol_cap400_shared = full | 20 | composite(bulk,metric)@hol-cap400 | hol-cap400@impairment=cap400-loss1+bulk=shared+flows=1+metric=p99
hol_probe::hol_cap400_shared_frame_delivery_diag = full | 20 | orthogonal@hol-frame | hol-frame@impairment=cap400-loss1+layer=rtp-frame+bulk=shared+load=saturating+report=diag+metric=delivery
hol_probe::hol_cap400_solo = full | 59 | baseline@hol-cap400 | hol-cap400@impairment=cap400-loss1+bulk=none+flows=1+metric=p99-median-of-3
hol_probe::hol_hostile_shared = full | 20 | orthogonal@hol-hostile | hol-hostile@impairment=hostile-real-link+cadence=200ms+bulk=shared+flows=1+metric=delivery
hol_probe::hol_hostile_shared_frame_delivery_diag = full | 21 | composite(cadence,impairment)@hol-frame | hol-frame@impairment=hostile-real-link+cadence=200ms+layer=rtp-frame+bulk=shared+load=saturating+report=diag+metric=delivery
hol_probe::hol_hostile_solo = full | 20 | baseline@hol-hostile | hol-hostile@impairment=hostile-real-link+cadence=200ms+bulk=none+flows=1+metric=delivery
hol_probe::hol_hostile_split = full | 20 | orthogonal@hol-hostile | hol-hostile@impairment=hostile-real-link+cadence=200ms+bulk=split+flows=1+metric=delivery
hol_probe::hol_paced_bulk_median_p99_regression = full | 59 | baseline@hol-paced | hol-paced@impairment=rtt100-ge5+layer=rtp-frame+bulk=shared+load=paced+metric=delivery-p50-and-p99-median-of-3
hol_probe::hol_rtp_mux_fec_default_on_recovery = full | 38 | composite(bulk,fec,impairment,layer,metric)@hol-fec | hol-fec-recovery@layer=rtp-mux+fec=default-on+impairment=fec-gaming-fat-pipe-20pct+bulk=controller-retention+metric=parity-and-reconstruction
hol_probe::hol_rtt100_clean_shared = full | 20 | orthogonal@hol-rtt100-clean | hol-rtt100-clean@impairment=rtt100-clean+bulk=shared+flows=1+metric=p99
hol_probe::hol_rtt100_clean_shared_frame_delivery_diag = full | 20 | baseline@hol-frame | hol-frame@impairment=rtt100-clean+layer=rtp-frame+bulk=shared+load=saturating+report=diag+metric=delivery
hol_probe::hol_rtt100_clean_solo = full | 20 | baseline@hol-rtt100-clean | hol-rtt100-clean@impairment=rtt100-clean+bulk=none+flows=1+metric=p99
hol_probe::hol_rtt100_clean_split = full | 20 | orthogonal@hol-rtt100-clean | hol-rtt100-clean@impairment=rtt100-clean+bulk=split+flows=1+metric=p99
hol_probe::hol_rtt100_ge1_loss1_shared = full | 20 | composite(bulk,impairment)@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge1-loss1+bulk=shared+flows=1+metric=p99
hol_probe::hol_rtt100_ge1_loss1_solo = full | 20 | orthogonal@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge1-loss1+bulk=none+flows=1+metric=p99
hol_probe::hol_rtt100_ge1_loss1_split = full | 20 | composite(bulk,impairment)@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge1-loss1+bulk=split+flows=1+metric=p99
hol_probe::hol_rtt100_ge1_shared_frame_delivery_diag = full | 20 | orthogonal@hol-frame | hol-frame@impairment=rtt100-ge1-loss1+layer=rtp-frame+bulk=shared+load=saturating+report=diag+metric=delivery
hol_probe::hol_rtt100_ge5_shared = full | 20 | orthogonal@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=shared+flows=1+metric=p99
hol_probe::hol_rtt100_ge5_shared_dual_lane = full | 20 | composite(bulk,lanes)@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=shared+flows=1+lanes=dual+metric=p99
hol_probe::hol_rtt100_ge5_shared_frame_delivery = full | 20 | composite(load,metric)@hol-paced | hol-paced@impairment=rtt100-ge5+layer=rtp-frame+bulk=shared+load=saturating+metric=delivery
hol_probe::hol_rtt100_ge5_solo = full | 20 | baseline@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=none+flows=1+metric=p99
hol_probe::hol_rtt100_ge5_split = full | 20 | orthogonal@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=split+flows=1+metric=p99
hol_probe::hol_rtt100_ge5_two_interactive_frame_delivery = full | 20 | orthogonal | interactive-scaling@flows=2+offer=sequential
hol_probe::hol_rtt100_ge5_v2_shared = full | 20 | composite(bulk,seeds)@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=shared+flows=1+seeds=v2+metric=p99
hol_probe::hol_rtt100_ge5_v2_solo = full | 20 | orthogonal@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=none+flows=1+seeds=v2+metric=p99
hol_probe::hol_rtt100_ge5_v3_shared = full | 20 | composite(bulk,seeds)@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=shared+flows=1+seeds=v3+metric=p99
hol_probe::hol_rtt100_ge5_v3_solo = full | 20 | orthogonal@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=none+flows=1+seeds=v3+metric=p99
hol_probe::hol_rtt100_ge5_v3_split = full | 20 | composite(bulk,seeds)@hol-rtt100-ge5 | hol-rtt100-ge5@impairment=rtt100-ge5+bulk=split+flows=1+seeds=v3+metric=p99
hol_probe::hol_rtt40_ge1_loss1_shared = full | 20 | composite(bulk,impairment)@hol-rtt40-ge1 | hol-rtt40-ge1@impairment=rtt40-ge1-loss1+bulk=shared+flows=1+metric=p99
hol_probe::hol_rtt40_ge1_loss1_solo = full | 20 | orthogonal@hol-rtt40-ge1 | hol-rtt40-ge1@impairment=rtt40-ge1-loss1+bulk=none+flows=1+metric=p99
hol_probe::hol_rtt40_ge1_loss1_split = full | 20 | composite(bulk,impairment)@hol-rtt40-ge1 | hol-rtt40-ge1@impairment=rtt40-ge1-loss1+bulk=split+flows=1+metric=p99
hol_probe::hol_rtt40_ge1_shared = full | 20 | orthogonal@hol-rtt40-ge1 | hol-rtt40-ge1@impairment=rtt40-ge1+bulk=shared+flows=1+metric=p99
hol_probe::hol_rtt40_ge1_solo = full | 20 | baseline@hol-rtt40-ge1 | hol-rtt40-ge1@impairment=rtt40-ge1+bulk=none+flows=1+metric=p99
hol_probe::hol_rtt40_ge1_split = full | 20 | orthogonal@hol-rtt40-ge1 | hol-rtt40-ge1@impairment=rtt40-ge1+bulk=split+flows=1+metric=p99
hol_probe::dual_lane_asym_frame_delivers_and_tears_down = full | 20 | baseline@dual-lane | hol-dual-lane@lane=dual+impairment=rtt100-ge5+interactive=rtp-frame+bulk=stock+flows=1+metric=delivery-and-teardown
hol_probe::hol_rtt100_ge5_shared_dual_lane_asym_frame_diag = full | 20 | orthogonal@dual-lane | hol-dual-lane@lane=dual+impairment=rtt100-ge5+interactive=rtp-frame+bulk=stock+flows=1+metric=delivery-liveness
hol_probe::hol_rtt100_ge5_shared_dual_lane_frame_delivery = full | 17 | composite(bulk,metric)@dual-lane | hol-dual-lane@lane=dual+impairment=rtt100-ge5+interactive=rtp-frame+bulk=rtp-frame+flows=1+metric=delivery-floor
hol_probe::hol_rtt100_ge5_dual_lane_two_interactive_frame_diag = full | 20 | composite(flows,metric)@dual-lane | hol-dual-lane@lane=dual+impairment=rtt100-ge5+interactive=rtp-frame+bulk=stock+flows=2+metric=delivery-liveness
hol_probe::hol_rtt100_ge5_dual_lane_two_interactive_stock_diag = full | 20 | composite(flows,interactive,metric)@dual-lane | hol-dual-lane@lane=dual+impairment=rtt100-ge5+interactive=stock+bulk=stock+flows=2+metric=delivery-liveness
rtp_mux_jitter::jitter_duallane_arms = perf | 280 | composite(impairment,metric,reorder)@dual-lane | hol-dual-lane-matched-load@lane=dual+impairment=owd25-jitter5-loss2pct-or-bulk-matched+interactive=rtp-frame+bulk=stock+flows=1+reorder=fast-forward-and-strict+metric=latency-and-bulk-goodput
hol_probe::fec_gaming_treatment_has_bad_path_and_large_capacity_headroom = default | 0 | baseline@fec-instrument-sanity | fec-instrument-sanity@layer=rtp-fec+metric=path-and-headroom
hol_probe::fec_saturated_pair_keys_loss_to_the_same_rtp_sequence = default | 0 | orthogonal@fec-instrument-sanity | fec-instrument-sanity@layer=rtp-fec+metric=sequence-keying
perf_probe::controller_fat_pipe_has_only_fixed_shaping = default | 0 | baseline@instrument-sanity | instrument-sanity@lane=controller-fat-pipe+metric=shaping-determinism
perf_probe::deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss = default | 0 | composite(lane,metric)@instrument-sanity | instrument-sanity@lane=deterministic-iid-loss-fat-pipe+metric=loss-determinism
perf_probe::probe_hostile_goodput_30s = full | 51 | baseline@hostile-probes | hostile-probe@layer=mux-over-rtp+impairment=hostile-real-link+shape=counting-sink+window=30s+metric=goodput-floor
perf_probe::probe_hostile_message_latency = full | 40 | composite(impairment,metric,shape)@hostile-probes | hostile-probe@layer=mux-over-rtp+impairment=hostile-periodic-bottleneck-300ms+shape=timestamped-messages+window=30s+metric=latency-percentiles
contested_latency::contested_capped_clean = full | 43 | baseline@contested | contested@rate=cap+jitter=0+loss=0+metric=p99-with-bulk
contested_latency::contested_hostile = perf | 44 | orthogonal@contested | contested@impairment=hostile-preset+metric=p99-with-bulk
mux_over_rtp_perf::mux_over_rtp_lossy_perf_smoke = default | 1 | baseline@mux-over-rtp | mux-over-rtp@impairment=lossy-400kib-per-sec+shape=echo+scale=1KiB+metric=delivery-and-wire
mux_over_rtp_perf::mux_over_rtp_400mib_hostile_perf = full | 344 | composite(impairment,metric,scale,shape)@mux-over-rtp | mux-over-rtp@impairment=hostile-fat-pipe+shape=sink+scale=400MiB+metric=delivery-and-goodput
rtp_mux_jitter::jitter_nonloss_impairments = perf | 210 | baseline@non-loss-impairment | non-loss-impairment@lane=interactive+layer=rtp-frame+shape=cadence+flows=1+loss=none+rate=none+load=none+impairment=jitter-reorder-duplication-and-strict-reorder+metric=p99
rtp_mux_jitter::jitter_cellular_timeline_arms = perf | 70 | composite(impairment,lane,report)@non-loss-impairment | non-loss-impairment@lane=dual+layer=rtp-frame+shape=cadence+flows=1+loss=none+rate=none+load=none+impairment=owd25-jitter100-and-200ms+report=liveness+metric=p99
```

Declared sums are `default` 149 s, `standard` 41 s, `full` 1646 s and `perf`
3471 s of the 300 s, 600 s, 1700 s and 3500 s budgets. `full` and `perf` are
raised as a declared change (200 → 300 and 1000 → 3000), and `full` again
(300 → 1200) for the 34 `full`-tier `hol` rows this revision adds, which cost
777 s; a tier's declared sum must fit its ceiling, and the sums are the
declared subset's costs, not the tiers' full run time. `perf` is raised again
as a declared change (3000 → 3300) for the `dual-lane` family's
`jitter_duallane_arms` (280 s), whose cell the same family's rename admits:
2867 + 280 = 3147 s, and the arithmetic is the declared sum the checker
prints. `full` is raised again as a declared change (1200 → 1700) for the four
`full`-tier rows the `hostile-probes`, `contested` and `mux-over-rtp` families
add, which cost 478 s and are measured rather than read: 1033 + 97 (the
`dual-lane` rows) = 1130, plus 51 + 40 + 43 + 344 = 1608 s. `perf` is raised a
second time as a declared change (3300 → 3500) for the `non-loss-impairment`
family's two `perf` rows, 3191 + 210 + 70 = 3471 s. Each raise is the smallest
ceiling that admits the measured sum with room for the tier's other,
still-undeclared rows, and the checker prints both the sum and the ceiling.
The new field-depth arm adds 38 s to `full` (1608 + 38 = 1646 s), which the
1700 s ceiling already admits, so no further raise is needed. On the runner's
own scale the `default` sum is 149 s (the M3 smoke row's 108 s plus the
constitution gate's 40 s and the lossy smoke's 1 s), inside its
300 s ceiling: no raise. The `standard`
ceiling keeps its 600 s and still carries the two cold-connection rows, its
only declared rows. The concurrent row is one
dimension (`offer`) away from the default baseline: the same four flows, the
same lane, the same seeds, the same offer, polled together instead of one
after another, and the two-flow rung declared with it is that same serialized
offer at half the flow count. The two `interactive-scaling` rows keep the
costs they already declared (66 s and 20 s, re-measured 69.0 s and 19.5 s),
and no previously declared row's tier, cost, relation or cells change.

**Cost provenance.** No cost here is invented. Of the twelve rows the earlier
revisions declared, `jitter_duallane_constitution_gate` is the "~40 s
wall-clock dual-lane run" this file's Tiers section records, seven are the
arm count in the row's own `#[ignore]` reason string times the ~35 s per-arm
wall-clock that string states — `three ~35 s dual-lane constitution runs` →
105 s, `five ~35 s arms` → 175 s, `six ~35 s frame+FEC arms` → 210 s,
`four ~35 s arms` → 140 s, `two ~35 s reorder arms` → 70 s — and four are the
measured costs their own sections record: the two `interactive-scaling` rows
(66 s and 20 s, measured 69.1 s and 19.5 s) and the two cold-connection rows
(28 s and 13 s).

The eleven rows the namespace repair adds are sourced the same way. Eight are
read off the run the row names, each from its own `#[ignore]` reason: the four
`interactive` rows are `~35 s` / `~35 s` / `~65 s measurement (two arms)` /
`~35 s` → 35/35/65/35 s; `jitter_bulk_idle_restart_arm` is `one ~35 s
idle-restart arm` → 35 s; `jitter_latency_dimension_arms` is `eleven ~35 s
dimension arms` → 385 s; `jitter_request_response_arms` is `eighteen ~65 s
request/response (lone-tail) arms` → 1170 s;
`bulk_lane_goodput_stays_above_capacity_fraction` is `three 15 s dual-lane
saturated runs` → 45 s; and `m1_lone_tail_field_rtt` states its own `~20 s`.
Two are **measured** for this declaration rather than read:
`mandate_smoke::m3_bulk_goodput_fraction` and
`jitter_shared_bottleneck_arms`. `jitter_shared_bottleneck_arms` ran in
156.10 s (for `cargo test --release -p rtp_mux --test rtp_mux_jitter --
--ignored --exact jitter_shared_bottleneck_arms --nocapture
--test-threads=1`, `real 156.33 s`) and is declared 157 s.
`mandate_smoke::m3_bulk_goodput_fraction` is stated on the scale the
checker's **measured side** uses, which is not the same scale as a serialized
single-test invocation. `check-gate.py --mandate-check-json` compares a
declared cost with `tools/mandate-check`'s stamp, and that runner invokes the
`mandate_smoke` target with libtest's default threading, so the four smoke
tests run four-way concurrent and the row's stamp carries whatever contention
the scheduler gives it. Four such runs stamped it at 61.564 s, 111.474 s,
112.541 s and 142.576 s — the first is the row finishing before the other
three have ramped, the last is the row sharing the box with all of them — so
the stamp is not one number and a declared figure has to sit inside the band
the 50 % tolerance admits on both ends: `[142.576/1.5, 61.564/0.5]` =
`[95.05, 123.13]`. It is declared 108 s, the geometric middle of that window
(`|61.564-108|/108` = 43 %, `|111.474-108|/108` = 3 %,
`|112.541-108|/108` = 4 %, `|142.576-108|/108` = 32 %). The earlier revision of
this declaration stated the `--exact` single-test figure, 61.56 s (with
`real 64.99 s` for the whole invocation), which is what the row costs without
contention at all — outside the window, and past the tolerance against the
runner's larger draws, which is why the drift comparison failed against it.
Every stated figure in this paragraph is a run this revision performed or the
previous one recorded; none is inferred, and the budget below carries the
nominal 108 s rather than the spread. A row whose
wall-clock appears in no document is **not** given a number: it is recorded as
a gap below, so an unmeasured cost is visibly pending rather than plausibly
guessed.

The 36 rows the `hol` rename adds take their costs from **running the arms**
at the declaration's own scale, not from a document: no `#[ignore]` reason in
`hol_probe.rs` states a wall-clock, so `RUSTC_BOOTSTRAP=1 cargo test --release
-p rtp_mux --test hol_probe -- --ignored --test-threads=1 -Z unstable-options
--report-time --nocapture` ran all 43 ignored arms of that target in one
invocation and let libtest stamp each one (43 passed, 0 failed, `finished in
984.45s`; the 43 stamps sum to 984.44 s, so the per-test costs fit the target's
own total). Each declared cost is its stamp rounded up to the next second:
20 s for the 19.5 s single-window arms, 59 s for the two triple-run arms
(`hol_cap400_solo` 58.609 s, `hol_paced_bulk_median_p99_regression` 58.599 s),
38 s for the two-arm FEC-recovery row (37.667 s) and 21 s for
`hol_hostile_shared_frame_delivery_diag` (20.086 s). The same run re-measured
the two already-declared rows on their own stamps and matched the costs they
already declared (`hol_rtt100_ge5_four_interactive_frame_delivery` 69.019 s
against 66 s, `..._four_interactive_concurrent_...` 19.524 s against 20 s),
which is the check that the stamps and the declaration are on one scale. The
seven rows of the target that this revision found undeclared are declared
below: five as the `dual-lane` family's topology rows and two as the
`fec-instrument-sanity` family's, with the `perf_probe` pair and
`jitter_duallane_arms` declared beside them.
The five dual-lane topology rows the declaration was blocked
on are
declared here on their own stamps (20 s for 19.574 s, 20 s for 19.553 s, 20 s
for 19.542 s, 20 s for 19.535 s, 17 s for 16.925 s), and the family's sixth
member is `jitter_duallane_arms`, whose cost is the `eight ~35 s dual-lane
arms (fast-forward + strict)` its own `#[ignore]` reason states, 8 x 35 = 280 s.

**The `dual-lane` family.** The five `hol_probe` dual-lane topology rows and
`jitter_duallane_arms` were blocked by a cell name, not by a cost: the first
five already carried `hol-dual-lane`, while the sixth carried
`dual-lane-matched-load`, which `members.dual-lane = hol-dual-lane*` does not
claim. The repair is the rename and it changed no arm — the sixth row's cell
is now `hol-dual-lane-matched-load` and nothing about the arm moved. The
family's reference is `dual_lane_asym_frame_delivers_and_tears_down`, the arm
that states its context most completely (two independent RTP connections, one
interactive lane in RTP frame-delivery mode and one stock byte-stream bulk
lane, at the `rtt100-ge5` seed pair, measuring delivery and teardown). The
axes the members state beside it are the two `DualLaneProbeConfig` modes the
probe actually sets (`interactive`, `bulk`), the interactive stream count
(`flows`), the impairment value and, for the sixth row, its `reorder` mode;
the `metric` each one asserts is stated too. One member is one declared
dimension from the reference (`..._asym_frame_diag`, whose metric is liveness
rather than delivery-and-teardown); the other four vary two or three, and are
recorded as composites below rather than presented as attributable.

**The two instrument-sanity families.** `hol_probe`'s FEC pair and
`perf_probe`'s determinism pair carried one cell name, `instrument-sanity`, so
neither could be filed: a cell name belongs to exactly one family and the two
pairs are not one family. The repair is a name split, not an arm change — the
FEC pair's cell is now `fec-instrument-sanity@…`, which is the namespace it is
declared against, and `instrument-sanity` names only the `perf_probe` pair.
All four costs are measured, none is stated in an `#[ignore]` reason, and all
four are 0 s: each is a `#[test]` that builds two preset configs and compares
their fields, and libtest's own stamp for each is `0.000s`
(`--exact fec_gaming_treatment_has_bad_path_and_large_capacity_headroom`,
`--exact fec_saturated_pair_keys_loss_to_the_same_rtp_sequence`,
`--exact controller_fat_pipe_has_only_fixed_shaping`,
`--exact deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss`, each run
in release with `-Z unstable-options --report-time`). Zero is below the
one-second resolution the other costs round to, and it is the honest figure
rather than a rounded-up one: the four are declared so the tier that runs on
every commit accounts for them, not because they cost anything.

**The hostile-probe, contested and mux-over-rtp rows.** Six rows are declared
here whose cost no `#[ignore]` reason states and no document records. Each is
its own libtest stamp from a single `--exact` run of its own target in
release, with `-Z unstable-options --report-time`:
`probe_hostile_goodput_30s` 50.017 s → 51 s,
`probe_hostile_message_latency` 39.013 s → 40 s,
`contested_capped_clean` 42.111 s → 43 s,
`contested_hostile` 43.092 s → 44 s,
`mux_over_rtp_lossy_perf_smoke` 0.033 s → 1 s, and
`mux_over_rtp_400mib_hostile_perf` 343.853 s → 344 s; each rounds up to the
next second. The `hostile-probes` family is the two `probe_hostile_*` rows on
their own: `ceiling` was shared with the `rtp` echo pair and is narrowed here
to `hostile-probe` on the hostile pair's side, which is what separates the two
families. The `contested` family lands with its clean reference
(`contested_capped_clean`'s 400 KiB/s cap, no loss, no jitter) and the
refiled `contested_hostile`, which varies the impairment alone — the hostile
link is where the capped queue is, so the two are one declared dimension
apart. The `mux-over-rtp` family lands with the lossy smoke as its reference
and the 400 MiB hostile arm refiled into it; that arm runs on the
`hostile-fat-pipe` preset, not the `hostile_real_link` the goodput probe uses,
so its cell names the preset it actually takes rather than a family-level
"hostile" label.

**The `non-loss-impairment` family.** `jitter_cellular_timeline_arms` and
`jitter_nonloss_impairments` were the two rows the pending declaration filed
into `lone-tail`, and they do not belong there: both offer the *cadence* shape
(`InteractiveLoad::Cadence`, or the equivalent fire-and-forget frame-delivery
loop), not the request/response shape a lone tail is about, so a `lone-tail@…`
cell would state a coverage neither arm has. The repair is a refiling rather
than that rename: they carry `non-loss-impairment@…` — the family's own name,
which is what they measure, an interactive lane impaired by something that is
not packet loss — and `jitter_nonloss_impairments` is the family's reference,
because it is the arm that states that context most completely (the clean
floor, jitter alone, a reorder window, duplication, reorder-with-duplication
and the strict reorder path, all at 25 ms one-way on the deployment's
interactive lane, frame layer). `jitter_cellular_timeline_arms` then varies
three declared dimensions from it: the topology (`lane=dual` rather than the
single interactive lane, with the bulk lane idle), the impairment (100/200 ms
uniform jitter on the same 25 ms delay, rather than the jitter/reorder/
duplication sweep) and the assertion strength (`report=liveness`, because that
arm is report-only and asserts only that it ran, while the reference's
`assert_sane` checks the summary). Its cost and the reference's are the arm
counts their own `#[ignore]` reasons state — `two ~35 s cellular-jitter arms`
→ 70 s, `six ~35 s non-loss arms` → 210 s — and neither arm, window, cadence,
threshold, seed, tier or assertion changed.

**The cold-connection rows** are the two `standard` rows of the
`establishment` family. `cold_connection_decomposition` decomposes a cold
dual-lane birth against rtp's own public API at two clean regimes (`owd20`
and `owd96`, calibrated base RTTs ~41 ms and ~192 ms) — the bare rtp opening
handshake on one lane, the same two lanes dialed sequentially and
concurrently, and the production `RtpMuxConnector` cold connect — and gates
the one part rtp_mux owns: the production birth must not pay two *serialized*
rtp opening handshakes when both lanes are independent.
`mux_lane_birth_is_one_round_trip` is its one-dimension-away member (the
metric changes from round trips to the stream to the mux pairing's own round
trips): on two already-established rtp sessions, the lane hello / pairing /
first-frame readiness costs exactly one base RTT, which pins the mux-side
share of the decomposition independently of rtp's handshake. Both costs are
measured, not stated in an `#[ignore]` reason: 28 s and 13 s.

Measured on loopback (median of nine cold connections per arm, achieved base
RTT calibrated per regime): the bare rtp opening handshake costs ~2.7 x and
~2.2 x base RTTs (the whole of a one-lane session's setup — the same connect
with the handshake off is ~0.0 x), two lanes dialed sequentially cost ~5.4 x
and ~4.3 x, the same two dialed concurrently ~3.0 x and ~2.2 x, the mux lane
birth 1.0 x, and the production cold connect ~4.1 x and ~3.2 x. The mux
lane birth on already-established sessions is the production birth's whole
residue over the concurrent bare rtp floor. The two dials being serialized
used to put that connect at ~6.4 x and ~5.1 x; the arms fail at both regimes
when the production path dials them one after the other.
`RTP_MUX_COLD_CONNECTION_FAULT=serialize` is the vacuity injection: it
replaces the production arm with a reproduction of the two-serialized-dial
critical path, which must fail the gate.

```gate-budgets
default = 300
standard = 600
full = 1700
perf = 3500
baseline = hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery
baseline.constitution = rtp_mux_jitter::jitter_duallane_constitution_gate
baseline.contested = contested_latency::contested_capped_clean
baseline.dual-lane = hol_probe::dual_lane_asym_frame_delivers_and_tears_down
baseline.establishment = cold_connection::cold_connection_decomposition
baseline.fec = rtp_mux_jitter::jitter_fec_arms_2pct
baseline.fec-instrument-sanity = hol_probe::fec_gaming_treatment_has_bad_path_and_large_capacity_headroom
baseline.frame-reorder = rtp_mux_jitter::jitter_frame_reorder_fec_arms
baseline.hol-cap400 = hol_probe::hol_cap400_solo
baseline.hol-fec = hol_probe::hol_cap400_fec_solo
baseline.hol-frame = hol_probe::hol_rtt100_clean_shared_frame_delivery_diag
baseline.hol-hostile = hol_probe::hol_hostile_solo
baseline.hol-paced = hol_probe::hol_paced_bulk_median_p99_regression
baseline.hol-rtt100-clean = hol_probe::hol_rtt100_clean_solo
baseline.hol-rtt100-ge5 = hol_probe::hol_rtt100_ge5_solo
baseline.hol-rtt40-ge1 = hol_probe::hol_rtt40_ge1_solo
baseline.hostile-probes = perf_probe::probe_hostile_goodput_30s
baseline.instrument-sanity = perf_probe::controller_fat_pipe_has_only_fixed_shaping
baseline.interactive = rtp_mux_jitter::jitter_interactive_solo
baseline.latency-sweep = rtp_mux_jitter::jitter_latency_dimension_arms
baseline.lone-tail = rtp_mux_jitter::jitter_request_response_arms
baseline.m3-bulk = mandate_smoke::m3_bulk_goodput_fraction
baseline.mux-over-rtp = mux_over_rtp_perf::mux_over_rtp_lossy_perf_smoke
baseline.non-loss-impairment = rtp_mux_jitter::jitter_nonloss_impairments
baseline.reorder = rtp_mux_jitter::jitter_reorder_direction
members.constitution = M*
members.contested = contested*
members.dual-lane = hol-dual-lane*
members.establishment = cold-connection*
members.fec = fec-tuning*
members.fec-instrument-sanity = fec-instrument-sanity*
members.frame-reorder = frame-reorder-fec
members.hol-cap400 = hol-cap400*
members.hol-fec = hol-fec*
members.hol-frame = hol-frame*
members.hol-hostile = hol-hostile*
members.hol-paced = hol-paced*
members.hol-rtt100-clean = hol-rtt100-clean*
members.hol-rtt100-ge5 = hol-rtt100-ge5*
members.hol-rtt40-ge1 = hol-rtt40-ge1*
members.hostile-probes = hostile-probe*
members.instrument-sanity = instrument-sanity*
members.interactive = interactive-cadence*
members.latency-sweep = latency-sweep*
members.lone-tail = lone-tail*
members.m3-bulk = m3-bulk*
members.mux-over-rtp = mux-over-rtp*
members.non-loss-impairment = non-loss-impairment*
members.reorder = reorder-*
drift = 0.5
drift_floor_s = 2.0
```

The rest of the pending declaration is **not** declared, and each family below
records why and what would make it declarable. The granularity is the family,
not the row: a family whose rows' cells are not family-derivable is one gap
naming the repair, because refiling those rows *is* the repair — recording the
mismatched rows individually would be noise that hides the distinct blockers.
Three blockers account for all of them: a cell name two or more families
claim, a wall-clock no document records, and a family whose rows' cells name a
context the arm does not have — where the recorded rename would state coverage
the arm cannot claim. A **new arm closes none of the first two** — a new arm
inherits the same cell name and is equally misfiled, and it needs its own cost,
the very thing missing — so no family here was closed by adding an arm: the
four families the `M*` collision blocked were closed by renaming cells only,
and the eight `hol` regimes by the same repair plus one measurement per row, a
cost no `#[ignore]` reason states.

```gate-coverage-gaps
cold-connection@lanes=dual+handshake=on+impairment=loss-or-jitter-or-reorder = the cold birth is measured on clean links only; what loss does to a birth is rtp's opening handshake's own retry behaviour (its `OPENING_TIMEOUT`/`RETRY_INTERVAL`), which the `rtp` crate owns and which a clean-link decomposition cannot attribute to rtp_mux, so no impaired birth row is claimed here.
cold-connection@lanes=dual+handshake=off = the same birth with the rtp opening handshake disabled is a *different* protocol configuration (the production server binds `handshake: true`); it is measured inside the row as the bare one-lane arm's no-handshake control (min 0.0-0.1 ms, i.e. the whole one-lane cost is the handshake), not as a separate row, because the deployed path never takes it.
cold-connection@lanes=single+shape=proxy-chain = the row measures the rtp_mux birth on loopback with no proxy in the path; the deployed chain's cold total (and the proxy's share of it) is measured by the proxy-path iteration that owns those arms, and loopback cannot speak to a real path's delay distribution, so no field-scale claim is made here.
cold-connection@metric=cpu = per-datagram CPU cost is measured by owning-symbol attribution (`tools/samply_hotspots.py`), not by a scenario in this crate.
cold-connection@lanes=dual+handshake=on+scale=multipath = the multi-path UDP transport (rtp's `mpudp`) has no rtp_mux birth arm; a cell for it belongs to the layer that owns that transport.
interactive-scaling@flows=2+offer=concurrent = the two-flow rung is the pre-existing serialized `hol_rtt100_ge5_two_interactive_frame_delivery`, now declared in the default family as `flows=2+offer=sequential`; it stays as an arm, and the concurrent offer is declared at four flows, the rung this family and M4 name, so a two-flow concurrent row would repeat it at a smaller N without a new regime.
interactive-scaling@flows=4+offer=concurrent+bulk=saturating = the family's bulk-sharing member (`hol_rtt100_ge5_shared_frame_delivery`) is single-flow with a saturating bulk stream; adding a saturating bulk stream to the concurrent row varies two dimensions from the baseline at once and would need its own derivation for what the shared bottleneck does to the offer floor, so it is left to its own row.
interactive-scaling@flows=4+offer=concurrent+impairment=clean-or-GE1-or-hostile = the concurrent row is declared on the family's GE5 seed pair (31/32) only; the clean, GE1 and hostile rtt100 rows are single-flow arms, and a concurrent arm on those links would be stated against a different baseline family.
interactive-scaling@flows=8+offer=concurrent = the sink attributes samples by first-byte tag (A, C..H after the reserved `b'B'`), so seven flows is the tag range's limit and an eight-flow row has no per-flow attribution; the four-flow rung is the largest this instrument can measure.
attribution@baseline-family=interactive = the family is now declared (its four cells renamed `interactive-cadence@…` off `M1`/`M2`), but two of its rows are composites: `jitter_interactive_with_bulk` varies two declared dimensions from the solo reference (rate: none -> 1MiBps; load: none -> bulk-burst-2MiB-per-3s) and `jitter_interactive_bulk_and_loss` three (loss, rate, load), so no existing arm attributes them; an arm that adds the bulk burst on an uncapped link (load alone), or caps an idle link (rate alone), is what closes this.
attribution@baseline-family=m3-bulk = the family is now declared (its three cells renamed `m3-bulk@…` off `M3`), but `jitter_bulk_idle_restart_arm` varies four declared dimensions from the smoke reference (rate: 1MiBps -> 2Mbps-c2s; load: saturated -> idle-restart-bursts-512KiB; window: 6s -> 34s; metric: capacity-fraction -> delivered-goodput), so no existing arm attributes it; a one-axis idle-restart arm on the family's own 1 MiB/s saturated link is what closes this.
attribution@baseline-family=lone-tail = the family is declared (its cells renamed `lone-tail@…` off `M1`) with the request/response pair. `mandate_smoke::m1_lone_tail_field_rtt` still varies two declared dimensions from the family's **reference** (depth: 1-and-2 -> 1; impairment: owd25-iid2pct-iid6pct-ge5pct-jitter5-100-200ms -> owd100-ge5pct-jitter100ms), so it keeps the `composite` label that says its two dimensions move together *against that reference*. What it no longer is, is unattributable: `mandate_smoke::m1_lone_tail_field_rtt_depth_sweep` is declared beside it at `depth=1-and-2` on the same field impairment, so it is exactly one declared dimension (`depth`) from the field row, and that is the repair this line asked for — a field-scale arm that sweeps depth. The sweep is measured, not asserted from a document: 37.194 s for both depths, its own libtest stamp, declared 38 s, and each depth carries the field row's own two guards (depth 1: 183 samples, delivery 1.000, p99 425.9 ms, 6.011 % over 250 ms; depth 2: 222 samples, delivery 1.000, p99 395.5 ms, 6.757 %), both inside the 1500 ms p99 guard and the 15 % over-250 share. Its vacuity demonstration is the same input fault the field row uses, `MANDATE_SMOKE_FAULT=M1_FIELD_RTT_slow`, which drives both guards past their bounds (depth 1's p99 reads 2198.6 ms and 100 % of samples over 250 ms) — so the sweep is a gate, not a printed table. The two rows the pending declaration filed here, `jitter_cellular_timeline_arms` and `jitter_nonloss_impairments`, are **not** declared into this family and are declared instead under the cadence-shaped `non-loss-impairment` family below, because renaming their cells to `lone-tail@…` would state a request/response coverage neither arm has.
attribution@baseline-family=decomposition = `jitter_frame_reorder_decomposition` carries `frame-reorder` and `jitter_decomposition` carries `loss-vs-queue`, so the family spans two names; the repair is to rename `jitter_decomposition`'s cell `frame-reorder@…` or to split the family, and because `jitter_decomposition` is two declared dimensions (arms, jitter) from its sibling, a single-axis decomposition arm beside it is the other half of the repair.
attribution@baseline-family=dual-lane = the family is now declared: all six of its rows carry `hol-dual-lane*` cells, the rename moved `jitter_duallane_arms`'s cell into that namespace with no change to the arm, and the five `hol_probe` dual-lane topology rows are declared on their own libtest stamps (20/20/20/20/17 s). Four of the six vary several declared dimensions from the reference `hol_probe::dual_lane_asym_frame_delivers_and_tears_down`, so no existing arm attributes them: `hol_rtt100_ge5_shared_dual_lane_frame_delivery` varies bulk, metric; `hol_rtt100_ge5_dual_lane_two_interactive_frame_diag` flows, metric; `hol_rtt100_ge5_dual_lane_two_interactive_stock_diag` flows, interactive, metric; and `jitter_duallane_arms` impairment, metric, reorder. A single-axis dual-lane arm beside the reference (the second interactive stream alone, or the interactive lane's frame mode alone, or the sixth row's matched bulk load on the reference's own link) is what closes this. The one-dimensional member is `hol_rtt100_ge5_shared_dual_lane_asym_frame_diag`, which is the `hol-dual-lane` arm already carrying the reference's whole config: the two differ only in the metric each asserts (liveness versus delivery-and-teardown), which is why it is the family's orthogonal member rather than a fifth gap.
attribution@baseline-family=fairness = the proposed family spans five cell names (`M4`, `fairness-sweep`, `fairness-longrun`, `dual-lane-longrun`, `multi-flow-longrun`) across two tiers; the repair is to split it into the three contexts it measures (`fairness-sweep`, `fairness-longrun`, `dual-lane-longrun`) and to rename the M4 arm's cell `fairness-m4@…`, since the M4 arm is a four-flow fairness arm and the longruns are multi-minute measurements, not the same reference's members.
attribution@baseline-family=hol-regimes = the eight regimes the pending declaration proposed are now eight declared families with their own cell names (`hol-cap400`, `hol-rtt100-ge5`, `hol-rtt40-ge1`, `hol-rtt100-clean`, `hol-hostile`, `hol-frame`, `hol-paced`, `hol-fec`), and 36 of the target's 43 ignored rows are declared against them with measured costs, so the collision that blocked them is closed. The seven rows that stayed undeclared when this family split were the five dual-lane topology rows and the target's two default-tier FEC instrument arms: the five are now the `dual-lane` family declared above and the two are now the `fec-instrument-sanity` family, each with its own measured cost. The `dual-lane` family's own attribution residue is on that line.
attribution@baseline-family=hostile-probes = the family is now declared, and the name collision that kept it from being so is closed: the two `probe_hostile_*` rows carry `hostile-probe@…` rather than the `ceiling` name the `rtp` echo pair also carries, and both are declared on their own measured stamps (51 s and 40 s). The residue is attribution: `perf_probe::probe_hostile_message_latency` varies three declared dimensions from the family's reference `perf_probe::probe_hostile_goodput_30s` (impairment, metric, shape) — the reference measures goodput on the `hostile-real-link` preset through a counting sink, the member measures message latency through the `hostile-periodic-bottleneck-300ms` preset — so no existing arm attributes it. A message-latency arm on the reference's own preset, or a goodput arm on the member's, is what closes this. The two rows were deliberately not given one abstract "hostile-preset" impairment value: they run on two different presets and the cell names the one each takes.
attribution@baseline-family=rtp-ceiling = the cell-name collision is closed — the hostile pair now carries `hostile-probe@…`, so `ceiling` is claimed by one namespace only and this family's two rows (`perf_probe::probe_rtp_echo_4mib_direct` and `_mss8k`, one declared dimension apart on `mss`) are separable. The residue is cost, not a name: no `#[ignore]` reason states either wall-clock and no document records one, so the family cannot be declared until both are measured, and `members.rtp-ceiling = ceiling` would otherwise claim a name this family is the only claimant of without its reference having a recorded cost.
attribution@baseline-family=mux-ceiling = `mux_ceiling_probe`'s echo and sink pairs all carry `loopback-ceiling`, so the proposed echo and sink families cannot both claim it; the repair is to split the name (`mux-ceiling-echo@…`, `mux-ceiling-sink@…`) and to record the four costs.
attribution@baseline-family=instrument-sanity = the name collision is closed: the `hol_probe` FEC pair's cell is now `fec-instrument-sanity@…` and it is declared as its own family, `instrument-sanity` names only `perf_probe`'s determinism pair, and all four rows are declared on their measured 0.000 s stamps. The residue is attribution rather than a name: `perf_probe::deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss` varies two declared dimensions (lane, metric) from its family's reference `perf_probe::controller_fat_pipe_has_only_fixed_shaping`, so no existing arm attributes it; a lane-only arm on either preset (or a metric-only arm beside one of them) is what closes this. The `fec-instrument-sanity` family carries no such residue: its two rows differ in the metric alone, which is the instrument property each asserts.
attribution@baseline-family=mux-over-rtp = the family is now declared for the two rows that carry its name: `mux_over_rtp_perf::mux_over_rtp_lossy_perf_smoke` is the reference (a 1 KiB echo on the `lossy-400kib-per-sec` preset, asserted intact plus `forwarded > 0` and `rate_limited > 0`) and `mux_over_rtp_400mib_hostile_perf` is refiled into it, at four declared dimensions (impairment, metric, scale, shape) from that reference. The third row the earlier line named, `mux_over_rtp_small_stream_while_bulk_perf`, still carries `small-stream-while-bulk`, which `members.mux-over-rtp = mux-over-rtp*` does not claim, and it has no recorded wall-clock either: so it stays undeclared, and the repair the line named — rename its cell so all three carry one name and record the third cost — is only half taken. Two of the three wall-clocks are measured now (1 s and 344 s, both on their own stamps); the ordering arm's is not.
attribution@baseline-family=hol-verify4 = `hol_verify4::v4_clean_muxbulk` and `v4_ge5_muxbulk` are internally coherent (`bulk-lane-ab`) and one dimension apart, so only their costs are missing — the `#[ignore]` reason states no wall-clock; one measurement per row declares the family with no cell change.
attribution@baseline-family=fec-recovery = `hol_probe::hol_rtp_mux_fec_default_on_recovery` is now declared, in the `hol-fec` family and five declared dimensions from that family's reference (bulk, fec, impairment, layer, metric), so it is a composite no existing arm attributes: its context is the `rtp-mux` layer on the fec-gaming fat pipe at 20 % loss with a controller-retention bulk lane, while the reference is the cap400 shaper with FEC on. A single-axis FEC-recovery arm beside either end is what closes this. Its cost is now measured (38 s, its own libtest stamp) — and the family is a two-row family rather than a one-row namespace, which the checker refuses, because a baseline no sibling row states against is a stale reference.
cost@metric=wall-clock = `mandate_smoke`'s three other arms (M1's, M2's and M4's; M3's is measured and declared above), the four `mux_ceiling_probe` rows, `perf_probe`'s remaining two rows (`probe_rtp_echo_4mib_direct` and `_mss8k`; the hostile pair and the instrument-sanity pair are measured and declared above), `contested_latency`'s one remaining row (`contested_capped_jitter_loss`; its clean reference and the refiled hostile arm are declared), `mux_over_rtp_perf`'s one remaining row (`mux_over_rtp_small_stream_while_bulk_perf`; the lossy smoke and the 400 MiB hostile arm are declared) and `hol_verify4`'s two rows have a coherent family or one repairable cell but no wall-clock in any document; each needs one measurement of its own tier's invocation — the tier's `--ignored` run, or a plain `cargo test --release -p rtp_mux --test <target> -- --exact <test>` for a default-tier row — before its family can be declared, because a cost the declaration invents is worse than a cost it records as pending. The 43 `hol_probe` rows this line used to name are measured now: 36 are declared above on their own stamps, and the other seven — the two default-tier FEC instrument arms (0.000 s each) and the five dual-lane rows (16.9-19.6 s each) — carry their measured costs on the `dual-lane` line above and in the cost-provenance paragraph, so those rows are unblocked except for their family.
M1@lane=single = the constitution's M1 arms run on the production dual-lane topology; a single-connection transport tail is covered by rtp's burst-loss and bufferbloat gates, whose perf declarations are pending, and is not claimed here.
M3@lane=single = M3 is asserted on the deployment's bulk lane (`dual_lane_mandates`); the single-connection goodput floor belongs to rtp, whose declaration is pending.
cpu-cost@metric=cpu = per-datagram CPU cost is measured by owning-symbol attribution (`tools/samply_hotspots.py`), not by a scenario in this crate.
large-scale@scale=over400MiB = the hostile bulk arm caps at 400 MiB; steady-state transfer beyond that is not claimed by any row here.
soak@scale=multi-hour = `rtp_longrun` is multi-minute; a multi-hour soak fits no tier's budget, so it exists as no test here.
multipath@impairment=multipath = the multi-path UDP transport (rtp's `mpudp`) has no rtp_mux arm; a cell for it belongs to the layer that owns that transport.
rate-asymmetry@impairment=rate-asymmetry = only the asymmetric frame-delivery diag arm varies lane asymmetry; an asymmetric *rate* with symmetric latency is not covered.
loaded-lone-tail@shape=request-response+load=bulk = the lone-tail arms run with the bulk lane idle; the request/response shape under a loaded bulk lane is not covered.
cellular-request-response@lane=cellular-timeline+shape=request-response = the cellular timeline arms use the cadence shape only.
policer@impairment=policer = a token-bucket policer (as opposed to the shaper and queue the harness models) is not in the impairment instrument, so no arm can cover it.
```

The concurrent row's vacuity demonstrations are `HOL_PROBE_FAULT=serialize`
(no flow but the first may offer until the first flow's window has closed: the
overlap assertion fails at −0.000 s while delivery, p50 and offer counts stay
green) and `HOL_PROBE_FAULT=throttle` (every other flow offered at an eighth
of the cadence with its window still spanning the run: the offer floor fails at
83 of 660 while the overlap, delivery and p50 signals stay green). Both are
offering-input faults on the shared path, so the baseline row is run under the
same selector and keeps its numbers — its flows are already disjoint and it
asserts nothing about an offer schedule.

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
`clean`; M2 asserts the mandate-2 bound (`delivery == 1.000`,
`offered <= own-wire <= 6x`) on `clean`; M3 asserts the mandate-3 floor (`>= 0.35x` of the configured link
rate) as the within-run delivered/shaper-forwarded fraction, median of three.
Those bounds and their derivations are the ones stated above; the smoke set
does not restate them.

The smoke set's three arms all run the deployment's **25 ms one-way** profile
(~50 ms round trip), which is not the RTT the deployed client sees: the field
reports a ~190 ms *minimum* round trip. `mandate_smoke::m1_lone_tail_field_rtt`
(`full` tier, `#[ignore]`d) is the same request/response shape moved onto the
field's scale — GE `gilbert_elliott_loss(5, 8)` and ~100 ms jitter at a
`100 ms` one-way delay, one unacked 256 B message at a time, no bulk lane — and
asserts the two regression guards in the table above. It is a **new arm**: the
`clean`, `hostile` and `lone_tail` arms keep their impairment, windows,
cadence and guards untouched, and the arm does not enter `M1`'s panel or
assertion. Its vacuity demonstration is the input fault
`MANDATE_SMOKE_FAULT=M1_FIELD_RTT_slow` (+1000 ms one-way on both directions),
which drives both guards past their bounds from the measurement path.

The arm exists because the tail is **not** RTT-invariant. The lone tail's
delivery is a repair ladder — a fresh single-symbol message carries a cover of
copies, and when that cover is consumed each further rung waits a repair
deadline — and the deadlines that drive it are constants rather than
RTT-derived values: the tail-loss probe window is floored at the `300 ms`
`TAIL_PROBED_MIN_RTO` (`rtp/src/traffic_shaping/recovery/tlp.rs`) while the
subsequent *retransmission* deadline is floored at the `1 s` `MIN_RTO`
(`rtp/src/traffic_shaping/recovery/rto.rs`). On a ~50 ms path the second floor
is ~20x the path RTT, so the ladder's step is a whole second per rung and the
measured rungs sit at 1001 ms; on a ~190 ms path the RTT-derived term and the
`300 ms` floor bind instead, and the same impairment produces a shorter
ladder. Measuring only at 50 ms RTT therefore hides the field scale, and this
arm is the tripwire that keeps it in the battery. The revision-to-revision
delta in the table is what the landed transport bought at the field's RTT;
the `1 s` floor is what still dominates at 50 ms.

`mandate_smoke::m1_lone_tail_field_rtt_depth_sweep` is the field arm's `depth`
dimension measured rather than inferred: `full` tier and `#[ignore]`d like the
arm it sits beside, the same field impairment, shape, window and both guards,
offered at `depth` 1 and then at `depth` 2. It exists for attribution. The
lone-tail family's reference is the 25 ms sweep at `depth=1-and-2`, so the
field row differs from it in *two* dimensions at once and a tail it reports
cannot be assigned to either; the sweep is one declared dimension from that
reference (its impairment) and one from the field row (its depth), so it
separates them. It is a **new arm**: nothing about the field row, the `clean`,
`hostile` or `lone_tail` arms changed, and it enters no panel. Its vacuity
demonstration is the field row's own input fault,
`MANDATE_SMOKE_FAULT=M1_FIELD_RTT_slow`, which fails both depths' guards.

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
| M1 field-RTT lone-tail p99 | `rtp v0.0.94`: 427–719 ms; landed `rtp` `bdacf5c0`: 293–432 ms | `1500 ms` (~2.1× the pinned band) |
| M1 field-RTT lone-tail `> 250 ms` share | `v0.0.94` 3.3–5.6 %; landed 2.6–5.3 % | `15 %` (~2.7×) |
| M1 field-RTT lone-tail max | `v0.0.94`: 1015–3868 ms; landed: 386–529 ms | not asserted (one sample at n≈200) |
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
cold_connection::cold_connection_decomposition = standard
cold_connection::mux_lane_birth_is_one_round_trip = standard
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
hol_probe::hol_rtt100_ge5_four_interactive_concurrent_frame_delivery = full
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
mandate_smoke::m1_lone_tail_field_rtt = full
mandate_smoke::m1_lone_tail_field_rtt_depth_sweep = full
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
cold_connection::cold_connection_decomposition
cold_connection::mux_lane_birth_is_one_round_trip
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
hol_probe::hol_rtt100_ge5_four_interactive_concurrent_frame_delivery
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
mandate_smoke::m1_lone_tail_field_rtt
mandate_smoke::m1_lone_tail_field_rtt_depth_sweep
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