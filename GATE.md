# The rtp_mux validation gate

This file is the authoritative scope of the rtp_mux scenario gate. `cargo test
-p rtp_mux` silently skips every `#[ignore]`d scenario, so the gate is defined
in tiers and the `gate-manifest` block below names every opt-in scenario and
its tier. The manifest is machine-checked by the shared checker
(`netem-tools check-gate`, parameterized per crate), which fails if a
scenario is added or removed without the manifest being updated, making an
unnoticed `#[ignore]` skip impossible.

Run the checker after adding, removing, or re-tiering any scenario. The
checker is `netem-tools check-gate` (run from the crate checkout; it resolves
the sibling kit trees from the shared crates root):

```sh
netem-tools check-gate \
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
2. **The interactive lane's latency does not degrade under a known offered
   throughput** — the lane is offered a **known rate** and the mandate is that
   its latency stays at the link's floor. The **throughput is the input** and
   the **goodput is inferred** from the latency holding: a lane draining what
   it is offered cannot be accumulating a queue, and a lane whose goodput fell
   would have to show the backlog as latency (or stop offering). The `both`
   arm's offer is the deterministic `MSG_BYTES` per `CADENCE` schedule (256 B
   every 25 ms = 10240 B/s, 40 msg/s). Bound (derived): the arm's measured
   `sent` must be that schedule (the input — a lane never offered the rate has
   no goodput to infer), its `delivery` must be exactly `1.000`, and its `p99`
   must stay within `INTERACTIVE_NONDEGRADING_P99_MS = 100 ms` — the link's
   `OWD + JITTER` = 30 ms one-way floor plus the repair margin, ~4× the
   measured ~26 ms p99 and well below M1's 250 ms ceiling, so it bites on a
   backlog under the offer rather than on the 2 % loss realisation. Asserted by
   `rtp_mux_jitter::jitter_duallane_constitution_gate` (**default tier** — an
   offer count, a delivery count and a floor-relative latency bound belong in
   the gate that always runs); it runs on every `cargo test -p rtp_mux`. The
   offered payload's byte-exact integrity is additionally asserted always-run
   at the mux layer beneath (mux's default tier, `mux/GATE.md`) and in
   rtp_mux's own default tests (`tests/bidirectional.rs`).
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

The rows below declare twenty-six families in the `gate-perf-design` grammar:
`<row> = <tier> | <cost_s> | <relation> | <cell>[,<cell>…]`, each cell
`<property>@<dimension>=<value>[+…]`. The **default** family is the residual —
every cell name no `members.<family>` claims — and holds the pre-existing
`interactive-scaling` rows plus the two-flow rung declared alongside them; the
twenty-five named families (`constitution`, `contested`, `dual-lane`,
`establishment`, `fec`, `fec-instrument-sanity`, `frame-reorder`, `hol-cap400`,
`hol-fec`, `hol-frame`, `hol-hostile`, `hol-paced`, `hol-rtt100-clean`,
`hol-rtt100-ge5`, `hol-rtt40-ge1`, `hostile-probes`, `instrument-sanity`,
`interactive`, `latency-sweep`, `lone-tail`, `m3-bulk`, `mux-over-rtp`,
`non-loss-impairment`, `reorder`, `spike-survival`) each carry
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
judgement call: of the 82 declared rows, 26 are a family's own reference,
29 vary exactly one dimension from it (`orthogonal`) and 27 vary several
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
rtp_mux_jitter::jitter_duallane_constitution_gate = default | 40 | baseline@constitution | M2@lane=dual+shape=cadence+arm-set=clean-and-hostile+metric=offered-load-latency
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
mandate_smoke::m1_lone_tail_rung_distribution = full | 75 | composite(depth,impairment,metric)@lone-tail | lone-tail@lane=dual+shape=request-response+depth=1+impairment=owd25-ge5pct-jitter100ms+metric=rung-count-vs-burst-law
mandate_smoke::m1_lone_tail_loss_model = full | 150 | composite(depth,impairment,metric)@lone-tail | lone-tail@lane=dual+shape=request-response+depth=1+impairment=owd25-iid5pct-vs-ge5pct-mean8-jitter100ms+metric=rung-count-vs-loss-model
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
spike_survival::a_floor_link_keeps_the_session_and_its_stream_usable = standard | 2 | baseline@spike-survival | spike-survival@spike=none+lanes=dual+impairment=owd95-floor+metric=delay-and-session-identity
spike_survival::a_field_magnitude_latency_spike_is_survived_without_a_reconnect = standard | 9 | orthogonal@spike-survival | spike-survival@spike=field-3205ms-round-trip+lanes=dual+impairment=owd95-floor+metric=delay-and-session-identity
```

Declared sums are `default` 149 s, `standard` 41 s, `full` 1871 s and `perf`
3471 s of the 300 s, 600 s, 1800 s and 3500 s budgets. `full` and `perf` are
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
1700 s ceiling already admits, and the lone-tail rung-distribution probe adds
75 s (1646 + 75 = 1721 s), which does not — so `full` is raised again as a
declared change (1700 → 1800) for a row whose cost is *measured* rather than
read (74.40 s real, `cargo test --release -p rtp_mux --test mandate_smoke
-- --ignored --exact m1_lone_tail_rung_distribution --nocapture
--test-threads=1`), and the 1800 s ceiling admits the sum with 79 s of room for
the tier's still-undeclared rows. The lone-tail loss-model probe adds 150 s
(1721 + 150 = 1871 s), which that 79 s of room does not admit, so `full` is
raised again as a declared change (1800 → 1900) for a row whose cost is
*measured* rather than read: 149.32 s of libtest's own time for eight ~19 s
windows (`RUSTC_BOOTSTRAP=1 cargo test --release -p rtp_mux --test mandate_smoke
-- --ignored --exact m1_lone_tail_loss_model --nocapture --test-threads=1
-Z unstable-options --report-time`, real 152.99 s), declared 150 s, and the
1900 s ceiling admits the sum with 29 s of room. On the runner's
own scale the `default` sum is 149 s (the M3 smoke row's 108 s plus the
constitution gate's 40 s and the lossy smoke's 1 s), inside its
300 s ceiling: no raise. The `standard`
ceiling keeps its 600 s and now carries four rows: the two cold-connection rows
(28 s + 13 s) and the two `spike-survival` rows (2 s + 9 s, measured 1.58/8.77 s),
52 s of 600. The concurrent row is one
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
The lone-tail rung-distribution probe is the third measured rather than read:
74.40 s of real time for its four windows (its libtest stamp reads 74.30 s),
declared 75 s.
Two are **measured** for this declaration rather than read:
`mandate_smoke::m3_bulk_goodput_fraction` and
`jitter_shared_bottleneck_arms`. `jitter_shared_bottleneck_arms` ran in
156.10 s (for `cargo test --release -p rtp_mux --test rtp_mux_jitter --
--ignored --exact jitter_shared_bottleneck_arms --nocapture
--test-threads=1`, `real 156.33 s`) and is declared 157 s.
`mandate_smoke::m3_bulk_goodput_fraction` is stated on the scale the
checker's **measured side** uses, which is not the same scale as a serialized
single-test invocation. `netem-tools check-gate --mandate-check-json` compares
a declared cost with `tools/mandate-check`'s stamp, and that runner invokes the
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

**The `spike-survival` family** is the row the `establishment` family's
coverage gap asked for. That family measures a session being **born**
(`cold_connection_decomposition`, `mux_lane_birth_is_one_round_trip`) and only
on clean links; `rtp_mux`'s deployed client multiplexes everything over **one
long-lived mux session**, and the field's own round trips reach 3205 ms (1063 ms
on another run) on a ~190 ms floor — **spikes on a live path**, not a dead path.
The two rows measure a live dual-lane session carrying a request/response loop
on the field's 190 ms floor, with the interactive lane's pair swapped mid-round:
the pinned harness has no runtime latency setter (`NetemConfig.latency` is fixed
at spawn and `NetemLink` exposes only `set_blackout`), so the pair is stopped
and respawned on the **same** client-side and server-side bind addresses with a
latency-only config — no address the app or the server knows moves, and the
swap itself is measured at ~5.6 ms. The baseline runs uncut; the member varies
the one `spike` dimension to the field's largest round trip (3205 ms, ~17x the
floor). They pin the **positive** property: every round completes — no error, no
EOF, no lost stream, the write in flight at the onset included — the steady
spike round observes the injected delay (3396 ms for the 3204 ms injection), the
stream returns to the floor (191 ms) when the path does, and the **session
identity never moves**: the sampler reads `RtpMuxConnector::probe_session` every
25 ms across the spike and sees one id, never absent (276 samples). The timer
inventory the row rests on is stated, with citations, in the target's module doc
and summarised here: mux's sliding receive deadline is `heartbeat_interval * 4`
= 20 s (`mux/src/central_io/reader.rs:25,117`), 5.6x above the spike; rtp's
`MIN_RTO` (1 s) and `TAIL_PROBED_MIN_RTO` (300 ms) are *retransmission cadences*
with no max-retry or give-up path in the reliable layer; and the proxy's pool
heartbeat is a 30 s write timeout
(`proxy/common/src/stream_runtime/pool.rs:19`). The one exception is a *birth*
deadline, and there are now two of them, both derived from the same field
measurement: `rtp`'s opening handshake is `OPENING_LEG_TIMEOUT = 4 s`
(`rtp/src/traffic_shaping/control/handshake/opening/mod.rs`, the smallest whole
second above the field's worst 3205 ms sample), and `rtp_mux`'s own
`BIRTH_LIVENESS_DEADLINE` (`src/shared.rs`), the mux-level backstop over the
whole dual-lane birth and the lanes' first receive. The second was **2.5 s
until this revision** — below the field's worst measured round trip — and the
record of that defect is the subsection below. A *live* session under a spike
is this family's arm; a **birth** under one is `birth_liveness`'s.

**The birth's own liveness deadline: `birth_liveness`.**
`BIRTH_LIVENESS_DEADLINE` is the one field-reachable timer in this crate whose
expiry is **pure elapsed time** rather than evidence of a dead path, and it was
recorded in no document before this revision. It is armed twice per dual-lane
birth (`src/connector/dial.rs`): as the mux lanes' *first receive* deadline (a
sliding deadline from the last byte that arrived,
`mux/src/central_io/reader.rs`) and as the race over the whole birth. Expiry
**aborts and reaps the birth's supervisor** — both rtp lane sessions and both
mux tasks — and returns `Err`; `retry_dual_connect` then starts a **fresh cold
birth** (`connect_dual_lane_once` opens two new rtp sessions with a new nonce),
so a birth killed for slowness re-pays the cold establishment and produces the
reconnect `AGENTS.md` calls worse than the spike. What bounds the cost is
`MAX_DUAL_CONNECT_ATTEMPTS = 3`: a stall longer than
`3 * (deadline + grace)` fails the dial outright.

The value is a **measurement**: the operator's path measures a 190 ms minimum
round trip with maxima of 1063 ms and **3205 ms**, and at the old **2500 ms** the
deadline sat below the worst of those — a birth on a spike was killed for
slowness. **4 s is the smallest whole second above 3205 ms** (a 25 % margin),
the same derivation `rtp`'s opening leg budget uses, so the mux-level backstop
can never fire before the transport's own opening budget has had its chance.
Measured by `birth_liveness::a_birth_is_not_killed_by_a_spike_scale_gap_but_still_times_out_beyond_its_budget`
(`full` tier, 17.4 s, its own libtest stamp; run with `--ignored --nocapture
--test-threads=1`), on the **birth** with the rtp opening handshake off so this
is the only birth timer in force:

| arm | round trip | deadline | birth wall | stream | verdict |
| --- | --- | --- | --- | --- | --- |
| `clean` | 50 ms | 4 s | 51.5 ms | round-trips | birth completes |
| `spike_scale` | 2600 ms (above the old 2500 ms, below the field's 3205 ms) | 4 s | 2603.0 ms | round-trips | birth completes |
| `beyond_budget` | 5000 ms | 4 s | 12 086.0 ms | — | fails, inside the 3 x (4 s + 250 ms) retry budget |

The `beyond_budget` arm is the vacuity: it proves the instrument can see an
expiry, so `spike_scale`'s pass is a measurement rather than an arm that cannot
fail, and it pins the retry cost. The mutation proof that `spike_scale` depends
on the constant: with `BIRTH_LIVENESS_DEADLINE` back at **2500 ms** the arm
fails — *"the spike_scale arm's birth did not complete (round trip 2600 ms,
wall 7588.8 ms): Some(BrokenPipe)"* — which is the field defect reproduced on a
bench (the failed attempt's retry is visible in the 7.6 s wall clock). The vacuity injections are
`SPIKE_SURVIVAL_FAULT=no_spike` (the injection is skipped, so the
delay-matches-injection check fails on a 191 ms round), `=churn_session`
(`RtpMuxConnector::reset()` mid-spike: the in-flight rounds error
`BrokenPipe`/`WriteFailed` and the survival check fails) and `=late_churn` (the
reset lands after every round has passed, so only the identity guard can catch
it — it does, on an absent id).

```gate-budgets
default = 300
standard = 600
full = 1900
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
members.spike-survival = spike-survival*
baseline.spike-survival = spike_survival::a_floor_link_keeps_the_session_and_its_stream_usable
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
cold-connection@lanes=dual+handshake=on+impairment=loss-or-jitter-or-reorder = the cold birth is measured on clean links only; what loss does to a birth is rtp's opening handshake's own retry behaviour (its `OPENING_TIMEOUT`/`RETRY_INTERVAL`), which the `rtp` crate owns and which a clean-link decomposition cannot attribute to rtp_mux, so no impaired birth row is claimed here. A *live* session under a delay spike is now claimed by the `spike-survival` family, but a *birth* under impairment (rtp's 3 s `OPENING_TIMEOUT` against a 3.2 s spike round trip) is not exercised by any row here and belongs to `rtp`.
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
m1-four-flow-clean@flows=4+impairment=clean+metric=p99-ceiling = **closed** by `mandate_smoke::m4_clean_lane_p99_ceiling` (default tier, 15.5 s, declared with its bound, its five coverage cells and its two vacuity probes in the M4 section above). What the line used to record was that M1's ceiling was asserted on M1's *one-flow* clean arm while the four-flow clean panel drew the same ceiling over four series without asserting it, and that a four-flow clean breach was therefore reported and drawn but asserted by no arm; and that it was reachable — raising `rtp`'s fresh-tail armour cover (4/5 to 8/9) left M1's one-flow clean arm untouched (`p99` 90.8 ms) while moving M4's four-flow clean arm to `clean_p50_max` 99.3-118.5 ms and `clean_p99_max` 253.4-265.0 ms, the last **above** the drawn ceiling. The new arm asserts the four-flow clean `clean_p99_max` against that same ceiling (`M1_CEILING_MS`, one authority), so the breach the cover sweep produced now fails a gate instead of only a panel. The gap's own text said what would close it — "a new arm asserting the four-flow clean p99 against the ceiling - declared beside M4, never by retuning it"; that is what this row records as done. The lever that would move the number rather than bound it is superseded by a **one-parameter change to `rtp`** — `INIT_SEND_RATE` 128 -> 1024, measured on this workspace's `rtp` revision (one-flow clean `p99` 90.8 -> 26.8 ms, four-flow clean p99 180.8 -> 176.1 ms). **That revision is not released and this crate's pin is unchanged** (`rtp` `v0.0.97`, whose arms still measure `clean_p99` 88.1 ms): the numbers above describe the local transport under test, and shipping them is the tag train's job — tag `rtp`, bump this crate's `rtp` pin, re-run this battery, tag this crate. The cover half of the pair was **swept and refused**: the two settings the sweep tried each break a safeguard (the ladder's monotone-non-increasing copy count at `m = 2`, the hostile arm's window-adequacy gate at `m = 4`). The frontier and both refusals are recorded in the M4-level section above and in `rtp/GATE.md`.
m1-four-flow-hostile@flows=4+impairment=hostile+metric=p99-ceiling = **closed** by `mandate_smoke::m4_hostile_lane_p99_ceiling` (default tier, 15.5 s, declared with its bound, its derivation, its three coverage cells and its two vacuity probes in the section `The four-flow hostile level` above). What the line records is that the production flow count's **hostile** tail had no level arm: M1's 250 ms ceiling is asserted on M1's one-flow clean arm, M4's `hostile` arm guards each flow against M1's loose 900 ms per-flow regression guard, and M4 reports the aggregate `hostile_p99_max` without asserting it — so the four `hostile_p99` bars of a passing battery sat above the 250 ms ceiling drawn on `M4-latency` while no gate named their level. The new arm asserts the four-flow hostile `hostile_p99_max` against `M4_HOSTILE_P99_CEILING_MS` (452 ms), derived as `mean + 4 sd` over the twenty healthy observations on record rather than picked or mirrored above the ceiling: the four-flow hostile tail measures `274.9-423.0 ms`, `1.10-1.69x` the M1 ceiling, so the bound **exceeds** the 250 ms ceiling by `1.81x` and the arm says so — the honest closure here is a bound where the measurement supports it plus the named hole, not a 250 ms assertion that is false on every run. The bound is `1.99x` tighter than M4's own guard, and the red proof shows the arm bites in the region between them: `MANDATE_SMOKE_FAULT=M4_HOSTILE_LEVEL_double` reads per-flow p99 `731.2/848.2/883.8/785.4` ms — every flow **inside** the 900 ms guard M4 asserts and every flow **outside** this arm's 452 ms ceiling — with delivery still `1.000` (a later invocation of the same probe read up to 1314.6 ms, past both bounds). The residual panel gap (the bound is not drawn as a line on `M4-latency`) has its own line above.
cellular-request-response@lane=cellular-timeline+shape=request-response = the cellular timeline arms use the cadence shape only.
policer@impairment=policer = a token-bucket policer (as opposed to the shaper and queue the harness models) is not in the impairment instrument, so no arm can cover it.
m1-four-flow-hostile-level@flows=4+impairment=hostile+metric=p99-ceiling-panel = **named, not drawn.** The four-flow hostile level bound (`M4_HOSTILE_P99_CEILING_MS`, 452 ms, asserted by `mandate_smoke::m4_hostile_lane_p99_ceiling`) is on the same `hostile_p99` series `M4-latency` draws, but it is not drawn as a line: this tool's `series_guard_bounds` draws a per-series guard line only under a **single** declared bound, and a second declared bound on that panel suppressed the run's own `hostile_p99_guard=900` line and made the render refuse itself (the trial bound was 455 ms; the tool reported `mandate_plot: error: panel 'latency': its label names the guard 900, but the artifact draws 2 bound line(s) at [249.9, 455.1] and none of them is that guard`), measured by rendering the panel with the bound added. So the level bound is carried by the arm's own `[m4-hostile-level]` line and the section `The four-flow hostile level`, while `M4-latency` keeps drawing the declaration's 250 ms ceiling and the run's own `900 ms` guard labelled `governs series hostile_p99` and attributes the crossing to that guard rather than implying a breach. The gap is the **panel reading**, not the assertion: relaxing the `series_guard_bounds` single-bound precondition lives in `netem-tools mandate-plot`, outside this crate. A reader who needs the level gate has it in the verdict line and in this file; a reader of the panel alone sees `900 ms`, which is M4's own arm guard and not the level.
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
cd crates/netem_test && tools/mandate-check --producer-path rtp_mux=../rtp_mux
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
`clean`; M2 asserts the mandate-2 relation on `clean` — the lane was **offered**
its known throughput (`MSG_BYTES / CADENCE`), it **delivered** it
(`delivery == 1.000`), and its **latency did not degrade under the offer**
(`p99 <= 100 ms`); M3 asserts the mandate-3 floor (`>= 0.35x` of the configured link
rate) as the within-run delivered/shaper-forwarded fraction, median of three.
Those bounds and their derivations are the ones stated above; the smoke set
does not restate them.

The two cadence arms' offer is written by `mandate_smoke::offer_cadence_on_deadline`
rather than by the pinned `rtp::testkit::rtp::send_timestamped_messages`, and that
is a repair rather than a preference: the pinned sender drives its cadence
through `tokio::time::interval` with `MissedTickBehavior::Delay`, which **drops** a
tick whenever the runtime wakes the task more than one cadence late, so the count
M2's offer floor reads was measuring the test host's scheduler rather than the
lane. Measured at load average 22-39 the arms offered 2270-2347 (clean) and
2281-2310 (hostile) of the 2400 messages their `WINDOW / CADENCE` schedule
requires, and the transport accepted **every** write attempted — attempts equal
accepts, zero write errors, 6-13 ms of `write_all` await across a 12 s window —
so the lane was not refusing the load: lateness at the window's quarter points
(139/268/324/435 ms and 138/232/358/500 ms) accumulated from the start instead of
stalling once, which is a wake-count shortfall and not a start-up or an
end-of-window stall. The deadline sender owes its schedule
`floor(run_for / CADENCE)` messages and writes the deadlines a late wake has
already passed back-to-back, so the count is host-independent and a shortfall now
means **the transport refused the offer** — the only thing the floor was ever for.
`M2_OFFER_TOLERANCE`'s 2 % is unchanged and now absorbs refused writes rather
than scheduler jitter; the floor, the arms' windows, cadence and tiers are
untouched. Its vacuity demonstration is the input fault
`MANDATE_SMOKE_FAULT=M2_offer` (the clean arm offers a tenth of the cadence):
`clean_offer_msgs=240` against the 2352 floor with `clean_delivery=1.000` and
`clean_p99=36.3` inside its 100 ms bound, so the offer is the failing clause and
nothing else. Do **not** put the cadence arms back on the wake-count sender to
"restore the original instrument": the wake count was the defect.

The same target carries a fourth asserting arm beside M4 —
`mandate_smoke::m4_clean_lane_p99_ceiling`, default tier, 15.5 s — because the
production shape runs four interactive flows over one long-lived mux session
and the interactive ceiling was asserted only on M1's one-flow clean arm. It
reads M4's own clean arm and asserts the four-flow `clean_p99_max` against
`M1_CEILING_MS`; the bound, its derivation, its coverage cells and its two
vacuity probes are in *The four-flow clean level* above. It prints its own
verdict line rather than a `MANDATE` line or an arm row, so it neither adds an
id the runner refuses nor lands its measurement in M4's attribution.

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
| M1 lone-tail max | 256.0–1863.7 ms over ten runs of one revision (7.28×) | not asserted (the window's largest GE burst; see *M1's observation window*) |
| M1 field-RTT lone-tail p99 | `rtp v0.0.94`: 427–719 ms; landed `rtp` `bdacf5c0`: 293–432 ms | `1500 ms` (~2.1× the pinned band) |
| M1 field-RTT lone-tail `> 250 ms` share | `v0.0.94` 3.3–5.6 %; landed 2.6–5.3 % | `15 %` (~2.7×) |
| M1 field-RTT lone-tail max | `v0.0.94`: 1015–3868 ms; landed: 386–529 ms | not asserted (one sample at n≈200) |
| M2 hostile delivery | 1.000 | `0.995` |
| M2 lone-tail delivery | 1.000 | `0.995` |

The smoke panels carry the mandate lines regardless: the M1 latency panel
draws the **250 ms ceiling**, the M2 panels draw the **1.000 delivery floor**
and the **100 ms non-degradation bound** and the
M3 panels draw the **0.35× floor**, so a hostile or lone-tail breach is visible
in the evidence even when that arm's assertion is only a regression guard. The
assertion is a tripwire; the panel shows what moved. The M1 evidence also
draws a p99 CDF panel (the ceiling itself cannot be a horizontal line on a
latency CDF), and every `MANDATE` line prints p50/p90/p99/p999/max and the
`> 250 ms` sample count for all three arms.

**Redundancy monotonicity is NOT a mandate** — it was only ever a proxy for
these outcomes, and with M2 an offered-load-latency mandate there is no wire
ratio left to assert. Every gate above is vacuity-checked (break the bound —
inject latency, drop a delivery, starve the bulk lane — and the
gate fails naming the mandate); the harness must not restate this
constitution.

### M1's observation window: the ladder an arm's own drain can hold

M1's arms report a maximum latency, and a maximum is a measurement only if the
arm could observe the climb it came from. The M1-latency panel cannot settle
that on its own: its x range is the data's own extent, so the lone tail's
largest sample sits exactly at the frame's right edge, and the panel draws the
gap a long round trip leaves between samples as a straight line — the lane's
1892.3 ms lone-tail record, after a 1.89 s silence equal to that record's own
round, is drawn as a near-vertical wall although it is one completed round.
`mandate_smoke::m1_latency_window_censoring` (`default` tier, not `#[ignore]`d)
is the instrument that answers it. It is a **new test with no arm retuned**:
the `clean`, `hostile` and `lone_tail` arms keep their impairment, seeds,
windows, cadence, tier, guards and `#[ignore]` reasons, and it reads the series
they already produce through the same arm-run cache M2 reads, so a whole-target
run pays for no measurement twice and it is the one declared row here with no
cost of its own: the `mandate_smoke` target's wall over the runs this revision
measured is 142.48 s with the new test skipped, 142.59 s and 143.18 s with it,
and 142.60 s on the parent revision — a 0.7 s band the increment does not
exceed — while its `--exact` invocation alone pays the one M1 measurement
(49.88 s, its own libtest stamp). The two `#[ignore]`d field-RTT arms print
their own reading and gain nothing else.

**The window each arm needs, from the ladder's own law.** The lone tail is the
only source on its direction, so a loss burst is consumed one forwarded
datagram per datagram the lane sends; one tail transmission emits `m = 6`
datagrams (the fresh-tail armour's `primary + 5 copies` cover, `m = 6`; the
value is fixed by `rtp`'s armour configuration and recorded in `rtp/GATE.md`,
and `mandate_smoke.rs`'s `TAIL_DATAGRAMS_PER_TRANSMISSION` is the one place this
arithmetic reads it); and each rung past the probe budget waits
rtp's `TAIL_PROBED_MIN_RTO = 300 ms`, the floor that binds on every M1 arm
because the corroborated term `srtt + max(rttvar, srtt / 4)` is 250 ms even at
the field arm's 190 ms round trip. A burst of `l` datagrams therefore costs
`floor(l / m)` rungs and `floor(l / m) * 300 ms` — **when it begins on a
transmission's first datagram**, which is the case `probe_lone_tail_finite_loss_ladder`
drives and the only case it drives. A burst beginning `r` datagrams into a group
leaves that group's other `m - r` copies delivered, the message arrives, and no
rung fires at all, so the rung-producing events are the bursts that start at a
transmission boundary: their rate is the number of **transmission starts** times
the per-datagram burst-start probability, one start per request/response round,
i.e. `rounds * loss / mean_burst` — the probe below measures 5.6-6.6 starts per
window over its four windows (900-1056 rounds each) — and not the all-datagram
rate `E`, which charges every burst the rung cost of an aligned one. `E` remains the right count for the burst *budget* this table's column
states (how many bursts the window holds, and so how large the largest of them
plausibly is); the alignment is what turns that budget into a rung frequency.
So the window an arm needs is `rungs * step + rtt`. Every input is the arm's
own — the burst distribution
from its `NetemConfig.loss_model` (`1 / p31` and `p13 / (p13 + p31)`), the
round trip from its one-way delay, the burst budget from the datagrams its own
c2s link accepted (`Counters::received`, the instrument's one addition to
`ArmRun`), and the room from its own drain: `GRACE` for a cadence arm, whose
sample must reach the collector snapshot, and the arm deadline less the offer
window for a request/response arm, whose round is awaited inside the offer
loop. The burst is the one the window is expected to contain once — `E` bursts
occur in it, and the burst exceeded with probability `1 / E` is
`1 + ln(E) / ln(1 / (1 - 1 / burst))`. That is a design point and not a
ceiling; the geometric tail is unbounded, and the margin beside it absorbs the
difference.

| arm | drain | datagrams | burst model | bursts `E` | burst | rungs | needs | room | margin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `clean` | sink + `GRACE` | 4320 | iid 2 % (no burst) | — | 1 | 0 | 50 ms | 2000 ms | 40× |
| `hostile` | sink + `GRACE` | 10281 | GE 5 %, mean 8 | 64.3 | 32.2 | 5 | 1550 ms | 2000 ms | 1.29× |
| `lone_tail` | round awaited in loop | 7950 | GE 5 %, mean 8 | 49.7 | 30.2 | 5 | 1550 ms | 105000 ms | 68× |
| `field_rtt` | round awaited in loop | 2730 | GE 5 %, mean 8 | 17.1 | 22.2 | 3 | 1100 ms | 105000 ms | 95× |
| `field_rtt` d2 | round awaited in loop | 2881 | GE 5 %, mean 8 | 18.0 | 22.6 | 3 | 1100 ms | 105000 ms | 95× |

Every window holds. The tightest is the **hostile cadence arm at 1.29×**: its
sample must reach the collector snapshot, so its room is `GRACE` = 2 s = 6
rungs, and its own burst budget (5 rungs = 1550 ms) leaves one rung of
headroom. That is the one M1 place where a longer impairment burst, a shorter
`GRACE` or a shorter ladder step would begin to truncate a report, and it is
recorded here as the reading that would move first. The request/response arms
are not close: the field's own observed 3.2 s ladder is 10 rungs at this
instrument's 300 ms step (3.0 s of rung plus its round trip, and the arm's own
25 samples over 250 ms across the ten runs below place on that rung) against
the `lone_tail` arm's 105 s room, 33× inside it, and a 10-rung ladder needs a
burst of at least 60 consecutive datagrams, which `gilbert_elliott_loss(5, 8)`
produces with probability `(7/8)^59 = 3.8e-4` per burst — and only an *aligned*
burst costs a rung, of which the arm's own drain offers
`(8610 / 8.96) * 5 % / 8 = 6.0` per window rather than the 53.8 bursts every
datagram in that drain contains (8.96 c2s datagrams per round is measured, not
assumed: 35171 over 3926 rounds), so about once in 440 windows. A **cadence**
arm could not
observe that ladder at all (2 s = 6 rungs); the request/response shape is the
one that can, and it does. The rung is the 300 ms floor and not the 1 s
retransmission floor on this arm, and the arm's own record is the second
witness: at the 1 s floor, 3.2 s would be 3 rungs — an 18-datagram burst, `E`
times `(7/8)^17` = 5.6 drawings per window by the all-datagram rate, and
`6.0 * (7/8)^17 = 0.63` by the aligned one — so a 3.2 s maximum would appear in
about half of all windows, where the sixteen `lone_tail` runs on record hold
exactly one maximum above 2.7 s.

**The derivation is corroborated by the arm's scale, not by its maximum.**
Ten runs of the `lone_tail` arm on one revision and configuration (`rtp
v0.0.96`, `mux v0.0.33`, the arms above unchanged) read:

| reading | min | max | spread | mean | cv |
| --- | --- | --- | --- | --- | --- |
| maximum | 256.0 ms | 1863.7 ms | 7.28× | 874.7 ms | 0.63 |
| p99 | 145.3 ms | 179.3 ms | 1.23× | 160.3 ms | 0.07 |
| `> 250 ms` samples | 1 | 5 | 5.0× | 2.5 | 0.51 |
| samples | 792 | 1048 | 1.32× | 964 | 0.07 |
| drain (c2s datagrams) | 7189 | 9380 | 1.30× | 8610 | 0.07 |

The maximum tracks neither the drain nor the run: its correlation with the
drain is `r = -0.005` over those ten, and its spread is 5.6× the drain's. It
is a **draw**, and the arm sets its *scale* rather than its value: at each
run's own drain the arithmetic above gives a 29.5–31.5-datagram
once-per-window burst and a **4–5 rung ladder, 1250–1550 ms (1.24× across
the ten)** — the ladder's height is the window's largest GE burst, geometric
in the burst length (`P(L >= 6k) = (7/8)^(6k-1)` per burst over that window's
own `E` = 45–59 bursts), plus the round trip. That design point is an *upper*
estimate of a typical window's maximum, measured rather than assumed: 7 of
the ten windows peaked below it at a median of 2.6 rungs and one exceeded it
at 6.05 rungs, and it is an upper estimate twice over — it is the largest of
the window's `E` bursts rather than of a typical window's, and only the bursts
that begin on a transmission boundary cost any rung at all.

**What the arm shows, measured.** `mandate_smoke::m1_lone_tail_rung_distribution`
is a **new** arm (`full` tier, `#[ignore]`d, 75 s declared and 74.4 s measured)
that reads the rung counts out of the arm's own round series and prints the law's
prediction beside them; nothing about `clean`, `hostile` or `lone_tail` changed.
Its four windows (3926 rounds; 35171 c2s datagrams of which 2273 were dropped,
i.e. **6.5 % applied on c2s**, 3.8 % on s2c and **5.2 % pooled** against the
declared 5 %) read:

| threshold | measured, per window | corrected law | law without the alignment term |
| --- | --- | --- | --- |
| `> 250 ms` — one rung or more | 2.00 (8 pooled) | 3.15 | 28.19 |
| `> 550 ms` — two or more | 1.00 (4) | 1.41 | 12.65 |
| `> 1150 ms` — four or more | 0.25 (1) | 0.28 | 2.55 |
| `> 1750 ms` — six or more | 0.00 (0) | 0.06 | 0.51 |

`> 250 ms` is the rigorous rung indicator rather than a threshold of
convenience: `sample_delay` clamps at zero and this arm's impairment is the only
thing delaying one of its datagrams, so a round trip that waited for nothing is
at most `2 * (25 + 100) = 250 ms` and every round above it waited a rung; the
higher rows read the rungs off the 300 ms grid the arm's own series shows (one
ladder at 1510.5 ms, one at 1037.2, one at 996.6, and five between 276.1 and
578.3). Three further readings of the same arm agree on the first row to within
one event: 2.5 per window over the ten runs above (25 rounds with a rung, its
own `> 250 ms` row) and 2.5 and 2.5 per window over two more four-window samples
(10 and 10 pooled). The law without the alignment term is
rejected by the arm's own series (112.8 rounds over these four windows against
the 8 shown), which is the measurement the correction rests on; the probe prints
that rejection beside its pass as its vacuity demonstration, and it asserts the
corrected rate back — so a product change that restored the old frequency (an
armour cover collapsing to one copy, say) fails the row while a ladder that
stopped firing altogether also fails it. The residual is honest: the second row
is 1.4× high and the first 1.6× high on 4 and 8 pooled events, so the corrected
rate is good to about a factor of two at these counts and the two largest rows
rest on one event and none.

The `field_rtt` arm reads the same way: its required 1100 ms
sits at the top of the band its runs measure (max 358.3 / 434.6 / 581.5 /
1043.6 / 1255.3 ms over six runs on `rtp v0.0.96`), five of those six below
the requirement and one above it. The readings that repeat are therefore the
**guards** — the p99 band at 1.23× and the `> 250 ms` count against its 8 %
guard (1–5 samples of ~960, under 0.6 %) — which is why the maximum is
asserted on no M1 arm; the room the requirement is compared against is 15–95×
it, so a conservative design point costs no coverage.

**The field's two recorded round trips, against this distribution.** The
field's 1063 ms is inside it: four of the ten windows peaked above it (1205.7,
1257.9, 1411.6, 1863.7 ms). The field's 3205 ms is not: no window here reached
it, and the highest `lone_tail` maximum on record is 2651.7 ms. The arithmetic
above says why — a 3205 ms maximum is a ≥ 60-datagram **aligned** burst,
`(7/8)^59 = 3.8e-4` per transmission start against the arm's own 6.0 starts per
window — so a single 15 s window reproduces the field's worst case roughly once
in **440** runs. That is an order of magnitude rarer than the 1-in-49 the
all-bursts rate gave, and the direction is the safe one: the corrected model
makes the field's worst case *rarer*, not shorter, because the alignment changes
how often a burst costs a rung and not what a rung costs. What remains
assumption in that number is the step (300 ms, so "3205 ms" means 10 rungs — at
the field's 1 s retransmission floor the same 3205 ms is 3 rungs and an
18-datagram burst, `6.0 * (7/8)^17 = 0.63` per window) and the transfer of the
arm's own datagram mix and round count to the field's path, which carries a
proxy chain and a real link rather than this in-process model. Three parts of
the field's regime are outside this
arm and are claimed by nothing here: its **floor** (the arm's link is
`latency: 25 ms` with `jitter: 100 ms`, the impairment draws `latency +
U(-jitter, +jitter)` clamped at zero, and one run's own c2s one-way readings
were 80 % under 1 ms with a body p50 near 0.3 ms — which is **not** a missing
impairment, and this revision measured the difference: the same window's own c2s
counter reads 5502 delayed of 8957 forwarded, i.e. the impairment applied to
61 % of datagrams, against `P(delay > 0) = 0.625`. Why the *frame's* visible
one-way is nonetheless ~0 is unattributed: the obvious mechanism, "the peer
reassembles a frame from whichever of its `m` copies arrives first, so its
one-way is the minimum of `m` draws", does not fit the measurement (`m` copies
predict `P(min > 0) = 0.625^m = 6 %` at `m = 6`, and the arm reads p50 0.11 ms
and p90 25.7 ms where that mechanism predicts p90 0) — but the field's *minimum*
round trip is 190 ms), its **shape**
(no bulk lane, no proxy chain, an
in-process modelled link rather than the field's real path) and its **step**
(the rung here is the 300 ms `TAIL_PROBED_MIN_RTO` floor, where the field's
records include 1 s-floor ladders up to 5315 ms). The `field_rtt` arm above
covers the field's round-trip *scale*; it carries the same jitter, so it does
not cover the field's floor either.

**The loss model, as a dimension: `m1_lone_tail_loss_model`.** The rung probe
above reads the law at **one** impairment — the four-state Gilbert-Elliott model
at a 5 % long-run rate with a mean burst of 8 — and the law's `burst` term is
the quantity that sets the ladder, so a law about burst length was measured with
a burst and never with its absence. That is a **coverage** gap of the kind the
dual mandate names: a regime the family claims, in the quantity the declaration
is about. `mandate_smoke::m1_lone_tail_loss_model` is the second point. It is
`full` tier and `#[ignore]`d, **150 s** measured (149.32 s of libtest's own time
for eight ~19 s windows; run command and stamp in the budget arithmetic above),
and its vacuity failure is produced by the measurement path.

*What it varies, and only that.* The sibling
`mandate_smoke::m1_lone_tail_rung_distribution` is its reference pair: from it
this arm is **one dimension** — the **loss model** — and it is one dimension from
the family's reference in the *same* three ways the sibling is
(`composite(depth,impairment,metric)@lone-tail`, the label the checker derives
for both, because a relation is derived against a family's own reference row and
not against a sibling). Everything else is the sibling's: the dual lane, the
request/response shape at `depth = 1`, both seeds (41 and 42), `owd25` with
100 ms jitter, the 15 s window, the 256 B message and no bulk lane. Both models
run **in the same test, alternately, per window**, so a difference between them
is attributable to the loss model and not to host drift between two tests. The
instrument is the sibling's own: `loss_shape`, `ladder_inputs`,
`corrected_rung_counts`, `rung_threshold_ms`, `RUNG_DIST_THRESHOLDS`,
`RUNG_DIST_BINS` and the `RUNG_DIST_BAND_LOW`/`_HIGH` band.

*The comparability, as arithmetic.* `gilbert_elliott_loss(pct, mean_burst)`
builds the two-state model `p14 = p23 = p32 = 0`, whose mean burst is `1 / p31`
and whose steady-state loss share is `p13 / (p13 + p31)`; the preset chooses
`p31 = 1 / mean_burst` and `p13 = pct / (mean_burst * (1 - pct))` so that share
is `pct`. At `pct = 5`, `mean_burst = 8` the scaled integers the link carries are
`p31 = 536870912` and `p13 = 28256364`, so the model's long-run rate is
`28256364 / 565127276 = 0.0500000004`. The independent twin's threshold is
`loss_pct(5) = 214748360` of `u32::MAX`, i.e. `0.0500000` — the two agree to
`~4e-9`, a relative difference of `~8e-8`. The arm does not take that on trust:
it re-reads each model's rate from the configuration the link carries
(`loss_shape`) and asserts the loss its own counters measured against it (in the
runs below, `4.9 %` applied where `5.0 %` is declared on the independent arm and
`6.6–6.8 %` on the correlated one, both inside the `[0.5×, 2×]` check the
sibling probe uses).

*What it gates.* Three assertions, the first three of them properties of the
law: the correlated arm's first-rung count sits inside its own law's band (the
sibling's check, so this arm carries the same law); the independent arm reaches
the **second** rung **never**, because its burst is one datagram and
`floor(1 / 6) = 0` while the correlated arm's own law puts `5.4–5.6` such rounds
in the same pool; and the independent arm's first-rung count stays under the
*upper* band of the correlated model's law, so a collapsed cover (which would
put ~70 rounds a window there) cannot pass as independence.

*What it measures and does not gate.* Three clean runs of this revision (four
windows per model each; libtest stamps 149.32 s and 149.81 s — the latter is the
run the whole `--ignored` target shares, `finished in 280.91s` with the field
and sibling probes):

| reading | correlated (GE 5 %, mean 8) | independent (iid 5 %) |
| --- | --- | --- |
| rounds (pooled) | 3894 / 3783 / 3747 | 5987 / 5084 / 5687 |
| `> 250 ms` — one rung or more | 13 / 8 / 12 | 8 / 10 / 5 |
| `> 550 ms` — two rungs or more | 2 / 3 / 3 | 0 / 0 / 0 |
| `> 1150 ms` — four rungs or more | 1 / 1 / 2 | 0 / 0 / 0 |
| deepest ladder | 1612.0 ms (5.37 rungs) / 1688.6 (5.63) / 1850.7 (6.17) | 430.0 ms (1.43) / 507.8 (1.69) / 426.2 (1.42) |
| p99 | 180.1 / 163.3 / 168.8 ms | 117.9 / 122.0 / 121.5 ms |
| p99.9 | 412.5 / 413.5 / 367.4 ms | 250.6 / 275.2 / 237.8 ms |
| loss applied (of 5.0 % declared) | 6.60 / 6.62 / 6.65 % | 4.88 / 4.94 / 4.87 % |

(The same instrument on the sibling's own four GE windows reads `> 250 ms` 8
pooled and `> 550 ms` 4 pooled, the four events the section above lists at
1510.5, 1037.2, 996.6 and one of 276.1–578.3 ms.)

The **direction**, stated plainly: **yes on depth, no on frequency.** The
correlated process produces a longer ladder — it reaches
the second rung in every run (`2`, `3` and `3` of `3894`, `3783` and `3747`
rounds) where the independent one reaches it in none (`0` of `5987`, `5084` and
`5687`), its deepest excursion is `3.3–3.9×` deeper (`1612.0`, `1688.6` and
`1850.7` ms against `430.0`, `507.8` and `426.2`), and its tail percentiles move
with it (`p99` `1.34–1.53×`, `p99.9` `1.33–1.65×`). Its *first-rung* count does
**not** order the two models — `13` against `8`, `8` against `10` and `12`
against `5` — i.e. the independent arm produced *more* rounds above the floor in
one of the three runs. The trade a correlated process raises — fewer loss
events, more repair per event — is therefore **not visible at this mean rate**,
and the honest reading is that a burst buys ladder depth.

*The negative, and what it costs the law.* The independent arm's `> 250 ms`
rounds are not zero, which is what the law's `burst = 1` branch predicts
(`floor(1 / 6) = 0`): they are `8`, `10` and `5`, in `250.2–430.0`,
`265.7–507.8` and `267.1–426.2` ms. So the first threshold is **not** a
burst-frequency instrument — an independent loss process produces rounds above
the derived floor too — and the count the sibling probe asserts is a burst
component *plus* a non-burst one. The first run's eight independent values are
`262.1 250.6 269.3 430.0 262.1 258.4 253.9 250.2`, a body sitting within
`0.2–20 ms` of the floor `sample_delay`'s clamp derives (`2 × (25 + 100) = 250
ms`) with one at `430.0`; the correlated arm's thirteen are the same body
(`253.3–523.1`) plus those that are plainly ladders (`956.0`, `1612.0`). Two
consequences are recorded rather than papered over: the law explains the
ladder's **height**, not the first-rung frequency, and the first threshold's
derivation (`any round above the floor waited for a repair`) holds as an upper
bound on what a repair-free round can reach rather than as a tight boundary — a
handful of rounds in the `250–270 ms` band may have waited for nothing, which is
the same unattributed component the `field_rtt` paragraph above records. That is
why the arm gates the *depth* side, where the law makes a hard statement, and
reports the frequency side with both arms' numbers. The maximum is asserted on
no arm here, for the reason the section above gives.

*Its vacuity pair, and the runs.* Both faults perturb the arm's **input** — the
impairment — and both are visible in the log *before* the verdict, because the
arm prints each model's `mean_burst` and declared rate before it measures a
window (this is the mutation-applied evidence, not an inference from the
verdict):

```
# MANDATE_SMOKE_FAULT=M1_LOSS_MODEL_uncorrelated
[loss-model] arm=lone_tail_iid mean_burst=1.0000 datagrams_per_transmission=6 declared_loss=0.049999999
[loss-model] arm=lone_tail_ge mean_burst=1.0000 datagrams_per_transmission=6 declared_loss=0.050000000
[loss-model] check=correlated-law observed=8 corrected=0.0 band=[0.00,0.00] verdict=FAIL
FAILED ... finished in 148.66s

# MANDATE_SMOKE_FAULT=M1_LOSS_MODEL_correlated
[loss-model] arm=lone_tail_iid mean_burst=8.0000 datagrams_per_transmission=6 declared_loss=0.050000000
[loss-model] arm=lone_tail_ge mean_burst=8.0000 datagrams_per_transmission=6 declared_loss=0.050000000
[loss-model] check=independent-cannot-climb iid_ge2=2 correlated_ge2=5 (the law's own rate over this pool is 5.68 correlated, 0.00 independent) verdict=FAIL
FAILED ... finished in 149.87s
```

The first forces the correlated model's mean burst to one datagram —
independent loss wearing the four-state model's name — so the law's own
prediction collapses to zero while the arm's shallow rounds do not, and the
band assertion fails. The second hands the control arm the correlated model, so
it climbs the second rung twice and the discrimination assertion fails. An arm
whose reference pair collapsed onto one loss model, or whose control lost its
independence, is red on the run that changed it, and each fault's own
`mean_burst` line is what shows the fault was applied before its verdict is
read.

**The instrument, and its vacuity pair.** `censoring` reads the per-sample
series for the conjunction of two facts, and both are needed. *The series ends
on a climb*: the final sample is the series maximum and either holds a whole
rung above every earlier sample, or closes a strictly-increasing run whose
steps are within one rung of each other. The record alone is not evidence —
with only a handful of extreme samples per run, the largest of them being last
is common: across the 52 M1 lone-tail runs on record the final sample is the
series maximum in 15 of them (29 %) — so the shape alone is a screen with a
measured false-positive rate, not a verdict. *The climb is wider than the
arm's room*: the final sample exceeds every value that drain could have
observed. A record the arm's room can contain was observed to completion and
is a maximum however it sits in the panel; a record past that room was cut off
at the room's edge and is a lower bound. The five vacuity cases are printed by
the test, so the demonstration is evidence in the log and not only an
assertion that passed:

```
[m1-censoring] vacuity=truncated-climb    final=  1460.0 rungs_at_edge= 1.00 rise_run=5   edge_gap_ms=   250.0 room=   1200.0 verdict=Censored
[m1-censoring] vacuity=contained-climb    final=  1460.0 rungs_at_edge= 1.00 rise_run=5   edge_gap_ms=   250.0 room=   4000.0 verdict=EdgeRecordContained
[m1-censoring] vacuity=decayed-peak       final=    24.0 rungs_at_edge=-4.79 rise_run=1   edge_gap_ms=   250.0 room=   1200.0 verdict=Clear
[m1-censoring] vacuity=widened-rungs      final=   800.0 rungs_at_edge= 0.80 rise_run=1   edge_gap_ms=  1000.0 room=   1200.0 verdict=Clear
[m1-censoring] vacuity=outrun-window      mean_burst=400.0    required=  7850.0 room=  2000.0 verdict=RED
```

All five M1 arms report green. `clean` and `hostile` do not end on a climb;
`lone_tail`, `field_rtt` and the depth sweep classify as `Clear` or
`EdgeRecordContained` depending on where the run's own record landed, and never
`Censored`. The run whose lone-tail record does reach the frame's edge —
1871.3 ms, `rungs_at_edge=4.14`, one sample with an 1871.3 ms gap before it,
which is the round waiting on the ladder while the offer loop is blocked in its
read — reports `EdgeRecordContained`: the ladder completed inside the arm's own
room, and its position at the edge is the offer window closing rather than the
ladder being cut off. No arm was retuned and no arm was added, because none was
needed: **the windows were already sufficient, and the battery could not tell.**

### The deployed baseline the impaired tail must not regress past

**M1 is a hard floor, not a point on a frontier.** An impaired arm's tail is
not a cost to weigh against M2's offered-load latency or the clean arm: a
candidate that
raises any impaired arm's `p99` or `p999` past the band the deployed baseline's
own repeats showed is **rejected**, and where a change improves the clean arm or
M2 while raising an impaired tail against a change that does neither, the one
that does neither is chosen. That rule is a **mechanism**, not a judgement:
`mandate_smoke::m1_interactive_tail_latency` (default tier, the arm the M1
mandate already asserts on — no arm, window, cadence, seed, tier or existing
threshold moved) now carries a recorded per-arm baseline and fails by name when
a candidate is worse on it.

**The recorded baseline is the deployed `rtp v0.0.98` transport** (`rtp dev
559fc2b3`: pacer seed `INIT_SEND_RATE = 1024` with the fresh-tail armour cover
at 4/5 copies, `m = 6`). Its **median** is that transport's own six full-window
reps; its **limit** is `mean + four sample standard deviations` over the
**thirty fault-free full-window reps now on record** — those six, the four
runner-configuration reps recorded here, and the twenty healthy reps measured
for this change — rounded up to the next whole millisecond. **`p99` is asserted;
`p999` is reported, not enforced**, and an asserted `p99` must clear both the
limit and the arm's own measured 40 % noise band:

| arm | `p99` median (6 deployed reps) | 30-rep `p99` range | `p99` limit (mean+4sd, 30 reps) | band bound (median+40 %) | effective `p99` bound | `p999` limit (reported) |
| --- | --- | --- | --- | --- | --- | --- |
| `hostile` | 166.7 ms | 126.0–275.0 | **298 ms** | 233.4 ms | **298 ms** | 405 ms |
| `lone_tail` | 159.0 ms | 131.0–200.4 | **223 ms** | 222.6 ms | **223 ms** | 1324 ms |

The 30 `p99` reps are, `hostile`: 218.4/156.7/210.7/168.6/164.7/152.3 (the six
deployed), 221.8/156.9/128.0/127.8 (runner configuration), 162.4/170.6/158.1/
140.2/168.2/200.1/163.8/185.2/173.7/130.7/195.0/153.0/178.9/275.0/126.0/154.7/
175.8/180.9/155.4/162.3; `lone_tail`: 155.2/176.5/162.8/177.1/151.3/151.2,
155.1/184.4/175.2/148.0, 173.0/159.6/166.4/200.4/154.4/177.8/180.5/178.9/
174.1/160.0/170.3/154.4/163.2/144.0/131.0/164.6/150.9/181.7/162.7/169.3.

**Why the limits changed: the six-rep `mean+3sd` bounds flaked, and the `p999`
bound cannot be enforced at all.** The previous revision asserted `mean + 3sd`
over the six deployed reps — `hostile p99` 265 ms, `lone_tail p99` 199 ms,
`hostile p999` 348 ms, `lone_tail p999` 1321 ms — and over **20 healthy
full-window runs of the deployed build it failed three times**: `lone_tail p99`
200.4 ms against 199.0 (that run's clean arm read 37.5 ms against its usual
26.5 ms), `hostile p99` 275.0 ms against 265.0, and the same run's `hostile
p999` 375.5 ms against 348.0. Every failure was a contended window — the clean
arm read 37.5, 52.5 and 78.0 ms in the three worst runs — and contention is the
runner's **normal** configuration (libtest's default threading runs six tests at
once). Three sigma is not a rejection rule at this sample size: over the 30-rep
set `mean + 3sd` is **266.0 ms** `hostile` and 208.4 ms `lone_tail`, and the
`hostile` arm has a healthy rep at **275.0 ms**, above it. Four sigma is the
smallest standard margin that covers the observed healthy range (297.8 /
222.8 ms), and 4 is also RFC 6298's `K`, the variance margin this transport's
own RTO uses. The `p999` instability is worse and cannot be fixed by any margin
worth calling a bound: the six deployed reps alone span 198.6-937.3 ms (4.7x),
the 30-rep set spans 185.6-1015.9 ms, and the workspace has a healthy reading at
**2723.3 ms** — a bound wide enough never to fire on that set would reject
nothing. So `p999` is printed with the same comparison and a `REPORTED` verdict
and asserted nowhere; what the gate still catches is the asserted `p99` and the
`>250 ms` share guards below.

**The limits are also the body of the change's measurement.** Six further
healthy reps of the new build all pass: `hostile p99`
214.1/190.6/193.3/152.0/138.9/133.7 ms (`lone_tail p99`
164.0/158.5/169.9/145.6/140.0/185.8) with the `p999` rows reported at
257.6/215.6/232.0/257.5/175.4/188.7 and 262.2/216.6/1095.3/222.9/354.9/317.7 ms
— 6 of 6 green, where the old bounds would have been within 6 % of firing on the
worst of them. The residual is stated rather than hidden: a systematic
`hostile`-`p99` rise **under 79 %** (166.7 -> below 298 ms) is no longer
rejected by this baseline assertion, because the healthy contended spread
reaches +65 %; the `>250 ms` share guards bound that regime.

The same six reps record the rest of the baseline for the reader, and are
asserted nowhere because each mandate already owns its own bound: **clean
`p99` 26.5 ms** (reps 26.1–26.6, bound 27 ms), and the M2 owner gate
(`rtp_mux_jitter::jitter_duallane_constitution_gate`, the 40 msg/s `both` arm)
at ~26 ms `p99`, comfortably inside its 100 ms non-degradation bound.

**The bound is a distributional limit and its sensitivity is the arms' own.**
The `lone_tail` `p999` band is wide (its reps span 198.6–937.3 ms, 4.7x) because
that arm's heaviest recovery episode lands inside a 15 s window or does not, so
that assertion is a coarse tripwire; the two `p99` assertions carry the teeth,
and `p999` still rejects a doubling of the ladder's height (the `>250 ms`
shares and every other reading stay printed on the arm line). The
band is **not** an artefact of the configuration the reps were taken in: four
further runs of the same arms in the runner's own configuration (libtest's
default threading, six concurrent tests — `m1_interactive_tail_latency` run
three times without `--test-threads=1`, plus the full `tools/mandate-check` run)
read `hostile p99` 221.8 / 156.9 / 128.0 / 127.8 ms, `hostile p999` 252.9 /
214.6 / 170.3 / 156.9, `lone_tail p99` 155.1 / 184.4 / 175.2 / 148.0 and
`lone_tail p999` 234.6 / 282.0 / 258.5 / 247.6 — every one inside the recorded
bounds, the tightest being `lone_tail p99` at 184.4 ms against 199 ms. The
gate prints one `[m1-baseline]` row per arm and metric — observation, recorded
baseline, bound, rep count, the reps' own range and the verdict — so a
borderline pass is visible in the run's log instead of being inferred from a
verdict string.

**Its vacuity demonstration, and what it says about the cover reduction.**
The gate is red when the property it guards is broken, and the proof is a
**deterministic input perturbation** rather than a draw:
`MANDATE_SMOKE_FAULT=M1_IMPAIRED_slow` adds one fixed 300 ms one-way delay to
the `hostile` **and** `lone_tail` links (the clean arm untouched, so a probe of
the bound cannot read as a probe of the mandate ceiling), and the arm then
reads

```
[m1-baseline] arm=hostile    metric=p99  observed=  1891.4 baseline_v0.0.98=   166.7 limit=   298.0 banded_bound=   298.0 asserted=true  baseline_reps=6 limit_reps=30 limit_reps_range=126.0..275.0 verdict=REGRESSED
[m1-baseline] arm=hostile    metric=p999 observed=  1962.0 baseline_v0.0.98=   235.2 limit=   405.0 banded_bound=   405.0 asserted=false baseline_reps=6 limit_reps=30 limit_reps_range=143.8..375.5 verdict=REPORTED
[m1-baseline] arm=lone_tail  metric=p99  observed=  1512.2 baseline_v0.0.98=   159.0 limit=   223.0 banded_bound=   223.0 asserted=true  baseline_reps=6 limit_reps=30 limit_reps_range=131.0..200.4 verdict=REGRESSED
[m1-baseline] arm=lone_tail  metric=p999 observed=  1512.2 baseline_v0.0.98=   349.7 limit=  1324.0 banded_bound=  1324.0 asserted=false baseline_reps=6 limit_reps=30 limit_reps_range=185.6..1015.9 verdict=REPORTED
[M1] the hostile arm's p99 is 1891.4 ms — WORSE than the deployed rtp v0.0.98 baseline (166.7 ms, the deployed 6 reps' median) beyond its effective bound 298.0 ms (the 30 reps on record give a limit of 298.0 ms as their mean + 4 sample standard deviations, and the arm's own measured 40% noise band gives 233.4 ms; the wider of the two binds): +1034.6% against the recorded baseline. M1's impaired tail is a hard floor — it must not be traded for M2's offered-load latency or the clean arm, and a candidate that does is rejected here rather than absorbed inside a guard.
```

and the test fails with both asserted `p99` metrics `REGRESSED` (the `p999` rows
are reported). The fault is a *level* shift, so the message the run shows is the
one the standing rule names; the assertion is placed first among M1's
impaired-arm assertions for the same reason (298 ms is the tightest bound any of
them carries), so a candidate that regressed is reported against the deployed
baseline rather than inside a superseded tripwire. No existing guard is
loosened, removed or reordered: they remain, and still fire for the failures
that move a share or a count without moving these percentiles.

**The honest counterpart — the production lever itself.** Two
runs with the fresh-tail armour cover removed
(`FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY` /
`..._NO_PARITY` 4/5 -> 0/0) read `lone_tail p99` 264.2 ms (past the 199 ms
bound, `REGRESSED`) and 184.5 ms (inside it, `OK`): a **production** mutation of
that family trips this gate on one run in two, which is exactly why the red
proof above is a deterministic input fault and not a production change — a
one-off red is not a vacuity proof. And the rejected cover reduction itself
(cover 3/4, `m = 5`) is **not** rejected by this gate at all: its three reps on
the same tree read `hostile p99` 197.7/170.0/159.3 (against the baseline's
152.3–218.4), `hostile p999` 237.8/227.8/194.6, `lone_tail p99`
204.7/166.1/174.0 and `lone_tail p999` 429.1/334.3/244.7, none past its bound.
So the `+47 %` `hostile p99` cost recorded for that change in `rtp/GATE.md`
(`127.2 -> 187.2 ms`) is a **single-rep pair**: eleven reps of its own baseline
(eight serialized, four in the runner's contended configuration) span
119.6–221.8 ms, and the `m = 5` reps span 159.3–204.7 ms, so the two
distributions overlap. The gate is therefore a *band* on the impaired tail —
it has teeth against a ladder's worth of degradation, and a one-rung cover
reduction is **smaller than this arm's own run-to-run spread**, so it is
recorded as undetectable here rather than chased by tightening the bound until
it fails. Closing that sensitivity needs more observation per arm (a
median-of-N or a longer window), which is a **new** arm under the perf-test
dual mandate and not a retune of this one.

**The seed dimension, swept.** `rtp/GATE.md` records the seed
(`INIT_SEND_RATE`) as the one dimension never swept between the two values that
were (`128` and `1024`), names the owner gate a **step function** of the seed
(its 40 msg/s `both` arm offers 40 x 6 = 240 pkt/s, so every seed at or above
`240` admits the whole declared fresh-tail cover) and refuses the seed as a wire
lever. That record is the wire measurement's, and the length of the step is its
owned fact; what the sweep also measures is the M1 tail, one dimension per arm,
on this workspace's `rtp` revision, with the full-window `mandate_smoke` arm set:

| seed | clean `p99` | `hostile p99` / `p999` | `lone_tail p99` / `p999` |
| --- | --- | --- | --- |
| 128 | 87.6 ms | 202.1 / 297.4 | 156.7 / 239.3 |
| 208 | 84.4 ms | 241.8 / 302.4 | 161.4 / 347.9 |
| 256 | 85.6 ms | 198.2 / 231.6 | 161.1 / 910.0 |
| 384 | 79.1 ms | 201.7 / 262.4 | 196.4 / 636.2 |
| 512 | 29.5 ms | 271.0 / 331.2 | 161.1 / 528.1 |
| 768 | 27.9 ms | 203.0 / 288.6 | 138.8 / 220.2 |
| **1024 (deployed)** | **26.5 ms** | 166.7 / 235.2 | 159.0 / 349.7 |

The M1 reading is that the deployed seed is the best swept point on the clean arm
by a factor of ~3 and is not worse on either impaired arm, so **the seed is not
an M1 lever either**: no seed in the swept range beats `1024` on the clean tail
without giving it back on an impaired one. The redesigned M2 does not read the
seed at all — its assertion is the offered schedule and the latency under it,
neither of which the seed moves — so the step-function record in `rtp/GATE.md`
stands as the wire measurement's own history and this sweep closes the seed as an
M1 dimension.

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
latency are measured, split by **when** the sink observed them (`on_time`,
`late`, `lost` — see the table below), and the data the assertion reads is
their own counts; the bulk lane is connected (the topology is the
production dual-lane one) but carries no stream, so the quantity measured is
the interactive lane's own split, which no other arm measures. A second arm
repeats the multi-flow offer on the M1/M2 `hostile` link.

Four quantities, asserted on both arms unless the row says otherwise:

| quantity | measured (M4's own runs) | bound (derived) |
| --- | --- | --- |
| per-flow delivery (`received_i / offered_i`, `received_i` = the messages the arm observed inside its whole horizon = `on_time_i + late_i`) | 1.000 on every flow of both arms, 29 runs | `>= 0.995` (no starvation) |
| per-flow lost (`lost_i` = `sent_i - received_i`: offered, never observed) | 0 on every flow of both arms in every steady-state run | the floor's own unit budget, stated below |
| per-flow late (`late_i`: observed after the arm's `window + GRACE` cutoff, inside the 1.2 s drain after it) | 0 on both arms in every steady-state run; 25-35 of the clean arm's ~2300 units under `MANDATE_SMOKE_FAULT=M4_late` | **not asserted**: reported per flow, per arm, on its own panel and in `M4.csv`. A late arrival is a latency event, bounded by the per-flow p99 bounds below and drawn against M1's ceiling -- the delivery floor is not a latency bound and does not pretend to be one |
| fair-share imbalance `max_i \|share_i − 1/N\| / (1/N)`, `share_i` = flow `i`'s share of the lane's delivered bytes | `0.46 %` (clean worst), `0.43 %` (hostile worst) | `1 %` (2.2× the worst measured, so a change that at least doubles the imbalance fails) |
| clean-arm p99 spread `max p99 / min p99` | `1.20×` (36 runs, both windows) | `2×` (1.67× the worst measured) |
| hostile-arm per-flow p99 | `<= 423 ms` | M1's hostile p99 regression guard (item 1's row above; not restated) |

The two delivery rows are one measurement split by **when** the sink observed it,
and the split is the whole reason the floor is a starvation bound. A cadence
arm's samples are read from the server sink, so before the split a message
still riding the repair ladder when the `window + GRACE` drain expired was
counted as lost although the lane delivered it: `recv` meant "observed within
`window + GRACE`" while the arm's own claim is "every flow delivers what it is
offered", and the two differ by the ladder (the 1 s `MIN_RTO` floor with
backoff, and M1's lone-tail arm -- the same ladder on the same lane -- measures
p99 1530 ms against its own 3200 ms guard, so `GRACE` sat *inside* the
documented repair tail). The drain is now `GRACE + 1.2 s` (`3200 - 2000`), it
ends the moment the whole offer has been observed, and only a message the arm
never observes is lost. Nothing was moved to make anything pass: the cutoff is
still `window + GRACE` and still what `on_time` means, and a late arrival is
still visible -- in the `late` cell, on the `M4-late` panel, and in the
percentile series (a late sample carries its own latency, so a lane that
stalls raises the per-flow p99 this arm draws against M1's ceiling).

**The floor's granularity, in the units it is made of.** M4 counts
sink-observed **messages** against `write_all`-accepted ones, so one unit is one
256 B message and the floor's slack is a count, not a decimal: at an offer of
`sent` the floor tolerates `floor(sent × 0.005)` lost units and the next one
fails it. Across the battery's 640 flow-arms `sent` spans 2037–2301, so
`budget_units` is **10-11** and the first failing count is **11-12 on every
run** (`tools/mandate-check` computes and prints exactly this on its `delivery:`
line every run, and records it in `mandate-check.json` under
`delivery_granularity`). The **event size** matters more than the budget: `mux`'s
reader is a **byte stream**, so one frame lost on the hostile link
head-of-line blocks every message of that flow offered behind it -- the arm's
smallest possible shortfall is **one tail block, not eleven independent
losses**. At this arm's 12 s window and ~2136-unit smallest offer that block is
`11 × 12 s / 2136` = **61.8 ms** of the flow's own offer (`block_ms`; the
measured band across the derived runs is 61.9–64.1 ms). **A floor of `0.995` is
therefore one block away from failing on every run**: the arm can see almost
nothing between a clean pass and a breach, and the smallest breach is a single
head-of-line event, not a statistical loss rate. That is why a shortfall here is
reported as a count with its block duration rather than as the ratio's third
decimal, and why the loss cell and the late cell are separate: before the split,
the same block could be one unit *late* or eleven units *lost* depending on a
two-second drain that the ladder itself outlives.

**The M2 arms keep their cutoff, and that is stated rather than fixed here.**
The single-flow `clean`/`hostile`/`lone_tail` arms still read `recv` at the
`window + GRACE` cutoff, so the same "late or lost?" ambiguity sits under their
delivery figures (the clean arm asserts `delivery == 1.000` exactly, and the
hostile/lone floors are `0.995` with 11- and 4-unit budgets). They do not carry
the starvation claim the M4 split exists to make -- there is no second flow to
starve -- and their samples are the same ones the M1 tails are derived from, so
changing their basis would move six recorded M1/M2 numbers in one edit. The gap
is recorded, not closed: an M2 delivery figure is still a figure the drain
timed, and a reader comparing the two mandates' delivery rows should know that
only M4's is a loss count.

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

M4 **reports** the absolute interactive ceiling rather than asserting it: the
4-flow clean arm measures p99 174.8–188.9 ms — `0.70–0.76` of M1's ceiling —
and it owns fairness and delivery, not the absolute level. M1 remains the
authority for the ceiling on **one** flow, and the four-flow levels are asserted
by the separate arms below -- the **clean** level against M1's 250 ms ceiling
and the **hostile** level against the ceiling its own measured distribution
supports -- which read M4's own arms rather than restating them. The M4 latency
panel draws the ceiling and the run's own `hostile_p99_guard`, and the `MANDATE M4` line
prints `clean_p50_max`/`clean_p99_max`/`hostile_p99_max`, so a multi-flow
latency regression is visible in the evidence and in the verdict line. The arm's own
cost is the queueing delay of four flows behind the shared interactive window
(p50 24 ms, p90 155 ms, p99 180 ms on the 12 s window where the one-flow arm
sits at the 25 ms floor): the split is fair, and the lane's latency budget
under 4× multiplexing is the finding that number carries.

M4 is **default tier** (not `#[ignore]`d): its asserted quantities are counts
and shares over a seeded link, the same class as the mandate-2 constitution
gate, and its ~31 s wall-clock belongs in the gate that always runs. The
measurement is part of the one command above — M4's evidence is `M4.json` plus
**six** panels: the four that were already there (per-flow shares against the
fair-share line, per-flow departure from it against the ±bound, per-flow
delivery against the floor, and per-flow p50/p99 against M1's ceiling) plus the
split of the delivery cell into the units the floor is made of — a per-flow
`M4-lost` line against the floor's own unit budget (the count that first
breaches it is the budget plus one) and a per-flow unattributed `M4-late` line
holding the backfill the old basis counted as lost. The three vacuity
demonstrations are `MANDATE_SMOKE_FAULT=M4_starve` (flow 0 is offered the whole
window and the rest only its second half: the fair-share bound fails naming M4
at 0.70 imbalance), `MANDATE_SMOKE_FAULT=M4_drop` (90 % loss on the clean link:
the per-flow delivery floor fails naming M4, and the failure line prints the
identity it was read from — `sent = on_time + late + lost`, measured `12 = 0 +
0 + 12` on the arm's first flow) and `MANDATE_SMOKE_FAULT=M4_late` (the clean
link's c2s direction delayed by `GRACE + 50 ms`, so the offers of the window's
last ~50 ms are observed after the cutoff and inside the drain: `late` is
non-zero on every flow with `lost` at 0 in two of three runs — the floor green,
`delivery 1.000`, the old basis's `on_time / sent` 0.879 — and `late=25,
lost=6` in the third, where the floor fails on the six never-observed messages
rather than on the 25 late ones). The `M4_late` fault is **composite** and the
declaration says so: it lengthens the round trip as well as crossing the cutoff,
so it moves the offer as well as the arrival, and it cannot isolate `late` from
`lost` on every run. What it isolates is the *cell*: in every run the three
counts are separate and the floor's verdict is read from `lost` alone.

### The four-flow clean level: `m4_clean_lane_p99_ceiling`

The production shape runs four interactive flows over one long-lived mux
session, and the interactive ceiling was asserted only on M1's **one-flow**
clean arm, so the four-flow clean p99 was measured and drawn but bounded by no
arm. `mandate_smoke::m4_clean_lane_p99_ceiling` closes that gap as a **new**
arm beside M4 (M4 itself is untouched): it takes
`fairness_arms("M4")`'s clean arm — the same `link(41/42, OWD, JITTER,
LOSS_2, 0)` interactive link, the same four tagged flows at the same cadence
over the same window, the same connected-but-unladen bulk lane — and asserts
four things on it.

| quantity | measured (this arm's derived runs) | bound (derived) |
| --- | --- | --- |
| aggregate four-flow clean `p99` (`max_i p99_i`) | `174.8–188.9 ms` across 10 runs, both revisions | `<= 250 ms` (`M4_CLEAN_P99_CEILING_MS`, one authority: [`M1_CEILING_MS`]) |
| per-flow delivery (`received_i / sent_i`) | `1.000` on every flow of every run | `>= 0.995` ([`M4_DELIVERY_FLOOR`], not restated) |
| clean p99 spread (`max p99 / min p99`) | `1.007–1.072` | `2×` ([`M4_LATENCY_SPREAD_BOUND`]) |
| `samples > 0`, `p99` finite and positive | `8557` delivered in the recorded run | asserted (an instrument sanity, not a bound) |

**Why the ceiling and not a looser guard.** The file's guard rule for a
regression bound is a multiple of the worst measured — `1.67×` for the
fair-latency bound, `2.2×` for the imbalance bound, `~3×` for the hostile
latency guards. Applied here it wants `2 × 188.9 = 377.8 ms`, which is
**above** the ceiling and therefore bounds nothing the product promises: a
bound the lane could breach while still meeting M1's ceiling is not a level
bound. The ceiling is thus the tightest level bound this arm's own
distribution supports, and its value is reused rather than picked: the
sensitivity is `250 / 188.9 = 1.32×` the worst measured (`1.415×` on the run
recorded below), so the arm fires on a third again as much tail. The two
remaining assertions are what keep a level pass meaningful, and they are read
**with** the level rather than instead of it: a lane that meets its p99 by
starving a flow, or by carrying three fast flows and one slow one, fails the
floor or the spread and is named.

**Vacuity, at the magnitude the property names.** Two probes perturb the arm's
own input (so the failure is produced by the measurement path, never by the
assertion), each an extra one-way delay on the arm's clean link:
`MANDATE_SMOKE_FAULT=M4_CLEAN_LEVEL_double` (`+100 ms`) lands the aggregate
p99 at **501.0 ms = `2.004×` the ceiling** — the doubling case — and fails the
bound by name (`p99 501.0 ms exceeds the 250 ms interactive ceiling`), and
`M4_CLEAN_LEVEL_slow` (`+200 ms`) drives it to **866.2 ms** with the fair-
latency and delivery assertions still green, i.e. the level bound is what
fails, not a side effect. `M4_CLEAN_LEVEL_slow` is **composite** and declared
as such: at `+200 ms` it lengthens the round trip enough to slow the arm's own
rate ramp as well as the one-way hop, so its reading is a floor on what the
probe costs and not an isolated measurement of the one-way delay.

**Cost and coverage.** Default tier (not `#[ignore]`d) beside M4, and it takes
the smoke set's shared `SERIAL` guard so its wall-clock measurement never
overlaps another arm's. Measured cost **15.5 s** on the 12 s window (`15.52 s`
by `--report-time` over 10 filtered-out tests, two runs), which is the window
plus `GRACE` plus ~1.5 s of connection and stream setup; `--quick` selects the
8 s window. Cells provided:
`M4-level@flows=4+impairment=clean+metric=p99-ceiling`,
`M4-level@flows=4+impairment=clean+metric=no-starvation`,
`M4-level@flows=4+impairment=clean+metric=fair-share`,
`M4-level@flows=4+impairment=clean+metric=fair-latency`,
`M4-level@instrument=degenerate-percentile`. Cells deliberately **not**
covered, with the reason: an impaired four-flow level (the hostile four-flow
lane is M4's own arm and its per-flow p99 is guarded there against M1's hostile
guard — the level to assert on a loss-driven tail is a different question and
would need its own derivation), and a flow count other than four (the sink
attributes samples by first-byte tag, whose range caps at seven flows; four is
the production shape).

**What the level's number is made of.** The four-flow clean p99 is **not** a
steady-state queueing tail: it is the connection's **start-up transient**.
Reading the arm's own per-sample timeline (`rtp_mux_it125` run `tl_m4`, the
same arm with a per-sample trace added to its collector), the four-flow clean
lane sits at `80–193 ms` from `t=1.5 s` to `t=5.0 s` and at the `24 ms` one-way
floor from `t=5.1 s` to the window's end, with a handful of `85–95 ms` blips
left in the clean stretch; the one-flow clean arm's own timeline (`M1.csv`, the
`mc_empty` run) has the same shape over `t=1.5–2.0 s` peaking at `99 ms`. The
median is identical across one and four flows because the floor is what most
samples are; the p99 doubles because the transient is four times as long and
twice as deep. The transient's own driver is measurable: attaching an
`rtp::metrics::MetricsObserver` to the interactive client connection (`rate_m4`
run) shows `send_rate_packets_per_second` climbing `128 → 4099.5` between
`t=1.503 s` and `t=6.689 s` and `delivery_sample_app_limited` flipping
to `true` at the moment it stops — the ramp starts at `INIT_SEND_RATE`
(`rtp/src/reliable/reliable_layer.rs:67`), and `cwnd` is derived from it
(`cwnd = rate × rtt × CWND_SEND_RATE_SCALE`, `rtp/src/traffic_shaping/recovery/
pkt_send_space.rs:30,1398`), so the offered load is served from a standing
sender-side backlog until the ramp overtakes it. The alternatives the same
measurements rule out: the transient is not loss/repair (delivery is `1.000`
on every flow and the deep-Latency samples carry no repair counters), and it
is not the `netem` link (the episode's end coincides with the rate crossing
the offer, not with any impairment change).

**A lever existed, is measured, was refused as a one-parameter change, and has
now landed as one and only one of its two parameters.** Raising
`INIT_SEND_RATE` from `128` to `1024` removes the transient almost entirely on
the **one-flow** clean arm — measured `clean_p99` `90.8 → 26.8 ms` and
`clean_p50` `24.0 → 21.6 ms` (two reps each, `26.6/26.8` ms; this run measures
`clean_p99` 26.2 ms, p999 28.7, max 30.0). The mechanism that buys it is
a wire measurement, printed as diagnosis and no longer weighed against any
budget: the clean arm's c2s forwarded wire rises `2.22× → 5.81×` of the offered
payload.  The *reason* the wire moves is the coupled second
parameter: at `128` the send pacer was the binding term and never admitted the
declared sixth datagram — measured, cutting the declared cover to one changed
neither the wire (2.08× → 2.07×) nor the p99 (87.3 → 82.8 ms) — so at `1024`
the whole declared budget reaches the wire for the first time.  The cover was
therefore swept as a frontier, and **refused at every setting below the one
that ships**; the table, the two safeguards and the numbers are in
`rtp/GATE.md` ("What `INIT_SEND_RATE` was bought with").  In short: `m = 2`
(closed-gate 1 / open-gate 4) reads one-flow clean p99 29.7 ms
and four-flow p99 130.4 ms but **breaks the ladder's monotone-non-increasing
invariant**; `m = 4` reads 27.7 ms with four-flow p99 137.2 ms but
**fails `m1_latency_window_censoring`**, an existing default-tier gate, because
the hostile arm's mean-8-datagram burst can then build 7 rungs against a frozen
2000 ms room (2150 ms required) and that arm's reported tail would be a lower
bound; `m = 1` is worse on both axes (87.0 ms).  What the landing
leaves open, stated rather than implied, is the four-flow clean tail
(`176.1 ms`, essentially unmoved from `180.8`).  The lever
for it is `m = 5`, which the window-adequacy arithmetic admits (5 rungs,
`1550 ms` ≤ 2000 ms) but which no *existing* hostile arm can observe — so
admitting it is a **new arm with a longer window**, declared here in `rtp_mux`,
not a retune of an existing one, and it is not taken in this change.

### The four-flow hostile level: `m4_hostile_lane_p99_ceiling`

The four-flow **clean** level above closes the clean shape's ceiling gap, but
the production shape's **hostile** tail had no level arm at all: M1's 250 ms
ceiling is asserted on M1's one-flow clean arm, M4's `hostile` arm guards each
flow against M1's loose `900 ms` per-flow regression guard, and M4 reports the
aggregate `hostile_p99_max` without asserting it. So the four `hostile_p99`
bars of the four-flow hostile arm sat **above** the 250 ms ceiling drawn on
`M4-latency` on a passing battery, and no gate named their level — the
`m1-four-flow-hostile@flows=4+impairment=hostile+metric=p99-ceiling` gap in
this file. `mandate_smoke::m4_hostile_lane_p99_ceiling` closes it as a **new**
arm beside M4 (M4 itself is untouched): it takes `fairness_arms("M4")`'s
hostile arm — the same `hostile_link(41/42)` GE `5 %`/mean-8 + 100 ms-jitter
link, the same four tagged flows at the same cadence and window, the same
connected-but-unladen bulk lane — and asserts four things on it.

| quantity | measured (this arm's derived runs) | bound (derived) |
| --- | --- | --- |
| aggregate four-flow hostile `p99` (`max_i p99_i`) | `274.9-423.0 ms` across the 18 fresh reps and 2 recorded observations | `<= 452 ms` (`M4_HOSTILE_P99_CEILING_MS`) |
| per-flow delivery (`received_i / sent_i`) | `1.000` on every flow of every run (including both vacuity probes) | `>= 0.995` ([`M4_DELIVERY_FLOOR`], not restated) |
| `samples > 0`, `p99` finite and positive | `9003` delivered in the recorded run | asserted (an instrument sanity, not a bound) |

**Why 452 ms, and why it exceeds the M1 ceiling.** The clean-lane arm could
reuse `M1_CEILING_MS` because that is what the product promises and the
four-flow clean p99 meets it (`0.70-0.76` of it). The hostile shape does not:
the same statistic measures `274.9-423.0 ms`, `1.10-1.69x` the ceiling, so a
250 ms bound here would be false on every run. The bound is instead derived by
the mechanism this file already uses for the impaired tail (*The deployed
baseline the impaired tail must not regress past*): `mean + 4` sample standard
deviations over the healthy reps on record, rounded up to the next whole
millisecond. The reps are eighteen full-window reps of M4's own `hostile` arm
measured on this crate's pinned `rtp v0.0.98` — `274.9`, `296.9`, `297.1`,
`301.0`, `308.0`, `315.6`, `315.7`, `316.5`, `323.5`, `324.1`, `331.1`,
`334.4`, `334.6`, `341.8`, `345.1`, `345.9`, `351.8`, `358.3` — plus the
harness baseline's own recorded `hostile_p99_max=306.5`
(`crates/netem_test/tools/mandate-baseline.json`) and the `<= 423 ms` worst
per-flow p99 M4's own bounds table records above. Over those `n = 20`
observations the mean is `327.3 ms` and the sample standard deviation `31.0 ms`,
so the limit is `327.3 + 4 × 31.0 = 451.1`, rounded up to `452 ms`. It fires on
a regression of `452 / 423 = 1.07x` over the recorded worst (`1.26x` over the
worst fresh rep).

The file's guard convention for a regression bound — a multiple of the worst
measured, `2 × 423 = 846 ms` — is the `900 ms` guard M4's arm already asserts,
so it would add no level of its own; the 452 ms limit is **1.99x tighter** than
that guard. The honest content of the arm is therefore twofold: it bounds the
production shape's hostile tail at the tightest level its own distribution
supports, and it **names the hole** — `452 / 250 = 1.81x` above the M1
ceiling, because the GE + 100 ms-jitter tail is the product's known hostile
defect (the `1 s` `MIN_RTO` repair floor plus backoff) and not a scheduler
artifact. A bound that hid that (a 250 ms assertion, or a 900 ms guard
presented as the level) would be worse than the named hole.

**Vacuity, at the magnitude the property names.** Two probes perturb the arm's
own input (so the failure is produced by the measurement path, never by the
assertion), each an extra one-way delay on the arm's hostile link.
`MANDATE_SMOKE_FAULT=M4_HOSTILE_LEVEL_double` (`+100 ms`) drives the aggregate
p99 past the bound and fails it by name, with the per-flow delivery floor still
green at `1.000` — so the level bound is what fails, not a side effect. The
probe's magnitude varies with the arm's own rate ramp under the added delay:
one recorded run read p99 `883.8 ms` (`p99 883.8 ms exceeds its
452.0 ms ceiling`) with per-flow p99 `731.2/848.2/883.8/785.4` — every flow
**inside M4's own `900 ms` guard**, so M4's arm would pass while this arm
fails, which is exactly the region between the old guard and the new ceiling —
and a later invocation of the same probe read up to `1314.6 ms`, past both
bounds. The pair of readings is the honest statement: the fault always fails
this arm, and it can fail it in the guard-gap region no other gate names. `M4_HOSTILE_LEVEL_slow`
(`+300 ms`) drives it to **2085.8 ms** (still delivery `1.000`); it is
**composite** and declared as such: at `+300 ms` it lengthens the round trip
enough to slow the arm's own rate ramp as well as the one-way hop, so its
reading is a floor on what the probe costs and not an isolated measurement of
the one-way delay.

**The panel: why the new ceiling is not drawn on it, and what the panel
says.** The new bound is on the same `hostile_p99` series the `M4-latency`
panel draws, so the question is whether the panel owes it a line. It does not,
and the decision is recorded rather than left implicit. The panel's drawn bound
is still the declaration's 250 ms ceiling, and the tool draws the run's own
`hostile_p99_guard=900` as a second line labelled `governs series
hostile_p99`; the crossing of the 250 ms ceiling by the hostile bars is
attributed to that guard, so the panel shows a pass under the arm's own guard
rather than an unattributed breach. Drawing the level bound as a third
line is not a one-line declaration change: this tool's `series_guard_bounds`
draws a per-series guard line only under a **single** declared bound, and
adding a second declared bound suppressed the 900 ms line and made the render refuse itself (the trial bound was 455 ms; the tool reported `its label names the guard 900, but the artifact draws 2 bound line(s) at [249.9, 455.1] and none of them is that guard`) — measured, by
rendering `M4-latency` with the bound added. Relaxing that precondition lives
in `netem-tools mandate-plot`, outside this crate, and the M4 panel is
M4's arm's evidence while the level bound belongs to a different arm; drawing
it there would imply M4's run asserts it. The panel therefore keeps
**attributing** the crossing (the brief's second branch) and the level gate is
read from this arm's own `[m4-hostile-level]` line and this section. The
discrepancy between the drawn `900 ms` guard and the asserted `452 ms` level is
stated here so a reader cannot mistake one for the other.

**Cost and coverage.** Default tier (not `#[ignore]`d) beside M4, and it takes
the smoke set's shared `SERIAL` guard so its wall-clock measurement never
overlaps another arm's. Measured cost **15.5 s** on the 12 s window (`15.52 s`
by the two vacuity runs and the green run, `wall_s=15.5`), the window plus
`GRACE` plus ~1.5 s of connection and stream setup; `--quick` selects the 8 s
window. Cells provided:
`M4-hostile-level@flows=4+impairment=hostile+metric=p99-ceiling`,
`M4-hostile-level@flows=4+impairment=hostile+metric=no-starvation`,
`M4-hostile-level@instrument=degenerate-percentile`. Each cell varies **one**
declared dimension from a stated baseline, M4's own hostile arm
(`M4@impairment=gilbert-elliott-5-8+jitter=100ms+lane=dual+flows=4+shape=cadence+load=none+metric=per-flow-share`
in `tools/mandate-arms.json`): the flows, impairment, lanes, shape and load are
held fixed and what the arm adds is the `p99-ceiling` level (and the
`no-starvation` read that keeps a level met by starving a flow from counting);
the `degenerate-percentile` cell is the instrument sanity, not a bound. Cells
deliberately **not** covered, with the reason: the fair-latency spread (on the
hostile link the per-flow p99 differences are a GE loss realization, not a
scheduler property — M4's own arm records a spread up to `2.93x` and asserts
M1's absolute guard there instead), the four-flow clean level (the separate
clean arm above), and a flow count other than four (the sink attributes
samples by first-byte tag, whose range caps at seven flows; four is the
production shape).

## Tiers

- **default** — not `#[ignore]`d, so a plain `cargo test -p rtp_mux` runs it.
  Every scenario here is seeded (deterministic impairment). The mandate-2
  constitution gate (`jitter_duallane_constitution_gate`) is a ~40 s
  wall-clock dual-lane run whose offer and delivery quantities are
  deterministic counts and whose non-degradation bound has ~4× headroom over
  the measured ~26 ms p99, so it still belongs in the gate that always runs;
  the crate's own
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
  `netem-tools check-gate` fails with the scenario name, its file, and the
  token if one does. It must also not reach an assertion through a helper: the
  checker derives the crate-local call-graph closure of every `perf` scenario
  and requires every asserting helper it reaches to be declared report-only in
  the `gate-perf-guard-helpers` block.

## Default tier (runs in `cargo test -p rtp_mux`)

The mandate-2 interactive constitution gate
(`rtp_mux_jitter::jitter_duallane_constitution_gate` — the offered schedule,
`delivery == 1.000`, and the non-degrading p99, over the seeded dual-lane
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
in this tier; `netem-tools check-gate` fails if one is re-`#[ignore]`d or
removed.

```gate-default-required
hol_probe::fec_gaming_treatment_has_bad_path_and_large_capacity_headroom
hol_probe::fec_saturated_pair_keys_loss_to_the_same_rtp_sequence
perf_probe::controller_fat_pipe_has_only_fixed_shaping
perf_probe::deterministic_iid_loss_fat_pipe_is_fixed_seeded_iid_loss
rtp_mux_jitter::jitter_duallane_constitution_gate
mandate_smoke::m1_interactive_tail_latency
mandate_smoke::m2_offered_load_latency
mandate_smoke::m3_bulk_goodput_fraction
mandate_smoke::m4_interactive_lane_fairness
mandate_smoke::m4_clean_lane_p99_ceiling
mandate_smoke::m4_hostile_lane_p99_ceiling
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
birth_liveness::a_birth_is_not_killed_by_a_spike_scale_gap_but_still_times_out_beyond_its_budget = full
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
mandate_smoke::m1_lone_tail_loss_model = full
mandate_smoke::m1_lone_tail_rung_distribution = full
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
spike_survival::a_field_magnitude_latency_spike_is_survived_without_a_reconnect = standard
spike_survival::a_floor_link_keeps_the_session_and_its_stream_usable = standard
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
mandate_smoke::m1_lone_tail_loss_model
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
birth_liveness::a_birth_is_not_killed_by_a_spike_scale_gap_but_still_times_out_beyond_its_budget
mandate_smoke::m1_interactive_tail_latency
spike_survival::a_field_magnitude_latency_spike_is_survived_without_a_reconnect
spike_survival::a_floor_link_keeps_the_session_and_its_stream_usable
mandate_smoke::m1_lone_tail_rung_distribution
mandate_smoke::m2_offered_load_latency
mandate_smoke::m3_bulk_goodput_fraction
mandate_smoke::m4_interactive_lane_fairness
mandate_smoke::m4_clean_lane_p99_ceiling
mandate_smoke::m4_hostile_lane_p99_ceiling
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

## The env-scaled opt-in surface

Seven surfaces in this crate are scaled from the process environment rather
than from an `#[ignore]` set, so no `gate-manifest` tier, no
`gate-default-required` line and no `gate-perf-design` row can see them: the
same green arm runs a 4 s window or a 15 s one, three reps or thirty, a 300 s
soak or a 30 s one, depending on variables the caller sets. They are declared
in the `gate-env-tier` block below, which is also what makes them visible to
the checker: a name this crate's sources read from the environment and no
declared surface names is an error, and a declared name the sources never
read is a stale declaration.

All seven rows are **scriptless** (`-`). Not one of these names is set by a
script of this crate — the checker's `_crate_scripts` finds no file at all
in the script suffixes it scans (`.sh`/`.py`/`.nu`/`.js`/`.ts`/…) under the
crate root — and the runner that does set two of them, the harness's
`netem_test/tools/mandate-check`, is not a script
of this crate. The checker's runner field can only name a script of the crate
it checks, so `-` is the honest record here and the prose names the external
runner instead.

Every name below is resolved from the sources, not from a list written here,
and this crate spells its reads all four of the ways the checker resolves: a
literal handed straight to `env::var` (`tests/mandate_smoke.rs:230,253,272`,
`tests/dynamic_contested.rs:49`, `tests/hol_probe.rs:2377`,
`tests/perf_probe.rs:212,221,230,263,338,502,617` and
`:831,840,849,861,900,1058`, `tests/mux_stream_fairness.rs:43`), a literal
forwarded through a crate-local helper that hands its parameter to
`env::var` (`rtp_longrun`'s `env_u64`/`env_usize`/`env_string` at
`tests/rtp_longrun.rs:107,114,119`, `mux_stream_fairness`'s `env_u64` at
`:42`, `perf_probe`'s `parse_flag_env` closure at `tests/perf_probe.rs:276`),
and a name spelled once as a per-file `const` alias and handed to `env::var`
by that alias (`tests/cold_connection.rs:60,653`,
`tests/spike_survival.rs:122,125`).

### The smoke set's own tier and sink: `MANDATE_SMOKE_QUICK`, `MANDATE_CHECK_DIR`

`MANDATE_SMOKE_QUICK` (`tests/mandate_smoke.rs:230`) is the smoke set's own
window tier: `1` takes the shortest window per arm — `QUICK_WINDOW` 4 s,
`RR_QUICK_WINDOW` 5 s, `QUICK_BULK_WINDOW` 2 s — and unset takes `WINDOW`
12 s, `RR_WINDOW` 15 s, `BULK_WINDOW` 6 s, for the same four mandate arms. It
is what `tools/mandate-check --quick` sets. It selects a window *set* rather
than sizing a count: the same four mandate measurements run either way, so
`4 * MANDATE_SMOKE_QUICK` would be arithmetic the surface does not do, and
this row carries no load.

`MANDATE_CHECK_DIR` (`:253`) names the directory the arms' evidence is
written to — `M1/M2/M3/M4.json` and `.csv` — and is what the runner always
sets so the evidence lands outside the target tree; a plain
`cargo test -p rtp_mux` falls back to `target/mandate-smoke` under
`CARGO_TARGET_TMPDIR`. It is a diagnostic output sink, and it sizes no count
either.

With no load field on this row, the surface's measured cost is recorded here
instead: the target's own wall, which this file records three times —
142.48 s with `m1_latency_window_censoring` skipped, 142.59 s and 143.18 s
with it, against 142.60 s on the parent revision, so the arm pays no
measurement of its own and reads the series the other arms already produce.
The per-arm costs the smoke set declares elsewhere are its `gate-perf-design`
rows (M3's 108 s and the four `m1_lone_tail_*` rows); M1's, M2's and M4's own
walls are still the recorded `cost@metric=wall-clock` gap, which is why no
per-arm shape is stated here.

### The battery's repetition count: `DYN_REPS`

`DYN_REPS` (`tests/dynamic_contested.rs:48`, read at `:275,404,564,719,896,1051,1069,1366,1425,1586,1766,1919`)
is the repetition count of every arm in the dynamic-packet-size
latency/bulk battery: each of the twelve `dynamic_contested` arms runs its
scenario `DYN_REPS` times (default 3) with a fresh seed per rep (the arm's
own base offset plus the rep index, e.g. `100 + seed_base` at `:166` and
`200 + seed_base` at `:1324`), drawing 200 B latency messages at a 25 ms
cadence with 1-in-16 bursts of 4–64 KiB against 64–512 KiB bulk chunks over
one shared 400 KiB/s bottleneck, and reports per-message one-way latency
percentiles and bulk goodput as the median over reps. Reps, not tests, are
the independent draws: the twelve arms are twelve scenarios and their seeds
advance with the rep index. The rep's own *length* is the harness kit's
`DYN_RUN_SECS` (15 s, `netem_test/netem-test/src/kit/contested.rs:13`), read
in the kit rather than in this crate's sources, so it is not a variable of
this surface and the load does not name it.

The load's `wall` is one `--ignored --test-threads=1` run of the target at
the default shape: 12 tests, all passing, `finished in 582.34s` (real
587.3 s), with per-test stamps from 45.32 s to 50.38 s. That is 36 rep
executions of a 15 s scenario window, so the measured wall fits the
`12 * DYN_REPS` the row states. The total is **derived** — arithmetic over
the surface's one variable, and over no measured quantity.

### The mux egress fairness long run: `MUX_FAIR_*`

`tests/mux_stream_fairness.rs`'s `mux_stream_fairness_longrun` reads six
names (`:337-341`, `:411-412`), and they are the only reads in the target:
the `full`-tier `mux_stream_fairness_sweep` uses the module constants
`WARMUP`/`STEADY` instead.

- `MUX_FAIR_WARMUP_SECS` (default 10) and `MUX_FAIR_STEADY_SECS`
  (default 240) size the run: the post-join ramp that is discarded, and the
  measured window that is binned. They are the surface's two cost keys.
- `MUX_FAIR_WINDOW_SECS` (default 10, floored at 1, `:339`) is the bin
  width of the steady window's per-stream byte-delta series, so it sets the
  *number* of Jain samples a run yields, not its duration.
- `MUX_FAIR_REPS` (default 1, `:341`) repeats both arms on a fresh seed
  (`500 + rep * 7`).
- `MUX_FAIR_STREAMS` (default 8, clamped to 1..=16, `:411`) is the stream
  count of each arm. It sizes each arm's volume — more streams, more streams
  held backlogged — but not its duration, because the shaped link and the
  steady window are both time-bounded; it is recorded in the load as a shape
  value and does not appear in the total.
- `MUX_FAIR_CHUNKS` (default unset, `:412`) is a comma-separated chunk-size
  list for the mixed arm. It is a selector, not a count, so it cannot be a
  load factor at all: the total below does not name it.

The row's `total` is the shape's own duration `2 * MUX_FAIR_REPS *
(MUX_FAIR_WARMUP_SECS + MUX_FAIR_STEADY_SECS)` = 500 s of nominal arm-run
time (two arms × one rep × 250 s), and it is **derived** rather than
measured: the arithmetic is the surface's own variables and the measured
`wall` is the run below. The two numbers are recorded separately because one
is arithmetic over the shape and the other is a measurement, not because they
are expected to differ: each arm also pays a few tens of milliseconds of
setup (server spawn, rtp connect, mux pairing) and overshoots its steady
phase by less than one `MUX_FAIR_WINDOW_SECS` bin, and the two largely
cancel.

Measured: one `--ignored --exact mux_stream_fairness_longrun
--test-threads=1` run at the default shape reported 1 passed,
`finished in 500.10s` (real 502.2 s), so the wall landed 0.1 s above the
500 s the row's arithmetic yields. The run yielded 24 windows per arm — 48 in
all, the denominator behind the row's `bound` — with the homogeneous arm's
whole-run Jain 1.0000, worst window Jain 0.9993 and worst minimum share
0.1208, and the mixed arm's whole-run Jain 1.0000, worst window Jain 1.0000
and worst minimum share 0.1246, and no stream's whole-run total at zero in
either arm.

### The multi-minute long-run arms: `RTP_LONGRUN_*`

`tests/rtp_longrun.rs`'s two `full`-tier arms read five names
(`longrun_duallane` at `:665-668`, `multiflow_duallane` at `:678-681`, and
`RTP_LONGRUN_LOSS_PCT` inside the shared `run_longrun` at `:368`):

- `RTP_LONGRUN_SECS` (default 300 for the single-flow arm, 180 for the
  multi-flow one) is the measurement window and the surface's cost key.
- `RTP_LONGRUN_INTERVAL_SECS` (default 10, floored at 1) is the CSV sampling
  interval, so it sets how many `[longrun-iv]` rows the run yields:
  `RTP_LONGRUN_SECS / RTP_LONGRUN_INTERVAL_SECS` of them.
- `RTP_LONGRUN_STREAMS` (default 1 / 4) is the interactive stream count
  sharing the lane. Like `MUX_FAIR_STREAMS` it sizes volume, not duration.
- `RTP_LONGRUN_LOSS_PCT` (default 2, `:368`) selects the independent-loss
  regime — `0` separates a multi-flow queueing tail from a loss-repair tail —
  and so selects a regime rather than a size.
- `RTP_LONGRUN_LABEL` (default `base` / `multiflow`, `:119`) is the CSV label
  suffix, a report selector.

The load records the **reference arm**'s default shape: the single-flow arm
at `RTP_LONGRUN_SECS=300`, `RTP_LONGRUN_INTERVAL_SECS=10`,
`RTP_LONGRUN_STREAMS=1`, i.e. `total=RTP_LONGRUN_SECS` = 300 s of nominal
measurement window — a **derived** count, arithmetic over the surface's own
cost key and over no measured quantity — with the multi-flow arm's 180 s
default recorded here in prose rather than folded into one total. Its `bound`
is the rule of three over the
30 `[longrun-iv]` samples the reference shape yields (`3 / (300 / 10) =
0.1`).

Measured: one `--ignored --test-threads=1` run of the target stamped
`longrun_duallane` ok in 304.565 s and `multiflow_duallane` ok in 184.550 s
(both passed, `finished in 489.11s`), so each arm's wall is its own
`RTP_LONGRUN_SECS` plus the same ~4.6 s of ramp, grace and teardown. The
reference arm's run reported delivery 1.0000 over 11 999 samples with p50
22.70 ms and p99 28.59 ms; the multi-flow arm reported delivery 1.0000 over
28 796 samples across four streams with p99 91.33 ms, its bulk lane's
goodput byte-comparable to the single-flow arm's (0.6739 against
0.6740 MiB/s), which is the reading the multi-flow arm exists to produce.

### The hostile perf probes' regime: `NETEM_PERF_*` and `RTP_RTX_DUP`

`tests/perf_probe.rs`'s two `full`-tier hostile probes read these names, and
they read them with **different defaults**, so the surface is split into one
row per arm rather than one row over two different "default shapes":

- `probe_hostile_goodput_30s` (`:212-338,502,617`) defaults
  `NETEM_PERF_WINDOW_SECONDS` to 30, `NETEM_PERF_WARMUP_SECONDS` to 20,
  `NETEM_PERF_LINK_PROFILE` to `hostile`, `NETEM_PERF_MSS_BYTES` to
  `LOOPBACK_MSS` and `NETEM_PERF_SEED` to 4.
- `probe_hostile_message_latency` (`:831-900,1058`) defaults the same window
  to 30 but the warmup to 5, the profile to
  `hostile-periodic-bottleneck-300ms` and the MSS to 1400.

`NETEM_PERF_WINDOW_SECONDS` and `NETEM_PERF_WARMUP_SECONDS` are the two
sizing keys: the window is the time-boxed measurement and the warmup is the
ramp discarded before it, so the goodput arm's shape is `30 + 20 = 50` s and
the message arm's is `30 + 5 = 35` s. Both totals are **derived** from the
surface's own variables; the two `wall`s are the costs this file already
declares for those rows (51 s and 40 s), measured from those rows' own libtest
stamps, so neither number is invented here. The message probe's accepted
profile list (`:849-856`) is a **subset** of the goodput probe's much longer
one (`:230-260`), which is why the two rows still share the name: the same
variable has two accepted value sets, and each row records the set its own
arm validates against.

The rest are selectors and carry no arithmetic:

- `NETEM_PERF_MSS_BYTES` sets the transport MSS the probe dials with, and
  `NETEM_PERF_SEED` the c2s draw (`s2c_seed = c2s_seed + 1` at `:341` and
  `:903`): a treatment and a reproducible draw, not a count.
- `NETEM_PERF_REVISION` (`:502`, `:1058`) is a string stamped into the
  `PerfTrace` artifact as the run's own revision label. It is report
  metadata.
- `NETEM_PERF_DIAGNOSTIC_MODE=1` (`:617`) is the one control here that
  **weakens a check**: when the median sub-window goodput is below
  `HOSTILE_GOODPUT_FLOOR_MIB_S` it prints the bypass line and skips the
  assertion instead of failing. It is a diagnostic escape hatch, not a fault
  injection — it perturbs no input — and the row gives it a cell of its own
  so that a run taken with it is visibly a run without the floor. It is read
  only by the goodput arm.
- `NETEM_PERF_FEC` and `RTP_RTX_DUP` (`:286`, `:884`) select the *treatment*
  of the paired FEC/armor arms: whether the data packet is wrapped in the FEC
  envelope, and whether a recovery send gets a duplicate wire copy. Both are
  parsed strictly (`0`/`1`/`false`/`true` only, anything else panics), so a
  typo cannot silently flip a treatment on a timed run. `RTP_RTX_DUP` is
  **not** a fault injection: it is `rtp`'s own production behaviour toggle,
  and `rtp/GATE.md`'s `reliability-path-defaults` row classifies it the same
  way (a behaviour an explicit per-connection argument overrides, sizing no
  measurement). It is declared here too because this crate's `parse_flag_env`
  closure hands its parameter to `env::var`, so this crate does read it.

### The red-proof fault selectors

The four remaining names are the **vacuity injections**, and they are not
measurements: each perturbs an arm's *input* — the impairment or the offered
payload — so that the failure a red-proof demonstration shows is produced by
the measurement path rather than by the assertion. None sizes anything, so
this row carries no load: a `total` over these names would be invented
arithmetic, and the grammar has no line for one. They belong to four
different arms:

- `MANDATE_SMOKE_FAULT` (read by `mandate_smoke::fault`) selects a perturbation
  of one smoke arm's input: `M1_latency`, `M1_IMPAIRED_slow`,
  `M1_FIELD_RTT_slow`,
  `M1_LOSS_MODEL_uncorrelated`, `M1_LOSS_MODEL_correlated`,
  `M2_delivery`, `M2_offer`, `M3_starve`, `M4_starve`, `M4_late`, `M4_drop`,
  `M4_CLEAN_LEVEL_double` and `M4_CLEAN_LEVEL_slow`. Unset in every real
  run — the runner never sets it. The value's prefix (`starts_with(mandate)` in
  `fault`) names the arm whose input is perturbed, and each arm's own
  matcher then tests the value it owns; an arm that does not own the selected
  value leaves its input alone (`_ => {}` in `mandate_arms`). `M2_offer` is
  M2's offer floor made falsifiable: it multiplies the clean arm's cadence
  interval by ten, so the lane is offered a tenth of the throughput the mandate
  names — the one clause that can fail. The two `M4_CLEAN_LEVEL_*`
  values name the four-flow level arm's own namespace so that probing *its*
  assertion cannot read as probing M4's; `M1_IMPAIRED_slow` (one fixed 300 ms
  one-way delay on the `hostile` and `lone_tail` links, clean arm untouched) is
  the red proof of the deployed-baseline bound above.
- `HOL_PROBE_FAULT` (`tests/hol_probe.rs:2377`) selects the two offering
  faults the concurrent frame-delivery arms' red proof uses: `serialize`
  (only the first flow may offer until its window closes) and `throttle`
  (every other flow offered at an eighth of the cadence).
- `SPIKE_SURVIVAL_FAULT` (`tests/spike_survival.rs:125`) selects one of
  `no_spike` (the injection is skipped, so the delay-matches-injection check
  fails on a 191 ms round), `churn_session` (`RtpMuxConnector::reset()`
  mid-spike) and `late_churn` (the reset lands after every round has passed,
  so only the session-identity guard can catch it).
- `RTP_MUX_COLD_CONNECTION_FAULT=serialize`
  (`tests/cold_connection.rs:653`) replaces the production cold-connect arm
  with a reproduction of the pre-fix critical path (two sequential bare rtp
  dials plus the mux pairing), which the birth gate must reject.

```gate-env-tier
tri-mandate-smoke-tier = MANDATE_SMOKE_QUICK,MANDATE_CHECK_DIR | - | the tri-mandate smoke set's own window tier and evidence sink rather than a load knob: MANDATE_SMOKE_QUICK=1 selects the shortest window per arm (4 s interactive cadence, 5 s request/response, 2 s saturating bulk) for the same four mandate arms and is what `tools/mandate-check --quick` sets, unset selects the 12 s / 15 s / 6 s tier, and MANDATE_CHECK_DIR names the directory the M1/M2/M3/M4 JSON and CSV evidence is written to (always set by `tools/mandate-check`, defaulting under target/mandate-smoke for a plain `cargo test`); neither variable sizes a count, so no arithmetic derives from this surface and its load is refused rather than invented | smoke-mandate@mandate=M1+arm-set=clean-and-hostile-and-lone-tail+metric=p99-and-over250-share, smoke-mandate@mandate=M2+arm-set=clean-and-hostile-and-lone-tail+metric=offer-and-delivery-and-latency, smoke-mandate@mandate=M3+arm-set=saturated-bulk+metric=capacity-fraction, smoke-mandate@mandate=M4+arm-set=four-flow+metric=no-starvation-and-share, smoke-evidence@artifact=mandate-json-and-csv, smoke-window-tier@knob=MANDATE_SMOKE_QUICK+window=quick-or-full
dynamic-contested-battery = DYN_REPS | - | the dynamic-packet-size latency/bulk battery's per-arm repetition count: each of the twelve `dynamic_contested` arms runs its scenario DYN_REPS times (default 3) with a fresh seed per rep, drawing 200 B latency messages at a 25 ms cadence with 1-in-16 bursts of 4-64 KiB against 64-512 KiB bulk chunks over one shared 400 KiB/s bottleneck, and reports per-message one-way latency percentiles and bulk goodput as the median over reps; the rep's own length is the harness kit's `DYN_RUN_SECS` (15 s), read outside this crate's sources, so it is not a variable of this surface | dyn-size-latency@arm-set=static-classification+metric=small-and-burst-p50-p99, dyn-size-bulk@metric=goodput-fraction, dyn-size-migration@arm-set=migrating-variants+metric=latency-and-goodput, dyn-size-reps@metric=rule-of-three+unit=rep | DYN_REPS=3,total=12*DYN_REPS,wall=582.34s,bound=8.3e-2/rep
mux-fair-longrun = MUX_FAIR_WARMUP_SECS,MUX_FAIR_STEADY_SECS,MUX_FAIR_WINDOW_SECS,MUX_FAIR_REPS,MUX_FAIR_STREAMS,MUX_FAIR_CHUNKS | - | the mux egress fair-queue long-run fairness measurement: one mux session over one rtp connection through a fixed-rate seeded NetemPair carrying MUX_FAIR_STREAMS bulk logical streams whose per-stream delivered bytes are counted by peer tag, with the MUX_FAIR_WARMUP_SECS post-join ramp discarded and the following MUX_FAIR_STEADY_SECS sampled in MUX_FAIR_WINDOW_SECS bins, reporting the windowed Jain index, the slower stream's minimum share and a per-stream starvation check, over two arms (homogeneous 64 KiB chunks, and a mixed-chunk arm set by MUX_FAIR_CHUNKS) repeated MUX_FAIR_REPS times on a fresh seed; MUX_FAIR_CHUNKS is a chunk-size list rather than a count, so it sizes nothing and is not a load factor | fair-window@metric=jain-window+scale=long-run, fair-share@metric=min-share+scale=long-run, byte-fairness@arm=mixed-chunk+metric=jain, starvation@metric=per-stream-delivered-bytes-zero, fair-rate@metric=rule-of-three+unit=window | MUX_FAIR_WARMUP_SECS=10,MUX_FAIR_STEADY_SECS=240,MUX_FAIR_WINDOW_SECS=10,MUX_FAIR_REPS=1,MUX_FAIR_STREAMS=8,total=2*MUX_FAIR_REPS*(MUX_FAIR_WARMUP_SECS+MUX_FAIR_STEADY_SECS),wall=500.1s,bound=6.3e-2/window
rtp-longrun-arms = RTP_LONGRUN_SECS,RTP_LONGRUN_INTERVAL_SECS,RTP_LONGRUN_STREAMS,RTP_LONGRUN_LOSS_PCT,RTP_LONGRUN_LABEL | - | the multi-minute long-run arms' measurement window and CSV sampling cadence, over the production dual-lane composition: an aggregate interactive p50/p99/max and offered/forwarded wire series plus per-stream rows and the sender controller state, sampled every RTP_LONGRUN_INTERVAL_SECS across a RTP_LONGRUN_SECS window with RTP_LONGRUN_STREAMS interactive streams sharing the lane, RTP_LONGRUN_LOSS_PCT selecting the independent-loss regime and RTP_LONGRUN_LABEL naming the CSV run; the load records the single-flow reference arm's default shape, and the multi-flow arm's 180 s default is prose in the section above rather than folded into one total | longrun-drift@shape=cadence+scale=multi-minute+metric=p50-p99-max-series, longrun-fairness@arm=multi-flow+metric=per-stream-p99, longrun-bulk@scale=multi-minute+metric=goodput, longrun-repair@metric=fec-and-rtx-breakdown, longrun-rate@metric=rule-of-three+unit=interval | RTP_LONGRUN_SECS=300,RTP_LONGRUN_INTERVAL_SECS=10,RTP_LONGRUN_STREAMS=1,total=RTP_LONGRUN_SECS,wall=304.57s,bound=1.0e-1/interval
hostile-goodput-probe = NETEM_PERF_WINDOW_SECONDS,NETEM_PERF_WARMUP_SECONDS,NETEM_PERF_LINK_PROFILE,NETEM_PERF_MSS_BYTES,NETEM_PERF_SEED,NETEM_PERF_REVISION,NETEM_PERF_DIAGNOSTIC_MODE,NETEM_PERF_FEC,RTP_RTX_DUP | - | the time-boxed counting-sink goodput window over the hostile link profiles: the window and its discarded warmup are the sizing pair, the profile selects the link (validated against the goodput probe's own list), the MSS and seed select the dial and its reproducible draw, the revision labels the PerfTrace artifact, NETEM_PERF_DIAGNOSTIC_MODE=1 makes the median sub-window goodput floor informational rather than asserted, and NETEM_PERF_FEC and RTP_RTX_DUP select the paired arms' FEC envelope and retransmission-armor treatment; the reported quantity is the median of the sub-window goodputs against the goodput floor, with every delivered byte verified by the sink | hostile-goodput@shape=counting-sink+window=30s+metric=median-subwindow-goodput, hostile-integrity@metric=every-delivered-byte-verified, probe-diagnostic-bypass@knob=NETEM_PERF_DIAGNOSTIC_MODE+effect=floor-becomes-informational, probe-treatment@knob=NETEM_PERF_FEC+mode=fec-off-or-on, probe-treatment@knob=RTP_RTX_DUP+mode=armor-off-or-on | NETEM_PERF_WINDOW_SECONDS=30,NETEM_PERF_WARMUP_SECONDS=20,total=NETEM_PERF_WINDOW_SECONDS+NETEM_PERF_WARMUP_SECONDS,wall=51s
hostile-message-latency-probe = NETEM_PERF_WINDOW_SECONDS,NETEM_PERF_WARMUP_SECONDS,NETEM_PERF_LINK_PROFILE,NETEM_PERF_MSS_BYTES,NETEM_PERF_SEED,NETEM_PERF_REVISION,NETEM_PERF_FEC,RTP_RTX_DUP | - | the sparse-message one-way latency probe over the periodic hostile bottleneck profiles: 64 B timestamped messages every 100 ms for NETEM_PERF_WINDOW_SECONDS after a NETEM_PERF_WARMUP_SECONDS ramp, with netem sampled every 50 ms and a four-second straggler allowance, reporting one-way latency p50/p95/p99 and frame delivery; the profile is validated against the message probe's own periodic-only list, and NETEM_PERF_FEC and RTP_RTX_DUP select the paired arms' treatment as above | hostile-message@shape=timestamped-messages+window=30s+metric=one-way-p50-p95-p99, hostile-message-delivery@metric=delivered-frame-count, probe-treatment@knob=NETEM_PERF_FEC+mode=fec-off-or-on, probe-treatment@knob=RTP_RTX_DUP+mode=armor-off-or-on | NETEM_PERF_WINDOW_SECONDS=30,NETEM_PERF_WARMUP_SECONDS=5,total=NETEM_PERF_WINDOW_SECONDS+NETEM_PERF_WARMUP_SECONDS,wall=40s
vacuity-fault-selectors = MANDATE_SMOKE_FAULT,HOL_PROBE_FAULT,SPIKE_SURVIVAL_FAULT,RTP_MUX_COLD_CONNECTION_FAULT | - | the red-proof input perturbations, not measurements: MANDATE_SMOKE_FAULT selects one smoke arm's perturbed input (a slowed or re-shaped link, a starved or dropped flow, or an offer cut to a tenth of the cadence), HOL_PROBE_FAULT the concurrent arms' offering serialization or throttle, SPIKE_SURVIVAL_FAULT the skipped spike, the mid-spike session churn or the late churn, and RTP_MUX_COLD_CONNECTION_FAULT the pre-fix sequential birth surrogate; each exists so the arm it perturbs can be shown to fail from the measurement path, each is unset in every real run, and none sizes anything, so this row's load is refused rather than invented | smoke-vacuity@fault=MANDATE_SMOKE_FAULT+targets=M1-M2-M3-M4, offer-vacuity@fault=HOL_PROBE_FAULT+targets=concurrent-offer-floor, spike-vacuity@fault=SPIKE_SURVIVAL_FAULT+targets=session-identity, birth-vacuity@fault=RTP_MUX_COLD_CONNECTION_FAULT+targets=dual-lane-birth
```

## Opt-in targets outside this manifest

`netem-tools check-gate` covers only the rtp_mux scenario targets. The crate's
own non-scenario targets (`bidirectional`, `duplex`, `explorer`, `lane_rejection`,
`session_stats`, `xsession` and the `support/**` plumbing) run in the default
tier and are not gated as scenarios. The harness crate has its own gate
(`netem_test/tests/GATE.md`) and keeps the perf-loop lane-role authority.