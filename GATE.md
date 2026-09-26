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

The rows below declare five families in the `gate-perf-design` grammar:
`<row> = <tier> | <cost_s> | <relation> | <cell>[,<cell>…]`, each cell
`<property>@<dimension>=<value>[+…]`. The **default** family is the residual —
every cell name no `members.<family>` claims — and holds the pre-existing
`interactive-scaling` rows; the four named families (`constitution`, `fec`,
`frame-reorder`, `reorder`) each carry their own baseline and their own
cell-name namespace in `gate-budgets`, so a row's cells decide whether it
belongs to the family its relation names. The four namespaces are the ones the
pending declaration proposes for those families; keeping only the rows whose
cells actually carry that name is what turns family membership from a free
label into a property of the row.

Every member below is **orthogonal** to its family's baseline — exactly one
dimension away — so the declared subset is 100 % attributive: no composite (a
confounded arm cannot attribute a failure) and no re-measurement (a declared
duplicate). The budget block covers the rows declared here, not every row of
each tier: the remaining rows are either cell-mismatched or uncosted and are
recorded as gaps below, so the per-tier sum the checker reports is the declared
subset.

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
```

Declared sums are `default` 40 s, `standard` 41 s, `full` 191 s and `perf`
910 s of the 300 s, 600 s, 200 s and 1000 s budgets. The `standard` ceiling
keeps its 600 s and now carries the two cold-connection rows, its only
declared rows. The concurrent row is one
dimension (`offer`) away from the default baseline: the same four flows, the
same lane, the same seeds, the same offer, polled together instead of one
after another. The two `interactive-scaling` rows keep the costs they already
declared (66 s and 20 s, measured 69.1 s and 19.5 s). `full` was 86 s while the
block covered only that family and is 200 s now that the constitution p99 arm
is declared beside it; `default`, `perf` and `standard` are new lines, not a
retune of an existing number.

**Cost provenance.** No cost here is invented and none is measured by this
declaration: `jitter_duallane_constitution_gate` is the "~40 s wall-clock
dual-lane run" this file's Tiers section records, and the other seven are the
arm count in the row's own `#[ignore]` reason string times the ~35 s per-arm
wall-clock that string states — `three ~35 s dual-lane constitution runs` →
105 s, `five ~35 s arms` → 175 s, `six ~35 s frame+FEC arms` → 210 s,
`four ~35 s arms` → 140 s, `two ~35 s reorder arms` → 70 s. A row whose
wall-clock appears in no document is **not** given a number: it is recorded as
a gap below, so an unmeasured cost is visibly pending rather than plausibly
guessed.

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
full = 200
perf = 1000
baseline = hol_probe::hol_rtt100_ge5_four_interactive_frame_delivery
baseline.constitution = rtp_mux_jitter::jitter_duallane_constitution_gate
baseline.establishment = cold_connection::cold_connection_decomposition
baseline.fec = rtp_mux_jitter::jitter_fec_arms_2pct
baseline.frame-reorder = rtp_mux_jitter::jitter_frame_reorder_fec_arms
baseline.reorder = rtp_mux_jitter::jitter_reorder_direction
members.constitution = M*
members.establishment = cold-connection*
members.fec = fec-tuning*
members.frame-reorder = frame-reorder-fec
members.reorder = reorder-*
drift = 0.5
drift_floor_s = 2.0
```

The rest of the pending declaration is **not** declared, and each family below
records why and what would make it declarable. The granularity is the family,
not the row: a family whose rows' cells are not family-derivable is one gap
naming the repair, because refiling those rows *is* the repair — recording the
mismatched rows individually would be noise that hides the distinct blockers.
Two blockers account for all of them: a cell name two or more families claim,
and a wall-clock no document records. A **new arm closes neither** — a new arm
inherits the same cell name and is equally misfiled, and it needs its own cost,
the very thing missing — so no family here was closed by adding an arm.

```gate-coverage-gaps
cold-connection@lanes=dual+handshake=on+impairment=loss-or-jitter-or-reorder = the cold birth is measured on clean links only; what loss does to a birth is rtp's opening handshake's own retry behaviour (its `OPENING_TIMEOUT`/`RETRY_INTERVAL`), which the `rtp` crate owns and which a clean-link decomposition cannot attribute to rtp_mux, so no impaired birth row is claimed here.
cold-connection@lanes=dual+handshake=off = the same birth with the rtp opening handshake disabled is a *different* protocol configuration (the production server binds `handshake: true`); it is measured inside the row as the bare one-lane arm's no-handshake control (min 0.0-0.1 ms, i.e. the whole one-lane cost is the handshake), not as a separate row, because the deployed path never takes it.
cold-connection@lanes=single+shape=proxy-chain = the row measures the rtp_mux birth on loopback with no proxy in the path; the deployed chain's cold total (and the proxy's share of it) is measured by the proxy-path iteration that owns those arms, and loopback cannot speak to a real path's delay distribution, so no field-scale claim is made here.
cold-connection@metric=cpu = per-datagram CPU cost is measured by owning-symbol attribution (`tools/samply_hotspots.py`), not by a scenario in this crate.
cold-connection@lanes=dual+handshake=on+scale=multipath = the multi-path UDP transport (rtp's `mpudp`) has no rtp_mux birth arm; a cell for it belongs to the layer that owns that transport.
interactive-scaling@flows=2+offer=concurrent = the two-flow rung is the pre-existing serialized `hol_rtt100_ge5_two_interactive_frame_delivery` and stays as it is; the concurrent offer is declared at four flows, the rung this family and M4 name, so a two-flow concurrent row would repeat it at a smaller N without a new regime.
interactive-scaling@flows=4+offer=concurrent+bulk=saturating = the family's bulk-sharing member (`hol_rtt100_ge5_shared_frame_delivery`) is single-flow with a saturating bulk stream; adding a saturating bulk stream to the concurrent row varies two dimensions from the baseline at once and would need its own derivation for what the shared bottleneck does to the offer floor, so it is left to its own row.
interactive-scaling@flows=4+offer=concurrent+impairment=clean-or-GE1-or-hostile = the concurrent row is declared on the family's GE5 seed pair (31/32) only; the clean, GE1 and hostile rtt100 rows are single-flow arms, and a concurrent arm on those links would be stated against a different baseline family.
interactive-scaling@flows=8+offer=concurrent = the sink attributes samples by first-byte tag (A, C..H after the reserved `b'B'`), so seven flows is the tag range's limit and an eight-flow row has no per-flow attribution; the four-flow rung is the largest this instrument can measure.
attribution@baseline-family=interactive = `jitter_interactive_solo`, `_with_loss`, `_with_bulk` and `_bulk_and_loss` carry the cell names `M1`/`M2`, which `members.constitution = M*` claims, so their cells and the family they name disagree; the repair is to rename those four cells `interactive-cadence@…` and to narrow the constitution namespace to that family's own cells (`constitution-*`), after which `jitter_interactive_solo` is the reference and `_with_bulk` and `_bulk_and_loss` are one and two dimensions (load; load+metric) away. All four costs are already recorded (35/35/65/35 s).
attribution@baseline-family=m3-bulk = `mandate_smoke::m3_bulk_goodput_fraction`, `jitter_bulk_idle_restart_arm` and `bulk_lane_goodput_stays_above_capacity_fraction` carry `M3`, which is inside `M*`; the repair is to rename the three cells `m3-bulk@…` and to record the smoke arm's wall-clock, after which the smoke arm is the reference and the restart arm is one dimension (load) from it with `rate` stated once. Two of the three costs are recorded (45 s, 35 s).
attribution@baseline-family=latency-sweep = `jitter_latency_dimension_arms` and `jitter_shared_bottleneck_arms` carry `M1`, inside `M*`; the repair is to rename both cells `latency-sweep@…` and to record the shared-bottleneck arm's wall-clock, because until both are done the family has one citable row and a one-row family leaves `baseline.latency-sweep` a stale reference no row states against.
attribution@baseline-family=lone-tail = `jitter_request_response_arms` and `jitter_cellular_timeline_arms` carry `M1` while `jitter_nonloss_impairments` carries `non-loss-impairment`, so the family spans two cell names and neither is its own; the repair is to rename all three `lone-tail@…` (the request/response context) and to state the reference's `depth`, after which the members are one dimension (impairment) away.
attribution@baseline-family=decomposition = `jitter_frame_reorder_decomposition` carries `frame-reorder` and `jitter_decomposition` carries `loss-vs-queue`, so the family spans two names; the repair is to rename `jitter_decomposition`'s cell `frame-reorder@…` or to split the family, and because `jitter_decomposition` is two declared dimensions (arms, jitter) from its sibling, a single-axis decomposition arm beside it is the other half of the repair.
attribution@baseline-family=dual-lane = `hol_probe::dual_lane_asym_frame_delivers_and_tears_down` carries `hol-dual-lane` while `jitter_duallane_arms` carries `dual-lane-matched-load`, which `members.dual-lane = hol-dual-lane*` does not claim; the repair is to rename one of the two so both carry the family's own name, and `jitter_duallane_arms` is two dimensions (load, reorder) from the reference, so a single-axis dual-lane arm is the other half.
attribution@baseline-family=fairness = the proposed family spans five cell names (`M4`, `fairness-sweep`, `fairness-longrun`, `dual-lane-longrun`, `multi-flow-longrun`) across two tiers; the repair is to split it into the three contexts it measures (`fairness-sweep`, `fairness-longrun`, `dual-lane-longrun`) and to rename the M4 arm's cell `fairness-m4@…`, since the M4 arm is a four-flow fairness arm and the longruns are multi-minute measurements, not the same reference's members.
attribution@baseline-family=hol = the seven `hol_probe` families the pending declaration proposes (`hol-cap400`, `hol-ge5-shared`, `hol-ge5-solo`, `hol-rtt40`, `hol-shared-frame`, `hol-solo`, `hol-split`) draw every cell from the same `hol*` space, so no namespace separates them — those regimes differ in their dimensions' values, not in their cells; the repair is one cell name per regime (`hol-cap400@…`, `hol-rtt100-ge5@…`, `hol-rtt40-ge1@…`, `hol-rtt100-clean@…`, `hol-hostile@…`, `hol-frame@…`, `hol-fec@…`, `hol-paced@…`) plus the wall-clock each `#[ignore]` reason does not state, which is a change to those rows' declarations and to no window, seed or tier.
attribution@baseline-family=hostile-probes = the proposed family spans three cell names (`ceiling`, `contested`, `mux-over-rtp`) and two tiers; the repair is to keep only `probe_hostile_goodput_30s` and `probe_hostile_message_latency` in it and to state `contested_latency::contested_hostile` and `mux_over_rtp_perf::mux_over_rtp_400mib_hostile_perf` against their own families, which also needs their costs.
attribution@baseline-family=rtp-ceiling = `perf_probe::probe_rtp_echo_4mib_direct` and `_mss8k` share the cell name `ceiling` with the hostile probes; the repair is to narrow one of the two names (the hostile pair to `hostile-probe@…`) so the families are separable, and to record both costs, which no `#[ignore]` reason states.
attribution@baseline-family=mux-ceiling = `mux_ceiling_probe`'s echo and sink pairs all carry `loopback-ceiling`, so the proposed echo and sink families cannot both claim it; the repair is to split the name (`mux-ceiling-echo@…`, `mux-ceiling-sink@…`) and to record the four costs.
attribution@baseline-family=instrument-sanity = `hol_probe`'s FEC pair and `perf_probe`'s determinism pair all carry `instrument-sanity`; the repair is to narrow the name to one target and to record all four costs, which the `#[ignore]` reasons do not state.
attribution@baseline-family=mux-over-rtp = `mux_over_rtp_perf`'s lossy pair carries `mux-over-rtp` while `mux_over_rtp_small_stream_while_bulk_perf` carries `small-stream-while-bulk`; the repair is to rename the third cell so all three carry one name and to record the three wall-clocks, none of which appears in a document.
attribution@baseline-family=hol-verify4 = `hol_verify4::v4_clean_muxbulk` and `v4_ge5_muxbulk` are internally coherent (`bulk-lane-ab`) and one dimension apart, so only their costs are missing — the `#[ignore]` reason states no wall-clock; one measurement per row declares the family with no cell change.
attribution@baseline-family=fec-recovery = `hol_probe::hol_rtp_mux_fec_default_on_recovery` carries `hol-fec-recovery`, which no family above claims, and the pending declaration files it in the `fec` family four dimensions from that family's reference (fec, impairment, layer, loss); the repair is either a single-axis FEC-recovery arm beside the `fec-tuning` reference or a `fec-recovery` cell name with its own reference, plus the row's cost.
cost@metric=wall-clock = `mandate_smoke`'s four arms, the four `mux_ceiling_probe` rows, `perf_probe`'s two rtp-echo and two hostile rows, `contested_latency`'s three rows, `mux_over_rtp_perf`'s three rows, `hol_verify4`'s two rows and the 27 `hol_probe` rows have a coherent family or one repairable cell but no wall-clock in any document; each needs one measurement of its own tier's invocation — the tier's `--ignored` run, or a plain `cargo test --release -p rtp_mux --test <target> -- --exact <test>` for a default-tier row — before its family can be declared, because a cost the declaration invents is worse than a cost it records as pending.
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