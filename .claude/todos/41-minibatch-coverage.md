# Mini-batch coverage: IncrementalGroupAggregate + the two-phase tails

**Status:** TODO. Prioritized 2026-07-03 (tier 2 of the coverage push — the most common real-world
fallback trigger after the Kafka gate).

**Why now.** Production Flink deployments routinely enable mini-batch
(`table.exec.mini-batch.enabled` + size/latency); it changes the plan shapes we see, and several of
those shapes fall back today, dragging whole queries to the host via the all-or-nothing gate:

- **Distinct aggregates under mini-batch: the DEFAULT shape is DONE (2026-07-05).** Plan-probing
  corrected the premise: without `table.optimizer.distinct-agg.split.enabled` (default off), a
  distinct aggregate plans as ordinary `LocalGroupAggregate → Exchange → GlobalGroupAggregate`
  with a distinct MapView partial — NOT as `IncrementalGroupAggregate`. That default shape now
  runs native: the local's bundle set rides a trailing (value, count) list column and the global
  merges it with multiplicities (COUNT over the set-carriable types, SUM over bigint/int).
  **Remaining: the opt-in split chain** — `PartialLocal → Exchange(keys+bucket) →
  IncrementalGroupAggregate → Exchange(keys) → FinalGlobal` over a `Calc` computing
  `MOD(HASH_CODE(x), 1024)`. Two viable designs, decision pending: (a) faithful replication (a new
  stateful incremental operator + native `HASH_CODE` parity for the bucket Calc — without it the
  Calc itself falls back); (b) chain-lowering — match the five-node pattern and lower it onto the
  no-split native pair (byte-identical final values; the bucket key never changes results; record
  in `divergences/` since it drops the skew-spreading topology the user opted into). The split is
  skew mitigation for Flink's per-key MapState; measure whether our DistinctSet even needs it
  before building (a).
- **Two-phase `AVG`: DONE (2026-07-03).** The local expands an AVG into a widened-sum state plus a
  COUNT over the same column (its two positional partials), the matchers walk partials with
  per-aggregate offsets, and the global folds the pre-summed `(sum, count)` pair into the ordinary
  AVG state via a per-aggregate count-partial column — the divide/truncate/cast-back emit is the
  single-phase code, byte-identical. Parity-tested keyed + global, mixed with single-partial
  aggregates, negative values included. Scope matches the two-phase set (bigint/int/double).
- **Wider two-phase value types: DONE (2026-07-05).** AVG admits the full AvgAggFunction family
  (smallint/tinyint/float included; the sum partial widens to bigint/double, the emit casts back) —
  the single-phase set, which for SUM/MIN/MAX remains bigint/int/double. Fixing this surfaced and
  fixed a single-phase bug: the group aggregator's value-column downcast lacked the narrow arms, so
  admitted AVG(SMALLINT/TINYINT) folded zeros and AVG(FLOAT) panicked the native library.
- **Two-phase decimal `SUM`/`AVG`: mini-batch pair DONE (2026-07-05).** SUM's partial rides as
  `DECIMAL(38, s)` (bundle overflow → NULL partial, skipped by the SUM merge and latched NULL by
  the AVG merge — verified against the host), MIN/MAX keep `DECIMAL(p, s)` through the extremes
  multiset, AVG merges the `(DECIMAL(38, s), bigint)` pair into the exact-division emit. Remaining:
  the **windowed** local/global split — the native windowed local emits single-field partials
  (matching Flink's one-column accumulators), but our decimal accumulator keeps a two-field
  `(sum, count)` state, so decimal (and int/narrow, whose wrapping accumulators are also two-field)
  stay gated to bigint/double there; carrying them needs split-specific single-field partial
  accumulators with skip-NULL merge (Flink's own merge expression).
- **Row-time mini-batch: DONE (2026-07-05).** The native assigner now has both modes: proc-time
  (markers from the clock) and row-time (upstream watermarks filtered to the interval, mirroring
  Flink's `RowTimeMiniBatchAssginerOperator` including the end-of-input flush).

**Acceptance:** each shape parity-verified against the host (the existing two-phase harness tests
are the template), `docs/coverage-and-fallbacks.md` updated in the same commit, and a Nexmark run
with mini-batch enabled to confirm routed fractions improve.

- **Two-phase FILTER clauses: DONE (2026-07-05).** Plain filters gate every local fold (the
  boolean column the Calc materializes; the merge is filter-blind), filtered distinct instances
  each get a native view/set per (args, filter) pair (identical final output to Flink's shared
  bitmask view), and string MIN/MAX partials ride the split — q15/q16/q17's tuned plans engage
  fully natively.
- **Retraction-bearing partial layouts: DONE (2026-07-05).** The local subtracts -U/-D rows
  (COUNT/AVG only — their accumulators are layout-invariant under retraction), the appended or
  reused count1 partial is modeled positionally, and the global drives per-key liveness from the
  merged count1 (-D + state drop at zero, Flink's RecordCounter semantics) — q4's tuned plan
  engages fully natively. SUM/MIN/MAX/DISTINCT under retraction still decline (extra accumulator
  fields / monotonicity exemptions / per-filter view counts).

## The tuned-Flink benchmark column: SHIPPED 2026-07-05

`NexmarkMatrixBenchmark#tunedMiniBatchMatrix` (SF_MATRIX_TUNED, 5M events so flush latency
amortizes; distinct split stays default-off — skew mitigation for parallelism, and its chain has
no native path). First results (docs/benchmarks.md carries the table): q23 2.81×, q19 2.59×
(net-diff Top-N, divergences/20), q9 1.93×, q18 1.74×, q20 1.27×, q3 0.71× (its untuned residual
at scale, not a mini-batch cost — the run exposed and fixed an unpruned-transpose tax first);
q4/q15/q16/q17 fall back per the two gaps above, which are now the ticket's remaining work.
