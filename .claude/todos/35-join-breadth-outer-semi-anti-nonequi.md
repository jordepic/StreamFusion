# Join breadth: outer / semi / anti, and a residual non-equi predicate

**Status:** DONE. **Regular updating join** — INNER/LEFT/RIGHT/FULL/SEMI/ANTI + residual non-equi
predicate. **Interval and window joins** — INNER/LEFT/RIGHT/FULL + residual non-equi predicate (semi/anti
do not arise as time-bounded joins — Flink plans those as regular joins). All parity-tested against the
host; see the readme compatibility chart. Commits: "Add outer and semi/anti to the regular updating join",
"Apply a residual non-equi predicate in the regular updating join", "Apply a residual non-equi predicate in
the interval and window joins", "Add LEFT/RIGHT/FULL outer to the event-time interval join", "Add
LEFT/RIGHT/FULL outer to the event-time window join". The design notes below are kept for reference.
**Source:** the operator-coverage gap (ticket 11) — joins are the highest-value remaining breadth.

## Remaining: interval/window outer (LEFT/RIGHT/FULL) — design
The matched pairs already flow correctly via the incremental INNER DataFusion join (emit-as-they-match);
outer adds only the **unmatched** rows, emitted once at eviction. Append-only output (the null-pad is
emitted once when the row's window/interval closes and is never retracted), so parity is `assertParity`.

Why eviction-time is correct: an outer row is evicted only once the watermark has passed the point where
any future other-side row could still match it, so by eviction *all* its potential matches have been seen
(even if some were themselves already evicted). A per-row "has matched" flag is therefore final at
eviction. (Re-deriving matches from the surviving buffer at eviction does **not** work — matching rows may
have been evicted first.)

Mechanism — per-row match tracking via row-ids:
- Append a synthetic `__rowid__` (Int64, per-side monotonic) to each buffered row; keep a per-side
  `matched: HashMap<i64, bool>`.
- On each incremental join, carry both sides' row-ids through the DataFusion join (they ride as extra
  columns); read the output's `left.__rowid__`/`right.__rowid__` to set `matched[id] = true` on both sides,
  then project the row-ids back out of the emitted pairs.
- `advance`/`flush` (eviction): for rows leaving an **outer** side whose `matched` is false, emit the row
  null-padded on the other side; drop evicted ids from the map. `advance` must now **return** a batch (the
  null-pads) that the operator emits; today it returns nothing.
- Null-padding needs the other side's column types before that side's first batch — pass both input Arrow
  schemas at construction via the C Data Interface (as the updating join's `createUpdatingJoiner` does).
- Snapshot must persist the buffers' `__rowid__` column, the `matched` maps, and the id counters.

Files: `IntervalJoiner`/`WindowJoiner` (join type + rowid/matched + eviction emission + snapshot), the
create/advance/flush JNI, `NativeIntervalJoinOperator`/`NativeWindowJoinOperator` (emit advance/flush
output; pass schemas + join type), the matchers (relax the INNER gate, pass the join-type code), and the
physical/exec nodes. Parity: `assertParity` for LEFT/RIGHT/FULL over append inputs (data-stream and the
columnar Parquet path). Order: interval first, then window.

## Current state (what to extend)
Three native joins, all gated to **INNER** with **no residual non-equi predicate**:
- **Regular (non-windowed) updating join** — `NativeColumnarUpdatingJoinOperator` (+ the Rust updating-join
  in `native/src/lib.rs`), matched by `RegularJoinMatcher`. Emits/consumes a changelog. Per-side keyed
  multiset, probes natively (divergences/14), no degree bookkeeping (INNER needs none).
- **Interval join** (event-time) — `NativeIntervalJoinOperator`, `IntervalJoinMatcher`. Append-only; buffers
  both sides, delegates the match to a DataFusion `HashJoinExec`, evicts on watermark (divergences/12).
- **Window join** (windowing-TVF) — `NativeWindowJoinOperator`, `WindowJoinMatcher`. Same DataFusion-delegated
  shape, joins per window on watermark close (divergences/12).

Each matcher gates: `joinSpec.getJoinType() != FlinkJoinType.INNER → fall back`; `getNonEquiCondition().isPresent() → fall back`;
≥1 null-filtering equi key. To extend, relax those gates per the design below and pass the join type +
non-equi condition through to the operator.

## The key design fact (divergences/14)
**INNER needs no degree table; outer/semi/anti do.** RisingWave keeps a per-side keyed row set and, only for
outer/semi/anti, a per-row **match-degree** (count of associations on the other side) to know when to emit or
retract the null-padded row (outer) or the bare row (semi/anti). Our INNER join already has the per-side
keyed multiset; the extension adds the degree.

## Reference-first (consult before writing)
Implementation reference priority: **Arroyo > RisingWave > Proton**. Flink is the *parity target*, not the
implementation reference (its output is what we must match bit-for-bit, or fall back).
- **Arroyo (primary)** `~/data/arroyo/crates/arroyo-worker/src/arrow/join_with_expiration.rs` +
  `crates/arroyo-planner/src/plan/join.rs` — the time-bounded (interval/window) join: buffer both sides,
  delegate the whole join (type + non-equi `filter`) to a DataFusion `Join`/`HashJoinExec`, re-run per
  incoming batch. Mirror this for interval/window outer/semi/anti and for the residual non-equi filter
  (Arroyo passes `join.filter` straight to the DataFusion `Join`). Caveat: Arroyo rejects *updating* join
  inputs and its per-batch re-run does **not** retract stale null-pads — so it cannot match Flink's
  collapsed retract stream for the regular updating join. Use RisingWave there instead.
- **RisingWave (secondary)** `~/data/risingwave/src/stream/src/executor/hash_join.rs` +
  `join/{hash_join,row}.rs` — the regular updating join's degree state: a per-row `DegreeType` = count of
  matches on the other side, kept only when `need_degree_table` (outer/semi/anti); degree 0 ⇒ emit a
  null-pad. This equals Flink's `numOfAssociations`.
- **Flink (parity target)** `~/data/flink/.../operators/join/stream/`: `StreamingJoinOperator` (INNER +
  LEFT/RIGHT/FULL outer) and `StreamingSemiAntiJoinOperator` (SEMI/ANTI) — the exact emit/retract state
  machine the collapsed result must reproduce; `state/OuterJoinRecordStateViews` stores
  `(appear-times, numOfAssociations)` per distinct row.
- **divergences/14** — the degree-table rationale and our guest-state stance (state snapshots into Flink, in
  memory not paged).

## Plan, by family (each its own change + parity test)
1. **Regular updating join — outer/semi/anti** (highest value; the changelog join SQL hits most):
   - Add a per-row **degree** alongside the per-side multiset (count of current matches on the other side).
   - LEFT/RIGHT/FULL OUTER: when a row's degree goes 0→1, retract the null-padded row and emit the matched
     row(s); when it goes 1→0 (other side retracts), emit the null-padded row. Null-pad the absent side's
     columns. Honor input `RowKind` (the join already consumes a changelog).
   - SEMI: emit the left row on degree 0→1 (`+I`), retract on 1→0 (`-D`). ANTI: the inverse.
   - Mirror `StreamingJoinOperator` (outer) and `StreamingSemiAntiJoinOperator` (semi/anti).
   - Parity: `NativeParity.assertChangelogParity` per join type, over append-only and changelog inputs.
2. **Interval / window joins — outer/semi/anti** (more involved; time-based):
   - The matched part can still go through the DataFusion join with the right join type. The new work is
     emitting **unmatched** rows at the time boundary: an outer row with 0 matches when its window/interval
     is evicted by the watermark emits a null-padded row (append-only — emitted once, no retraction);
     semi/anti emit/suppress at eviction. This needs degree tracking against the buffered other side at
     eviction time.
   - Parity: `assertParity` (these stay append-only even for outer — the null row is emitted once at close).
3. **Residual non-equi predicate** (all three; can land alongside or after the types):
   - Updating join: the predicate gates whether a candidate pair is a "match" (so it feeds the degree and
     the emitted pair). Window/interval: pass it as the DataFusion `HashJoinExec` join filter (the interval
     bounds are already a residual filter there — generalize).
   - Parity-test each; an un-admitted predicate shape still falls back.

## Files to touch
- Matchers: `RegularJoinMatcher`, `IntervalJoinMatcher`, `WindowJoinMatcher` — relax the INNER/non-equi gates,
  pass `joinType` + the encoded non-equi condition (reuse `RexExpression`/the expression encoder for the
  predicate) to the physical node / exec node.
- Physical + exec nodes: `StreamPhysicalNativeColumnarUpdatingJoin` / `…IntervalJoin` / `…WindowJoin` and their
  exec nodes — carry the join type + predicate.
- Operators: `NativeColumnarUpdatingJoinOperator`, `NativeIntervalJoinOperator`, `NativeWindowJoinOperator`.
- Rust (`native/src/lib.rs`): the updating-join state (add degree + per-type emit), and the interval/window
  join (join type + boundary unmatched emission + non-equi filter).

## Suggested order
Regular updating join LEFT/RIGHT/FULL → SEMI/ANTI → non-equi predicate (updating) → interval/window
outer/semi/anti → interval/window non-equi. Re-run the full suite (now ~1 min) after each; gate anything not
bit-identical to the host and record divergences.
