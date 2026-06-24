# Keyed / event-time operators unlocked by columnar watermarks + shuffle

**Status:** open (roadmap) — infrastructure landed, operators not yet built.
**Source:** what the columnar watermark assigner + columnar keyed shuffle (both done;
divergences/10) + the generic window core unlocked.

## Why these are newly feasible
The columnar keyed exchange (carry Arrow across a `keyBy`, co-locating keys with
watermark propagation) + the columnar watermark assigner (event-time in the
columnar world) + `NativeWindowOperatorCore<OUT>` (a stateful op can emit Arrow
partials) together form the backbone for the whole **keyed + event-time +
append-only** operator family. Each below reuses the exact pipeline shape we
already run for windows: `columnar source → watermark assigner → columnar keyed
exchange → columnar keyed operator`.

Each line becomes its own ticket when picked up; parity-gated per the usual
discipline (matcher admits only what we reproduce exactly; verified against the
host by `NativeParity`).

## Directly unlocked — single-input, drop straight into the existing shape
- [~] **Event-time `OVER` aggregation** — DONE, **columnar**, for the default
      `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame, with or without
      `PARTITION BY` (bigint/int/string keys), SUM/MIN/MAX/COUNT/AVG over one
      bigint/int/double value column. The native `OverWindowAggregator` buffers input
      batches and, on a watermark, emits the completed rows with the running
      aggregate appended — input columns pass through, so it rides the columnar
      shuffle with no transpose (native source → wm assigner → columnar exchange →
      columnar OVER, parity-tested incl. partitioned over a Parquet source). AVG
      works via Flink's `$SUM0`+`COUNT` decomposition (divide on the host). The
      `$SUM0` = SUM mapping holds because an OVER frame is never empty.
      **Follow-ups (deferred):** bounded frames / `ROWS` — needs per-frame row removal
      (the frame slides, so rows leave the bottom): keep the frame's rows per key and
      recompute per row (works for MIN/MAX; retract is an optimization for SUM/COUNT/AVG
      only), plus watermark-driven eviction. A sibling operator, not an extension of the
      unbounded `OverWindowAggregator`. Also: proctime. Deferred in favor of joins.
- [ ] **Append-only deduplication** — keep-*first*-row
      (`ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt) = 1`). Keyed, insert-only.
      (Keep-*last* is retracting — changelog support now exists; tracked in ticket 11.)
- [ ] **Window Top-N** and **window deduplication** — built on the windowing-TVF +
      keyed-shuffle infra we now have; append-only.
- [ ] **Event-time sort** (`ORDER BY rt`) — watermark-driven, ~no shuffle;
      unlocked by the watermark assigner alone.

## Two-input columnar operator — built
The two-input columnar operator (two `ColumnarInput` edges; connected watermark = min of both
inputs) now exists (`NativeIntervalJoinOperator`). On top of it:
- [x] **Interval join** (`a.rt BETWEEN b.rt - X AND b.rt + Y`) — DONE, INNER, **columnar**: keyed
      shuffle on the join key + watermark-bounded state cleanup. See [divergences/12](../../divergences/12-joins-delegate-match-own-state.md).
- [x] **Window join** (windowing-TVF based) — DONE, INNER, **columnar**: buffer both sides, join per
      window on watermark close (the window bounds join the equi-keys). See [divergences/12](../../divergences/12-joins-delegate-match-own-state.md).

## Changelog operators — unblocked and largely shipped
These emit retracting/updating changelogs, which once needed changelog/`RowKind` support that now
exists (divergences/13, /14). The non-windowed `GROUP BY` aggregate, the regular (non-windowed)
INNER join, and append-only streaming Top-N are done; keep-last deduplication and temporal joins
remain, tracked in the coverage tracker (ticket 11).

## Pointers
- Pipeline shape + own-hash co-location: [divergences/10](../../divergences/10-columnar-exchange-own-hash.md).
- Per-row late-data parity (needed by any event-time op): [divergences/09](../../divergences/09-per-batch-watermark-assignment.md).
- Operator catalogue cross-reference: [ticket 11](11-arroyo-operator-coverage.md).
