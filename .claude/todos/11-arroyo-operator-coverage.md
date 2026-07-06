# Arroyo operator coverage — route everything Arroyo supports

**Status:** open (tracking) — all window aggregates, OVER (running + bounded frames, event-time and
proctime), interval/window joins (event-time and proctime, INNER/LEFT/RIGHT/FULL + residual), the
non-windowed `GROUP BY` aggregate (changelog emission *and* consumption, incl. MIN/MAX retraction),
the regular updating join (INNER/LEFT/RIGHT/FULL/SEMI/ANTI), Top-N (rank number, `OFFSET`, retracting
input), all four deduplication variants, window Top-N/dedup, event-time sort, filter/projection,
watermark, shuffle, and Parquet source/sink are done. Lookup join (sync + async) is done via the
within-batch model. What remains is async UDF (ticket 01 — low priority, pure I/O) plus the OVER
aggregate tail: `AVG`, `COUNT(*)`, and decimal value columns (the matcher declines them today).
**Source:** user direction — "everything Arroyo already supports, routed over"

Goal: reach Flink parity (identical results, verified by the parity harness) for
the operators Arroyo implements. Each line becomes its own change/ticket as it
is picked up. Operators are in `~/data/arroyo/crates/arroyo-worker/src/arrow/`.

## Aggregating windows
- [x] Tumbling aggregate (sum/min/max/count/avg), 0–1 int key
- [x] Two-phase tumbling (sum/min/max/count)
- [x] Multiple aggregates per window over one value column (one- and two-phase)
- [x] Hopping / sliding window — one-phase (multi-window assignment) and
      two-phase (slice-sharing, slide divides size). Reverse-engineered from
      Flink's slicing internals, not Arroyo (see divergences/06).
- [x] Session window (`session_aggregating_window.rs`) — dynamic merge on gap,
      including late elements that bridge two open sessions (ticket 14)
- [x] Cumulative window (`CUMULATE`) — one-phase; nested windows sharing a start.
      Net-new: Arroyo has no cumulative window (see divergences/05). Two-phase
      slice-sharing is done (divergences/06).
- [x] Wider value/key types: every non-decimal numeric value type, decimal MIN/MAX/COUNT,
      multiple value columns, `COUNT(*)`, and bigint/int/string/boolean/date/timestamp/decimal
      grouping keys (see `docs/aggregate-type-support.md`).

## Other stateful operators
- [x] Window functions / OVER (`window_fn.rs`) — running (UNBOUNDED PRECEDING), bounded-ROWS, and
      bounded-RANGE event-time frames, plus proctime running/bounded-ROWS; aggregates
      SUM/MIN/MAX/COUNT/FIRST_VALUE/LAST_VALUE over numeric value columns, and the window functions
      ROW_NUMBER/RANK/DENSE_RANK; incremental per-key state (divergences/11). Remaining: `AVG`,
      `COUNT(*)`, and decimal value columns (matcher declines). LAG/LEAD, FOLLOWING/descending
      frames, and multiple window groups are parity (Flink rejects them in streaming).
- [x] Interval/temporal join (`join_with_expiration.rs`) — event-time equi-join
      with an interval on the rowtimes; buffer + delegate the match to a DataFusion
      hash join, own watermark eviction (divergences/12). INNER/LEFT/RIGHT/FULL with
      outer null-pads at watermark eviction, plus a residual non-equi predicate. Proctime
      bounds are native too (clock-timed rows, processing-time-timer eviction). Done —
      no remaining tail. (Semi/anti are regular joins, not time-bounded.)
- [~] Instant join (`instant_join.rs`) — its main use, a windowed equi-join, is covered
      by our window join (INNER, on shared window bounds, divergences/12) via a hash
      join rather than a direct port. The general per-instant primitive is not ported.
- [x] Non-windowed group aggregation (`incremental_aggregator.rs`) — emits *and*
      consumes a retract changelog (`+I`/`-U`/`+U`/`-D`, matching the host's per-record
      `GroupAggFunction`). SUM/COUNT/MIN/MAX (AVG via the host's SUM/COUNT rewrite) over
      bigint/int/double; SUM/COUNT retract a running value, MIN/MAX a per-key value
      multiset (Arroyo's batch state). Any converter-supported keys, global aggregation.
- [x] Regular (non-windowed) join — updating equi-join, emits *and* consumes a
      changelog. Per-side keyed multiset probed incrementally (native, not DataFusion
      delegation — divergences/14). INNER/LEFT/RIGHT/FULL/SEMI/ANTI, equi-key, null keys
      dropped, plus a residual non-equi predicate (per-row match-degree, RisingWave's
      degree table). Done — no remaining tail.
- [x] Streaming Top-N (`ROW_NUMBER`) — append-only and retracting-input, rank ≤ N with an
      optional `OFFSET`, rank number optionally projected (the rank-shift cascade);
      per-partition buffer emitting the insert/delete changelog (divergences/14). Done — the
      one matcher decline is a non-constant (variable) rank range; `RANK`/`DENSE_RANK` are
      parity (Flink rejects them in streaming).
- [x] Deduplication — all four variants: rowtime keep-first (insert-only, watermark-released)
      and keep-last (retracting), and proctime keep-first/keep-last (arrival order, eager
      emit). A value-ordered rank-1 is a Top-N (handled separately). Done — no remaining tail.
- [x] Window Top-N / window deduplication — `WindowRank`/`WindowDeduplicate` over the windowing
      TVF: per window and partition key, keep the top-N (or first/last) rows and emit them when a
      watermark closes the window. Append-only; one native window-rank operator serves both
      (dedup = limit 1). window_start/window_end rendered session-local on emit (UTC internally).
- [x] Event-time sort (`TemporalSort`) — `ORDER BY rowtime`: buffer rows, release them in
      ascending rowtime order as the watermark advances (stable for ties). Insert-only.
- [x] Lookup join (`lookup_join.rs`) — stateless enrichment against an external table, sync and async
      connectors, INNER + LEFT. Arrow in/out, per-row `LookupFunction` (sync) or per-distinct-key
      concurrent `asyncLookup` awaited within the batch (async) — Arroyo's own within-batch model, no
      operator mailbox (divergences: see ticket 40 / ticket 01). Remaining: calc/residual on the dim.
- [ ] Async UDF (`async_udf.rs`) — async scalar UDF; the same within-batch-concurrent trick would apply
      if wanted (ticket 01), but it's pure external I/O with no compute to accelerate — low priority.

## Stateless
- [x] Filter + projection routed from SQL via the native expression engine — the
      planner's `Calc` node (optional filter + projections) and a broad function set
      (see `docs/coverage-and-fallbacks.md` §3 / divergences/07). Remaining tail:
      number↔string `CAST` and obscure functions.

## Connectors (later)
- [x] Parquet source + sink (local `file:`), exactly-once sink.
- [ ] Remote/columnar sources and sinks (Iceberg, `hdfs:`/`s3:`) — ticket 24.
- [ ] Native columnar sources beyond Fluss (Iceberg-CDC) via Flink Source API +
      availability futures — ticket 01. (The native Fluss log-table source is done.)

## Cross-cutting (do alongside, not after)
- Acceleration config (allowIncompatible / master / per-operator flags) — done (`NativeConfig`)
- Native columnar shuffle — done (divergences/10)
- Changelog/retract support — done (`RowKind` carriage + the GROUP BY, updating join, and Top-N
  emit/consume changelogs; divergences/13, /14)
