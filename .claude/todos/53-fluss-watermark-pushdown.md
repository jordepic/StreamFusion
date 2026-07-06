# 53: Regenerate the pushed watermark in the native Fluss source

## Why

A watermarked Fluss table currently falls back entirely (`FlussTables.plan` declines any
`scan.requireWatermark()`), so no event-time query runs natively off Fluss — and the Nexmark
matrix's Fluss rung skips the windowed queries (q5/q7/q8/q11) for want of a time attribute.
The stock half is already free: Fluss's `FlinkTableSource` implements
`SupportsWatermarkPushDown` and the Fluss catalog persists `WATERMARK` specs
(`CatalogPropertiesUtils.serializeWatermarkSpecs`), so a `WATERMARK FOR dateTime AS ...`
clause in the benchmark's Fluss DDL works on stock Flink today. Only our native source —
which replaces the scan the watermark was pushed into — needs to regenerate it, exactly the
problem the native Kafka source already solved (ticket 33; readme "regenerates the table's
watermark per partition").

## Plan — reuse the Kafka machinery, which is already connector-agnostic

Everything hard exists; this is mostly moving it out of the `kafka` namespace:

1. **Spec parsing**: `planner/KafkaWatermarkSpec` reads the planner's `WatermarkPushDownSpec`
   ability, not anything Kafka — bounded out-of-orderness over a direct rowtime column or
   `TO_TIMESTAMP_LTZ(col, 3)`, declining on-event emit / alignment / `SOURCE_WATERMARK()`.
   Rename/share it (e.g. `SourceWatermarkSpec` naming is taken by Flink — pick carefully) and
   use it in `FlussTables.plan` instead of the blanket decline: null → unwatermarked (today's
   path), UNSUPPORTED → fallback with the reason, else carry the spec. The Fluss rowtime is
   the simplest case — a physical plain-`TIMESTAMP` column (`RexInputRef`); mind the
   re-indexing against the pushed projection (`withRowtimeIndex`, as the Kafka build does).
2. **Strategy**: `kafka/NativeKafkaWatermarks` is a pure `ArrowBatch` max-rowtime generator —
   reuse as-is (share it; nothing Kafka in it), passed by `NativeFlussSourceExecNode` in place
   of `noWatermarks()`. `StreamPhysicalNativeFlussSource.requireWatermark()` returns true when
   a spec is present (mirror `StreamPhysicalNativeKafkaSource`).
3. **Per-batch max rowtime**: add `maxRowtimeMillis` to `NativeFlussRecord` and emit it as the
   record timestamp (mirror `NativeKafkaRecordEmitter`'s null-sentinel handling). The native
   side computes it per batch: reuse `kafka.rs`'s `max_rowtime_millis`, but (a) it lives behind
   the `kafka` cargo feature — move it somewhere shared — and (b) the Fluss reader normalizes
   timestamps to `Timestamp(ns)`, so it needs a nanosecond arm (÷1_000_000 to millis).
   Idleness: same option precedence helper (`scan.watermark.idle-timeout`, then the global
   `table.exec.source.idle-timeout`).
4. **Tests**: the Kafka watermark tests are the template — a harness e2e where a windowed
   query over a watermarked Fluss table fires windows mid-stream (native vs stock row-parity),
   plus the spec-shape decline cases (on-event emit, alignment, `SOURCE_WATERMARK()`).
   Update `docs/coverage-and-fallbacks.md` in the same commit (the Fluss watermark decline
   entry becomes shape-conditional, matching the Kafka wording).

## Benchmark follow-through (the actual q5/q7/q8/q11 unlock)

Declare the `WATERMARK` in the matrix's Fluss DDL and unskip q5/q7/q8/q11. One trap: the
count-N target is calibrated on the **bounded** generator, whose end-of-input flushes the
final windows — an **unbounded** Fluss run never reaches end-of-input, so that N would hang
the rung. Fix by preloading one sentinel event after the real rows with an `event_type`
outside 0/1/2 (visible to the table's watermark, filtered from every person/auction/bid view)
and a far-future `dateTime`: the watermark passes every real window end, all windows fire, and
the bounded-generator N is reached exactly.

- q12 stays skipped: a proctime window's output count is wall-clock-dependent, so no
  deterministic N exists (CLAUDE.md's non-determinism carve-out).
- q13 may be unskippable **today** with no code change — `PROCTIME()` comes from a view over
  the source and its lookup-join output count is deterministic (one row per bid); verify and
  trim the skip set if so.

## Related cleanup (from the PR-3 review follow-up)

The partition-removal plumbing diverges from Fluss's own shape without a reason: Fluss keeps
the "which splits belong to the removed partitions" question **in the split reader**
(`FlinkSourceFetcherManager.removePartitions` enqueues one task; `FlinkSourceSplitReader
.removePartitions(Map)` answers from its subscribed-bucket state and returns the buckets to
ack). Ours instead grew an `assignedSplits` map in `NativeFlussSourceReader` plus a custom
`removeSplitsAndAck` two-step in the fetcher manager — duplicated split bookkeeping the JNI
reader already holds natively (`split_ids`). Aligning to the Fluss shape deletes the reader-side
map and the two-phase ack task. Mechanism is pinned by the harness drop tests, so refactor
under green.
