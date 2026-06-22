# Columnar watermark assignment matches Flink's deterministic per-row dropping

**Kind:** behavioral — when the event-time watermark advances relative to the
rows it governs, and which late rows are dropped.
**Status:** matched for Flink's deterministic behavior; the only residual is
Flink's own non-deterministic periodic emission (see below).

## Flink's behavior
Flink's table `WatermarkAssignerOperator` runs the watermark generator **per
row**: each record updates the running max and emits eagerly when the watermark
jumps past the auto-watermark interval. So within a run of rows the watermark can
advance *between* two rows, and a later row whose window that watermark already
closed is dropped as late (at the downstream window, against the watermark it has
received — at parallelism > 1 the min across its input channels).

## What we do
The columnar assigner is per-batch by default, but to match the per-row late-data
dropping it **slices a batch at each point its running watermark jumps past the
interval** and emits `(sub-batch, watermark)` in order — replicating Flink's
eager emission. A row that a sub-batch's watermark closes out therefore arrives at
the window *after* that watermark (across the shuffle, where Flink takes the min
across channels), and the native aggregator **drops rows whose window has already
closed** (`window_end <= current_watermark`, the highest flushed watermark). So
the deterministic late-drop is reproduced byte-for-byte — verified end to end by
`FlinkColumnarWindowSqlHarnessTest.outOfOrderWithinBatchDropsLateRowLikeHost`.

The drop is done in the aggregator (post-shuffle), not the assigner, because at
p > 1 a window's effective watermark is the min across its input channels, which a
single assigner can't know — only fine-grained watermarks flowing through the
shuffle reproduce it. That is why the assigner must slice rather than the window
filter locally.

A **monotonic-rowtime batch can have no within-batch late row** (a later row's
window can't be closed by an earlier, smaller rowtime), so it takes a fast path:
the whole batch is forwarded with a single watermark, no slicing. In-order data —
every benchmark, the common case — therefore pays nothing, and the windowed
benchmark is unchanged at 1.91×.

## Residual: Flink's non-deterministic periodic emission
Besides the eager rule, Flink also emits on a 200 ms **processing-time** timer.
For out-of-order data near a window boundary, that makes **Flink itself
non-deterministic** — two runs of the same job can drop different rows depending
on wall-clock timer firing. We replicate only the deterministic eager path, so we
match Flink whenever the eager rule decides the drop (the bounded / in-burst case,
where the processing-time timer doesn't fire mid-data). The genuinely
timing-dependent case is unmatchable in principle, because there is no single
"correct" Flink answer to match.

## Scope
- The watermark *value* (`max(rowtime) - delay`, floored at 0, MAX at end of
  input) matches Flink exactly (`NativeColumnarWatermarkAssignerOperatorTest`).
- Slicing reproduces the eager per-row emission; the aggregator's
  `window_end <= current_watermark` drop reproduces the late-data discard. Both
  the slicing (unit test) and the end-to-end drop (parity test) are covered.
- The aggregator's `current_watermark` is carried in the snapshot metadata, so
  late-dropping survives checkpoint/restore.
