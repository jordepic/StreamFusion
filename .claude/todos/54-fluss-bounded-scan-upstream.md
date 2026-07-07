# 54: Contribute `scan.bounded.mode` to apache/fluss, then collapse the Fluss rung's harness

## Why

The Fluss Flink connector has no bounded read for log tables — no `scan.bounded.mode`, no
stopping-offset surface (bounded reads exist only via the lake source in batch mode and the
pushed-`LIMIT` RPC). That absence is the entire reason the Nexmark Fluss rung needs its
special machinery: the count-to-Nth-row cancel with a bounded-generator calibration, the
far-future watermark sentinel that closes the final event-time windows, and the poison-marker
finish line for the queries whose changelog cardinality is non-deterministic (q4/q9) or zero
(q21). It is also the only reason q12 cannot be measured on this rung at all: a proctime
window flushes early only at end-of-input, which no data row can fake, and only a bounded
source can signal.

## The upstream contribution

Kafka-shaped `scan.bounded.mode = 'latest-offset'` (and plausibly `timestamp`) for log-table
scans. Every ingredient already exists upstream:

- `OffsetsInitializer.latest()` resolves per-bucket end offsets (it backs
  `scan.startup.mode = 'latest'`) — the bounded read resolves the same offsets at enumerator
  start and stamps them as **stopping** offsets instead.
- `LogSplit` already carries `stoppingOffset` (used by the hybrid snapshot→log handoff), and
  the reader's finished-split machinery already terminates on it.
- The enumerator already diffs/assigns per-bucket splits; boundedness is one more field at
  split construction plus `Boundedness.BOUNDED` on the source.

Our side needs nothing new: `NativeFlussSource` reuses Fluss's enumerator verbatim (a bounded
assignment flows through unchanged), and `NativeFlussSplitReader` + `native/src/fluss.rs`
already implement the stopping-offset path (immediate-finish for empty ranges, batch slicing
at the stop, finished-split reporting) — currently exercised only by tests.

## When it lands in a Fluss release

- Re-point the benchmark's Fluss rung at the Kafka rung's method: both engines read bounded
  (`scan.bounded.mode = 'latest-offset'`), the job ends by itself, and the measurement is
  end-to-end time — identical semantics to every other rung.
- Delete the rung's special machinery: `flussTargetRows` calibration, the watermark sentinel
  row, the traced table + poison pair + marker sink, and the skip-reason for q12 — all 23
  runnable queries then measure uniformly, q12 included.
- The stock baseline needs the released connector version; do not ship a patched
  `fluss-flink` jar (unlike the fluss-rs cargo pin, a Maven fork is not a self-contained
  build).
