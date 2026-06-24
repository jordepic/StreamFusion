# Non-blocking integration with the task mailbox

**Status:** DECIDED + locked in — stateful operators run synchronously on the mailbox
(no async bridge), now guarded by a checkpoint-interleaving test. The async pattern's
only worthwhile unlock is a native columnar source; lookup join / async UDF are
I/O-bound and don't fit a compute accelerator (see "What the async pattern unlocks").

## Decision
Run stateful native operators synchronously on the task (mailbox) thread, with
bounded per-batch work, exactly as Flink runs its own window/join/aggregate
operators. This is what our operators already do, so it is not a divergence. We
deliberately do **not** offload stateful native compute to another thread: it
has no Flink precedent, `AsyncWaitOperator`'s buffer-and-replay trick is unsafe
for stateful work (replay double-counts), and Arroyo's async-actor model only
works because Arroyo owns checkpoint coordination, which we delegate to Flink.

## Operators that WILL need non-blocking work (and the Flink pattern to use)
Add it only for these, when we build them — not for stateful compute:

- **Native sources** — implement Flink's Source API with availability futures so
  polling native/columnar input (e.g. Fluss, Iceberg/CDC) never blocks the
  mailbox. (Own ticket when we add a native source.)
- **Async lookup join / async table function** — stateless per-row async I/O
  enrichment. Use `AsyncWaitOperator`'s pattern: ordered in-flight queue,
  `MailboxExecutor.yield()`/`execute()`, snapshot-and-replay of in-flight inputs.
  (Arroyo's `lookup_join` is the logic reference.)
- **Async sinks** — if a native sink flushes asynchronously, use the sink
  writer's mailbox-based async flush.

## Where we are
Every native call runs synchronously on the Flink task (mailbox) thread. For
the current operators that is bounded per-batch CPU work that returns promptly,
so checkpoint barriers and timers already interleave correctly (the
checkpoint/restore test demonstrates this).

## What Flink does
- One mailbox thread per task. **Stateful operators — window aggregations,
  joins, group aggregates — run synchronously on it.** Flink has no mechanism to
  offload stateful operator compute to another thread; it relies on the work
  being bounded and incremental per record/batch.
- The one async pattern, `AsyncWaitOperator`, is for **stateless** async I/O
  enrichment: it buffers in-flight inputs in an ordered queue, uses
  `mailboxExecutor.yield()` while the queue is full and `mailboxExecutor.execute()`
  to emit results back on the task thread, and at checkpoint **snapshots the
  in-flight inputs and replays them on restore**. Replay is safe only because
  the function holds no state.
- Sources keep the mailbox responsive via availability futures rather than
  blocking.

## What Arroyo does
Every operator (stateful or not) is an autonomous async task with a
`tokio::select!` loop over input channels and control messages. Checkpoint
barriers travel **in-band** (Chandy-Lamport) and are handled inside the loop
(process barrier, snapshot state, forward barrier). Backpressure is the bounded
channels. There is no shared mailbox — each operator owns its own scheduling.

## Conclusion: don't build an async bridge for stateful operators
Our native operators are stateful (windowed aggregation). Two facts fall out:

- Running them **synchronously on the mailbox is exactly what Flink does** for
  its own stateful operators, so our current design is already Flink-aligned —
  not a divergence.
- An async offload bridge would diverge from Flink with **no precedent for
  stateful operators**. `AsyncWaitOperator`'s buffer-and-replay trick does not
  apply, because our work mutates state (replay would double-count); we'd need a
  bespoke drain-in-flight-at-checkpoint mechanism Flink doesn't have. Arroyo
  runs stateful operators async only because it *owns* checkpoint coordination
  via in-band barriers — adopting that means re-implementing Flink's
  coordination, which contradicts "Flink is the control plane."

So the mailbox concern legitimately applies only where Flink itself goes async:

1. **Native sources** — implement Flink's Source API with availability futures
   so polling native input never blocks the mailbox.
2. **A future stateless native operator** doing async I/O — follow
   `AsyncWaitOperator`'s buffer + `MailboxExecutor` + snapshot-and-replay pattern.

For stateful native compute, keep it synchronous and bounded per batch (today's
design). Revisit only if a native operator needs to block on something other
than CPU.

## Follow-ups (if/when we add the above)
- ✅ **Lock in the current guarantee with a checkpoint-interleaving test.** Done —
  `NativeWindowAggregateOperatorTest.bufferedInputSurvivesCheckpointMidStream` snapshots
  with rows still buffered (a barrier landing mid-stream, before the window's watermark),
  restores into a fresh operator, and confirms the buffered rows survived and combine
  with post-restore rows. Proves snapshot-flushes-pending + synchronous-mailbox loses
  nothing across a checkpoint.
- Source-operator availability-future bridge (separate ticket when we add a
  native source).

## What the async pattern actually unlocks — and whether it's worth building
Ticket 1's `AsyncWaitOperator` pattern would enable three Arroyo operators. They are
**not** equally worth building for a *compute* accelerator:

- **Native columnar source** (availability futures) — **worth it.** A columnar source
  feeds a fully-native pipeline (no input transpose) and is throughput-relevant. This is
  the valuable async-gated work; it overlaps [ticket 24](24-columnar-endpoints-beyond-local-parquet.md).
- **Lookup join** (`lookup_join.rs`) and **async UDF** (`async_udf.rs`) — **low value,
  likely not worth building.** Both are *I/O-bound*: the lookup hits a JVM
  `LookupTableSource` (JDBC/HBase/…) and the async UDF calls an external function — the
  bottleneck is the external call, not row compute, and that I/O **cannot** move to Rust
  (it's a JVM connector). A "native" version would transpose RowData↔Arrow around a join
  concat / function call with no columnar compute to accelerate — pure overhead for zero
  throughput gain. By the project's own bar ("if benchmarks don't improve, reconsider the
  feature"), these should stay on the host. Build only if a concrete reason appears
  (e.g. the probe side carries heavy native compute that dominates the I/O).
