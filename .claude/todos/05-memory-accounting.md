# Account native memory against Flink's MemoryManager — remaining operators

**Status:** partially done — the mechanism shipped for the aligned-window aggregate family
(tumbling/hopping/cumulative, one- and two-phase). This ticket tracks extending it to the rest.
**Source:** research findings §4 (open question #1)

## What shipped (see divergences/16)
The transformation declares an operator-scope `ManagedMemoryUseCase.OPERATOR` weight
(`streamfusion.memory.operator-weight-mb`, master switch `streamfusion.memory.accounting.enabled`);
the operator reserves the computed fraction from Flink's `MemoryManager` at open and releases at
close (`ManagedMemoryBudget`); the budget crosses JNI once at handle creation and natively becomes a
bounded DataFusion `GreedyMemoryPool` + `MemoryReservation`, resized per batch from incrementally
tracked state bytes (touched groups only). Exceeding it throws `NativeMemoryLimitException` with the
remedy in the message — restore included, so a snapshot that no longer fits fails at restore, not as
a container OOM.

## Remaining
Extend the same pattern (budget param on create/restore + a tracked reservation + throw-on-exceed;
remember the drop-batch-before-throw JNI rule) to the other stateful natives, roughly by state size:
1. **Joins** — interval, window, temporal, updating (buffer both sides; the largest states). Their
   match runs as a DataFusion `HashJoinExec` over buffered batches — build their per-operator
   `SessionContext`/`RuntimeEnv` with the same bounded pool so the join's own working memory is
   accounted too (Comet's `RuntimeEnvBuilder::with_memory_pool` wiring).
2. **Non-windowed GROUP BY aggregate** and **changelog normalize** (live key maps).
3. **OVER aggregation** and **event-time sort / dedup** (buffered rows until watermark).
4. **Top-N** (bounded per-partition buffers — small, but cheap to add).
5. **Session windows** (`SessionAggregator` — same shape as the shipped aligned-window path).
6. **Native file/decode sources** — DataFusion scan streams; pool via `RuntimeEnv` as in comet.

Also:
- The **Java-side Arrow allocator** (`NativeAllocator.SHARED`) is transient per-batch and
  refcounted, deliberately outside the budget (the allocator is not the accounting seam — see the
  class comment). Revisit only if transpose buffering ever becomes long-lived.
- **Spill gap** stands: a denied reservation is a hard failure because streaming operators have no
  runtime spill; the structural answer is disaggregated/cached state (ticket 37).

## Acceptance criteria (for each operator picked up)
- Create/restore take the budget; state growth resizes the reservation; close releases it.
- Exceeding the budget fails with `NativeMemoryLimitException`, covered by a test.
- The accounted-path micro-bench shows no meaningful regression vs. unaccounted.

## Pointers
- Shipped implementation: `ManagedMemoryBudget` / `NativeManagedMemory` (Java),
  `with_memory_budget` / `accumulate_grouped` delta tracking (`native/src/lib.rs`), divergences/16.
- Comet `native/core/src/execution/memory_pools/` and `CometTaskMemoryManager.java` for the
  RuntimeEnv wiring of DataFusion-executed fragments.
