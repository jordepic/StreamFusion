# Share one FFI Arrow allocator across native operators (Comet's pattern)

**Status:** open.
**Source:** every native operator currently creates its own `RootAllocator` in `open()` and closes it
in `close()` (filter, calc, GROUP BY, Top-N, both joins, window/OVER, the two transposes, the keyed
split, the batch serializer — ~13 classes). datafusion-comet instead shares **one** long-lived
allocator for the whole execution process. This ticket aligns us with Comet's pattern; the file
sources already use a single shared allocator (`NativeFileArrowBulkFormat`) and are the seed.

## Why (Comet's rationale, which we should follow)
Arrow Java buffers are **reference-counted**: an exported/imported batch keeps the underlying buffers
alive until its refcount hits zero. If a *consumer* finishes after the *producing* operator's
allocator is closed, the allocator sees outstanding buffers and reports a (false) leak — i.e. throws
`IllegalStateException: Attempting operation on allocator when allocator is closed`. Comet avoids this
with a single process-wide `RootAllocator(Long.MaxValue)` (`CometArrowAllocator`, `package.scala`),
never closed during execution; buffers are reclaimed by refcount as batches are released downstream.

Per-operator allocators are safe **only** while every consumer is synchronous (a chained
`output.collect()` runs the downstream `processElement` to completion) or copied across the network
(the IPC serializer deserializes into the receiver's allocator). That invariant is implicit and
fragile — the async file source already violated it (its reader thread closed before the task thread
drained the batches), which is why the file sources moved to a shared allocator. Bringing all
operators onto one shared allocator removes the latent constraint and matches the reference impl.
(Memory note in [[comet-memory-model]].)

## What to change
- Promote the file sources' shared allocator into one process-wide holder (the StreamFusion analog of
  `CometArrowAllocator`) — a single long-lived `RootAllocator` (+ a `CDataDictionaryProvider`), never
  closed during execution.
- Replace each operator's per-instance `new RootAllocator()` / `allocator.close()` with imports/exports
  through that shared allocator. Drop the allocate-in-`open` / close-in-`close` lifecycle.
- Keep using the *input batch's* allocator for the C Data **export** (the established rule — the input
  was imported into the shared allocator anyway, so this collapses to the shared allocator too).

## Tradeoff (accepted, as Comet does)
- We lose per-operator **close-time leak detection** (Arrow asserts `allocated == 0` on `close()`).
  Comet deliberately accepts this. Optional mitigation: a debug/test-only switch that still uses
  per-operator allocators, or an assertion that the shared allocator's `getAllocatedMemory()` returns
  to its baseline at checkpoint/teardown, so a real leak is still caught in CI.

## Explicitly NOT this ticket
- **Memory accounting / limits** are orthogonal to allocator scope: that is a DataFusion `MemoryPool`
  with named per-operator consumers bridged to Flink's `MemoryManager` (Comet's
  `CometUnifiedMemoryPool` → `CometTaskMemoryManager`), tracked under memory accounting
  ([ticket 05](05-memory-accounting.md)). A shared FFI allocator neither helps nor hinders it.

## Reference-first
- datafusion-comet `CometArrowAllocator` (`spark/.../comet/package.scala`) and `NativeUtil`
  import/export; the `comet-memory-model` memory note.

## Acceptance
- One shared allocator; no per-operator `RootAllocator`. Full suite green (the synchronous operators
  behave identically; the change is purely allocator ownership). A leak-catch path remains in tests.
