# Memory profiling + leak detection for the native side

**Status:** partially done — the standing close-time leak audit shipped; what remains is metrics
export, the heap-profiling recipe, and a soak test.
**Source:** requested alongside the memory-accounting work (shipped — divergences/16) — accounting
tells Flink how much we hold; this ticket is about *verifying* those numbers are honest and that
nothing leaks.

## Shipped: every test is now a leak test

The native side counts live JNI handles by type (every create increments, every close decrements),
exposed to Java as a breakdown string; the shared test-cluster extension asserts after every test
that the breakdown is empty and that the shared Arrow FFI allocator holds zero bytes (imports
register as foreign allocations, so a dropped C Data release callback in either direction shows up
too). A failure names the leaking handle type and the outstanding byte count; rerunning with
`-Dsf.extraJvmArgs=-Darrow.memory.debug.allocator=true` dumps a creation stack per outstanding
buffer. Comet only logs a warning on the same condition (pool `Drop` / iterator close); tests can
afford to fail hard.

The first full-suite audit caught a real leak class: Flink drops in-flight records at task teardown
with no close hook, so a failing job abandoned its queued source batches — off-heap buffers leaked
on every failover for the TaskManager's lifetime. Fixed with a `Cleaner` backstop on the columnar
stream record: a batch collected without any consumer having taken its root closes the root once
unreachable; taking the root disarms the backstop so buffers in use are never freed (see the
`ArrowBatch` javadoc for the ownership contract).

## What remains

1. **Allocator/pool introspection as Flink metrics.** Surface the native footprint — the shared FFI
   Arrow allocator, the per-operator pool's reserved bytes, and the live-handle counts — as Flink
   operator metrics so a running job's native memory is visible in the Flink UI/metrics reporter
   next to its JVM numbers. The JNI surface now exists for handles; pools still need a getter.
2. **Native heap profiling recipe.** A documented, repeatable way to profile the native heap under a
   real job: jemalloc/`heaptrack`/Instruments (macOS) or `valgrind --tool=massif`/ASAN+LSAN (Linux,
   CI-friendly) against the Nexmark suite, including how to get symbols from the `bench` release
   build. Written up in `docs/` so any contributor can run it. This covers what the allocator checks
   cannot: Rust-side heap growth inside a live handle (a state map that never shrinks).
3. **Soak test.** An opt-in long-running job (like `SF_BENCHMARK=true`) over a keyed stateful
   operator with an unbounded key stream + eviction, asserting RSS/pool usage plateaus rather than
   grows without bound — the class of leak short tests can't see.

## Acceptance criteria

- Native usage (allocator + pool, per operator) is queryable from Java and exported as Flink
  metrics.
- A `docs/` page describes the heap-profiling workflow and it has been run once against Nexmark,
  with findings (or a clean bill) recorded.
- A soak test exists that would have caught an unbounded-state or dropped-release leak.

## Pointers

- divergences/16 (memory accounting) — the pool this ticket introspects; shipped for every
  stateful native operator.
- Comet's memory-pool stats and Arrow Java's allocator leak detection for prior art.
- `.claude/todos/20-profiling-and-benchmarks.md` item 2 (native timing/counter hook) — same
  feature-flagged native introspection surface; consider building them together.
