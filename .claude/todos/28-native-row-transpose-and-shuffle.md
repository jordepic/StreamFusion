# Native row→Arrow transpose (+ fused keyed shuffle) — investigated, rejected; row-major build kept

**Status:** closed. The native-decode idea was prototyped and **rejected on benchmark grounds**;
the measurement instead found a safe, no-divergence win (a row-major, pre-sized build) which shipped.

## What was proposed
Move the `RowData → Arrow` columnar build off the JVM (Arrow-Java `setSafe` per cell) to native
Rust (`arrow-rs` builders), the way `arrow-avro`'s `RecordDecoder` turns row records into Arrow: the
JVM does a cheap tight row encode, native decodes (and optionally splits by key in one fused pass).

## Why it was rejected (the benchmark, M1 Max, 4096-row mixed schema)
First, the reference check the rule demands: **DataFusion Comet builds Arrow on the JVM**, cell-at-a-
time (`CometArrowConverters` → `ArrowWriter.write(row)` → `vector.setSafe(i, row.getX(col))`), then
FFI-exports the `VectorSchemaRoot`. It has **no native row-decode path**. So shipping rows to native
is a divergence from our reference — it earns scrutiny, and the numbers killed it:

| Path | µs/batch |
|---|---|
| JVM tight encode (`RowBatchEncoder`) | 142 |
| Native decode (`RowBatchDecoder`, `cargo bench`) | ~96 |
| **Native path (encode + decode)** | **~238** |
| JVM build, reused/pre-sized vectors (honest baseline) | ~250–256 |

The native path only **ties** the honest JVM build, and the ~238 µs omits the JNI call and JVM-side
FFI import — include those and it loses. A native transpose is not worth a divergence from Comet for
no gain. (The encode + decode + round-trip prototype was reverted; only the benchmark survives.)

## What shipped instead (commit: row-major, pre-sized build)
Isolating where the build cost went exposed a structural inefficiency in our own converter, unrelated
to native vs JVM:

| Strategy | µs/batch |
|---|---|
| `RowDataArrowConverter.write` (was: column-major, grow-on-write) | 354 |
| row-major fill into pre-sized vectors (now) | 265 |
| pre-sized / reused vectors (theoretical best) | ~256–260 |

The converter walked the row list **once per column** (column-major) and let `setSafe` realloc as
each vector grew, plus allocated a `getFieldNames()` list per column. Rewriting `write()` to fill
**row-major** (one pass, every column per row) into **pre-sized** vectors took it 354 → 265 µs (~25%),
matching the theoretical best. Row-major is what Comet's `ArrowWriter` does, so this *aligns* with the
reference rather than diverging. Parity unchanged; shipped.

## Also rejected: reusing vectors across batches
Comet reuses its root (`reset()` + refill). Measured, reuse is **no faster than a pre-sized fresh
build** (the realloc, not the allocation, was the cost — and pre-sizing removes it). It is also unsafe
in our architecture: FFI export shares Arrow buffers by refcount, so resetting a root a downstream
native operator still holds (the joins/window aggs buffer batches) would corrupt it. No gain, real
hazard → not done.

## Out of scope / not pursued
- Fused `decode + split-by-key` shuffle — moot once native decode was rejected (the JVM still builds
  Arrow; the keyed split is already native via `partition_batch`).
- `BinaryRowData`/Kryo as the wire format — Kryo is Flink's slow generic fallback with no native
  decoder; `BinaryRowData` doesn't avoid an encode at the source boundary (rows are `GenericRowData`
  there). Neither was worth it given native decode itself didn't win.
- The `Arrow → RowData` (read) transpose — separate, and native can't build JVM `RowData`.
