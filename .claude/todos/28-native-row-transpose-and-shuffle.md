# Native row→Arrow transpose (+ fused keyed shuffle), inspired by arrow-avro

**Status:** planned (design ready; implement after compaction).
**Source:** perf sprint — the `RowData → Arrow` transpose is the tax that caps row-source ops
(~1.2×) and sinks the bare filter (0.72×) / Parquet sink (0.91×). Idea: move the columnar
construction off the JVM (slow Arrow-Java `setSafe` per cell) to native Rust (fast `arrow-rs`
builders), the way `arrow-avro` decodes row-oriented Avro into Arrow.

## The problem we're attacking
`RowDataArrowConverter.write` builds Arrow on the **JVM, cell-at-a-time**: for every row × column it
does `vector.setSafe(i, row.getX(col))` — an off-heap write through the Arrow-Java API (bounds
check, null bit, occasional realloc) per cell. That per-cell cost is the transpose tax. The read
side (`Arrow → RowData`) is a separate problem (can't move native — it builds JVM `RowData`; the
`ColumnarRowData` zero-copy view has an off-heap-lifetime hazard — see the sprint notes); **this
ticket is the WRITE side only** (`row → Arrow`, entering the native region: Kafka/DataStream source,
or any row producer feeding a native operator).

## The arrow-avro technique (the inspiration — `~/data/arrow-rs/arrow-avro`)
`reader/record.rs` `RecordDecoder`:
- `fields: Vec<Decoder>` — one per column; each `Decoder` variant owns an Arrow array **builder**
  (`Int64Builder`, `StringBuilder`, `Decimal128Builder`, …).
- `decode(&mut self, buf: &[u8], count: usize) -> Result<usize>` — decodes `count` row-records from a
  byte buffer, appending each field to its builder; returns bytes consumed.
- `flush(&mut self) -> Result<RecordBatch>` — finishes the builders into arrays, assembles a batch.

i.e. **(row-oriented bytes + count) → per-field builders → `RecordBatch`**, in one native pass. That
is the row→columnar transpose, done where Arrow construction is fast.

## Design
Split the transpose: the JVM does a **cheap, sequential, on-heap row serialization**; native does the
**columnar build**. One JNI call per batch (not per cell).

1. **JVM `RowBatchEncoder`** — serialize a `List<RowData>` of a fixed `RowType` into one growable
   byte buffer: a tight, length-prefixed row encoding (per-row null bitset, then fixed-width
   primitives inline, var-length string/decimal length-prefixed). Sequential `byte[]`/`MemorySegment`
   writes — no Arrow API, no off-heap per-cell, JIT-friendly. Pass `(buffer, rowCount)` over JNI
   (direct `ByteBuffer` to avoid a copy).
2. **Native `RowBatchDecoder`** — the arrow-avro `RecordDecoder` analog for *our* encoding and the
   types `RowDataArrowConverter` supports (tinyint/smallint/int/bigint/float/double/bool/string/
   timestamp/date/decimal): `new(schema)` builds per-field builders; `decode(buf, rowCount)` appends;
   `flush()` exports an Arrow batch (C Data Interface, as the operators already do on the way out).
3. **Operator wiring** — a native variant of `RowDataToArrow`: `processElement` accumulates rows,
   encodes the batch, calls `Native.decodeRowBatch(...)`, emits the `ArrowBatch`. Behind a flag so it
   coexists with the JVM converter until benchmarked.

## Fused keyed shuffle on the native side (the second half of the ask)
For a **row source feeding a keyBy** (e.g. Kafka → window), today: JVM transpose → native columnar
exchange (`partition_batch` split-by-key). Fuse them: `Native.decodeRowBatchAndSplit(buf, rowCount,
keyColumns) -> Vec<(channel, ArrowBatch)>` — decode rows to Arrow **and** split by key in one native
pass, reusing the existing `partition_batch`/`ColumnarKeyGroupPartitioner`. The JVM only ever does the
cheap row encode; decode + shuffle are native. This is the "shuffling on the native side" goal.

## Encoding choice (decide by prototype + benchmark)
- **Custom tight encoding (recommended target):** we own both ends and ~12 types, so a minimal
  length-prefixed format is the fastest to encode/decode and needs no dependency. Write the decoder
  ourselves (mirrors arrow-avro's per-field builder dispatch).
- **Reuse `arrow-avro` directly (validation path):** JVM emits Avro, native decodes with arrow-avro's
  `Decoder` — least native code, proven, but Avro encode overhead on the JVM + a new dep. Good for a
  first "does native decode even win?" measurement before investing in the custom format.

## Must-benchmark hypothesis (this is the whole point — CLAUDE.md rule)
The win is real only if `(JVM tight encode) + JNI + (native decode + arrow-rs build)` beats
`(JVM Arrow-Java cell-at-a-time build)`. Plausible (Arrow-Java `setSafe` is slow; arrow-rs builders
are fast; one JNI call/batch), but **not assumed**. First slice must measure the row→columnar
transpose both ways and only keep native if it wins. Targets to move: bare filter 0.72×, Parquet sink
0.91×, and the row-source tumbling/OVER ~1.2× (their input transpose), and Kafka→Parquet pipelines.

## Reference-first (per .claude/claude.md)
- **Operator/decoder structure:** arrow-avro `reader/record.rs` (RecordDecoder, per-field builders,
  `decode`/`flush`) and `reader/mod.rs` (the push `Decoder` API).
- **JNI / Java↔Rust handover (mandatory):** consult **DataFusion Comet** — how `CometSparkToColumnarExec`
  moves Spark rows to Arrow (JVM-side or native?), and Comet's off-heap buffer transfer pattern across
  JNI. Check whether Comet builds Arrow on the JVM (like us today) or ships rows to native; that
  informs whether this idea is even how Comet does it. **Do this before writing the JNI.**

## Build slices (each green + benchmarked)
1. Native `RowBatchDecoder` (custom encoding) + rust tests + a criterion bench (decode 4096 rows).
2. JVM `RowBatchEncoder` (the inverse) + a round-trip parity test (encode→decode == original rows).
3. JNI `decodeRowBatch` + a native `RowDataToArrow` operator variant (flag-gated) + e2e benchmark
   vs the JVM converter. **Keep only if it wins.**
4. (Stretch) `decodeRowBatchAndSplit` — fuse the keyed shuffle; benchmark Kafka→keyed-window.

## Out of scope
- The `Arrow → RowData` (read) transpose — native can't build JVM `RowData`; tracked separately.
- The acceleration-policy/config work (ticket 09).
