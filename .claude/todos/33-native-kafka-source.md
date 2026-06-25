# Fully native Kafka source (Rust consumer → Arrow, no JVM round-trip)

**Status:** open — the fast path for streaming ingest, built alongside ticket 32's shallow path and
**falling back to it** when the consumer config can't be replicated natively. We build both and
benchmark the difference to justify the native path's cost rather than assuming it pays off.

## Relationship to the shallow path (ticket 32) and the deciding benchmark
The shallow path (Flink's `KafkaSource` + byte-passthrough deserializer → row→Arrow transpose →
native format-decode operator) is the **fallback**: Flink owns the consumer, offsets, checkpointing,
and — critically — all the SASL/SSL/auth config, so it works everywhere. The native path replaces
only the fetch+decode with rdkafka, reusing Flink's `KafkaSourceEnumerator` + `KafkaPartitionSplit` +
checkpointed offsets, and **degrades to the shallow path** for any source whose consumer settings it
can't faithfully translate (see the config-fidelity checklist below). The native path's whole
justification is throughput over the shallow path; **benchmark both on the same topic first** (the
heap→off-heap copy + JVM Kafka-client overhead + GC it removes vs the rdkafka fetch it adds) before
committing to the config-translation work.

## Config fidelity — must match Flink's consumer exactly (the hard part of going native)
Flink builds a `Properties` (user `setProperties` + forced overrides) and the `KafkaPartitionSplitReader`
uses **manual `assign()` + `seek()` per split — never `subscribe()`/consumer-group rebalance**; the
enumerator owns assignment, the group id is only for committed-offset reads/commits. A native reader
must reproduce:
- **`assign()` + `seek()` to the split's offset** (not subscribe); honor the checkpointed offset on restore.
- Forced overrides: `enable.auto.commit=false` (offsets live in Flink checkpoints), `auto.offset.reset`
  = the `OffsetsInitializer` strategy, byte (raw) value/key deserializers, `isolation.level` passed
  through (read_committed for EOS topics — must match or we read aborted records).
- **Java consumer config → librdkafka translation**, the real risk: clean 1:1 for bootstrap/group/
  client.id/enable.auto.commit/auto.offset.reset/isolation.level/fetch.min.bytes; renamed for
  `fetch.max.wait.ms`→`fetch.wait.max.ms`, `sasl.mechanism`→`sasl.mechanisms`; **no clean mapping** for
  `sasl.jaas.config` (parse → `sasl.username`/`password` or kerberos keytab/principal) and JKS
  `ssl.truststore`/`keystore` (→ PEM `ssl.ca.location`/cert/key); `max.poll.records` has no analog.
  SASL/SSL is where a silent divergence would bite — must be tested against the real Kerberos cluster.
**Source:** even with native decode (ticket 32), a Kafka pipeline still pays one unavoidable copy —
Kafka's client lands each message value on the JVM heap, and a moving GC means Rust cannot read it
in place, so the bytes must be copied off-heap before native decode. The *only* way to remove that
copy is to consume Kafka in Rust, so the bytes land in Rust-readable memory from the socket onward.

## Why this is its own ticket (and not just "finish ticket 32")
Ticket 32 keeps Flink's `KafkaSource` and accepts the one off-heap copy — cheap relative to the
`RowData` materialization it removes, and it reuses all of Flink's hard-won connector semantics. A
native source throws that reuse away and must **reimplement the parts that make Kafka correct**:

- **Split (partition) discovery and assignment** across parallel subtasks, with dynamic rebalancing
  as partitions/topics appear.
- **Offset management** — committed offsets, `auto.offset.reset` semantics, starting-offset modes
  (earliest/latest/timestamp/specific).
- **Checkpoint / restore integration** — offsets must be part of Flink's checkpoint so exactly-once
  (or at-least-once) holds across failure; the FLIP-27 `SourceReader`/`SplitEnumerator` state model.
- **Watermark generation** per split, with idleness, aligned to Flink's event-time model.
- **Consumer group coordination, TLS/SASL auth, schema-registry fetch** for the wire formats.

This is exactly the surface FLIP-27 `KafkaSource` + the Kafka client already cover. Reimplementing it
is justified only by the zero-copy win, so it is deliberately back-burner until the decode-operator
path (ticket 32) has proven the throughput ceiling and shown the copy is the next bottleneck.

## Reference-first (per repo CLAUDE.md)
- **Arroyo** (`~/data/arroyo`) already has a native Rust Kafka source feeding DataFusion — this is the
  primary thing to rip out and adapt, not reinvent. Map its source reader, offset/checkpoint model,
  and how it emits Arrow batches before designing ours. Record any deviation in `divergences/`.
- Rust Kafka clients: `rdkafka` (librdkafka bindings — what Arroyo uses) vs a pure-Rust client.
- Decode reuses ticket 32's native decoders (`arrow-json`/`arrow-avro`/…), so this ticket is
  "native *consumption* → Arrow", layered on the decode primitives already built.

## Shape (rough)
- A FLIP-27 `Source<ArrowBatch, KafkaSplit, EnumState>` whose `SourceReader` runs the Rust consumer
  over JNI: native `poll` returns Arrow batches (decoded in Rust), the Java reader forwards offsets to
  the checkpoint and emits `ArrowBatch` downstream. The `SplitEnumerator` may stay on the JVM (it is
  coordination, not hot path) while the per-split fetch+decode is native.
- Alternatively, a thinner step: a custom `KafkaRecordDeserializationSchema` that hands raw bytes to
  Rust in bulk (ticket 32) is the bridge; this ticket is the larger "own the consumer" move.
- Offsets are the checkpointed state; the native reader is restored to them, like the Parquet
  source's file cursor but with Kafka's commit semantics.

## Parity / correctness
- Byte/row-identical output to Flink's `KafkaSource` + stock format over the same topic/offsets.
- Exactly-once across a checkpoint/restore cycle (kill-and-recover test): no duplicates, no loss.
- Watermark/event-time behavior matches Flink's per-partition watermarking incl. idleness.
- A vs-Flink ingest throughput number (the payoff: zero-copy vs the ticket-32 one-copy path).

## Dependencies
- Build ticket 32 first (native decoders + the off-heap decode-operator path). This ticket only
  becomes worthwhile once that path is in and the copy is measurably the bottleneck.
