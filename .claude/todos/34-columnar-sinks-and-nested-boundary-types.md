# Columnar sinks + nested types across the native↔host boundary

**Status:** open
**Source:** the row↔Arrow transpose (`RowDataArrowConverter`) carries only scalar/string/temporal/
decimal columns; it has no case for `ROW`/`ARRAY`/`MAP`/`VARBINARY`. So any native operator that
*produces* a nested column cannot hand it back to a rowwise host operator (a sink, `collect()`, or a
not-yet-native downstream op) — the Arrow→RowData edge throws `unsupported column type`.

## Why this matters now
The native decoders already produce nested Arrow for several formats — protobuf messages with nested
`message`/`repeated`/`map` fields, Avro records with nested records/arrays, JSON with nested objects.
But because the boundary transpose can't carry those columns, a `SELECT * FROM t` (which lands in a
rowwise `collect`/sink) would fail at the transpose. So the planner currently **gates those tables to
fall back** (e.g. `ProtobufDescriptors.isFlatScalarMessage` routes only flat scalar protobuf; nested/
repeated/map/bytes/enum fall back). The decoders are capable; the boundary is the limit.

## Two directions (the end state)
1. **Custom columnar sinks** — the preferred end state for columnar workflows. A native columnar sink
   (and columnar `collect`) consuming `ArrowBatch` directly means a native source→…→sink pipeline pays
   **no transpose at all**, nested columns included. This is the real fix for throughput: the transpose
   exists only to bridge to rowwise host operators, so removing the rowwise edge removes the problem.
   Relates to ticket 24 (columnar endpoints) — the Parquet sink is already columnar; generalize the
   pattern and add a columnar collect for SQL result fetching.
2. **Nested types in the transpose** — the fallback bridge for when a rowwise host operator is genuinely
   downstream. Extend `RowDataArrowConverter` (both `read` and `write`) to map `StructVector`↔`RowData`,
   `ListVector`↔`ArrayData`, `MapVector`↔`MapData`, and `VarBinary`↔`byte[]`. This lets complex messages
   route even into a rowwise edge, and benefits **every** format (not just protobuf).

## When this lands, revisit the gates
- Drop `isFlatScalarMessage` (or widen it) so nested/repeated/map protobuf routes; add parity tests for
  nested `ROW`, `ARRAY`, and `MAP` against Flink's protobuf format.
- Revisit the unsigned-int and enum exclusions: `bytes`→`VARBINARY` needs the converter's VARBINARY case
  (direction 2); enum needs a decision on int-vs-name representation matched to the declared column type;
  unsigned/fixed ints decode unsigned in the Arrow decoder but signed in Flink (a value divergence to
  reconcile, not just a boundary gap).
- Avro/JSON nested-object decode can then route too (same boundary fix).
