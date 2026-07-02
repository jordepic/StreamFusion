# 18 — JSON decode: simd-json tape walk instead of arrow-json

**Arroyo's choice.** Arroyo decodes every Kafka format through `arrow-json`: JSON bytes feed
`arrow_json::reader::Decoder` directly, and Avro/protobuf values are first converted to
`serde_json::Value` and pushed through the same decoder. One Arrow-building path, simple.

**Ours.** The JSON and CDC-envelope decoders parse each document with **simd-json** (the Rust port
of simdjson: SIMD structural indexing, then a flat parse tape) and walk the tape schema-driven,
appending straight into typed Arrow builders. arrow-json's tokenizer is a scalar byte-at-a-time
state machine; on the ingest edge that parse is most of the decode cost, and the swap measured
~27% faster on a realistic Nexmark-bid-sized document (~8% on a tiny 3-field one). RisingWave made
the same call (simd-json into its own builders); we differ from RisingWave only in building Arrow
arrays instead of engine-native rows.

**Why not patch arrow-json instead.** Its tape module is private, so a SIMD stage-1 cannot be
injected from outside; upstreaming one is a larger discussion (simd-json requires `&mut` input,
arrow-json's API takes `&[u8]`).

**Semantics are pinned, not reinvented.** The walker replicates arrow-json's per-type parsing —
numbers through `NumCast` (floats truncate toward zero for integer columns), strings through
arrow-cast's `Parser`/`string_to_datetime`, missing field == explicit null, unknown keys skipped,
duplicate keys last-wins — because those are the semantics the Kafka JSON parity tests pin against
Flink. Swapping the parser changes no output.

**The DECIMAL carve-out.** simd-json parses numbers eagerly to `i64`/`f64` and drops the raw
literal; a decimal with more significant digits than an f64 carries would round. arrow-json keeps
each number's raw digit string and parses decimals from it exactly — as does Flink (Jackson →
`BigDecimal`). So a schema containing a DECIMAL column (at any nesting depth) decodes via the
retained arrow-json path; everything else takes the simd-json walk. Coverage is unchanged either
way — the split is internal to the decoder.
