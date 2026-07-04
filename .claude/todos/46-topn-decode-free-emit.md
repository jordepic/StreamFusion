# Streaming Top-N: per-batch net-diff emission (parity-gated)

**Status:** the decode half SHIPPED 2026-07-04 — what remains is the net-diff staging question,
which is **blocked on a parity decision**, not engineering.

**Shipped:** the append-only ranker's emit decodes each **distinct** buffered row once per batch
(the cascade emits the same `Arc`-shared rows at many rank positions) and rebuilds the emitted
positions with a vectorized `take` — output byte-identical, decode work O(distinct) instead of
O(emitted). q19's in-operator decode share fell 72% → 6% and the query gained +13% on the
generator profile loop (112 → 127 iterations). The operator is now bound by output
materialization (`take_bytes`/memmove building the amplified cascade's string columns) — i.e. by
the *volume* of the cascade itself, plus the exit transpose downstream.

## What remains: emit the net diff per batch?

The cascade's volume is Flink's own `AppendOnlyTopNFunction` contract: per input record, a
`-U`/`+U` pair for every shifted rank. Staging the batch and emitting the net rank diff (the
`RetractableTopNRanker` already implements exactly that diff) would cut q19's emitted volume by
roughly the per-partition input multiplicity per batch — but the raw changelog would no longer
match non-mini-batch Flink row for row (the *materialized* result is identical; the intermediate
retractions differ, exactly as Flink's own mini-batch mode differs). A raw changelog sink
(kafka debezium/canal) observes the difference; an upsert or materializing consumer does not.

Options, needing a deliberate call (record in `divergences/` when made):
- Gate net-diff emission on the host plan being mini-batch (parity target = mini-batch Flink);
  ticket 41's mini-batch coverage would make that the natural pairing.
- Offer it as an opt-in (like `allowIncompatible`, but this is a changelog-encoding divergence,
  not a value divergence — arguably a separate flag).
- Accept the cascade as the price of byte parity and close this ticket (the decode fix already
  took the sting out; the remaining cost is proportional to genuinely-emitted rows).

RisingWave's three-zone Top-N cache (`top_n/top_n_cache.rs`) and `TopNStaging` net-diff are the
reference if the gated path is built.
