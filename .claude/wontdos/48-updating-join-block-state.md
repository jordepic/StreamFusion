# WONTDO — Updating join: block-based state store (columnar blocks + row refs)

**Decided 2026-07-05, on the post-round profile the ticket itself gated on.**

The Proton-style block store (state as retained columnar blocks indexed by key → `{block, row}`
ref lists, emit by `take`/`interleave`; references: Proton `RefCountDataBlockList.h`, RisingWave
`JoinRowSet`) was ticketed behind one question: after the borrowed-byte-probe stage shipped, is
the joiner still bound by stored-row decode (`convert_rows` in the candidate gather) or
first-insert copies?

It is not. The 2026-07-05 q23 flame graph (the largest joiner island, 75s generator loop) puts the
joiner's native leaves at `UpdatingJoiner::push` bookkeeping ~4.3%, hashing ~3.4%, `memcmp` ~2.3%,
and `arrow_row` decode ~2.3% — `convert_rows` no longer registers above the 2% floor at all. The
two ByteKey stages (2026-07-04 state keys, 2026-07-05 the remaining maps) took exactly the
allocator/decode churn the block store was designed to take; q23 gained a further +8.5% from the
second stage (164 → 178 iterations). What remains is intrinsic hash-join work.

A state-store redesign — with its `num_assoc` parallel bookkeeping, block compaction policy, and a
snapshot-format change — is not worth a ~2% ceiling. If a future workload profile shows the
stored-row gather dominating again (e.g. much wider payload rows or higher match fan-out than
Nexmark's), this can move back to the board; the design notes above are the starting point.
