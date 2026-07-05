# Native lookup join — remaining extension: columnar assembly / bounded-dim preload

**Status:** v1 DONE (2026-07-01); async DONE (2026-07-01); calc-on-temporal-table, residual +
pre-filter conditions, constant lookup keys, and distributed execution DONE (2026-07-03) — the native
operators now drive **Flink's own generated lookup runners** (`LookupJoinRunner` /
`AsyncLookupJoinRunner`, incl. the WithCalc variants) over each Arrow probe batch, so key building
(field refs + constants), pre-filter, dim-side calc, residual condition, and LEFT null-padding are
byte-identical to the host by construction, and the generated code + embedded function instances
serialize to task managers exactly as Flink's own exec node ships them. The async operator fires
every probe row's lookup concurrently within the batch (bounded by Flink's
`table.exec.async-lookup.buffer-capacity`, the host's own backpressure) and awaits on the task thread
— still the Arroyo/RisingWave within-batch model, no mailbox, nothing in flight across a checkpoint
barrier (see ticket 01 for why within-batch beats the `AsyncWaitOperator` port). The only remaining
matcher gate is the **upsert-materialized** (keyed-state) lookup, which needs a changelog probe the
island doesn't admit anyway.

## What remains (perf, not coverage) — deprioritized 2026-07-05

- **Columnar assembly / preload for bounded dims.** The sync operator's per-row defensive copy
  (`RowDataSerializer.copy`, ~27% of q13's lookup path) SHIPPED away 2026-07-04: the collector now
  writes each looked-up row's fields into the Arrow builders at collect time (q13 +22% on the
  generator profile loop, with the transpose string fast path). Still open: preload a bounded dim
  into a native hash table once (RisingWave/Arroyo cache the dim side) and probe fully columnar —
  zero per-batch JVM crossing in steady state; and Arroyo's within-batch key dedup (one connector
  call for the batch's distinct missing keys, `lookup_join.rs`).
  **Deprioritized on the 2026-07-05 q13 profile:** after the collect-time assembly shipped, no
  lookup leaf registers above the 2% floor — the query is bound by the rowwise perimeter and the
  generator itself. Nexmark's dim is an in-memory test connector, so the preload's real win (a
  slow external dim: JDBC/HBase-class latency) is invisible here by construction. Pick this up
  when a real connector-backed workload profiles lookup-bound, not to chase a benchmark number.
- **Cross-batch async overlap** (`AsyncWaitOperator` port) — only if per-lookup latency is so high
  that blocking on a single batch stalls checkpoints unacceptably; not the case today.
