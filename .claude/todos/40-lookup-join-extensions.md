# Native lookup join — extensions beyond the v1 sync path

**Status:** v1 DONE (2026-07-01). This ticket tracks the follow-ups the v1 operator deliberately gates
out (they fall back to Flink today).

## What v1 does

`NativeLookupJoinOperator` accelerates a **synchronous** processing-time lookup join (Nexmark q13):
`probe JOIN dim FOR SYSTEM_TIME AS OF probe.proctime ON probe.k = dim.key`. It stays an Arrow
operator so the probe-side Calc/source keep running natively (the whole point — otherwise the lookup
would break the columnar island and drag the surrounding operators back to rowwise), and per probe row
it calls the connector's real `LookupFunction` (obtained via `LookupJoinUtil.getLookupFunction`, exactly
as Flink's `LookupJoinRunner` does), so the result is byte-identical. INNER + LEFT, field-reference
lookup keys, no calc/residual. Wiring: `LookupJoinMatcher` → `StreamPhysicalNativeLookupJoin` →
`NativeLookupJoinExecNode` → operator; verified by `FlinkLookupJoinSqlHarnessTest`.

## Async connectors — DONE (2026-07-01)

An `AsyncLookupFunctionProvider` connector (`isAsyncEnabled`) now routes to
`NativeAsyncLookupJoinOperator`. Rather than Flink's `AsyncWaitOperator` (cross-batch in-flight queue +
mailbox + snapshot/replay of in-flight rows), it uses the **within-batch concurrent** model that Arroyo's
`lookup_join` and RisingWave's `temporal_join` both use: per probe `ArrowBatch` it fires the connector's
real `asyncLookup` (from `getLookupFunction(..., async=true, ...)`) for each **distinct** key
concurrently, `CompletableFuture.allOf(...).join()`s on the task thread, then assembles the joined batch.
Because all the I/O begins and ends inside one `processElement`, **nothing is in flight across a batch
boundary** — a checkpoint barrier (itself a task-thread action) only runs between batches, so there is no
in-flight state to snapshot and **no mailbox is needed**, exactly as for the sync operator. The win over
sync is that a batch's lookups overlap (≈ one round-trip instead of N). Distinct-key dedup is safe: dim
state is fixed within a batch, so it can only differ from Flink's per-row calls when the dim mutates
mid-batch, where Flink's own concurrent lookups already race. Byte-identical, verified by
`FlinkAsyncLookupJoinSqlHarnessTest` (INNER, LEFT, duplicate keys). See [ticket 01](01-mailbox-threading.md)
for why within-batch beats the `AsyncWaitOperator` port here.

The remaining `AsyncWaitOperator` port (cross-batch I/O overlap) is only worth building if per-lookup
latency is so high that blocking on a single batch stalls checkpoints unacceptably — not the case today.

## Follow-ups (each currently falls back with a named reason)

- **Calc on the temporal table.** `calcOnTemporalTable` (projection/filter on dim rows before the join)
  — evaluate it natively via the existing expression engine over the dim columns, or in the JVM bridge.
- **Residual (non-equi) join condition.** `finalRemainingCondition` / `finalPreFilterCondition` —
  evaluate over the joined batch with the native Calc engine (the pre-filter gates the probe row before
  the lookup, as Flink does).
- **Constant lookup keys.** v1 admits only `FieldRef` keys; support `FunctionCallUtil.Constant` (a
  literal key component) by materializing the constant into the key row.
- **Columnar assembly / preload for bounded dims.** v1 copies probe+dim rows per output row. For a
  bounded side input, preload the whole dim into a native hash table once (RisingWave/Arroyo cache the
  dim side) and probe fully columnar — zero per-batch JVM crossing in steady state. Otherwise keep the
  per-row `LookupFunction` call but assemble the output columnar (take probe cols by index + matched dim
  cols) instead of row copies.
- **Distributed execution.** Like the UDF bridge (ticket 38), the operator holds a `LookupFunction`
  built at plan time in the planner JVM — fine for local/embedded/benchmark. For distributed task
  managers, carry the serializable `TemporalTableSourceSpec` into the operator and build the function at
  `open()` (what Flink's own exec node does).
