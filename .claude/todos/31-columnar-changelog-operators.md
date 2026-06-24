# Columnar variants of the changelog operators (row-fed → Arrow in/out)

**Status:** not started
**Source:** the CLAUDE.md principle — *native operators are columnar* (Arrow `ArrowBatch` in and out;
only the transpose operators bridge to `RowData`). Three operators violate it today.

## The problem
The changelog operators were built **row-fed** for correctness first:

- `NativeGroupAggregateOperator`
- `NativeUpdatingJoinOperator`
- `NativeTopNOperator`

Each is `OneInput`/`TwoInputStreamOperator<RowData, …, RowData>`: it buffers Flink `RowData`,
transposes the batch to Arrow (`RowDataArrowConverter.write`), calls the native kernel, then
transposes the result back to `RowData` (`RowDataArrowConverter.read`). So a `RowData → Arrow →
RowData` round-trip is paid on **every** batch — even when the neighbours are themselves native — which
is exactly why the row-fed GROUP BY benchmarks at **0.77× vs Flink** (a cheap per-key op cannot
amortize two transposes). The native kernels already operate on Arrow `RecordBatch`es; only the
operator's I/O type is wrong.

Contrast the window aggregate / `OVER` / joins, which are `ColumnarInput`/`ColumnarOutput`: they flow
`ArrowBatch` directly between adjacent native operators and only transpose at the native↔host edge
(divergences/08), so the windowed-over-columnar-source pipeline clears 1.82×.

## What to build
Give each of the three operators a **columnar variant** (`ArrowBatch` in/out), mirroring how the
window aggregate has both a row-fed and a `…ColumnarWindowAggregate` variant chosen in
`PhysicalPlanScan`:

1. **Columnar operators** — `ArrowBatch` in, `ArrowBatch` out, carrying the changelog `RowKind` as the
   `$row_kind$` byte column already present on the batch (divergences/13). The **native FFI is
   unchanged** (it already imports/exports Arrow with `$row_kind$`); this is a new Java operator plus
   a `ColumnarInput`/`ColumnarOutput` physical rel + exec node per operator.
2. **Keyed columnar exchange** — the stateful ones (GROUP BY, join, Top-N) are keyed, so feed them
   through the native columnar exchange (`StreamPhysicalNativeColumnarExchange`) when the input sits
   on a columnar producer, the same coupling the window aggregate/joins use. **Verify the columnar
   exchange passes the `$row_kind$` column through the keyed split** (it is a pass-through data column
   — confirm/extend).
3. **Planner choice** — in `PhysicalPlanScan`, substitute the columnar variant + columnar exchange
   when the input is a columnar producer over an exchange; otherwise the row-fed variant (or the
   columnar variant fronted by a transition-inserted transpose). Mirror the window-aggregate branch.
4. **Re-benchmark** (`-Pbench`, release) fed from a columnar source/exchange. The row-fed numbers
   (GROUP BY 0.77×) should cross 1× once the transpose is gone; if they do not, reconsider per
   CLAUDE.md. Add the join and Top-N to `ThroughputBenchmark` while here (they have no vs-Flink number
   yet).

## Longer-term simplification
Because a columnar operator wrapped in transition-inserted transposes on both edges costs the *same*
two transposes as the bespoke row-fed operator (and strictly less inside a native chain), the clean
end state is **columnar-only**: drop the row-fed operators and let the transition pass handle host
edges. Evaluate doing this across all operators (incl. retiring the row-fed window-aggregate variant)
once the columnar variants exist and the isolated-operator case is confirmed no worse.

## Acceptance criteria
- The three changelog operators flow `ArrowBatch` end-to-end in a native chain (no per-operator
  transpose), verified by a parity test over a columnar source and by the routed plan.
- A release benchmark showing the columnar GROUP BY (and join/Top-N) vs Flink, with the transpose
  removed — ideally ≥ 1× when columnar-fed.
- CLAUDE.md's "native operators are columnar" principle holds for every operator except transpose.
