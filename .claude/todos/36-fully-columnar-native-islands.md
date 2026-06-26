# Fully-columnar native islands (Arrow in / Arrow out for every operator but source/sink)

**Status:** open — the invariant to converge the engine on. Partially true today; two operator groups
and the planner's island model remain.

## The invariant
A native subplan is a **contiguous columnar island**: every native operator is `Arrow → Arrow`. The only
operators allowed a non-Arrow edge are:
- **sources** — raw bytes (Kafka) or host `RowData` in → Arrow out, and
- **sinks** — Arrow in → commit / host `RowData` out.

Transposes (`RowDataToArrowOperator` / `ArrowToRowDataOperator`, both already built) appear **only at the
plan's perimeter** — where the native region borders a *rowwise* source/sink — and **never between two
native operators**. Whether a perimeter transpose is needed depends on that source/sink:
- a **columnar** source/sink (e.g. the native Parquet source) → **no transpose** at that edge;
- a **rowwise (Flink) source/sink** → one transpose (`Row→Arrow` after the source, `Arrow→Row` before
  the sink).

All of these are fine: `Flink-src → [Row→Arrow] → native… → [Arrow→Row] → Flink-sink`;
`native-src → native… → native-sink`; `native-src → native… → [Arrow→Row] → Flink-sink`;
`Flink-src → [Row→Arrow] → native… → native-sink`.

**All-or-nothing, at the whole-query level.** The moment any operator that is **not** a source or sink
would fall back to row-wise execution, we substitute **nothing** — the entire query runs as stock Flink.
No partial/operator-by-operator acceleration, no row-fed native operators, no mid-plan
`Arrow→Row→Arrow`. A rowwise Flink *source/sink* is **not** a fall-back trigger — it is handled by a
perimeter transpose. Only a non-source/sink operator that cannot run columnar trips the whole-query
fallback. (This replaces today's operator-by-operator substitution.)

Internal representation is a separate matter: Top-N and the regular updating join keep row-materialized
state (`Vec<ScalarValue>`) for sort/retract correctness — that "sort-like" internal row-wiseness is
fine; their **boundary** is already `Arrow → Arrow`, which is all this invariant governs.

## Where we are
- **Already `Arrow → Arrow`:** filter, calc, non-windowed GROUP BY, columnar local window aggregate,
  Top-N, regular updating join, interval join, window join, OVER, columnar watermark assigner, columnar
  exchange.
- **Edge operators (allowed exceptions):** the Kafka decoders (`byte[]`/Arrow → Arrow — ingest side),
  the Parquet sink (Arrow → commit), the Parquet/ORC sources (Arrow out).

## Gaps to close
### Group A — emit Arrow instead of `RowData` (3 single-phase/final aggregates)
`NativeColumnarWindowAggregateOperator`, `NativeColumnarGlobalWindowAggregateOperator`,
`NativeColumnarSessionWindowAggregateOperator` consume Arrow but emit result *rows*. Make them emit
`ArrowBatch`: move the row materialization out of `NativeRowWindowOperatorCore.emitFinal` into the
existing `ArrowToRowDataOperator`, mark the physical nodes `ColumnarOutput`, and let the transition pass
insert the transpose before a rowwise sink/`collect`. No behavioural tradeoff — the transpose still
happens, just relocated to the perimeter. (`NativeColumnarLocalWindowAggregateOperator` already emits
Arrow — the two-phase partial — and is the template.)

### Group B — delete the row-fed aggregate variants (4)
`NativeWindowAggregateOperator`, `NativeLocalWindowAggregateOperator`,
`NativeGlobalWindowAggregateOperator`, `NativeSessionWindowAggregateOperator` take `RowData` in/out. They
exist to avoid a transpose when the upstream is a host rowwise operator. Under this invariant they are
removed; the planner always selects the columnar variant and inserts a `RowDataToArrowOperator` at the
host-rowwise input edge. Tradeoff (accepted): a `host-row-source → window-agg → row-sink` pipeline goes
from 0 transposes to 2 (one at each perimeter) — uniformity over the host-edge micro-optimization. The
`NativeWindowOperatorBase` row-fed scaffolding goes with them.

### Planner — whole-query all-or-nothing
Today the scan substitutes operator-by-operator and leaves the rest on the host (mid-plan transposes
bridging the boundaries). Change it to a two-pass decision: (1) check every operator the query would
accelerate — if **any** non-source/sink operator can't run columnar, substitute **nothing** and let the
query run as stock Flink; (2) otherwise substitute them all, columnar, inserting a transpose only where
the region meets a *rowwise* source/sink. No interior transposes, no row-fed native operators. Once
Group A/B land the Rust side is all-columnar at the boundary (no Rust operator consumes `RowData`), so
this is purely a planner-side change.

## Done when
Every native operator except sources and sinks is `Arrow → Arrow`; no native operator takes `RowData`
in; `RowData` appears only at a rowwise source/sink perimeter via the two dedicated transpose operators;
and a query either accelerates fully (all-columnar) or not at all (stock Flink). Re-run the full suite
(parity unchanged) and the benchmarks (watch the host-edge window-agg paths for the added perimeter
transpose). Relates to ticket 34 (columnar sinks / nested boundary types) and ticket 28 (the transpose +
shuffle).
