# Fully-columnar native islands (Arrow in / Arrow out for every operator but source/sink)

**Status:** open — the invariant to converge the engine on. Partially true today; two operator groups
and the planner's island model remain.

## The invariant
A native subplan is a **contiguous columnar island**: every native operator is `Arrow → Arrow`. The only
operators allowed a non-Arrow edge are:
- **sources** — raw bytes (Kafka) or host `RowData` in → Arrow out, and
- **sinks** — Arrow in → commit / host `RowData` out.

Transposes (`RowDataToArrowOperator` / `ArrowToRowDataOperator`, both already built) appear **only at the
island's perimeter** — where it borders a host rowwise operator — and **never between two native
operators**. Concretely a `Row→Arrow` transpose goes after a *host* source feeding the island and an
`Arrow→Row` transpose before a *host* sink/`collect`; a native columnar source/sink needs none.

**All-or-nothing.** Within a connected native region there are no row-fed native operators and no
mid-plan `Arrow→Row→Arrow` round-trips. If an operator can't be columnar it falls back to the host
(shrinking the island), rather than being run row-fed in the middle of otherwise-columnar work. We
either accelerate nothing in a region or accelerate it all, columnar.

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

### Planner — enforce the island model
The transition-inserter currently bridges a row-fed native operator with mid-plan transposes. Change it
so a native region is uniformly columnar: transposes only at the perimeter (host↔native), and an
operator that can't be columnar falls back rather than forcing an interior transpose. The Rust side is
already all-columnar at the boundary once Group A/B land (no Rust operator consumes `RowData`).

## Done when
Every native operator except sources and sinks is `Arrow → Arrow`; `RowData` appears only at island
perimeters via the two dedicated transpose operators; no native operator takes `RowData` in. Re-run the
full suite (parity unchanged) and the benchmarks (watch the host-edge window-agg paths for the added
perimeter transpose). Relates to ticket 34 (columnar sinks / nested boundary types) and ticket 28 (the
transpose + shuffle).
