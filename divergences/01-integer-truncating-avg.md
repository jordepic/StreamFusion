# Integer-truncating AVG

**Kind:** semantic — output values differ from Arroyo for the same query.
**Diverges from:** Arroyo / DataFusion.
**Forced by parity:** yes.

## Their decision
Arroyo delegates `AVG` to DataFusion's `avg` aggregate. For an integer input
column, DataFusion's `avg` returns a **floating-point** result (IEEE division),
which is the standard DataFusion/Spark behavior Comet would also inherit.

## What we do instead
For `AVG` over a bigint column we compute Flink's semantics: integer division of
the sum by the count, **truncating toward zero**, returning a bigint. This lives
in a small custom `IntegerAvgAccumulator` rather than DataFusion's `avg`.

## Why
Flink's SQL returns the *input* numeric type for `AVG` of an integer column, so
`AVG` of `{1, 2}` is `1`, not `1.5`. If we used DataFusion's float `avg` our
result would differ from stock Flink, breaking the prime directive. This is the
clearest case where Arroyo's choice and Flink's behavior simply disagree and we
must follow Flink.

## Scope / consequences
- Only the bigint case diverges from Arroyo. A future float-input `AVG` would use
  DataFusion's `avg` directly and agree with both engines.
- Because the partial state is `(sum, count)` — two fields — integer `AVG` is
  currently supported only as a lone aggregate and only single-phase; see
  [02](02-two-phase-local-global-aggregation.md) for the multi-field-partial
  limitation.
