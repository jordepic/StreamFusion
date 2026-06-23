# Wider input schemas: types, multiple columns, grouping keys

**Status:** partial — all non-decimal numeric value types and wide grouping keys
land; multiple value columns, `COUNT(*)`, decimal, and decimal/timestamp keys remain
**Source:** running theme across the operator/matcher work

## Done
The window operator is keyed by `(window, key)` and supports one integer
grouping key in addition to the window (`GROUP BY k, window_start, window_end`),
matching the host's per-key results and output column order. Multiple aggregates
per window are supported. The native engine is value-type agnostic; the
accelerated value types are the parity intersection in
`docs/aggregate-type-support.md` — all aggregates over every non-decimal numeric
type. Bigint/double are both one- and two-phase; int/smallint/tinyint/float are
one-phase with custom accumulators that keep the host's type and precision (integer
wrap/truncate, float 4-byte sum, float/double avg in double), verified at the
overflow boundary and under float accumulation error. Their value column rides a
typed Arrow vector decoded by a per-type value-type code. Grouping keys may be
bigint/int/string/boolean/date (the native key path is type-general, so each is a
matcher gate + a JVM vector). Remaining below:

- **DECIMAL value columns (all aggregates):** precision/scale derivation is
  exotic; a matcher gate + a decimal Arrow vector path.
- **More key types:** decimal/timestamp keys remain (a JVM vector + boxing each;
  timestamp also carries a precision). The join and `OVER` partition paths still
  carry only bigint/int/string — widening them reuses the same machinery once
  covered by tests.
- **Multiple distinct value columns and `COUNT(*)`:** all aggregates still read a
  single shared value column. `SUM(a), SUM(b)` and `COUNT(*)` need the native fold
  to bind each aggregate to its own column (and a row-count path for `COUNT(*)`).

## Problem
The native operators assume a narrow shape: a single int value column, and
for windows a group-by-window-only aggregation. Real queries have other value
types (long/double/decimal), GROUP BY keys in addition to the window, and
multiple aggregates in one window.

## Goal
Generalize the matcher and operators to:
- value columns of types beyond int32/int64,
- one or more extra grouping keys alongside the window,
- multiple aggregate calls in a single window aggregate.

## Acceptance criteria
- A `GROUP BY key, window_start, window_end` tumbling query with two aggregates
  routes to native with identical results.
- Type coverage matches the safe intersection documented in research §8.

## Pointers
- Per-key state: the native aggregator becomes keyed by `(group_key, window)`
  rather than just `window`.
- Reuse Flink's type mapping (`ArrowUtils`) for the value/key columns;
  research §8 (type mapping) is the guardrail for the supported intersection.
- Depends conceptually on [03] for multi-aggregate output schema handling.
