# Aggregate × value-type support (parity intersection)

The native side runs DataFusion's aggregates, whose result types and arithmetic
differ from Flink's. Since the prime directive is byte-identical results, the
matcher only accelerates `(aggregate, value-type)` pairs where the two agree.
This table is the guardrail (the matcher enforces it); rows marked ✗ fall back.

Flink semantics (from `flink-table` `aggfunctions/` @ release-2.2.0):
- **SUM** preserves the input type and accumulates in it — `SUM(INT)→INT`,
  wrapping at 2³¹; `SUM(FLOAT)→FLOAT`; `SUM(DECIMAL(p,s))→DECIMAL(38,s)`.
- **AVG** returns the input type; for integers it is truncating integer division
  (sum in bigint, `÷` count, cast back) — `AVG(INT)→INT`.
- **MIN/MAX** return the input type (no arithmetic).
- **COUNT** is always `BIGINT`.

DataFusion: `sum(intN)→Int64`, `sum(floatN)→Float64`, `avg(int)→Float64`,
`min`/`max`→input type, `count`→Int64.

| value type | SUM | AVG | MIN | MAX | COUNT |
|---|---|---|---|---|---|
| BIGINT  | ✓ | ✓ (custom truncating) | ✓ | ✓ | ✓ |
| DOUBLE  | ✓ | ✓ | ✓ | ✓ | ✓ |
| INT     | ✗ widen/wrap | ✗ truncate | ✓ | ✓ | ✓ |
| SMALLINT / TINYINT | ✗ | ✗ | ✓ | ✓ | ✓ |
| FLOAT (REAL) | ✗ FLOAT≠DOUBLE | ✗ | ✓ | ✓ | ✓ |
| DECIMAL | ✗ precision rules | ✗ | ✓ | ✓ | ✓ |

Notes / divergences this avoids:
- **SUM over narrow ints** widens (Int64) and never wraps at 2³¹ in DataFusion, so
  it disagrees with Flink. Matching Flink needs a custom wrapping accumulator per
  width (i32/i16/i8) — a future item, not yet built.
- **AVG over integers** is DataFusion `Float64`; Flink truncates to the input
  type. We already match this for `BIGINT` with a custom accumulator; extending to
  narrower ints means casting the truncated result back to that width.
- **DECIMAL** SUM/AVG precision/scale derivation is exotic; excluded.

## Status
- Implemented value types: `BIGINT`, `DOUBLE` (all five aggregates), and `INT`
  for `MIN`/`MAX`/`COUNT`. The native value path is type-general; adding the
  remaining MIN/MAX/COUNT types (SMALLINT/TINYINT/FLOAT/DECIMAL) is mechanical
  (an Arrow vector class + getter + a value-type code).
- SUM/AVG over non-`BIGINT`/`DOUBLE` await the custom wrapping/truncating
  accumulators above.
- Grouping keys beyond a single bigint key are tracked separately in ticket 04.
