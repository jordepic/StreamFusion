# Native expression layer (general RexNode evaluation)

**Status:** stage 1 DONE. Native decoder + compile-once handle
(`createFilterExpression`/`filterExpression`/`closeFilterExpression`, caching a
coerced `PhysicalExpr`, evaluated synchronously). Planner-side `RexExpression`
encoder translates the (SEARCH-expanded) `RexNode` condition to the encoding; the
filter matcher/operator route through the handle, replacing the old comparison/DNF
arrays and the per-batch `filterBatch` plan (now removed). All filter parity tests
pass via the general path, plus new arithmetic-predicate tests and an unsupported-
function fallback test. The encoding follows Comet over Substrait and compiles once
per operator — see [divergences/07](../../divergences/07-expression-encoding-and-compile-once.md).

**Integer-overflow parity: CLOSED.** DataFusion already evaluates `+`/`-`/`*` with
wrapping kernels, matching Flink's Java integer semantics; the remaining mismatch was
that the encoder promoted every integer literal to i64, so `int * 2` computed in i64.
Literals now carry their declared width (i8/i16/i32/i64) so the arithmetic stays in the
host's type. Verified by a parity test at the `INT` overflow boundary. Residual: narrow-
int (`TINYINT`/`SMALLINT`) column arithmetic promotion — see the type-support doc.

**Source:** the foundational piece for general projections and richer predicates
(unblocks equality filters, computed columns, arithmetic/function predicates).

## Why
Today filters carry a fixed shape (conjunctions/disjunctions of column-vs-literal
comparisons) and projections are column subsets. Real Calcs have arbitrary
expressions: arithmetic, functions, CAST, `a + b`, `WHERE a * 2 > b`, and the
constant-folded column that blocks equality. A general RexNode→native evaluator
handles all of these for both the filter condition and the projection.

## Approach — hand-translated, encoded, DataFusion-evaluated
Like Comet (and unlike Substrait — see divergences for the rationale), we
hand-translate `RexNode` to a compact encoding and rebuild a DataFusion `Expr`
natively, then evaluate over the imported batch (`DataFrame.filter`/`.select`).
No new dependency, full control over which ops we admit.

**Encoding** (pre-order, parallel arrays, so JNI stays primitive arrays):
- `int[] kinds`: 0 INPUT_REF, 1 LIT_LONG, 2 LIT_DOUBLE, 3 LIT_STRING, 4 LIT_BOOL,
  5 LIT_NULL, 6 CALL.
- `int[] payload`: INPUT_REF → column index; CALL → op code; LIT_* → index into
  the matching literal pool; else -1.
- `int[] childCounts`: CALL → operand count; else 0.
- literal pools: `long[]`, `double[]`, `String[]` (booleans fold into longs).
The native side walks the pre-order stream, using `childCounts` to recurse, and
builds an `Expr`: INPUT_REF → `col(field name)`, LIT → `lit(value)`, CALL → the
binary/unary op or function, CAST → `Expr.cast_to(type)`.

**Op codes (CALL):** start with the parity-safe core — `+ - *`, the six
comparisons, `AND`/`OR`/`NOT`, `CAST` to a numeric/string type. Expand
cautiously: `/` and `%` (integer-division parity!), string/temporal functions,
etc. each only once a parity test confirms DataFusion matches Flink.

## Parity discipline
DataFusion's expression semantics differ from Flink's in places (integer
division, decimal arithmetic, null handling, string collation). So the **matcher
gates** to the admitted op set and the admitted operand types, and every admitted
op gets a parity test. An un-admitted op anywhere in the expression makes the
whole Calc fall back. This is the same parity-first stance as the rest of the
project; record any deliberate semantic choice in `divergences/`.

## Stages (each green + parity-tested)
1. **Encoding + native decoder.** ✅ DONE — `RexExpression` encoder (planner) and
   the native `Expr` builder, plus the compile-once `createFilterExpression`/
   `filterExpression` handle. The filter routes through it; all existing filter
   tests pass via the general path, with added arithmetic-predicate and
   unsupported-function-fallback tests.
2. **General projection.** The Calc projection becomes a list of expressions
   evaluated via `select`; output rows built from the result. Subsumes the
   column-subset projection and unlocks computed columns and equality (the folded
   constant projection becomes a literal expression).
3. **Fuse + expand.** One native Calc operator doing projection+filter in a
   single pass; widen the admitted op/function set with parity tests.

## Acceptance criteria
- Existing filter/projection tests pass via the general expression path.
- `SELECT a + b FROM t WHERE a * 2 > b` (arithmetic in projection and predicate)
  routes with identical results to the host; an un-admitted function falls back.
