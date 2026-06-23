# Surface fallback reasons (Comet-style plan explanation)

**Status:** done. Calc + per-condition operator reasons, `explainSql` annotation, readme docs.
**Source:** user direction — when a query does not accelerate, the user has no way to see *why*.

## Done (first cut)
- `RexExpression` captures the first un-admitted node's reason at each reject site (`reject(...)`):
  unsupported function/operator (names it, e.g. `ABS`, `CONCAT`), literal type, CAST
  (`source→target, only widening numeric`), SUBSTRING bound, TRIM form. Exposed via
  `RexExpression.reasonForCalc(calc)`.
- `PhysicalPlanScan` collects Calc fallbacks into `fallbackReasons()` (a getter alongside
  `substitutions()`/`operatorTypes()`), prefixed by node (`"Calc: …"`), and — under
  `-Dstreamfusion.logFallbackReasons=true` — logs each at plan time. This is the analog of Comet's
  `FALLBACK_REASONS` tag + `COMET_LOG_FALLBACK_REASONS`.
- `NativeParity.assertFallbackReasonContains(env, sql, reason)`; the ABS/CONCAT/SUBSTRING fallback
  tests assert the reason names the cause.

## Done (operator-level)
- `PhysicalPlanScan.operatorReason(node)` returns an operator-level reason for each recognized
  stateful shape its matcher declined — window/global aggregate, OVER, interval/window join — naming
  the operator and its requirements. Recorded into the same `fallbackReasons()` + log path; the
  interval-join fallback test asserts it. (Source/sink connector mismatches are intentionally *not*
  reported — a non-parquet sink is a different connector, not an interesting fallback. Non-insert-only
  changelog still bails at the early guard without a reason.)
- Documented in `readme.md` ("Seeing why a query fell back").

## Done (per-condition + explain)
- **Per-condition operator reasons.** Interval join, window join, OVER, and global window aggregate
  converted from boolean `matches` to `unsupportedReason` (matches delegates), so the fallback names
  the specific failing condition (e.g. "interval join: only INNER joins", "global window aggregate:
  HOP slide must divide size"). The row/local window-aggregate path matches several variants with
  extra gates, so it intentionally keeps a coarse operator-level reason (precise would be unreliable).
- **explainSql annotation.** `NativePlanner.explain(env, sql)` returns Flink's `explainSql` plan (which
  already names substituted native operators) plus `PhysicalPlanScan.explainSummary()` — count of
  native operators and the fallback reasons. Flink has no SPI to inject text into its EXPLAIN renderer
  (no `ExtendedExplainGenerator` analog) and fallback nodes are host classes, so this matches Comet's
  flat fallback-list explain format by appending. Tested (a fallback names its reason; a supported
  query reports none).

## Remaining (optional)
- **Changelog / connector reasons.** A reason at the insert-only guard (non-insert-only → "changelog
  not supported") and for filesystem+parquet sinks on a remote path. Not yet surfaced.
- Inline (per-node-in-the-tree) explain annotation would need a Flink RelNode explain hook that does
  not currently exist; the appended-section form is the faithful equivalent.

## Problem
Today substitution is silent: a matcher that can't handle a node simply declines, and the operator
runs on the host. There is no signal to the user distinguishing "fell back because the op set / type
isn't admitted" from "the planner never considered it." Every reject path is a dead end:
- `RexExpression.encode`/`encodeCalc` return `null` on the first un-admitted node — the *reason*
  (which op, which type, a non-literal SUBSTRING bound, a narrowing CAST, CONCAT's NULL semantics, …)
  is known at the reject point but discarded.
- The operator matchers (`CalcMatcher`, `FilterCalcMatcher`, the window/join/sink matchers) return
  "not matched" without recording why.

## What Comet does (reference)
Comet threads a `fallbackReasons: ListBuffer[String]` through its support checks (e.g.
`isTypeSupported(dt, name, fallbackReasons)` in `CometSparkToColumnarExec`, and `CometExecRule`'s
per-operator checks). Unsupported nodes get tagged, and `CometExplainInfo` /
`EXTENDED_EXPLAIN_PROVIDERS` ("CometExtendedExplainInfo") prints the reasons in the plan explanation,
so `EXPLAIN` shows exactly which expression/operator/type forced a node onto Spark. It is reason
*collection at the reject site* + *surfacing through the explain path* — not a new mechanism.

## What to build
1. **Collect reasons at the reject site.** Give the encoder and matchers a way to record a short
   reason string when they decline (e.g. `RexExpression.encode` returns a result carrying either the
   encoding or a reason; matchers append to a per-plan reason sink). Cover the concrete cases we
   already fall back on: un-admitted op/function, unsupported column type, non-literal/out-of-range
   SUBSTRING bound, narrowing/string CAST, CONCAT (NULL semantics), bare `IS NULL` Sarg if relevant.
2. **Surface them.** At least log them at plan time behind a flag; better, expose through Flink's
   explain. Investigate whether Flink's `explain()` / `ExplainDetail` can carry custom annotations on
   a `FlinkPhysicalRel`, or whether we emit a side-channel report (a structured log / a debug string
   the harness can assert on). Match Comet's UX: the reason names the node and the cause.
3. **Test.** A test that an un-admitted query (e.g. `ABS(v)`, `CONCAT(s,'x')`) yields a fallback
   reason mentioning the offending function/type — the explain analog of the existing
   `assertFallback`.

## Notes / decisions
- Keep reasons cheap and allocation-free on the success path (only build strings when declining).
- This pairs with the acceleration-policy work ([ticket 09](09-acceleration-policy-keep-chains.md)):
  the config surface (per-op enable flags) and the fallback-reason surface are the two halves of
  Comet's user-facing "why/whether accelerated" story.
- Reference-first: read Comet's `CometExplainInfo`, `ExtendedExplainInfo`, and how `CometExecRule`
  tags nodes, before designing our surfacing path.
