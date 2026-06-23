# When to accelerate: keep columnar chains (mimic Comet)

**Status:** partially done — the **expression `allowIncompatible` config surface shipped**
(`NativeConfig`, `-Dstreamfusion.expression.<NAME>.allowIncompatible`, mirroring Comet). Remaining:
per-operator enable flags + a master native on/off switch.
**Source:** user direction; mimic DataFusion Comet.

## Done: expression allowIncompatible flags (Comet's `spark.comet.expression.<EXPR>.allowIncompatible`)
Functions whose native result diverges from the host only at a precision/locale edge — `UPPER`/`LOWER`
(case folding), `ROUND` (BigDecimal), transcendental math (`SIN`/`EXP`/`POWER`/`SQRT`/… last-ULP) —
fall back by default and are opt-in per function (or blanket) via `NativeConfig`, read from JVM system
properties like the fallback-reason log flag. The encoder gates them in `RexExpression` (an
`INCOMPATIBLE_UNARY` map plus gated `POWER`/`ROUND`); off → a fallback reason naming the flag, on →
native. Tests cover both. A true value divergence (`CONCAT` NULL handling) is never opt-in.

## Remaining: operator-level config
- A master switch to disable native acceleration entirely (today it is all-or-nothing via
  `NativePlanner.install`).
- Per-operator enable flags (e.g. disable the native filter on a row-source island that measures
  < 1×), the operator analog of the per-expression flags above.

**What Comet actually does (read `CometExecRule` + `EliminateRedundantTransitions`):**
1. **Greedy bottom-up, per-operator, no cost model.** An operator becomes columnar iff *all its
   children are already native* and it is supported + enabled (`transformUp` + `convertNode`). There
   is **no** heuristic, threshold, or minimum-chain-length that refuses a "lone island." Comet
   converts every supported operator in a contiguous chain, anchored by **columnar scans**.
2. **Transitions at boundaries, then cancel inverse pairs.** Spark inserts `ColumnarToRow`/
   `RowToColumnar`; Comet's `EliminateRedundantTransitions` removes `ColumnarToRow(RowToColumnar(x))`
   and friends.
3. **One targeted island revert only:** `revertRedundantColumnarShuffle` reverts a columnar shuffle
   stranded between two *row* aggregates (no columnar consumer on either side). Config-gated.
4. **User control is config, not cost:** per-operator flags (`spark.comet.exec.<op>.enabled`) + a
   master switch. No automatic cost model.

**So the original "refuse lone islands via a cost model" premise was wrong** — Comet does not do
that. A lone filter looks bad only with a *row* source; in Spark the source is a columnar scan, so
the filter's child is native and the region is columnar-anchored. Our bare-filter 0.75× is the
`row → columnar → row` case Comet also pays with a row source; its only mitigation is config.

**What we already match:** greedy bottom-up substitution + transposes only at columnar↔row edges
(`PhysicalPlanScan`); no redundant inverse transposes (our per-edge insertion can't create adjacent
inverse pairs — Comet needs the cleanup pass only because Spark inserts transitions blind); no
stranded columnar shuffle (we only insert the columnar exchange when coupling a native columnar
window/join). So our end state equals Comet's by construction.

**The genuine gap vs Comet:** we are all-or-nothing (`NativePlanner.install`). Comet exposes a master
switch + per-operator enable flags so a user can disable acceleration where it does not pay (e.g. a
lone filter on a row-source pipeline). **That config surface — not a cost heuristic — is the faithful
next step.** (Original cost-model notes kept below for history.)

## Problem
Today the planner substitutes each supported operator independently. Every
native operator transposes RowData -> Arrow on input and Arrow -> RowData on
output. So an isolated native operator surrounded by host operators pays two
transpositions for one operator's worth of native speedup — which can be a net
loss. Worse, two adjacent native operators each transpose at their shared edge
even though the data could have stayed columnar between them.

## What Comet does
Comet runs operators columnar and inserts explicit transition operators
(columnar->row, row->columnar) only at the boundary between accelerated and
non-accelerated regions. It converts **maximal connected subtrees** of
supported operators so transitions happen at the edges of a chain, not per
operator, and it avoids creating tiny "columnar islands" whose transition cost
outweighs the gain. The decision is cost/heuristic- and config-driven.

## What we want
A substitution policy, not just per-node matching:

1. Identify **maximal connected chains** of supported (substitutable) operators
   in the optimized plan.
2. Transpose RowData<->Arrow (or use a native shuffle, see ticket 10) only at
   the chain's boundaries with host operators.
3. Don't substitute a chain whose only member(s) wouldn't beat the transition
   cost — a heuristic/threshold (e.g. min chain length, or a cost model), and a
   config to force/disable.
4. Per the repo's benchmark rule: a single accelerated operator should be
   benchmarked against the transition overhead; if it doesn't win, it should
   stay on the host or only be taken as part of a longer chain.

## Notes
- This reframes the matcher from "replace a node" to "find and replace a
  region", inserting a single ingress transpose and a single egress transpose
  per chain.
- Interacts with ticket 10 (native shuffle): a shuffle inside a chain currently
  forces a transpose; a native columnar shuffle keeps the chain columnar across
  it.
- Reference: Comet's session extensions / columnar transition insertion.
