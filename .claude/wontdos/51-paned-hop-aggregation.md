# WONTDO — Paned/tiered HOP aggregation

**Decided 2026-07-05, in two steps, both empirical.**

**Step 1 — the original premise was already built.** The ticket (from the 2026-07 reference
survey; Arroyo's `TieredRecordBatchHolder`) proposed folding each row once into a slide-width pane
and merging `size/slide` panes per window fire, to cut the hopping aggregator's per-row O(s/d)
fold. Checking the q5 plan and flame graph showed the two-phase split already computes exactly
that shape: the local window aggregate pre-aggregates per slice (one fold per row), the global
merges slice partials into the overlapping windows — the same slicing architecture Flink itself
uses (`SliceSharedAssigner`). The per-row O(s/d) fold survives only on the single-phase path,
which the planner takes only when two-phase is unavailable; no Nexmark query is bound by it.

**Step 2 — the residual doesn't clear the bar.** What remained was the global's slice merge
(loop hygiene: a key clone per (window, slice); architecture: Arroyo-style tiered merges). The
2026-07-05 q5 profile puts the ENTIRE slice-merge subtree — `accumulators` entry, the merge loop,
the per-row windows Vec — at roughly 3–4% of the query, which runs at 1.52x on the generator.
Neither the hygiene pass (+~2% ceiling) nor tiering (state + snapshot-format change for a slice of
that) clears the standing swap-only-with-a-bench rule.

Reopen if a workload with a much larger size/slide ratio (the merge is O(s/d) per slice per key)
or a single-phase-forced plan shows the hop aggregation dominating. The pane-merge parity notes
from the original ticket (SUM/COUNT merge trivially, AVG as (sum, count), MIN/MAX extremes,
DISTINCT gated off panes) still apply, and slice-merge trees are the host-faithful shape.
