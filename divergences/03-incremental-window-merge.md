# Incremental accumulator merge for session (and sliding) windows

**Kind:** algorithmic — internal implementation differs; output is identical.
**Diverges from:** Arroyo.
**Forced by parity:** no — this is a chosen optimization, not a requirement.

## Their decision
Arroyo's session and sliding aggregating windows **store raw input batches** in a
tiered structure (e.g. `batches_by_start_time`) and **re-aggregate** the relevant
rows whenever a window advances or sessions merge. The raw rows are the source of
truth; aggregation is recomputed from them.

## What we do instead
We keep, per key, only the **incremental accumulators** for each open window, and
when two windows merge we fold one accumulator's partial state into the other via
`merge_batch`. Raw rows are discarded after they are folded in.

## Why
Every aggregate we support (`SUM`/`MIN`/`MAX`/`COUNT`, and integer `AVG`) has
associative, commutative, mergeable partial state, so merging accumulators yields
exactly the same answer as re-aggregating the union of rows — but in bounded
memory (state per window, not per row) and without recomputation on merge. This
matches the incremental model we already use for tumbling/hopping, so it keeps
one accumulator path across all window types.

The result is identical to Flink (and to Arroyo), verified by the parity harness,
including the case where a late, out-of-order element bridges two open sessions.

## Why diverge at all, given the "copy them" default
This is the one divergence not forced by Flink parity, so it earns its place by
two stronger reasons: (1) it reuses machinery we already had rather than adding
Arroyo's raw-batch retention as a second model, and (2) Arroyo's raw-store
approach was built partly to support aggregates whose state is not cleanly
mergeable — a generality we do not need yet. If we later add a non-mergeable
aggregate, we would revisit and likely adopt Arroyo's retain-and-recompute model
for that case.
