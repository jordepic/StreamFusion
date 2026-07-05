# The opt-in distinct-agg split chain (IncrementalGroupAggregate)

**Status:** WONTDO (2026-07-05). Extracted from the mini-batch coverage ticket (41), where the
decision was left pending between faithful replication and chain-lowering.

**What it is.** With `table.optimizer.distinct-agg.split.enabled` (default **off**), a distinct
aggregate under mini-batch plans as the five-node chain `PartialLocal → Exchange(keys + bucket) →
IncrementalGroupAggregate → Exchange(keys) → FinalGlobal`, over a `Calc` computing
`MOD(HASH_CODE(x), 1024)`. The bucket key spreads one grouping key's distinct state across
subtasks. StreamFusion has no native path for the chain, so enabling the knob drags the query to
the host (all-or-nothing gate); the default no-split plan runs fully native, filters included.

**Why we're not building it.**

- **The knob mitigates a Flink-specific bottleneck.** The split exists because Flink keeps a hot
  grouping key's distinct MapView in per-key state (RocksDB in production), where one subtask pays
  a per-record state get/put against an ever-growing map. Our distinct state is an in-process
  ahash set probed by borrowed key bytes with an i64 fast path — per-record cost is constant and
  does not degrade with set size the way a state-backend map does.
- **The hot-key workload is already measured, and we win without the split.** Nexmark q15/q16
  group by `day` — a SINGLE live grouping key carrying every record's bidder/auction distinct
  sets, the exact skew the knob targets. In the tuned (mini-batch on both engines, 5M events)
  matrix the native no-split plan runs q15 at 1.28x and q16 at 1.23x Flink's throughput.
- **Faithful replication is a large build for an off-by-default knob**: a new stateful incremental
  operator (two key spaces, partial-distinct merge) plus native byte-parity `HASH_CODE` for the
  bucket `Calc` — without which the Calc itself falls back and drags the query anyway.
- **Chain-lowering (matching the five nodes and lowering them onto the no-split native pair)**
  would produce byte-identical final values — the bucket key never changes results — but silently
  discards a topology the user explicitly opted into, and needs multi-node rewrite machinery our
  per-node substitution deliberately doesn't have.

**Cost of the decision:** enabling the split with StreamFusion means host execution (correct, just
not accelerated), reported precisely by the fallback summary and documented in
`docs/coverage-and-fallbacks.md` (§1 `IncrementalGroupAggregate`).

**Revisit if:** a real deployment shows the native distinct set saturating one subtask at
parallelism where Flink-with-split keeps up — i.e. evidence the *shuffle-level* skew (not the
state layer) is the bottleneck — or the columnar exchange grows its own skew-spreading story.
