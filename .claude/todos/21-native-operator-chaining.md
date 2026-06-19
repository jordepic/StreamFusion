# Native operator chaining (keep columnar between native operators)

**Status:** design — needs architecture sign-off before the build.
**Source:** the end-to-end benchmark ([ticket 20](20-profiling-and-benchmarks.md)):
single-operator substitution is *slower* than Flink (filter 0.58×, tumbling 0.81×)
because every substituted operator converts `RowData → Arrow` on the way in and back
on the way out. The native compute is fast; the per-boundary conversion is the tax.
Chaining removes the conversions *between* adjacent native operators, which is the
only way the project beats Flink.

## The problem with the current model
Each native operator is its own Flink `OneInputStreamOperator<RowData, RowData>`. Two
adjacent native operators therefore convert Arrow→RowData (upstream out) then
RowData→Arrow (downstream in) for nothing — the data was already columnar. The ratio
rises with compute per row (filter → tumbling), confirming the conversion is a fixed
per-row cost that only amortizes when more work rides on each converted batch.

## How Comet does it (and why we can't copy it directly)
Comet replaces operators with columnar (`CometExec`) equivalents, then a transition
pass inserts `ColumnarToRow`/`RowToColumnar` **only** where a columnar operator meets a
row operator. Adjacent Comet operators exchange `ColumnarBatch` and never convert. This
rides on Spark's built-in columnar execution framework (`ColumnarRule`,
`supportsColumnar`, `ColumnarBatch` as a first-class exchange type).

**Flink has no equivalent.** Its runtime is row-based (`RowData`/`StreamRecord`); there
is no `supportsColumnar` contract and no framework that auto-inserts transitions. So we
cannot lean on the engine the way Comet leans on Spark — we have to provide the
columnar boundary ourselves.

## Decision — fuse, don't exchange
Two ways to keep data columnar between native operators:

1. **Columnar stream element + transitions.** Make Arrow a `StreamRecord` payload type
   between native operators and insert conversion operators at native↔host boundaries —
   a hand-built version of Comet's transition pass. Cost: a columnar record type,
   serializers for it (Arrow IPC across any non-chained edge), and transition
   insertion in the planner. Fights Flink's row-based runtime at every turn.
2. **Fuse a native subtree into one Flink operator** (chosen). The planner replaces a
   maximal **shuffle-free connected component** of native-able operators with a *single*
   Flink operator that runs the whole sub-pipeline in Arrow internally, converting
   `RowData → Arrow` once at the component's outer input and `Arrow → RowData` once at
   its outer output. Within the operator there is no boundary, so no inter-op conversion.
   This is Comet's "native subtree as one `CometExec`" mapped onto Flink without needing
   a framework columnar API.

Fusion is chosen because it works *with* Flink's row-based operator model (one operator
in, one operator out, RowData on the wire) and needs no columnar serializer until we
tackle the shuffle. It also yields the maximal win (zero conversion inside the subtree).
This is a deliberate divergence from Comet's framework-assisted transitions — see the
divergences note.

## What anchors the columnar region — the source and sink formats
The conversion is a tax whose *placement* is dictated by the endpoint formats, not by the
operator chain alone. A columnar source (Iceberg/Parquet) hands us Arrow with no
transpose — Flink would instead deserialize it to `RowData`, so reading it columnar both
saves that and starts the native region for free. A columnar sink consumes Arrow with no
transpose. So **run columnar whenever an endpoint is already columnar, and place the
single row↔columnar transpose at each row-based endpoint:**

- **columnar → columnar** (Iceberg → Iceberg): fully columnar, no transpose — the biggest
  win, and where Flink is weakest (it round-trips through `RowData`).
- **row → columnar** (Kafka → Iceberg): transpose row→Arrow once *at the source*, stay
  columnar through to the sink.
- **columnar → row** (Iceberg → Kafka): stay columnar from the source, transpose
  Arrow→row once *before the sink*.
- **row → row** (Kafka → Kafka): both ends are row-based, so going columnar costs two
  transposes; only worth it if the native compute saves more than both. Not the default.
- A **non-native operator** in the middle breaks the region: transpose to row for it, and
  back to columnar after it if a downstream columnar region remains.

This is the same logic as Comet — Comet's columnar region is anchored by its columnar
Parquet scan/write. We generalize it: anchor on whichever endpoints are columnar. It also
explains the benchmark ([ticket 20](20-profiling-and-benchmarks.md)): an in-memory row
source into a discarding sink is the row→row case — the *worst* one, paying conversion
with no columnar endpoint to amortize it (hence 0.58×). The favorable cases need a
columnar endpoint, which means native columnar source/sink support is on the critical
path, not just operator fusion.

## Phased plan (each green + parity-tested + benchmarked vs Flink)
1. **Stateless fusion (mechanism).** Fuse a chain of stateless native ops (e.g. `WHERE`
   feeding a projection) into one operator that applies the sequence to each Arrow batch.
   The planner grows from per-node substitution to per-connected-component. This proves
   the fusion machinery; with row endpoints it removes the *inter*-operator transpose but
   not the entry/exit ones, so expect it to help the ratio without necessarily crossing 1×.
2. **A columnar endpoint (the real win).** Read a columnar source (Iceberg/Parquet) as
   Arrow natively — a DataFusion scan instead of Flink deserializing to `RowData` — so the
   native region *starts* columnar with no source transpose, and/or write a columnar sink
   from Arrow. This is where Flink is weakest and where the ratio should cross 1×. The
   transpose lives only at any row-based endpoint (Kafka), per the cost model above.
3. **Stateless prefix into a stateful window.** Fuse a filter/projection into a window
   aggregate so the native filter runs on each input batch before aggregating, all in
   Arrow — compounding with a columnar source.
4. **Arrow across the shuffle.** Two-phase aggregation has a `keyBy` between local and
   global; carry Arrow over that network edge (Arrow IPC) so local→global stays columnar.
   Hardest phase; gated on the others proving out.

## Acceptance criteria
- A query with two adjacent native operators substitutes as one fused native operator,
  with RowData↔Arrow conversion only at its outer boundaries (verify via the plan).
- A columnar source/sink keeps the region columnar end-to-end with the transpose only at
  row endpoints.
- Identical results to Flink across the parity suite.
- `ThroughputBenchmark` shows a columnar-anchored pipeline crossing 1× vs Flink.
