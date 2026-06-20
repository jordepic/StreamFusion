# Benchmarks

Acceleration claims in this project are measured, not asserted. This is where the
method and the numbers live.

## Native operator micro-benchmarks

[`native/benches/operators.rs`](../native/benches/operators.rs) measures each native
operator's steady-state hot loop over an in-memory Arrow batch, isolated from the JVM
bridge and from Flink's job scheduling (which otherwise dominates and hides operator
cost). Built on [Criterion](https://github.com/bheisler/criterion.rs).

Run:

```bash
cd native && cargo bench
```

Criterion reports time per batch with a confidence interval and compares against the
previous run, so a regression in a hot loop is visible commit-to-commit. Each bench
declares its row count as throughput, so Criterion also prints elements/s.

Current benches:
- `filter/gt_literal` — the compiled-predicate filter (`v > 0`) over a 4096-row batch,
  half passing. The predicate is compiled once before the loop, so this measures
  evaluation + the Arrow filter kernel, not planning.
- `tumbling/sum_update_flush` — a tumbling `SUM` over 16 windows: one `update` of a
  4096-row batch followed by a `flush` of all closed windows, from fresh state each
  iteration.
- `tumbling/sum_keyed_update_flush` — the same, grouped by a bigint key (64 distinct
  values), so it exercises the per-row grouping-key path the unkeyed bench does not.

### Results

Numbers are only comparable within a machine; record the host (CPU) alongside.

**Apple M1 Max** (median of 100 Criterion samples):

| Benchmark | Rows/batch | Time/batch | Elements/s | Notes |
|---|---|---|---|---|
| `filter/gt_literal` | 4096 | 2.56 µs | ~1.60 Gelem/s | compiled predicate, ~50% selectivity |
| `tumbling/sum_update_flush` | 4096 | 110 µs | ~37.4 Melem/s | 16 windows, no key |
| `tumbling/sum_keyed_update_flush` | 4096 | 262 µs | ~15.7 Melem/s | 16 windows, 64 bigint keys |
| `session/sum_keyed_update_flush` | 4096 | 2.70 ms | ~1.5 Melem/s | gap merge, 64 bigint keys |

The gap between filter and aggregation is the signal: the filter is a compiled
expression plus one Arrow kernel, while the tumbling aggregator groups every row by a
`GroupKey` (`Vec<ScalarValue>`) — and the keyed case still costs ~2.4× the unkeyed one.
Profiling-driven cuts so far (tumbling, 4096-row batch):

- reusing the per-row window buffer instead of allocating one per row (244 → 181 µs);
- moving the row's key into its last window instead of cloning it for every window
  (181 → 171 µs unkeyed, 395 → 323 µs keyed);
- a fast hash (`ahash`) for the grouping map instead of the stdlib SipHash
  (171 → 110 µs unkeyed, 323 → 262 µs keyed).

Net so far: the unkeyed path is ~2.2× faster (244 → 110 µs) and the keyed path ~1.5×
(395 → 262 µs). The remaining per-row `GroupKey` allocation is the next target
(row-format or dictionary-encoded keys) — see the [profiling
ticket](../.claude/todos/20-profiling-and-benchmarks.md).

The session aggregator is ~10× slower per element than tumbling because its `update`
slices the value column one row at a time (a `take` per row) rather than once per group;
batching that slice is its first optimization target. Its grouping hash is a negligible
fraction here, so the `ahash` swap was deliberately not applied to it.

## End to end vs. Flink

`ThroughputBenchmark` (opt-in: `SF_BENCHMARK=true mvn test -Pbench -Dtest=ThroughputBenchmark`)
runs the same query over a large generated source (5M rows; override with `SF_ROWS`) into a
sink, once with native substitution installed and once on stock Flink, single slot. It reports
best-of-3 rows/s for each and the native/Flink ratio. A warmup run absorbs JIT and minicluster
startup so the measured runs reflect execution.

**The `-Pbench` profile is mandatory** — it builds and loads the *release* native library.
Without it, `mvn test` uses the debug build (fast to compile, ~10–20× slower to run), which
makes every native number misleadingly low. (Measured: the columnar copy below ran 0.48× on
the debug build and 3.0× on release — same code.)

| Operator | Query | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.33 M rows/s | 4.23 M rows/s | **3.19×** |
| Tumbling | `SUM` by 1s window | 1.48 M rows/s | 1.79 M rows/s | **1.21×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.16 M rows/s | 1.22 M rows/s | **1.05×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 2.82 M rows/s | 2.34 M rows/s | **0.83×** |

Native wins where it does real columnar work: the fully-columnar copy is **3.19×**, a
tumbling aggregate **1.21×** (despite a transpose still at its input), the row-fed sink is
par (**1.05×**), and only the trivial stateless filter is below 1× (**0.83×**) — a single
cheap predicate does not earn back the lone operator's `RowData → Arrow → RowData`
round-trip; it crosses 1× once fed by a columnar source or chained with other native
operators (no transpose between them).

### How we got these numbers (a profiling lesson)

The first end-to-end numbers were *far* worse — the columnar copy measured **0.45×**, which
made no sense for a zero-transpose pipeline. Rather than tune blindly, we profiled, and the
chain of measurements is worth recording:

1. **Pure-native ceiling**: a Rust-only Parquet copy of 5M rows ran in **0.36s (14 M rows/s)**
   — so native compute was never the bottleneck; the JVM job was ~13× slower than the compute.
2. **Fixed vs. variable**: at 100K rows native and Flink tied (~0.66s, all fixed job overhead);
   the gap only appeared at scale, so it was a per-row/per-batch cost.
3. **Component timing**: the sink's `Native.writeParquet` dominated (**5.8s of 7.3s**), ~17×
   slower per batch than the *same* native write standalone. Export/serialization were
   negligible (the operators chained, so no IPC).
4. **GC ruled out**: a `-verbose:gc` run showed exactly **one** 5.7ms pause — not GC.
5. **Root cause**: the Maven build loaded the **debug** native library (`cargo build`, no
   `--release`). Debug Rust on Parquet byte-encoding is ~10–20× slower. Building release
   (`-Pbench`) moved the copy from **0.45× to 3.19×** — same code.

The lesson is baked into the harness: benchmarks must run under `-Pbench` (release), and
`mvn test` keeps the fast debug build for the correctness loop only.
