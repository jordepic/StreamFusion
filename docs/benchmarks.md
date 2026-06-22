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
- `session/sum_keyed_update_flush` — a session `SUM` grouped by key (gap merge).
- `over/running_sum_keyed`, `over/row_number_keyed` — the columnar `OVER` push+flush, for a
  running `SUM` (DataFusion accumulator per row) and `ROW_NUMBER` (per-key counter).
- `interval_join/equi_key_push`, `window_join/equi_key_flush` — the two joins with a unique key
  (1:1 match, no cross product), so they measure the DataFusion hash-join construction per batch.

### Results

Numbers are only comparable within a machine; record the host (CPU) alongside.

**Apple M1 Max** (median of 100 Criterion samples):

| Benchmark | Rows/batch | Time/batch | Elements/s | Notes |
|---|---|---|---|---|
| `filter/gt_literal` | 4096 | 2.56 µs | ~1.60 Gelem/s | compiled predicate, ~50% selectivity |
| `tumbling/sum_update_flush` | 4096 | 106 µs | ~38.6 Melem/s | 16 windows, no key |
| `tumbling/sum_keyed_update_flush` | 4096 | 252 µs | ~16.3 Melem/s | 16 windows, 64 bigint keys |
| `interval_join/equi_key_push` | 4096 | 100 µs | ~41 Melem/s | INNER, 1:1, equi-key + interval filter |
| `window_join/equi_key_flush` | 4096 | 175 µs | ~23 Melem/s | INNER, 1:1, equi-key + window bounds |
| `over/row_number_keyed` | 4096 | 532 µs | ~7.7 Melem/s | per-key counter, 64 keys |
| `over/running_sum_keyed` | 4096 | 1.56 ms | ~2.6 Melem/s | per-row running aggregate, 64 keys |
| `session/sum_keyed_update_flush` | 4096 | ~3 ms | ~1.4 Melem/s | gap merge, 64 keys (high-variance) |

The gap between filter and aggregation is the signal: the filter is a compiled
expression plus one Arrow kernel, while the tumbling aggregator groups every row by a
`GroupKey` (`Vec<ScalarValue>`) — and the keyed case still costs ~2.4× the unkeyed one.
Profiling-driven cuts so far (tumbling, 4096-row batch):

- reusing the per-row window buffer instead of allocating one per row (244 → 181 µs);
- moving the row's key into its last window instead of cloning it for every window
  (181 → 171 µs unkeyed, 395 → 323 µs keyed);
- a fast hash (`ahash`) for the grouping map instead of the stdlib SipHash
  (171 → ~106 µs unkeyed, 323 → ~252 µs keyed).

Net so far: the unkeyed path is ~2.3× faster (244 → ~106 µs) and the keyed path ~1.6×
(395 → ~252 µs). The remaining per-row `GroupKey` allocation is the next target
(row-format or dictionary-encoded keys) — see the [profiling
ticket](../.claude/todos/20-profiling-and-benchmarks.md).

The two slow operators both fold **per row**. The running `OVER` aggregate
(~2.6 Melem/s) drives the DataFusion accumulator one row at a time — a `take` +
`update_batch` + `evaluate` per row — rather than batching per group; a specialized running
fold is its optimization target. The session aggregator (~1.4 Melem/s, high-variance) merges
open windows over a per-key `BTreeMap` and likewise slices the value column one row at a time.

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
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.32 M rows/s | 3.45 M rows/s | **2.61×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| Tumbling (row source) | `SUM` by 1s window | 1.63 M rows/s | 1.99 M rows/s | **1.22×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.85 M rows/s | 1.04 M rows/s | **1.22×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.22 M rows/s | 1.11 M rows/s | **0.91×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.11 M rows/s | 2.25 M rows/s | **0.72×** |

The gain tracks how much of the pipeline stays columnar. Fully-columnar paths lead — the copy
**2.61×**, the windowed aggregate over a columnar source **1.82×**, the event-time interval join
**1.71×** (Flink's interval join is slow; ours delegates the match to a DataFusion hash join).
Row-source stateful ops land near **1.22×** (tumbling, `OVER`): identical native compute, but a
`RowData → Arrow` transpose still sits at the input. The two below 1× are both **lone row-source
operators** where the transpose tax exceeds the native gain — the Parquet sink **0.91×** (one
boundary transpose ≈ the native write gain) and the stateless filter **0.72×** (a single cheap
predicate cannot earn back the `RowData → Arrow → RowData` round-trip). A lone operator crosses 1×
once fed by a columnar source or chained with other native operators (no transpose between them) —
the native-operator-chaining work in [ticket 21](../.claude/todos/21-native-operator-chaining.md).

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
