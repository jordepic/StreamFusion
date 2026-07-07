> [!NOTE]
> This project is not part of Apache Flink or Apache DataFusion.

# StreamFusion

[![CI](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml/badge.svg)](https://github.com/datafusion-contrib/StreamFusion/actions/workflows/ci.yml)

Run Apache Flink SQL faster by executing supported operators natively (Rust + Apache
Arrow/DataFusion over JNI) while Flink continues to own planning, coordination, and
everything not yet supported. Substitution is transparent and conservative: a query is
planned by Flink, the operators we can reproduce **exactly** are swapped for native ones, and
anything else falls back to Flink with identical results.

It is DataFusion Comet's idea — a native, columnar accelerator behind an unchanged SQL
front end — applied to streaming instead of batch: stateful windowing, joins, aggregations,
changelog processing, and columnar sources/sinks, not just stateless projection and filter.

## What it accelerates

A query accelerates only when it forms **one fully-columnar island**: every operator except a
rowwise source/sink runs natively, exchanging Arrow batches (the row↔Arrow transpose is paid
once at the host edges, never between native operators). A single unsupported interior operator
drags the whole query back to Flink.

Native coverage is broad — most of the streaming SQL surface:

- **Stateless:** projection/`Calc`, filter, `UNION ALL`, `GROUPING SETS`/`CUBE`/`ROLLUP`, `UNNEST`.
- **Windowed aggregates:** `TUMBLE`/`HOP`/`SESSION`/`CUMULATE` (event-time and proctime, one- and
  two-phase), and `OVER` window functions.
- **Joins:** regular (updating) equi-joins, event-time/proctime interval and window joins,
  event-time temporal-table joins, and processing-time lookup joins (sync and async).
- **Changelog:** non-windowed `GROUP BY`, streaming Top-N / `LIMIT`, deduplication, changelog
  normalization — all consuming and emitting a retract changelog.
- **Connectors:** Parquet/ORC file sources and a Parquet sink (native Arrow scan/write); Kafka
  source decode for JSON/CSV/raw/Avro/protobuf and Debezium/OGG CDC — JSON/Avro/protobuf via a
  fully native rdkafka source (the default path; it also regenerates the table's watermark
  per partition, exactly as Flink's source does).
- **UDFs:** a Flink `ScalarFunction` the expression engine can't implement itself is invoked over
  Arrow columns by a native→JVM upcall (Comet's `JvmScalarUdfExpr` pattern), one JNI crossing per
  batch, so the pipeline stays native *through* the UDF and the result is byte-identical.

The exact per-operator terms, and **every** condition that causes a fallback (unsupported
operators, types, expressions, and connector options), live in
**[docs/coverage-and-fallbacks.md](docs/coverage-and-fallbacks.md)** — the single source of truth
for what does and doesn't run natively. The short version of what stays on Flink: lateral table
functions and `MATCH_RECOGNIZE`, PyFlink UDFs, the three-phase distinct aggregate, remote
(`hdfs:`/`s3:`) file paths, a handful of expression/type edges where native execution would
diverge from the JVM (opt-in behind `allowIncompatible`), and connector options we can't yet
reproduce bit-identically (registry-framed Avro, Maxwell/Canal CDC, some protobuf field types).

**Determinism.** Results are byte-identical to stock Flink for everything admitted. The one caveat
is late-data dropping on out-of-order event-time streams, where Flink is itself non-deterministic
(periodic watermarks); we match Flink's deterministic path, which governs in-order data and every
benchmark. Details in [divergences/09](divergences/09-per-batch-watermark-assignment.md).

## Inspiration

StreamFusion is built by porting established engines rather than reinventing operators:

- **[DataFusion Comet](https://github.com/apache/datafusion-comet)** — the model for the whole
  project (native columnar accelerator behind an unchanged SQL planner) and the reference for the
  JNI / Arrow C Data Interface bridge, off-heap memory accounting, the config surface, and
  fallback-reason reporting.
- **[Arroyo](https://github.com/ArroyoSystems/arroyo)** — the streaming-operator implementations
  we port (it already runs on DataFusion); the reference for join/window/changelog logic.
- **[Apache DataFusion](https://github.com/apache/datafusion)** — the native execution and
  expression engine underneath (hash joins, aggregates, Arrow kernels).
- **[RisingWave](https://github.com/risingwavelabs/risingwave)** — the reference for changelog
  semantics and memcomparable arrow-row state encoding.
- **[Apache Flink](https://github.com/apache/flink)** — the **parity target**: every operator is a
  faithful port of Flink's own, verified for identical output by a parity harness.

Divergences from these references are recorded in [`divergences/`](divergences/).

## Nexmark benchmarks

The steelman: the source is the rowwise `nexmark` datagen (wide event row) and the sink is
`blackhole` (also rowwise) — exactly the published Nexmark plan — so a native island pays a
`RowData → Arrow` transpose at the source **and** an `Arrow → RowData` transpose at the sink. Both
transposes are kept in the measured path on purpose: a real deployment feeds rowwise records and
drains to a rowwise sink, so this is the honest end-to-end number. Object reuse is on for both
engines (standard tuned-prod setting).

StreamFusion runs **every runnable Nexmark query** (q0–q5, q7–q23) natively end-to-end with no
fallback and no flags; only q6 stays out, because Flink SQL itself can't run it
([analysis](.claude/wontdos/39-nexmark-q6-exclusion.md)). Native vs. stock Flink, 500K events, on the
recommended `mimalloc` native build (the Kafka feature is a default), from a rowwise `RowData` source, a local Parquet file, a
Fluss log table (the opt-in `fluss` cargo feature), and each Kafka value format, ordered by query number. Both engines run Flink's **default configuration**
(mini-batch off) apart from the object reuse noted above; the mini-batch-tuned comparison is a
separate table in [docs/benchmarks.md](docs/benchmarks.md):

Each Kafka cell is the fully native rdkafka source — Rust owns the consume *and* the decode. Since
the consume fast path ([divergences/19](divergences/19-kafka-consume-fast-path.md)) it beats the
other source rungs (JVM transpose, Rust decode over a JVM poll) on every single query, so the best
rung is always the same rung. Several queries run a byte-parity default with a faster opt-in path that can
diverge from Flink at an edge; where the two differ enough to matter (**q21**) both are shown as
separate rows, and where the opt-in measures within noise (**‡ q1**, **§ q10/q14/q15/q16/q17**) it stays
one row with a footnote.

| Query | Shape | From RowData | From Parquet file | From Fluss | From JSON on Kafka | From Avro on Kafka | From Protobuf on Kafka |
|---|---|---|---|---|---|---|---|
| q0 | pass-through projection of `bid` | **1.33×** | **3.21×** | **2.83×** | **2.71×** | **3.42×** | **2.58×** |
| q1 ‡ | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.13×** | **3.07×** | **2.59×** | **2.39×** | **3.35×** | **2.62×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.30×** | **3.56×** | **2.87×** | **2.04×** | **2.48×** | **2.09×** |
| q3 | updating join `auction ⋈ person` | 0.95× | **3.57×** | **2.27×** | **1.97×** | **2.38×** | **1.80×** |
| q4 | regular join → `MAX` → `AVG` per category | **1.31×** | **3.61×** | **1.51×** ¶ | **2.43×** | **3.27×** | **2.66×** |
| q5 | Hot Items (window re-agg + window join) | **1.47×** | **3.45×** | **1.61×** | **2.32×** | **3.35×** | **3.04×** |
| q7 | tumble `MAX` ⋈ bid | **1.61×** | **4.22×** | **2.48×** | **2.89×** | **4.11×** | **3.21×** |
| q8 | tumble windowed-distinct ⋈ join | 0.87× | **4.37×** | **2.20×** | **2.22×** | **2.94×** | **2.58×** |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | **1.18×** | **1.94×** | **1.03×** ¶ | **2.24×** | **2.13×** | **2.35×** |
| q10 § | `DATE_FORMAT` projection | **1.11×** | **2.54×** | **1.45×** | **2.69×** | **2.64×** | **2.26×** |
| q11 | session-window `COUNT` per bidder | **2.79×** | **5.39×** | **4.17×** | **3.93×** | **5.18×** | **5.55×** |
| q12 | proctime tumble `COUNT` per bidder | **1.52×** | **3.23×** | — ¶ | **2.31×** | **2.55×** | **2.14×** |
| q13 | lookup join (bounded dimension) | **1.07×** | **2.26×** | **1.66×** | **2.20×** | **2.75×** | **2.14×** |
| q14 § | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.02×** | **3.30×** | **1.49×** | **2.47×** | **3.50×** | **2.66×** |
| q15 § | multi-`DISTINCT` `COUNT`s per day (`DATE_FORMAT` group) | **1.42×** | **2.07×** | 0.78× | **2.76×** | **3.06×** | **2.52×** |
| q16 § | multi-`DISTINCT` per channel/day | **1.36×** | **1.37×** | 0.85× | **1.86×** | **1.87×** | **1.65×** |
| q17 § | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | **1.32×** | **2.23×** | 0.84× | **2.49×** | **2.59×** | **2.23×** |
| q18 | `ROW_NUMBER` dedup (≤ 1) | **1.13×** | **2.27×** | **1.28×** | **2.52×** | **3.02×** | **2.58×** |
| q19 | `ROW_NUMBER` topN (≤ 10) | **1.50×** | **1.75×** | 0.97× | **1.98×** | **1.89×** | **1.85×** |
| q20 | updating join (`category = 10`) | 0.84× | **3.40×** | **2.31×** | **2.38×** | **3.40×** | **2.92×** |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.96× | **2.77×** | **2.28×** ¶ | **2.44×** | **2.98×** | **2.64×** |
| q21 † | …opt-in native regex/case (`allowIncompatible`) | **1.54×** | **6.14×** | **5.25×** ¶ | **2.46×** | **3.01×** | **2.62×** |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.18×** | **3.58×** | **1.87×** | **2.49×** | **2.83×** | **2.28×** |
| q23 | three-way join `bid ⋈ person ⋈ auction` | **1.38×** | **3.91×** | **1.41×** | **2.09×** | **2.85×** | **2.38×** |

From `RowData`, 19 of 23 queries win outright — the joins, Top-N/dedup family, projections and
aggregates, lifted across the 2026-07 profiling round (shared rowwise prefix, allocation-free
state probes across every changelog operator and keyed loop, cached changelog emit,
decode-deduplicated Top-N, byte-path parity upcalls) — q14 crossed the line this round (1.02×).
What still trails 1× there: the widest updating join (q20), the transpose-bound window join (q8),
and q3 (0.95×) with q21's byte-parity upcall (0.96×) at the line — the opt-in q21 path clears it
at 1.54×.

**From a local Parquet file the native island reads Arrow straight from the scan** — no `RowData →
Arrow` ingest transpose — so **every query clears the bar**, most by **2–5.4×** across projection,
filter, window, and join (the floor is q16 at 1.37×). This is the columnar-source case the engine is
built for, and the gap between it and the `RowData` column is how much of that column's cost is the
perimeter transpose rather than the operator.

**† q21's opt-in native regex/case** (pure-Rust `regex`/case folding under `allowIncompatible`) is a
real **up to ~2.2× swing** over the byte-parity default (which routes `REGEXP_EXTRACT`/`LOWER` through
Flink's own code via a JVM upcall) — the honest, measured cost of the parity guarantee. The default
clears 1× on every columnar-fed rung; on the bare `RowData` generator it sits at the line (0.96× on
this combined run).
**‡ q1** and **§ q10/q14/q15/q16/q17** also have an opt-in path, but it measures **within noise** of the
default, so they stay one row. q1's is approximate decimal. The `§` queries now run
`DATE_FORMAT`/`HOUR` over the Kafka `TIMESTAMP_LTZ` **natively** (these cells were `—` before): the
default routes the LTZ case through Flink's own zone-aware datetime code via the JVM upcall (byte-parity),
and the opt-in pure-Rust `chrono-tz` path is no faster here because the datetime call isn't the
bottleneck — so parity is free (unlike q21's regex). On `RowData`/Parquet those queries use a plain
`TIMESTAMP` and never take the LTZ path. See
[divergences/17](divergences/17-ltz-datetime-session-zone.md).

**From a Fluss log table the wire format *is* Arrow** — the native fluss-rs reader consumes the
table's log batches directly, no ingest transpose and no decode — and eleven of the fourteen
measured queries beat the stock Fluss connector: projections and filters at **1.4–3.1×** (the
highest absolute native rates of any streaming rung), and the updating joins that trail on the
`RowData` rung (q3 0.95×, q20 0.84×) clearing it decisively here (**1.59×**, **2.29×**). The distinct-agg
family (q15–q17) trails 1× on this rung only — the fluss-rs scanner emits one small Arrow batch
per producer wire batch, and per-batch overhead lands hardest on the changelog-aggregate chain;
batch coalescing is the known follow-up.
**¶** The Fluss rung times *time-to-Nth-row* on an unbounded log (Fluss has no bounded scan
mode), so it skips queries with no deterministic finish line: the windowed/proctime/lookup set
(the benchmark's Fluss table declares no time attribute yet — watermark push-down regeneration
is on the board), q4/q9 (a two-input join feeds an update-collapsing aggregate/rank, so the
changelog row *count* varies with join-input interleaving even between two stock Flink runs),
and q21 (zero output rows over this generator's channels/URLs). All of them stay measured on
the bounded rungs, which run to end-of-input and need no row target.

**From a Kafka source, owning the consume in Rust compounds the operator verdict**: with the native
rdkafka source every query on every format clears **1.65×**, all but a handful clear **2×**, and the
peak is q11's **3.9–5.6×**. Queries whose operators trail on the bare generator (the updating joins,
q21's parity upcall) are pulled well past 1× by the consume+decode saving — even the changelog-bound
q9/q19, which no earlier rung could lift, land at **1.85–2.35×**. And **under production tuning the
margins widen**: with `table.exec.mini-batch.*` enabled on both engines (5M events, the full suite),
native beats the tuned Flink baseline on 20 of 23 — q23 3.01×, q11 3.01×, q4 2.85×, q19 2.36×,
q9 2.15×, q18 2.02× — because the native Top-N emits the net per-batch diff under mini-batch plans
while stateless perimeter costs amortize away. The full per-rung ladder,
method, tuned table, and end-to-end tables are in **[docs/benchmarks.md](docs/benchmarks.md)**.

_Apple M1 Max; numbers are comparable only within a machine._

## Running and configuration

Install acceleration by hooking the planner once (`NativePlanner.install(env)`), then run Flink SQL
as normal. Two things to set in a real deployment:

**Deployment JVM flags** — run the TaskManager JVM with Arrow's safety checks off (as Comet/Spark
do); profiling showed ~1/3 of the transpose CPU was per-accessor bounds/refcount checks:

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

**Configuration** (JVM system properties, mirroring Comet's config surface):

- `-Dstreamfusion.native.enabled=false` — master switch; run entirely on Flink.
- `-Dstreamfusion.operator.<name>.enabled=false` — keep one operator on the host (e.g. leave a lone
  cheap `filter` on a row source, which can't earn back the transpose round-trip).
- `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` — opt into the faster pure-Rust path for
  expressions that otherwise use a byte-exact JVM upcall or fall back (`UPPER`/`LOWER`/
  `REGEXP_EXTRACT`, `ROUND` on float, transcendental math). Off by default (parity-first).
- `-Dstreamfusion.memory.accounting.enabled` (default on) — native stateful operators reserve an
  operator-scope share of the slot's managed memory from Flink's `MemoryManager` and bound their
  state by it, failing with a `NativeMemoryLimitException` naming the remedy rather than an
  unattributed OOM ([divergences/16](divergences/16-upfront-managed-memory-reservation.md)).

**Seeing why a query fell back** — substitution is silent by default. `NativePlanner`'s
`fallbackReasons()` lists each node that stayed on Flink and why; `-Dstreamfusion.logFallbackReasons=true`
logs each reason as it's decided.

**Benchmarks** — the end-to-end suites (`ThroughputBenchmark`, `NexmarkBenchmark`,
`NexmarkKafkaBenchmark`, `NexmarkMatrixBenchmark`) run under `SF_BENCHMARK=true mvn test -Pbench`;
the `-Pbench` profile is required (it loads the **release** native library — the debug build is
~10–20× slower and misleading). The Criterion micro-benchmarks run with `cd native && cargo bench`.
See [docs/benchmarks.md](docs/benchmarks.md).

## Related work

Three native Flink accelerators exist, all **closed source**:

- **Flash** (Alibaba Cloud) — a C++ native + SIMD vectorized engine with a custom state backend
  (ForStDB). Stateful, production-deployed at scale; claims 5–10× on streaming Nexmark, 3×+ on batch
  TPC-DS, and ~50% cost reduction across 100k+ compute units. Proprietary, on Alibaba Cloud.
  ([blog](https://www.alibabacloud.com/blog/flash-a-next-gen-vectorized-stream-processing-engine-compatible-with-apache-flink_602088))
- **Vera X** (Ververica, the original Flink creators) — a proprietary native vectorized engine with
  a drop-in compatibility layer and a new state store. Stateful; claims 5–10× on Nexmark SQL and
  ~52% lower resource usage. Implementation undisclosed.
  ([blog](https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine))
- **Iron Vector** (Irontools) — the same stack as us (Rust + Arrow + DataFusion over zero-copy JNI,
  Substrait plan serialization, transparent fallback), but **stateless only** today (projections,
  filters, expressions); windows, joins, and exactly-once are described as planned. Claims ~97%
  higher throughput on a stateless ETL pipeline.
  ([blog](https://irontools.dev/blog/introducing-iron-vector/))

Where StreamFusion differs: it is **open source**, and every substitution is gated and verified for
identical results against stock Flink by a parity harness rather than asserted. It is already native
on stateful windowing, joins, and changelog processing — the hard, closed part of the field — where
Iron Vector is stateless-only; it is earlier-stage than Flash and Vera X and doesn't match their
operator breadth or published benchmarks, but its acceleration is auditable and parity-first by
construction.

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or
<https://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed
as above, without any additional terms or conditions.
