# Nexmark q6 and q13: why they stay outside the accelerating set

**Status:** Documented exclusion (not a bug, not planned). Recorded 2026-07-01.

Of the 24 Nexmark queries, every one that Flink can run, StreamFusion now accelerates fully and by
default (q0–q5, q7–q23; see `NexmarkExplainTest`). Two remain outside the runnable set. Neither is a
StreamFusion fallback — both are properties of the query/engine, not of the native operators — so this
ticket records the reasoning rather than a fix.

## q6 — `AVG(price) OVER (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)` over winning bids

q6 takes the "winning bid" per auction (a retracting Top-1: `ROW_NUMBER() … = 1` over an interval join,
the same core as q4) and averages the last 10 per seller with a row-count-bounded `OVER` window.

**Flink SQL itself cannot execute this.** Flink's `OVER` window operator does not consume retractions,
and the winning-bid Top-N is a retracting (changelog) input — the query file's own comment says so
("this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't support to
consume retractions"). In this Flink build it doesn't even validate (`Column 'rownum' not found`, from
the alias-in-`WHERE` Top-N shape).

**Why StreamFusion can't "accelerate" it.** StreamFusion overrides Flink's *physical plan* with native
operators and verifies every operator byte-for-byte against the host. If Flink produces no plan and no
output, there is nothing to override and nothing to check parity against. Making q6 run would mean
building a retraction-consuming bounded-`OVER` operator that Flink lacks — inventing new streaming
semantics with no host reference — which is a different project from accelerating Flink, and unverifiable
by the parity harness that underwrites correctness here.

## q13 — bounded side-input lookup join (`FOR SYSTEM_TIME AS OF B.p_time`)

q13 enriches the bid stream by joining it, at processing time, against a bounded side-input table
(`JOIN side_input FOR SYSTEM_TIME AS OF B.p_time`). This is a Flink `LookupJoin` / processing-time
temporal join.

**Why it's outside the columnar island.** A lookup join is an async external-I/O operator: for each
probe row it looks the key up in the connector-backed table (Flink's `LookupJoinRunner` + async I/O).
That is the same class of work as a source/sink connector — it belongs to Flink, not to the native
compute island, which accelerates columnar *transforms* over in-flight Arrow batches. The cost of q13 is
the lookup I/O, not CPU, so there is little compute to accelerate even in principle. (In this harness it
also fails to plan — the datagen `side_input` has no primary key, which a temporal join requires; the
real Nexmark uses a filesystem CSV connector with the lookup key. Fixing the DDL would make it plan as a
`LookupJoin`, which the native planner leaves on Flink by design.)

**Reconsider if:** a native, in-memory broadcast/hash-join operator against a *bounded, static*
side table is ever built (load the small table once, probe columnar). That would be a genuine native
lookup-join operator — worth it only if lookup joins show up on a hot path; for Nexmark they do not.
