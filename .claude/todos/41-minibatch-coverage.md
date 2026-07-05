# Mini-batch coverage: the windowed two-phase decimal/narrow split

**Status:** one remainder. Everything else on this ticket shipped (2026-07-03 → 2026-07-05): the
whole ordinary two-phase family, distinct aggregates in the default no-split plan (filters
included, one native set per (args, filter) pair), two-phase decimal SUM/MIN/MAX/AVG, narrow AVG
types, string MIN/MAX partials, row-time mini-batch, retraction-bearing partials (count1 record
counter), and the tuned-Flink benchmark column — all ten changelog-family queries run native
under production mini-batch tuning (docs/benchmarks.md carries the table). The opt-in
`distinct-agg.split.enabled` incremental chain was closed as a deliberate non-goal
(`wontdos/52-distinct-split-chain.md`).

**Remaining: the windowed two-phase decimal/narrow split.** The native *windowed* local emits
single-field partials — `flush_partial` takes each accumulator's first state field, matching
Flink's one-column accumulators — but our decimal accumulator keeps a two-field `(sum, count)`
state, and the int/narrow wrapping accumulators are two-field for the same empty-vs-zero reason.
So the windowed local/global split stays gated to bigint (hopping local) / bigint+double
(window-attached local), even though the single-phase windowed path carries every numeric.
Carrying decimal/int/narrow/float through the split needs split-specific single-field partial
accumulators whose state is the nullable sum alone (NULL = empty **or** overflowed — Flink's own
accumulator representation) with skip-NULL merge (Flink's merge expression), plus the matcher
widening on both halves. Parity tests must cover the empty-bundle NULL partial and the decimal
overflow-then-merge path.
