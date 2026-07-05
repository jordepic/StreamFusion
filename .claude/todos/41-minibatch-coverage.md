# Mini-batch coverage: the windowed two-phase decimal/narrow split

**Status:** one remainder. Everything else on this ticket shipped (2026-07-03 → 2026-07-05): the
whole ordinary two-phase family, distinct aggregates in the default no-split plan (filters
included, one native set per (args, filter) pair), two-phase decimal SUM/MIN/MAX/AVG, narrow AVG
types, string MIN/MAX partials, row-time mini-batch, retraction-bearing partials (count1 record
counter), and the tuned-Flink benchmark column — all ten changelog-family queries run native
under production mini-batch tuning (docs/benchmarks.md carries the table). The opt-in
`distinct-agg.split.enabled` incremental chain was closed as a deliberate non-goal
(`wontdos/52-distinct-split-chain.md`).

**Remaining: the windowed two-phase decimal/narrow split.** (Not a mini-batch item — the windowed
local/global split occurs whenever the planner two-phases a window; it lives here because it
surfaced during the decimal two-phase work.) The native *windowed* local emits single-field
partials — `flush_partial` takes each accumulator's first state field — but our custom SUM
accumulators (int32 wrapping, narrow wrapping, float, decimal) keep a two-field `(sum, count)`
state for empty-vs-zero and overflow tracking. So the windowed split stays gated to bigint
(hopping local) / bigint+double (window-attached local), even though the single-phase windowed
path carries every numeric.

Scoping notes (2026-07-05):
- **MIN/MAX/COUNT over every type are nearly free**: they build DataFusion single-field builtin
  accumulators already — widening `allValueTypes`/`allPartialsMergeable` (and the global's gate)
  admits them with no native change.
- **The SUMs need split-mode accumulators, and the parity semantics must be host-probed first.**
  Flink's accumulator is the nullable sum alone: NULL = empty *or* overflowed. Its merge
  expression **skips** a NULL partial — an overflowed bundle's contribution silently vanishes and
  the merged result can be non-NULL — where our single-phase sticky-overflow latch would report
  NULL. Its accumulate likewise **resets** after overflow (`isNull(sum) → sum = value`). Both
  behaviors need parity tests against the host before implementation (they also bear on whether
  the single-phase sticky latch diverges on overflow-then-add — untested today).
- Since the local→global segment is native-internal (all-or-nothing island), the wire format can
  also carry our own two-field pairs instead — but then the global's merge must still *reproduce*
  Flink's skip-NULL outcome, and bundle boundaries only align with the host's under matched
  mini-batch settings, so the single-field Flink-mirroring design is the safer parity bet.
- One `TumblingAggregator` handle serves the single-phase, local, and global operators (only the
  update/flush calls differ), so split-mode accumulator construction needs a creator flag through
  the JNI.
