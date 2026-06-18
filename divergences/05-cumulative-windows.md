# Cumulative (CUMULATE) window aggregation

**Kind:** net-new operator — built with no reference from either influence.
**Diverges from:** *both* Comet and Arroyo (neither has it).
**Forced by parity:** no — chosen to cover a real Flink window TVF.

## What the influences do
- **Arroyo** implements tumbling, sliding, and session aggregating windows, but
  **no cumulative window** — there is no `cumulative_aggregating_window.rs`.
- **Comet** is a batch Spark accelerator and has no streaming windows at all.

So unlike every other window type we've accelerated, there was no existing
architecture to copy. Cumulative windows (`CUMULATE`) are a Flink window TVF —
nested windows that share a bucket start and grow by a step up to a max size
(`[base, base+step)`, `[base, base+2·step)`, …, `[base, base+maxSize)`).

## What we do, and why this stays *close* to the influences in spirit
We did not invent a parallel structure. Cumulative windows are *aligned* (like
tumbling and hopping), so they reuse the same native aligned-window aggregator and
the same accumulator/checkpoint machinery; only the per-element window assignment
differs (nested ends sharing a start, vs one window per slide). The native side
distinguishes them by a flag set at creation through a dedicated entry point,
exactly the pattern the session operator already uses.

The one structural change this forced: the aligned-window aggregator keyed open
windows by `window_start`, but cumulative windows collide on start. So window
identity became the `(start, end)` pair, keyed by `end` (unique per window), with
the end carried explicitly through the flush. This generalized — and simplified —
the existing tumbling and hopping paths too (the operator no longer derives the
end as `start + size`), and is covered by the full parity + checkpoint suite,
which stayed green through the change.

## Why diverge at all, given "copy the influences" is the default
This is a deliberate exception: the goal here is Flink *feature coverage*, and
`CUMULATE` is a first-class Flink window with real user demand, so "Arroyo
doesn't have it" is not a reason to skip it. We accept owning the design because
(1) it produces identical results to the host, verified by the parity harness,
and (2) it cost almost no new architecture — it is the existing aligned-window
engine with one more assignment mode, not a new subsystem. Two-phase
(slice-sharing) cumulative is **not** done; like two-phase hopping it falls back
(see `.claude/todos/13-two-phase-hopping-slices.md`).
