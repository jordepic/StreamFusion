# Two-phase local/global window aggregation

**Kind:** structural — an extra operator and a mergeable partial intermediate
that Arroyo's architecture does not have.
**Diverges from:** Arroyo.
**Forced by parity:** yes (it is the plan shape Flink hands us).

## Their decision
Arroyo aggregates a window in a **single operator**. It shuffles *raw rows* by
key across the network, and the one aggregating operator runs DataFusion's
partial/finish phases *internally* (purely a local CPU optimization). There is no
partial aggregate state crossing the network boundary, and no separate global
merge operator.

## What we do instead
We mirror Flink's two-operator split: a native `LocalWindowAggregate` that emits
Flink's **exact** partial intermediate (`[key, partial0…partialN-1, slice_end]`)
*before* the shuffle, Flink's own hash exchange by key, then a native
`GlobalWindowAggregate` that merges those partials. The intermediate schema is
preserved byte-for-byte so Flink's shuffle wiring is untouched.

## Why
Under default planning Flink's optimizer produces `LocalWindowAggregate →
exchange → GlobalWindowAggregate`, not a single node. To substitute transparently
we have to match that structure: we can only replace nodes the optimizer actually
emits, and the partial intermediate that rides the exchange is part of Flink's
contract. Adopting Arroyo's single-stage model would mean re-planning the query
and re-deriving the shuffle, which is exactly the kind of host-plan rewriting a
Comet-style guest avoids.

## Scope / consequences
- Aggregates with multi-field partial state (integer `AVG`, see
  [01](01-integer-truncating-avg.md)) are not split; they fall back or run
  single-phase.
- Session windows are exempt: Flink itself refuses to two-phase them, so we only
  ever see (and substitute) the single combined node there.
- The end result equals a one-phase aggregation; this divergence is about
  *plan shape*, not output.
