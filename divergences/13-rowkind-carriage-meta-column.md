# 13 — Carry RowKind as a four-way byte column, not Arroyo's two-way flag

## The influences' decisions
Flink carries a row's changelog kind as **per-row metadata on `RowData`** — a
four-way `RowKind` enum (`+I` INSERT, `-U` UPDATE_BEFORE, `+U` UPDATE_AFTER,
`-D` DELETE). It is not a column; operators read/set it via `getRowKind`/`setRowKind`.

Arroyo carries change information **in the batch**, but as a **two-way** flag: an
appended `UPDATING_META_FIELD` struct whose `is_retract` boolean says only
retract-vs-append. Its runtime never distinguishes `-U` from `-D`.

## What we do instead
Arrow has no per-row metadata, so to move a changelog stream across the row↔Arrow
boundary the kind has to become a column. We append a hidden byte column
(`$row_kind$`) holding `RowKind.toByteValue()` — the **full four-way** kind — and
restore it onto each row on the way back. Carriage is opt-in: insert-only paths
omit the column entirely and read back as `+I`, so nothing changes for them.

## Why four-way, not Arroyo's two-way
Parity. Research §6 flags the 2-vs-4 width gap as a silent-correctness hazard:
**upsert sinks drop `-U` but act on `-D`**, so a two-way encoding that collapses
them produces wrong results against an upsert sink. Since the prime directive is
byte-for-byte parity with Flink — which is four-way — the boundary encoding must
preserve all four kinds. A byte column is the cheapest faithful representation and
keeps the data schema unchanged (the kind rides alongside, not among, the fields).

This is the representational foundation for the native changelog operators that build on it: the
non-windowed `GROUP BY` aggregate, the regular (updating) INNER join, and append-only streaming
Top-N all honor and emit these kinds.
