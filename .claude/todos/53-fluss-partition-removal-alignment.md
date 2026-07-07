# 53: Align the Fluss partition-removal plumbing with the connector's own shape

*(The original body of this ticket — regenerating the pushed watermark in the native Fluss source —
shipped: the Kafka watermark machinery was shared (`planner/ScanWatermarkSpec`,
`operator/NativeSourceWatermarks`), the Rust reader reports per-batch max rowtimes, and the Nexmark
Fluss rung runs the windowed queries via the watermarked table + sentinel-event calibration. See
`docs/coverage-and-fallbacks.md` (Fluss watermark shapes) and `docs/benchmarks.md`. What remains is
the review-cleanup item below.)*

The partition-removal plumbing diverges from Fluss's own shape without a reason: Fluss keeps
the "which splits belong to the removed partitions" question **in the split reader**
(`FlinkSourceFetcherManager.removePartitions` enqueues one task; `FlinkSourceSplitReader
.removePartitions(Map)` answers from its subscribed-bucket state and returns the buckets to
ack). Ours instead grew an `assignedSplits` map in `NativeFlussSourceReader` plus a custom
`removeSplitsAndAck` two-step in the fetcher manager — duplicated split bookkeeping the JNI
reader already holds natively (`split_ids`). Aligning to the Fluss shape deletes the reader-side
map and the two-phase ack task. Mechanism is pinned by the harness drop tests, so refactor
under green.
