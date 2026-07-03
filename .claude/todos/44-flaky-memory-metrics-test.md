# Flaky: NativeMemoryMetricsTest in full-suite runs (metric "not registered")

**Status:** TODO (test-infra flake, low priority — but it costs a full-suite re-run whenever it
fires). First observed 2026-07-03: 2 failures across ~5 full `mvn test` runs, never in isolation
(8/8 green when run alone, and green in most full runs).

**Signature:** `nativeFootprintIsExportedAsOperatorMetrics` fails with
`no metric named <X> was registered` where `<X>` varied between runs (`nativeArrowAllocatorBytes`
once, `nativeStateBytes` once) — yet `nativeStateBudgetBytes`, registered in the SAME
`ManagedMemoryBudget.registerMetrics` call as both, WAS found. All three gauges register in one
synchronous call on the task thread at operator open, so a partial registration is not our code's
ordering; the suspicion is Flink's `InMemoryReporter` (createWithRetainedMetrics) under
full-suite conditions — many prior MiniClusters/reporters in the same JVM — either a race in its
removal/retention handling or scope-map interference. One failure happened under heavy external CPU
load, one without.

**Next steps when picked up:** reproduce with a suite-subset bisection (which preceding test
primes it); read `InMemoryReporter`'s retention path for a remove/retain race; if it is the
reporter, either poll-with-timeout in `gaugeValue` (bounded retry is defensible for metrics
delivery) or scope the assertion to the operator group directly.
