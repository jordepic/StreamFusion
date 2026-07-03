# Native Kafka source (Rust consumer → Arrow) — built and now the fast path; remaining gaps

**Status:** BUILT and, as of 2026-07-02, **decisively faster than the shallow path** on every
format — the 2026-06-25 shelving verdict is reversed. The re-profiling found the delivery thread
(not the app thread) was the ceiling, and four fixes flipped it: the `alloc-override` mimalloc
feature (librdkafka's per-message op calloc/free), `check.crcs` following librdkafka's default,
the callback-queue drain + inline decode (no decode thread), and metadata warm-up before assign
(a cold assign lost ~0.5s to leader-query). Numbers, per-thread profile, and the reasoning live in
`divergences/19-kafka-consume-fast-path.md` and `docs/optimizations.md`. The production split
reader on 10M msgs: Avro 5.21M/s vs shallow 4.11M/s (1.27x); JSON 3.87M/s vs 2.75M/s (1.41x);
raw consume 1.21x the Java client — all within noise of a hand-rolled ideal consume loop.
Next validation step: rerun the Nexmark Kafka ladder/matrix with the fast path.

The planner gate (`kafkaSource`) still defaults to **false**. Remaining work before flipping it,
roughly ordered:

## Productionize the fast path
- **Replace `alloc-override` with a targeted librdkafka→mimalloc redirect.** The process-wide
  zone swap is benchmark-grade only: it crashed SIGSEGV in `mi_thread_init` under a full Nexmark
  run (racy when the dylib is dlopen'd into a JVM concurrently creating threads), so it must not
  ship as the default. The safe design: compile the bundled librdkafka's allocation calls against
  `mi_malloc`/`mi_calloc`/`mi_free`/`mi_realloc`/`mi_strdup` directly (CFLAGS define-redirect or a
  build.rs patch of rdkafka-sys's mklove build), linking plain libmimalloc-sys WITHOUT the
  override feature — only librdkafka's allocations move, no zone registration, works identically
  on Linux (no `-Bsymbolic` questions). Re-measure the +19%, then decide whether `kafka` implies it.
- **Multi-broker parallel fetch** remains unmeasured (single-broker Testcontainers only).
  librdkafka fetches per-broker on parallel threads; the Java client is one network thread. This
  can only widen the native win, but confirm on a 3-broker cluster before quoting it.
- **rdkafka crate bump** (0.36 bundles librdkafka 2.3.0; current is 2.12.x) — retest after.
- **Upstream librdkafka ARM hardware CRC32C** (`crc32c.c` is x86 SSE4.2 only; 2.12.1 still says
  "FIXME: Hardware support on ARM"). Would let us re-pin `check.crcs=true` at ~no cost on ARM.

## Feature gaps (fall back to the shallow path today)
- **Watermarks/event-time**: the source emits `noWatermarks()`; per-partition watermarking +
  idleness matching Flink's model is not wired.
- **Startup modes**: specific-offsets and topic-pattern subscribe fall back.
- **Formats**: `key.format` / multi-format tables fall back.
- **SASL/SSL at runtime**: the bundled librdkafka is built without SSL/SASL (our clusters are
  PLAINTEXT; Kerberos is HDFS-only). A SASL/SSL cluster needs the `ssl` + `gssapi-vendored`
  rdkafka features (heavier build); the translator already emits the config.
- **wakeUp()** is a no-op (bounded poll timeout makes it unnecessary); revisit if poll timeouts grow.

## Config parity (delivered, for reference)
`kafka.KafkaConfigTranslator` (unit-tested, no broker): Flink consumer `Properties` → librdkafka
map or a logged fallback reason. Pins Java defaults for silent-divergence keys (`isolation.level`,
`allow.auto.create.topics`, connection timing keys) — deliberately NOT socket buffers or
`check.crcs` (see divergences/19 and the translator's comments); renames/value-maps
(`fetch.max.wait.ms`, `sasl.mechanism`, `auto.offset.reset` incl. `by_duration` → fallback);
parses PLAIN/SCRAM + Kerberos JAAS; converts JKS/PKCS12 → temp PEM; falls back on unrecognized
login modules / JSSE-only keys / no-analog keys. Anything untranslatable routes the table to the
shallow path — never a silent mis-translation.
