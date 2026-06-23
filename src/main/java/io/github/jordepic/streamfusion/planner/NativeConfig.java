package io.github.jordepic.streamfusion.planner;

import java.util.Locale;

/**
 * Opt-in configuration for the native planner, read from JVM system properties (like {@code
 * -Dstreamfusion.logFallbackReasons}). Mirrors DataFusion Comet's {@code allowIncompatible} surface:
 * some native expressions are known to diverge from the host (locale-sensitive case folding,
 * {@code BigDecimal} rounding, last-ULP transcendental math). They fall back by default; a user who
 * accepts the divergence — typically because their data avoids the edge — can enable them per
 * function or all at once.
 */
public final class NativeConfig {

  private NativeConfig() {}

  /**
   * Whether an expression whose native result may differ from the host is allowed to run natively.
   * Enabled by the per-function property {@code streamfusion.expression.<NAME>.allowIncompatible} or
   * the blanket {@code streamfusion.expression.allowIncompatible}.
   */
  public static boolean allowsIncompatible(String functionName) {
    return Boolean.getBoolean("streamfusion.expression.allowIncompatible")
        || Boolean.getBoolean(
            "streamfusion.expression."
                + functionName.toUpperCase(Locale.ROOT)
                + ".allowIncompatible");
  }
}
