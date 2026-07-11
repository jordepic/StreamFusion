package io.github.jordepic.streamfusion.format;

/**
 * A native implementation of one Flink value format. Format artifacts register providers with Java's
 * {@link java.util.ServiceLoader}; connectors use this SPI rather than taking a dependency on every
 * format they may carry.
 */
public interface NativeFormatProvider {

  String formatIdentifier();

  boolean honorsProjection();

  boolean supportsIgnoreParseErrors();

  /** Returns whether this artifact supports the table's exact format options. */
  boolean supports(NativeFormatContext context);

  NativeMessageDecoderFactory createDecoder(NativeFormatContext context);
}
