package io.github.jordepic.streamfusion.format.csv;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native CSV format implementation. */
public final class NativeCsvFormat {

  static {
    NativeExtensionLoader.load(NativeCsvFormat.class, "csv");
  }

  private NativeCsvFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  static native long createDecoder(
      long schemaArrayAddress, long schemaAddress, boolean skipParseErrors, String formatOptions);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
