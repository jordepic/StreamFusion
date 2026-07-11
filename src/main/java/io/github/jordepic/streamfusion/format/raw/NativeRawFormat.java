package io.github.jordepic.streamfusion.format.raw;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native raw format implementation. */
public final class NativeRawFormat {

  static {
    NativeExtensionLoader.load(NativeRawFormat.class, "raw");
  }

  private NativeRawFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  static native long createDecoder(long schemaArrayAddress, long schemaAddress);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
