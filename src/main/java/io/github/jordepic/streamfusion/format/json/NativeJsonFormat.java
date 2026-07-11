package io.github.jordepic.streamfusion.format.json;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native JSON and JSON-CDC format implementation. */
public final class NativeJsonFormat {

  static {
    NativeExtensionLoader.load(NativeJsonFormat.class, "json");
  }

  private NativeJsonFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  static native long createDecoder(
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      boolean skipParseErrors,
      String formatOptions);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
