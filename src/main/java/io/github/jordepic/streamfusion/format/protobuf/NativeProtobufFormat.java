package io.github.jordepic.streamfusion.format.protobuf;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native protobuf format implementation. */
public final class NativeProtobufFormat {

  static {
    NativeExtensionLoader.load(NativeProtobufFormat.class, "protobuf");
  }

  private NativeProtobufFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  static native long createDecoder(
      byte[] descriptor, String messageName, long schemaArrayAddress, long schemaAddress);

  static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  static native void closeDecoder(long handle);
}
