package io.github.jordepic.streamfusion.format.avro;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Avro format implementation. */
public final class NativeAvroFormat {

  static {
    NativeExtensionLoader.load(NativeAvroFormat.class, "avro");
  }

  private NativeAvroFormat() {}

  /** Probes that this optional library has loaded. */
  public static native boolean isLoaded();

  public static native long createDecoder(boolean confluent, String writerSchema, String readerSchema);

  public static native void registerWriterSchema(long handle, int schemaId, String schema);

  public static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void closeDecoder(long handle);
}
