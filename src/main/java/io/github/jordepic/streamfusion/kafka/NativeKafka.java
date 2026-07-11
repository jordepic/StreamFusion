package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.NativeExtensionLoader;

/** JNI entry point for the optional native Kafka source. */
public final class NativeKafka {

  static {
    NativeExtensionLoader.load(NativeKafka.class, "kafka");
  }

  private NativeKafka() {}

  public static native boolean featureBuilt();

  public static native long openKafkaConsumer(String[] configKeys, String[] configValues);

  public static native void assignKafkaSplits(
      long handle, String[] topics, long[] partitions, long[] startOffsets);

  public static native void unassignKafkaSplits(long handle, String[] topics, long[] partitions);

  public static native int pollKafkaBatch(long handle, int maxRecords, long timeoutMillis);

  public static native int drainKafkaSplit(
      long handle, long[] splitMeta, String[] outTopic, long outArrayAddress, long outSchemaAddress);

  public static native void closeKafkaConsumer(long handle);

  public static native long benchmarkKafkaConsume(
      String brokers, String topic, long schemaArrayAddress, long schemaAddress, long maxMessages);

  public static native long benchmarkNativeConsume(
      String[] configKeys,
      String[] configValues,
      String topic,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      int schemaId,
      long maxMessages);

  public static native long benchmarkNativeConsumeSerial(
      String[] configKeys,
      String[] configValues,
      String topic,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      int schemaId,
      long maxMessages);

  public static native long benchmarkConsumeOnly(
      String[] configKeys, String[] configValues, String topic, long maxMessages);
}
