package io.github.jordepic.streamfusion;

import io.github.jordepic.streamfusion.proto.Row;
import io.github.jordepic.streamfusion.proto.Scalars;
import io.github.jordepic.streamfusion.proto.WithNested;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end tests for the protobuf native-decode path: a {@code CREATE TABLE ...
 * 'connector'='kafka', 'format'='protobuf'} with a {@code protobuf.message-class-name} either routes
 * through the planner to {@link io.github.jordepic.streamfusion.planner.StreamPhysicalNativeKafkaDecode}
 * — Flink consumes the raw messages and a native operator decodes them straight to Arrow against the
 * descriptor reflectively extracted from the generated class — or, for a message the native decode does
 * not reproduce identically, falls back cleanly to Flink's own {@code protobuf} decoder.
 *
 * <ul>
 *   <li><b>Flat scalar message</b> (every signed-int/float/double/bool/string field type): routes
 *       natively. {@link NativeParity#assertParity} compares the native decode against Flink's
 *       {@code protobuf} format row for row — a real host-parity comparison, with messages produced via
 *       the same generated class Flink parses.
 *   <li><b>Message with a nested field</b>: the native row boundary cannot carry a {@code ROW} column
 *       (see ticket 34), so the planner gates it to fall back. {@link NativeParity#assertFallback}
 *       asserts the query stays on Flink and still produces Flink's result.
 * </ul>
 *
 * <p>All fields are set to non-default values, so proto3's missing-field semantics don't enter the
 * comparison. The main module pins protobuf-java 4.x to match Flink's protobuf runtime; the ORC host
 * baseline lives in the {@code orc-baseline} module because the two need incompatible protobuf-java
 * versions. Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka).
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeProtobufDecodeSqlHarnessTest {

  private static final int MESSAGES = 2_000;
  private static final String PKG = "io.github.jordepic.streamfusion.proto";

  @Test
  void flatScalarRoutesWithParity_nestedFallsBack() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();

      // Every scalar field type the native decode reproduces identically — routes, host-parity verified.
      produce(brokers, "pb-scalars", scalarMessages());
      NativeParity.assertParity(
          environment(
              brokers,
              "pb-scalars",
              "i32 INT, i64 BIGINT, flag BOOLEAN, f32 FLOAT, f64 DOUBLE, text STRING, si32 INT, si64 BIGINT",
              PKG + ".Scalars"),
          "SELECT * FROM t");

      // A nested message field → ROW column the row boundary can't carry → must fall back to Flink.
      produce(brokers, "pb-nested", nestedMessages());
      NativeParity.assertFallback(
          environment(
              brokers,
              "pb-nested",
              "id BIGINT, nested ROW<id BIGINT, name STRING, score DOUBLE>",
              PKG + ".WithNested"),
          "SELECT * FROM t");
    }
  }

  private static List<byte[]> scalarMessages() {
    List<byte[]> values = new ArrayList<>(MESSAGES);
    for (int i = 0; i < MESSAGES; i++) {
      values.add(
          Scalars.newBuilder()
              .setI32(i + 1)
              .setI64(i + 1L)
              .setFlag(i % 2 == 0)
              .setF32(i + 0.5f)
              .setF64(i + 0.25)
              .setText("row-" + i)
              .setSi32(-i - 1) // negative to exercise zigzag decoding
              .setSi64(-i - 1L)
              .build()
              .toByteArray());
    }
    return values;
  }

  private static List<byte[]> nestedMessages() {
    List<byte[]> values = new ArrayList<>(MESSAGES);
    for (int i = 0; i < MESSAGES; i++) {
      values.add(
          WithNested.newBuilder()
              .setId(i + 1L)
              .setNested(Row.newBuilder().setId(i).setName("n-" + i).setScore(i + 0.5).build())
              .build()
              .toByteArray());
    }
    return values;
  }

  private static Supplier<TableEnvironment> environment(
      String brokers, String topic, String columns, String messageClass) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.executeSql(
          "CREATE TABLE t ("
              + columns
              + ") WITH ('connector' = 'kafka', 'topic' = '"
              + topic
              + "', 'properties.bootstrap.servers' = '"
              + brokers
              + "', 'properties.group.id' = '"
              + topic
              + "', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
              + "'format' = 'protobuf', 'protobuf.message-class-name' = '"
              + messageClass
              + "')");
      return tEnv;
    };
  }

  private static void produce(String brokers, String topic, List<byte[]> values) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (byte[] value : values) {
        producer.send(new ProducerRecord<>(topic, 0, null, value));
      }
      producer.flush();
    }
  }
}
