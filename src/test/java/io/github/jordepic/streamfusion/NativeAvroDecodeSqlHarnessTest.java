package io.github.jordepic.streamfusion;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end parity test for the bare-Avro native-decode path: a {@code CREATE TABLE ...
 * 'connector'='kafka', 'format'='avro'} routes through the planner to {@link
 * io.github.jordepic.streamfusion.planner.StreamPhysicalNativeKafkaDecode} — Flink's own KafkaSource
 * consumes the raw Avro datums and a native operator decodes them straight to Arrow, against the reader
 * schema derived from the table's {@code RowType} with the same converter Flink's {@code avro} format
 * uses.
 *
 * <p>{@link NativeParity#assertParity} runs the query on stock Flink (its own {@code avro} decoder) and
 * on the native decode and asserts the rows match, so the native decode reproduces Flink's result. The
 * messages are produced as bare Avro datums (no Confluent framing, no object-container header), which is
 * exactly what Flink's {@code avro} value format reads.
 *
 * <p>Opt-in via {@code SF_BENCHMARK=true} (Docker for Testcontainers Kafka). A bounded ({@code
 * latest-offset}) scan so both runs terminate.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeAvroDecodeSqlHarnessTest {

  private static final String TOPIC = "native-avro-decode-sql-it";
  private static final int MESSAGES = 2_000;

  private static final RowType ROW_TYPE =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("id", DataTypes.BIGINT()),
                  DataTypes.FIELD("name", DataTypes.STRING()),
                  DataTypes.FIELD("score", DataTypes.DOUBLE()))
              .getLogicalType();

  @Test
  void avroKafkaTableDecodesNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      produceAvro(brokers, MESSAGES);

      Supplier<TableEnvironment> environment =
          () -> {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            tEnv.executeSql(avroTable("t", brokers));
            return tEnv;
          };

      NativeParity.assertParity(environment, "SELECT * FROM t");
    }
  }

  private static String avroTable(String name, String brokers) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING, score DOUBLE) WITH ("
        + "'connector' = 'kafka', "
        + "'topic' = '"
        + TOPIC
        + "', 'properties.bootstrap.servers' = '"
        + brokers
        + "', 'properties.group.id' = 'native-avro-decode-sql-it', "
        + "'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset', "
        + "'format' = 'avro')";
  }

  private static void produceAvro(String brokers, int messages) throws Exception {
    // The same reader schema the planner derives (the row forced non-null → a record, not a top-level
    // union), so the produced datums decode identically on both paths.
    Schema schema = AvroSchemaConverter.convertToSchema(ROW_TYPE.copy(false));
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      List<byte[]> values = new ArrayList<>(messages);
      for (int i = 0; i < messages; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", (long) i);
        record.put("name", "row-" + i);
        record.put("score", i + 0.5);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        values.add(out.toByteArray());
      }
      for (byte[] value : values) {
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
  }
}
