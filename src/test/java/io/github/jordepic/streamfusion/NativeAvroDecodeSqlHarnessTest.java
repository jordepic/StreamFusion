package io.github.jordepic.streamfusion;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * End-to-end parity tests for the bare-Avro native-decode path: a {@code 'format'='avro'} Kafka table
 * routes to a native operator that decodes the raw datums to Arrow against the reader schema derived from
 * the table's {@code RowType} with the same converter Flink's {@code avro} format uses, so the two decode
 * the same bytes. {@link NativeParity#assertParity} compares against Flink's own {@code avro} decoder.
 *
 * <p>Covers a flat record and a record with a nested record, an array, and a map — the complex column
 * shapes the row boundary carries. Complex columns are read element-wise ({@code nested.a}, {@code
 * nums[1]}, {@code tags['a']}) so the compared values are scalars. Opt-in via {@code SF_BENCHMARK=true}.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeAvroDecodeSqlHarnessTest {

  private static final int MESSAGES = 2_000;

  @Test
  void avroMessagesDecodeNativelyWithFlinkParity() throws Exception {
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();

      RowType flat =
          (RowType)
              DataTypes.ROW(
                      DataTypes.FIELD("id", DataTypes.BIGINT()),
                      DataTypes.FIELD("name", DataTypes.STRING()),
                      DataTypes.FIELD("score", DataTypes.DOUBLE()))
                  .getLogicalType();
      produce(brokers, "avro-flat", flat, NativeAvroDecodeSqlHarnessTest::flatRecord);
      NativeParity.assertParity(
          environment(brokers, "avro-flat", "id BIGINT, name STRING, score DOUBLE"), "SELECT * FROM t");

      RowType complex =
          (RowType)
              DataTypes.ROW(
                      DataTypes.FIELD("id", DataTypes.BIGINT()),
                      DataTypes.FIELD(
                          "nested",
                          DataTypes.ROW(
                              DataTypes.FIELD("a", DataTypes.BIGINT()),
                              DataTypes.FIELD("b", DataTypes.STRING()))),
                      DataTypes.FIELD("nums", DataTypes.ARRAY(DataTypes.BIGINT())),
                      DataTypes.FIELD("tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())))
                  .getLogicalType();
      produce(brokers, "avro-complex", complex, NativeAvroDecodeSqlHarnessTest::complexRecord);
      NativeParity.assertParity(
          environment(
              brokers,
              "avro-complex",
              "id BIGINT, nested ROW<a BIGINT, b STRING>, nums ARRAY<BIGINT>, tags MAP<STRING, BIGINT>"),
          "SELECT id, nested.a, nested.b, nums[1], nums[2], tags['a'], tags['b'] FROM t");
    }
  }

  @Test
  void nestedProjectionPrunesDecodedColumns() throws Exception {
    // A wide record read through a strict subset: the planner pushes the projection into the decode,
    // which decodes the full writer datum but materializes only event_type + the two bid sub-fields via
    // a narrowed reader schema (Avro resolution). Must still match Flink's full-record decode + calc.
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))) {
      kafka.start();
      String brokers = kafka.getBootstrapServers();
      String columns =
          "event_type INT, person ROW<id BIGINT, name STRING, email STRING, city STRING>,"
              + " bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING>";
      RowType wide =
          (RowType)
              DataTypes.ROW(
                      DataTypes.FIELD("event_type", DataTypes.INT()),
                      DataTypes.FIELD(
                          "person",
                          DataTypes.ROW(
                              DataTypes.FIELD("id", DataTypes.BIGINT()),
                              DataTypes.FIELD("name", DataTypes.STRING()),
                              DataTypes.FIELD("email", DataTypes.STRING()),
                              DataTypes.FIELD("city", DataTypes.STRING()))),
                      DataTypes.FIELD(
                          "bid",
                          DataTypes.ROW(
                              DataTypes.FIELD("auction", DataTypes.BIGINT()),
                              DataTypes.FIELD("bidder", DataTypes.BIGINT()),
                              DataTypes.FIELD("price", DataTypes.BIGINT()),
                              DataTypes.FIELD("channel", DataTypes.STRING()),
                              DataTypes.FIELD("url", DataTypes.STRING()))))
                  .getLogicalType();
      produce(brokers, "avro-wide", wide, NativeAvroDecodeSqlHarnessTest::wideRecord);
      NativeParity.assertParity(
          environment(brokers, "avro-wide", columns),
          "SELECT bid.auction, bid.price FROM t WHERE event_type = 2");
    }
  }

  private static void wideRecord(GenericRecord record, int i, Schema schema) {
    int eventType = i % 2 == 0 ? 0 : 2;
    record.put("event_type", eventType);
    if (eventType == 0) {
      GenericRecord person = new GenericData.Record(recordBranch(schema.getField("person").schema()));
      person.put("id", (long) i);
      person.put("name", "n-" + i);
      person.put("email", "e-" + i);
      person.put("city", "c-" + i);
      record.put("person", person);
      record.put("bid", null);
    } else {
      GenericRecord bid = new GenericData.Record(recordBranch(schema.getField("bid").schema()));
      bid.put("auction", (long) i);
      bid.put("bidder", (long) (i + 1));
      bid.put("price", (long) (i * 10));
      bid.put("channel", "ch-" + i);
      bid.put("url", "u-" + i);
      record.put("person", null);
      record.put("bid", bid);
    }
  }

  private static void flatRecord(GenericRecord record, int i, Schema schema) {
    record.put("id", (long) i);
    record.put("name", "row-" + i);
    record.put("score", i + 0.5);
  }

  private static void complexRecord(GenericRecord record, int i, Schema schema) {
    record.put("id", (long) i);
    GenericRecord nested = new GenericData.Record(recordBranch(schema.getField("nested").schema()));
    nested.put("a", (long) (i + 1));
    nested.put("b", "b-" + i);
    record.put("nested", nested);
    record.put("nums", List.of((long) i, (long) (i + 100)));
    Map<String, Long> tags = new HashMap<>();
    tags.put("a", (long) (i + 1));
    tags.put("b", (long) (i + 2));
    record.put("tags", tags);
  }

  /** The record branch of a (possibly null-union-wrapped) Avro field schema. */
  private static Schema recordBranch(Schema fieldSchema) {
    if (fieldSchema.getType() == Schema.Type.RECORD) {
      return fieldSchema;
    }
    return fieldSchema.getTypes().stream()
        .filter(s -> s.getType() == Schema.Type.RECORD)
        .findFirst()
        .orElseThrow();
  }

  private interface Filler {
    void fill(GenericRecord record, int i, Schema schema);
  }

  private static void produce(String brokers, String topic, RowType rowType, Filler filler)
      throws Exception {
    // The same reader schema the planner derives (the row forced non-null → a record, not a top-level
    // union), so the produced datums decode identically on both paths.
    Schema schema = AvroSchemaConverter.convertToSchema(rowType.copy(false));
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      List<byte[]> values = new ArrayList<>(MESSAGES);
      for (int i = 0; i < MESSAGES; i++) {
        GenericRecord record = new GenericData.Record(schema);
        filler.fill(record, i, schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        values.add(out.toByteArray());
      }
      for (byte[] value : values) {
        producer.send(new ProducerRecord<>(topic, 0, null, value));
      }
      producer.flush();
    }
  }

  private static Supplier<TableEnvironment> environment(String brokers, String topic, String columns) {
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
              + "'format' = 'avro')");
      return tEnv;
    };
  }
}
