package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/**
 * The native protobuf decode operator: a batch of bare protobuf message bodies in, a typed Arrow batch
 * out — proving the full JVM→native path (descriptor bytes built with protobuf-java, exactly as the
 * planner would serialize them off a {@code message-class-name}, → ptars decode → Arrow).
 */
class NativeProtobufDecodeOperatorTest {

  // bench.Row { int64 id = 1; string name = 2; double score = 3; } — built dynamically (no codegen),
  // serialized to a FileDescriptorSet exactly like the JVM would off a generated message class.
  private static FileDescriptor rowFile() throws Exception {
    FieldDescriptorProto id = field("id", 1, FieldDescriptorProto.Type.TYPE_INT64);
    FieldDescriptorProto name = field("name", 2, FieldDescriptorProto.Type.TYPE_STRING);
    FieldDescriptorProto score = field("score", 3, FieldDescriptorProto.Type.TYPE_DOUBLE);
    DescriptorProto row =
        DescriptorProto.newBuilder().setName("Row").addField(id).addField(name).addField(score).build();
    FileDescriptorProto file =
        FileDescriptorProto.newBuilder()
            .setName("bench.proto")
            .setPackage("bench")
            .addMessageType(row)
            .build();
    return FileDescriptor.buildFrom(file, new FileDescriptor[0]);
  }

  private static FieldDescriptorProto field(String name, int number, FieldDescriptorProto.Type type) {
    return FieldDescriptorProto.newBuilder()
        .setName(name)
        .setNumber(number)
        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
        .setType(type)
        .build();
  }

  @Test
  void decodesProtobufBodiesToTypedBatch() throws Exception {
    FileDescriptor file = rowFile();
    Descriptor row = file.findMessageTypeByName("Row");
    byte[] descriptorSet =
        FileDescriptorSet.newBuilder().addFile(file.toProto()).build().toByteArray();

    byte[] m0 = message(row, 1L, "a", 1.5);
    byte[] m1 = message(row, 2L, "b", 2.5);

    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeProtobufDecodeOperator(descriptorSet, "bench.Row"), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(bodies(allocator, m0, m1)));

      assertEquals(List.of(List.of(1L, "a", 1.5), List.of(2L, "b", 2.5)), collect(harness));
    }
  }

  private static byte[] message(Descriptor row, long id, String name, double score) {
    return DynamicMessage.newBuilder(row)
        .setField(row.findFieldByName("id"), id)
        .setField(row.findFieldByName("name"), name)
        .setField(row.findFieldByName("score"), score)
        .build()
        .toByteArray();
  }

  /** Builds an Arrow batch of one binary column ("body") holding the raw protobuf messages. */
  private static ArrowBatch bodies(BufferAllocator allocator, byte[]... messages) {
    VarBinaryVector vector = new VarBinaryVector("body", allocator);
    vector.allocateNew(messages.length);
    for (int i = 0; i < messages.length; i++) {
      vector.setSafe(i, messages[i]);
    }
    vector.setValueCount(messages.length);
    VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));
    root.setRowCount(messages.length);
    return new ArrowBatch(root);
  }

  /** Reads the output batch's columns by proto field name (the schema ptars derives from the descriptor). */
  private static List<List<Object>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            rows.add(
                List.of(
                    ((Number) root.getVector("id").getObject(i)).longValue(),
                    root.getVector("name").getObject(i).toString(),
                    ((Number) root.getVector("score").getObject(i)).doubleValue()));
          }
        }
      }
    }
    return rows;
  }
}
