package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeJsonBytesDecodeOperatorTest {

  private static final RowType OUTPUT =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});

  @Test
  void checkpointBarrierDrainsPartialDecodeBatch() throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeJsonBytesDecodeOperator(OUTPUT, 100), BytePrimitiveArraySerializer.INSTANCE)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>("{\"id\": 7}".getBytes(StandardCharsets.UTF_8)));
      assertEquals(List.of(), collectIds(harness));

      harness.prepareSnapshotPreBarrier(1L);
      assertEquals(List.of(7L), collectIds(harness));
    }
  }

  private static List<Long> collectIds(
      OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness) {
    List<Long> ids = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, OUTPUT)) {
            ids.add(row.getLong(0));
          }
        }
      }
    }
    return ids;
  }
}
