package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** The columnar updating INNER join: Arrow batches in on both sides, a changelog of batches out. */
class NativeColumnarUpdatingJoinOperatorTest {

  private static final RowType LEFT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "lv"});
  private static final RowType RIGHT =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "rv"});
  // Join output: left columns then right columns.
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"k", "lv", "k0", "rv"});

  private static NativeColumnarUpdatingJoinOperator operator() {
    return new NativeColumnarUpdatingJoinOperator(new int[] {0}, new int[] {0});
  }

  @Test
  void emitsMatchesAndRetractsFromArrowBatches() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness =
            new TwoInputStreamOperatorTestHarness<>(operator())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      // Right (k=1, rv=100), then left (k=1, lv=10) → +I match; then retract the left → -D match.
      harness.processElement2(new StreamRecord<>(batch(allocator, RIGHT, row(RowKind.INSERT, 1, 100))));
      harness.processElement1(new StreamRecord<>(batch(allocator, LEFT, row(RowKind.INSERT, 1, 10))));
      harness.processElement1(new StreamRecord<>(batch(allocator, LEFT, row(RowKind.DELETE, 1, 10))));
      assertEquals(
          List.of(change(RowKind.INSERT, 1, 10, 1, 100), change(RowKind.DELETE, 1, 10, 1, 100)),
          collect(harness));
    }
  }

  private static RowData row(RowKind kind, long key, long value) {
    GenericRowData row = new GenericRowData(2);
    row.setRowKind(kind);
    row.setField(0, key);
    row.setField(1, value);
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowType schema, RowData... rows) {
    // Carry the kind: the inputs are changelogs (the join consumes -D/-U).
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), schema, allocator, true));
  }

  private static List<Object> change(RowKind kind, long lk, long lv, long rk, long rv) {
    return List.of(kind, lk, lv, rk, rv);
  }

  private static List<List<Object>> collect(
      TwoInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(r.getRowKind(), r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3)));
          }
        }
      }
    }
    return rows;
  }
}
