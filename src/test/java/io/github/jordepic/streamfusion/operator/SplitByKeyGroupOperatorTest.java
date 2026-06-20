package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.ColumnarKeyGroupPartitioner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class SplitByKeyGroupOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  @SuppressWarnings("unchecked")
  void splitsABatchByKeyIntoTaggedSubBatches() throws Exception {
    int channels = 4;
    int n = 500;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new SplitByKeyGroupOperator(new int[] {0}, channels), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      List<RowData> rows = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        rows.add(row(i % 53, i));
      }
      VectorSchemaRoot in = RowDataArrowConverter.write(rows, SCHEMA, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(in)));

      int total = 0;
      Map<Long, Integer> keyToChannel = new HashMap<>();
      for (Object record : harness.getOutput()) {
        if (!(record instanceof StreamRecord)) {
          continue;
        }
        ArrowBatch batch = ((StreamRecord<ArrowBatch>) record).getValue();
        int channel = batch.destination();
        assertTrue(channel >= 0 && channel < channels, "destination in range");
        try (VectorSchemaRoot sub = batch.root()) {
          for (RowData r : RowDataArrowConverter.read(sub, SCHEMA)) {
            long key = r.getLong(0);
            Integer prev = keyToChannel.put(key, channel);
            if (prev != null) {
              assertEquals(prev.intValue(), channel, "key " + key + " split across channels");
            }
            total++;
          }
        }
      }
      assertEquals(n, total, "all rows preserved");
    }
  }

  @Test
  void partitionerRoutesByDestination() {
    ColumnarKeyGroupPartitioner partitioner = new ColumnarKeyGroupPartitioner();
    partitioner.setup(4);
    StreamPartitioner<ArrowBatch> p = partitioner;
    // selectChannel reads the batch's tagged destination; the serializer is never consulted.
    SerializationDelegate<StreamRecord<ArrowBatch>> delegate = new SerializationDelegate<>(null);
    delegate.setInstance(new StreamRecord<>(new ArrowBatch(null, 2)));
    assertEquals(2, p.selectChannel(delegate));
    delegate.setInstance(new StreamRecord<>(new ArrowBatch(null, -1)));
    assertEquals(0, p.selectChannel(delegate), "an unrouted batch goes to channel 0");
  }
}
