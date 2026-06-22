package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * The OVER aggregate operator emits each input row with the running SUM over `RANGE UNBOUNDED
 * PRECEDING` appended, once the watermark passes its rowtime — including shared values for rows that
 * tie on rowtime.
 */
class NativeOverAggregateOperatorTest {

  // Input schema [v BIGINT, rt TIMESTAMP_LTZ(3)]; output appends the running SUM (BIGINT).
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new LocalZonedTimestampType(3)},
          new String[] {"v", "rt"});

  @Test
  void emitsRunningSumSharingRangeTies() throws Exception {
    NativeOverAggregateOperator operator =
        new NativeOverAggregateOperator(INPUT, 1, 0, 0, new int[] {0});
    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.setup();
      harness.open();

      harness.processElement(row(10, 0));
      harness.processElement(row(20, 1000));
      harness.processElement(row(30, 1000));
      harness.processElement(row(40, 2000));
      // Nothing emitted before the watermark reaches the rows' rowtimes.
      assertEquals(List.of(), collect(harness));

      harness.processWatermark(new Watermark(2000));
      // rt 1000 ties (20,30) share 10+20+30=60; emitted in input order with the input columns kept.
      assertEquals(
          List.of(
              List.of(10L, 0L, 10L),
              List.of(20L, 1000L, 60L),
              List.of(30L, 1000L, 60L),
              List.of(40L, 2000L, 100L)),
          collect(harness));
    }
  }

  private static StreamRecord<RowData> row(long v, long rtMillis) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, v);
    row.setField(1, TimestampData.fromEpochMillis(rtMillis));
    return new StreamRecord<>(row);
  }

  /** Drains the output as [v, rt-millis, sum] triples. */
  private static List<List<Long>> collect(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        RowData r = (RowData) ((StreamRecord<?>) event).getValue();
        rows.add(List.of(r.getLong(0), r.getTimestamp(1, 3).getMillisecond(), r.getLong(2)));
      }
    }
    return rows;
  }
}
