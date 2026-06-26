package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * Columnar single-phase session-window aggregation: the same native session aggregator as {@link
 * NativeSessionWindowAggregateOperator}, but fed Arrow batches directly instead of buffered rows. The
 * planner substitutes this when the session's keyed input is kept columnar across the exchange, so the
 * data never transposes to {@link RowData} on the way in. Output is still rows ({@code [key?, agg…,
 * window_start, window_end]}) — like the other single-phase aggregates — so a row consumer downstream
 * needs no transpose either.
 */
public class NativeColumnarSessionWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final long gapMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  public NativeColumnarSessionWindowAggregateOperator(
      long gapMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId) {
    // Sessions have no fixed size or slide; the gap is the only window parameter, carried separately.
    super("streamfusion-session-aggregate-state", 0, 0, valueTypes, aggregateKinds, timeZoneId);
    this.gapMillis = gapMillis;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
  }

  @Override
  protected long createHandle() {
    return Native.createSessionAggregator(gapMillis, valueTypes, aggregateKinds);
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreSessionAggregator(gapMillis, valueTypes, aggregateKinds, snapshot);
  }

  @Override
  protected void updateHandle(long arrayAddress, long schemaAddress) {
    Native.updateSessionAggregator(handle, arrayAddress, schemaAddress);
  }

  @Override
  protected void flushHandle(long watermark, long arrayAddress, long schemaAddress) {
    Native.flushSessionAggregator(handle, watermark, arrayAddress, schemaAddress);
  }

  @Override
  protected byte[] snapshotHandle() {
    return Native.snapshotSessionAggregator(handle);
  }

  @Override
  protected void closeHandle() {
    Native.closeSessionAggregator(handle);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (VectorSchemaRoot in = element.getValue().root()) {
      updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
    }
  }

  @Override
  protected void flushPending() {
    // Each batch is folded into the aggregator as it arrives; nothing is buffered here.
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}
