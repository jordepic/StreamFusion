package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar single-phase window aggregation: the same native aggregator as {@link
 * NativeWindowAggregateOperator}, but fed Arrow batches directly instead of buffered rows, and
 * emitting Arrow batches ({@code [key?, agg…, window_start, window_end]}). The whole operator is
 * Arrow → Arrow; a rowwise sink downstream is reached through the dedicated
 * {@code ArrowToRowDataOperator} the planner inserts at the island perimeter.
 *
 * <p>Event-time windows assign each row by its rowtime column and fire on a watermark (the core's
 * default). A **proctime** window instead assigns every row in a batch to the window of the
 * operator's current processing time (Flink's processing-time assigner uses the clock, not a row
 * value) and fires on a processing-time timer registered at each window's end — so a closed window is
 * emitted when wall-clock passes its boundary even with no further input. Remaining open windows are
 * flushed when the (bounded) input finishes.
 */
public class NativeColumnarWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final boolean cumulative;
  private final boolean proctime;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  /** The latest processing-time boundary already scheduled, so each window-end timer registers once. */
  private transient long registeredTimer;

  public NativeColumnarWindowAggregateOperator(
      boolean cumulative,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      RowType outputType,
      boolean proctime) {
    super(
        "streamfusion-window-aggregate-state",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        outputType);
    this.cumulative = cumulative;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.proctime = proctime;
  }

  @Override
  public void open() throws Exception {
    super.open();
    registeredTimer = Long.MIN_VALUE;
  }

  @Override
  protected long createHandle() {
    return cumulative
        ? Native.createCumulativeAggregator(windowMillis, slideMillis, valueTypes, aggregateKinds)
        : super.createHandle();
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return cumulative
        ? Native.restoreCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, snapshot)
        : super.restoreHandle(snapshot);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (var in = element.getValue().root()) {
      if (proctime) {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        updateColumnarProctime(in, now, valueColumns, keyColumns, keyTypes);
        emitClosedWindows(now); // emit windows the clock has already passed
        // Fire when the window holding `now` closes; processing time only advances, so registering
        // the latest boundary seen is enough to schedule each distinct window-end once.
        long boundary = Math.floorDiv(now, slideMillis) * slideMillis + windowMillis;
        if (boundary > registeredTimer) {
          getProcessingTimeService().registerTimer(boundary, this);
          registeredTimer = boundary;
        }
      } else {
        updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
      }
    }
  }

  @Override
  public void onProcessingTime(long time) {
    emitClosedWindows(getProcessingTimeService().getCurrentProcessingTime());
  }

  @Override
  public void finish() throws Exception {
    if (proctime) {
      emitClosedWindows(Long.MAX_VALUE); // end of input: close every remaining window
    }
    super.finish();
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
