package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;

/** One Arrow batch from a Fluss log split plus the split offset after the batch. */
final class NativeFlussRecord {

  private final ArrowBatch batch;
  private final long nextOffset;
  private final long maxRowtimeMillis;

  NativeFlussRecord(ArrowBatch batch, long nextOffset, long maxRowtimeMillis) {
    this.batch = batch;
    this.nextOffset = nextOffset;
    this.maxRowtimeMillis = maxRowtimeMillis;
  }

  ArrowBatch batch() {
    return batch;
  }

  long nextOffset() {
    return nextOffset;
  }

  /**
   * Max of the batch's rowtime column in epoch millis, or {@code Long.MIN_VALUE} when the table has
   * no watermark (or every rowtime in the batch is null). Emitted as the batch's record timestamp so
   * the source operator's per-split watermark generator sees it.
   */
  long maxRowtimeMillis() {
    return maxRowtimeMillis;
  }
}
