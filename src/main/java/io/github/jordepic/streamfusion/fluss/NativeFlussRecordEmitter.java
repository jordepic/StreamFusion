package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.fluss.flink.source.split.SourceSplitState;

/**
 * Emits native Fluss Arrow batches and advances the Fluss log split state. A watermarked table's
 * batch is collected with its max rowtime as the record timestamp: the source operator's per-split
 * watermark generator ({@link io.github.jordepic.streamfusion.operator.NativeSourceWatermarks})
 * folds it in, which is equivalent to feeding every row because the delay is constant and the
 * generator keeps a max.
 */
final class NativeFlussRecordEmitter
    implements RecordEmitter<NativeFlussRecord, ArrowBatch, SourceSplitState> {

  @Override
  public void emitRecord(
      NativeFlussRecord record, SourceOutput<ArrowBatch> output, SourceSplitState splitState) {
    if (record.maxRowtimeMillis() == Long.MIN_VALUE) {
      output.collect(record.batch());
    } else {
      output.collect(record.batch(), record.maxRowtimeMillis());
    }
    splitState.asLogSplitState().setNextOffset(record.nextOffset());
  }
}
