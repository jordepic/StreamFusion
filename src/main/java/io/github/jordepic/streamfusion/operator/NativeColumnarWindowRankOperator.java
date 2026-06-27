package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar window Top-N / window deduplication over a windowing-TVF input (the host's {@code
 * WindowRank}/{@code WindowDeduplicate}): Arrow in, Arrow out. Within each window (the attached
 * {@code window_start}/{@code window_end} columns) and partition key, the native ranker keeps the
 * top {@code limit} rows by the sort columns and emits them when a watermark closes the window —
 * append-only, emitted once. Deduplication is the {@code limit = 1} case. The buffering, ranking,
 * and late-row drop live in the native ranker; this layer moves batches across the bridge and owns
 * the handle's checkpointed state.
 */
public class NativeColumnarWindowRankOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int windowStartColumn;
  private final int windowEndColumn;
  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;
  private final boolean outputRankNumber;
  private final String timeZoneId;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient ZoneId zone;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeColumnarWindowRankOperator(
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      String timeZoneId) {
    this.windowStartColumn = windowStartColumn;
    this.windowEndColumn = windowEndColumn;
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
    this.timeZoneId = timeZoneId;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-window-rank-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createWindowRanker(
                windowStartColumn,
                windowEndColumn,
                partitionColumns,
                sortIndices,
                sortAscending,
                sortNullsFirst,
                limit,
                outputRankNumber)
            : Native.restoreWindowRanker(
                windowStartColumn,
                windowEndColumn,
                partitionColumns,
                sortIndices,
                sortAscending,
                sortNullsFirst,
                limit,
                outputRankNumber,
                snapshot);
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    zone = ZoneId.of(timeZoneId);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      Native.pushWindowRanker(handle, array.memoryAddress(), schema.memoryAddress());
    } finally {
      in.close();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushWindowRanker(
          handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        // The native side keeps window_start/window_end as UTC epoch (so eviction compares against the
        // UTC watermark); render them as session-local wall-clock TIMESTAMPs on emit, as the host does
        // (window_time stays the UTC rowtime). Same toLocal shift as the window aggregate.
        shiftToLocal(out, windowStartColumn);
        shiftToLocal(out, windowEndColumn);
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no window closed on this watermark
      }
    }
    super.processWatermark(mark);
  }

  /** Rewrites a UTC-epoch timestamp column to the session-local wall-clock the host emits. */
  private void shiftToLocal(VectorSchemaRoot out, int column) {
    if (!(out.getVector(column) instanceof TimeStampNanoVector)) {
      return;
    }
    TimeStampNanoVector ts = (TimeStampNanoVector) out.getVector(column);
    for (int i = 0; i < out.getRowCount(); i++) {
      if (ts.isNull(i)) {
        continue;
      }
      long utcMillis = ts.get(i) / 1_000_000L;
      long localMillis =
          Instant.ofEpochMilli(utcMillis).atZone(zone).toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
      ts.setSafe(i, localMillis * 1_000_000L);
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotWindowRanker(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeWindowRanker(handle);
      handle = 0;
    }
    super.close();
  }
}
