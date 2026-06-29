package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar keep-last deduplication (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime DESC) = 1`):
 * Arrow in, Arrow out. The analog of the host's {@code RowTimeDeduplicateFunction} (keep-last). Per
 * partition key it keeps the maximum-rowtime row and emits a retract changelog **eagerly** on each
 * input batch (no watermark buffering, unlike keep-first): the kernel emits `+I` for a key's first
 * row, and `-U`(previous)/`+U`(new) when a later row replaces it — the row kind rides the batch's
 * {@code $row_kind$} column. Insert-only input, changelog output. Keys are co-located by the columnar
 * shuffle; the per-key stored row and the checkpointed handle state live here.
 */
public class NativeColumnarKeepLastDeduplicateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final boolean generateUpdateBefore;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeColumnarKeepLastDeduplicateOperator(
      int[] partitionColumns, int rowtimeColumn, boolean generateUpdateBefore) {
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.generateUpdateBefore = generateUpdateBefore;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-keep-last-deduplicate-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createKeepLastDeduplicator(partitionColumns, rowtimeColumn, generateUpdateBefore)
            : Native.restoreKeepLastDeduplicator(
                partitionColumns, rowtimeColumn, generateUpdateBefore, snapshot);
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.pushKeepLastDeduplicator(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotKeepLastDeduplicator(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeKeepLastDeduplicator(handle);
      handle = 0;
    }
    super.close();
  }
}
