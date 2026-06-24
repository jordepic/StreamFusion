package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
 * Columnar non-windowed {@code GROUP BY} aggregation: the same native aggregator as {@link
 * NativeGroupAggregateOperator}, but fed Arrow batches and emitting Arrow batches directly instead of
 * transposing rows on each side. The planner substitutes this when the aggregate's keyed input is
 * kept columnar across the exchange, so a native changelog chain pays no per-operator transpose; the
 * row↔Arrow conversion happens only at the host edges (inserted by the transition pass). The output
 * batch carries the changelog kind on its {@code $row_kind$} column.
 */
public class NativeColumnarGroupAggregateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final boolean generateUpdateBefore;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeColumnarGroupAggregateOperator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      boolean generateUpdateBefore) {
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
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
                    "streamfusion-group-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createGroupAggregator(
                aggregateKinds, valueTypes, valueColumns, keyColumns, generateUpdateBefore)
            : Native.restoreGroupAggregator(
                aggregateKinds, valueTypes, valueColumns, keyColumns, generateUpdateBefore, snapshot);
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
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
      Native.updateGroupAggregator(
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
    handleState.add(Native.snapshotGroupAggregator(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeGroupAggregator(handle);
      handle = 0;
    }
    if (dictionaries != null) {
      dictionaries.close();
    }
    if (allocator != null) {
      allocator.close();
    }
    super.close();
  }
}
