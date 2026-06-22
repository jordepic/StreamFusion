package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/**
 * Event-time `OVER (ORDER BY rt RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` aggregation, no
 * partition key. Each input row is buffered until the watermark passes its rowtime, then folded into
 * the native running accumulator (in rowtime order, ties shared) and emitted as the input row with
 * the aggregate column(s) appended — matching the host's per-row OVER output. Append-only: every
 * input row produces exactly one output row.
 *
 * <p>The buffer (rows not yet complete) is checkpointed directly; the running accumulator is
 * snapshotted natively, so the running total survives restore. A row whose rowtime the watermark has
 * already passed is late and dropped, as the host does.
 */
public class NativeOverAggregateOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData> {

  private static final int TIMESTAMP_PRECISION = 3;
  private static final int TYPE_DOUBLE = 1;
  private static final int TYPE_INT = 2;

  private final RowType inputType;
  private final int timeColumn;
  private final int valueColumn;
  private final int valueType;
  private final int[] aggregateKinds;

  private transient int inputArity;
  private transient RowData.FieldGetter[] getters;
  private transient List<RowData> buffer;
  private transient long currentWatermark;
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<RowData> bufferState;
  private transient ListState<byte[]> handleState;

  public NativeOverAggregateOperator(
      RowType inputType, int timeColumn, int valueColumn, int valueType, int[] aggregateKinds) {
    this.inputType = inputType;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    inputArity = inputType.getFieldCount();
    getters = new RowData.FieldGetter[inputArity];
    for (int i = 0; i < inputArity; i++) {
      getters[i] = RowData.createFieldGetter(inputType.getTypeAt(i), i);
    }
    bufferState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-over-buffer",
                    InternalTypeInfo.of(inputType)
                        .createSerializer(getExecutionConfig().getSerializerConfig())));
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-over-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    buffer = new ArrayList<>();
    for (RowData row : bufferState.get()) {
      buffer.add(row);
    }
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createOverAggregator(valueType, aggregateKinds)
            : Native.restoreOverAggregator(valueType, aggregateKinds, snapshot);
    currentWatermark = Long.MIN_VALUE;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    RowData row = element.getValue();
    long rt = row.getTimestamp(timeColumn, TIMESTAMP_PRECISION).getMillisecond();
    if (rt <= currentWatermark) {
      return; // late: its window already closed, dropped as the host does
    }
    buffer.add(row);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    currentWatermark = Math.max(currentWatermark, mark.getTimestamp());
    emitComplete();
    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    bufferState.clear();
    bufferState.addAll(buffer);
    handleState.clear();
    handleState.add(Native.snapshotOverAggregator(handle));
  }

  /** Folds every buffered row the watermark has now completed and emits it with its aggregate(s). */
  private void emitComplete() {
    List<RowData> complete = new ArrayList<>();
    List<RowData> pending = new ArrayList<>();
    for (RowData row : buffer) {
      long rt = row.getTimestamp(timeColumn, TIMESTAMP_PRECISION).getMillisecond();
      (rt <= currentWatermark ? complete : pending).add(row);
    }
    buffer = pending;
    if (complete.isEmpty()) {
      return;
    }
    int aggregates = aggregateKinds.length;
    try (VectorSchemaRoot in = buildInput(complete);
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
      Native.updateOverAggregator(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      try (VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
        FieldVector[] results = new FieldVector[aggregates];
        for (int a = 0; a < aggregates; a++) {
          results[a] = (FieldVector) out.getVector("result" + a);
        }
        for (int i = 0; i < complete.size(); i++) {
          RowData row = complete.get(i);
          GenericRowData output = new GenericRowData(inputArity + aggregates);
          for (int c = 0; c < inputArity; c++) {
            output.setField(c, getters[c].getFieldOrNull(row));
          }
          for (int a = 0; a < aggregates; a++) {
            output.setField(inputArity + a, readScalar(results[a], i));
          }
          this.output.collect(new StreamRecord<>(output));
        }
      }
    }
  }

  /** Builds the {@code [rt, value]} batch the native aggregator folds, in the rows' current order. */
  private VectorSchemaRoot buildInput(List<RowData> rows) {
    int n = rows.size();
    BigIntVector rt = new BigIntVector("rt", allocator);
    FieldVector value;
    if (valueType == TYPE_DOUBLE) {
      value = new Float8Vector("value", allocator);
    } else if (valueType == TYPE_INT) {
      value = new IntVector("value", allocator);
    } else {
      value = new BigIntVector("value", allocator);
    }
    for (int i = 0; i < n; i++) {
      RowData row = rows.get(i);
      rt.setSafe(i, row.getTimestamp(timeColumn, TIMESTAMP_PRECISION).getMillisecond());
      if (valueType == TYPE_DOUBLE) {
        ((Float8Vector) value).setSafe(i, row.getDouble(valueColumn));
      } else if (valueType == TYPE_INT) {
        ((IntVector) value).setSafe(i, row.getInt(valueColumn));
      } else {
        ((BigIntVector) value).setSafe(i, row.getLong(valueColumn));
      }
    }
    List<FieldVector> vectors = List.of(rt, value);
    VectorSchemaRoot root = new VectorSchemaRoot(vectors);
    root.setRowCount(n);
    return root;
  }

  /** Reads aggregate result cell {@code row}, boxed by its column type for {@link GenericRowData}. */
  private static Object readScalar(FieldVector vector, int row) {
    if (vector instanceof Float8Vector) {
      return ((Float8Vector) vector).get(row);
    }
    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(row);
    }
    return ((BigIntVector) vector).get(row);
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeOverAggregator(handle);
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
