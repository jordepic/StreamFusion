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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Stateless native filter: buffers rows into a batch, converts the whole batch to Arrow, applies a
 * single-comparison predicate ({@code column op literal}) natively, and emits the surviving rows
 * unchanged. The output schema is the input schema (a pure filter), so rows round-trip through the
 * whole-row converter. Watermarks pass through after the buffer is flushed, preserving their order
 * relative to the records.
 */
public class NativeFilterOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

  private final RowType inputRowType;
  private final int[] projection;
  private final int[] columnIndices;
  private final int[] opCodes;
  private final double[] literals;
  private final String[] stringLiterals;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;

  public NativeFilterOperator(
      RowType inputRowType,
      int[] projection,
      int[] columnIndices,
      int[] opCodes,
      double[] literals,
      String[] stringLiterals,
      int batchSize) {
    this.inputRowType = inputRowType;
    this.projection = projection;
    this.columnIndices = columnIndices;
    this.opCodes = opCodes;
    this.literals = literals;
    this.stringLiterals = stringLiterals;
    this.batchSize = batchSize;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
    buffer = new ArrayList<>(batchSize);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    buffer.add(element.getValue());
    if (buffer.size() >= batchSize) {
      flush();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flush();
    super.processWatermark(mark);
  }

  @Override
  public void endInput() {
    flush();
  }

  @Override
  public void close() throws Exception {
    if (dictionaries != null) {
      dictionaries.close();
    }
    if (allocator != null) {
      allocator.close();
    }
    super.close();
  }

  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    try (VectorSchemaRoot in = RowDataArrowConverter.write(buffer, inputRowType, allocator);
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
      Native.filterBatch(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress(),
          columnIndices,
          opCodes,
          literals,
          stringLiterals);
      try (VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
        for (RowData survivor : RowDataArrowConverter.read(out, inputRowType)) {
          // Project the selected input columns (a subset/reorder) into the output row.
          GenericRowData input = (GenericRowData) survivor;
          GenericRowData row = new GenericRowData(projection.length);
          for (int j = 0; j < projection.length; j++) {
            row.setField(j, input.getField(projection[j]));
          }
          output.collect(new StreamRecord<>(row));
        }
      }
    }
    buffer.clear();
  }
}
