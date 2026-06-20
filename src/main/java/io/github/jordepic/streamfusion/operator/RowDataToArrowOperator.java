package io.github.jordepic.streamfusion.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Transpose entering a columnar region: buffers rows and emits them as {@link ArrowBatch}es. Sits
 * where a rowwise (host) operator feeds a native columnar one, so the row→Arrow conversion happens
 * once at the boundary rather than inside every native operator.
 *
 * <p>Ownership of an emitted batch passes to the downstream operator, which closes it once read (in
 * a chained task the downstream consumes it inline). Watermarks pass through after the buffer flushes.
 */
public class RowDataToArrowOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<RowData, ArrowBatch>, BoundedOneInput {

  private final RowType rowType;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient List<RowData> buffer;

  public RowDataToArrowOperator(RowType rowType, int batchSize) {
    this.rowType = rowType;
    this.batchSize = batchSize;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
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
    if (allocator != null) {
      allocator.close();
    }
    super.close();
  }

  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    VectorSchemaRoot root = RowDataArrowConverter.write(buffer, rowType, allocator);
    output.collect(new StreamRecord<>(new ArrowBatch(root)));
    buffer.clear();
  }
}
