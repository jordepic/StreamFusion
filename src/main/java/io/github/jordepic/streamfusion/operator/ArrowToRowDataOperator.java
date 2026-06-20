package io.github.jordepic.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Transpose leaving a columnar region: reads each {@link ArrowBatch} back into rows. Sits where a
 * native columnar operator feeds a rowwise (host) one, so the Arrow→row conversion happens once at
 * the boundary. It consumes (and closes) each batch it receives.
 */
public class ArrowToRowDataOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final RowType rowType;

  public ArrowToRowDataOperator(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (VectorSchemaRoot root = element.getValue().root()) {
      for (RowData row : RowDataArrowConverter.read(root, rowType)) {
        output.collect(new StreamRecord<>(row));
      }
    }
  }
}
