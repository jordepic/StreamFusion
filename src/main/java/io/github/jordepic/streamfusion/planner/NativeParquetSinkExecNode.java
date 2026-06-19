package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeParquetSinkOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the native Parquet sink operator into the plan as a terminal sink transformation. */
public class NativeParquetSinkExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-parquet-sink";

  private final RowType inputType;
  private final String path;

  public NativeParquetSinkExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      RowType inputType,
      String path) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-parquet-sink_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.inputType = inputType;
    this.path = path;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> input =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    NativeParquetSinkOperator operator =
        new NativeParquetSinkOperator(inputType, path, BATCH_SIZE);
    // A LegacySinkTransformation makes the operator a real sink (terminal in the job graph) and
    // delivers checkpoint-completion callbacks to it, which the operator's exactly-once commit needs.
    return new LegacySinkTransformation<>(
        input,
        TRANSFORMATION,
        SimpleOperatorFactory.of(operator),
        input.getParallelism(),
        false);
  }
}
