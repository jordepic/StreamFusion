package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeOverAggregateOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the native event-time OVER aggregate operator into the plan (row-fed, row out). */
public class NativeOverAggExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final String TRANSFORMATION = "native-over-aggregate";

  private final RowType inputType;
  private final int timeColumn;
  private final int valueColumn;
  private final int[] keyColumns;
  private final int[] keyTypes;
  private final int valueType;
  private final int[] aggregateKinds;

  public NativeOverAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      RowType inputType,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int[] keyTypes,
      int valueType,
      int[] aggregateKinds) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-over-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.inputType = inputType;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> input =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeOverAggregateOperator(
            inputType, timeColumn, valueColumn, keyColumns, keyTypes, valueType, aggregateKinds),
        InternalTypeInfo.of(getOutputType()),
        input.getParallelism(),
        false);
  }
}
