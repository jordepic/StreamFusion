package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeGroupAggregateOperator;
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

/**
 * Execution node wiring the native non-windowed {@code GROUP BY} aggregate operator into the plan.
 * It carries the aggregate kinds, value columns/types, and grouping keys, plus the input and output
 * row types the row/Arrow conversion needs and the host's update-before flag.
 */
public class NativeGroupAggExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-group-aggregate";

  private final RowType inputRowType;
  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final boolean generateUpdateBefore;

  public NativeGroupAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType inputRowType,
      RowType outputType,
      String description,
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      boolean generateUpdateBefore) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-group-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.inputRowType = inputRowType;
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.generateUpdateBefore = generateUpdateBefore;
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
        new NativeGroupAggregateOperator(
            aggregateKinds,
            valueTypes,
            valueColumns,
            keyColumns,
            generateUpdateBefore,
            inputRowType,
            (RowType) getOutputType(),
            BATCH_SIZE),
        InternalTypeInfo.of(getOutputType()),
        input.getParallelism(),
        false);
  }
}
