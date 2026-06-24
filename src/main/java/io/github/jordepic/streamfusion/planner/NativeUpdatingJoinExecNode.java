package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeUpdatingJoinOperator;
import java.util.Arrays;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the native regular (updating) INNER join operator into the plan; row-fed, changelog out. */
public class NativeUpdatingJoinExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-updating-join";

  private final RowType leftRowType;
  private final RowType rightRowType;
  private final int[] leftKeys;
  private final int[] rightKeys;

  public NativeUpdatingJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      RowType leftRowType,
      RowType rightRowType,
      RowType outputType,
      String description,
      int[] leftKeys,
      int[] rightKeys) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-updating-join_1"),
        tableConfig,
        Arrays.asList(leftInputProperty, rightInputProperty),
        outputType,
        description);
    this.leftRowType = leftRowType;
    this.rightRowType = rightRowType;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> left =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    Transformation<RowData> right =
        (Transformation<RowData>) getInputEdges().get(1).translateToPlan(planner);
    return ExecNodeUtil.createTwoInputTransformation(
        left,
        right,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeUpdatingJoinOperator(
            leftKeys, rightKeys, leftRowType, rightRowType, (RowType) getOutputType(), BATCH_SIZE),
        InternalTypeInfo.of(getOutputType()),
        left.getParallelism(),
        false);
  }
}
