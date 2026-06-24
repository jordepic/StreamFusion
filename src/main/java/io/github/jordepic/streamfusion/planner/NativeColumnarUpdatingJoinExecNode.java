package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarUpdatingJoinOperator;
import java.util.Arrays;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the columnar regular (updating) INNER join operator into the plan; Arrow batches in and out. */
public class NativeColumnarUpdatingJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-updating-join";

  private final int[] leftKeys;
  private final int[] rightKeys;

  public NativeColumnarUpdatingJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
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
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> left =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    Transformation<ArrowBatch> right =
        (Transformation<ArrowBatch>) getInputEdges().get(1).translateToPlan(planner);
    return ExecNodeUtil.createTwoInputTransformation(
        left,
        right,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeColumnarUpdatingJoinOperator(leftKeys, rightKeys),
        ArrowBatchTypeInformation.INSTANCE,
        left.getParallelism(),
        false);
  }
}
