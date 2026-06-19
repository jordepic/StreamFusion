package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.NativeFilterOperator;
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

/** Wraps the stateless native filter operator into the plan. */
public class NativeFilterExecNode extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  private static final int BATCH_SIZE = 1024;
  private static final String TRANSFORMATION = "native-filter";

  private final int[] columnIndices;
  private final int[] opCodes;
  private final double[] literals;

  public NativeFilterExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] columnIndices,
      int[] opCodes,
      double[] literals) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-filter_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.columnIndices = columnIndices;
    this.opCodes = opCodes;
    this.literals = literals;
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
        new NativeFilterOperator(
            (RowType) getOutputType(), columnIndices, opCodes, literals, BATCH_SIZE),
        InternalTypeInfo.of(getOutputType()),
        input.getParallelism(),
        false);
  }
}
