package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a pure filter the native operator runs. It keeps the replaced node's
 * output type (the input type — a filter does not rewrite columns) and traits, and carries the
 * single-comparison predicate the operator applies. Stateless, so it needs no watermark.
 */
public class StreamPhysicalNativeFilter extends SingleRel implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final int[] projection;
  private final int[] columnIndices;
  private final int[] opCodes;
  private final double[] literals;
  private final String[] stringLiterals;
  private final int[] groups;

  public StreamPhysicalNativeFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] projection,
      int[] columnIndices,
      int[] opCodes,
      double[] literals,
      String[] stringLiterals,
      int[] groups) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.projection = projection;
    this.columnIndices = columnIndices;
    this.opCodes = opCodes;
    this.literals = literals;
    this.stringLiterals = stringLiterals;
    this.groups = groups;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeFilter(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        projection,
        columnIndices,
        opCodes,
        literals,
        stringLiterals,
        groups);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFilterExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        projection,
        columnIndices,
        opCodes,
        literals,
        stringLiterals,
        groups);
  }
}
