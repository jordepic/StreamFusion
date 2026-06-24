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
 * Physical plan node standing in for a non-windowed {@code GROUP BY} aggregate the native operator
 * handles. It preserves the replaced node's output type and traits — including its retracting
 * changelog mode — so the surrounding plan is unaffected. No watermark is required (the aggregate
 * fires per record, not on event time).
 */
public class StreamPhysicalNativeGroupAggregate extends SingleRel implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final boolean generateUpdateBefore;

  public StreamPhysicalNativeGroupAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      boolean generateUpdateBefore) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.generateUpdateBefore = generateUpdateBefore;
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
    return new StreamPhysicalNativeGroupAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        generateUpdateBefore);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeGroupAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        generateUpdateBefore);
  }
}
