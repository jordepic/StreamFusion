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
 * Physical node standing in for an event-time {@code OVER} aggregate (RANGE unbounded preceding, no
 * partition key) the native operator runs. Row-fed: it consumes and produces {@link
 * org.apache.flink.table.data.RowData} (each input row with the aggregate appended), so it needs no
 * columnar transpose. Requires a watermark, since each row is emitted once the watermark passes its
 * rowtime.
 */
public class StreamPhysicalNativeOverAggregate extends SingleRel implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final int timeColumn;
  private final int valueColumn;
  private final int valueType;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeOverAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int timeColumn,
      int valueColumn,
      int valueType,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  public boolean requireWatermark() {
    return true;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeOverAggregate(
        getCluster(), traitSet, inputs.get(0), outputRowType, timeColumn, valueColumn, valueType,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeOverAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        timeColumn,
        valueColumn,
        valueType,
        aggregateKinds);
  }
}
