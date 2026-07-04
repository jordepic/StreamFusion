package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort} the native
 * sorter runs: an event-time sort (`ORDER BY rowtime`). Columnar in and out ({@link ColumnarInput}
 * and {@link ColumnarOutput}): it buffers Arrow batches and, on a watermark, emits the rows it has
 * completed in ascending rowtime order, forwarding the batch's columns unchanged. It requires an
 * upstream watermark — the watermark is what releases buffered rows in order.
 */
public class StreamPhysicalNativeTemporalSort extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int rowtimeColumn;

  public StreamPhysicalNativeTemporalSort(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int rowtimeColumn) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.rowtimeColumn = rowtimeColumn;
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
    return new StreamPhysicalNativeTemporalSort(
        getCluster(), traitSet, inputs.get(0), outputRowType, rowtimeColumn);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeTemporalSortExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        rowtimeColumn);
  }

  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}

