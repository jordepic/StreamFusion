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
 * Columnar form of Flink's {@code MiniBatchAssigner}: forwards Arrow batches ({@link ColumnarInput}
 * and {@link ColumnarOutput}) and emits processing-time mini-batch marker watermarks that the
 * downstream native local aggregate flushes on. Insert-only (it adds no rows and touches no
 * {@code $row_kind$}), so it requires no changelog handling.
 */
public class StreamPhysicalNativeMiniBatchAssigner extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final long intervalMs;

  public StreamPhysicalNativeMiniBatchAssigner(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long intervalMs) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.intervalMs = intervalMs;
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
    return new StreamPhysicalNativeMiniBatchAssigner(
        getCluster(), traitSet, inputs.get(0), outputRowType, intervalMs);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeMiniBatchAssignerExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        intervalMs);
  }

  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}

