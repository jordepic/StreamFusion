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
 * Physical node standing in for window Top-N / window deduplication the native operator runs (the
 * host's {@code WindowRank}/{@code WindowDeduplicate} over a windowing-TVF input). Columnar in and
 * out ({@link ColumnarInput} and {@link ColumnarOutput}): the input is shuffled by the partition key
 * (a columnar exchange) and, on a watermark, the operator emits each closed window's top-N rows.
 * Deduplication is the {@code limit = 1} case. Requires an upstream watermark — the watermark closes
 * windows and drives emission.
 */
public class StreamPhysicalNativeWindowRank extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int windowStartColumn;
  private final int windowEndColumn;
  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;
  private final boolean outputRankNumber;

  public StreamPhysicalNativeWindowRank(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.windowStartColumn = windowStartColumn;
    this.windowEndColumn = windowEndColumn;
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
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
    return new StreamPhysicalNativeWindowRank(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        windowStartColumn,
        windowEndColumn,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit,
        outputRankNumber);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWindowRankExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        windowStartColumn,
        windowEndColumn,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit,
        outputRankNumber);
  }
}
