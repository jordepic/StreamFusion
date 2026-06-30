package io.github.jordepic.streamfusion.planner;

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Leaf physical node for the shallow native-decode Kafka path: Flink's own {@code KafkaSource} consumes
 * the topic's raw value bytes (offsets/checkpointing/auth all Flink's), and a native operator decodes
 * those bytes straight to Arrow — skipping Flink's per-record {@code RowData} materialization. The data
 * starts columnar at the source edge. Carries the scan's row type and raw table options (the exec node
 * builds the byte source + picks the decode format from them).
 */
public class StreamPhysicalNativeKafkaDecode extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {

  private final RelDataType outputRowType;
  private final Map<String, String> options;

  public StreamPhysicalNativeKafkaDecode(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options) {
    super(cluster, traitSet);
    this.outputRowType = outputRowType;
    this.options = options;
  }

  Map<String, String> options() {
    return options;
  }

  /** A copy decoding only {@code rowType}'s columns/fields — set by the planner's projection pushdown. */
  StreamPhysicalNativeKafkaDecode withRowType(RelDataType rowType) {
    return new StreamPhysicalNativeKafkaDecode(getCluster(), getTraitSet(), rowType, options);
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
    return new StreamPhysicalNativeKafkaDecode(getCluster(), traitSet, outputRowType, options);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer)
        .item("topic", options.get("topic"))
        .item("format", options.getOrDefault("value.format", options.get("format")));
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaDecodeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        options);
  }
}
