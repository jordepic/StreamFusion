package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;

/**
 * Recognizes the global half of a two-phase tumbling aggregation: a slice-attached tumbling window,
 * no extra key or a single integer key, and a single mergeable aggregate over the partial column
 * (SUM/MIN/MAX — count merges as a sum). Its input is the local half's {@code [key?, partial,
 * slice_end]}.
 */
final class GlobalWindowAggregateMatcher {

  private GlobalWindowAggregateMatcher() {}

  static boolean matches(StreamPhysicalGlobalWindowAggregate aggregate) {
    WindowingStrategy windowing = aggregate.windowing();
    if (!(windowing instanceof SliceAttachedWindowingStrategy)) {
      return false;
    }
    if (!(windowing.getWindow() instanceof TumblingWindowSpec)) {
      return false;
    }
    int[] grouping = aggregate.grouping();
    if (grouping.length > 1 || aggregate.aggCalls().size() != 1) {
      return false;
    }
    if (grouping.length == 1
        && aggregate
                .getInput()
                .getRowType()
                .getFieldList()
                .get(grouping[0])
                .getType()
                .getSqlTypeName()
            != SqlTypeName.BIGINT) {
      return false;
    }
    AggregateCall call = aggregate.aggCalls().apply(0);
    int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
    // Single-field mergeable partial only: sum (also count's merge), min, max.
    return call.getArgList().size() == 1 && kind >= 0 && kind != WindowAggregateMatcher.KIND_AVG;
  }

  static long windowMillis(StreamPhysicalGlobalWindowAggregate aggregate) {
    return ((TumblingWindowSpec) aggregate.windowing().getWindow()).getSize().toMillis();
  }

  static int keyColumn(StreamPhysicalGlobalWindowAggregate aggregate) {
    int[] grouping = aggregate.grouping();
    return grouping.length == 1 ? grouping[0] : -1;
  }

  static int partialColumn(StreamPhysicalGlobalWindowAggregate aggregate) {
    return aggregate.aggCalls().apply(0).getArgList().get(0);
  }

  static int sliceEndColumn(StreamPhysicalGlobalWindowAggregate aggregate) {
    return ((SliceAttachedWindowingStrategy) aggregate.windowing()).getSliceEnd();
  }

  static int aggregateKind(StreamPhysicalGlobalWindowAggregate aggregate) {
    return WindowAggregateMatcher.aggregateKind(aggregate.aggCalls().apply(0).getAggregation().getKind());
  }
}
