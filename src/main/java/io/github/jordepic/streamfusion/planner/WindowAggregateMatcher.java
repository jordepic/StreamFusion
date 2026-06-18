package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;

/**
 * Recognizes the window aggregations the native operator implements: an event-time tumbling window
 * over a local-time-zone attribute, with no extra grouping keys or a single integer key, and a
 * single aggregate over one column reducing an int to an int (SUM/MIN/MAX/COUNT/AVG). Works on the
 * windowing/grouping/aggregate components so the single-phase and local-phase nodes share it.
 */
final class WindowAggregateMatcher {

  static final int KIND_AVG = 4;

  private WindowAggregateMatcher() {}

  static boolean matches(
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing instanceof TimeAttributeWindowingStrategy) || !windowing.isRowtime()) {
      return false;
    }
    // Window bounds are emitted via the session zone, which matches the host only for a
    // local-time-zone event-time attribute.
    if (windowing.getTimeAttributeType().getTypeRoot()
        != org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return false;
    }
    if (!(windowing.getWindow() instanceof TumblingWindowSpec)) {
      return false;
    }
    if (grouping.length > 1 || aggCalls.size() != 1) {
      return false;
    }
    if (grouping.length == 1
        && inputType.getFieldList().get(grouping[0]).getType().getSqlTypeName()
            != SqlTypeName.BIGINT) {
      return false;
    }
    AggregateCall call = aggCalls.apply(0);
    if (call.getArgList().size() != 1 || aggregateKind(aggCalls) < 0) {
      return false;
    }
    // The operators read the value column as a long, so only a bigint value is safe; anything else
    // (int, double, decimal, ...) falls back to the host rather than being mis-read.
    return inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()
        == SqlTypeName.BIGINT;
  }

  static long windowMillis(WindowingStrategy windowing) {
    return ((TumblingWindowSpec) windowing.getWindow()).getSize().toMillis();
  }

  static int timeColumn(WindowingStrategy windowing) {
    return ((TimeAttributeWindowingStrategy) windowing).getTimeAttributeIndex();
  }

  static int valueColumn(scala.collection.Seq<AggregateCall> aggCalls) {
    return aggCalls.apply(0).getArgList().get(0);
  }

  static int keyColumn(int[] grouping) {
    return grouping.length == 1 ? grouping[0] : -1;
  }

  static int aggregateKind(scala.collection.Seq<AggregateCall> aggCalls) {
    return aggregateKind(aggCalls.apply(0).getAggregation().getKind());
  }

  /** Native code for the aggregate, or -1 if unsupported. Mirrors the kinds in {@code Native}. */
  static int aggregateKind(SqlKind kind) {
    switch (kind) {
      case SUM:
        return 0;
      case MIN:
        return 1;
      case MAX:
        return 2;
      case COUNT:
        return 3;
      case AVG:
        return KIND_AVG;
      default:
        return -1;
    }
  }
}
