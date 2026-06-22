package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

/**
 * Recognizes the event-time INNER window joins the native operator implements:
 * {@code a JOIN b ON a.k = b.k} where both sides were windowed by identical event-time windowing
 * TVFs, so each row carries matching {@code window_start}/{@code window_end} columns. Requires an
 * INNER join, one or more equi-join keys of supported types (bigint/int/string), null-filtering
 * keys, no residual non-equi predicate, and a window-attached event-time windowing on both sides.
 * Anything else (outer joins, proctime, an extra filter, an unsupported key type) falls back.
 */
final class WindowJoinMatcher {

  private WindowJoinMatcher() {}

  static boolean matches(StreamPhysicalWindowJoin join) {
    JoinSpec joinSpec = ((CommonPhysicalJoin) join).joinSpec();
    if (joinSpec.getJoinType() != FlinkJoinType.INNER) {
      return false;
    }
    int[] leftKeys = joinSpec.getLeftKeys();
    int[] rightKeys = joinSpec.getRightKeys();
    if (leftKeys.length == 0 || leftKeys.length != rightKeys.length) {
      return false; // need at least one equi-join key
    }
    if (joinSpec.getNonEquiCondition().isPresent()) {
      return false; // a residual filter we would not apply — fall back rather than diverge
    }
    for (boolean filterNull : joinSpec.getFilterNulls()) {
      if (!filterNull) {
        return false; // INNER equi-join drops null keys; the native side relies on that
      }
    }
    if (!(join.leftWindowing() instanceof WindowAttachedWindowingStrategy)
        || !(join.rightWindowing() instanceof WindowAttachedWindowingStrategy)
        || !join.leftWindowing().isRowtime()
        || !join.rightWindowing().isRowtime()) {
      return false; // both sides must carry an event-time, window-attached window
    }
    RelDataType leftType = join.getLeft().getRowType();
    RelDataType rightType = join.getRight().getRowType();
    for (int i = 0; i < leftKeys.length; i++) {
      if (!WindowAggregateMatcher.supportedKeyType(
              leftType.getFieldList().get(leftKeys[i]).getType().getSqlTypeName())
          || !WindowAggregateMatcher.supportedKeyType(
              rightType.getFieldList().get(rightKeys[i]).getType().getSqlTypeName())) {
        return false; // equi-join keys must be bigint/int/string
      }
    }
    return true;
  }

  static int[] leftKeys(StreamPhysicalWindowJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getLeftKeys();
  }

  static int[] rightKeys(StreamPhysicalWindowJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getRightKeys();
  }

  static int leftWindowStart(StreamPhysicalWindowJoin join) {
    return attached(join.leftWindowing()).getWindowStart();
  }

  static int leftWindowEnd(StreamPhysicalWindowJoin join) {
    return attached(join.leftWindowing()).getWindowEnd();
  }

  static int rightWindowStart(StreamPhysicalWindowJoin join) {
    return attached(join.rightWindowing()).getWindowStart();
  }

  static int rightWindowEnd(StreamPhysicalWindowJoin join) {
    return attached(join.rightWindowing()).getWindowEnd();
  }

  private static WindowAttachedWindowingStrategy attached(WindowingStrategy windowing) {
    return (WindowAttachedWindowingStrategy) windowing;
  }
}
