package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

/**
 * Recognizes the regular (non-windowed) equi-joins the native updating join implements:
 * {@code a JOIN b ON a.k = b.k}, where both inputs may be changelogs. Supports INNER, LEFT/RIGHT/FULL
 * outer, and SEMI/ANTI (the native joiner tracks a per-row match-degree for the latter families).
 * Requires at least one null-filtering equi-join key, no residual non-equi predicate, and input/output
 * row types the row/Arrow conversion supports. A residual filter or an unsupported column type falls
 * back to the host. The join keys may be any converter-supported type (the join keys its state by
 * their scalar values).
 */
final class RegularJoinMatcher {

  private RegularJoinMatcher() {}

  static boolean matches(StreamPhysicalJoin join) {
    return unsupportedReason(join) == null;
  }

  static String unsupportedReason(StreamPhysicalJoin join) {
    JoinSpec joinSpec = ((CommonPhysicalJoin) join).joinSpec();
    if (joinTypeCode(joinSpec.getJoinType()) < 0) {
      return "regular join: unsupported join type " + joinSpec.getJoinType();
    }
    int[] leftKeys = joinSpec.getLeftKeys();
    if (leftKeys.length == 0 || leftKeys.length != joinSpec.getRightKeys().length) {
      return "regular join: needs at least one equi-join key";
    }
    if (joinSpec.getNonEquiCondition().isPresent()) {
      return "regular join: a residual non-equi condition is not applied";
    }
    for (boolean filterNull : joinSpec.getFilterNulls()) {
      if (!filterNull) {
        return "regular join: requires null-dropping equi keys";
      }
    }
    if (!RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getLeft().getRowType()))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getRight().getRowType()))) {
      return "regular join: an input column type is not supported";
    }
    return null;
  }

  /** The native join-type code (0=INNER,1=LEFT,2=RIGHT,3=FULL,4=SEMI,5=ANTI), or -1 if unsupported. */
  static int joinTypeCode(FlinkJoinType joinType) {
    switch (joinType) {
      case INNER:
        return 0;
      case LEFT:
        return 1;
      case RIGHT:
        return 2;
      case FULL:
        return 3;
      case SEMI:
        return 4;
      case ANTI:
        return 5;
      default:
        return -1;
    }
  }

  static int joinTypeCode(StreamPhysicalJoin join) {
    return joinTypeCode(((CommonPhysicalJoin) join).joinSpec().getJoinType());
  }

  static int[] leftKeys(StreamPhysicalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getLeftKeys();
  }

  static int[] rightKeys(StreamPhysicalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getRightKeys();
  }
}
