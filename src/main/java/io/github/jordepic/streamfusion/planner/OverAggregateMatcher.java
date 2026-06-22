package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;

/**
 * Recognizes the event-time {@code OVER} aggregations the native operator implements: a single
 * window group ordered by a rowtime (local-time-zone) attribute, the default {@code RANGE BETWEEN
 * UNBOUNDED PRECEDING AND CURRENT ROW} frame, optional {@code PARTITION BY} on bigint/int/string
 * keys, and one or more {@code SUM}/{@code MIN}/{@code MAX}/{@code COUNT} aggregates that all read
 * the same bigint/int/double value column. Anything else (ROWS frame, a bounded frame, proctime,
 * AVG, multiple value columns, an unsupported key type) falls back to the host.
 */
final class OverAggregateMatcher {

  private OverAggregateMatcher() {}

  static boolean matches(StreamPhysicalOverAggregate over) {
    Window window = over.logicWindow();
    if (window.groups.size() != 1) {
      return false;
    }
    Window.Group group = window.groups.get(0);
    RelDataType inputType = over.getInput().getRowType();
    for (int key : group.keys.toArray()) {
      if (!WindowAggregateMatcher.supportedKeyType(
          inputType.getFieldList().get(key).getType().getSqlTypeName())) {
        return false; // PARTITION BY only on bigint/int/string
      }
    }
    if (group.isRows
        || !(group.lowerBound.isUnbounded() && group.lowerBound.isPreceding())
        || !group.upperBound.isCurrentRow()) {
      return false; // only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    }
    Integer order = orderColumn(group);
    if (order == null
        || !FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(
            inputType.getFieldList().get(order).getType())) {
      return false; // exactly one ascending rowtime order column
    }
    return valueColumn(group, inputType) != null;
  }

  /** The single ascending ORDER BY column index, or null if the order is not a lone ascending key. */
  private static Integer orderColumn(Window.Group group) {
    List<RelFieldCollation> collations = group.orderKeys.getFieldCollations();
    if (collations.size() != 1
        || collations.get(0).getDirection() != RelFieldCollation.Direction.ASCENDING) {
      return null;
    }
    return collations.get(0).getFieldIndex();
  }

  /**
   * The shared value column index if every aggregate is a supported kind over the same single
   * bigint/int/double input column, or null otherwise.
   */
  private static Integer valueColumn(Window.Group group, RelDataType inputType) {
    Integer column = null;
    for (Window.RexWinAggCall aggCall : group.aggCalls) {
      RexCall call = aggCall;
      int kind = overKind(call.getOperator().getKind());
      if (call.getOperands().size() != 1
          || !(call.getOperands().get(0) instanceof RexInputRef)
          || kind < 0
          || kind == WindowAggregateMatcher.KIND_AVG) {
        return null; // SUM/SUM0/MIN/MAX/COUNT only (no AVG yet), single column argument
      }
      RexNode operand = call.getOperands().get(0);
      int index = ((RexInputRef) operand).getIndex();
      if (column == null) {
        column = index;
      } else if (column != index) {
        return null; // all aggregates must read the same value column
      }
    }
    if (column == null) {
      return null;
    }
    SqlTypeName valueType = inputType.getFieldList().get(column).getType().getSqlTypeName();
    return valueType == SqlTypeName.BIGINT
            || valueType == SqlTypeName.INTEGER
            || valueType == SqlTypeName.DOUBLE
        ? column
        : null;
  }

  static int timeColumn(StreamPhysicalOverAggregate over) {
    return orderColumn(over.logicWindow().groups.get(0));
  }

  /** The PARTITION BY column indices (empty for no partition). */
  static int[] keyColumns(StreamPhysicalOverAggregate over) {
    return over.logicWindow().groups.get(0).keys.toArray();
  }

  static int valueColumnIndex(StreamPhysicalOverAggregate over) {
    return valueColumn(over.logicWindow().groups.get(0), over.getInput().getRowType());
  }

  /** Value-type code matching the native side: 0 = bigint, 1 = double, 2 = int. */
  static int valueTypeCode(StreamPhysicalOverAggregate over) {
    switch (over
        .getInput()
        .getRowType()
        .getFieldList()
        .get(valueColumnIndex(over))
        .getType()
        .getSqlTypeName()) {
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      default:
        return 0;
    }
  }

  static int[] kinds(StreamPhysicalOverAggregate over) {
    List<Window.RexWinAggCall> aggCalls = over.logicWindow().groups.get(0).aggCalls;
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      kinds[i] = overKind(aggCalls.get(i).getOperator().getKind());
    }
    return kinds;
  }

  /**
   * Native aggregate code, mapping Calcite's {@code $SUM0} (the planner decomposes user {@code SUM}
   * into {@code $SUM0} + {@code COUNT}) to the native sum: an OVER frame always contains the current
   * row, so it is never empty and {@code $SUM0} equals {@code SUM}.
   */
  private static int overKind(SqlKind kind) {
    return kind == SqlKind.SUM0 ? 0 : WindowAggregateMatcher.aggregateKind(kind);
  }
}

