package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import scala.collection.Seq;

/**
 * Recognizes the local half of a two-phase non-windowed {@code GROUP BY}: a stateless per-batch
 * pre-aggregate that emits one partial row per key ({@code [grouping.., partial0..]}) for the
 * {@link GlobalGroupAggregateMatcher global half} to merge. Scope mirrors the single-phase
 * {@link GroupAggregateMatcher}: SUM/MIN/MAX/COUNT (no AVG/distinct) over bigint/int/double values,
 * with grouping keys the boundary carries.
 *
 * <p>The native local emits each aggregate's partial in its running type ({@code SUM/MIN/MAX} of a
 * value keep that value's type, COUNT is bigint). The global reads those partials by their declared
 * (Flink) type, so each partial column's declared type must equal what the native side emits —
 * otherwise the two halves disagree on the column type. Flink's SUM partial keeps the value's own
 * type (verified against the planner — only AVG widens its sum partial), so the equality check below
 * is defensive: a partial declared wider than its value would fall back cleanly rather than
 * mismatch at the boundary.
 */
final class LocalGroupAggregateMatcher {

  private LocalGroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalLocalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    // The input row crosses into the native local; the partials never reach the host (they flow to
    // the native global), so only the input types must be carriable.
    if (!RowDataArrowConverter.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))) {
      return false;
    }
    int[] grouping = agg.grouping();
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return false;
      }
    }
    RelDataType outputType = agg.getRowType(); // [grouping.., partial0..]
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isDistinct() || call.isApproximate() || call.filterArg >= 0) {
        return false;
      }
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0 || kind == WindowAggregateMatcher.KIND_AVG || call.getArgList().size() > 1) {
        return false; // SUM/MIN/MAX/COUNT only
      }
      SqlTypeName partialType =
          outputType.getFieldList().get(grouping.length + i).getType().getSqlTypeName();
      if (kind == WindowAggregateMatcher.KIND_COUNT) {
        // COUNT(*) (empty argList) or COUNT(col); the partial is the bigint running count either way.
        if (partialType != SqlTypeName.BIGINT) {
          return false;
        }
        continue;
      }
      // SUM/MIN/MAX read a typed running value; it must be a running type, and the partial Flink
      // declares must match the value type (no widening), so the native emit and the global read
      // agree on the column type.
      SqlTypeName valueType =
          inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
      if (!isRunningType(valueType) || partialType != valueType) {
        return false;
      }
    }
    return true;
  }

  private static boolean isRunningType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.DOUBLE;
  }

  static int[] kinds(StreamPhysicalLocalGroupAggregate agg) {
    return WindowAggregateMatcher.kinds(agg.aggCalls());
  }

  static int[] valueColumns(StreamPhysicalLocalGroupAggregate agg) {
    return WindowAggregateMatcher.valueColumns(agg.aggCalls());
  }

  static int[] valueTypeCodes(StreamPhysicalLocalGroupAggregate agg) {
    return WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType());
  }

  static int[] keyColumns(StreamPhysicalLocalGroupAggregate agg) {
    return WindowAggregateMatcher.keyColumns(agg.grouping());
  }
}
