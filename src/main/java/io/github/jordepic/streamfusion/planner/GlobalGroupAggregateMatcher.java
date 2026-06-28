package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;

/**
 * Recognizes the global half of a two-phase non-windowed {@code GROUP BY}: it merges the local
 * half's partials into the final per-key result, emitting a changelog exactly like the single-phase
 * {@link GroupAggregateMatcher} — so it reuses the same native group-aggregate operator. Its input
 * is the local's {@code [grouping.., partial0..partialN-1]}; the partials are positional (the i-th
 * partial is the i-th aggregate's), so the merge aggregate's own argList is not used to locate it.
 *
 * <p>Each merge folds a partial into the running result: SUM/MIN/MAX merge as themselves, and a
 * COUNT merges by <em>summing</em> the partial counts (so the global treats COUNT as a SUM over its
 * bigint partial column). Scope matches the local half: bigint/int/double partials, and grouping
 * keys the boundary carries.
 */
final class GlobalGroupAggregateMatcher {

  private GlobalGroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalGlobalGroupAggregate agg) {
    return unsupportedReason(agg) == null;
  }

  static String unsupportedReason(StreamPhysicalGlobalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    if (!RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getRowType()))) {
      return "global group aggregate: an output column type the boundary cannot carry";
    }
    int[] grouping = agg.grouping();
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return "global group aggregate: grouping keys must be bigint/int/string/boolean/date/"
            + "timestamp/decimal";
      }
    }
    int base = grouping.length;
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0 || kind == WindowAggregateMatcher.KIND_AVG || call.getArgList().size() > 1) {
        return "global group aggregate: only single-field SUM/MIN/MAX/COUNT merges (no AVG)";
      }
      SqlTypeName partialType =
          inputType.getFieldList().get(base + i).getType().getSqlTypeName();
      if (partialCode(partialType) < 0) {
        return "global group aggregate: partial columns must be bigint/int/double";
      }
    }
    return null;
  }

  /** Native value-type code for a partial column, or -1 if unsupported. */
  private static int partialCode(SqlTypeName type) {
    switch (type) {
      case BIGINT:
        return 0;
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      default:
        return -1;
    }
  }

  static int[] keyColumns(StreamPhysicalGlobalGroupAggregate agg) {
    return agg.grouping();
  }

  /** The positional partial columns: {@code [grouping.., partial0..]}, one per aggregate. */
  static int[] valueColumns(StreamPhysicalGlobalGroupAggregate agg) {
    int base = agg.grouping().length;
    int[] columns = new int[agg.aggCalls().size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = base + i;
    }
    return columns;
  }

  static int[] valueTypeCodes(StreamPhysicalGlobalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    int base = agg.grouping().length;
    int[] codes = new int[agg.aggCalls().size()];
    for (int i = 0; i < codes.length; i++) {
      codes[i] = partialCode(inputType.getFieldList().get(base + i).getType().getSqlTypeName());
    }
    return codes;
  }

  /** Merge kinds: COUNT merges by summing its partial counts, so it folds as a SUM. */
  static int[] kinds(StreamPhysicalGlobalGroupAggregate agg) {
    int[] kinds = new int[agg.aggCalls().size()];
    for (int i = 0; i < kinds.length; i++) {
      int kind = WindowAggregateMatcher.aggregateKind(agg.aggCalls().apply(i).getAggregation().getKind());
      kinds[i] = kind == WindowAggregateMatcher.KIND_COUNT ? WindowAggregateMatcher.KIND_SUM : kind;
    }
    return kinds;
  }

  static boolean generateUpdateBefore(StreamPhysicalGlobalGroupAggregate agg) {
    return ChangelogPlanUtils.generateUpdateBefore(agg);
  }
}
