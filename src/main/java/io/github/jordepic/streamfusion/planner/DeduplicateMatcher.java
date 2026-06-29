package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes row-time deduplication: {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime …) =
 * 1}. The host plans this as a {@link StreamPhysicalRank} whose sole order key is an event-time
 * attribute and whose range is exactly rank 1 — distinct from a value-ordered Top-N (handled by
 * {@link TopNMatcher}). Ascending is keep-first (Flink's insert-only {@code
 * RowTimeDeduplicateKeepFirstRowFunction}: per key emit the minimum-rowtime row once, on the
 * watermark); descending is keep-last (Flink's {@code RowTimeDeduplicateFunction}: per key keep the
 * maximum-rowtime row, emitting a retract changelog eagerly). A non-time order key is a Top-N, not
 * deduplication.
 */
final class DeduplicateMatcher {

  private DeduplicateMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER || rank.outputRankNumber()) {
      return false;
    }
    if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return false;
    }
    ConstantRankRange range = (ConstantRankRange) rank.rankRange();
    if (range.getRankStart() != 1 || range.getRankEnd() != 1) {
      return false; // exactly the top row per key
    }
    if (rank.orderKey().getFieldCollations().size() != 1) {
      return false; // a single order key (the rowtime); ascending = keep-first, descending = keep-last
    }
    if (!isRowtimeOrder(rank)) {
      return false; // a non-time order key is a value Top-N, not deduplication
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()));
  }

  /** Keep-last (descending rowtime) vs keep-first (ascending). Keep-last emits a retract changelog. */
  static boolean keepLast(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().get(0).getDirection().isDescending();
  }

  /** Whether the host wants UPDATE_BEFORE rows on this node's output edge (keep-last only). */
  static boolean generateUpdateBefore(StreamPhysicalRank rank) {
    return ChangelogPlanUtils.generateUpdateBefore(rank);
  }

  /** Whether the rank's sole order key is an event-time (rowtime) attribute — the dedup signal. */
  static boolean isRowtimeOrder(StreamPhysicalRank rank) {
    List<RelFieldCollation> collations = rank.orderKey().getFieldCollations();
    if (collations.size() != 1) {
      return false;
    }
    RelDataType orderType =
        rank.getInput().getRowType().getFieldList().get(collations.get(0).getFieldIndex()).getType();
    return FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(orderType);
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  static int rowtimeColumn(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().get(0).getFieldIndex();
  }

  static String unsupportedReason(StreamPhysicalRank rank) {
    return "deduplication: needs ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime ASC|DESC) = 1"
        + " (keep-first or keep-last) over an insert-only input with zero idle-state TTL";
  }
}
