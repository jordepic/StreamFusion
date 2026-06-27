package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;

/**
 * Recognizes a global {@code FETCH}/{@code LIMIT} — Flink's {@code StreamPhysicalLimit} (plain
 * {@code LIMIT n}) and {@code StreamPhysicalSortLimit} ({@code ORDER BY … LIMIT n}). Both extend
 * Calcite {@link Sort} and both lower to a global ({@code ALL_IN_ONE}, no partition) {@code ROW_NUMBER}
 * rank with range {@code [offset+1, offset+fetch]} — exactly the append-only streaming Top-N the
 * native ranker already runs, with an empty partition key. {@code LIMIT} has an empty sort collation
 * (the ranker then keeps the first {@code N} rows by arrival, evicting the newest beyond {@code N},
 * which never enters — so it is insert-only), while {@code SORT LIMIT} carries the order keys and
 * emits a changelog as the top set changes.
 *
 * <p>Matched only when there is no {@code OFFSET} (rank start 1 — an offset is not yet supported), a
 * {@code FETCH} is present (streaming requires it), and the row type the row/Arrow conversion carries.
 * The caller additionally requires an insert-only input — only the append-only ranker is implemented
 * (a retracting input makes the host pick its retract strategy, which we do not reproduce).
 */
final class LimitMatcher {

  private LimitMatcher() {}

  static boolean matches(Sort sort) {
    if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
      return false; // an OFFSET (rank start > 1) is not supported
    }
    if (sort.fetch == null) {
      return false; // a streaming FETCH/LIMIT must bound the output (Flink rejects an unbounded one)
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(sort.getRowType()));
  }

  static long limit(Sort sort) {
    return RexLiteral.intValue(sort.fetch);
  }

  static int[] sortIndices(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }

  static int[] sortAscending(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .mapToInt(fc -> fc.getDirection().isDescending() ? 0 : 1)
        .toArray();
  }

  static int[] sortNullsFirst(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .mapToInt(fc -> nullsFirst(fc) ? 1 : 0)
        .toArray();
  }

  /** Whether nulls sort first for this column, resolving the unspecified case from the direction. */
  private static boolean nullsFirst(RelFieldCollation fc) {
    RelFieldCollation.NullDirection effective =
        fc.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
            ? fc.getDirection().defaultNullDirection()
            : fc.nullDirection;
    return effective == RelFieldCollation.NullDirection.FIRST;
  }

  static String unsupportedReason(Sort sort) {
    if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
      return "limit: an OFFSET is not supported";
    }
    if (sort.fetch == null) {
      return "limit: a FETCH/LIMIT count is required on a streaming query";
    }
    return "limit: needs a row type the Arrow conversion supports and an insert-only input";
  }
}
