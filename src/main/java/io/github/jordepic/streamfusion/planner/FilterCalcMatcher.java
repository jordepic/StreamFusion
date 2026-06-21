package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;

/**
 * Recognizes a filter the native operator can run: a {@link Calc} with an input-ref projection (a
 * column subset/reorder — general projections are separate) whose condition the native expression
 * engine can evaluate, over an input whose every column the whole-row converter handles. The
 * condition is encoded by {@link RexExpression}; anything it cannot encode — an unsupported
 * operation or operand — falls back, as do computed/constant projections and unsupported column
 * types.
 */
final class FilterCalcMatcher {

  private FilterCalcMatcher() {}

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    if (program.getCondition() == null || projection(calc) == null) {
      return false;
    }
    if (!convertibleRow(calc.getInput().getRowType())) {
      return false;
    }
    return encodedCondition(calc) != null;
  }

  /** The condition encoded for the native engine, or null if it contains an unsupported operation. */
  static RexExpression encodedCondition(Calc calc) {
    RexProgram program = calc.getProgram();
    RexNode condition = program.expandLocalRef(program.getCondition());
    // Calcite folds `col = literal`, IN, and ranges into a SEARCH/Sarg; expand it back into the
    // comparisons the native engine evaluates. The native side handles arbitrary AND/OR/NOT trees,
    // so no disjunctive-normal-form rewrite is needed.
    RexNode expanded = RexUtil.expandSearch(calc.getCluster().getRexBuilder(), null, condition);
    return RexExpression.encode(expanded);
  }

  /**
   * The output→input column mapping if every projected expression is a plain input column reference
   * (a subset/reorder), or null if any projection is a computed expression or constant.
   */
  static int[] projection(Calc calc) {
    RexProgram program = calc.getProgram();
    List<RexLocalRef> projects = program.getProjectList();
    int[] columns = new int[projects.size()];
    for (int i = 0; i < projects.size(); i++) {
      RexNode expr = program.expandLocalRef(projects.get(i));
      if (!(expr instanceof RexInputRef)) {
        return null;
      }
      columns[i] = ((RexInputRef) expr).getIndex();
    }
    return columns;
  }

  /** Whether every input column has a type the whole-row converter can carry. */
  static boolean convertibleRow(RelDataType inputType) {
    for (RelDataType field : inputType.getFieldList().stream().map(f -> f.getType()).toList()) {
      switch (field.getSqlTypeName()) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case CHAR:
        case VARCHAR:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DATE:
        case DECIMAL:
          break;
        default:
          return false;
      }
    }
    return true;
  }
}
