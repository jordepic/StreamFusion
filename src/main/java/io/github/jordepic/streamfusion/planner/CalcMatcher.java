package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexProgram;

/**
 * Recognizes a general {@link Calc} the native engine can run — an optional condition plus arbitrary
 * projection expressions (computed columns, constants, column subsets) — over an input whose columns
 * the whole-row converter handles. The pure filter-plus-column-subset shape stays with {@link
 * FilterCalcMatcher} (its column-transfer projection avoids evaluating identity expressions); this
 * matcher covers everything else the native expression engine can encode, and falls back otherwise.
 */
final class CalcMatcher {

  private CalcMatcher() {}

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    // A condition with an all-column-reference projection is the filter path's case; leave it there.
    if (program.getCondition() != null && allInputRefs(program)) {
      return false;
    }
    if (!FilterCalcMatcher.convertibleRow(calc.getInput().getRowType())) {
      return false;
    }
    return RexExpression.encodeCalc(calc) != null;
  }

  /** The encoded Calc (condition + projections), or null if it contains an unsupported operation. */
  static RexExpression encode(Calc calc) {
    return RexExpression.encodeCalc(calc);
  }

  private static boolean allInputRefs(RexProgram program) {
    for (RexLocalRef ref : program.getProjectList()) {
      if (!(program.expandLocalRef(ref) instanceof RexInputRef)) {
        return false;
      }
    }
    return true;
  }
}
