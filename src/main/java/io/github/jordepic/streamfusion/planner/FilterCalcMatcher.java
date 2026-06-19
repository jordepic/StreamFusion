package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Recognizes a pure filter the native operator can run: a {@link Calc} with an identity projection
 * (no column rewriting — projection support is separate) whose condition is a single comparison of
 * a numeric column against a literal, over an input whose every column the whole-row converter
 * handles. Anything else — projections, compound or non-comparison predicates, unsupported column
 * types — falls back.
 */
final class FilterCalcMatcher {

  private FilterCalcMatcher() {}

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    if (program.getCondition() == null || !isIdentityProjection(program)) {
      return false;
    }
    RelDataType inputType = calc.getInput().getRowType();
    if (!convertibleRow(inputType)) {
      return false;
    }
    RexNode condition = program.expandLocalRef(program.getCondition());
    return comparison(condition, inputType) != null;
  }

  /** The predicate column index. */
  static int columnIndex(Calc calc) {
    return comparison(condition(calc), calc.getInput().getRowType())[0];
  }

  /** The comparison op code: 0=&gt;, 1=&gt;=, 2=&lt;, 3=&lt;=, 4==, 5=&lt;&gt;. */
  static int opCode(Calc calc) {
    return comparison(condition(calc), calc.getInput().getRowType())[1];
  }

  /** The literal compared against, as a double. */
  static double literal(Calc calc) {
    RexCall call = (RexCall) condition(calc);
    for (RexNode operand : call.getOperands()) {
      if (operand instanceof RexLiteral) {
        return ((RexLiteral) operand).getValueAs(Double.class);
      }
    }
    throw new IllegalStateException("no literal in comparison");
  }

  private static RexNode condition(Calc calc) {
    RexProgram program = calc.getProgram();
    return program.expandLocalRef(program.getCondition());
  }

  /**
   * For a comparison of a supported numeric column against a literal, returns {@code [columnIndex,
   * opCode]}; otherwise null. Handles either operand order, flipping the operator when the literal
   * is on the left.
   */
  private static int[] comparison(RexNode condition, RelDataType inputType) {
    if (!(condition instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) condition;
    int op = opCode(call.getKind());
    if (op < 0 || call.getOperands().size() != 2) {
      return null;
    }
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    RexInputRef ref;
    boolean literalOnLeft;
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      ref = (RexInputRef) left;
      literalOnLeft = false;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      ref = (RexInputRef) right;
      literalOnLeft = true;
    } else {
      return null;
    }
    if (!numeric(inputType.getFieldList().get(ref.getIndex()).getType().getSqlTypeName())) {
      return null;
    }
    return new int[] {ref.getIndex(), literalOnLeft ? flip(op) : op};
  }

  private static boolean isIdentityProjection(RexProgram program) {
    List<RexLocalRef> projects = program.getProjectList();
    if (projects.size() != program.getInputRowType().getFieldCount()) {
      return false;
    }
    for (int i = 0; i < projects.size(); i++) {
      RexNode expr = program.expandLocalRef(projects.get(i));
      if (!(expr instanceof RexInputRef) || ((RexInputRef) expr).getIndex() != i) {
        return false;
      }
    }
    return true;
  }

  private static int opCode(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return 0;
      case GREATER_THAN_OR_EQUAL:
        return 1;
      case LESS_THAN:
        return 2;
      case LESS_THAN_OR_EQUAL:
        return 3;
      case EQUALS:
        return 4;
      case NOT_EQUALS:
        return 5;
      default:
        return -1;
    }
  }

  /** Swaps the operator's sense when the literal is on the left (e.g. {@code 5 < x} ≡ {@code x > 5}). */
  private static int flip(int op) {
    switch (op) {
      case 0:
        return 2;
      case 1:
        return 3;
      case 2:
        return 0;
      case 3:
        return 1;
      default:
        return op; // = and <> are symmetric
    }
  }

  private static boolean numeric(SqlTypeName type) {
    return type == SqlTypeName.TINYINT
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.BIGINT
        || type == SqlTypeName.FLOAT
        || type == SqlTypeName.DOUBLE;
  }

  /** Whether every input column has a type the whole-row converter can carry. */
  private static boolean convertibleRow(RelDataType inputType) {
    for (RelDataType field :
        inputType.getFieldList().stream().map(f -> f.getType()).toList()) {
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
          break;
        default:
          return false;
      }
    }
    return true;
  }
}
