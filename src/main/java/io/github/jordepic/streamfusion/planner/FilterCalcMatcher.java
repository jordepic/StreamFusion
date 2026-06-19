package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Recognizes a pure filter the native operator can run: a {@link Calc} with an identity projection
 * (no column rewriting — projection support is separate) whose condition is a conjunction of
 * comparisons of a numeric column against a literal, over an input whose every column the whole-row
 * converter handles. Anything else — projections, disjunctions, non-comparison terms, unsupported
 * column types — falls back.
 */
final class FilterCalcMatcher {

  private FilterCalcMatcher() {}

  /**
   * One {@code column op literal} comparison: op codes 0=&gt;, 1=&gt;=, 2=&lt;, 3=&lt;=, 4==, 5=&lt;&gt;.
   * A string comparison (equality only) carries its value in {@code stringLiteral}; a numeric one in
   * {@code literal} with {@code stringLiteral} null.
   */
  private static final class Comparison {
    final int column;
    final int op;
    final double literal;
    final String stringLiteral;

    Comparison(int column, int op, double literal, String stringLiteral) {
      this.column = column;
      this.op = op;
      this.literal = literal;
      this.stringLiteral = stringLiteral;
    }
  }

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    // A filter (the native value), with a projection that is a list of input columns — a column
    // subset/reorder. A projection containing a computed expression or a constant (as `col =
    // literal` constant-folds the column into) falls back until general projection lands.
    if (program.getCondition() == null || projection(calc) == null) {
      return false;
    }
    RelDataType inputType = calc.getInput().getRowType();
    if (!convertibleRow(inputType)) {
      return false;
    }
    List<Comparison> comparisons = flatten(condition(calc), inputType);
    return comparisons != null && !comparisons.isEmpty();
  }

  static int[] columnIndices(Calc calc) {
    List<Comparison> comparisons = flatten(condition(calc), calc.getInput().getRowType());
    int[] columns = new int[comparisons.size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = comparisons.get(i).column;
    }
    return columns;
  }

  static int[] opCodes(Calc calc) {
    List<Comparison> comparisons = flatten(condition(calc), calc.getInput().getRowType());
    int[] ops = new int[comparisons.size()];
    for (int i = 0; i < ops.length; i++) {
      ops[i] = comparisons.get(i).op;
    }
    return ops;
  }

  static double[] literals(Calc calc) {
    List<Comparison> comparisons = flatten(condition(calc), calc.getInput().getRowType());
    double[] values = new double[comparisons.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = comparisons.get(i).literal;
    }
    return values;
  }

  static String[] stringLiterals(Calc calc) {
    List<Comparison> comparisons = flatten(condition(calc), calc.getInput().getRowType());
    String[] values = new String[comparisons.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = comparisons.get(i).stringLiteral;
    }
    return values;
  }

  private static RexNode condition(Calc calc) {
    RexProgram program = calc.getProgram();
    RexNode condition = program.expandLocalRef(program.getCondition());
    // Calcite folds `col = literal`, IN, and ranges into a SEARCH/Sarg; expand it back so a point
    // search becomes an equality and a range becomes an AND of comparisons the matcher understands.
    return RexUtil.expandSearch(calc.getCluster().getRexBuilder(), null, condition);
  }

  /**
   * Flattens a conjunction of comparisons into a list, or null if the condition contains anything
   * other than `AND`s of supported `column op literal` comparisons.
   */
  private static List<Comparison> flatten(RexNode condition, RelDataType inputType) {
    if (!(condition instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) condition;
    if (call.getKind() == SqlKind.AND) {
      List<Comparison> all = new ArrayList<>();
      for (RexNode operand : call.getOperands()) {
        List<Comparison> nested = flatten(operand, inputType);
        if (nested == null) {
          return null;
        }
        all.addAll(nested);
      }
      return all;
    }
    Comparison comparison = comparison(call, inputType);
    if (comparison == null) {
      return null;
    }
    List<Comparison> single = new ArrayList<>();
    single.add(comparison);
    return single;
  }

  /**
   * A single comparison of a supported numeric column against a literal, or null. Handles either
   * operand order, flipping the operator when the literal is on the left.
   */
  private static Comparison comparison(RexCall call, RelDataType inputType) {
    int op = opCode(call.getKind());
    if (op < 0 || call.getOperands().size() != 2) {
      return null;
    }
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    RexInputRef ref;
    RexLiteral value;
    boolean literalOnLeft;
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      ref = (RexInputRef) left;
      value = (RexLiteral) right;
      literalOnLeft = false;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      ref = (RexInputRef) right;
      value = (RexLiteral) left;
      literalOnLeft = true;
    } else {
      return null;
    }
    SqlTypeName columnType = inputType.getFieldList().get(ref.getIndex()).getType().getSqlTypeName();
    int finalOp = literalOnLeft ? flip(op) : op;
    if (numeric(columnType)) {
      Double literal = value.getValueAs(Double.class);
      return literal == null ? null : new Comparison(ref.getIndex(), finalOp, literal, null);
    }
    if (columnType == SqlTypeName.VARCHAR || columnType == SqlTypeName.CHAR) {
      // Strings support equality only (= and <>, which are symmetric under operand flip).
      if (finalOp != 4 && finalOp != 5) {
        return null;
      }
      String literal = value.getValueAs(String.class);
      return literal == null ? null : new Comparison(ref.getIndex(), finalOp, 0.0, literal);
    }
    return null;
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
          break;
        default:
          return false;
      }
    }
    return true;
  }
}
