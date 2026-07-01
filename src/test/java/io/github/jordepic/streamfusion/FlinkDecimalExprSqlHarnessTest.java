package io.github.jordepic.streamfusion;

import java.math.BigDecimal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Decimal arithmetic in a Calc — Nexmark q1's {@code 0.908 * price}. Add/subtract/multiply run natively
 * and byte-exactly: the operands reach the native side as Decimal128 (columns already are; literals emit
 * as exact Decimal128), Arrow's Decimal128 arithmetic matches Flink's, and the wrapping cast to the
 * declared DECIMAL rounds HALF_UP — the same rounding Flink uses. Division/modulo derive a rounded
 * quotient scale the two engines disagree on, so they stay behind the approximate-decimal flag.
 */
class FlinkDecimalExprSqlHarnessTest {

  @Test
  void decimalTimesDecimalExactByDefault() throws Exception {
    // q1 exactly: a DECIMAL literal times a DECIMAL(23,3) column, widening to DECIMAL(28,6).
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, 0.908 * price AS price FROM t");
  }

  @Test
  void decimalTimesBigintExactByDefault() throws Exception {
    // A DECIMAL literal times a BIGINT column (bigint coerced to DECIMAL(19,0) before the multiply).
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::bigintPriceEnvironment,
        "SELECT auction, 0.908 * price AS price FROM t");
  }

  @Test
  void decimalCastExactByDefault() throws Exception {
    // The sink coercion in q1: the DECIMAL(28,6) product cast down to DECIMAL(23,3), HALF_UP — exact.
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, CAST(0.908 * price AS DECIMAL(23, 3)) AS price FROM t");
  }

  @Test
  void decimalPlusMinusExactByDefault() throws Exception {
    NativeParity.assertParity(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, price + 1.5 AS a, price - 0.001 AS b FROM t");
  }

  @Test
  void decimalDivisionFallsBackByDefault() throws Exception {
    // Division's quotient scale is engine-specific, so it is not admitted without the flag.
    NativeParity.assertFallbackReasonContains(
        FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
        "SELECT auction, price / 3 AS price FROM t",
        "decimal division/modulo not native by default");
  }

  @Test
  void decimalDivisionRoutesUnderFlag() throws Exception {
    System.setProperty("streamfusion.expression.decimalArithmetic.approximate", "true");
    try {
      NativeParity.assertRoutes(
          FlinkDecimalExprSqlHarnessTest::decimalPriceEnvironment,
          "SELECT auction, price / 3 AS price FROM t");
    } finally {
      System.clearProperty("streamfusion.expression.decimalArithmetic.approximate");
    }
  }

  private static TableEnvironment decimalPriceEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"auction", "price"}, Types.LONG, Types.BIG_DEC),
            Row.of(1L, new BigDecimal("100.000")),
            Row.of(2L, new BigDecimal("999.999")),
            Row.of(3L, new BigDecimal("0.001")),
            Row.of(4L, new BigDecimal("12345.678")));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("auction", DataTypes.BIGINT())
            .column("price", DataTypes.DECIMAL(23, 3))
            .build());
    return tEnv;
  }

  private static TableEnvironment bigintPriceEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"auction", "price"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 999L),
            Row.of(3L, 1L));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("auction", DataTypes.BIGINT())
            .column("price", DataTypes.BIGINT())
            .build());
    return tEnv;
  }
}
