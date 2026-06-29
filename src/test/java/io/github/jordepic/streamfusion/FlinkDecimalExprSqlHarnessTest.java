package io.github.jordepic.streamfusion;

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
 * Decimal arithmetic in a Calc — Nexmark q1's {@code 0.908 * price} (a DECIMAL literal times a BIGINT,
 * widening to DECIMAL(23,3)). Native decimal arithmetic is not implemented yet, so this must fall back
 * cleanly (the native engine would compute it in float and the converter would mis-read a DECIMAL
 * column). The fallback still produces the host result.
 */
class FlinkDecimalExprSqlHarnessTest {

  @Test
  void decimalArithmeticFallsBackCleanly() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkDecimalExprSqlHarnessTest::environment,
        "SELECT auction, 0.908 * price AS price FROM t",
        "decimal arithmetic not yet native");
  }

  private static TableEnvironment environment() {
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
