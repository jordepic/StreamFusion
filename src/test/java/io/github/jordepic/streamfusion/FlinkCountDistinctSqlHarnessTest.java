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
 * Non-windowed {@code GROUP BY COUNT(DISTINCT x)}: the native aggregate keeps a per-key
 * value→multiplicity map (Flink's {@code DistinctAccumulator}) and reports the number of live
 * distinct values, growing it when a value first appears and shrinking it when its last occurrence is
 * retracted. The collapsed changelog must match the host. {@code SUM}/{@code MIN}/{@code MAX} DISTINCT
 * still fall back.
 */
class FlinkCountDistinctSqlHarnessTest {

  @Test
  void countDistinctMatchesHost() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, COUNT(DISTINCT v) AS dv FROM t GROUP BY k");
  }

  @Test
  void countDistinctStringAndMixedMatchesHost() throws Exception {
    // A string distinct count alongside a plain COUNT(*) and a second distinct count in one GROUP BY.
    NativeParity.assertChangelogParity(
        FlinkCountDistinctSqlHarnessTest::environment,
        "SELECT k, COUNT(DISTINCT s) AS ds, COUNT(*) AS c, COUNT(DISTINCT v) AS dv "
            + "FROM t GROUP BY k");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    // Repeats per key so distinct < total: k=1 has v {10,20} (10 twice), s {a,b}; k=2 has v {5}, s {c}.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v", "s"}, Types.LONG, Types.LONG, Types.STRING),
            Row.of(1L, 10L, "a"),
            Row.of(1L, 10L, "a"),
            Row.of(1L, 20L, "b"),
            Row.of(2L, 5L, "c"),
            Row.of(2L, 5L, "c"),
            Row.of(1L, 20L, "b"));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
