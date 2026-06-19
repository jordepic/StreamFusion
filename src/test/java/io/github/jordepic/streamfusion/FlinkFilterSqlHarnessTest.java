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

class FlinkFilterSqlHarnessTest {

  @Test
  void numericFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v > 20");
  }

  @Test
  void reversedOperandFilterMatchesHost() throws Exception {
    // Literal on the left: `20 >= v` is `v <= 20` — the matcher flips the operator.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE 20 >= v");
  }

  @Test
  void bigintInequalityFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE k <> 2");
  }

  @Test
  void conjunctionFilterMatchesHost() throws Exception {
    // A conjunction of comparisons across two columns; the native filter ANDs the masks.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v > 15 AND k <= 3");
  }

  @Test
  void projectionWithFilterMatchesHost() throws Exception {
    // A column subset/reorder projection alongside the filter — projected after filtering.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT s, k FROM f WHERE v > 15");
  }

  @Test
  void disjunctionFilterMatchesHost() throws Exception {
    // OR of comparisons across columns — disjunctive normal form, ORed in the native filter.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v > 35 OR k < 2");
  }

  @Test
  void mixedAndOrFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment,
        "SELECT * FROM f WHERE (v >= 30 AND k <> 4) OR s <> 'a'");
  }

  @Test
  void rangeFilterMatchesHost() throws Exception {
    // BETWEEN folds to a SEARCH range, which expands to an AND of two comparisons.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE v BETWEEN 20 AND 35");
  }

  @Test
  void stringInequalityFilterMatchesHost() throws Exception {
    // `<>` preserves the column (no constant folding), so a string inequality routes.
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE s <> 'b'");
  }

  @Test
  void mixedStringAndNumericFilterMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkFilterSqlHarnessTest::environment, "SELECT * FROM f WHERE s <> 'a' AND v >= 20");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "v", "s"}, Types.LONG, Types.INT, Types.STRING),
            Row.of(1L, 10, "a"),
            Row.of(2L, 30, "b"),
            Row.of(3L, 20, "c"),
            Row.of(4L, 40, "d"));
    tEnv.createTemporaryView(
        "f",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
