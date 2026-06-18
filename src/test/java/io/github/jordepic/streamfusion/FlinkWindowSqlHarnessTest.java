package io.github.jordepic.streamfusion;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkWindowSqlHarnessTest {

  @Test
  void tumblingSumMatchesHost() throws Exception {
    assertWindowParity("SUM(`value`)");
  }

  @Test
  void tumblingMaxMatchesHost() throws Exception {
    assertWindowParity("MAX(`value`)");
  }

  @Test
  void tumblingAvgMatchesHost() throws Exception {
    assertWindowParity("AVG(`value`)");
  }

  private static void assertWindowParity(String aggregate) throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, "
            + aggregate
            + " AS agg "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static TableEnvironment environmentWithSource() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");

    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"ts", "value"}, Types.LONG, Types.LONG),
                Row.of(0L, 1L),
                Row.of(500L, 2L),
                Row.of(1000L, 3L),
                Row.of(1500L, 4L),
                Row.of(2500L, 5L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(0)));

    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("ts", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
