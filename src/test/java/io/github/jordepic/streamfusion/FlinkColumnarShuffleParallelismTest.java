package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
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
 * Exercises the columnar keyed shuffle across multiple channels end to end at parallelism 2. The
 * input is written as several Parquet files; the native source shards them across two subtasks, and
 * a keyed window runs at parallelism 2, so the columnar exchange splits each batch by key and routes
 * the sub-batches to two downstream window subtasks. With eight keys spread across both channels,
 * this is the real cross-channel routing the p=1 pipeline never reaches — and it must still match
 * the host.
 */
class FlinkColumnarShuffleParallelismTest {

  private static final String WINDOW_QUERY =
      "SELECT k, window_start, window_end, SUM(v) AS total "
          + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
          + "GROUP BY k, window_start, window_end";

  @Test
  void keyedWindowAtParallelismTwoMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cshuffle-p2-in");
    writeInput(input);
    NativeParity.assertParity(() -> readEnvironment(input), WINDOW_QUERY);
  }

  /** Writes the input as multiple Parquet files (parallel write) so the sharded read has work per subtask. */
  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    // 48 rows: 8 keys, spread across a few 1-second windows.
    DataStream<Row> source =
        env.fromSequence(0, 47)
            .map(
                i ->
                    Row.of(
                        i % 8,
                        i,
                        Instant.ofEpochMilli((i / 8) * 1000L + (i % 8) * 100L)))
            .returns(Types.ROW_NAMED(new String[] {"k", "v", "rt"}, Types.LONG, Types.LONG, Types.INSTANT));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .build());
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql("INSERT INTO in_write SELECT * FROM s").await();
  }

  private static TableEnvironment readEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    // Delay larger than the whole data span (~5.7s) so no window closes before end-of-input MAX —
    // isolates the shuffle routing from the per-batch watermark divergence (divergences/09), which
    // at p>1 otherwise shows up as the host late-dropping a borderline first window.
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), "
            + "WATERMARK FOR rt AS rt - INTERVAL '10' SECOND) WITH ('connector' = 'filesystem', "
            + "'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
