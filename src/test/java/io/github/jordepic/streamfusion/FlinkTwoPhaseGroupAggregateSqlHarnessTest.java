package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Two-phase (mini-batch) non-windowed {@code GROUP BY}: the planner splits the aggregate into a
 * local pre-aggregate, a keyed shuffle, and a global merge, above a {@code MiniBatchAssigner}. The
 * native side runs a stateless per-batch local pre-aggregate (eliding the assigner) and merges the
 * partials in the same native group-aggregate operator the single-phase path uses — so the whole
 * pipeline is one columnar island. The collapsed changelog must match the host two-phase result.
 */
class FlinkTwoPhaseGroupAggregateSqlHarnessTest {

  @Test
  void keyedTwoPhaseGroupByMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-keyed-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, SUM(v) AS s, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY k");
  }

  @Test
  void globalTwoPhaseAggregateMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-global-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input), "SELECT SUM(v) AS s, COUNT(*) AS c FROM t");
  }

  @Test
  void doubleValueTwoPhaseMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-double-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input), "SELECT k, SUM(vd) AS s, MAX(vd) AS mx FROM t GROUP BY k");
  }

  @Test
  void intMinMaxCountTwoPhaseMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-int-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, MIN(vi) AS mn, MAX(vi) AS mx, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void perOperatorFlagKeepsTwoPhaseOnHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-flag-in");
    writeInput(input);
    System.setProperty("streamfusion.operator.localGroupAggregate.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          readEnvironment(input),
          "SELECT k, SUM(v) AS s FROM t GROUP BY k",
          "localGroupAggregate: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.localGroupAggregate.enabled");
    }
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, vd DOUBLE, vi INT) WITH ('connector' ="
            + " 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES (1, 10, 1.5, 7), (1, 20, 2.5, 3), (2, 5, 0.5, 9),"
                + " (1, 30, 3.5, 1), (2, 15, 1.0, 4)")
        .await();
  }

  private static Supplier<TableEnvironment> readEnvironment(Path directory) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
      tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
      tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      tEnv.getConfig().set("table.exec.mini-batch.size", "100");
      tEnv.executeSql(
          "CREATE TABLE t (k BIGINT, v BIGINT, vd DOUBLE, vi INT) WITH ('connector' = 'filesystem',"
              + " 'path' = '"
              + directory.toUri()
              + "', 'format' = 'parquet')");
      return tEnv;
    };
  }
}
