package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeFormatProviders;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeBytesDecodeOperator;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Zero-input exec node for the shallow native-decode Kafka path. It chains two transformations: Flink's
 * {@code KafkaSource} producing raw value {@code byte[]}s, then a {@link NativeBytesDecodeOperator} that
 * batches the bytes and decodes them natively to Arrow. The result starts the pipeline columnar without
 * Flink ever materializing a {@code RowData} for the message.
 */
public class NativeKafkaDecodeExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String SOURCE_TRANSFORMATION = "kafka-bytes-source";
  private static final String DECODE_TRANSFORMATION = "native-decode";
  // Bytes accumulated into one Arrow body batch before a native decode (matches the columnar batch size
  // the rest of the pipeline uses).
  private static final int BATCH_SIZE = 8192;
  // Longest a buffered record waits before a partial batch flushes — the latency bound the batching
  // trades against per-batch decode efficiency (the native Kafka source's poll timeout is the same
  // order, so both ingest paths cap tail latency alike).
  private static final long FLUSH_INTERVAL_MILLIS = 100;
  private final RowType outputType;
  private final RowType writerType;
  private final Map<String, String> options;

  public NativeKafkaDecodeExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      RowType writerType,
      String description,
      Map<String, String> options) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-kafka-decode_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.outputType = outputType;
    this.writerType = writerType;
    this.options = options;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    KafkaSource<byte[]> source = KafkaTables.buildBytesSource(options);
    DataStreamSource<byte[]> bytes =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            SOURCE_TRANSFORMATION,
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
    NativeFormatContext formatContext =
        new NativeFormatContext(outputType, writerType, options, KafkaTables.ignoreParseErrors(options));
    NativeFormatProvider formatProvider =
        NativeFormatProviders.find(formatContext)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No installed StreamFusion provider for format "
                            + NativeFormatProviders.formatIdentifier(options)));
    DataStream<ArrowBatch> decoded =
        bytes.transform(
            DECODE_TRANSFORMATION,
            ArrowBatchTypeInformation.INSTANCE,
            new NativeBytesDecodeOperator(
                outputType,
                BATCH_SIZE,
                formatProvider.createDecoder(formatContext),
                FLUSH_INTERVAL_MILLIS));
    return decoded.getTransformation();
  }
}
