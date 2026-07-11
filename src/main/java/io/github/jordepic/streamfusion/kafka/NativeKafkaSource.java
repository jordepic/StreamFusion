package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * FLIP-27 Kafka source that emits Arrow batches with one binary {@code body} column. The native reader
 * owns rdkafka consumption while a subsequent format-provider transformation performs decode; connector
 * and format DSOs therefore share Arrow C Data rather than private Rust handles.
 */
public final class NativeKafkaSource
    implements Source<ArrowBatch, KafkaPartitionSplit, KafkaSourceEnumState> {

  private static final long serialVersionUID = 1L;

  private final KafkaSubscriber subscriber;
  private final OffsetsInitializer startingOffsets;
  private final OffsetsInitializer stoppingOffsets;
  private final Boundedness boundedness;
  private final Properties props;
  private final String[] configKeys;
  private final String[] configValues;
  private final int maxRecords;
  private final long pollTimeoutMillis;

  public NativeKafkaSource(
      KafkaSubscriber subscriber,
      OffsetsInitializer startingOffsets,
      OffsetsInitializer stoppingOffsets,
      Boundedness boundedness,
      Properties props,
      String[] configKeys,
      String[] configValues,
      int maxRecords,
      long pollTimeoutMillis) {
    this.subscriber = subscriber;
    this.startingOffsets = startingOffsets;
    this.stoppingOffsets = stoppingOffsets;
    this.boundedness = boundedness;
    this.props = props;
    if (boundedness == Boundedness.BOUNDED) {
      props.setProperty("partition.discovery.interval.ms", "-1");
    }
    this.configKeys = configKeys;
    this.configValues = configValues;
    this.maxRecords = maxRecords;
    this.pollTimeoutMillis = pollTimeoutMillis;
  }

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SourceReader<ArrowBatch, KafkaPartitionSplit> createReader(SourceReaderContext context) {
    Supplier<SplitReader<NativeKafkaRecord, KafkaPartitionSplit>> splitReaderSupplier =
        () -> new NativeKafkaSplitReader(configKeys, configValues, maxRecords, pollTimeoutMillis);
    return new NativeKafkaSourceReader(
        splitReaderSupplier, new NativeKafkaRecordEmitter(), toConfiguration(props), context);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
    return new KafkaSourceEnumerator(
        subscriber, startingOffsets, stoppingOffsets, props, enumContext, boundedness);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint)
      throws IOException {
    return new KafkaSourceEnumerator(
        subscriber, startingOffsets, stoppingOffsets, props, enumContext, boundedness, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
    return new KafkaPartitionSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
    return new KafkaSourceEnumStateSerializer();
  }

  private static Configuration toConfiguration(Properties props) {
    Configuration config = new Configuration();
    props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
    return config;
  }
}
