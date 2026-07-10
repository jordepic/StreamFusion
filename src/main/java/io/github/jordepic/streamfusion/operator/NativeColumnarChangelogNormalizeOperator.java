package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Changelog normalization (Flink's {@code ChangelogNormalize}), fed Arrow batches and emitting Arrow
 * batches. Keeps the last full row per unique key and turns an upsert/duplicate-bearing changelog
 * into a regular INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog (the row kind read and written on
 * the batch's {@code $row_kind$} column). Proctime — it emits synchronously per input batch, so it
 * forwards watermarks unchanged. Columnar in and out, so it pays no per-operator transpose; the keyed
 * shuffle stays columnar where the input is a columnar producer.
 */
public class NativeColumnarChangelogNormalizeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyColumns;
  private final int[] keyTimestampPrecisions;
  private final boolean generateUpdateBefore;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarChangelogNormalizeOperator(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      int maxParallelism) {
    this.keyColumns = keyColumns;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.generateUpdateBefore = generateUpdateBefore;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException(
          "native changelog-normalization state requires a positive max parallelism");
    }
    this.maxParallelism = maxParallelism;
  }

  @Override
  protected boolean isUsingCustomRawKeyedState() {
    return true;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    java.util.List<byte[]> snapshots = RawKeyedState.restore(context);
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    handle =
        snapshots.isEmpty()
            ? Native.createChangelogNormalizer(
                keyColumns, keyTimestampPrecisions, generateUpdateBefore, memoryBudget.bytes())
            : Native.restoreChangelogNormalizerPartitions(
                keyColumns,
                keyTimestampPrecisions,
                generateUpdateBefore,
                snapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.pushChangelogNormalizer(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.changelogNormalizerStateBytes(handle));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    int[] keyGroups =
        Native.changelogNormalizerSnapshotKeyGroups(
            handle, maxParallelism, keyTimestampPrecisions);
    RawKeyedState.snapshot(
        context,
        keyGroups,
        keyGroup ->
            Native.snapshotChangelogNormalizerKeyGroup(
                handle, keyGroup, maxParallelism, keyTimestampPrecisions));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeChangelogNormalizer(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
