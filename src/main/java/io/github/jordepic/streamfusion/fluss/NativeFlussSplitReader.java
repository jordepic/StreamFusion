package io.github.jordepic.streamfusion.fluss;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.NativeAllocator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.fluss.flink.source.split.SourceSplitBase;

/**
 * Native Fluss split reader for one Flink subtask. Fluss' enumerator assigns concrete log splits;
 * this reader subscribes those buckets in fluss-rs and drains Arrow {@code RecordBatch}es directly.
 */
final class NativeFlussSplitReader implements SplitReader<NativeFlussRecord, SourceSplitBase> {

  private static final long NO_PARTITION = Long.MIN_VALUE;
  private static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;

  private final long handle;
  private final long pollTimeoutMillis;
  private final BufferAllocator allocator = NativeAllocator.SHARED;
  private final Map<String, Long> stoppingOffsets = new HashMap<>();
  private final Map<String, Long> positions = new HashMap<>();
  private final Map<String, NativeFlussLogSplit> splitsById = new HashMap<>();
  private final Set<String> finished = new HashSet<>();
  private final Set<String> pendingFinishedSplits = new HashSet<>();

  NativeFlussSplitReader(
      String[] configKeys,
      String[] configValues,
      String databaseName,
      String tableName,
      int[] projectedFields,
      int rowtimeIndex,
      long pollTimeoutMillis) {
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.handle =
        NativeFluss.openFlussReader(
            configKeys, configValues, databaseName, tableName, projectedFields, rowtimeIndex);
  }

  @Override
  public RecordsWithSplitIds<NativeFlussRecord> fetch() {
    if (!pendingFinishedSplits.isEmpty()) {
      RecordsBySplits.Builder<NativeFlussRecord> finishedBuilder = new RecordsBySplits.Builder<>();
      finishedBuilder.addFinishedSplits(pendingFinishedSplits);
      pendingFinishedSplits.clear();
      return finishedBuilder.build();
    }
    int pending = NativeFluss.pollFlussBatch(handle, pollTimeoutMillis);
    RecordsBySplits.Builder<NativeFlussRecord> builder = new RecordsBySplits.Builder<>();
    for (int i = 0; i < pending; i++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[2];
        String[] splitId = new String[1];
        NativeFluss.drainFlussSplit(
            handle, meta, splitId, outArray.memoryAddress(), outSchema.memoryAddress());
        VectorSchemaRoot root =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        positions.put(splitId[0], meta[0]);
        builder.add(splitId[0], new NativeFlussRecord(new ArrowBatch(root), meta[0], meta[1]));
      }
    }

    List<NativeFlussLogSplit> justFinished = new ArrayList<>();
    for (Map.Entry<String, Long> stop : stoppingOffsets.entrySet()) {
      String splitId = stop.getKey();
      if (!finished.contains(splitId)
          && positions.getOrDefault(splitId, Long.MIN_VALUE) >= stop.getValue()) {
        builder.addFinishedSplit(splitId);
        finished.add(splitId);
        justFinished.add(splitsById.get(splitId));
      }
    }
    if (!justFinished.isEmpty()) {
      NativeFluss.unassignFlussSplits(
          handle,
          tableIds(justFinished),
          partitionIds(justFinished),
          buckets(justFinished));
    }
    return builder.build();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {
    if (splitsChanges instanceof SplitsRemoval) {
      removeSplits(splitsChanges.splits());
      return;
    }
    List<SourceSplitBase> splits = splitsChanges.splits();
    List<NativeFlussLogSplit> nativeSplits = new ArrayList<>(splits.size());
    for (SourceSplitBase split : splits) {
      NativeFlussLogSplit nativeSplit = FlussSplitTranslator.translateLogSplit(split);
      OptionalLong stoppingOffset = nativeSplit.stoppingOffset();
      if (stoppingOffset.isPresent() && nativeSplit.startingOffset() >= stoppingOffset.getAsLong()) {
        pendingFinishedSplits.add(nativeSplit.splitId());
        continue;
      }
      nativeSplits.add(nativeSplit);
      splitsById.put(nativeSplit.splitId(), nativeSplit);
      positions.put(nativeSplit.splitId(), nativeSplit.startingOffset());
      stoppingOffset.ifPresent(stop -> stoppingOffsets.put(nativeSplit.splitId(), stop));
    }
    if (!nativeSplits.isEmpty()) {
      NativeFluss.assignFlussSplits(
          handle,
          splitIds(nativeSplits),
          tableIds(nativeSplits),
          partitionIds(nativeSplits),
          buckets(nativeSplits),
          startingOffsets(nativeSplits),
          stoppingOffsets(nativeSplits));
    }
  }

  /**
   * Unsubscribes every assigned split belonging to the removed partitions and returns their table
   * buckets for the coordinator ack — the split reader answers the "which splits" question from
   * its own bookkeeping, exactly as Fluss's {@code FlinkSourceSplitReader.removePartitions} does.
   * The removed splits are reported as finished on the next {@link #fetch()} so the source reader
   * drops them from checkpoint state.
   */
  Set<org.apache.fluss.metadata.TableBucket> removePartitions(Map<Long, String> removedPartitions) {
    List<NativeFlussLogSplit> removed = new ArrayList<>();
    Set<org.apache.fluss.metadata.TableBucket> buckets = new HashSet<>();
    for (NativeFlussLogSplit split : List.copyOf(splitsById.values())) {
      OptionalLong partitionId = split.partitionId();
      if (partitionId.isPresent() && removedPartitions.containsKey(partitionId.getAsLong())) {
        removed.add(split);
        buckets.add(
            new org.apache.fluss.metadata.TableBucket(
                split.tableId(), partitionId.getAsLong(), (int) split.bucket()));
      }
    }
    for (NativeFlussLogSplit split : removed) {
      splitsById.remove(split.splitId());
      stoppingOffsets.remove(split.splitId());
      positions.remove(split.splitId());
      if (!finished.remove(split.splitId())) {
        pendingFinishedSplits.add(split.splitId());
      }
    }
    if (!removed.isEmpty()) {
      NativeFluss.unassignFlussSplits(
          handle, tableIds(removed), partitionIds(removed), buckets(removed));
    }
    return buckets;
  }

  private void removeSplits(List<SourceSplitBase> splits) {
    List<NativeFlussLogSplit> nativeSplits = new ArrayList<>(splits.size());
    for (SourceSplitBase split : splits) {
      NativeFlussLogSplit nativeSplit = splitsById.remove(split.splitId());
      boolean assigned = nativeSplit != null;
      if (nativeSplit == null) {
        nativeSplit = FlussSplitTranslator.translateLogSplit(split);
      }
      nativeSplits.add(nativeSplit);
      stoppingOffsets.remove(nativeSplit.splitId());
      positions.remove(nativeSplit.splitId());
      if (assigned && !finished.remove(nativeSplit.splitId())) {
        pendingFinishedSplits.add(nativeSplit.splitId());
      }
    }
    if (!nativeSplits.isEmpty()) {
      NativeFluss.unassignFlussSplits(
          handle,
          tableIds(nativeSplits),
          partitionIds(nativeSplits),
          buckets(nativeSplits));
    }
  }

  @Override
  public void wakeUp() {
    // Native poll uses a short bounded timeout; no interrupt is needed.
  }

  @Override
  public void close() {
    NativeFluss.closeFlussReader(handle);
  }

  private static String[] splitIds(List<NativeFlussLogSplit> splits) {
    String[] values = new String[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = splits.get(i).splitId();
    }
    return values;
  }

  private static long[] tableIds(List<NativeFlussLogSplit> splits) {
    long[] values = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = splits.get(i).tableId();
    }
    return values;
  }

  private static long[] partitionIds(List<NativeFlussLogSplit> splits) {
    long[] values = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = splits.get(i).partitionId().orElse(NO_PARTITION);
    }
    return values;
  }

  private static long[] buckets(List<NativeFlussLogSplit> splits) {
    long[] values = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = splits.get(i).bucket();
    }
    return values;
  }

  private static long[] startingOffsets(List<NativeFlussLogSplit> splits) {
    long[] values = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = splits.get(i).startingOffset();
    }
    return values;
  }

  private static long[] stoppingOffsets(List<NativeFlussLogSplit> splits) {
    long[] values = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = splits.get(i).stoppingOffset().orElse(NO_STOPPING_OFFSET);
    }
    return values;
  }
}
