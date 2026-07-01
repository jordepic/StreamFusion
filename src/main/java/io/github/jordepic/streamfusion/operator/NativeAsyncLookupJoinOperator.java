package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.arrow.ArrowReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Processing-time lookup join against an <b>async</b> connector, columnar in and out. The async sibling
 * of {@link NativeLookupJoinOperator}: for each probe {@link ArrowBatch} it materialises the rows,
 * fires the connector's real {@link AsyncLookupFunction#asyncLookup} for each <em>distinct</em> key
 * concurrently, and joins the results once they all complete — so the batch's lookup I/O overlaps
 * rather than being paid serially. Because it runs the connector's own async function and Flink's
 * {@link JoinedRowData} join semantics (inner drops non-matches, left null-pads), the result is
 * byte-identical to the host.
 *
 * <p><b>No mailbox, no in-flight state across batches.</b> Unlike Flink's {@code AsyncWaitOperator} —
 * which keeps lookups in flight across records and therefore needs the operator mailbox, an ordered
 * result queue, and a snapshot/replay of in-flight rows at checkpoint — this operator does all of a
 * batch's concurrent lookups <em>inside</em> {@link #processElement} and blocks on the task thread
 * until they finish (RisingWave's temporal-join and Arroyo's {@code lookup_join} do the same: overlap
 * within a batch, await before emitting). The Arrow batch is already the overlap unit, so a checkpoint
 * barrier — itself a task-thread action — can only run between batches, when nothing is in flight;
 * there is no in-flight state to persist, exactly as for the synchronous operator. The cost is that
 * I/O does not overlap <em>across</em> batches, which is the standard bounded-work-per-batch bargain
 * every synchronous operator makes.
 *
 * <p>Distinct-key dedup is safe: within one batch the dimension state is fixed, so calling the lookup
 * once per key rather than once per row (as Flink does) can only differ when the dimension mutates
 * mid-batch — a case where Flink's own concurrent lookups already race to a nondeterministic result.
 */
public class NativeAsyncLookupJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final AsyncLookupFunction asyncFunction;
  private final RowType probeType;
  private final RowType dimType;
  private final RowType outputType;
  private final int[] probeKeyIndices;
  private final int joinType;

  private transient BufferAllocator allocator;
  private transient RowData.FieldGetter[] keyGetters;
  private transient RowDataSerializer probeSerializer;
  private transient RowDataSerializer dimSerializer;
  private transient GenericRowData nullDimRow;

  public NativeAsyncLookupJoinOperator(
      AsyncLookupFunction asyncFunction,
      RowType probeType,
      RowType dimType,
      RowType outputType,
      int[] probeKeyIndices,
      int joinType) {
    this.asyncFunction = asyncFunction;
    this.probeType = probeType;
    this.dimType = dimType;
    this.outputType = outputType;
    this.probeKeyIndices = probeKeyIndices;
    this.joinType = joinType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    keyGetters = new RowData.FieldGetter[probeKeyIndices.length];
    for (int i = 0; i < probeKeyIndices.length; i++) {
      keyGetters[i] =
          RowData.createFieldGetter(probeType.getTypeAt(probeKeyIndices[i]), probeKeyIndices[i]);
    }
    probeSerializer = new RowDataSerializer(probeType);
    dimSerializer = new RowDataSerializer(dimType);
    nullDimRow = new GenericRowData(dimType.getFieldCount());
    asyncFunction.open(new FunctionContext(getRuntimeContext()));
  }

  @Override
  public void close() throws Exception {
    asyncFunction.close();
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    // Materialise the probe rows off the Arrow buffers before the lookup wait: the reader hands back
    // rows backed by the vectors, which are freed when the batch closes.
    List<RowData> probes = new ArrayList<>();
    try (VectorSchemaRoot root = element.getValue().root()) {
      ArrowReader reader = ArrowConversion.createArrowReader(root, probeType);
      int rowCount = root.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        probes.add(probeSerializer.copy(reader.read(i)));
      }
    }

    // Fire one lookup per distinct key so the batch's I/O overlaps, then wait for all of them.
    List<RowData> keys = new ArrayList<>(probes.size());
    Map<RowData, CompletableFuture<Collection<RowData>>> futures = new HashMap<>();
    for (RowData probe : probes) {
      RowData key = buildKey(probe);
      keys.add(key);
      futures.computeIfAbsent(key, asyncFunction::asyncLookup);
    }
    CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).join();

    List<RowData> outRows = new ArrayList<>();
    for (int i = 0; i < probes.size(); i++) {
      RowData probe = probes.get(i);
      Collection<RowData> matches = futures.get(keys.get(i)).join();
      if (matches != null && !matches.isEmpty()) {
        for (RowData dim : matches) {
          outRows.add(new JoinedRowData(RowKind.INSERT, probe, dimSerializer.copy(dim)));
        }
      } else if (joinType == 1) { // LEFT: null-pad the dimension side
        outRows.add(new JoinedRowData(RowKind.INSERT, probe, nullDimRow));
      }
    }
    // Insert-only: a processing-time lookup requires an append-only probe, so no row-kind column.
    VectorSchemaRoot out = RowDataArrowConverter.write(outRows, outputType, allocator, false);
    output.collect(new StreamRecord<>(new ArrowBatch(out)));
  }

  private RowData buildKey(RowData probe) {
    GenericRowData key = new GenericRowData(keyGetters.length);
    for (int i = 0; i < keyGetters.length; i++) {
      key.setField(i, keyGetters[i].getFieldOrNull(probe));
    }
    return key;
  }
}
