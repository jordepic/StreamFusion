package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * The shallow ingest path's format-neutral decode core. It turns raw message bodies into typed Arrow
 * batches while a format extension supplies the native decoder through the provider SPI. This class
 * owns batching, checkpoint flushing, and the Arrow C Data Interface bridge, so connector and format
 * artifacts can be installed independently.
 *
 * <p>The operator is stateless across batches. It flushes partial batches at end of input, before a
 * checkpoint barrier, and on a processing-time timer; this preserves Flink's source-checkpoint
 * contract while bounding low-volume ingest latency.
 */
public class NativeBytesDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<byte[], ArrowBatch>, BoundedOneInput {

  private final RowType outputType;
  private final int batchSize;
  private final NativeMessageDecoderFactory decoderFactory;
  private final long flushIntervalMillis;

  private transient BufferAllocator allocator;
  private transient NativeMessageDecoder decoder;
  private transient VarBinaryVector body;
  private transient int count;
  private transient boolean flushTimerPending;

  public NativeBytesDecodeOperator(
      RowType outputType,
      int batchSize,
      NativeMessageDecoderFactory decoderFactory,
      long flushIntervalMillis) {
    this.outputType = outputType;
    this.batchSize = batchSize;
    this.decoderFactory = decoderFactory;
    this.flushIntervalMillis = flushIntervalMillis;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    decoder = decoderFactory.create();
    decoder.open(allocator, outputType);
    newBody();
  }

  private void newBody() {
    body = new VarBinaryVector("body", allocator);
    body.allocateNew(batchSize);
    count = 0;
  }

  @Override
  public void processElement(StreamRecord<byte[]> element) {
    body.setSafe(count++, element.getValue());
    if (count >= batchSize) {
      flush();
    } else if (count == 1 && flushIntervalMillis > 0 && !flushTimerPending) {
      flushTimerPending = true;
      ProcessingTimeService timeService = getProcessingTimeService();
      timeService.registerTimer(
          timeService.getCurrentProcessingTime() + flushIntervalMillis,
          timestamp -> {
            flushTimerPending = false;
            if (count > 0) {
              flush();
            }
          });
    }
  }

  @Override
  public void endInput() {
    if (count > 0) {
      flush();
    }
  }

  /** Flushes before a barrier because the source checkpoint already considers buffered bytes delivered. */
  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    if (count > 0) {
      flush();
    }
  }

  private void flush() {
    try {
      decoder.beforeDecode(body, count);
      body.setValueCount(count);
      try (VectorSchemaRoot in = new VectorSchemaRoot(List.of(body));
          ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        in.setRowCount(count);
        Data.exportVectorSchemaRoot(allocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
        decoder.decodeInto(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
        VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        if (out.getRowCount() > 0) {
          output.collect(new StreamRecord<>(new ArrowBatch(out)));
        } else {
          out.close();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("native format decode failed", e);
    } finally {
      body.close();
      newBody();
    }
  }

  @Override
  public void close() throws Exception {
    if (decoder != null) {
      decoder.close();
      decoder = null;
    }
    if (body != null) {
      body.close();
    }
    super.close();
  }

}
