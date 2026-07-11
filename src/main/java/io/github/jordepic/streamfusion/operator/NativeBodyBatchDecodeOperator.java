package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Decodes a native connector's Arrow binary body batches with a separately installed format artifact.
 * No opaque native handle crosses the two DSO boundaries: the Arrow C Data Interface is the explicit
 * interchange contract.
 */
public final class NativeBodyBatchDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final RowType outputType;
  private final NativeMessageDecoderFactory decoderFactory;

  private transient BufferAllocator allocator;
  private transient NativeMessageDecoder decoder;

  public NativeBodyBatchDecodeOperator(
      RowType outputType, NativeMessageDecoderFactory decoderFactory) {
    this.outputType = outputType;
    this.decoderFactory = decoderFactory;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    decoder = decoderFactory.create();
    decoder.open(allocator, outputType);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ArrowBatch batch = element.getValue();
    try (VectorSchemaRoot in = batch.root();
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
      decoder.decodeInto(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
      if (out.getRowCount() > 0) {
        if (element.hasTimestamp()) {
          output.collect(new StreamRecord<>(new ArrowBatch(out), element.getTimestamp()));
        } else {
          output.collect(new StreamRecord<>(new ArrowBatch(out)));
        }
      } else {
        out.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("native format decode failed", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (decoder != null) {
      decoder.close();
      decoder = null;
    }
    super.close();
  }
}
