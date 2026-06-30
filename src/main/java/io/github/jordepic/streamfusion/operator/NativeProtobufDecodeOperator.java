package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Decodes a batch of one binary column — raw protobuf message bodies, one bare message per row — into a
 * typed Arrow batch, natively (via ptars). This is the protobuf counterpart of
 * {@link NativeColumnarJsonDecodeOperator}, matching Flink's {@code protobuf} format: each body is the
 * whole serialized message (no Confluent framing), decoded against a descriptor the JVM serialized off
 * the generated message class into a {@code FileDescriptorSet}.
 *
 * <p>Unlike the JSON operator, the output schema is *derived from the descriptor* by the native decoder,
 * not handed in as a {@code RowType} — so this operator carries no protobuf-java dependency itself; it
 * only forwards the descriptor bytes + message name. Stateless: on recovery the decoder is rebuilt from
 * those two fields.
 */
public class NativeProtobufDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final byte[] descriptorSet;
  private final String messageName;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;

  /**
   * @param descriptorSet an encoded protobuf {@code FileDescriptorSet} (the message's {@code .proto}
   *     file + its transitive dependencies)
   * @param messageName the fully-qualified message type to decode each body as
   */
  public NativeProtobufDecodeOperator(byte[] descriptorSet, String messageName) {
    this.descriptorSet = descriptorSet;
    this.messageName = messageName;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    handle = Native.createProtobufDecoder(descriptorSet, messageName, 0L, 0L);
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
      Native.decodeInto(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeDecoder(handle);
      handle = 0;
    }
    super.close();
  }
}
