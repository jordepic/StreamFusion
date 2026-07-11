package io.github.jordepic.streamfusion.format;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.flink.table.types.logical.RowType;

/**
 * Task-local bridge to one native format library. The generic batching operator owns Arrow C Data
 * Interface export/import and calls this implementation only for format-specific work.
 */
public interface NativeMessageDecoder extends AutoCloseable {

  void open(BufferAllocator allocator, RowType outputType) throws Exception;

  default void beforeDecode(VarBinaryVector bodies, int count) throws Exception {}

  void decodeInto(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress)
      throws Exception;

  @Override
  void close() throws Exception;
}
