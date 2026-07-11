package io.github.jordepic.streamfusion.format.avroconfluent;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoder;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.avro.NativeAvroFormat;
import io.github.jordepic.streamfusion.kafka.ConfluentSchemaRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.avro.Schema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

/** Native provider for Flink's {@code avro-confluent} format. */
public final class AvroConfluentFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "avro-confluent";
  }

  @Override
  public boolean honorsProjection() {
    return true;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    return !context.ignoreParseErrors() && ConfluentSchemaRegistry.fromOptions(context.options()) != null;
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromOptions(context.options());
    String readerSchema = AvroSchemaConverter.convertToSchema(context.outputType().copy(false)).toString();
    return () -> new Decoder(registry, readerSchema);
  }

  private static final class Decoder implements NativeMessageDecoder {
    private final ConfluentSchemaRegistry registry;
    private final String readerSchemaText;
    private transient long handle;
    private transient Set<Integer> registeredSchemaIds;
    private transient Schema readerSchema;

    private Decoder(ConfluentSchemaRegistry registry, String readerSchemaText) {
      this.registry = registry;
      this.readerSchemaText = readerSchemaText;
    }

    @Override
    public void open(BufferAllocator allocator, RowType outputType) {
      handle = NativeAvroFormat.createDecoder(true, "", readerSchemaText);
      registeredSchemaIds = new HashSet<>();
      readerSchema = new Schema.Parser().parse(readerSchemaText);
    }

    @Override
    public void beforeDecode(VarBinaryVector bodies, int count) {
      for (int i = 0; i < count; i++) {
        byte[] message = bodies.get(i);
        if (message == null || message.length < 5 || message[0] != 0) {
          continue;
        }
        int id =
            ((message[1] & 0xff) << 24)
                | ((message[2] & 0xff) << 16)
                | ((message[3] & 0xff) << 8)
                | (message[4] & 0xff);
        if (registeredSchemaIds.add(id)) {
          try {
            Schema writer = registry.fetchWriterSchema(id);
            NativeAvroFormat.registerWriterSchema(
                handle, id, ConfluentSchemaRegistry.aliasedToReader(writer, readerSchema).toString());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeAvroFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeAvroFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
