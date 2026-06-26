package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class RowDataArrowConverterTest {

  private static final int COLUMNS = 11;

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType(),
            new BooleanType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new TimestampType(9),
            new DateType(),
            new DecimalType(10, 2)
          },
          new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"});

  @Test
  void roundTripsEveryColumnTypeAndNulls() {
    GenericRowData first = new GenericRowData(COLUMNS);
    first.setField(0, (byte) 1);
    first.setField(1, (short) 2);
    first.setField(2, 3);
    first.setField(3, 4L);
    first.setField(4, 5.5f);
    first.setField(5, 6.5);
    first.setField(6, true);
    first.setField(7, StringData.fromString("hello"));
    // Sub-millisecond nanos verify the nanosecond round-trip preserves full precision.
    first.setField(8, TimestampData.fromEpochMillis(1234L, 567000));
    first.setField(9, 18000); // DATE as an epoch-day count
    first.setField(10, DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2));

    // A row that is null in every column exercises the null path on every vector type.
    GenericRowData nulls = new GenericRowData(COLUMNS);
    for (int c = 0; c < COLUMNS; c++) {
      nulls.setField(c, null);
    }

    List<RowData> rows = List.of(first, nulls);
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, SCHEMA, allocator)) {
      assertEquals(2, root.getRowCount());
      List<RowData> back = RowDataArrowConverter.read(root, SCHEMA);
      assertEquals(2, back.size());
      RowData backFirst = back.get(0);
      RowData backNulls = back.get(1);
      for (int c = 0; c < COLUMNS; c++) {
        RowData.FieldGetter getter = RowData.createFieldGetter(SCHEMA.getTypeAt(c), c);
        assertEquals(first.getField(c), getter.getFieldOrNull(backFirst), "column " + c);
        assertEquals(null, getter.getFieldOrNull(backNulls), "null column " + c);
      }
    }
  }

  @Test
  void carriesEveryRowKindWhenRequested() {
    RowType schema = RowType.of(new LogicalType[] {new IntType()}, new String[] {"a"});
    RowKind[] kinds = RowKind.values(); // +I, -U, +U, -D — Flink's full four-way changelog
    List<RowData> rows = new java.util.ArrayList<>();
    for (int i = 0; i < kinds.length; i++) {
      GenericRowData row = new GenericRowData(1);
      row.setRowKind(kinds[i]);
      row.setField(0, i);
      rows.add(row);
    }

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, schema, allocator, true)) {
      List<RowData> back = RowDataArrowConverter.read(root, schema);
      for (int i = 0; i < kinds.length; i++) {
        assertEquals(kinds[i], back.get(i).getRowKind(), "kind " + i);
        assertEquals(i, back.get(i).getInt(0), "value " + i);
      }
    }
  }

  @Test
  void omitsRowKindColumnByDefault() {
    RowType schema = RowType.of(new LogicalType[] {new IntType()}, new String[] {"a"});
    GenericRowData row = new GenericRowData(1);
    row.setRowKind(RowKind.DELETE);
    row.setField(0, 7);

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row), schema, allocator)) {
      // No hidden column when not requested, and the read defaults to an insert.
      assertEquals(1, root.getSchema().getFields().size());
      assertEquals(RowKind.INSERT, RowDataArrowConverter.read(root, schema).get(0).getRowKind());
    }
  }

  @Test
  void reportsUnsupportedSchemas() {
    assertTrue(RowDataArrowConverter.supports(SCHEMA));
    RowType withTime =
        RowType.of(new LogicalType[] {new IntType(), new TimeType(0)}, new String[] {"a", "t"});
    assertFalse(RowDataArrowConverter.supports(withTime));
  }
}
