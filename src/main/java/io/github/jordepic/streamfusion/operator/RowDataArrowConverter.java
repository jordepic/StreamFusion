package io.github.jordepic.streamfusion.operator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * Converts whole {@link RowData} rows of a fixed schema to and from an Arrow {@link
 * VectorSchemaRoot}, one Arrow vector per column. Unlike the window operators, which extract only
 * the few columns they aggregate, this carries an entire row across the boundary — the foundation
 * for stateless operators (filter, projection) and, later, keeping columnar chains native.
 *
 * <p>Only the column types below are supported; {@link #supports} reports whether a schema is
 * convertible so the planner can fall back otherwise. Nulls are preserved per cell.
 */
public final class RowDataArrowConverter {

  private RowDataArrowConverter() {}

  /** Whether every column of the schema has a type this converter handles. */
  public static boolean supports(RowType rowType) {
    for (LogicalType type : rowType.getChildren()) {
      switch (type.getTypeRoot()) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case CHAR:
        case VARCHAR:
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DATE:
        case DECIMAL:
          break;
        default:
          return false;
      }
    }
    return true;
  }

  /** A timestamp column's declared precision, for {@link RowData#getTimestamp}. */
  private static int timestampPrecision(LogicalType type) {
    return type instanceof TimestampType
        ? ((TimestampType) type).getPrecision()
        : ((LocalZonedTimestampType) type).getPrecision();
  }

  /**
   * Builds an Arrow batch holding {@code rows} under {@code rowType}. The caller closes the root.
   *
   * <p>Fills the vectors row-major (one pass over the rows, writing every column per row) into
   * vectors pre-sized to the row count, the way Comet's {@code ArrowWriter} does — far cheaper than
   * a column-major pass per field, which re-walks the row list once per column and lets {@code
   * setSafe} realloc as each vector grows.
   */
  public static VectorSchemaRoot write(
      List<RowData> rows, RowType rowType, BufferAllocator allocator) {
    int rowCount = rows.size();
    int fieldCount = rowType.getFieldCount();
    List<String> names = rowType.getFieldNames();
    FieldVector[] vectors = new FieldVector[fieldCount];
    LogicalType[] types = new LogicalType[fieldCount];
    for (int column = 0; column < fieldCount; column++) {
      LogicalType type = rowType.getTypeAt(column);
      types[column] = type;
      FieldVector vector = createVector(type, names.get(column), allocator);
      vector.setInitialCapacity(rowCount);
      vector.allocateNew();
      vectors[column] = vector;
    }
    for (int i = 0; i < rowCount; i++) {
      RowData row = rows.get(i);
      for (int column = 0; column < fieldCount; column++) {
        writeCell(vectors[column], types[column], row, column, i);
      }
    }
    List<FieldVector> columns = new ArrayList<>(fieldCount);
    for (FieldVector vector : vectors) {
      vector.setValueCount(rowCount);
      columns.add(vector);
    }
    VectorSchemaRoot root = new VectorSchemaRoot(columns);
    root.setRowCount(rowCount);
    return root;
  }

  /** Reads the rows of an Arrow batch back into {@link RowData}s under {@code rowType}. */
  public static List<RowData> read(VectorSchemaRoot root, RowType rowType) {
    int rowCount = root.getRowCount();
    List<GenericRowData> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      rows.add(new GenericRowData(rowType.getFieldCount()));
    }
    for (int column = 0; column < rowType.getFieldCount(); column++) {
      readColumn(root.getVector(column), rowType.getTypeAt(column), rows, column);
    }
    return new ArrayList<>(rows);
  }

  /** Creates the Arrow vector for a column's logical type (unallocated; the caller pre-sizes it). */
  private static FieldVector createVector(
      LogicalType type, String name, BufferAllocator allocator) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return new TinyIntVector(name, allocator);
      case SMALLINT:
        return new SmallIntVector(name, allocator);
      case INTEGER:
        return new IntVector(name, allocator);
      case BIGINT:
        return new BigIntVector(name, allocator);
      case FLOAT:
        return new Float4Vector(name, allocator);
      case DOUBLE:
        return new Float8Vector(name, allocator);
      case BOOLEAN:
        return new BitVector(name, allocator);
      case CHAR:
      case VARCHAR:
        return new VarCharVector(name, allocator);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return new TimeStampNanoVector(name, allocator);
      case DATE:
        // Flink represents DATE as an int day count, as does Arrow's day-precision date vector.
        return new DateDayVector(name, allocator);
      case DECIMAL:
        return new DecimalVector(
            name, allocator, ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
      default:
        throw new IllegalArgumentException("unsupported column type: " + type);
    }
  }

  /** Writes row {@code i}'s value (or null) for one column into its pre-created vector. */
  private static void writeCell(
      FieldVector vector, LogicalType type, RowData row, int column, int i) {
    boolean isNull = row.isNullAt(column);
    switch (type.getTypeRoot()) {
      case TINYINT: {
        TinyIntVector v = (TinyIntVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getByte(column));
        break;
      }
      case SMALLINT: {
        SmallIntVector v = (SmallIntVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getShort(column));
        break;
      }
      case INTEGER: {
        IntVector v = (IntVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getInt(column));
        break;
      }
      case BIGINT: {
        BigIntVector v = (BigIntVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getLong(column));
        break;
      }
      case FLOAT: {
        Float4Vector v = (Float4Vector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getFloat(column));
        break;
      }
      case DOUBLE: {
        Float8Vector v = (Float8Vector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getDouble(column));
        break;
      }
      case BOOLEAN: {
        BitVector v = (BitVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getBoolean(column) ? 1 : 0);
        break;
      }
      case CHAR:
      case VARCHAR: {
        VarCharVector v = (VarCharVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getString(column).toBytes());
        break;
      }
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
        TimeStampNanoVector v = (TimeStampNanoVector) vector;
        if (isNull) {
          v.setNull(i);
        } else {
          TimestampData ts = row.getTimestamp(column, timestampPrecision(type));
          v.setSafe(i, ts.getMillisecond() * 1_000_000L + ts.getNanoOfMillisecond());
        }
        break;
      }
      case DATE: {
        DateDayVector v = (DateDayVector) vector;
        if (isNull) v.setNull(i);
        else v.setSafe(i, row.getInt(column));
        break;
      }
      case DECIMAL: {
        DecimalVector v = (DecimalVector) vector;
        if (isNull) {
          v.setNull(i);
        } else {
          int precision = ((DecimalType) type).getPrecision();
          int scale = ((DecimalType) type).getScale();
          v.setSafe(i, row.getDecimal(column, precision, scale).toBigDecimal());
        }
        break;
      }
      default:
        throw new IllegalArgumentException("unsupported column type: " + type);
    }
  }

  private static void readColumn(
      FieldVector vector, LogicalType type, List<GenericRowData> rows, int column) {
    int n = rows.size();
    for (int i = 0; i < n; i++) {
      if (vector.isNull(i)) {
        rows.get(i).setField(column, null);
        continue;
      }
      Object value;
      switch (type.getTypeRoot()) {
        case TINYINT:
          value = ((TinyIntVector) vector).get(i);
          break;
        case SMALLINT:
          value = ((SmallIntVector) vector).get(i);
          break;
        case INTEGER:
          value = ((IntVector) vector).get(i);
          break;
        case BIGINT:
          value = ((BigIntVector) vector).get(i);
          break;
        case FLOAT:
          value = ((Float4Vector) vector).get(i);
          break;
        case DOUBLE:
          value = ((Float8Vector) vector).get(i);
          break;
        case BOOLEAN:
          value = ((BitVector) vector).get(i) != 0;
          break;
        case CHAR:
        case VARCHAR:
          value = StringData.fromBytes(((VarCharVector) vector).get(i));
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
          long nanos = ((TimeStampNanoVector) vector).get(i);
          value = TimestampData.fromEpochMillis(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
          break;
        }
        case DATE:
          value = ((DateDayVector) vector).get(i);
          break;
        case DECIMAL: {
          BigDecimal decimal = ((DecimalVector) vector).getObject(i);
          value =
              DecimalData.fromBigDecimal(
                  decimal, ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
          break;
        }
        default:
          throw new IllegalArgumentException("unsupported column type: " + type);
      }
      rows.get(i).setField(column, value);
    }
  }
}
