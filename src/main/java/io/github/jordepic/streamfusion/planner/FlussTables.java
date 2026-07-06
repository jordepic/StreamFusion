package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.fluss.FlussConfigTranslator;
import io.github.jordepic.streamfusion.fluss.NativeFlussSource;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.StartupOptions;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

/** Planner support for replacing Fluss log-table scans with the native fluss-rs reader. */
final class FlussTables {

  private FlussTables() {}

  /** Null when the native Fluss source can faithfully run this scan, else the fallback reason. */
  static String fallbackReason(StreamPhysicalTableSourceScan scan) {
    if (!Native.flussFeatureBuilt()) {
      return "the native library was built without the fluss feature";
    }
    return plan(scan).fallbackReason;
  }

  static NativeFlussSource build(StreamPhysicalTableSourceScan scan) {
    Planned planned = plan(scan);
    if (planned.plan == null) {
      throw new IllegalArgumentException(
          "Fluss scan is not supported by the native Fluss source: " + planned.fallbackReason);
    }
    SourcePlan plan = planned.plan;
    return new NativeFlussSource(
        plan.flussConfig,
        plan.tablePath,
        plan.hasPrimaryKey,
        plan.partitioned,
        plan.offsetsInitializer,
        plan.scanPartitionDiscoveryIntervalMs,
        plan.streaming,
        plan.partitionFilters,
        plan.lakeSource,
        plan.leaseContext,
        plan.nativeConfigKeys,
        plan.nativeConfigValues,
        plan.projectedFields);
  }

  private static Planned plan(StreamPhysicalTableSourceScan scan) {
    if (scan.requireWatermark()) {
      return Planned.fallback(
          "the table's WATERMARK is pushed into the scan, which the native source does not"
              + " regenerate");
    }
    if (!FilesystemTables.allPhysicalColumns(scan)) {
      return Planned.fallback("metadata/computed columns are not produced natively");
    }
    DynamicTableSource source = tableSource(scan);
    if (!(source instanceof FlinkTableSource)) {
      return Planned.fallback("the scan's table source is not the Fluss FlinkTableSource");
    }
    String columnReason = unsupportedColumnReason(scan);
    if (columnReason != null) {
      return Planned.fallback(columnReason);
    }
    Map<String, String> options = FilesystemTables.options(scan);
    if (options == null) {
      return Planned.fallback("the table's connector options could not be resolved");
    }
    // fluss-rs's RecordBatchLogScanner reads only the ARROW log format (its scan validation errors
    // on any other); the option key/default mirror Fluss' ConfigOptions.TABLE_LOG_FORMAT.
    String logFormat = options.getOrDefault("table.log.format", "ARROW");
    if (!"ARROW".equalsIgnoreCase(logFormat)) {
      return Planned.fallback(
          "table.log.format=" + logFormat + " — fluss-rs scans only the ARROW log format");
    }
    FlinkTableSource flussSource = (FlinkTableSource) source;
    try {
      TablePath tablePath = field(flussSource, "tablePath", TablePath.class);
      org.apache.fluss.config.Configuration flussConfig =
          field(flussSource, "flussConfig", org.apache.fluss.config.Configuration.class);
      int[] primaryKeyIndexes = field(flussSource, "primaryKeyIndexes", int[].class);
      int[] partitionKeyIndexes = field(flussSource, "partitionKeyIndexes", int[].class);
      boolean streaming = field(flussSource, "streaming", Boolean.class);
      StartupOptions startupOptions = field(flussSource, "startupOptions", StartupOptions.class);
      long scanPartitionDiscoveryIntervalMs =
          field(flussSource, "scanPartitionDiscoveryIntervalMs", Long.class);
      boolean isDataLakeEnabled = field(flussSource, "isDataLakeEnabled", Boolean.class);
      LeaseContext leaseContext = field(flussSource, "leaseContext", LeaseContext.class);
      int[] projectedFields = field(flussSource, "projectedFields", int[].class);
      Object singleRowFilter = fieldObject(flussSource, "singleRowFilter");
      Object modificationScanType = fieldObject(flussSource, "modificationScanType");
      boolean selectRowCount = field(flussSource, "selectRowCount", Boolean.class);
      long limit = field(flussSource, "limit", Long.class);
      Predicate partitionFilters = field(flussSource, "partitionFilters", Predicate.class);
      @SuppressWarnings("unchecked")
      LakeSource<LakeSplit> lakeSource =
          (LakeSource<LakeSplit>) fieldObject(flussSource, "lakeSource");

      if (primaryKeyIndexes.length > 0) {
        // fluss-rs RecordBatchLogScanner is log-table append-only only.
        return Planned.fallback("primary-key tables read a changelog the native log scanner"
            + " does not carry");
      }
      if (isDataLakeEnabled || lakeSource != null) {
        return Planned.fallback("datalake-enabled tables read through the lake source");
      }
      if (singleRowFilter != null) {
        return Planned.fallback("a pushed-down single-row filter is not supported natively");
      }
      if (modificationScanType != null) {
        return Planned.fallback("a modification scan type is not supported natively");
      }
      if (selectRowCount) {
        return Planned.fallback("a row-count scan is not supported natively");
      }
      if (limit > 0) {
        return Planned.fallback("a pushed-down LIMIT is not supported natively");
      }
      if (partitionFilters != null) {
        return Planned.fallback("pushed-down partition filters are not supported natively");
      }
      if (projectedFields != null && projectedFields.length == 0) {
        return Planned.fallback("an empty projection is not supported natively");
      }

      FlussConfigTranslator.Result translated = FlussConfigTranslator.translate(flussConfig.toMap());
      if (!translated.isTranslated()) {
        return Planned.fallback(
            translated.fallbackReason().orElse("client config is not translatable to fluss-rs"));
      }
      String[] keys = translated.config().keySet().toArray(new String[0]);
      String[] values = new String[keys.length];
      for (int i = 0; i < keys.length; i++) {
        values[i] = translated.config().get(keys[i]);
      }

      return Planned.of(
          new SourcePlan(
              flussConfig,
              tablePath,
              false,
              partitionKeyIndexes.length > 0,
              offsetsInitializer(startupOptions),
              scanPartitionDiscoveryIntervalMs,
              streaming,
              null,
              null,
              leaseContext == null ? LeaseContext.DEFAULT : leaseContext,
              keys,
              values,
              projectedFields == null ? new int[0] : projectedFields));
    } catch (ReflectiveOperationException | RuntimeException e) {
      return Planned.fallback("Fluss table source introspection failed: " + e);
    }
  }

  /**
   * The first column whose type is outside the verified fluss-rs ↔ vendored-Arrow surface (as a
   * fallback reason), or null when every physical column is safe.
   *
   * <p>The whitelist is the intersection of fluss-rs' {@code to_arrow_type} export and what the
   * vendored {@code ArrowConversion} readers accept: the fixed-width scalars, CHAR/VARCHAR (Utf8
   * both sides), DECIMAL (Decimal128 both sides), DATE (Date32), TIME (the same per-precision
   * Time32/Time64 unit on both sides), and VARBINARY (Binary). TIMESTAMP and TIMESTAMP_LTZ are
   * excluded: fluss-rs exports a per-precision unit — with a UTC zone for TIMESTAMP_LTZ — while
   * {@code ArrowConversion} pins {@code Timestamp(NANOSECOND, null)} and rejects any zoned
   * timestamp vector. BINARY is excluded because {@code ArrowConversion.toArrowSchema} has no
   * BINARY mapping, and nested types (ARRAY/MAP/ROW) are unverified across the boundary.
   */
  private static String unsupportedColumnReason(StreamPhysicalTableSourceScan scan) {
    RowType rowType = FilesystemTables.physicalRowType(scan);
    if (rowType == null) {
      return "the table's physical row type could not be resolved";
    }
    for (LogicalType type : rowType.getChildren()) {
      switch (type.getTypeRoot()) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case CHAR:
        case VARCHAR:
        case DECIMAL:
        case DATE:
        case TIME_WITHOUT_TIME_ZONE:
        case VARBINARY:
          break;
        default:
          return "column type "
              + type.asSummaryString()
              + " is not verified across the fluss-rs Arrow boundary";
      }
    }
    return null;
  }

  private static DynamicTableSource tableSource(StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      return table == null ? null : table.tableSource();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static OffsetsInitializer offsetsInitializer(StartupOptions options) {
    if (options == null || options.startupMode == null) {
      return OffsetsInitializer.full();
    }
    ScanStartupMode mode = options.startupMode;
    if (mode == ScanStartupMode.EARLIEST) {
      return OffsetsInitializer.earliest();
    }
    if (mode == ScanStartupMode.LATEST) {
      return OffsetsInitializer.latest();
    }
    if (mode == ScanStartupMode.TIMESTAMP) {
      return OffsetsInitializer.timestamp(options.startupTimestampMs);
    }
    return OffsetsInitializer.full();
  }

  private static Object fieldObject(Object target, String name) throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  @SuppressWarnings("unchecked")
  private static <T> T field(Object target, String name, Class<T> type)
      throws ReflectiveOperationException {
    Object value = fieldObject(target, name);
    return (T) value;
  }

  private static final class Planned {
    private final SourcePlan plan;
    private final String fallbackReason;

    private Planned(SourcePlan plan, String fallbackReason) {
      this.plan = plan;
      this.fallbackReason = fallbackReason;
    }

    static Planned of(SourcePlan plan) {
      return new Planned(plan, null);
    }

    static Planned fallback(String reason) {
      return new Planned(null, reason);
    }
  }

  private static final class SourcePlan {
    private final org.apache.fluss.config.Configuration flussConfig;
    private final TablePath tablePath;
    private final boolean hasPrimaryKey;
    private final boolean partitioned;
    private final OffsetsInitializer offsetsInitializer;
    private final long scanPartitionDiscoveryIntervalMs;
    private final boolean streaming;
    private final Predicate partitionFilters;
    private final LakeSource<LakeSplit> lakeSource;
    private final LeaseContext leaseContext;
    private final String[] nativeConfigKeys;
    private final String[] nativeConfigValues;
    private final int[] projectedFields;

    private SourcePlan(
        org.apache.fluss.config.Configuration flussConfig,
        TablePath tablePath,
        boolean hasPrimaryKey,
        boolean partitioned,
        OffsetsInitializer offsetsInitializer,
        long scanPartitionDiscoveryIntervalMs,
        boolean streaming,
        Predicate partitionFilters,
        LakeSource<LakeSplit> lakeSource,
        LeaseContext leaseContext,
        String[] nativeConfigKeys,
        String[] nativeConfigValues,
        int[] projectedFields) {
      this.flussConfig = flussConfig;
      this.tablePath = tablePath;
      this.hasPrimaryKey = hasPrimaryKey;
      this.partitioned = partitioned;
      this.offsetsInitializer = offsetsInitializer;
      this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
      this.streaming = streaming;
      this.partitionFilters = partitionFilters;
      this.lakeSource = lakeSource;
      this.leaseContext = leaseContext;
      this.nativeConfigKeys = nativeConfigKeys;
      this.nativeConfigValues = nativeConfigValues;
      this.projectedFields = projectedFields;
    }
  }
}
