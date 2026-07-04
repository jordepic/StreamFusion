use crate::*;

/// Encodes a single Arrow batch to a Parquet file. The core of the native columnar sink: the batch
/// is written in its columnar form directly, skipping the host's row-to-Parquet encoding.
pub(crate) fn write_parquet(batch: &RecordBatch, path: &str) {
    let file = std::fs::File::create(path).expect("failed to create parquet file");
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
        .expect("failed to create parquet writer");
    writer.write(batch).expect("failed to write batch");
    writer.close().expect("failed to close parquet writer");
}

/// Writes an Arrow batch the JVM exported to a Parquet file at `path`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_writeParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    path: JString<'local>,
) {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let path: String = env.get_string(&path).expect("failed to read path").into();
    write_parquet(&batch, &path);
}

/// A Parquet sink that holds one open `ArrowWriter` across many batches, so the output file count is
/// decoupled from the input batch size: the JVM rolls a file by closing this handle when its row
/// target is reached (and on checkpoint), not once per batch. The writer is created lazily from the
/// first batch's schema; an empty file (closed before any batch) writes a valid header-only Parquet.
pub(crate) struct ParquetSink {
    path: String,
    writer: Option<parquet::arrow::ArrowWriter<std::fs::File>>,
}

impl ParquetSink {
    fn new(path: String) -> ParquetSink {
        ParquetSink { path, writer: None }
    }

    fn write(&mut self, batch: RecordBatch) {
        if self.writer.is_none() {
            let file = std::fs::File::create(&self.path).expect("failed to create parquet file");
            self.writer = Some(
                parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                    .expect("failed to create parquet writer"),
            );
        }
        self.writer.as_mut().unwrap().write(&batch).expect("failed to write batch");
    }

    fn close(self) {
        if let Some(writer) = self.writer {
            writer.close().expect("failed to close parquet writer");
        }
    }
}

/// Opens a Parquet file for writing and returns an opaque handle. Batches are appended with
/// `parquetWriterWrite`; the file is finalized (and the handle released) by `closeParquetWriter`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createParquetWriter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    let path: String = env.get_string(&path).expect("failed to read path").into();
    into_handle(ParquetSink::new(path))
}

/// Appends an Arrow batch the JVM exported to the open file behind `handle`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_parquetWriterWrite<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let sink = unsafe { &mut *(handle as *mut ParquetSink) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    sink.write(batch);
}

/// Finalizes the Parquet file (writes its footer) and releases the writer handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeParquetWriter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    let sink = unsafe { from_handle::<ParquetSink>(handle) };
    sink.close();
}

/// Reorders/selects a batch's columns to match the requested projection (by name); identity when no
/// projection was requested. Shared by the file sources so projection pushdown behaves identically.
/// A native source the JVM pulls one Arrow batch at a time, until the split is exhausted. The concrete
/// reader is hidden behind this trait so the file formats share one open/next/close bridge — the bytes
/// are read and decoded directly in the engine, never crossing into the row world.
pub(crate) trait BatchSource {
    fn next_batch(&mut self) -> Option<RecordBatch>;
}

/// A scan of one file split, driven by DataFusion's file-scan execution — the same path
/// datafusion-comet uses. DataFusion selects the row groups (Parquet) / stripes (ORC) whose start
/// falls in the split's byte range, pushes the projection into the decode (only the wanted columns are
/// read), and yields Arrow batches, which we pull synchronously on the shared runtime. Parquet and ORC
/// differ only in the `FileSource` constructed.
pub(crate) struct FileScan {
    stream: datafusion::physical_plan::SendableRecordBatchStream,
}

impl FileScan {
    fn open(
        file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource>,
        path: &str,
        schema: SchemaRef,
        projection: &[String],
        range_start: i64,
        range_length: i64,
    ) -> FileScan {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::FileScanConfigBuilder;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_plan::ExecutionPlan;

        let size = std::fs::metadata(path).expect("failed to stat source file").len();
        let file =
            PartitionedFile::new_with_range(path.to_string(), size, range_start, range_start + range_length);
        // Projection as column indices in plan order; an empty projection reads every column.
        let indices: Vec<usize> = if projection.is_empty() {
            (0..schema.fields().len()).collect()
        } else {
            projection
                .iter()
                .map(|name| schema.index_of(name).expect("projected column not in source file"))
                .collect()
        };
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file(file)
            .with_projection_indices(Some(indices))
            .expect("failed to set scan projection")
            .build();
        let stream = DataSourceExec::from_data_source(config)
            .execute(0, SessionContext::new().task_ctx())
            .expect("failed to start file scan");
        FileScan { stream }
    }
}

impl BatchSource for FileScan {
    fn next_batch(&mut self) -> Option<RecordBatch> {
        runtime()
            .block_on(async { self.stream.next().await })
            .map(|batch| batch.expect("failed to read file batch"))
    }
}

/// The Arrow schema of a Parquet file, read from its footer to build the scan's file source.
pub(crate) fn parquet_file_schema(path: &str) -> SchemaRef {
    let file = std::fs::File::open(path).expect("failed to open parquet file");
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to read parquet metadata")
        .schema()
        .clone()
}

/// The Arrow schema of an ORC file, read from its footer.
pub(crate) fn orc_file_schema(path: &str) -> SchemaRef {
    let file = std::fs::File::open(path).expect("failed to open orc file");
    orc_rust::ArrowReaderBuilder::try_new(file)
        .expect("failed to read orc metadata")
        .schema()
}

/// Opens one Parquet split — the row groups of `path` within `[range_start, range_start +
/// range_length)` — and returns an opaque handle, released with `closeSource`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    projection: JObjectArray<'local>,
    range_start: jlong,
    range_length: jlong,
) -> jlong {
    let path: String = env.get_string(&path).expect("failed to read path").into();
    let projection = read_strings(&mut env, &projection)
        .into_iter()
        .map(|name| name.expect("projection column name was null"))
        .collect::<Vec<_>>();
    let schema = parquet_file_schema(&path);
    let file_source = Arc::new(
        datafusion::datasource::physical_plan::ParquetSource::new(schema.clone()),
    ) as Arc<dyn datafusion::datasource::physical_plan::FileSource>;
    let source: Box<dyn BatchSource> =
        Box::new(FileScan::open(file_source, &path, schema, &projection, range_start, range_length));
    into_handle(source)
}

/// Exports the next Arrow batch from the source into the consumer-allocated C structs, returning
/// true if a batch was produced and false once the directory is exhausted. Shared by every native
/// file source.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_nextBatch<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jboolean {
    let source = unsafe { &mut *(handle as *mut Box<dyn BatchSource>) };
    match source.next_batch() {
        Some(batch) => {
            export_record_batch(batch, out_array_address, out_schema_address);
            1
        }
        None => 0,
    }
}

/// Releases a native file source handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSource<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<Box<dyn BatchSource>>(handle));
    }
}

/// Opens one ORC split — the stripes of `path` within `[range_start, range_start + range_length)` —
/// and returns an opaque handle, released with `closeSource`. Driven by datafusion-orc's file source,
/// which maps the split's byte range to the stripes it covers.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openOrc<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    projection: JObjectArray<'local>,
    range_start: jlong,
    range_length: jlong,
) -> jlong {
    let path: String = env.get_string(&path).expect("failed to read path").into();
    let projection = read_strings(&mut env, &projection)
        .into_iter()
        .map(|name| name.expect("projection column name was null"))
        .collect::<Vec<_>>();
    let schema = orc_file_schema(&path);
    let table_schema = datafusion::datasource::table_schema::TableSchema::from(schema.clone());
    let file_source = Arc::new(datafusion_orc::OrcSource::new(table_schema))
        as Arc<dyn datafusion::datasource::physical_plan::FileSource>;
    let source: Box<dyn BatchSource> =
        Box::new(FileScan::open(file_source, &path, schema, &projection, range_start, range_length));
    into_handle(source)
}
