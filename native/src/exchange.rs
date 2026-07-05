use crate::*;

/// The partition a key's arrow-row bytes map to, by a consistent hash of the encoded key values.
/// The hash is internal — it only distributes rows across partitions and need not match Flink's,
/// because the downstream keyed consumer is our own native operator that re-groups internally (see
/// todo 10). A fixed-seed hasher keeps the mapping identical across subtasks/processes, and the
/// arrow-row encoding is a pure function of the key's type and value, so the two sides of a join
/// co-locate equal keys exactly as the scalar hash did (divergences/10).
pub(crate) fn partition_for_key(key: &[u8], num_partitions: usize) -> usize {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(key);
    (hasher.finish() % num_partitions as u64) as usize
}

/// Splits a batch into up to `num_partitions` sub-batches by a consistent hash of the given key
/// columns, so every row with the same key lands in the same partition. Each sub-batch keeps the
/// full input schema (a row subset); empty partitions are omitted. This is the by-key split the
/// columnar shuffle routes to channels. The keys encode to arrow-row bytes in one vectorized pass;
/// no per-row scalar materialization.
pub(crate) fn partition_batch(
    batch: &RecordBatch,
    key_columns: &[usize],
    num_partitions: usize,
) -> Vec<(usize, RecordBatch)> {
    let key_arrays: Vec<&ArrayRef> = key_columns.iter().map(|&i| batch.column(i)).collect();
    let mut converter = None;
    let keys_encoded = encode_keys(&mut converter, &key_arrays, batch.num_rows());
    let mut rows_by_partition: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for row in 0..batch.num_rows() {
        let partition = partition_for_key(keys_encoded.row(row).data(), num_partitions);
        rows_by_partition[partition].push(row as u32);
    }
    let mut out = Vec::new();
    for (partition, rows) in rows_by_partition.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let indices = UInt32Array::from(rows);
        let columns: Vec<ArrayRef> =
            batch.columns().iter().map(|c| take(c, &indices, None).expect("take")).collect();
        out.push((partition, RecordBatch::try_new(batch.schema(), columns).expect("sub batch")));
    }
    out
}

/// Holds the per-partition sub-batches of one split, pulled out one at a time by the JVM.
pub(crate) struct SplitState {
    partitions: Vec<(usize, RecordBatch)>,
    cursor: usize,
}

/// Splits a batch from the JVM by key into per-partition sub-batches and returns a handle to pull
/// them with `nextSplit`; released with `closeSplit`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_splitByKey<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    num_partitions: jint,
) -> jlong {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let keys: Vec<usize> = read_int_array(&env, &key_columns).into_iter().map(|k| k as usize).collect();
    let partitions = partition_batch(&batch, &keys, num_partitions as usize);
    into_handle(SplitState { partitions, cursor: 0 })
}

/// Exports the next sub-batch into the consumer-allocated C structs and returns its partition, or
/// -1 once the split is exhausted.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_nextSplit<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    let state = unsafe { &mut *(handle as *mut SplitState) };
    if state.cursor >= state.partitions.len() {
        return -1;
    }
    let (partition, batch) = state.partitions[state.cursor].clone();
    state.cursor += 1;
    export_record_batch(batch, out_array_address, out_schema_address);
    partition as jint
}

/// Releases a split handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSplit<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<SplitState>(handle));
    }
}
