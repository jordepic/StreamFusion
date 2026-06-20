package io.github.jordepic.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * The columnar stream record passed between native operators: one Arrow batch. Carrying batches
 * instead of {@link org.apache.flink.table.data.RowData} lets a chain of native operators stay
 * columnar, with the row↔columnar transpose pushed to the boundary with the host engine.
 *
 * <p>A batch is produced fresh by one operator and handed to the next; the consumer owns it and
 * closes it once read. Within a chained task this hand-off is in-memory (no serialization); only a
 * network edge serializes it (Arrow IPC, via the batch's type serializer).
 */
public final class ArrowBatch {

  private final VectorSchemaRoot root;
  // The destination channel for a key-partitioned batch (the columnar shuffle); -1 when unrouted.
  private final int destination;

  public ArrowBatch(VectorSchemaRoot root) {
    this(root, -1);
  }

  public ArrowBatch(VectorSchemaRoot root, int destination) {
    this.root = root;
    this.destination = destination;
  }

  public VectorSchemaRoot root() {
    return root;
  }

  public int destination() {
    return destination;
  }

  public int rowCount() {
    return root.getRowCount();
  }
}
