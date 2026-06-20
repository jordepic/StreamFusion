package io.github.jordepic.streamfusion.planner;

/**
 * Marks a physical rel as a native columnar operator — one that both consumes and produces {@link
 * io.github.jordepic.streamfusion.operator.ArrowBatch}es rather than rows. The transition pass uses
 * this to place a row↔columnar transpose wherever a columnar rel meets a rowwise one, so adjacent
 * columnar operators flow batches with no conversion between them.
 */
public interface ColumnarRel {}
