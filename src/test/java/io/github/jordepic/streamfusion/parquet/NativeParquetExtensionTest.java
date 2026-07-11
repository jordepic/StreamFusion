package io.github.jordepic.streamfusion.parquet;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NativeParquetExtensionTest {

  @Test
  void loadsTheParquetJniFacadeFromTheDevelopmentLibrary() {
    assertTrue(NativeParquet.isLoaded());
  }
}
