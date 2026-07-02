package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NativeBridgeTest {

  @Test
  void nativeLibraryReportsVersion() {
    String version = Native.version();
    assertNotNull(version);
    assertTrue(version.matches("\\d+\\.\\d+\\.\\d+"), "unexpected version: " + version);
  }

  @Test
  void nativeRuntimeDrivesAsyncWorkToCompletion() {
    assertEquals(42, Native.blockingAnswer());
  }

  /** The live-handle sentinel the harness leak check polls must see creates and drain on close. */
  @Test
  void liveHandleBreakdownTracksCreateAndClose() {
    assertEquals("", Native.liveNativeHandles());
    long sorter = Native.createTemporalSorter(0, -1);
    String breakdown = Native.liveNativeHandles();
    assertTrue(breakdown.contains("TemporalSorter=1"), "unexpected breakdown: " + breakdown);
    Native.closeTemporalSorter(sorter);
    assertEquals("", Native.liveNativeHandles());
  }
}
