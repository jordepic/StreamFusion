package io.github.jordepic.streamfusion;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/** Loads an optional StreamFusion native extension from the JAR that declares its Java API. */
public final class NativeExtensionLoader {

  private static final String RESOURCE_PREFIX = "/io/github/jordepic/streamfusion/native/";

  private NativeExtensionLoader() {}

  public static void load(Class<?> owner, String extension) {
    String libraryName = "streamfusion_" + extension;
    try {
      System.loadLibrary(libraryName);
      return;
    } catch (UnsatisfiedLinkError libraryPathFailure) {
      if (loadBundled(owner, extension, libraryName)) {
        return;
      }

      if (isPackaged(owner)) {
        UnsatisfiedLinkError error =
            new UnsatisfiedLinkError(
                "No bundled native library for StreamFusion extension '"
                    + extension
                    + "' on "
                    + Native.nativePlatform()
                    + "/"
                    + Native.nativeArchitecture());
        error.initCause(libraryPathFailure);
        throw error;
      }

      // A source-tree test build carries all enabled JNI entry points in the development core
      // library. Release extension JARs never use this fallback: they include their own DSO.
      try {
        System.loadLibrary("streamfusion");
        return;
      } catch (UnsatisfiedLinkError developmentLibraryFailure) {
        developmentLibraryFailure.addSuppressed(libraryPathFailure);
        throw developmentLibraryFailure;
      }
    }
  }

  private static boolean isPackaged(Class<?> owner) {
    URL classResource = owner.getResource(owner.getSimpleName() + ".class");
    return classResource != null && "jar".equals(classResource.getProtocol());
  }

  private static boolean loadBundled(Class<?> owner, String extension, String libraryName) {
    for (String resource : resourcePaths(extension, libraryName)) {
      try (InputStream stream = owner.getResourceAsStream(resource)) {
        if (stream == null) {
          continue;
        }
        String fileName = System.mapLibraryName(libraryName);
        String suffix = fileName.substring(fileName.lastIndexOf('.'));
        Path extracted = Files.createTempFile(libraryName + "-", suffix);
        try {
          Files.copy(stream, extracted, StandardCopyOption.REPLACE_EXISTING);
          System.load(extracted.toAbsolutePath().toString());
          return true;
        } finally {
          extracted.toFile().deleteOnExit();
        }
      } catch (IOException error) {
        throw new IllegalStateException(
            "Unable to extract bundled StreamFusion " + extension + " native library.", error);
      }
    }
    return false;
  }

  private static List<String> resourcePaths(String extension, String libraryName) {
    return List.of(
        RESOURCE_PREFIX
            + extension
            + "/"
            + Native.nativePlatform()
            + "/"
            + Native.nativeArchitecture()
            + "/"
            + System.mapLibraryName(libraryName),
        RESOURCE_PREFIX + extension + "/" + System.mapLibraryName(libraryName));
  }
}
