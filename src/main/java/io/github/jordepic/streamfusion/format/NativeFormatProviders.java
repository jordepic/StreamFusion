package io.github.jordepic.streamfusion.format;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.HashSet;

/** Discovers installed native format artifacts through the same provider pattern Flink uses. */
public final class NativeFormatProviders {

  private NativeFormatProviders() {}

  /** Returns the table's value-format name using Flink's {@code value.format} precedence. */
  public static String formatIdentifier(Map<String, String> options) {
    return options.getOrDefault("value.format", options.get("format"));
  }

  /** Finds an installed provider that accepts this table's exact decoder options. */
  public static Optional<NativeFormatProvider> find(NativeFormatContext context) {
    String identifier = formatIdentifier(context.options());
    if (identifier == null) {
      return Optional.empty();
    }
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader providerLoader = NativeFormatProviders.class.getClassLoader();
    Set<String> seen = new HashSet<>();
    for (ClassLoader loader : new ClassLoader[] {contextLoader, providerLoader}) {
      if (loader == null) {
        continue;
      }
      Optional<NativeFormatProvider> provider = findIn(loader, identifier, context, seen);
      if (provider.isPresent()) {
        return provider;
      }
    }
    return Optional.empty();
  }

  private static Optional<NativeFormatProvider> findIn(
      ClassLoader loader,
      String identifier,
      NativeFormatContext context,
      Set<String> seen) {
    try {
      for (NativeFormatProvider provider : ServiceLoader.load(NativeFormatProvider.class, loader)) {
        if (!seen.add(provider.getClass().getName())) {
          continue;
        }
        if (identifier.equals(provider.formatIdentifier()) && provider.supports(context)) {
          return Optional.of(provider);
        }
      }
    } catch (ServiceConfigurationError | LinkageError ignored) {
      // Optional deployment artifacts can be incomplete or carry an unavailable transitive
      // dependency. Treat that exactly like an absent native format and keep the table on Flink.
    }
    return Optional.empty();
  }
}
