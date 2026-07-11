package io.github.jordepic.streamfusion.format;

import java.util.Map;

/** Native decoder option encoding shared by planner gates and format artifacts. */
public final class NativeFormatOptions {

  private NativeFormatOptions() {}

  public static String option(Map<String, String> options, String suffix) {
    String valueFormat = options.get("value.format");
    return valueFormat != null
        ? options.get("value." + valueFormat + "." + suffix)
        : options.get(options.get("format") + "." + suffix);
  }

  /**
   * Renders the options the native JSON/CSV implementations reproduce, or returns {@code null} when
   * the table needs behavior that must stay on Flink.
   */
  public static String encode(Map<String, String> options) {
    String format = NativeFormatProviders.formatIdentifier(options);
    StringBuilder encoded = new StringBuilder();
    if ("json".equals(format)
        || "debezium-json".equals(format)
        || "ogg-json".equals(format)
        || "maxwell-json".equals(format)
        || "canal-json".equals(format)) {
      if ("true".equalsIgnoreCase(option(options, "fail-on-missing-field"))) {
        return null;
      }
      String timestampFormat = option(options, "timestamp-format.standard");
      if (timestampFormat == null || "SQL".equals(timestampFormat)) {
        return encoded.toString();
      }
      return "ISO-8601".equals(timestampFormat) ? "timestamp-format=ISO-8601\n" : null;
    }
    if (!"csv".equals(format)) {
      return encoded.toString();
    }
    String delimiter = option(options, "field-delimiter");
    if (delimiter != null) {
      Character c = unescapedDelimiter(delimiter);
      if (c == null || !appendChar(encoded, "csv.field-delimiter", c)) {
        return null;
      }
    }
    String quote = option(options, "quote-character");
    if (quote != null && !appendChar(encoded, "csv.quote-character", quote.charAt(0))) {
      return null;
    }
    if ("true".equalsIgnoreCase(option(options, "disable-quote-character"))) {
      encoded.append("csv.disable-quote-character=true\n");
    }
    if (option(options, "escape-character") != null) {
      return null;
    }
    if ("true".equalsIgnoreCase(option(options, "allow-comments"))) {
      encoded.append("csv.allow-comments=true\n");
    }
    String nullLiteral = option(options, "null-literal");
    if (nullLiteral != null) {
      if (nullLiteral.contains("\n") || nullLiteral.contains("\r")) {
        return null;
      }
      encoded.append("csv.null-literal=").append(nullLiteral).append('\n');
    }
    return encoded.toString();
  }

  private static boolean appendChar(StringBuilder encoded, String key, char c) {
    if (c > 127 || c == '\n' || c == '\r') {
      return false;
    }
    encoded.append(key).append('=').append(c).append('\n');
    return true;
  }

  private static Character unescapedDelimiter(String raw) {
    if (raw.length() == 1) {
      return raw.charAt(0);
    }
    if (raw.length() == 2 && raw.charAt(0) == '\\') {
      switch (raw.charAt(1)) {
        case 't':
          return '\t';
        case 'b':
          return '\b';
        case 'f':
          return '\f';
        case '\\':
          return '\\';
        case '\'':
          return '\'';
        case '"':
          return '"';
        default:
          return null;
      }
    }
    if (raw.length() == 6 && raw.startsWith("\\u")) {
      try {
        return (char) Integer.parseInt(raw.substring(2), 16);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
