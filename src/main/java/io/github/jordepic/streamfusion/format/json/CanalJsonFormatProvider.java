package io.github.jordepic.streamfusion.format.json;

/** Service-loadable Canal JSON provider. */
public final class CanalJsonFormatProvider extends JsonFormatProvider.Cdc {
  public CanalJsonFormatProvider() {
    super("canal-json", 9);
  }
}
