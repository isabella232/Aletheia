package com.outbrain.aletheia.metrics.common;

public interface Gauge<T> {

  /**
   * Returns the metric's current value.
   *
   * @return the metric's current value
   */

  T getValue(final String... labelValues);

  void set(final T value, final String... labelValues);
}
