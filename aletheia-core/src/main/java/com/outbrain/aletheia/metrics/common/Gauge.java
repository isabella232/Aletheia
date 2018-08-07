package com.outbrain.aletheia.metrics.common;

public interface Gauge<T> {

  /**
   * Returns the metric's current value.
   *
   * @return the metric's current value
   */

  T getValue(final String... labelValues);

  /**
   * Change value of the Gauge
   *
   * @param value       the new value
   * @param labelValues values for labels
   */
  void set(final T value, final String... labelValues);
}
