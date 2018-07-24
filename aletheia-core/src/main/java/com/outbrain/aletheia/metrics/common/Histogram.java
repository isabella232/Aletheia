package com.outbrain.aletheia.metrics.common;

public interface Histogram {

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  void update(final int value, final String... labelValues);

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  void update(final long value, final String... labelValues);

}
