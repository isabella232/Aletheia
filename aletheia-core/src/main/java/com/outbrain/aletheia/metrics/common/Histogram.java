package com.outbrain.aletheia.metrics.common;

public interface Histogram {

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  void update(int value);

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  void update(long value);

}