package com.outbrain.aletheia.metrics.common;

public interface Histogram {

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  public void update(int value);

  /**
   * Adds a recorded value.
   *
   * @param value the length of the value
   */
  public void update(long value);

}