package com.outbrain.aletheia.metrics.common;

public interface Counter {
  /**
   * Increment the counter by one.
   */
  void inc();

  /**
   * Increment the counter by n.
   *
   * @param n the amount by which the counter will be increased
   */
  void inc(long n);

  /**
   * Decrement the counter by one.
   */
  void dec();

  /**
   * Decrement the counter by n.
   *
   * @param n the amount by which the counter will be increased
   */
  void dec(long n);

  /**
   * Returns the counter's current value.
   *
   * @return the counter's current value
   */
  long getCount();

}