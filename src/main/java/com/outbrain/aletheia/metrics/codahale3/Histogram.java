package com.outbrain.aletheia.metrics.codahale3;


public class Histogram implements com.outbrain.aletheia.metrics.common.Histogram {
  private final com.codahale.metrics.Histogram histogram;

  public Histogram(final com.codahale.metrics.Histogram histogram) {
    this.histogram = histogram;
  }

  @Override
  public void update(final int value) {
    histogram.update(value);
  }

  @Override
  public void update(final long value) {
    histogram.update(value);
  }
}