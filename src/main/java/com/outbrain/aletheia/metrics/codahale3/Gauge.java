package com.outbrain.aletheia.metrics.codahale3;

public class Gauge<T> implements com.outbrain.aletheia.metrics.common.Gauge<T> {
  private final com.codahale.metrics.Gauge<T> gauge;

  public Gauge(final com.codahale.metrics.Gauge<T> gauge) {
    this.gauge = gauge;
  }

  @Override
  public T getValue() {
    return gauge.getValue();
  }
}
