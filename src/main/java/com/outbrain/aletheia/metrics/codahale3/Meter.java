package com.outbrain.aletheia.metrics.codahale3;

/**
 * Created by slevin on 9/16/14.
 */
public class Meter implements com.outbrain.aletheia.metrics.common.Meter {
  private final com.codahale.metrics.Meter meter;

  public Meter(final com.codahale.metrics.Meter meter) {
    this.meter = meter;
  }

  @Override
  public void mark() {
    meter.mark();
  }

  @Override
  public void mark(final long n) {
    meter.mark(n);
  }
}
