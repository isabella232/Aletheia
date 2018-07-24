package com.outbrain.aletheia.metrics.common;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class GaugeFacade implements Gauge<Double> {
  private com.outbrain.swinfra.metrics.Gauge gauge;

  public GaugeFacade(final com.outbrain.swinfra.metrics.Gauge gauge) {
    this.gauge = gauge;
  }


  @Override
  public Double getValue(final String... labelValues) {
    return gauge.getValue(labelValues);
  }

  @Override
  public void set(final Double value, final String... labelValues) {

    throw new NotImplementedException();

  }
}
