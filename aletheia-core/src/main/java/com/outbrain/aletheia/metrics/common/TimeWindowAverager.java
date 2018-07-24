package com.outbrain.aletheia.metrics.common;


import java.util.function.DoubleSupplier;

//TODO: [av] adapt to prometheus
public class TimeWindowAverager implements Gauge<Double>, DoubleSupplier {

  private final TimeWindowRatio ratio;

  public TimeWindowAverager(final double windowSizeInSeconds,
                            final int expectedValueEstimate,
                            final double noCallsValue) {
    ratio = new TimeWindowRatio(windowSizeInSeconds, noCallsValue, new SafeRatioHolder(expectedValueEstimate, 1));
  }

  @Override
  public Double getValue(final String... labelValues) {
    return ratio.getValue();
  }

  @Override
  public void set(Double value, String... labelValues) {

  }

  public void addSample(final int value) {
    ratio.addRatio(value, 1);
  }

  @Override
  public double getAsDouble() {
    return ratio.getValue();
  }
}
