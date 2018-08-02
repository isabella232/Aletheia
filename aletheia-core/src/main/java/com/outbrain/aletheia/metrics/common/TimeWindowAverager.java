package com.outbrain.aletheia.metrics.common;


import java.util.function.DoubleSupplier;

public class TimeWindowAverager implements DoubleSupplier {

  private final TimeWindowRatio ratio;

  public TimeWindowAverager(final double windowSizeInSeconds,
                            final int expectedValueEstimate,
                            final double noCallsValue) {
    ratio = new TimeWindowRatio(windowSizeInSeconds, noCallsValue, new SafeRatioHolder(expectedValueEstimate, 1));
  }

  @Override
  public double getAsDouble() {
    return ratio.getValue();
  }

  public void addSample(final int value) {
    ratio.addRatio(value, 1);
  }
}
