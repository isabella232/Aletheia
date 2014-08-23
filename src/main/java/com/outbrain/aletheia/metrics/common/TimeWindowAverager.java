package com.outbrain.aletheia.metrics.common;


public class TimeWindowAverager implements Gauge<Double>, GaugeStateHolder {

  private final TimeWindowRatio ratio;

  public TimeWindowAverager(final double windowSizeInSeconds,
                            final int expectedValueEstimate,
                            final double noCallsValue) {
    ratio = new TimeWindowRatio(windowSizeInSeconds, noCallsValue, new SafeRatioHolder(expectedValueEstimate, 1));
  }

  @Override
  public Double getValue() {
    return ratio.getValue();
  }

  public void addSample(final int value) {
    ratio.addRatio(value, 1);
  }
}
