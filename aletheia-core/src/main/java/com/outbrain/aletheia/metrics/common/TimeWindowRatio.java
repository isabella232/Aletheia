package com.outbrain.aletheia.metrics.common;

import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TimeWindowRatio implements Gauge<Double> {

  private final RatioHolder averager;
  private double lastValueBeforeReset;
  private final AtomicReference<DateTime> lastResetTime;
  private final int windowSizeInMilliseconds;
  private final AtomicBoolean isResetting = new AtomicBoolean(false);
  private final double noCallsValue;

  public TimeWindowRatio(final double windowSizeInSeconds,
                         final double noCallsValue,
                         final RatioHolder ratioContainer) {
    this.noCallsValue = noCallsValue;
    windowSizeInMilliseconds = (int) (windowSizeInSeconds * 1000);
    lastResetTime = new AtomicReference<>(new DateTime());
    averager = ratioContainer;
  }

  public void addRatio(final int nominatorDelta, final int denominatorDelta) {
    resetIfNeeded();
    averager.addDeltas(nominatorDelta, denominatorDelta);
  }

  @Override
  public Double getValue(final String... labelValues) {
    resetIfNeeded();
    return lastValueBeforeReset;
  }

  private void resetIfNeeded() {
    final DateTime possibleLastWindowStart = new DateTime().minusMillis(windowSizeInMilliseconds);
    if (lastResetTime.get().isBefore(possibleLastWindowStart)) {
      if (isResetting.compareAndSet(false, true)) {
        try {
          if (lastResetTime.get().isBefore(possibleLastWindowStart)) {
            final Double optionalValue = averager.resetAndReturnLastValue();
            if (optionalValue != null) {
              lastValueBeforeReset = optionalValue;
            } else {
              lastValueBeforeReset = noCallsValue;
            }
            lastResetTime.set(new DateTime());
          }
        } finally {
          isResetting.set(false);
        }
      }
    }
  }


  @Override
  public void set(final Double value, final String... labelValues) {

  }
}
