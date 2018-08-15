package com.outbrain.aletheia.metrics.common;

import com.outbrain.swinfra.metrics.timing.Clock;
import com.outbrain.swinfra.metrics.timing.Timer;

import java.util.function.DoubleSupplier;

public interface MetricsFactory {

  MetricsFactory NULL = new MetricsFactory() {

    @Override
    public Counter createCounter(final String component, final String methodName, final String... labelNames) {
      return new Counter() {

        @Override
        public void inc(final long n, final String... labelValues) {

        }

        @Override
        public void dec() {

        }

        @Override
        public void dec(final long n) {

        }

        @Override
        public long getCount() {
          return 0;
        }

        @Override
        public void inc(final String... labelValues) {

        }
      };
    }

    @Override
    public Gauge createGauge(final String component, final String methodName, final DoubleSupplier doubleSupplier, final String... labelNames) {
      return new Gauge() {


        @Override
        public Object getValue(String... labelValues) {
          return null;
        }

        @Override
        public void set(Object value, String... labelValues) {

        }
      };
    }

    @Override
    public Gauge createSettableGauge(final String name, final String help, final String... labelNames) {
      return null;
    }

    @Override
    public Histogram createHistogram(final String name, final String help, final double[] buckets, final String... labelNames) {
      return new Histogram() {

        @Override
        public void update(final int value, final String... labelValues) {

        }

        @Override
        public void update(final long value, final String... labelValues) {

        }
      };
    }

    @Override
    public Summary createSummary(String name, String help, String... labelNames) {
      return new Summary() {

        @Override
        public Timer startTimer(String... labelValues) {
          return new Timer(Clock.DEFAULT_CLOCK, value -> {

          });
        }

        public void observe(long value, String... labelValues) {

        }
      };
    }


  };

  Counter createCounter(final String name, final String help, final String... labelNames);

  Gauge createGauge(final String name, final String help, final DoubleSupplier metric, final String... labelNames);

  Gauge createSettableGauge(final String name, final String help, final String... labelNames);

  Histogram createHistogram(final String name, final String help, final double[] buckets, final String... labelNames);

  Summary createSummary(final String name, final String help, final String... labelNames);


}
