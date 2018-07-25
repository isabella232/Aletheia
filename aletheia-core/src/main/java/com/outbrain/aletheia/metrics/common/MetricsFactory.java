package com.outbrain.aletheia.metrics.common;

import com.outbrain.swinfra.metrics.timing.Clock;
import com.outbrain.swinfra.metrics.timing.Timer;

import java.util.function.DoubleSupplier;

public interface MetricsFactory {

  MetricsFactory NULL = new MetricsFactory() {

    @Override
    public Counter createCounter(final String component, final String methodName, String... labelNames) {
      return new Counter() {

        @Override
        public void inc(final long n, String... labelValues) {

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
    public Gauge createGauge(final String component, final String methodName, final DoubleSupplier doubleSupplier, String... labelNames) {
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
    public Gauge createSettableGauge(String name, String help, String... labelNames) {
      return null;
    }

    @Override
    public Histogram createHistogram(final String name, final String help, final double[] buckets, String... labelNames) {
      return new Histogram() {

        @Override
        public void update(int value, String... labelValues) {

        }

        @Override
        public void update(long value, String... labelValues) {

        }
      };
    }

    @Override
    public Summary createSummary(String name, String help, String... labelNames) {
      return labelValues -> new Timer(Clock.DEFAULT_CLOCK, value -> {

      });
    }

  };

  Counter createCounter(final String name, final String help, String... labelNames);

  Gauge createGauge(String name, String help, DoubleSupplier metric, String... labelNames);

  Gauge createSettableGauge(String name, String help, String... labelNames);

  Histogram createHistogram(String name, String help, double[] buckets, String... labelNames);

  Summary createSummary(final String name, final String help, String... labelNames);


}
