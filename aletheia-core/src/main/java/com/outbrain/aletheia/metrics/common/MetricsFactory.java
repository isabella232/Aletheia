package com.outbrain.aletheia.metrics.common;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public interface MetricsFactory {

  MetricsFactory NULL = new MetricsFactory() {
    @Override
    public Timer createTimer(final String component, final String methodName, String... labelNames) {
      return new Timer() {
        @Override
        public void update(final long duration, final TimeUnit unit) {

        }

        @Override
        public <T> T time(final Callable<T> event) throws Exception {
          return null;
        }

        @Override
        public Context time() {
          return new Context() {
            @Override
            public void stop() {

            }
          };
        }
      };
    }

    @Override
    public Counter createCounter(final String component, final String methodName, String... labelNames) {
      return new Counter() {
        @Override
        public void inc() {

        }

        @Override
        public void inc(final long n) {

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
      };
    }

    @Override
    public <T> Gauge<T> createGauge(final String component, final String methodName, final Gauge<T> metric, String... labelNames) {
      return metric != null ? metric : new Gauge<T>() {
        @Override
        public T getValue() {
          return null;
        }
      };
    }

    @Override
    public Meter createMeter(final String component, final String methodName, final String eventType, String... labelNames) {
      return new Meter() {
        @Override
        public void mark() {

        }

        @Override
        public void mark(final long n) {

        }
      };
    }

    @Override
    public Histogram createHistogram(final String component, final String methodName, final boolean biased, String... labelNames) {
      return new Histogram() {
        @Override
        public void update(final int value) {

        }

        @Override
        public void update(final long value) {

        }
      };
    }

    @Override
    public Summary createSummary(String name, String help, String... labelNames) {
      return null;
    }

  };

  Timer createTimer(final String name, final String help, String... labelNames);

  Counter createCounter(final String name, final String help, String... labelNames);

  <T> Gauge<T> createGauge(String component, String methodName, Gauge<T> metric, String... labelNames);

  Meter createMeter(String component, String methodName, String eventType, String... labelNames);

  Histogram createHistogram(String component, String methodName, boolean biased, String... labelNames);

  Summary createSummary(final String name, final String help, String... labelNames);


}
