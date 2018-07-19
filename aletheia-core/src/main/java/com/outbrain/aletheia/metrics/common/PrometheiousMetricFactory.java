package com.outbrain.aletheia.metrics.common;


import com.outbrain.swinfra.metrics.MetricRegistry;
import com.outbrain.swinfra.metrics.SettableGauge.SettableGaugeBuilder;
import com.outbrain.swinfra.metrics.Summary.SummaryBuilder;
import com.outbrain.swinfra.metrics.timing.Clock;
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.TimeUnit;

import static com.outbrain.swinfra.metrics.Counter.CounterBuilder;

public class PrometheiousMetricFactory implements MetricsFactory {

  public static final TimeUnit DEFAULT_TIMERS_MEASUREMENT_UNIT = TimeUnit.MILLISECONDS;

  final private MetricRegistry prometheusMetricRegistry;
  final private String[] fixedLabels;


  public PrometheiousMetricFactory(final MetricRegistry registry, final String... fixedLabels) {
    this.prometheusMetricRegistry = registry;
    this.fixedLabels = fixedLabels;
  }

  @Override
  public Timer createTimer(String component, String methodName, String... labelNames) {
    return null;
  }

  @Override
  public Summary createSummary(final String name,
                               final String help,
                               final String... labelNames) {
    return prometheusMetricRegistry.getOrRegister(new SummaryBuilder(name, help)
            .withLabels(labelNames)
            .withClock(new Clock.SystemClock(DEFAULT_TIMERS_MEASUREMENT_UNIT))
            .build());
  }

  @Override
  public Counter createCounter(String name, String help, String... labelNames) {

    return prometheusMetricRegistry.getOrRegister(new CounterBuilder(name, help)
            .withLabels((String[]) ArrayUtils.add(fixedLabels, labelNames))
            .build());
  }

  @Override
  public <T> Gauge<T> createGauge(String name, String help, Gauge<T> metric, String... labelNames) {
    return prometheusMetricRegistry.getOrRegister(new SettableGaugeBuilder(name, help)
            .withLabels((String[]) ArrayUtils.add(fixedLabels, labelNames))
            .build());
  }

  @Override
  public Meter createMeter(String component, String methodName, String eventType, String... labelNames) {
    return null;
  }

  @Override
  public Histogram createHistogram(String component, String methodName, boolean biased, String... labelNames) {
    return null;
  }


}
