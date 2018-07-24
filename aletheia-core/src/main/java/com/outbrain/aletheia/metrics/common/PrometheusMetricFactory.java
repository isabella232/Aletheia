package com.outbrain.aletheia.metrics.common;


import com.outbrain.aletheia.metrics.prometheus.CounterFacade;
import com.outbrain.aletheia.metrics.prometheus.HistogramFacade;
import com.outbrain.aletheia.metrics.prometheus.SettableGaugeFacade;
import com.outbrain.aletheia.metrics.prometheus.SummaryFacade;
import com.outbrain.swinfra.metrics.MetricRegistry;
import com.outbrain.swinfra.metrics.SettableGauge.SettableGaugeBuilder;
import com.outbrain.swinfra.metrics.Summary.SummaryBuilder;
import com.outbrain.swinfra.metrics.timing.Clock;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static com.outbrain.swinfra.metrics.Counter.CounterBuilder;

public class PrometheusMetricFactory implements MetricsFactory {

  public static final TimeUnit DEFAULT_TIMERS_MEASUREMENT_UNIT = TimeUnit.MILLISECONDS;

  final private MetricRegistry prometheusMetricRegistry;
  private String[] fixedLabelNames;
  private String[] fixedLabelValues;

  public PrometheusMetricFactory(final MetricRegistry registry, final Map<String, String> fixedLabels) {
    this.prometheusMetricRegistry = registry;
    labels(fixedLabels);
  }

  @Override
  public Summary createSummary(final String name,
                               final String help,
                               final String... labelNames) {
    return new SummaryFacade(prometheusMetricRegistry.getOrRegister(new SummaryBuilder(name, help)
            .withLabels(labelNames)
            .withClock(new Clock.SystemClock(DEFAULT_TIMERS_MEASUREMENT_UNIT))
            .build()));
  }

  @Override
  public Counter createCounter(final String name, final String help, final String... labelNames) {
    return new CounterFacade(prometheusMetricRegistry.getOrRegister(new CounterBuilder(name, help)
            .withLabels((String[]) ArrayUtils.addAll(fixedLabelNames, labelNames))
            .build()), this.fixedLabelValues);
  }

  @Override
  public Gauge createSettableGauge(final String name, final String help, final String... labelNames) {
    return new SettableGaugeFacade(prometheusMetricRegistry.getOrRegister(new SettableGaugeBuilder(name, help)
            .withLabels((String[]) ArrayUtils.addAll(fixedLabelNames, labelNames))
            .build()), this.fixedLabelValues);
  }

  @Override
  public Gauge createGauge(final String name, String help, final DoubleSupplier metric, final String... labelNames) {
    return new GaugeFacade(prometheusMetricRegistry.getOrRegister(new com.outbrain.swinfra.metrics.Gauge.GaugeBuilder(name, help)
            .withLabels((String[]) ArrayUtils.addAll(fixedLabelNames, labelNames))
            .withValueSupplier(metric, (String[]) ArrayUtils.addAll(fixedLabelNames, labelNames))
            .build()));


  }


  @Override
  public Histogram createHistogram(final String name,
                                   final String help,
                                   final double[] buckets,
                                   final String... labelNames) {

    return new HistogramFacade(new com.outbrain.swinfra.metrics.Histogram.HistogramBuilder(name, help)
            .withBuckets(buckets)
            .withLabels((String[]) ArrayUtils.addAll(fixedLabelNames, labelNames))
            .withClock(new Clock.SystemClock(DEFAULT_TIMERS_MEASUREMENT_UNIT)) //Default measuring unit for timers
            .build(), this.fixedLabelValues);
  }

  private void labels(final Map<String, String> fixedLabels) {
    fixedLabelNames = new String[fixedLabels.size()];
    fixedLabels.keySet().toArray(fixedLabelNames);

    fixedLabelValues = new String[fixedLabels.size()];
    fixedLabels.values().toArray(fixedLabelValues);
  }
}
