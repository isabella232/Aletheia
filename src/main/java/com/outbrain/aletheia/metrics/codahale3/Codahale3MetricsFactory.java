package com.outbrain.aletheia.metrics.codahale3;

import com.codahale.metrics.MetricRegistry;

public class Codahale3MetricsFactory implements com.outbrain.aletheia.metrics.common.MetricsFactory {

  private static class GaugeContainer<T> implements com.codahale.metrics.Gauge<T> {

    private final com.outbrain.aletheia.metrics.common.Gauge<T> metric;

    public GaugeContainer(final com.outbrain.aletheia.metrics.common.Gauge<T> metric) {
      this.metric = metric;
    }

    @Override
    public T getValue() {
      return metric.getValue();
    }

    public com.outbrain.aletheia.metrics.common.Gauge<T> getMetric() {
      return metric;
    }
  }

  private final MetricRegistry registry;

  private Codahale3MetricsFactory(final MetricRegistry registry) {
    this.registry = registry;
  }

  public static com.outbrain.aletheia.metrics.common.MetricsFactory from(final MetricRegistry registry) {
    return new Codahale3MetricsFactory(registry);
  }

  private <T> com.outbrain.aletheia.metrics.common.Gauge<T> registerGauge(final String component,
                                                                          final String methodName,
                                                                          final com.outbrain.aletheia.metrics.common.Gauge<T> metric) {

    final GaugeContainer<T> bla = registry.register(MetricRegistry.name(component, methodName),
                                                    new GaugeContainer<>(metric));

    return bla.getMetric();
  }

  @Override
  public com.outbrain.aletheia.metrics.common.Timer createTimer(final String component, final String methodName) {
    final com.codahale.metrics.Timer timer = registry.timer(MetricRegistry.name(component, methodName));
    return new com.outbrain.aletheia.metrics.codahale3.Timer(timer);
  }

  @Override
  public com.outbrain.aletheia.metrics.common.Counter createCounter(final String component, final String methodName) {
    final com.codahale.metrics.Counter counter = registry.counter(MetricRegistry.name(component, methodName));
    return new com.outbrain.aletheia.metrics.codahale3.Counter(counter);
  }

  @Override
  public <T> com.outbrain.aletheia.metrics.common.Gauge<T> createGauge(final String component,
                                                                       final String methodName,
                                                                       final com.outbrain.aletheia.metrics.common.Gauge<T> metric) {
    return registerGauge(component, methodName, metric);
  }

  @Override
  public Meter createMeter(final String component, final String methodName, final String eventType) {
    final com.codahale.metrics.Meter meter = registry.meter(MetricRegistry.name(component, methodName));
    return new com.outbrain.aletheia.metrics.codahale3.Meter(meter);
  }

  @Override
  public Histogram createHistogram(final String component, final String methodName, final boolean biased) {
    final com.codahale.metrics.Histogram histogram = registry.histogram(MetricRegistry.name(component, methodName));
    return new com.outbrain.aletheia.metrics.codahale3.Histogram(histogram);
  }

}