package com.outbrain.aletheia.metrics.common;

public class PrefixedMetricFactory implements MetricsFactory {

  private final String prefix;
  private final MetricsFactory decoratedMetricFactory;

  public String getPrefix() {
    return prefix;
  }

  public MetricsFactory getDecoratedMetricFactory() {
    return decoratedMetricFactory;
  }

  public PrefixedMetricFactory(final String prefix, final MetricsFactory decoratedMetricFactory) {
    if (decoratedMetricFactory instanceof PrefixedMetricFactory) {
      this.prefix = ((PrefixedMetricFactory) decoratedMetricFactory).getPrefix() + "." + prefix;
      this.decoratedMetricFactory = ((PrefixedMetricFactory) decoratedMetricFactory).getDecoratedMetricFactory();
    } else {
      this.prefix = prefix;
      this.decoratedMetricFactory = decoratedMetricFactory;
    }
  }

  @Override
  public Timer createTimer(final String component, final String methodName) {
    return decoratedMetricFactory.createTimer(getComponentWithPrefix(component), methodName);
  }

  protected String getFullPrefix() {
    if (decoratedMetricFactory instanceof PrefixedMetricFactory) {
      final String fullPrefix = ((PrefixedMetricFactory) decoratedMetricFactory).getFullPrefix();
      return fullPrefix + "." + prefix;
    } else {
      return prefix;
    }
  }

  private String getComponentWithPrefix(final String component) {
    return prefix + "." + component;
  }

  @Override
  public Counter createCounter(final String component, final String methodName) {
    return decoratedMetricFactory.createCounter(getComponentWithPrefix(component), methodName);
  }

  @Override
  public <T> Gauge<T> createGauge(final String component, final String methodName, final Gauge<T> metric) {
    return decoratedMetricFactory.createGauge(getComponentWithPrefix(component), methodName, metric);
  }

  @Override
  public Meter createMeter(final String component, final String methodName, final String eventType) {
    return decoratedMetricFactory.createMeter(getComponentWithPrefix(component), methodName, eventType);
  }

  @Override
  public Histogram createHistogram(final String component, final String methodName, final boolean biased) {
    return decoratedMetricFactory.createHistogram(getComponentWithPrefix(component), methodName, biased);
  }

}
