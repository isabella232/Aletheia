package com.outbrain.aletheia.datum.metrics.kafka;

import com.outbrain.aletheia.metrics.common.Gauge;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.stats.Snapshot;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by slevin on 3/23/15.
 */
class MetricRegistry2MetricsFactoryBridge implements MetricProcessor<Object> {

  private static class SettableGauge<T> implements Gauge<T> {

    protected volatile T value;

    public SettableGauge(final T value) {
      this.value = value;
    }

    public void setValue(final T value) {
      this.value = value;
    }

    @Override
    public T getValue() {
      return value;
    }

  }

  private final MetricsFactory metricsFactory;

  public MetricRegistry2MetricsFactoryBridge(final MetricsFactory metricsFactory) {
    this.metricsFactory = metricsFactory;
  }

  private <T> SettableGauge<T> createOrSetGauge(final String component, final String methodName, final T value) {
    final SettableGauge<T> gauge =
            (SettableGauge<T>) metricsFactory.createGauge(component,
                                                          methodName,
                                                          new SettableGauge<>(value));

    gauge.setValue(value);
    return gauge;
  }

  private <T> SettableGauge<T> createOrSetGauge(final MetricName metricName, final String suffix, final T value) {
    return createOrSetGauge(componentName(metricName),
                            metricName.getName() + (StringUtils.isBlank(suffix) ? "" : "." + suffix),
                            value);


  }

  private <T> SettableGauge<T> createOrSetGauge(final MetricName metricName, final T value) {
    return createOrSetGauge(metricName, "", value);
  }

  public static String componentName(final MetricName metricName) {
    return metricName.getGroup() + "." + metricName.getType();
  }

  @Override
  public void processHistogram(final MetricName name,
                               final com.yammer.metrics.core.Histogram histogram,
                               final Object context) throws Exception {

    final Snapshot snapshot = histogram.getSnapshot();

    createOrSetGauge(name, "75thPercentile", snapshot.get75thPercentile());
    createOrSetGauge(name, "95thPercentile", snapshot.get95thPercentile());
    createOrSetGauge(name, "99thPercentile", snapshot.get99thPercentile());
    createOrSetGauge(name, "median", snapshot.getMedian());
    createOrSetGauge(name, "min", histogram.min());
    createOrSetGauge(name, "max", histogram.max());
    createOrSetGauge(name, "stdDev", histogram.stdDev());

  }

  @Override
  public void processMeter(final MetricName name,
                           final com.yammer.metrics.core.Metered meter,
                           final Object context) throws Exception {

    createOrSetGauge(name, "15MinuteRate", meter.fifteenMinuteRate());
    createOrSetGauge(name, "5MinuteRate", meter.fiveMinuteRate());
    createOrSetGauge(name, "1MinuteRate", meter.oneMinuteRate());
    createOrSetGauge(name, "meanRate", meter.meanRate());
    createOrSetGauge(name, "count", meter.count());

  }

  @Override
  public void processTimer(final MetricName name,
                           final com.yammer.metrics.core.Timer timer,
                           final Object context) throws Exception {

    createOrSetGauge(name, "15MinuteRate", timer.fifteenMinuteRate());
    createOrSetGauge(name, "5MinuteRate", timer.fiveMinuteRate());
    createOrSetGauge(name, "1MinuteRate", timer.oneMinuteRate());
    createOrSetGauge(name, "meanRate", timer.meanRate());
    createOrSetGauge(name, "count", timer.count());
  }

  @Override
  public void processCounter(final MetricName name,
                             final com.yammer.metrics.core.Counter counter,
                             final Object context) throws Exception {
    createOrSetGauge(name, counter.count());
  }

  @Override
  public void processGauge(final MetricName name,
                           final com.yammer.metrics.core.Gauge<?> gauge,
                           final Object context) throws Exception {
    createOrSetGauge(name, gauge.value());
  }
}
