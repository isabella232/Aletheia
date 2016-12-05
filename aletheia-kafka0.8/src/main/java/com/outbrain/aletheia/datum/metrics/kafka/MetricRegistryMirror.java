package com.outbrain.aletheia.datum.metrics.kafka;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by slevin on 3/23/15.
 */
class MetricRegistryMirror {

  private static Logger log = LoggerFactory.getLogger(MetricRegistryMirror.class);

  private final MetricRegistry2MetricsFactoryBridge metricRegistry2MetricsFactoryBridge;

  private MetricRegistryMirror(final MetricsFactory targetMetricsFactory) {
    metricRegistry2MetricsFactoryBridge = new MetricRegistry2MetricsFactoryBridge(targetMetricsFactory);
  }

  private Predicate<MetricName> metricPredicate(final Predicate<String> filter) {
    return new Predicate<MetricName>() {
      @Override
      public boolean apply(final MetricName metricName) {
        return filter.apply(MetricRegistry2MetricsFactoryBridge.componentName(metricName) + "." +
                            metricName.getScope() + "." + metricName.getName());
      }
    };
  }

  private ImmutableSet<Map.Entry<MetricName, Metric>> filter(final MetricsRegistry metricsRegistry,
                                                             final Predicate<String> metricNameFilter) {

    return FluentIterable.from(metricsRegistry.allMetrics().entrySet())
                         .filter(new Predicate<Map.Entry<MetricName, Metric>>() {
                           @Override
                           public boolean apply(final Map.Entry<MetricName, Metric> entry) {
                             final MetricName metricName = entry.getKey();
                             return metricPredicate(metricNameFilter).apply(metricName);
                           }
                         })
                         .toSet();
  }

  public static MetricRegistryMirror mirrorTo(final MetricsFactory targetMetricsFactory) {
    return new MetricRegistryMirror(targetMetricsFactory);
  }

  public void mirrorFrom(final MetricsRegistry sourceMetrics,
                         final Predicate<String> metricFilter,
                         final Function<MetricName, MetricName> metricNameAdjuster) {

    for (final Map.Entry<MetricName, Metric> entry : filter(sourceMetrics, metricFilter)) {
      try {
        final Metric metric = entry.getValue();
        final MetricName metricName = entry.getKey();
        metric.processWith(metricRegistry2MetricsFactoryBridge, metricNameAdjuster.apply(metricName), new Object());
      } catch (final Exception e) {
        log.error("Could not process metrics", e);
      }
    }
  }
}
