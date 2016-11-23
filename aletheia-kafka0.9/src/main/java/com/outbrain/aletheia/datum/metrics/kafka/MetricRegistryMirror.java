package com.outbrain.aletheia.datum.metrics.kafka;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import com.outbrain.aletheia.metrics.common.Gauge;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;


class MetricRegistryMirror {

  private static Logger log = LoggerFactory.getLogger(MetricRegistryMirror.class);

  private final MetricsFactory metricsFactory;

  private MetricRegistryMirror(final MetricsFactory targetMetricsFactory) {
      metricsFactory = targetMetricsFactory;
  }
  private Predicate<MetricName> metricPredicate(final Predicate<String> filter) {
    return new Predicate<MetricName>() {
      @Override
      public boolean apply(final MetricName metricName) {
        return filter.apply(metricName.toString());
      }
    };
  }

  private ImmutableSet<Map.Entry<MetricName, KafkaMetric>> filter(final ConcurrentMap<MetricName, KafkaMetric> kafkaMetrics,
                                                                  final Predicate<String> metricNameFilter) {

    return FluentIterable.from(kafkaMetrics.entrySet())
                         .filter(new Predicate<Map.Entry<MetricName, KafkaMetric>>() {
                           @Override
                           public boolean apply(final Map.Entry<MetricName, KafkaMetric> entry) {
                             final MetricName metricName = entry.getKey();
                             return metricPredicate(metricNameFilter).apply(metricName);
                           }
                         })
                         .toSet();
  }
  public static MetricRegistryMirror mirrorTo(final MetricsFactory targetMetricsFactory) {
    return new MetricRegistryMirror(targetMetricsFactory);
  }

  public void mirrorFrom(final ConcurrentMap<MetricName, KafkaMetric> kafkaMetrics,
                         final Predicate<String> metricFilter,
                         final Function<MetricName, String> metricNameAdjuster) {

    for (final Map.Entry<MetricName, KafkaMetric> entry : filter(kafkaMetrics, metricFilter)) {
      try {
        final MetricName metricName = entry.getKey();
        final String realMetricName = metricNameAdjuster.apply(metricName);
        metricsFactory.createGauge(metricName.group(), realMetricName, new Gauge<Double>() {
          @Override
          public Double getValue() {
            return entry.getValue().value();
          }
        });
      } catch (final Exception e) {
        log.error("Could not process metrics", e);
      }
    }
  }
}
