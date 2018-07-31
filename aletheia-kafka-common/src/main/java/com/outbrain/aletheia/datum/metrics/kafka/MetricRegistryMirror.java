package com.outbrain.aletheia.datum.metrics.kafka;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


class MetricRegistryMirror {

  private static final Logger log = LoggerFactory.getLogger(MetricRegistryMirror.class);

  private final MetricsFactory metricsFactory;

  private MetricRegistryMirror(final MetricsFactory targetMetricsFactory) {
    metricsFactory = targetMetricsFactory;
  }

  private ImmutableSet<Map.Entry<MetricName, KafkaMetric>> filter(final ConcurrentMap<MetricName, KafkaMetric> kafkaMetrics,
                                                                  final Predicate<String> metricNameFilter) {

    return ImmutableSet.copyOf(kafkaMetrics.entrySet()
            .stream()
            .filter(entry -> {
              return ((Predicate<MetricName>) metricName -> {
                return metricNameFilter.test(metricName.toString());
              }).test(entry.getKey());
            }).collect(Collectors.toSet()));
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
        metricsFactory.createSettableGauge(
                (metricName.group() + "_" + realMetricName).replaceAll("(-|\\.)", "_"),
                Strings.isNullOrEmpty(metricName.description()) ? "No description found" : metricName.description());
      } catch (final Exception e) {
        log.error("Could not process metrics", e);
      }
    }
  }
}
