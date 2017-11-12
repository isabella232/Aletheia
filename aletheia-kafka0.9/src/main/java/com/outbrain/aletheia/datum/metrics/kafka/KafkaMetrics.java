package com.outbrain.aletheia.datum.metrics.kafka;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A utility class used to report Kafka clients' metrics using Aletheia's metrics system.
 */
public class KafkaMetrics implements MetricsReporter {

  private static Logger log = LoggerFactory.getLogger(KafkaMetrics.class);
  private static final ConcurrentMap<MetricName, KafkaMetric> kafkaMetrics = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  public static ScheduledExecutorService reportTo(final MetricsFactory metricsFactory,
                                                  final String kafkaClientId,
                                                  final Duration syncInterval) {
    return reportTo(metricsFactory, kafkaClientId, syncInterval, Predicates.<String>alwaysTrue());
  }

  public static ScheduledExecutorService reportTo(final MetricsFactory metricsFactory,
                                                  final String kafkaClientId,
                                                  final Duration syncInterval,
                                                  final Predicate<String> metricFilter) {
    final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor();
    scheduledExecutorService
            .scheduleWithFixedDelay(
                    new Runnable() {

                      final Function<MetricName, String> metricNameAdjuster =
                              new Function<MetricName, String>() {

                                private String replaceKafkaClientId(final String str) {
                                  return str.replaceAll(kafkaClientId, "client");
                                }

                                @Override
                                public String apply(final MetricName metric) {
                                    final StringBuilder sb = new StringBuilder();
                                    for (final Map.Entry<String, String> tag : new TreeMap<>(metric.tags()).entrySet()) {
                                        if (!tag.getKey().isEmpty() && !tag.getValue().isEmpty()) {
                                            sb.append(tag.getValue()).append('.');
                                        }
                                    }
                                    sb.append(metric.name());
                                    return replaceKafkaClientId(sb.toString());
                                }
                              };

                      @Override
                      public void run() {
                        try {
                          MetricRegistryMirror
                                  .mirrorTo(metricsFactory)
                                  .mirrorFrom(kafkaMetrics,
                                              Predicates.and(Predicates.containsPattern(kafkaClientId),
                                                             metricFilter),
                                              metricNameAdjuster);
                          log.debug("Kafka client metrics sync completed.");
                        } catch (final Exception e) {
                          log.error("Error while mirroring metrics", e);
                        }
                      }
                    },
                    0,
                    syncInterval.getStandardSeconds(),
                    TimeUnit.SECONDS);
    return scheduledExecutorService;
  }

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (final KafkaMetric metric : metrics) {
            metricChange(metric);
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        kafkaMetrics.put(metric.metricName(), metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        kafkaMetrics.remove(metric.metricName());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
