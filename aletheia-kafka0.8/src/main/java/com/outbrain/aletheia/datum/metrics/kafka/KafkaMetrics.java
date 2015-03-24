package com.outbrain.aletheia.datum.metrics.kafka;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A utility class used to report Kafka clients' metrics using Aletheia's metrics system.
 */
public class KafkaMetrics {

  private static Logger log = LoggerFactory.getLogger(KafkaMetrics.class);

  public static void reportTo(final MetricsFactory metricsFactory,
                              final String kafkaClientId,
                              final Duration syncInterval) {
    reportTo(metricsFactory, kafkaClientId, syncInterval, Predicates.<String>alwaysTrue());
  }

  public static void reportTo(final MetricsFactory metricsFactory,
                              final String kafkaClientId,
                              final Duration syncInterval,
                              final Predicate<String> metricFilter) {
    Executors
            .newSingleThreadScheduledExecutor()
            .scheduleWithFixedDelay(
                    new Runnable() {

                      final Function<MetricName, MetricName> metricNameAdjuster =
                              new Function<MetricName, MetricName>() {

                                private String replaceKafkaClientId(final String str) {
                                  return str.replaceAll(kafkaClientId, "client");
                                }

                                @Override
                                public MetricName apply(final MetricName name) {
                                  return new MetricName(replaceKafkaClientId(name.getGroup()),
                                                        replaceKafkaClientId(name.getType()),
                                                        replaceKafkaClientId(name.getName()).replaceAll("\\.", "_"));
                                }
                              };

                      @Override
                      public void run() {
                        try {
                          MetricRegistryMirror
                                  .mirrorTo(metricsFactory)
                                  .mirrorFrom(Metrics.defaultRegistry(),
                                              Predicates.and(Predicates.containsPattern("kafka"),
                                                             Predicates.containsPattern(kafkaClientId),
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

  }
}
