package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * The default implementation for Aletheia specific metrics with pre-defined component names and hierarchical
 * metric tree structure.
 */
public class DefaultMetricFactoryProvider extends AletheiaMetricFactoryProvider {

  public static final String META = "Meta";

  public DefaultMetricFactoryProvider(final Class domainClass,
                                      final String componentName,
                                      final MetricsFactory metricsFactory) {
    super(domainClass, componentName, metricsFactory);
  }

  @Override
  public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint) {
    return MetricFactoryPrefixer.prefix(metricsFactory).with(ALETHEIA,
                                                             DATUM_TYPES,
                                                             datumTypeId(),
                                                             componentName,
                                                             endPoint.getName(),
                                                             DATA,
                                                             PRODUCTION,
                                                             endPoint.getClass().getSimpleName());
  }

  @Override
  public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {
    return MetricFactoryPrefixer.prefix(metricsFactory).with(ALETHEIA,
                                                             DATUM_TYPES,
                                                             datumTypeId(),
                                                             componentName,
                                                             endPoint.getName());
  }

  @Override
  public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint) {
    return MetricFactoryPrefixer.prefix(metricsFactory).with(ALETHEIA,
                                                             DATUM_TYPES,
                                                             datumTypeId(),
                                                             componentName,
                                                             endPoint.getName(),
                                                             DATA,
                                                             Tx,
                                                             endPoint.getClass().getSimpleName());
  }

  @Override
  public MetricsFactory forAuditingDatumConsumer(final EndPoint endPoint) {
    return MetricFactoryPrefixer.prefix(metricsFactory).with(ALETHEIA,
                                                             DATUM_TYPES,
                                                             datumTypeId(),
                                                             componentName,
                                                             endPoint.getName(),
                                                             DATA,
                                                             CONSUMPTION,
                                                             endPoint.getClass().getSimpleName());
  }

  @Override
  public MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint) {
    return MetricFactoryPrefixer.prefix(metricsFactory).with(ALETHEIA,
                                                             DATUM_TYPES,
                                                             datumTypeId(),
                                                             componentName,
                                                             endPoint.getName(),
                                                             DATA,
                                                             Rx,
                                                             endPoint.getClass().getSimpleName());
  }

  @Override
  public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
    return MetricFactoryPrefixer.prefix(metricsFactory).with(ALETHEIA,
                                                             DATUM_TYPES,
                                                             datumTypeId(),
                                                             componentName,
                                                             endPoint.getName(),
                                                             DATA,
                                                             Rx,
                                                             endPoint.getClass().getSimpleName(),
                                                             META);
  }
}
