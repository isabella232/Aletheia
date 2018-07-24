package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.PrometheusMetricFactory;

import java.util.HashMap;
import java.util.Map;

public class InternalPrometheusMetricFactoryProvider extends PrometheusMetricFactoryProvider {

  public InternalPrometheusMetricFactoryProvider(final String datumTypeId, final String componentName, final MetricsFactory metricsFactory) {
    super(datumTypeId, componentName, metricsFactory);
  }

  @Override
  public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint) {
    return buildMetricsFactory(endPoint);
  }

  @Override
  public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint) {
    return buildMetricsFactory(endPoint);
  }

  @Override
  public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {

    return super.forInternalBreadcrumbProducer(endPoint);
  }

  private MetricsFactory buildMetricsFactory(final EndPoint endPoint) {
    Map<String, String> labels = new HashMap<>();
    labels.put(DATUM_TYPE_ID, datumTypeId());
    labels.put(DIRECTION, PRODUCTION);
    labels.put(ENDPOINT_CLASS, endPoint.getClass().getSimpleName());
    return new PrometheusMetricFactory(
            metricRegistry,
            labels
    );
  }
}
