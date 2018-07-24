package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.PrometheusMetricFactory;

import java.util.HashMap;
import java.util.Map;

public class PrometheusMetricFactoryProvider extends AletheiaMetricFactoryProvider {

  public PrometheusMetricFactoryProvider(final String datumTypeId, final String componentName, final MetricsFactory metricsFactory) {
    super(datumTypeId, componentName, metricsFactory);
  }

  @Override
  public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint) {
    return buildMetricsFactory(endPoint);
  }

  @Override
  public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {

    Map<String, String> labels = new HashMap<>();
    labels.put(DATUM_TYPE_ID, datumTypeId());
    labels.put(COMPONENT, componentName);
    labels.put(ENDPOINT_NAME, endPoint.getName());

    return new PrometheusMetricFactory(metricRegistry, labels);
  }

  @Override
  public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint) {

    return buildMetricsFactory(endPoint);

  }

  @Override
  public MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint) {
    return buildMetricsFactory(endPoint);

  }

  @Override
  public MetricsFactory forAuditingDatumStreamConsumer(final EndPoint endPoint) {
    return buildMetricsFactory(endPoint);

  }

  @Override
  public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
    return buildMetricsFactory(endPoint);

  }

  private MetricsFactory buildMetricsFactory(EndPoint endPoint) {
    Map<String, String> labels = new HashMap<>();
    labels.put(DATUM_TYPE_ID, datumTypeId());
    labels.put(COMPONENT, componentName);
    labels.put(ENDPOINT_NAME, endPoint.getName());
    labels.put(DIRECTION, PRODUCTION);
    labels.put(ENDPOINT_CLASS, endPoint.getClass().getSimpleName());


    return new PrometheusMetricFactory(metricRegistry, labels);
  }
}
