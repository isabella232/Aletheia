package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.PrometheiousMetricFactory;

public class PrometheiousMetricFactoryProvider extends AletheiaMetricFactoryProvider {


  public PrometheiousMetricFactoryProvider(String datumTypeId, String componentName, MetricsFactory metricsFactory) {
    super(datumTypeId, componentName, metricsFactory);
  }

  @Override
  public MetricsFactory forAuditingDatumProducer(EndPoint endPoint) {
    return null;
  }

  @Override
  public MetricsFactory forInternalBreadcrumbProducer(EndPoint endPoint) {
    return new PrometheiousMetricFactory(metricRegistry,
            ALETHEIA,
            DATUM_TYPES,
            datumTypeId(),
            componentName,
            endPoint.getName());
  }

  @Override
  public MetricsFactory forDatumEnvelopeSender(EndPoint endPoint) {
    return null;
  }

  @Override
  public MetricsFactory forDatumEnvelopeFetcher(EndPoint endPoint) {
    return null;
  }

  @Override
  public MetricsFactory forAuditingDatumStreamConsumer(EndPoint endPoint) {
    return null;
  }

  @Override
  public MetricsFactory forDatumEnvelopeMeta(EndPoint endPoint) {
    return null;
  }
}
