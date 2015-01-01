package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * A base interface fulfilling Aletheia's component specific metrics needs.
 */
public interface MetricFactoryProvider {

  public static MetricFactoryProvider NULL = new MetricFactoryProvider() {
    @Override
    public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forAuditingDatumConsumer(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }
  };

  MetricsFactory forAuditingDatumProducer(final EndPoint endPoint);

  MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint);

  MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint);

  MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint);

  MetricsFactory forAuditingDatumConsumer(final EndPoint endPoint);

  MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint);
}
