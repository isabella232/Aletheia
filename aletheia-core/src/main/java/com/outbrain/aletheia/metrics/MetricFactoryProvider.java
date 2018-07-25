package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * A base interface fulfilling Aletheia's component specific metrics needs.
 */
public interface MetricFactoryProvider {

  MetricFactoryProvider NULL = new MetricFactoryProvider() {
    @Override
    public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint, boolean isBreadcrambs) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint, boolean isBreadcrambs) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forAuditingDatumStreamConsumer(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }

    @Override
    public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
      return MetricsFactory.NULL;
    }
  };

  MetricsFactory forAuditingDatumProducer(final EndPoint endPoint, boolean isBreadcrambs);

  MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint);

  MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint, boolean isBreadcrambs);

  MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint);

  MetricsFactory forAuditingDatumStreamConsumer(final EndPoint endPoint);

  MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint);
}
