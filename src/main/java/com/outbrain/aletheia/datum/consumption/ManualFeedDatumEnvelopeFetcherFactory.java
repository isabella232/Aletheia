package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Created by slevin on 8/15/14.
 */
public class ManualFeedDatumEnvelopeFetcherFactory implements DatumEnvelopeFetcherFactory<ManualFeedConsumptionEndPoint> {

  @Override
  public DatumEnvelopeFetcher buildDatumEnvelopeFetcher(final ManualFeedConsumptionEndPoint consumptionEndPoint,
                                                        final MetricsFactory metricFactory) {
    return new ManualFeedDatumEnvelopeFetcher(consumptionEndPoint, metricFactory);
  }
}
