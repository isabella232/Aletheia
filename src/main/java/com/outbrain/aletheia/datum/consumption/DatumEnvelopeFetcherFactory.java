package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Created by slevin on 8/15/14.
 */
public interface DatumEnvelopeFetcherFactory<TConsumptionEndPoint extends ConsumptionEndPoint> {
  DatumEnvelopeFetcher buildDatumEnvelopeFetcher(TConsumptionEndPoint consumptionEndPoint,
                                                 MetricsFactory metricFactory);
}