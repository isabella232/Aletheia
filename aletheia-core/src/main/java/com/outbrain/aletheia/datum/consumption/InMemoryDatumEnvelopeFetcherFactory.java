package com.outbrain.aletheia.datum.consumption;

import com.google.common.collect.Lists;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.List;

/**
 * A {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory} for building
 * {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher}s capable of consuming data from endpoints of
 * type {@link com.outbrain.aletheia.datum.consumption.FetchConsumptionEndPoint}.
 */
public class InMemoryDatumEnvelopeFetcherFactory implements DatumEnvelopeFetcherFactory<FetchConsumptionEndPoint<byte[]>> {

  @Override
  public List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(final FetchConsumptionEndPoint<byte[]> consumptionEndPoint,
                                                              final MetricsFactory metricFactory) {

    return Lists.<DatumEnvelopeFetcher>newArrayList(new InMemoryDatumEnvelopeFetcher(consumptionEndPoint,
                                                                                     metricFactory));
  }
}
