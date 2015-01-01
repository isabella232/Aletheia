package com.outbrain.aletheia.datum;

import com.google.common.collect.Lists;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.FetchConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.InMemoryDatumEnvelopeFetcher;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.List;

/**
 * A {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory} for building {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher}s capable of
 * consuming data from endpoints of type {@link com.outbrain.aletheia.datum.consumption.ManualFeedConsumptionEndPoint}.
 */
public class InMemoryDatumEnvelopeFetcherFactory implements DatumEnvelopeFetcherFactory<FetchConsumptionEndPoint<byte[]>> {

  @Override
  public List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(final FetchConsumptionEndPoint<byte[]> consumptionEndPoint,
                                                              final MetricsFactory metricFactory) {
    // slevin: TODO handle raw string datums,
    // currently there's a hidden assumption payload is always AVRO
    return Lists.<DatumEnvelopeFetcher>newArrayList(new InMemoryDatumEnvelopeFetcher(consumptionEndPoint,
                                                                                     metricFactory));
  }
}
