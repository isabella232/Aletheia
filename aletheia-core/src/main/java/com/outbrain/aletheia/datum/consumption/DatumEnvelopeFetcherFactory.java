package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.List;

/**
 * A base interface to be implemented as part of adding a custom {@code ConsumptionEndPoint} type to Aletheia.
 * Should a consumption point of type {@code TConsumptionEndPoint} be provided,
 * an appropriate {@code DatumEnvelopeFetcherFactory} must be registered in order to fetch data from it.
 *
 * @param <TConsumptionEndPoint> The type of {@code ConsumptionEndPoint} this
 *                               {@code DatumEnvelopeFetcherFactory} can build fetchers for.
 */
public interface DatumEnvelopeFetcherFactory<TConsumptionEndPoint extends ConsumptionEndPoint> {

  /**
   * Builds {@code DatumEnvelopeFetcher} capable of fetching {@code DatumEnvelope}s from the given
   * consumption end point instance.
   *
   * @param consumptionEndPoint Describes the source to fetch {@code DatumEnvelope}s from.
   * @param metricFactory       The metric factory to report metrics to.
   * @return A list of {@code DatumEnvelopeFetcher} that can be used to fetch {@code DatumEnvelopes}
   * from the source described by consumptionEndPoint. If the consumption point defines no parallelism
   * (i.e., parallelism = 1) the list will contain only one {@code DatumEnvelopeFetcher},
   * if the parallelism level > 1, the returned list will contain multiple {@code DatumEnvelopeFetcher},
   * that can be used in parallel to increase fetching throughput.
   */
  List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(TConsumptionEndPoint consumptionEndPoint,
                                                       MetricsFactory metricFactory);
}