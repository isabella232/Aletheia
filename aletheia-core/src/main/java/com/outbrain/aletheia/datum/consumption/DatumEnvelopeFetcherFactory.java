package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.List;

/**
 * A base interface to be implemented as part of adding a custom {@link ConsumptionEndPoint} type to Aletheia.
 * Should a consumption point of type {@link TConsumptionEndPoint} be provided,
 * an appropriate {@link DatumEnvelopeFetcherFactory} must be registered in order to fetch data from it.
 *
 * @param <TConsumptionEndPoint> The type of {@link ConsumptionEndPoint} this
 *                               {@link DatumEnvelopeFetcherFactory} can build fetchers for.
 */
public interface DatumEnvelopeFetcherFactory<TConsumptionEndPoint extends ConsumptionEndPoint> {

  /**
   * Builds {@link DatumEnvelopeFetcher} capable of fetching {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s from the given
   * consumption end point instance.
   *
   * @param consumptionEndPoint Describes the source to fetch {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s from.
   * @param metricFactory       The metric factory to report metrics to.
   * @return A list of {@link DatumEnvelopeFetcher} that can be used to fetch {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s
   * from the source described by consumptionEndPoint. If the consumption point defines no parallelism
   * (i.e., parallelism = 1) the list will contain only one {@link DatumEnvelopeFetcher},
   * if the parallelism level > 1, the returned list will contain multiple {@link DatumEnvelopeFetcher},
   * that can be used in parallel to increase fetching throughput.
   */
  List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(TConsumptionEndPoint consumptionEndPoint,
                                                       MetricsFactory metricFactory);
}