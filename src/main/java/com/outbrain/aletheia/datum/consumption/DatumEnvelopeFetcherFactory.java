package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.List;

/**
 * A base interface to be implemented as part of adding a custom <code>ConsumptionEndPoint</code> type to Aletheia.
 * Should a consumption point of type <code>TConsumptionEndPoint</code> be provided,
 * an appropriate <code>DatumEnvelopeFetcherFactory</code> must be registered in order to fetch data from it.
 *
 * @param <TConsumptionEndPoint> The type of <code>ConsumptionEndPoint</code> this
 *                              <code>DatumEnvelopeFetcherFactory</code> can build fetchers for.
 */
public interface DatumEnvelopeFetcherFactory<TConsumptionEndPoint extends ConsumptionEndPoint> {

  /**
   * Builds <code>DatumEnvelopeFetcher</code> capable of fetching <code>DatumEnvelope</code>s from the given
   * consumption end point instance.
   *
   * @param consumptionEndPoint Describes the source to fetch <code>DatumEnvelope</code>s from.
   * @param metricFactory The metric factory to report metrics to.
   * @return A list of <code>DatumEnvelopeFetcher</code> that can be used to fetch <code>DatumEnvelopes</code>
   * from the source described by consumptionEndPoint. If the consumption point defines no parallelism
   * (i.e., parallelism = 1) the list will contain only one <code>DatumEnvelopeFetcher</code>,
   * if the parallelism level > 1, the returned list will contain multiple <code>DatumEnvelopeFetcher</code>,
   * that can be used in parallel to increase fetching throughput.
   */
  List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(TConsumptionEndPoint consumptionEndPoint,
                                                       MetricsFactory metricFactory);
}