package com.outbrain.aletheia.datum.production;

/**
 * The base interface for a {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream}.
 *
 * @param <TDomainClass> The type of the datum to be consumed by this {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream}.
 */
public interface DatumProducer<TDomainClass> {
  /**
   * Delivers a datum to a destination.
   *
   * @param datum The datum instance to deliver.
   */
  void deliver(TDomainClass datum);

  /**
   * Delivers a datum to a destination and invokes the provided callback when delivery was acknowledged by the endpoint sender.
   * Callback can be invoked several times per datum - for every endpoint that supports async callbacks. Callbacks will be ignored for synchronous endpoints.
   * For more information about callback support in enpoints types - please check the relevant {@link com.outbrain.aletheia.datum.production.Sender} implementation.
   *
   * @param datum The datum instance to deliver.
   * @param deliveryCallbackPerEndpoint The callback that will be invoked upon every completed delivery to an endpoint.
   *
   */
  void deliver(TDomainClass datum, DeliveryCallback deliveryCallbackPerEndpoint);
}
