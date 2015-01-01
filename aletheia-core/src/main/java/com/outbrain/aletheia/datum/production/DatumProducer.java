package com.outbrain.aletheia.datum.production;

/**
 * The base interface for a {@link com.outbrain.aletheia.datum.consumption.DatumConsumer}.
 *
 * @param <TDomainClass> The type of the datum to be consumed by this {@link com.outbrain.aletheia.datum.consumption.DatumConsumer}.
 */
public interface DatumProducer<TDomainClass> {
  /**
   * Delivers a datum to a destination.
   *
   * @param datum The datum instance to deliver.
   */
  void deliver(TDomainClass datum);
}
