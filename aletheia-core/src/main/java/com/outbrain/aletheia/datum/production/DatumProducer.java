package com.outbrain.aletheia.datum.production;

/**
 * The base interface for a <code>DatumConsumer</code>.
 *
 * @param <TDomainClass> The type of the datum to be consumed by this <code>DatumConsumer</code>.
 */
public interface DatumProducer<TDomainClass> {
  /**
   * Delivers a datum to a destination.
   *
   * @param datum The datum instance to deliver.
   */
  void deliver(TDomainClass datum);
}
