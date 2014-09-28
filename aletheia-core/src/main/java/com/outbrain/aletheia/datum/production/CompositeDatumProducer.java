package com.outbrain.aletheia.datum.production;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Combines multiple {@code DatumProducer} by creating a new one that upon a request to deliver a datum,
 * delivers it using all the internal {@code DatumProducer}s is a sequential order (the order they were passed in).
 */
public class CompositeDatumProducer<TDomainClass> implements DatumProducer<TDomainClass> {

  private final List<DatumProducer<TDomainClass>> datumProducers;

  @SafeVarargs
  public CompositeDatumProducer(final DatumProducer<TDomainClass>... datumProducers) {
    this(Lists.newArrayList(datumProducers));
  }

  public CompositeDatumProducer(final List<DatumProducer<TDomainClass>> datumProducers) {
    this.datumProducers = datumProducers;
  }

  @Override
  public void deliver(final TDomainClass datum) {
    for (final DatumProducer<TDomainClass> datumProducer : datumProducers) {
      datumProducer.deliver(datum);
    }
  }
}
