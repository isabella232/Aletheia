package com.outbrain.aletheia.datum.consumption;

/**
 * The base interface for a {@link DatumConsumer}, capable of providing clients with datum instances consumed
 * from a data source.
 *
 * @param <TDomainClass> The type of the datum to be consumed by this {@link DatumConsumer}.
 */
public interface DatumConsumer<TDomainClass> {

  /**
   * Returns an iterable data from some datum source, one at a time, and blocking if none is available.
   *
   * @return An {@link Iterable<TDomainClass>} that represents the incoming datum stream.
   */
  Iterable<TDomainClass> datums();
}
