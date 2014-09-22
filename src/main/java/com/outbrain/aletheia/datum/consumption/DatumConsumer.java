package com.outbrain.aletheia.datum.consumption;

/**
 * The base interface for a <code>DatumConsumer</code>, capable of providing clients with datum instances consumed
 * from a data source.
 *
 * @param <TDomainClass> The type of the datum to be consumed by this <code>DatumConsumer</code>.
 */
public interface DatumConsumer<TDomainClass> {

  /**
   * Returns an iterable data from some datum source, one at a time, and blocking if none is available.
   *
   * @return An <code>Iterable<TDomainClass></code> that represents the incoming datum stream.
   */
  Iterable<TDomainClass> datums();
}
