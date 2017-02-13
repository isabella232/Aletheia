package com.outbrain.aletheia.datum.consumption;

/**
 * The base interface for a {@link DatumConsumerStream}, capable of providing clients with datum instances consumed
 * from a data source.
 *
 * @param <TDomainClass> The type of the datum to be consumed by this {@link DatumConsumerStream}.
 */
public interface DatumConsumerStream<TDomainClass> {

  /**
   * Returns an iterable data from some datum source, one at a time, and blocking if none is available.
   *
   * @return An {@link Iterable<TDomainClass>} that represents the incoming datum stream.
   */
  Iterable<TDomainClass> datums();

  /**
   * Commits consumed offsets to Kafka, blocks until commit either succeeds or an error is encountered
   */
  void commitConsumedOffsets();
}
