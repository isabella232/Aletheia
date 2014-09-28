package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

/**
 * The base interface for fetchers capable of fetching {@code DatumEnvelope}s from a data source.
 */
public interface DatumEnvelopeFetcher {

  /**
   * Returns an iterable of {@code DatumEnvelope}s from some source, one at a time, and blocking if none is available.
   *
   * @return An {@code Iterable<TDomainClass>} that represents the {@code DatumEnvelope} stream.
   */
  Iterable<DatumEnvelope> datumEnvelopes();
}
