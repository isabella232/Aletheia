package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

/**
 * The base interface for fetchers capable of fetching {@link DatumEnvelope}s from a data source.
 */
public interface DatumEnvelopeFetcher {

  /**
   * Returns an iterable of {@link DatumEnvelope}s from some source, one at a time, and blocking if none is available.
   *
   * @return An {@link Iterable<DatumEnvelope>} that represents the {@link DatumEnvelope} stream.
   */
  Iterable<DatumEnvelope> datumEnvelopes();
}
