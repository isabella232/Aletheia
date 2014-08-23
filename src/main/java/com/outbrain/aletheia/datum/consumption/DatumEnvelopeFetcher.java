package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

/**
 * Created by slevin on 8/15/14.
 */
public interface DatumEnvelopeFetcher {
  Iterable<DatumEnvelope> datumEnvelopes();
}
