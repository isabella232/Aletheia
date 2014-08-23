package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

public final class DatumEnvelopeUtils {

  private DatumEnvelopeUtils() {
  }

  public static String getDatumTypeId(final DatumEnvelope datumEnvelope) {
    return datumEnvelope.getDatumTypeId().toString();
  }
}
