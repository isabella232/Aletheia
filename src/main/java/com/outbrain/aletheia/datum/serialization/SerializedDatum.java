package com.outbrain.aletheia.datum.serialization;

import java.nio.ByteBuffer;

public final class SerializedDatum {

  private final ByteBuffer payload;
  private final DatumTypeVersion datumTypeVersion;

  public SerializedDatum(final ByteBuffer payload, final DatumTypeVersion datumTypeVersion) {
    this.payload = payload;
    this.datumTypeVersion = datumTypeVersion;
  }

  public ByteBuffer getPayload() {
    return payload;
  }

  public DatumTypeVersion getDatumTypeVersion() {
    return datumTypeVersion;
  }


}
