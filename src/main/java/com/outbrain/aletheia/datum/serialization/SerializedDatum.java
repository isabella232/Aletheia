package com.outbrain.aletheia.datum.serialization;

import java.nio.ByteBuffer;

public final class SerializedDatum {

  private final ByteBuffer payload;
  private final VersionedDatumTypeId versionedDatumTypeId;

  public SerializedDatum(final ByteBuffer payload, final VersionedDatumTypeId versionedDatumTypeId) {
    this.payload = payload;
    this.versionedDatumTypeId = versionedDatumTypeId;
  }

  public ByteBuffer getPayload() {
    return payload;
  }

  public VersionedDatumTypeId getVersionedDatumTypeId() {
    return versionedDatumTypeId;
  }


}
