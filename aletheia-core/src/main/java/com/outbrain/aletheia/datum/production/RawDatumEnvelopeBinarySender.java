package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.nio.ByteBuffer;

/**
 * A {@link Sender} implementation that sends {@link DatumEnvelope} without manipulating them
 * in any way.
 */
public class RawDatumEnvelopeBinarySender implements NamedSender<DatumEnvelope> {

  private final AvroDatumEnvelopeSerDe datumEnvelopeSerializer = new AvroDatumEnvelopeSerDe();

  private final DatumKeyAwareNamedSender<ByteBuffer> binaryDataTransporter;

  public RawDatumEnvelopeBinarySender(final DatumKeyAwareNamedSender<ByteBuffer> binaryDataTransporter) {
    this.binaryDataTransporter = binaryDataTransporter;
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope) throws SilentSenderException {
    final ByteBuffer binaryDatumEnvelope = datumEnvelopeSerializer.serializeDatumEnvelope(datumEnvelope);
    final String key = datumEnvelope.getDatumKey() != null ? datumEnvelope.getDatumKey().toString() : null;
    binaryDataTransporter.send(binaryDatumEnvelope, key);
  }

  @Override
  public String getName() {
    return binaryDataTransporter.getName();
  }
}
