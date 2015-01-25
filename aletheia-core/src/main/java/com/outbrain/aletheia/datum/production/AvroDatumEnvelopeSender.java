package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.nio.ByteBuffer;

/**
 * A {@link Sender} implementation that serializes {@link DatumEnvelope} into Avro format and sends it using
 * a provided binary {@link Sender}.
 */
public class AvroDatumEnvelopeSender implements NamedSender<DatumEnvelope> {

  private final AvroDatumEnvelopeSerDe datumEnvelopeSerializer = new AvroDatumEnvelopeSerDe();

  private final DatumKeyAwareNamedSender<byte[]> binarySender;

  public AvroDatumEnvelopeSender(final DatumKeyAwareNamedSender<byte[]> binarySender) {
    this.binarySender = binarySender;
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope) throws SilentSenderException {
    final ByteBuffer serializedDatumEnvelope = datumEnvelopeSerializer.serializeDatumEnvelope(datumEnvelope);
    final String key = datumEnvelope.getDatumKey() != null ? datumEnvelope.getDatumKey().toString() : null;
    binarySender.send(serializedDatumEnvelope.array(), key);
  }

  @Override
  public String getName() {
    return binarySender.getName();
  }
}
