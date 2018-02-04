package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.nio.ByteBuffer;

/**
 * A {@link Sender} implementation that serializes {@link DatumEnvelope} into Avro format and sends it using
 * a provided binary {@link Sender}.
 * Deliver with callback api support depends on whether the provided binarySender supports the api.
 */
public class AvroDatumEnvelopeSender implements NamedSender<DatumEnvelope> {

  private final AvroDatumEnvelopeSerDe datumEnvelopeSerializer = new AvroDatumEnvelopeSerDe();

  private final DatumKeyAwareNamedSender<byte[]> binarySender;

  @Override
  public void close() throws Exception {
    binarySender.close();
  }

  public AvroDatumEnvelopeSender(final DatumKeyAwareNamedSender<byte[]> binarySender) {
    this.binarySender = binarySender;
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope) throws SilentSenderException {
    send(datumEnvelope, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope, final DeliveryCallback deliveryCallback) throws SilentSenderException {
    final ByteBuffer serializedDatumEnvelope = datumEnvelopeSerializer.serializeDatumEnvelope(datumEnvelope);
    final String key = datumEnvelope.getDatumKey() != null ? datumEnvelope.getDatumKey().toString() : null;
    binarySender.send(serializedDatumEnvelope.array(), key, deliveryCallback);
  }

  @Override
  public String getName() {
    return binarySender.getName();
  }
}
