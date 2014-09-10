package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.nio.ByteBuffer;

/**
 * A <code>Sender</code> implementation that sends <code>DatumEnvelope</code> without manipulating them
 * in any way.
 */
public class RawDatumEnvelopeBinarySender implements NamedSender<DatumEnvelope> {

  private final AvroDatumEnvelopeSerDe datumEnvelopeSerializer = new AvroDatumEnvelopeSerDe();;
  private final NamedSender<ByteBuffer> binaryDataTransporter;

  public RawDatumEnvelopeBinarySender(final NamedSender<ByteBuffer> binaryDataTransporter) {
    this.binaryDataTransporter = binaryDataTransporter;
  }

  public RawDatumEnvelopeBinarySender(final Sender<ByteBuffer> binaryDataSender, final String name) {
    this.binaryDataTransporter = new NamedSender<ByteBuffer>() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public void send(final ByteBuffer byteBuffer) throws SilentSenderException {
        binaryDataSender.send(byteBuffer);
      }
    };
  }

  @Override
  public void send(final DatumEnvelope datumEnvelope) throws SilentSenderException {
    final ByteBuffer binaryDatumEnvelope = datumEnvelopeSerializer.serializeDatumEnvelope(datumEnvelope);
    binaryDataTransporter.send(binaryDatumEnvelope);
  }

  @Override
  public String getName() {
    return binaryDataTransporter.getName();
  }
}
