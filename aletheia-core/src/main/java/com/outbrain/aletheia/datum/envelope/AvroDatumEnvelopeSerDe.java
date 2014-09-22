package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Serializes a <code>DatumEnvelope</code> instance to an Avro encoded ByteBuffer.
 */
public class AvroDatumEnvelopeSerDe {

    public ByteBuffer serializeDatumEnvelope(final DatumEnvelope envelope) {

    try {
      final SpecificDatumWriter<DatumEnvelope> envelopeWriter = new SpecificDatumWriter<>(envelope.getSchema());
      final ByteArrayOutputStream envelopeByteStream = new ByteArrayOutputStream();
      BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(envelopeByteStream, null);
      binaryEncoder = EncoderFactory.get().directBinaryEncoder(envelopeByteStream, binaryEncoder);

      envelopeWriter.write(envelope, binaryEncoder);
      binaryEncoder.flush();
      envelopeByteStream.flush();

      return ByteBuffer.wrap(envelopeByteStream.toByteArray());
    } catch (final Exception e) {
      throw new RuntimeException("Could not serialize datum envelope", e);
    }

  }

  public DatumEnvelope deserializeDatumEnvelope(final ByteBuffer buffer) {

    final DatumReader<DatumEnvelope> datumReader = SpecificData.get().createDatumReader(DatumEnvelope.getClassSchema());

    final InputStream byteBufferInputStream = new ByteBufferInputStream(Collections.singletonList(buffer));
    final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteBufferInputStream, null);

    try {
      final DatumEnvelope envelope = datumReader.read(null, decoder);
      byteBufferInputStream.close();
      return envelope;
    } catch (final IOException e) {
      throw new RuntimeException("Could not decode datum envelope", e);
    }
  }
}
