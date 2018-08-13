package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope_old;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Serializes a {@link DatumEnvelope} instance to an Avro encoded ByteBuffer.
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

  /**
   * @param buffer data
   * @param writer writer schema for the SpecificDatumReader, useful when producers may use different schema versions
   * @return deserialized object
   */
  public DatumEnvelope deserializeDatumEnvelope(final ByteBuffer buffer, final Schema writer) {
    try (final InputStream byteBufferInputStream = new ByteBufferInputStream(Collections.singletonList(buffer))) {
      // hack alert: using old envelope to reconcile version diffs
      final DatumReader<DatumEnvelope> datumReader = new SpecificDatumReader<>(writer, DatumEnvelope.getClassSchema());
      final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteBufferInputStream, null);
      return datumReader.read(null, decoder);
    } catch (final Exception e) {
      throw new RuntimeException("Could not deserialize datum envelope", e);
    }
  }

  public DatumEnvelope deserializeDatumEnvelope(final ByteBuffer buffer) {
    return deserializeDatumEnvelope(buffer, DatumEnvelope_old.getClassSchema());
  }
}
