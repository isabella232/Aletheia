package com.outbrain.aletheia.datum.envelope;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Serializes a {@link DatumEnvelope} instance to an Avro encoded ByteBuffer.
 */
public class AvroDatumEnvelopeSerDe {

    protected Map<Integer, Schema> schemas = ImmutableMap.of(
            1, DatumEnvelope_old.getClassSchema(),
            2, DatumEnvelope.getClassSchema()

    );


    public ByteBuffer serializeDatumEnvelope(final DatumEnvelope envelope) {
        try {
            final SpecificDatumWriter<DatumEnvelope> envelopeWriter = new SpecificDatumWriter<>(envelope.getSchema());
            final ByteArrayOutputStream envelopeByteStream = new ByteArrayOutputStream();

            addSchemaVersionToStream(envelopeByteStream, envelope);

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

        Schema writerSchema = getWriterSchema(buffer);

        try (final InputStream byteBufferInputStream = new ByteBufferInputStream(Collections.singletonList(buffer))) {
            final DatumReader<DatumEnvelope> datumReader = new SpecificDatumReader<>(
                    writerSchema,
                    DatumEnvelope.getClassSchema());
            final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteBufferInputStream, null);
            return datumReader.read(null, decoder);

        } catch (final Exception e) {
            throw new RuntimeException("Could not deserialize datum envelope", e);
        }
    }


    protected Schema getWriterSchema(ByteBuffer buffer) {

        int version = buffer.getInt();
        if (version > 1 && version < 100 && schemas.containsKey(version)) {
            return schemas.get(version);
        } else {
            buffer.rewind();
            return schemas.get(1);
        }
    }

    protected void addSchemaVersionToStream(ByteArrayOutputStream envelopeByteStream, DatumEnvelope envelope) throws IOException {
        envelopeByteStream.write(Ints.toByteArray(envelope.getEnvelopeVersion()));
    }
}
