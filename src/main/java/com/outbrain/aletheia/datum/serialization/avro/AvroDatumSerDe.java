package com.outbrain.aletheia.datum.serialization.avro;

import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.datum.serialization.avro.schema.DatumSchemaRepository;
import com.outbrain.aletheia.datum.DatumUtils;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * A default implementation for an Avro based datum serialization.
 *
 * @param <TDomainClass> The type of the datum to be serialized.
 */
public class AvroDatumSerDe<TDomainClass> implements DatumSerDe<TDomainClass> {

  protected final AvroRoundTripProjector<TDomainClass> avroRoundTripProjector;
  protected DatumSchemaRepository datumSchemaRepository;

  public AvroDatumSerDe(final AvroRoundTripProjector<TDomainClass> avroRoundTripProjector,
                        final DatumSchemaRepository datumSchemaRepository) {

    this.avroRoundTripProjector = avroRoundTripProjector;
    this.datumSchemaRepository = datumSchemaRepository;
  }

  public SerializedDatum serializeDatum(final TDomainClass domainObject) {

    try {
      final SpecificRecord record = avroRoundTripProjector.toAvro(domainObject);
      final Schema schema = record.getSchema();
      final SpecificDatumWriter bodyWriter = new SpecificDatumWriter(schema);

      final ByteArrayOutputStream bodyByteStream = new ByteArrayOutputStream();
      final BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bodyByteStream, null);

      bodyWriter.write(record, binaryEncoder);
      binaryEncoder.flush();
      bodyByteStream.flush();

      final ByteBuffer datumBody = ByteBuffer.wrap(bodyByteStream.toByteArray());

      bodyByteStream.close();

      final int datumSchemaVersion = datumSchemaRepository.getDatumTypeVersion(schema).getVersion();

      return new SerializedDatum(datumBody,
                                 new DatumTypeVersion(DatumUtils.getDatumTypeId(domainObject.getClass()),
                                                          datumSchemaVersion));

    } catch (final Exception e) {
      throw new RuntimeException("Could not create datum body", e);
    }
  }

  public TDomainClass deserializeDatum(final SerializedDatum serializedDatum) {

    try {

      final DatumTypeVersion datumTypeVersion = serializedDatum.getDatumTypeVersion();

      final Schema incomingDatumSchema = datumSchemaRepository.getSchema(datumTypeVersion);

      final Schema repositoryLatestDatumSchema =
              datumSchemaRepository.getLatestSchema(datumTypeVersion.getDatumTypeId());

      final DatumReader<? extends SpecificRecord> datumReader =
              new SpecificDatumReader<>(incomingDatumSchema, repositoryLatestDatumSchema);

      final InputStream byteBufferInputStream =
              new ByteBufferInputStream(Collections.singletonList(serializedDatum.getPayload()));

      final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteBufferInputStream, null);

      final SpecificRecord record = datumReader.read(null, decoder);
      byteBufferInputStream.close();
      return avroRoundTripProjector.fromAvro(record);
    } catch (final IOException e) {
      throw new RuntimeException("Could not deserialize versioned payload to domain object", e);
    }
  }
}
