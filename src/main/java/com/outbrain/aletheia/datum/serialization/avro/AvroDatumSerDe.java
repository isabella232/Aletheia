package com.outbrain.aletheia.datum.serialization.avro;

import com.outbrain.aletheia.datum.serialization.avro.schema.DatumSchemaRepository;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.datum.serialization.VersionedDatumTypeId;
import com.outbrain.aletheia.datum.utils.DatumUtils;
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

      // RLRL Reuse encoder
      // RLRL use buffered encoder
      final BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bodyByteStream, null);

      bodyWriter.write(record, binaryEncoder);
      binaryEncoder.flush();
      bodyByteStream.flush();

      // RLRL Prevent double copy
      final ByteBuffer datumBody = ByteBuffer.wrap(bodyByteStream.toByteArray());

      bodyByteStream.close();

      final int datumSchemaVersion = datumSchemaRepository.retrieveSchemaVersion(schema).getVersion();

      return new SerializedDatum(datumBody,
                                 new VersionedDatumTypeId(DatumUtils.getDatumTypeId(domainObject.getClass()),
                                                          datumSchemaVersion));

    } catch (final Exception e) {
      throw new RuntimeException("Could not create datum body", e);
    }
  }

  public TDomainClass deserializeDatum(final SerializedDatum serializedDatum) {

    try {

      final VersionedDatumTypeId versionedDatumTypeId = serializedDatum.getVersionedDatumTypeId();

      final Schema incomingDatumSchema = datumSchemaRepository.retrieveSchema(versionedDatumTypeId);
      final Schema repositoryLatestDatumSchema =
              datumSchemaRepository.retrieveLatestSchema(versionedDatumTypeId.getDatumTypeId());

      final DatumReader<? extends SpecificRecord> datumReader = new SpecificDatumReader<SpecificRecord>(
              incomingDatumSchema,
              repositoryLatestDatumSchema);

      final InputStream byteBufferInputStream =
              new ByteBufferInputStream(Collections.singletonList(serializedDatum.getPayload()));

      // RLRL See if we can reuse the decoder
      final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteBufferInputStream, null);

      final SpecificRecord record = datumReader.read(null, decoder);
      byteBufferInputStream.close();
      return avroRoundTripProjector.fromAvro(record);
    } catch (final IOException e) {
      throw new RuntimeException("Could not deserialize versioned payload to domain object", e);
    }
  }
}
