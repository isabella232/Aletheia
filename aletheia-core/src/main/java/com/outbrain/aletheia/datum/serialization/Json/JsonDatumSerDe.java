package com.outbrain.aletheia.datum.serialization.Json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.DatumTypeVersion;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;

import java.nio.ByteBuffer;

/**
 * A default implementation for a Json based datum serialization.
 *
 * @param <TDomainClass> The type of the datum to be serialized.
 */
public class JsonDatumSerDe<TDomainClass> implements DatumSerDe<TDomainClass> {

  public static final String UTF_8 = "UTF-8";

  private static final int VERSION = 1;
  private final ObjectMapper jsonSerDe;
  private final Class<TDomainClass> datumClass;

  public JsonDatumSerDe(final Class<TDomainClass> datumClass) {
    this.datumClass = datumClass;
    jsonSerDe = new ObjectMapper();
    jsonSerDe.registerModule(new JodaModule());
  }

  @Override
  public SerializedDatum serializeDatum(final TDomainClass datum) {
    final byte[] bytes;
    try {
      bytes = jsonSerDe.writeValueAsBytes(datum);
      return new SerializedDatum(ByteBuffer.wrap(bytes),
                                 new DatumTypeVersion(DatumUtils.getDatumTypeId(datumClass), VERSION));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TDomainClass deserializeDatum(final SerializedDatum serializedDatum) {
    try {
      final byte[] datumJsonStringBytes = new byte[serializedDatum.getPayload().remaining()];
      serializedDatum.getPayload().get(datumJsonStringBytes);
      final String datumJsonString = new String(datumJsonStringBytes, UTF_8);
      return jsonSerDe.readValue(datumJsonString, datumClass);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
