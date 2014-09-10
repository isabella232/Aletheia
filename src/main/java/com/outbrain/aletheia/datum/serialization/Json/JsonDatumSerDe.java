package com.outbrain.aletheia.datum.serialization.Json;

import com.google.gson.Gson;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.datum.serialization.VersionedDatumTypeId;
import com.outbrain.aletheia.datum.utils.DatumUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * A default implementation for a Json based datum serialization.
 *
 * @param <TDomainClass> The type of the datum to be serialized.
 */
public class JsonDatumSerDe<TDomainClass> implements DatumSerDe<TDomainClass> {

  public static final String UTF_8 = "UTF-8";
  private final Gson gson = new Gson();
  private final Class<TDomainClass> datumClass;

  public JsonDatumSerDe(final Class<TDomainClass> datumClass) {
    this.datumClass = datumClass;
  }

  @Override
  public SerializedDatum serializeDatum(final TDomainClass datum) {
    final byte[] bytes;
    try {
      bytes = gson.toJson(datum).getBytes(UTF_8);
      return new SerializedDatum(ByteBuffer.wrap(bytes),
                                 new VersionedDatumTypeId(DatumUtils.getDatumTypeId(Breadcrumb.class), 1));
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TDomainClass deserializeDatum(final SerializedDatum serializedDatum) {
    try {
      final byte[] breadcrumbStringBytes = new byte[serializedDatum.getPayload().remaining()];
      serializedDatum.getPayload().get(breadcrumbStringBytes);
      final String breadcrumbString = new String(breadcrumbStringBytes, UTF_8);
      return gson.fromJson(breadcrumbString, datumClass);
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
