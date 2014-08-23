package com.outbrain.aletheia.breadcrumbs;

import com.google.gson.Gson;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.SerializedDatum;
import com.outbrain.aletheia.datum.serialization.VersionedDatumTypeId;
import com.outbrain.aletheia.datum.utils.DatumUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
* Created by slevin on 8/11/14.
*/
public class BreadcrumbDatumSerDe implements DatumSerDe<Breadcrumb> {

  public static final String UTF_8 = "UTF-8";
  Gson gson = new Gson();

  @Override
  public SerializedDatum serializeDatum(final Breadcrumb breadcrumb) {
    final byte[] bytes;
    try {
      bytes = gson.toJson(breadcrumb).getBytes(UTF_8);
      return new SerializedDatum(ByteBuffer.wrap(bytes),
                                 new VersionedDatumTypeId(DatumUtils.getDatumTypeId(Breadcrumb.class),
                                                          1));
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Breadcrumb deserializeDatum(final SerializedDatum serializedDatum) {
    try {
      final byte[] breadcrumbStringBytes = new byte[serializedDatum.getPayload().remaining()];
      serializedDatum.getPayload().get(breadcrumbStringBytes);
      final String breadcrumbString = new String(breadcrumbStringBytes, UTF_8);
      return gson.fromJson(breadcrumbString, Breadcrumb.class);
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
