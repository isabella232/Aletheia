package com.outbrain.aletheia.datum.serialization;

import com.outbrain.aletheia.datum.type.SampleDomainClass;
import com.outbrain.aletheia.datum.utils.DatumUtils;
import org.joda.time.Instant;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by slevin on 7/22/14.
 */
public class SampleClassStringSerDe implements DatumSerDe<SampleDomainClass> {

  public static final String UTF_8 = "UTF-8";

  private static final int WHATEVER = 0;


  @Override
  public SerializedDatum serializeDatum(final SampleDomainClass domainObject) {
    try {
      final byte[] payload = domainObject.getDomainObjectAsString().getBytes(UTF_8);
      return new SerializedDatum(ByteBuffer.wrap(payload),
                                 new VersionedDatumTypeId(DatumUtils.getDatumTypeId(SampleDomainClass.class),
                                                          WHATEVER));
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public SampleDomainClass deserializeDatum(final SerializedDatum serializedDatum) {
    try {
      final byte[] array = new byte[serializedDatum.getPayload().remaining()];
      serializedDatum.getPayload().get(array);
      final String tsv = new String(array, UTF_8);
      final String[] split = tsv.split("\t");
      return new SampleDomainClass(Integer.parseInt(split[0]),
                                   Double.parseDouble(split[1]),
                                   split[2],
                                   Instant.parse(split[3]),
                                   Boolean.parseBoolean(split[4]));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

  }
}
