package com.outbrain.aletheia.datum.serialization;

import com.outbrain.aletheia.datum.serialization.avro.AvroRoundTripProjector;
import com.outbrain.aletheia.datum.avro.test_domain_class;
import com.outbrain.aletheia.datum.type.SampleDomainClass;
import org.apache.avro.specific.SpecificRecord;
import org.joda.time.Instant;

/**
 * Created by slevin on 7/27/14.
 */
public class SampleDomainClassAvroRoundTripProjector implements AvroRoundTripProjector<SampleDomainClass> {

  @Override
  public SpecificRecord toAvro(final SampleDomainClass domainObject) {
    return test_domain_class.newBuilder()
                            .setId(domainObject.getId())
                            .setMyNumber(domainObject.getMyNumber())
                            .setMyString(domainObject.getMyString())
                            .setEventTimestamp(domainObject.getEventTimestamp().getMillis())
                            .setShouldBeSent(domainObject.isDiscarded())
                            .build();
  }

  @Override
  public SampleDomainClass fromAvro(final SpecificRecord domainObject) {
    final test_domain_class avroObject = (test_domain_class) domainObject;
    return new SampleDomainClass(avroObject.getId(),
                                 avroObject.getMyNumber(),
                                 avroObject.getMyString().toString(),
                                 new Instant(avroObject.getEventTimestamp()),
                                 avroObject.getShouldBeSent());
  }
}
