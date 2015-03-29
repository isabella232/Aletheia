package com.outbrain.aletheia.datum.serialization.avro;

import org.apache.avro.specific.SpecificRecord;

public interface AvroRoundTripProjector<TDomainClass> {

  SpecificRecord toAvro(final TDomainClass element);

  TDomainClass fromAvro(final SpecificRecord element);
}
