package com.outbrain.aletheia.datum.serialization.avro;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.avro.specific.SpecificRecord;

@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS)
public interface AvroRoundTripProjector<TDomainClass> {

  SpecificRecord toAvro(final TDomainClass element);

  TDomainClass fromAvro(final SpecificRecord element);
}
