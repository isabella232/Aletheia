package com.outbrain.aletheia.datum.serialization.avro;

import org.apache.avro.specific.SpecificRecord;

public interface AvroRoundTripProjector<TDomainClass> {

  public SpecificRecord toAvro(final TDomainClass element);

  public TDomainClass fromAvro(final SpecificRecord element);
}
