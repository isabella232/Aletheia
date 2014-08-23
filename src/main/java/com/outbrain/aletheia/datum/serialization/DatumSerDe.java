package com.outbrain.aletheia.datum.serialization;

/**
 * Created by slevin on 7/16/14.
 */
public interface DatumSerDe<TDomainClass> {
  SerializedDatum serializeDatum(TDomainClass domainObject);
  TDomainClass deserializeDatum(SerializedDatum serializedDatum);
}
