package com.outbrain.aletheia.datum.serialization;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A base interface for serializing and deserializing a datum.
 *
 * @param <TDomainClass> The type of Datum this {@link DatumSerDe} will be serializing and deserializing.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface DatumSerDe<TDomainClass> {
  SerializedDatum serializeDatum(TDomainClass domainObject);

  TDomainClass deserializeDatum(SerializedDatum serializedDatum);
}
