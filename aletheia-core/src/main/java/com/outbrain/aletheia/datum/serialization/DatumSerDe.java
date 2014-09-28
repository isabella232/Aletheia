package com.outbrain.aletheia.datum.serialization;

/**
 * A base interface for serializing and deserializing a datum.
 *
 * @param <TDomainClass> The type of Datum this {@code DatumSerDe} will be serializing and deserializing.
 */
public interface DatumSerDe<TDomainClass> {
  SerializedDatum serializeDatum(TDomainClass domainObject);

  TDomainClass deserializeDatum(SerializedDatum serializedDatum);
}
