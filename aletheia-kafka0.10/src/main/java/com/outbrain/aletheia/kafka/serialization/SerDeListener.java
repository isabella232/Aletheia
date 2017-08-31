package com.outbrain.aletheia.kafka.serialization;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

/**
 * A Listener for serializing/deserializing events. Provides access to DatumEnvelope.
 *
 * @param <TDomainClass> The type of Datum.
 */
public interface SerDeListener<TDomainClass> {

  SerDeListener EMPTY = new SerDeListener() {
    @Override
    public void onSerialize(String topic, Object datum, DatumEnvelope datumEnvelope, long dataSizeBytes) {

    }

    @Override
    public void onDeserialize(String topic, Object datum, DatumEnvelope datumEnvelope, long dataSizeBytes) {

    }
  };

  void onSerialize(final String topic,
                   final TDomainClass datum,
                   final DatumEnvelope datumEnvelope,
                   final long dataSizeBytes);

  void onDeserialize(final String topic,
                     final TDomainClass datum,
                     final DatumEnvelope datumEnvelope,
                     final long dataSizeBytes);
}
