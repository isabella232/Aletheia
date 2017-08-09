package com.outbrain.aletheia.kafka.serialization;

import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeBuilder;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.outbrain.aletheia.kafka.serialization.AletheiaSerdes.ALETHEIA_PRODUCER_INCARNATION;
import static com.outbrain.aletheia.kafka.serialization.AletheiaSerdes.ALETHEIA_PRODUCER_SOURCE;

/**
 * Implementation of a Kafka Serializer for serializing a datum.
 *
 * @param <TDomainClass> The type of Datum.
 */
public class AletheiaKafkaSerializer<TDomainClass> implements Serializer<TDomainClass> {

  private final Class<TDomainClass> datumClass;
  private final DatumSerDe<TDomainClass> datumSerDe;
  private final DatumKeySelector<TDomainClass> keySelector;

  private DatumEnvelopeBuilder<TDomainClass> envelopeBuilder;
  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  public AletheiaKafkaSerializer(final Class<TDomainClass> datumClass,
                                 final DatumSerDe<TDomainClass> datumSerDe,
                                 final DatumKeySelector<TDomainClass> keySelector) {
    this.datumClass = datumClass;
    this.datumSerDe = datumSerDe;
    this.keySelector = keySelector;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    final Integer incarnation = (Integer) configs.get(ALETHEIA_PRODUCER_INCARNATION);
    final String source = String.valueOf(configs.get(ALETHEIA_PRODUCER_SOURCE));
    envelopeBuilder = new DatumEnvelopeBuilder<>(datumClass, datumSerDe, keySelector, incarnation, source);
  }

  @Override
  public byte[] serialize(String topic, TDomainClass data) {
    final DatumEnvelope datumEnvelope = envelopeBuilder.buildEnvelope(data);
    final ByteBuffer serializedDatumEnvelope = avroDatumEnvelopeSerDe.serializeDatumEnvelope(datumEnvelope);
    return serializedDatumEnvelope.array();
  }

  @Override
  public void close() {

  }
}
