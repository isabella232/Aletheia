package com.outbrain.aletheia.kafka.serialization;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by irolnik on 9/5/17.
 */
public class AletheiaKafkaEnvelopeSerializer implements Serializer {

  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();
  @Override
  public void configure(final Map configs, final boolean isKey) {

  }

  @Override
  public byte[] serialize(final String topic, final Object datumEnvelope) {
    final ByteBuffer serializedDatumEnvelope = avroDatumEnvelopeSerDe.serializeDatumEnvelope((DatumEnvelope) datumEnvelope);
    final byte[] array = serializedDatumEnvelope.array();
    return array;
  }

  @Override
  public void close() {

  }
}
