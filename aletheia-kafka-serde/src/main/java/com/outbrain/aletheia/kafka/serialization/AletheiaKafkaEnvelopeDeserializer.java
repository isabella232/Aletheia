package com.outbrain.aletheia.kafka.serialization;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by irolnik on 9/5/17.
 */
public class AletheiaKafkaEnvelopeDeserializer implements Deserializer {

  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  @Override
  public void configure(final Map configs, final boolean isKey) {

  }

  @Override
  public DatumEnvelope deserialize(final String topic, final byte[] data) {
    return avroDatumEnvelopeSerDe.deserializeDatumEnvelope(ByteBuffer.wrap(data));
  }

  @Override
  public void close() {

  }
}
