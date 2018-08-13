package com.outbrain.aletheia.kafka.serialization;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope_old;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by irolnik on 9/5/17.
 */
public class AletheiaKafkaEnvelopeDeserializer implements Deserializer {

  public static final String ENVELOPE_WRITER_SCHEMA = "aletheia.envelope.kafka.writer.schema";
  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();
  private final Schema.Parser parser = new Schema.Parser();
  private Schema writerSchema = DatumEnvelope_old.getClassSchema();


  @Override
  public void configure(final Map configs, final boolean isKey) {
    if (configs.containsKey(ENVELOPE_WRITER_SCHEMA)) {
      writerSchema = parser.parse((String) configs.get(ENVELOPE_WRITER_SCHEMA));
    }
  }

  @Override
  public DatumEnvelope deserialize(final String topic, final byte[] data) {
    return avroDatumEnvelopeSerDe.deserializeDatumEnvelope(ByteBuffer.wrap(data), writerSchema);
  }

  @Override
  public void close() {

  }
}
