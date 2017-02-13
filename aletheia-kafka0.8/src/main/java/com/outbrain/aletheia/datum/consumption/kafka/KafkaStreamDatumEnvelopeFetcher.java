package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import kafka.consumer.KafkaStream;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher} implementation capable of fetching
 * {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s from a Kafka stream.
 */
class KafkaStreamDatumEnvelopeFetcher implements DatumEnvelopeFetcher {

  private final Iterable<DatumEnvelope> datumEnvelopeIterable =
          new Iterable<DatumEnvelope>() {
            @Override
            public Iterator<DatumEnvelope> iterator() {
              return new Iterator<DatumEnvelope>() {
                @Override
                public boolean hasNext() {
                  return kafkaMessageStream.iterator().hasNext();
                }

                @Override
                public DatumEnvelope next() {
                  final byte[] message = kafkaMessageStream.iterator().next().message();
                  return avroDatumEnvelopeSerDe.deserializeDatumEnvelope(ByteBuffer.wrap(message));
                }

                @Override
                public void remove() {

                }
              };
            }
          };

  private final KafkaStream<byte[], byte[]> kafkaMessageStream;
  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  public KafkaStreamDatumEnvelopeFetcher(final KafkaStream<byte[], byte[]> kafkaMessageStream) {
    this.kafkaMessageStream = kafkaMessageStream;
  }

  @Override
  public Iterable<DatumEnvelope> datumEnvelopes() {
    return datumEnvelopeIterable;
  }

  @Override
  public void commitConsumedOffsets() {
    throw new UnsupportedOperationException("Offset management is not supported in Kafka 0.8");
  }
}
