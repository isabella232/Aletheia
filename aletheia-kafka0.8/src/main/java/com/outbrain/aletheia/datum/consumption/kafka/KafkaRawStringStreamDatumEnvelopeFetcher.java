package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import kafka.consumer.KafkaStream;
import org.joda.time.Instant;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
* Created by slevin on 11/17/14.
*/
class KafkaRawStringStreamDatumEnvelopeFetcher implements DatumEnvelopeFetcher {

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
                  return new DatumEnvelope("RawStringDatum",
                                           -1,
                                           Instant.now().getMillis(),
                                           -1,
                                           "",
                                           Instant.now().getMillis(),
                                           ByteBuffer.wrap(message),
                                           null,
                                           null);
                }

                @Override
                public void remove() {

                }
              };
            }
          };

  private final KafkaStream<byte[], byte[]> kafkaMessageStream;

  public KafkaRawStringStreamDatumEnvelopeFetcher(final KafkaStream<byte[], byte[]> kafkaMessageStream) {
    this.kafkaMessageStream = kafkaMessageStream;
  }

  @Override
  public Iterable<DatumEnvelope> datumEnvelopes() {
    return datumEnvelopeIterable;
  }
}
