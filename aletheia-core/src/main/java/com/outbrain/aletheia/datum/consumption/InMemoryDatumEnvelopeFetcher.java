package com.outbrain.aletheia.datum.consumption;

import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by slevin on 8/15/14.
 */
public class InMemoryDatumEnvelopeFetcher implements DatumEnvelopeFetcher {

  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  private final Iterable<DatumEnvelope> datumEnvelopeIterable =
          () -> new Iterator<DatumEnvelope>() {
            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public DatumEnvelope next() {
              return fetchDatumEnvelope();
            }

            @Override
            public void remove() {
            }
          };

  private final FetchConsumptionEndPoint<byte[]> consumptionEndPoint;
  private final Counter receivedDatumEnvelopeCount;
  private final Counter failureCount;

  public InMemoryDatumEnvelopeFetcher(final FetchConsumptionEndPoint<byte[]> consumptionEndPoint,
                                      final MetricsFactory metricFactory) {

    this.consumptionEndPoint = consumptionEndPoint;

    receivedDatumEnvelopeCount = metricFactory.createCounter("Receive_Attempts_Success", "Number of successful attempts to fetch data");
    failureCount = metricFactory.createCounter("Receive_Attempts_Failure", "Number of failed attempts to fetch envelope");
  }

  private DatumEnvelope fetchDatumEnvelope() {
    try {
      final ByteBuffer datumEnvelopeByteBuffer = ByteBuffer.wrap(consumptionEndPoint.fetch());
      final DatumEnvelope datumEnvelope = avroDatumEnvelopeSerDe.deserializeDatumEnvelope(datumEnvelopeByteBuffer);

      receivedDatumEnvelopeCount.inc();

      return datumEnvelope;
    } catch (final Exception e) {
      failureCount.inc();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<DatumEnvelope> datumEnvelopes() {
    return datumEnvelopeIterable;
  }

  @Override
  public void commitConsumedOffsets() {
    // Empty implementation for in memory fetcher
  }

  @Override
  public void close() {
  }
}
