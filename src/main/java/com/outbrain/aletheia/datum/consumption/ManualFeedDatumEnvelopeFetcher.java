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
public class ManualFeedDatumEnvelopeFetcher implements DatumEnvelopeFetcher {

  private class DatumEnvelopeIterator implements Iterator<DatumEnvelope> {

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

  }

  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();
  private final ManualFeedConsumptionEndPoint consumptionEndPoint;
  private final Counter receivedDatumEnvelopeCount;
  private final Counter failureCount;
  private final DatumEnvelopeIterator datumEnvelopeIterator = new DatumEnvelopeIterator();

  public ManualFeedDatumEnvelopeFetcher(final ManualFeedConsumptionEndPoint consumptionEndPoint,
                                        final MetricsFactory metricFactory) {

    this.consumptionEndPoint = consumptionEndPoint;

    receivedDatumEnvelopeCount = metricFactory.createCounter("Receive.Attempts", "Success");
    failureCount = metricFactory.createCounter("Receive.Attempts", "Failure");
  }

  private DatumEnvelope fetchDatumEnvelope() {
    try {
      final ByteBuffer datumEnvelopeByteBuffer = ByteBuffer.wrap(consumptionEndPoint.fetch());
      final DatumEnvelope datumEnvelope = avroDatumEnvelopeSerDe.deserializeDatumEnvelope(datumEnvelopeByteBuffer);

      receivedDatumEnvelopeCount.inc();

      return datumEnvelope;
    } catch (final InterruptedException e) {
      failureCount.inc();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<DatumEnvelope> datumEnvelopes() {
    return new Iterable<DatumEnvelope>() {
      @Override
      public Iterator<DatumEnvelope> iterator() {
        return datumEnvelopeIterator;
      }
    };
  }
}
