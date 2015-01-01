package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.nio.ByteBuffer;

/**
 * Builds {@link com.outbrain.aletheia.datum.production.NamedSender<com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope>}s capable of sending data to an {@link InMemoryProductionEndPoint}.
 */
public class InMemoryDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<InMemoryProductionEndPoint> {

  private <T> InMemoryAccumulatingNamedSender<T> wireInMemoryProductionEndPoint(final InMemoryProductionEndPoint productionEndPoint,
                                                                                final InMemoryAccumulatingNamedSender<T> sender) {
    productionEndPoint.setReceivedData(sender.getSentData());
    return sender;
  }

  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final InMemoryProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {

    if (productionEndPoint.getEndPointType() == InMemoryProductionEndPoint.EndPointType.RawDatumEnvelope) {

      return new RawDatumEnvelopeBinarySender(

              new DatumKeyAwareNamedSender<ByteBuffer>() {

                final InMemoryAccumulatingNamedSender<byte[]> byteArraySender =
                        wireInMemoryProductionEndPoint(productionEndPoint,
                                                       new InMemoryAccumulatingNamedSender<byte[]>());

                @Override
                public String getName() {
                  return byteArraySender.getName();
                }

                @Override
                public void send(final ByteBuffer data, final String key) throws SilentSenderException {
                  byteArraySender.send(data.array(), key);
                }
              });
    } else if (productionEndPoint.getEndPointType() == InMemoryProductionEndPoint.EndPointType.String) {
      return new DatumEnvelopePeelingStringSender(
              wireInMemoryProductionEndPoint(productionEndPoint,
                                             new InMemoryAccumulatingNamedSender<String>()));
    } else {
      throw new IllegalArgumentException(String.format("Unknown encoding type %s",
                                                       productionEndPoint.getEndPointType()));
    }
  }
}
