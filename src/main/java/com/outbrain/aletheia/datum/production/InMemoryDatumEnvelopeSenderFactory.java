package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Created by slevin on 7/22/14.
 */
public class InMemoryDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<InMemoryProductionEndPoint> {
  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final InMemoryProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {

    if (productionEndPoint.getEndPointType() == InMemoryProductionEndPoint.EndPointType.RawDatumEnvelope) {
      return new RawDatumEnvelopeBinarySender(getInMemoryTransporter(productionEndPoint));
    } else if (productionEndPoint.getEndPointType() == InMemoryProductionEndPoint.EndPointType.String) {
      return new DatumEnvelopePeelingStringSender(getInMemoryTransporter(productionEndPoint));
    } else {
      throw new IllegalArgumentException(String.format("Unknown encoding type %s",
                                                       productionEndPoint.getEndPointType()));
    }
  }

  private InMemoryAccumulatingSender getInMemoryTransporter(final InMemoryProductionEndPoint inMemoryProductionEndPoint) {
    final InMemoryAccumulatingSender inMemoryTransporter = new InMemoryAccumulatingSender();
    inMemoryProductionEndPoint.setReceivedData(inMemoryTransporter.getSentData());
    return inMemoryTransporter;
  }
}
