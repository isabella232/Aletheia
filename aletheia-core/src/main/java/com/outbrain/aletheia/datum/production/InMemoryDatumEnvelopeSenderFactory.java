package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * Builds {@link com.outbrain.aletheia.datum.production.NamedSender<com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope>}s capable of sending data to an {@link com.outbrain.aletheia.datum.InMemoryEndPoint}.
 */
public class InMemoryDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<InMemoryEndPoint> {

  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final InMemoryEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {

    if (productionEndPoint instanceof InMemoryEndPoint.WithBinaryStorage) {
      return new RawDatumEnvelopeBinarySender(productionEndPoint);
    } else if (productionEndPoint instanceof InMemoryEndPoint.WithStringStorage) {
      return new DatumEnvelopePeelingStringSender(productionEndPoint);
    } else {
      throw new IllegalArgumentException(String.format("Unknown encoding type %s", productionEndPoint));
    }
  }
}
