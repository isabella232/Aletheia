package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.AvroDatumEnvelopeSender;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

public class KafkaDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<KafkaTopicProductionEndPoint> {
  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final KafkaTopicProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {
    return new AvroDatumEnvelopeSender(new KafkaBinarySender(productionEndPoint, metricFactory));
  }
}
