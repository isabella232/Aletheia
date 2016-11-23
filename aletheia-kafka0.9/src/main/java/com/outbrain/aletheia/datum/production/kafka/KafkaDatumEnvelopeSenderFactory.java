package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.AvroDatumEnvelopeSender;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

/**
 * A {@link DatumEnvelopeSenderFactory} capable of producing senders capable of sending
 * {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s to a Kafka.
 */
public class KafkaDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<KafkaTopicProductionEndPoint> {
  @Override
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final KafkaTopicProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {
    return new AvroDatumEnvelopeSender(new KafkaBinarySender(productionEndPoint, metricFactory));
  }
}
