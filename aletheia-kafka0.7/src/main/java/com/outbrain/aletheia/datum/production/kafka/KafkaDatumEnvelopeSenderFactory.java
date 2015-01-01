package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.DatumEnvelopePeelingStringSender;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.RawDatumEnvelopeBinarySender;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by slevin on 7/22/14.
 */
@Deprecated
public class KafkaDatumEnvelopeSenderFactory implements DatumEnvelopeSenderFactory<KafkaTopicProductionEndPoint> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaDatumEnvelopeSenderFactory.class);

  @Override
  @Deprecated
  public NamedSender<DatumEnvelope> buildDatumEnvelopeSender(final KafkaTopicProductionEndPoint productionEndPoint,
                                                             final MetricsFactory metricFactory) {

    if (productionEndPoint.getEndPointType()
                          .equals(KafkaTopicProductionEndPoint.EndPointType.RawDatumEnvelope)) {
      logger.info("Creating kafka transporter for input type: " + KafkaTopicProductionEndPoint.EndPointType.RawDatumEnvelope);

      return new RawDatumEnvelopeBinarySender(new KafkaBinarySender(productionEndPoint, metricFactory));

    } else if (productionEndPoint.getEndPointType()
                                 .equals(KafkaTopicProductionEndPoint.EndPointType.String)) {

      logger.info("Creating kafka transporter for input type: " + KafkaTopicProductionEndPoint.EndPointType.String);

      return new DatumEnvelopePeelingStringSender(new KafkaStringSender(productionEndPoint, metricFactory));
    } else {
      throw new IllegalArgumentException(String.format("Unknown transporter input type %s",
                                                       productionEndPoint.getEndPointType()));
    }
  }
}
