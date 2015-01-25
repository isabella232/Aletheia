
package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

import java.util.Properties;

@Deprecated
public class KafkaBinarySender extends KafkaNamedSender<byte[], Message> {

  @Deprecated
  public KafkaBinarySender(final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint,
                           final MetricsFactory metricFactory) {
    super(kafkaTopicDeliveryEndPoint, metricFactory);
  }

  @Override
  protected ProducerConfig customizeConfig(final ProducerConfig config) {
    final Properties props = config.props();
    props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    return new ProducerConfig(props);
  }

  @Override
  protected Message convertInputToSendingFormat(final byte[] bytes) {
    return new Message(bytes);
  }

  @Override
  protected int getPayloadSize(final Message payload) {
    return payload.payloadSize();
  }

  @Override
  protected void validateConfiguration(final ProducerConfig config) {
    super.validateConfiguration(config);
    if (config.serializerClass().equals("kafka.serializer.DefaultEncoder")) {
      throw new IllegalArgumentException("Can't publish binary data with non default serializer. given serializer is '" + config
              .serializerClass());
    }
  }


}


