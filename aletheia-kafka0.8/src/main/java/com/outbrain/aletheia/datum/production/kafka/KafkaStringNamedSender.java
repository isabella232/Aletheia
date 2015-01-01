package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaStringNamedSender extends KafkaNamedSender<String, String> {

  @Override
  protected ProducerConfig customizeConfig(final ProducerConfig config) {
    final Properties props = config.props().props();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    return new ProducerConfig(props);
  }

  public KafkaStringNamedSender(final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint,
                                final MetricsFactory metricFactory) {
    super(kafkaTopicDeliveryEndPoint, metricFactory);
  }

  @Override
  protected String convertInputToSendingFormat(final String string) {
    return string;
  }

  @Override
  protected int getPayloadSize(final String s) {
    return s.length();
  }

  @Override
  protected void validateConfiguration(final ProducerConfig config) {
    super.validateConfiguration(config);
    if (config.serializerClass().equals("kafka.serializer.StringEncoder")) {
      throw new IllegalArgumentException("Can't publish String data with non String serializer. given serializer is '" + config
              .serializerClass());
    }
  }

}
