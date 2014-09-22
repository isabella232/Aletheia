package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaStringSender extends KafkaSender<String,String> {

  @Override
  protected ProducerConfig customizeConfig(final ProducerConfig config) {
    final Properties props = config.props().props();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    return new ProducerConfig(props);
  }

  public KafkaStringSender(final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint,
                           final MetricsFactory metricFactory) {
    super(kafkaTopicDeliveryEndPoint, metricFactory);
  }

  @Override
  protected String convertInputToSendingFormat(final String s) {
    return s;
  }

  @Override
  protected int getPayloadSize(final String s) {
    return s.length();
  }

  @Override
  protected void validateConfiguration(final ProducerConfig config) {
    super.validateConfiguration(config);
    if (config.serializerClass().equals("kafka.serializer.StringEncoder")) {
      throw new IllegalArgumentException("Can't publish String data with non String serializer. given serializer is '" + config.serializerClass());
    }
  }

}
