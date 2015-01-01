package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.util.Properties;

public class KafkaBinaryNamedSender extends KafkaNamedSender<ByteBuffer, byte[]> {

  public KafkaBinaryNamedSender(final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint,
                                final MetricsFactory metricFactory) {
    super(kafkaTopicDeliveryEndPoint, metricFactory);
  }

  @Override
  protected ProducerConfig customizeConfig(final ProducerConfig config) {
    final Properties props = config.props().props();
    props.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    return new ProducerConfig(props);
  }

  @Override
  protected byte[] convertInputToSendingFormat(final ByteBuffer byteBuffer) {
    return byteBuffer.array();
  }

  @Override
  protected int getPayloadSize(final byte[] payload) {
    return payload.length;
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


