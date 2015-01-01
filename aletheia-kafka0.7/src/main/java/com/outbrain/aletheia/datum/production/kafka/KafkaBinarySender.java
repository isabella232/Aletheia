package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.util.Properties;

public class KafkaBinarySender extends KafkaNamedSender<ByteBuffer, Message> {

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
  protected Message convertInputToSendingFormat(final ByteBuffer byteBuffer) {
    final byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
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


