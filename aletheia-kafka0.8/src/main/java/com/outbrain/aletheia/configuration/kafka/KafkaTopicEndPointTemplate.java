package com.outbrain.aletheia.configuration.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.outbrain.aletheia.configuration.endpoint.EndPointTemplate;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.production.kafka.KafkaTopicProductionEndPoint;

import java.util.Properties;

/**
 * Provides Kafka {@link ProductionEndPoint}s and {@link ConsumptionEndPoint}s.
 */
public class KafkaTopicEndPointTemplate implements EndPointTemplate {

  public static final String TYPE = "kafka";

  private final String topicName;
  private final Properties producerProps;
  private final Properties consumerProps;

  public KafkaTopicEndPointTemplate(@JsonProperty(value = "topic.name", required = true) final String topicName,
                                    @JsonProperty("produce") final Properties producerProps,
                                    @JsonProperty("consume") final Properties consumerProps) {
    this.topicName = topicName;
    this.producerProps = producerProps;
    this.consumerProps = consumerProps;
  }

  public String getTopicName() {
    return topicName;
  }

  public Properties getProducerProps() {
    return producerProps;
  }

  public Properties getConsumerProps() {
    return consumerProps;
  }


  private String validateConsumer(final String propertyName, final Properties properties, final String endPointName) {
    return validate(propertyName, properties, "consumer", endPointName);
  }

  private String validateProducer(final String propertyName, final Properties properties, final String endPointName) {
    return validate(propertyName, properties, "producer", endPointName);
  }

  private String validate(final String propertyName,
                          final Properties properties,
                          final String clientType,
                          final String endPointName) {
    final String propertyValue = properties.getProperty(propertyName);
    final String errorMessageTemplate =
            clientType + "'s %s must not be null/empty for endpoint id: \"" + endPointName + "\"";
    Preconditions.checkArgument(!Strings.isNullOrEmpty(propertyValue), errorMessageTemplate, propertyName);
    return propertyValue;
  }

  @Override
  public ProductionEndPoint getProductionEndPoint(final String endPointName) {
    final String brokerList = validateProducer("broker.list", producerProps, endPointName);

    return new KafkaTopicProductionEndPoint(brokerList, getTopicName(), endPointName, producerProps);
  }

  @Override
  public ConsumptionEndPoint getConsumptionEndPoint(final String endPointName) {
    final String groupId = validateConsumer("group.id", consumerProps, endPointName);
    final String concurrencyLevel = validateConsumer("concurrency.level", consumerProps, endPointName);
    final String zkChroot = validateConsumer("zookeeper.chroot", consumerProps, endPointName);
    final String zkConnect = validateConsumer("zookeeper.connect", consumerProps, endPointName);

    return new KafkaTopicConsumptionEndPoint(zkConnect + zkChroot,
                                             getTopicName(),
                                             groupId,
                                             endPointName,
                                             Integer.parseInt(concurrencyLevel),
                                             consumerProps);
  }
}
