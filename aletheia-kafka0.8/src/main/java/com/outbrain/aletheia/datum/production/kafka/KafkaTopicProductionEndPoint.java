package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Properties;

/**
 * A {@link com.outbrain.aletheia.datum.production.ProductionEndPoint} for producing data to a Kafka topic.
 */
public class KafkaTopicProductionEndPoint implements ProductionEndPoint {

  private final int batchSize;
  private final Properties properties;
  private final String topicName;
  private final String endPointName;
  private boolean addShutdownHook;
  private final String brokerList;

  public KafkaTopicProductionEndPoint(final String brokerList,
                                      final String topicName,
                                      final int batchSize,
                                      final String endPointName,
                                      final Properties properties) {
    this.batchSize = batchSize;
    this.brokerList = brokerList;
    this.properties = properties;
    this.topicName = topicName;
    this.endPointName = endPointName;
  }

  public String getBrokerList() {
    return brokerList;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Properties getProperties() {
    return properties;
  }

  public String getTopicName() {
    return topicName;
  }

  public boolean getAddShutdownHook() {
    return addShutdownHook;
  }

  @Override
  public String getName() {
    return endPointName;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}

