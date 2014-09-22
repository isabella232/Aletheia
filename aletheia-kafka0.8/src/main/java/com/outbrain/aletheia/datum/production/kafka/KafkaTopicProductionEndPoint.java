package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.production.ProductionEndPoint;

import java.util.Properties;

/**
 * A production endpoint for producing data to a Kafka topic.
 */
public class KafkaTopicProductionEndPoint extends ProductionEndPoint {

  public enum EndPointType {RawDatumEnvelope, String}

  private final int batchSize;
  private final Properties properties;
  private final String topicName;
  private final EndPointType endPointType;
  private final String endPointName;
  private boolean addShutdownHook;
  private final String brokerList;

  public KafkaTopicProductionEndPoint(final String brokerList,
                                      final String topicName,
                                      final EndPointType endPointType,
                                      final int batchSize,
                                      final String endPointName,
                                      final Properties properties) {
    this.batchSize = batchSize;
    this.brokerList = brokerList;
    this.properties = properties;
    this.topicName = topicName;
    this.endPointType = endPointType;
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

  public EndPointType getEndPointType() {
    return endPointType;
  }

  @Override
  public String getName() {
    return endPointName;
  }
}

