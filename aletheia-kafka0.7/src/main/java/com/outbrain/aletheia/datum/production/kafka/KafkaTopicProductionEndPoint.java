package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.production.ProductionEndPoint;

import java.util.Properties;

public class KafkaTopicProductionEndPoint implements ProductionEndPoint {

  public enum EndPointType {RawDatumEnvelope, String}

  private final int batchSize;
  private final String zkConnect;
  private final Properties properties;
  private final String topicName;
  private final EndPointType endPointType;
  private final String endPointName;
  private boolean addShutdownHook;

  public KafkaTopicProductionEndPoint(final String zkConnect,
                                      final String topicName,
                                      final EndPointType endPointType,
                                      final int batchSize,
                                      final String endPointName,
                                      final Properties properties) {
    this.batchSize = batchSize;
    this.zkConnect = zkConnect;
    this.properties = properties;
    this.topicName = topicName;
    this.endPointType = endPointType;
    this.endPointName = endPointName;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public String getZkConnect() {
    return zkConnect;
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