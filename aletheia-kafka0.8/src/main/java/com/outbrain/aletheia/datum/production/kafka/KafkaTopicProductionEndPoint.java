package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.Properties;

/**
 * A {@link com.outbrain.aletheia.datum.production.ProductionEndPoint} for producing data to a Kafka topic.
 */
public class KafkaTopicProductionEndPoint implements ProductionEndPoint {

  private final Properties properties;
  private final String topicName;
  private final String endPointName;
  private boolean addShutdownHook;
  private final String brokerList;

  public KafkaTopicProductionEndPoint(final String brokerList,
                                      final String topicName,
                                      final String endPointName,
                                      final Properties properties) {
    this.brokerList = brokerList;
    this.properties = properties;
    this.topicName = topicName;
    this.endPointName = endPointName;
  }

  public String getBrokerList() {
    return brokerList;
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
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}

