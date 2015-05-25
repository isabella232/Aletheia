package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.Properties;

/**
 * A {@link com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint} capable of consuming data from a Kafka topic.
 */
public class KafkaTopicConsumptionEndPoint implements ConsumptionEndPoint {

  private final String zkConnect;
  private final String topicName;
  private final String groupId;
  private final int concurrencyLevel;
  private final Properties properties;
  private final String endPointName;

  public KafkaTopicConsumptionEndPoint(final String zkConnect,
                                       final String topicName,
                                       final String groupId,
                                       final String endPointName,
                                       final int concurrencyLevel,
                                       final Properties properties) {
    this.zkConnect = zkConnect;
    this.topicName = topicName;
    this.groupId = groupId;
    this.concurrencyLevel = concurrencyLevel;
    this.properties = properties;
    this.endPointName = endPointName;
  }

  public String getZkConnect() {
    return zkConnect;
  }

  public String getTopicName() {
    return topicName;
  }

  public Properties getProperties() {
    return properties;
  }

  public String getGroupId() {
    return groupId;
  }

  public int getConcurrencyLevel() {
    return concurrencyLevel;
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
