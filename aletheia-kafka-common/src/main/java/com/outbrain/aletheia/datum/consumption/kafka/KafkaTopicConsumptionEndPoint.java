package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Properties;

/**
 * A {@link ConsumptionEndPoint} capable of consuming data from a Kafka topic.
 */
public class KafkaTopicConsumptionEndPoint implements ConsumptionEndPoint {

  private final String brokers;
  private final String topicName;
  private final String groupId;
  private final int concurrencyLevel;
  private final Properties properties;
  private final String endPointName;

  public KafkaTopicConsumptionEndPoint(final String brokers,
                                       final String topicName,
                                       final String groupId,
                                       final String endPointName,
                                       final int concurrencyLevel,
                                       final Properties properties) {
    this.brokers = brokers;
    this.topicName = topicName;
    this.groupId = groupId;
    this.concurrencyLevel = concurrencyLevel;
    this.properties = properties;
    this.endPointName = endPointName;
  }

  public String getBrokers() {
    return brokers;
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
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final KafkaTopicConsumptionEndPoint that = (KafkaTopicConsumptionEndPoint) o;

    return EqualsBuilder.reflectionEquals(this, that);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  @Override
  public String getName() {
    return endPointName;
  }
}
