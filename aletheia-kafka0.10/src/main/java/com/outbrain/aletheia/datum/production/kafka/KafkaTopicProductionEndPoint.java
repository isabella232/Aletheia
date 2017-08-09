package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.production.ProductionEndPoint;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Properties;

/**
 * A {@link ProductionEndPoint} for producing data to a Kafka topic.
 */
public class KafkaTopicProductionEndPoint implements ProductionEndPoint {

  private final Properties properties;
  private final String topicName;
  private final String endPointName;
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final KafkaTopicProductionEndPoint that = (KafkaTopicProductionEndPoint) o;

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

