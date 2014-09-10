package com.outbrain.aletheia.datum.consumption;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Configuration details for <code>DatumConsumer</code>s.
 */
public class DatumConsumerConfig {

  private final int incarnation;
  private final String hostname;

  /**
   * @param incarnation incarnation provides means to further control data filtering, e.g., discarding all data
   *                    below a certain incarnation.
   * @param hostname the hostname this <code>DatumProducer</code> instance is operating from.
   */
  public DatumConsumerConfig(final int incarnation, final String hostname) {

    this.incarnation = incarnation;
    this.hostname = hostname;
  }

  public int getIncarnation() {
    return incarnation;
  }

  public String getHostname() {
    return hostname;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}
