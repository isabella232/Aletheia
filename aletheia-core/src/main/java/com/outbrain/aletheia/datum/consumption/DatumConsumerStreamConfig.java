package com.outbrain.aletheia.datum.consumption;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Configuration details for {@link DatumConsumerStream}s.
 */
public class DatumConsumerStreamConfig {

  private final int incarnation;
  private final String hostname;

  /**
   * @param incarnation incarnation provides means to further control data filtering, e.g., discarding all data
   *                    below a certain incarnation.
   * @param hostname    the hostname this {@link com.outbrain.aletheia.datum.production.DatumProducer} instance is operating from.
   */
  public DatumConsumerStreamConfig(final int incarnation, final String hostname) {

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
