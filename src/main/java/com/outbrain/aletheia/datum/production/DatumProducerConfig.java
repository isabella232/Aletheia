package com.outbrain.aletheia.datum.production;

import org.apache.commons.lang.builder.ToStringBuilder;

public class DatumProducerConfig {

  private final int incarnation;
  private final String hostname;

  public DatumProducerConfig(final int incarnation, final String hostname) {

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
