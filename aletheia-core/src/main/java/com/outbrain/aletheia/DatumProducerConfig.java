package com.outbrain.aletheia;

import com.outbrain.aletheia.datum.production.DatumProducer;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Configuration details for {@link DatumProducer}s.
 */
public class DatumProducerConfig {

  private final int incarnation;
  private final String source;

  /**
   * @param incarnation incarnation provides means to further control data filtering, e.g.,
   *                    discarding all data below a certain incarnation.
   * @param source      the source of this {@link DatumProducer} (e.g. the hostname or service name
   *                    it's running from)
   */
  public DatumProducerConfig(final int incarnation, final String source) {

    this.incarnation = incarnation;
    this.source = source;
  }

  public int getIncarnation() {
    return incarnation;
  }

  public String getSource() {
    return source;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
