package com.outbrain.aletheia.configuration.routing;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Created by slevin on 5/13/15.
 */
public class Route {

  private final String endPointId;
  private final String serDeId;

  public Route(@JsonProperty(value = "endpoint", required = true) final String endPointId,
               @JsonProperty(value = "serDe", required = true) final String serDeId) {
    this.endPointId = endPointId;
    this.serDeId = serDeId;
  }

  public String getEndPointId() {
    return endPointId;
  }

  public String getSerDeId() {
    return serDeId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    final Route that = (Route) o;

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
}
