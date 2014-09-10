package com.outbrain.aletheia;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Provides deriving <code>EndPoint</code> classes with a pretty toString().
 */
public abstract class PrettyToStringEndPoint implements EndPoint {

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

}
