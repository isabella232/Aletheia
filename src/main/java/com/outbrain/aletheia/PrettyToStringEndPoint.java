package com.outbrain.aletheia;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Created by slevin on 8/13/14.
 */
public abstract class PrettyToStringEndPoint implements EndPoint {

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

}
