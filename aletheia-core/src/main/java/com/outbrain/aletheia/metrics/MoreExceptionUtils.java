package com.outbrain.aletheia.metrics;

import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Created by slevin on 3/1/15.
 */
public class MoreExceptionUtils {

  public static String getType(final Exception error) {
    final Throwable rootCause = ExceptionUtils.getRootCause(error);
    final Throwable nonNullRootCause = rootCause != null ? rootCause : error;

    return nonNullRootCause.getClass().getSimpleName();
  }
}
