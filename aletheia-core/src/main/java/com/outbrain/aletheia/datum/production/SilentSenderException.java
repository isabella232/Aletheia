package com.outbrain.aletheia.datum.production;

/**
 * Exception thrown by {@link DatumKeyAwareSender}s and {@link Sender}s, to indicate high rate errors
 * that need not be reported to logs to avoid flooding, but should be reported as metrics.
 */
public class SilentSenderException extends Exception {
  public SilentSenderException(final Exception e) {
    super(e);
  }
}
