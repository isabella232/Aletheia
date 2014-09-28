package com.outbrain.aletheia.datum.production;

/**
 * A {@code Sender} that has an alias.
 */
public interface NamedSender<T> extends Sender<T> {
  String getName();
}
