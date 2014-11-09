package com.outbrain.aletheia.datum.production;

/**
 * A {@code Sender} that has an alias.
 */
public interface NamedKeyAwareSender<T> extends Named, KeyAwareSender<T> {
  String getName();
}
