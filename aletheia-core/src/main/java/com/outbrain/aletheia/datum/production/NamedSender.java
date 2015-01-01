package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.Named;

/**
 * A {@link Sender} that has an alias.
 */
public interface NamedSender<T> extends Named, Sender<T> {
  String getName();
}
