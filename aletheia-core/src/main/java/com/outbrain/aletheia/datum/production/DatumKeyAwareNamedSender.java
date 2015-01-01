package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.Named;

/**
 * A {@link Sender} that has an alias.
 */
public interface DatumKeyAwareNamedSender<T> extends Named, DatumKeyAwareSender<T> {
  String getName();
}
