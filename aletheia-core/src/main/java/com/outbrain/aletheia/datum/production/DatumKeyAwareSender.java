package com.outbrain.aletheia.datum.production;

import java.io.Closeable;

public interface DatumKeyAwareSender<TInput> extends Closeable {
  void send(final TInput data, final String key) throws SilentSenderException;
  void send(final TInput data, final String key, final DeliveryCallback deliveryCallback) throws SilentSenderException;
}

