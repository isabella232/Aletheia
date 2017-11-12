package com.outbrain.aletheia.datum.production;

import java.io.Closeable;

public interface Sender<TInput> extends Closeable {
  void send(final TInput data) throws SilentSenderException;
  void send(final TInput data, final DeliveryCallback deliveryCallback) throws SilentSenderException;
}

