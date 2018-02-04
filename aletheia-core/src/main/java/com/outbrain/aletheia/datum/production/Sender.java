package com.outbrain.aletheia.datum.production;

public interface Sender<TInput> extends AutoCloseable {
  void send(final TInput data) throws SilentSenderException;
  void send(final TInput data, final DeliveryCallback deliveryCallback) throws SilentSenderException;
}

