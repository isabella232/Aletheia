package com.outbrain.aletheia.datum.production;

public interface Sender<TInput> {
  void send(final TInput data) throws SilentSenderException;
  void send(final TInput data, final DeliveryCallback deliveryCallback) throws SilentSenderException;
}

