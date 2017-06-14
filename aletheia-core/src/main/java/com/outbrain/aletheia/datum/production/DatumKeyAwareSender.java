package com.outbrain.aletheia.datum.production;

public interface DatumKeyAwareSender<TInput> {
  void send(final TInput data, final String key) throws SilentSenderException;
  void send(final TInput data, final String key, final DeliveryCallback deliveryCallback) throws SilentSenderException;
}

