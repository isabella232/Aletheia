package com.outbrain.aletheia.datum.production;

public interface KeyAwareSender<TInput> {
  void send(final TInput data, String key) throws SilentSenderException;
}

