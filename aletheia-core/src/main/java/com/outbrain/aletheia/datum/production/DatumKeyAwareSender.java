package com.outbrain.aletheia.datum.production;

public interface DatumKeyAwareSender<TInput> {
  void send(final TInput data, String key) throws SilentSenderException;
}

