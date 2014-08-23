package com.outbrain.aletheia.datum.production;

public interface Sender<TInput> {
  void send(final TInput data) throws SilentSenderException;
}

