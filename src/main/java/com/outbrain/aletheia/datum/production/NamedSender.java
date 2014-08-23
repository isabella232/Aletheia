package com.outbrain.aletheia.datum.production;

/**
 * Created by slevin on 7/15/14.
 */
public interface NamedSender<T> extends Sender<T> {
  String getName();
}
