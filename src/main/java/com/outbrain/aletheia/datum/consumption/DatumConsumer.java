package com.outbrain.aletheia.datum.consumption;

/**
 * Created by slevin on 8/16/14.
 */
public interface DatumConsumer<TDomainClass> {
  Iterable<TDomainClass> datums();
}
