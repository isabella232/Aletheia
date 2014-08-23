package com.outbrain.aletheia.datum.consumption;

/**
 * Created by slevin on 7/24/14.
 */
public interface DatumHandler<TDomainClass> {
  void handleDatum(TDomainClass datum);
}
