package com.outbrain.aletheia.datum.consumption;

/**
 * Created by slevin on 1/1/15.
 */
public interface DatumKeyAwareFetchEndPoint<T> extends ConsumptionEndPoint {

  T fetch(String key);

}
