package com.outbrain.aletheia.datum.consumption;

/**
 * Created by slevin on 1/1/15.
 */
public interface FetchConsumptionEndPoint<T> extends ConsumptionEndPoint {

  T fetch();

}
