package com.outbrain.aletheia.datum;

/**
 * A base interface for both {@link com.outbrain.aletheia.datum.production.ProductionEndPoint}s and {@link com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint}s.
 */
public interface EndPoint {
  String getName();
}
