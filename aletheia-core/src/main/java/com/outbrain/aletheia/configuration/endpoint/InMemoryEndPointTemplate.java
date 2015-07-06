package com.outbrain.aletheia.configuration.endpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.outbrain.aletheia.datum.InMemoryEndPoints;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;

/**
 * Provides in-memory {@link ProductionEndPoint}s and {@link ConsumptionEndPoint}s.
 */
public class InMemoryEndPointTemplate implements EndPointTemplate {

  public static final String TYPE = "inMemory";

  private final int size;

  public InMemoryEndPointTemplate(@JsonProperty(value = "size", required = true) final int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  @Override
  public ProductionEndPoint getProductionEndPoint(final String endPointName) {
    return InMemoryEndPoints.register(endPointName, size);
  }

  @Override
  public ConsumptionEndPoint getConsumptionEndPoint(final String endPointName) {
    return InMemoryEndPoints.register(endPointName, size);
  }
}
