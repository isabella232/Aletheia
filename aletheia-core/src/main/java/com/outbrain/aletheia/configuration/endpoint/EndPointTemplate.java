package com.outbrain.aletheia.configuration.endpoint;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;

/**
 * Provides {@link ProductionEndPoint}s and {@link ConsumptionEndPoint}s according to the type of
 * {@link com.outbrain.aletheia.datum.EndPoint} it represents (e.g, Kafka, LogFile, etc.).
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface EndPointTemplate {

  /**
   * @param endPointName the name to be given to the constructed {@link ProductionEndPoint}
   * @return a {@link ProductionEndPoint} instance of the type this {@link EndPointTemplate} represents.
   */
  ProductionEndPoint getProductionEndPoint(final String endPointName);

  /**
   * @param endPointName the name to be given to the constructed {@link ConsumptionEndPoint}
   * @return a {@link ConsumptionEndPoint} instance of the type this {@link EndPointTemplate} represents.
   */
  ConsumptionEndPoint getConsumptionEndPoint(final String endPointName);
}
