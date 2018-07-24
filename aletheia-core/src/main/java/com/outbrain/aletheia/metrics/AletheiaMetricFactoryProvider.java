package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.swinfra.metrics.MetricRegistry;

/**
 * An abstract class for Aletheia specific metrics with pre-defined component names.
 */
public abstract class AletheiaMetricFactoryProvider implements MetricFactoryProvider {

  protected static final String COMPONENT = "component";
  protected static final String DATUM_TYPE_ID = "datum_type_id";
  protected static final String DIRECTION = "direction";
  protected static final String ENDPOINT_CLASS = "endpoint_class";
  protected static final String ENDPOINT_NAME = "endpoint_name";


  protected static final String PRODUCTION = "Production";
  protected static final String CONSUMPTION = "Consumption";
  protected static final String Tx = "Production";
  protected static final String Rx = "Consumption";

  protected final String datumTypeId;
  protected final String componentName;
  protected final MetricsFactory metricsFactory;
  protected final MetricRegistry metricRegistry = new MetricRegistry();

  public AletheiaMetricFactoryProvider(final String datumTypeId,
                                       final String componentName,
                                       final MetricsFactory metricsFactory) {
    this.datumTypeId = datumTypeId;
    this.componentName = componentName;
    this.metricsFactory = metricsFactory;
  }

  public String datumTypeId() {
    return datumTypeId;
  }
}
