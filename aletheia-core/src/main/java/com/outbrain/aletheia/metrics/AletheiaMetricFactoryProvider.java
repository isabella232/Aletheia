package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.swinfra.metrics.MetricRegistry;

/**
 * An abstract class for Aletheia specific metrics with pre-defined component names.
 */
public abstract class AletheiaMetricFactoryProvider implements MetricFactoryProvider {

  protected static final String ALETHEIA = "Aletheia";
  protected static final String DATUM_TYPES = "DatumTypes";
  protected static final String DATA = "Data";
  protected static final String PRODUCTION = "Production";
  protected static final String CONSUMPTION = "Consumption";
  protected static final String Tx = "Tx";
  protected static final String Rx = "Rx";

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
