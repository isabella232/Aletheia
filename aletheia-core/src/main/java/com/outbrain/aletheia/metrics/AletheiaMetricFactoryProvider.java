package com.outbrain.aletheia.metrics;

import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

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

  private final Class domainClass;
  protected final String componentName;
  protected final MetricsFactory metricsFactory;

  public AletheiaMetricFactoryProvider(final Class domainClass,
                                       final String componentName,
                                       final MetricsFactory metricsFactory) {
    this.domainClass = domainClass;
    this.componentName = componentName;
    this.metricsFactory = metricsFactory;
  }

  public String datumTypeId() {
    return DatumUtils.getDatumTypeId(domainClass);
  }
}
