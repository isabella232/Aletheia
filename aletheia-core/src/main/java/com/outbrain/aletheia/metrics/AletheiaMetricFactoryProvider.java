package com.outbrain.aletheia.metrics;

/**
 * An abstract class for Aletheia specific metrics with pre-defined component names.
 */
public abstract class AletheiaMetricFactoryProvider implements MetricFactoryProvider {

  //label names
  protected static final String COMPONENT = "component";
  protected static final String DATUM_TYPE_ID = "datum_type_id";
  protected static final String DIRECTION = "direction";
  protected static final String ENDPOINT_CLASS = "endpoint_class";
  protected static final String ENDPOINT_NAME = "endpoint_name";

  //Direction label values
  protected static final String PRODUCTION = "Production";
  protected static final String CONSUMPTION = "Consumption";

  protected final String datumTypeId;
  protected final String componentName;


  public AletheiaMetricFactoryProvider(final String datumTypeId,
                                       final String componentName
  ) {
    this.datumTypeId = datumTypeId;
    this.componentName = componentName;
  }

  public String datumTypeId() {
    return datumTypeId;
  }
}
