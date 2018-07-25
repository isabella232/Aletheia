package com.outbrain.aletheia;

import com.google.common.collect.Maps;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbHandler;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.InMemoryDatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.InMemoryDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.Properties;

abstract class BaseAletheiaBuilder {


  protected class BreadcrumbProducingHandler implements BreadcrumbHandler {

    private final DatumProducer<Breadcrumb> breadcrumbDatumProducer;

    BreadcrumbProducingHandler(final DatumProducerConfig datumProducerConfig,
                               final MetricFactoryProvider metricFactoryProvider) {

      final DatumProducerBuilder<Breadcrumb> breadcrumbProducerBuilder =
              configurableBreadcrumbProducerBuilder(metricFactoryProvider);

      breadcrumbDatumProducer = registerEndPointTypes(breadcrumbProducerBuilder).build(datumProducerConfig);
    }

    private DatumProducerBuilder<Breadcrumb> configurableBreadcrumbProducerBuilder(final MetricFactoryProvider metricFactoryProvider) {
      return DatumProducerBuilder
              .forDomainClass(Breadcrumb.class)
              .reportMetricsTo(metricFactoryProvider)
              .deliverDataTo(breadcrumbsProductionEndPoint, new JsonDatumSerDe<>(Breadcrumb.class));
    }

    private DatumProducerBuilder<Breadcrumb> registerEndPointTypes(final DatumProducerBuilder<Breadcrumb> breadcrumbDatumProducerBuilder) {

      DatumProducerBuilder<Breadcrumb> configuredBreadcrumbDatumProducerBuilder = breadcrumbDatumProducerBuilder;

      for (final Class<? extends ProductionEndPoint> productionEndPointType : envelopeSenderTypesRegistry.keySet()) {
        configuredBreadcrumbDatumProducerBuilder =
                breadcrumbDatumProducerBuilder
                        .registerProductionEndPointType(productionEndPointType,
                                                        getEnvelopeSenderFactory(productionEndPointType));
      }

      return configuredBreadcrumbDatumProducerBuilder;
    }

    @Override
    public void handle(final Breadcrumb breadcrumb) {
      breadcrumbDatumProducer.deliver(breadcrumb);
    }

    @Override
    public void close() throws Exception {
      breadcrumbDatumProducer.close();
    }
  }

  private final Map<Class, DatumEnvelopeSenderFactory> envelopeSenderTypesRegistry = Maps.newHashMap();
  private final Map<Class, DatumEnvelopeFetcherFactory> envelopeFetcherTypesRegistry = Maps.newHashMap();
  BreadcrumbsConfig breadcrumbsConfig;
  private ProductionEndPoint breadcrumbsProductionEndPoint;
  private MetricFactoryProvider metricFactoryProvider = MetricFactoryProvider.NULL;

  BaseAletheiaBuilder() {
    registerKnownProductionEndPointsTypes();
    registerKnownConsumptionEndPointTypes();
  }

  public class EnvelopeTimestampSelector implements DatumType.TimestampSelector<DatumEnvelope> {

    @Override
    public DateTime extractDatumDateTime(DatumEnvelope envelope) {
      return new DateTime(envelope.getLogicalTimestamp());
    }
  }

  boolean isBreadcrumbProductionDefined() {
    return breadcrumbsConfig != null && breadcrumbsProductionEndPoint != null;
  }

  public void setBreadcrumbsEndpoint(final ProductionEndPoint breadcrumbProductionEndPoint,
                                     final BreadcrumbsConfig breadcrumbsConfig) {

    this.breadcrumbsProductionEndPoint = breadcrumbProductionEndPoint;
    this.breadcrumbsConfig = breadcrumbsConfig;

  }

  protected Properties getBreadcrumbEnvironment(Properties properties) {
    final Properties breadcrumbEnv = new Properties();

    if (properties != null) {
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        if (String.valueOf(entry.getKey()).startsWith("aletheia.breadcrumbs.")) {
          breadcrumbEnv.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
      }

      breadcrumbEnv.setProperty(AletheiaConfig.MULTIPLE_CONFIGURATIONS_PATH,
              properties.getProperty(AletheiaConfig.MULTIPLE_CONFIGURATIONS_PATH, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.ENDPOINT_GROUPS_EXTENSION,
              properties.getProperty(AletheiaConfig.ENDPOINT_GROUPS_EXTENSION, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.ENDPOINTS_EXTENSION,
              properties.getProperty(AletheiaConfig.ENDPOINTS_EXTENSION, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.ROUTING_EXTENSION,
              properties.getProperty(AletheiaConfig.ROUTING_EXTENSION, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.SERDES_EXTENSION,
              properties.getProperty(AletheiaConfig.SERDES_EXTENSION, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.ENDPOINTS_CONFIG_PATH,
              properties.getProperty(AletheiaConfig.ENDPOINTS_CONFIG_PATH, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.ROUTING_CONFIG_PATH,
              properties.getProperty(AletheiaConfig.ROUTING_CONFIG_PATH, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.ENDPOINT_GROUPS_CONFIG_PATH,
              properties.getProperty(AletheiaConfig.ENDPOINT_GROUPS_CONFIG_PATH, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.SERDES_CONFIG_PATH,
              properties.getProperty(AletheiaConfig.SERDES_CONFIG_PATH, ""));
      breadcrumbEnv.setProperty(AletheiaConfig.DEFAULT_ENDPOINT,
              properties.getProperty(AletheiaConfig.DEFAULT_ENDPOINT, ""));
    }
    return breadcrumbEnv;
  }

  private void registerKnownProductionEndPointsTypes() {
    this.registerEnvelopeSenderType(InMemoryEndPoint.class, new InMemoryDatumEnvelopeSenderFactory());
  }

  private void registerKnownConsumptionEndPointTypes() {
    this.registerEnvelopeFetcherType(InMemoryEndPoint.class, new InMemoryDatumEnvelopeFetcherFactory());
  }

  <TProductionEndPoint extends ProductionEndPoint, UProductionEndPoint extends TProductionEndPoint> void registerEnvelopeSenderType(
      final Class<TProductionEndPoint> endPointType,
      final DatumEnvelopeSenderFactory<? super UProductionEndPoint> datumEnvelopeSenderFactory) {

    envelopeSenderTypesRegistry.put(endPointType, datumEnvelopeSenderFactory);
  }

  <TConsumptionEndPoint extends ConsumptionEndPoint, UConsumptionEndPoint extends TConsumptionEndPoint> void registerEnvelopeFetcherType(
      final Class<TConsumptionEndPoint> consumptionEndPointType,
      final DatumEnvelopeFetcherFactory<? super UConsumptionEndPoint> datumEnvelopeFetcherFactory) {

    envelopeFetcherTypesRegistry.put(consumptionEndPointType, datumEnvelopeFetcherFactory);
  }

  DatumEnvelopeSenderFactory getEnvelopeSenderFactory(final Class endPointType) {
    return envelopeSenderTypesRegistry.get(endPointType);
  }

  DatumEnvelopeFetcherFactory getEnvelopeFetcherFactory(final Class endPointType) {
    return envelopeFetcherTypesRegistry.get(endPointType);
  }

  /**
   * Configures metrics reporting.
   *
   * @param metricFactoryProvider A MetricsFactory instance to report metrics to.
   */
  public void setMetricsFactoryProvider(final MetricFactoryProvider metricFactoryProvider) {
    this.metricFactoryProvider = metricFactoryProvider;
  }

  public MetricFactoryProvider getMetricsFactoryProvider() {
    return metricFactoryProvider;
  }
}
