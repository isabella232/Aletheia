package com.outbrain.aletheia;

import com.google.common.collect.Maps;

import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbHandler;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryPrefixer;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.Properties;

abstract class BaseAletheiaBuilder {

  /**
   * A special case {@link AletheiaMetricFactoryProvider} used only when reporting metrics from a
   * breadcrumb dedicated {@link DatumProducer}, that is, a {@link DatumProducer} whose only purpose in life
   * is to produce breadcrumbs (breadcrumb is itself, a special kind of datum).
   */
  protected static class InternalBreadcrumbProducerMetricFactoryProvider extends AletheiaMetricFactoryProvider {

    public InternalBreadcrumbProducerMetricFactoryProvider(final String datumTypeId,
                                                           final String componentName,
                                                           final MetricsFactory metricsFactory) {
      super(datumTypeId, componentName, metricsFactory);
    }

    @Override
    public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint) {
      return MetricFactoryPrefixer.prefix(metricsFactory).with(datumTypeId(),
                                                               PRODUCTION,
                                                               endPoint.getClass().getSimpleName());
    }

    @Override
    public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {
      throw new IllegalStateException(
              "No MetricFactory for internal breadcrumb producer instance should be asked for when already in internal breadcrumb deliverer mode.");
    }

    @Override
    public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint) {
      return MetricFactoryPrefixer.prefix(metricsFactory).with(datumTypeId(),
                                                               Tx,
                                                               endPoint.getClass().getSimpleName());
    }

    @Override
    public MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint) {
      throw new IllegalStateException(
              "No MetricFactory for datum envelope fetcher instance should be asked for when already in internal breadcrumb producer mode.");
    }

    @Override
    public MetricsFactory forAuditingDatumStreamConsumer(final EndPoint endPoint) {
      throw new IllegalStateException(
              "No MetricFactory for auditing datum stream consumer instance should be asked for when already in internal breadcrumb producer mode.");
    }

    @Override
    public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
      throw new IllegalStateException(
              "No MetricFactory for datum envelope metadata should be asked for when already in internal breadcrumb producer mode.");
    }
  }

  protected class BreadcrumbProducingHandler implements BreadcrumbHandler {

    private final DatumProducer<Breadcrumb> breadcrumbDatumProducer;

    public BreadcrumbProducingHandler(final DatumProducerConfig datumProducerConfig,
                                      final MetricsFactory metricsFactory) {

      final DatumProducerBuilder<Breadcrumb> breadcrumbProducerBuilder =
              configurableBreadcrumbProducerBuilder(metricsFactory);

      breadcrumbDatumProducer = registerEndPointTypes(breadcrumbProducerBuilder).build(datumProducerConfig);
    }

    private DatumProducerBuilder<Breadcrumb> configurableBreadcrumbProducerBuilder(final MetricsFactory metricsFactory) {
      return DatumProducerBuilder
              .forDomainClass(Breadcrumb.class)
              .reportMetricsTo(metricsFactory)
              .deliverDataTo(breadcrumbsProductionEndPoint, new JsonDatumSerDe<>(Breadcrumb.class));
    }

    private DatumProducerBuilder<Breadcrumb> registerEndPointTypes(final DatumProducerBuilder<Breadcrumb> breadcrumbDatumProducerBuilder) {

      DatumProducerBuilder<Breadcrumb> configuredBreadcrumbDatumProducerBuilder = breadcrumbDatumProducerBuilder;

      for (final Class<? extends ProductionEndPoint> productionEndPointType : endpoint2datumEnvelopeSenderFactory.keySet()) {
        configuredBreadcrumbDatumProducerBuilder =
                breadcrumbDatumProducerBuilder
                        .registerProductionEndPointType(productionEndPointType,
                                                        endpoint2datumEnvelopeSenderFactory.get(
                                                                productionEndPointType));
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

  protected final Map<Class, DatumEnvelopeSenderFactory> endpoint2datumEnvelopeSenderFactory = Maps.newHashMap();
  protected ProductionEndPoint breadcrumbsProductionEndPoint;
  protected BreadcrumbsConfig breadcrumbsConfig;
  protected MetricsFactory metricFactory = MetricsFactory.NULL;

  public class EnvelopeTimestampSelector implements DatumType.TimestampSelector<DatumEnvelope> {

    @Override
    public DateTime extractDatumDateTime(DatumEnvelope envelope) {
      return new DateTime(envelope.getLogicalTimestamp());
    }
  }

  protected boolean isBreadcrumbProductionDefined() {
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
      breadcrumbEnv.setProperty("aletheia.breadcrumbs.endpoint.id",
              properties.getProperty("aletheia.breadcrumbs.endpoint.id", ""));
    }
    return breadcrumbEnv;
  }
}
