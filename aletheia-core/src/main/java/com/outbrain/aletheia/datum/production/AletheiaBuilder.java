package com.outbrain.aletheia.datum.production;

import com.google.common.collect.Maps;
import com.outbrain.aletheia.breadcrumbs.*;
import com.outbrain.aletheia.datum.DatumAuditor;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryPrefixer;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.Map;

/**
 * @param <TDomainClass> The datum type this builder will be building a
 *                       {@link DatumProducer} or {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream} for.
 * @param <TBuilder>     The concrete type of builder, used for type safety, to be filled in by deriving classes.
 */
public abstract class AletheiaBuilder<TDomainClass, TBuilder extends AletheiaBuilder<TDomainClass, ?>> {

  /**
   * A special case {@link AletheiaMetricFactoryProvider} used only when reporting metrics from a
   * breadcrumb dedicated {@link DatumProducer}, that is, a {@link DatumProducer} whose only purpose in life
   * is to produce breadcrumbs (breadcrumb is itself, a special kind of datum).
   */
  protected static class InternalBreadcrumbProducerMetricFactoryProvider extends AletheiaMetricFactoryProvider {

    public InternalBreadcrumbProducerMetricFactoryProvider(final Class domainClass,
                                                           final String componentName,
                                                           final MetricsFactory metricsFactory) {
      super(domainClass, componentName, metricsFactory);
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

  }

  protected final Class<TDomainClass> domainClass;
  protected final Map<Class, DatumEnvelopeSenderFactory> endpoint2datumEnvelopeSenderFactory = Maps.newHashMap();
  private ProductionEndPoint breadcrumbsProductionEndPoint;
  private BreadcrumbsConfig breadcrumbsConfig;
  protected MetricsFactory metricFactory = MetricsFactory.NULL;

  protected AletheiaBuilder(final Class<TDomainClass> domainClass) {
    this.domainClass = domainClass;
    registerKnownProductionEndPointsTypes();
  }

  protected BreadcrumbDispatcher<TDomainClass> getDatumAuditor(final DatumProducerConfig datumProducerConfig,
                                                               final EndPoint endPoint,
                                                               final MetricFactoryProvider metricFactoryProvider) {
    return new DatumAuditor<>(
            breadcrumbsConfig.getBreadcrumbBucketDuration(),
            DatumUtils.getDatumTimestampExtractor(domainClass),
            new StartTimeWithDurationBreadcrumbBaker(breadcrumbsConfig.getSource(),
                                                     endPoint.getName(),
                                                     breadcrumbsConfig.getTier(),
                                                     breadcrumbsConfig.getDatacenter(),
                                                     breadcrumbsConfig.getApplication(),
                                                     DatumUtils.getDatumTypeId(domainClass)),
            new BreadcrumbProducingHandler(datumProducerConfig,
                                           metricFactoryProvider.forInternalBreadcrumbProducer(endPoint)),
            breadcrumbsConfig.getBreadcrumbBucketFlushInterval());
  }

  protected abstract TBuilder This();

  protected boolean isBreadcrumbProductionDefined() {
    return breadcrumbsConfig != null && breadcrumbsProductionEndPoint != null;
  }

  protected void registerKnownProductionEndPointsTypes() {
    final InMemoryDatumEnvelopeSenderFactory datumEnvelopeSenderFactory = new InMemoryDatumEnvelopeSenderFactory();
    this.registerProductionEndPointType(InMemoryEndPoint.class, datumEnvelopeSenderFactory);
  }

  /**
   * Configures {@link Breadcrumb} sending to a given destination, and configuration.
   *
   * @param breadcrumbProductionEndPoint a {@link ProductionEndPoint} instance where
   *                                     {@link Breadcrumb} will be sent.
   * @param breadcrumbsConfig            a configuration for the breadcrumb dispatching mechanism.
   * @return A {@link TBuilder} instance whose breadcrumbs have been configured.
   */
  public TBuilder deliverBreadcrumbsTo(final ProductionEndPoint breadcrumbProductionEndPoint,
                                       final BreadcrumbsConfig breadcrumbsConfig) {

    this.breadcrumbsProductionEndPoint = breadcrumbProductionEndPoint;
    this.breadcrumbsConfig = breadcrumbsConfig;

    return This();
  }

  /**
   * Registers a ProductionEndPoint type. After the registration, data can be produced to an instance of this endpoint
   * type.
   *
   * @param endPointType               the type of the custom endpoint to register.
   * @param datumEnvelopeSenderFactory a {@link DatumEnvelopeSenderFactory} capable of building
   *                                   {@link com.outbrain.aletheia.datum.production.NamedSender<com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope>}'s from the specified endpoint type.
   * @return A {@link TBuilder} instance with the custom production endpoint registered.
   */
  public <TProductionEndPoint extends ProductionEndPoint, UProductionEndPoint extends TProductionEndPoint> TBuilder registerProductionEndPointType(
          final Class<TProductionEndPoint> endPointType,
          final DatumEnvelopeSenderFactory<? super UProductionEndPoint> datumEnvelopeSenderFactory) {

    endpoint2datumEnvelopeSenderFactory.put(endPointType, datumEnvelopeSenderFactory);

    return This();
  }

  /**
   * Configures metrics reporting.
   *
   * @param metricFactory A MetricsFactory instance to report metrics to.
   * @return A {@link TBuilder} instance with metrics reporting configured.
   */
  public TBuilder reportMetricsTo(final MetricsFactory metricFactory) {
    this.metricFactory = metricFactory;

    return This();
  }
}
