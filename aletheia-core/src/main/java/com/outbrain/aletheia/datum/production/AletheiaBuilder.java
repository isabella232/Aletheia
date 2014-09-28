package com.outbrain.aletheia.datum.production;

import com.google.common.collect.Maps;
import com.outbrain.aletheia.EndPoint;
import com.outbrain.aletheia.breadcrumbs.*;
import com.outbrain.aletheia.datum.DatumAuditor;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryPrefixer;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.Map;

/**
 * @param <TDomainClass> The datum type this builder will be building a
 *                       <code>DatumProducer</code> or <code>DatumConsumer</code> for.
 * @param <TBuilder>     The concrete type of builder, used for type safety, to be filled in by deriving classes.
 */
public abstract class AletheiaBuilder<TDomainClass, TBuilder extends AletheiaBuilder<TDomainClass, ?>> {

  /**
   * A special case <code>AletheiaMetricFactoryProvider</code> used only when reporting metrics from a
   * breadcrumb dedicated <code>DatumProducer</code>, that is, a <code>DatumProducer</code> whose only purpose in life
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
    public MetricsFactory forAuditingDatumConsumer(final EndPoint endPoint) {
      throw new IllegalStateException(
              "No MetricFactory for datum envelope consumer instance should be asked for when already in internal breadcrumb producer mode.");
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
                                                        endpoint2datumEnvelopeSenderFactory.get(productionEndPointType));
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
  protected ProductionEndPoint breadcrumbsProductionEndPoint;
  protected BreadcrumbsConfig breadcrumbsConfig;
  protected MetricsFactory metricFactory = MetricsFactory.NULL;

  public AletheiaBuilder(final Class<TDomainClass> domainClass) {
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
    this.registerProductionEndPointType(InMemoryProductionEndPoint.class, new InMemoryDatumEnvelopeSenderFactory());
  }

  /**
   * Configures <code>Breadcrumb</code> sending to a given destination, and configuration.
   *
   * @param breadcrumbProductionEndPoint a <code>ProductionEndPoint</code> instance where
   *                                     <code>Breadcrumbs</code> will be sent.
   * @param breadcrumbsConfig            a configuration for the breadcrumb dispatching mechanism.
   * @return A <code>TBuilder</code> instance whose breadcrumbs have been configured.
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
   * @param datumEnvelopeSenderFactory a <code>DatumEnvelopeSenderFactory</code> capable of building
   *                                   <code>DatumEnvelopeSender</code>s from the specified endpoint type.
   * @return A <code>TBuilder</code> instance with the custom production endpoint registered.
   */
  public <TProductionEndPoint extends ProductionEndPoint, UProductionEndPoint extends TProductionEndPoint> TBuilder registerProductionEndPointType(
          final Class<TProductionEndPoint> endPointType,
          final DatumEnvelopeSenderFactory<UProductionEndPoint> datumEnvelopeSenderFactory) {

    endpoint2datumEnvelopeSenderFactory.put(endPointType, datumEnvelopeSenderFactory);

    return This();
  }

  /**
   * Configures metrics reporting.
   *
   * @param metricFactory A MetricsFactory instance to report metrics to.
   * @return A <code>TBuilder</code> instance with metrics reporting configured.
   */
  public TBuilder reportMetricsTo(final MetricsFactory metricFactory) {
    this.metricFactory = metricFactory;

    return This();
  }
}
