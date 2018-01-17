package com.outbrain.aletheia;

import com.google.common.collect.Maps;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.StartTimeWithDurationBreadcrumbBaker;
import com.outbrain.aletheia.datum.DatumAuditor;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.InMemoryDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import java.util.Map;

/**
 * @param <TDomainClass> The datum type this builder will be building a
 *                       {@link DatumProducer} or {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream} for.
 * @param <TBuilder>     The concrete type of builder, used for type safety, to be filled in by deriving classes.
 */
abstract class AletheiaBuilder<TDomainClass, TBuilder extends AletheiaBuilder<TDomainClass, ?>> extends BaseAletheiaBuilder {

  /**
   * A special case {@link AletheiaMetricFactoryProvider} used only when reporting metrics from a
   * breadcrumb dedicated {@link DatumProducer}, that is, a {@link DatumProducer} whose only purpose in life
   * is to produce breadcrumbs (breadcrumb is itself, a special kind of datum).
   */

  protected final Class<TDomainClass> domainClass;
  protected final Map<Class, DatumEnvelopeSenderFactory> endpoint2datumEnvelopeSenderFactory = Maps.newHashMap();
  protected MetricsFactory metricFactory = MetricsFactory.NULL;

  protected AletheiaBuilder(final Class<TDomainClass> domainClass) {
    this.domainClass = domainClass;
    registerKnownProductionEndPointsTypes();
  }

  protected BreadcrumbDispatcher<TDomainClass> getTypedBreadcrumbsDispatcher(final DatumProducerConfig datumProducerConfig,
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

  protected void registerKnownProductionEndPointsTypes() {
    final InMemoryDatumEnvelopeSenderFactory datumEnvelopeSenderFactory = new InMemoryDatumEnvelopeSenderFactory();
    this.registerProductionEndPointType(InMemoryEndPoint.class, datumEnvelopeSenderFactory);
  }

  /**
   * Registers a ProductionEndPoint type. After the registration, data can be produced to an instance of this endpoint
   * type.
   *
   * @param endPointType               the type of the custom endpoint to register.
   * @param datumEnvelopeSenderFactory a {@link DatumEnvelopeSenderFactory} capable of building
   *                                   {@link NamedSender<com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope>}'s from the specified endpoint type.
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
