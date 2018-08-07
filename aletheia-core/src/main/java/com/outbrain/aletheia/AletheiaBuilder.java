package com.outbrain.aletheia;

import com.outbrain.aletheia.breadcrumbs.AggregatingBreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.StartTimeWithDurationBreadcrumbBaker;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.PeriodicBreadcrumbDispatcher;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import org.joda.time.Duration;

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

  AletheiaBuilder(final Class<TDomainClass> domainClass) {
    this.domainClass = domainClass;
  }

  BreadcrumbDispatcher<TDomainClass> getTypedBreadcrumbsDispatcher(final DatumProducerConfig datumProducerConfig,
                                                                   final EndPoint endPoint,
                                                                   final MetricFactoryProvider metricFactoryProvider) {

    final BreadcrumbDispatcher<TDomainClass> breadcrumbDispatcher = new AggregatingBreadcrumbDispatcher<>(
        breadcrumbsConfig.getBreadcrumbBucketDuration(),
        DatumUtils.getDatumTimestampExtractor(domainClass),
        new StartTimeWithDurationBreadcrumbBaker(breadcrumbsConfig.getSource(),
            endPoint.getName(),
            breadcrumbsConfig.getTier(),
            breadcrumbsConfig.getDatacenter(),
            breadcrumbsConfig.getApplication(),
            DatumUtils.getDatumTypeId(domainClass)),
        new BreadcrumbProducingHandler(datumProducerConfig,
                metricFactoryProvider),
        Duration.standardDays(1));

    return new PeriodicBreadcrumbDispatcher<>(breadcrumbDispatcher, breadcrumbsConfig.getBreadcrumbBucketFlushInterval());
  }

  protected abstract TBuilder This();

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

    this.<TProductionEndPoint, UProductionEndPoint>registerEnvelopeSenderType(endPointType, datumEnvelopeSenderFactory);

    return This();
  }

  /**
   * Registers a ConsumptionEndPoint type. After the registration, data can be consumed from an instance of this
   * endpoint type.
   *
   * @param consumptionEndPointType     the consumption endpoint to add.
   * @param datumEnvelopeFetcherFactory a {@link DatumEnvelopeFetcherFactory} capable of building
   *                                    {@link DatumEnvelopeFetcher}s from the specified endpoint type.
   * @return a {@link TBuilder} instance capable of consuming data from the specified consumption
   * endpoint type.
   */
  public <TConsumptionEndPoint extends ConsumptionEndPoint, UConsumptionEndPoint extends TConsumptionEndPoint> TBuilder registerConsumptionEndPointType(
      final Class<TConsumptionEndPoint> consumptionEndPointType,
      final DatumEnvelopeFetcherFactory<? super UConsumptionEndPoint> datumEnvelopeFetcherFactory) {

    this.<TConsumptionEndPoint, UConsumptionEndPoint>registerEnvelopeFetcherType(consumptionEndPointType, datumEnvelopeFetcherFactory);

    return This();
  }

  /**
   * Configures metrics reporting.
   *
   * @param metricFactoryProvider A MetricsFactoryProvider instance to report metrics to.
   * @return A {@link TBuilder} instance with metrics reporting configured.
   */
  public TBuilder reportMetricsTo(final MetricFactoryProvider metricFactoryProvider) {
    setMetricsFactoryProvider(metricFactoryProvider);

    return This();
  }
}
