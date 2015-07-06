package com.outbrain.aletheia;

import com.google.common.base.Preconditions;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Provides a fluent API for building a {@link DatumConsumerStream}.
 *
 * @param <TDomainClass> The type of datum for which a {@link DatumConsumerStream} is to be built.
 */
public class RoutingDatumConsumerStreamsBuilder<TDomainClass>
        extends RoutingAletheiaBuilder<TDomainClass, RoutingDatumConsumerStreamsBuilder<TDomainClass>> {

  private static final Logger logger = LoggerFactory.getLogger(RoutingDatumConsumerStreamsBuilder.class);

  private AletheiaConfig config;

  RoutingDatumConsumerStreamsBuilder(final Class<TDomainClass> domainClass, final AletheiaConfig config) {
    super(domainClass, config);
    saveBuilder(DatumConsumerStreamsBuilder.forDomainClass(domainClass));
    this.config = new AletheiaConfig(properties);
  }

  private DatumConsumerStreamsBuilder<TDomainClass> getBuilder() {
    return (DatumConsumerStreamsBuilder<TDomainClass>) this.builder;
  }

  @Override
  protected RoutingDatumConsumerStreamsBuilder<TDomainClass> This() {
    return this;
  }

  /**
   * Registers a ConsumptionEndPoint type. After the registration, data can be consumed from an instance of this
   * endpoint type.
   *
   * @param consumptionEndPointType     the consumption endpoint to add.
   * @param datumEnvelopeFetcherFactory a {@link DatumEnvelopeFetcherFactory} capable of building
   *                                    {@link DatumEnvelopeFetcher}s from the specified endpoint type.
   * @return a {@link RoutingDatumConsumerStreamsBuilder} instance capable of consuming data from the specified consumption
   * endpoint type.
   */
  public <TConsumptionEndPoint extends ConsumptionEndPoint, UConsumptionEndPoint extends TConsumptionEndPoint> RoutingDatumConsumerStreamsBuilder<TDomainClass> registerConsumptionEndPointType(
          final Class<TConsumptionEndPoint> consumptionEndPointType,
          final DatumEnvelopeFetcherFactory<? super UConsumptionEndPoint> datumEnvelopeFetcherFactory) {
    saveBuilder(getBuilder().registerConsumptionEndPointType(consumptionEndPointType, datumEnvelopeFetcherFactory));
    return This();
  }

  private RoutingDatumConsumerStreamsBuilder<TDomainClass> consumeDataFrom(final String endPointId,
                                                                           final DatumSerDe<TDomainClass> datumSerDe) {

    final ConsumptionEndPoint consumptionEndPoint = config.getConsumptionEndPoint(endPointId);

    Preconditions.checkNotNull(consumptionEndPoint,
                               "Could not resolve consumption endpoint id: \"%s\"",
                               endPointId);

    saveBuilder(getBuilder().consumeDataFrom(consumptionEndPoint, datumSerDe));

    configureBreadcrumbProduction();

    return this;
  }

  public RoutingDatumConsumerStreamsBuilder<TDomainClass> consumeDataFrom(final Route route) {
    return consumeDataFrom(route.getEndPointId(),
                           config.<TDomainClass>serDe(route.getSerDeId()));
  }

  /**
   * Builds a {@link DatumConsumerStream} that can be used to consume data.
   *
   * @return a fully configured {@link DatumConsumerStream} instance.
   */
  public List<DatumConsumerStream<TDomainClass>> build() {
    return getBuilder().build(config.getDatumConsumerConfig());
  }
}