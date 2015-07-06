package com.outbrain.aletheia;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.configuration.routing.RoutingInfo;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by slevin on 6/28/15.
 */
public class RoutingDatumProducerBuilder<TDomainClass>
        extends RoutingAletheiaBuilder<TDomainClass, RoutingDatumProducerBuilder<TDomainClass>> {

  private static final Logger logger = LoggerFactory.getLogger(RoutingDatumProducerBuilder.class);

  private AletheiaConfig config;

  RoutingDatumProducerBuilder(final Class<TDomainClass> domainClass, final AletheiaConfig properties) {
    super(domainClass, properties);
    saveBuilder(DatumProducerBuilder.forDomainClass(domainClass));
    config = new AletheiaConfig(this.properties);
  }

  private void addRoutes(final AletheiaConfig config) {

    final RoutingInfo routingInfo = config.getRouting(DatumUtils.getDatumTypeId(domainClass));

    validateProductionRoutingInfo(routingInfo);

    final ImmutableList<ProductionEndPointInfo> productionEndPointInfos =
            FluentIterable
                    .from(routingInfo.getRoutes())
                    .transform(new Function<Route, ProductionEndPointInfo>() {
                      @Override
                      public ProductionEndPointInfo apply(final Route route) {

                        final ProductionEndPoint productionEndPoint =
                                config.getProductionEndPoint(route.getEndPointId());
                        Preconditions.checkNotNull(productionEndPoint,
                                                   "Could not resolve production endpoint id: \"%s\"",
                                                   route.getEndPointId());

                        final DatumSerDe<TDomainClass> datumSerDe = config.serDe(route.getSerDeId());
                        Preconditions.checkNotNull(datumSerDe,
                                                   "Could not resolve serDe id: \"%s\"",
                                                   route.getSerDeId());

                        return new ProductionEndPointInfo<>(productionEndPoint, datumSerDe, null);
                      }
                    })
                    .toList();

    for (final ProductionEndPointInfo productionEndPointInfo : distinctEndPoints(productionEndPointInfos)) {
      saveBuilder(getBuilder().deliverDataTo(productionEndPointInfo.getProductionEndPoint(),
                                             productionEndPointInfo.getDatumSerDe()));

      logger.warn("Production endpoint {} with serDe {} has been added to pipeline.",
                  productionEndPointInfo.getProductionEndPoint(),
                  productionEndPointInfo.getDatumSerDe().getClass().getSimpleName());
    }
  }

  private List<ProductionEndPointInfo> distinctEndPoints(final List<ProductionEndPointInfo> productionEndPointInfos) {
    final ImmutableListMultimap<Integer, ProductionEndPointInfo> groupedProductionEndPointInfos =
            Multimaps.index(productionEndPointInfos, new Function<ProductionEndPointInfo, Integer>() {
              @Override
              public Integer apply(final ProductionEndPointInfo productionEndPointInfo) {
                return productionEndPointInfo.getProductionEndPoint().hashCode();
              }
            });

    return FluentIterable
            .from(groupedProductionEndPointInfos.asMap().values())
            .transform(new Function<Collection<ProductionEndPointInfo>, ProductionEndPointInfo>() {
              @Override
              public ProductionEndPointInfo apply(final Collection<ProductionEndPointInfo> equivalent) {
                return Iterables.getFirst(equivalent, null);
              }
            })
            .toList();
  }

  private void validateProductionRoutingInfo(final RoutingInfo routingInfo) {
    final String datumTypeId = DatumUtils.getDatumTypeId(domainClass);
    Preconditions.checkNotNull(routingInfo,
                               "No routing information for datum type id \"%s\" was found.",
                               datumTypeId);
    Preconditions.checkState(routingInfo.getRoutes().size() > 0,
                             "No routes were configured for datum type id \"%s\".",
                             datumTypeId);
  }

  private DatumProducerBuilder<TDomainClass> getBuilder() {
    return (DatumProducerBuilder<TDomainClass>) this.builder;
  }

  @Override
  protected RoutingDatumProducerBuilder<TDomainClass> This() {
    return this;
  }

  /**
   * Registers a ProductionEndPoint type. After the registration, data can be produced to an instance of this endpoint
   * type.
   *
   * @param endPointType               the type of the custom endpoint to register.
   * @param datumEnvelopeSenderFactory a {@link DatumEnvelopeSenderFactory} capable of building
   *                                   {@link com.outbrain.aletheia.datum.production.NamedSender<com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope>}'s from the specified endpoint type.
   * @return A {@link RoutingDatumProducerBuilder} instance with the custom production endpoint registered.
   */
  public <TProductionEndPoint extends ProductionEndPoint, UProductionEndPoint extends TProductionEndPoint> RoutingDatumProducerBuilder<TDomainClass> registerProductionEndPointType(
          final Class<TProductionEndPoint> endPointType,
          final DatumEnvelopeSenderFactory<? super UProductionEndPoint> datumEnvelopeSenderFactory) {
    builder.registerProductionEndPointType(endPointType, datumEnvelopeSenderFactory);
    return This();
  }

  /**
   * Builds a {@link DatumProducer} instance.
   *
   * @return a fully configured {@link DatumProducer} instance.
   */
  public DatumProducer<TDomainClass> build() {

    final RoutingInfo routingInfo = config.getRouting(DatumUtils.getDatumTypeId(domainClass));

    // the case of a null datum key selectors is handled RoutingInfo.
    saveBuilder(getBuilder().selectDatumKeyUsing(routingInfo.getDatumKeySelector()));

    addRoutes(config);

    configureBreadcrumbProduction();

    return getBuilder().build(config.getDatumProducerConfig());
  }
}
