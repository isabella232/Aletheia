package com.outbrain.aletheia;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeBuilder;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.AuditingDatumProducer;
import com.outbrain.aletheia.datum.production.CompositeDatumProducer;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.production.Sender;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides a fluent API for building a {@link DatumProducer}.
 *
 * @param <TDomainClass> The type of datum for which a {@link DatumProducer} is to be built.
 */
public class DatumProducerBuilder<TDomainClass>
        extends AletheiaBuilder<TDomainClass, DatumProducerBuilder<TDomainClass>> {

  private static final Logger logger = LoggerFactory.getLogger(DatumProducerBuilder.class);


  private final List<ProductionEndPointInfo<TDomainClass>> productionEndPointInfos = Lists.newArrayList();
  private DatumKeySelector<TDomainClass> datumKeySelector;

  private DatumProducerBuilder(final Class<TDomainClass> domainClass) {
    super(domainClass);
  }

  private DatumProducer<TDomainClass> createDatumProducer(final DatumProducerConfig datumProducerConfig,
                                                          final ProductionEndPointInfo<TDomainClass> productionEndPointInfo) {

    logger.info("Creating a datum producer for production end point: {} with config: {}",
            productionEndPointInfo.getProductionEndPoint(),
            datumProducerConfig);

    final BreadcrumbDispatcher<TDomainClass> datumAuditor;
    final boolean isBreadcrumbs = domainClass.equals(Breadcrumb.class);

    if (!isBreadcrumbs) {
      if (isBreadcrumbProductionDefined()) {
        datumAuditor = getTypedBreadcrumbsDispatcher(datumProducerConfig,
                productionEndPointInfo.getProductionEndPoint(),
                getMetricsFactoryProvider());
      } else {
        datumAuditor = BreadcrumbDispatcher.NULL;
      }
    } else {
      datumAuditor = BreadcrumbDispatcher.NULL;
    }

    final Sender<DatumEnvelope> sender =
            getSender(productionEndPointInfo.getProductionEndPoint(),
                    getMetricsFactoryProvider()
                            .forDatumEnvelopeSender(
                                    productionEndPointInfo.getProductionEndPoint(), isBreadcrumbs));

    final DatumEnvelopeBuilder<TDomainClass> datumEnvelopeBuilder =
            new DatumEnvelopeBuilder<>(domainClass,
                    productionEndPointInfo.getDatumSerDe(),
                    datumKeySelector != null ? datumKeySelector : DatumKeySelector.NULL,
                    datumProducerConfig.getIncarnation(),
                    datumProducerConfig.getSource()
            );


    return new AuditingDatumProducer<>(datumEnvelopeBuilder,
            sender,
            productionEndPointInfo.getFilter(),
            datumAuditor,
            getMetricsFactoryProvider()
                    .forAuditingDatumProducer(
                            productionEndPointInfo.getProductionEndPoint(),
                            isBreadcrumbs));
  }

  private NamedSender<DatumEnvelope> getSender(final ProductionEndPoint productionEndPoint,
                                               final MetricsFactory aMetricFactory) {

    final DatumEnvelopeSenderFactory datumEnvelopeSenderFactory = getEnvelopeSenderFactory(productionEndPoint.getClass());

    Preconditions.checkState(datumEnvelopeSenderFactory != null,
            String.format("No datum sender factory for production end point of type [%s] was provided.",
                    productionEndPoint.getClass().getSimpleName()));

    return datumEnvelopeSenderFactory.buildDatumEnvelopeSender(productionEndPoint, aMetricFactory);
  }

  @Override
  protected DatumProducerBuilder<TDomainClass> This() {
    return this;
  }

  /**
   * Adds a production endpoint to deliver data to, using the specified {@link DatumSerDe} instance.
   *
   * @param dataProductionEndPoint the production endpoint to add.
   * @param datumSerDe             the {@link DatumSerDe} instance to use to serialize data.
   * @return a {@link DatumProducerBuilder} instance configured with the specified production endpoint and
   * serialization method.
   */
  public DatumProducerBuilder<TDomainClass> deliverDataTo(final ProductionEndPoint dataProductionEndPoint,
                                                          final DatumSerDe<TDomainClass> datumSerDe) {
    return deliverDataTo(dataProductionEndPoint, datumSerDe, Predicates.<TDomainClass>alwaysTrue());
  }

  /**
   * Adds a production endpoint to deliver data to, using the specified {@link DatumSerDe} and filter instances.
   *
   * @param dataProductionEndPoint the production endpoint to add.
   * @param datumSerDe             the {@link DatumSerDe} instance to use to serialize data.
   * @param datumFilter            a filter to apply before delivering data.
   * @return a {@link DatumProducerBuilder} instance configured with the specified production endpoint,
   * serialization method and filter.
   */
  public DatumProducerBuilder<TDomainClass> deliverDataTo(final ProductionEndPoint dataProductionEndPoint,
                                                          final DatumSerDe<TDomainClass> datumSerDe,
                                                          final Predicate<TDomainClass> datumFilter) {

    productionEndPointInfos.add(new ProductionEndPointInfo<>(dataProductionEndPoint,
            datumSerDe,
            datumFilter));
    return this;
  }

  /**
   * Configures a datum key selection strategy.
   *
   * @param datumKeySelector the DatumKeySelector instance to use in order to select the datum key from incoming data.
   * @return a {@link DatumProducerBuilder} instance configured with the specified DatumKeySelector.
   */
  public DatumProducerBuilder<TDomainClass> selectDatumKeyUsing(final DatumKeySelector<TDomainClass> datumKeySelector) {

    this.datumKeySelector = datumKeySelector;

    return this;
  }

  /**
   * Builds a {@link DatumProducer} instance.
   *
   * @param datumProducerConfig the configuration information to use for building the {@link DatumProducer}
   *                            instance configured.
   * @return a fully configured {@link DatumProducer} instance.
   */
  public DatumProducer<TDomainClass> build(final DatumProducerConfig datumProducerConfig) {

    final List<DatumProducer<TDomainClass>> datumProducers = productionEndPointInfos
            .stream()
            .map(configuredProductionEndPoint -> createDatumProducer(datumProducerConfig, configuredProductionEndPoint))
            .collect(Collectors.toList());

    return new CompositeDatumProducer<>(datumProducers);
  }

  /**
   * Builds a {@link AletheiaBuilder} instance.
   *
   * @param domainClass    the type of the datum to be produced.
   * @param <TDomainClass> the type of the datum to be produced.
   * @return a fluent {@link AletheiaBuilder} to be used for building a {@link DatumProducer} instances.
   */
  @Deprecated
  public static <TDomainClass> DatumProducerBuilder<TDomainClass> forDomainClass(final Class<TDomainClass> domainClass) {
    return new DatumProducerBuilder<>(domainClass);
  }

  public static <TDomainClass> RoutingDatumProducerBuilder<TDomainClass> withConfig(final Class<TDomainClass> domainClass,
                                                                                    final AletheiaConfig config) {
    return new RoutingDatumProducerBuilder<>(domainClass, config);
  }
}
