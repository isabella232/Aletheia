package com.outbrain.aletheia.datum.consumption;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.production.AletheiaBuilder;
import com.outbrain.aletheia.datum.production.DatumProducerConfig;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.metrics.DefaultMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Provides a fluent API for building a {@link DatumConsumerStream}.
 *
 * @param <TDomainClass> The type of datum for which a {@link DatumConsumerStream} is to be built.
 */
public class DatumConsumerStreamsBuilder<TDomainClass> extends AletheiaBuilder<TDomainClass, DatumConsumerStreamsBuilder<TDomainClass>> {

  private static final Logger logger = LoggerFactory.getLogger(DatumConsumerStreamsBuilder.class);

  private static final String DATUM_STREAM = "DatumConsumerStream";

  private static class ConsumptionEndPointInfo<TDomainClass> {

    private final ConsumptionEndPoint consumptionEndPoint;
    private final DatumSerDe<TDomainClass> datumSerDe;
    private final Predicate<TDomainClass> filter;

    private ConsumptionEndPointInfo(final ConsumptionEndPoint consumptionEndPoint,
                                    final DatumSerDe<TDomainClass> datumSerDe,
                                    final Predicate<TDomainClass> filter) {
      this.consumptionEndPoint = consumptionEndPoint;
      this.datumSerDe = datumSerDe;
      this.filter = filter;
    }

    public Predicate<TDomainClass> getFilter() {
      return filter;
    }

    public ConsumptionEndPoint getConsumptionEndPoint() {
      return consumptionEndPoint;
    }

    public DatumSerDe<TDomainClass> getDatumSerDe() {
      return datumSerDe;
    }

  }

  private ConsumptionEndPointInfo<TDomainClass> consumptionEndPointInfo = null;

  private final Map<Class, DatumEnvelopeFetcherFactory> endpoint2datumEnvelopeFetcherFactory =
          Maps.newHashMap();

  private DatumConsumerStreamsBuilder(final Class<TDomainClass> domainClass) {
    super(domainClass);
    registerKnownConsumptionEndPointTypes();
  }

  private void registerKnownConsumptionEndPointTypes() {
    registerConsumptionEndPointType(InMemoryEndPoint.WithBinaryStorage.class,
                                    new InMemoryDatumEnvelopeFetcherFactory());
    registerConsumptionEndPointType(ManualFeedConsumptionEndPoint.class, new InMemoryDatumEnvelopeFetcherFactory());
  }

  private List<DatumConsumerStream<TDomainClass>> createDatumConsumerStream(final DatumProducerConfig datumProducerConfig,
                                                                            final ConsumptionEndPointInfo<TDomainClass> consumptionEndPointInfo) {

    logger.info("Creating a datum stream for end point: {} with config: {}",
                consumptionEndPointInfo.getConsumptionEndPoint(),
                datumProducerConfig);

    final BreadcrumbDispatcher<TDomainClass> datumAuditor;
    final MetricFactoryProvider metricFactoryProvider = new DefaultMetricFactoryProvider(domainClass,
                                                                                         DATUM_STREAM,
                                                                                         metricFactory);
    if (domainClass.equals(Breadcrumb.class) || !isBreadcrumbProductionDefined()) {
      datumAuditor = BreadcrumbDispatcher.NULL;
    } else {
      datumAuditor = getDatumAuditor(datumProducerConfig,
                                     consumptionEndPointInfo.getConsumptionEndPoint(),
                                     metricFactoryProvider);
    }

    final DatumEnvelopeOpener<TDomainClass> datumEnvelopeOpener =
            new DatumEnvelopeOpener<>(datumAuditor,
                                      consumptionEndPointInfo.getDatumSerDe(),
                                      metricFactoryProvider.forDatumEnvelopeMeta(
                                              consumptionEndPointInfo.getConsumptionEndPoint()));

    final DatumEnvelopeFetcherFactory datumEnvelopeFetcherFactory =
            endpoint2datumEnvelopeFetcherFactory.get(consumptionEndPointInfo.getConsumptionEndPoint().getClass());

    Preconditions.checkState(datumEnvelopeFetcherFactory != null,
                             String.format(
                                     "No datum sender factory for production end point of type [%s] was provided.",
                                     consumptionEndPointInfo.getClass().getSimpleName()));

    final List<DatumEnvelopeFetcher> datumEnvelopeFetchers =
            datumEnvelopeFetcherFactory
                    .buildDatumEnvelopeFetcher(consumptionEndPointInfo.getConsumptionEndPoint(),
                                               metricFactoryProvider
                                                       .forDatumEnvelopeFetcher(
                                                               consumptionEndPointInfo.getConsumptionEndPoint()));

    final Function<DatumEnvelopeFetcher, DatumConsumerStream<TDomainClass>> toDatumConsumerStreams =
            new Function<DatumEnvelopeFetcher, DatumConsumerStream<TDomainClass>>() {
              @Override
              public AuditingDatumConsumerStream<TDomainClass> apply(final DatumEnvelopeFetcher datumEnvelopeFetcher) {
                return new AuditingDatumConsumerStream<>(datumEnvelopeFetcher,
                                                         datumEnvelopeOpener,
                                                         consumptionEndPointInfo.getFilter(),
                                                         metricFactoryProvider
                                                                 .forAuditingDatumStreamConsumer(
                                                                         consumptionEndPointInfo.getConsumptionEndPoint()));
              }
            };

    return Lists.transform(datumEnvelopeFetchers, toDatumConsumerStreams);
  }

  @Override
  protected DatumConsumerStreamsBuilder<TDomainClass> This() {
    return this;
  }

  /**
   * Registers a ConsumptionEndPoint type. After the registration, data can be consumed from an instance of this
   * endpoint type.
   *
   * @param consumptionEndPointType     the consumption endpoint to add.
   * @param datumEnvelopeFetcherFactory a {@link DatumEnvelopeFetcherFactory} capable of building
   *                                    {@link DatumEnvelopeFetcher}s from the specified endpoint type.
   * @return a {@link DatumConsumerStreamsBuilder} instance capable of consuming data from the specified consumption
   * endpoint type.
   */
  public <TConsumptionEndPoint extends ConsumptionEndPoint, UConsumptionEndPoint extends TConsumptionEndPoint> DatumConsumerStreamsBuilder<TDomainClass> registerConsumptionEndPointType(
          final Class<TConsumptionEndPoint> consumptionEndPointType,
          final DatumEnvelopeFetcherFactory<? super UConsumptionEndPoint> datumEnvelopeFetcherFactory) {

    endpoint2datumEnvelopeFetcherFactory.put(consumptionEndPointType, datumEnvelopeFetcherFactory);

    return This();
  }

  /**
   * Adds a consumption endpoint to consume data from, using the specified {@link DatumSerDe} instance.
   *
   * @param consumptionEndPoint the consumption endpoint to add.
   * @param datumSerDe          the {@link DatumSerDe} instance to use to serialize data.
   * @return a {@link DatumConsumerStreamsBuilder} instance configured with the specified consumption endpoint and
   * serialization method.
   */
  public DatumConsumerStreamsBuilder<TDomainClass> consumeDataFrom(final ConsumptionEndPoint consumptionEndPoint,
                                                                   final DatumSerDe<TDomainClass> datumSerDe) {
    return consumeDataFrom(consumptionEndPoint, datumSerDe, Predicates.<TDomainClass>alwaysTrue());
  }

  /**
   * Adds a consumption endpoint to consume data from, using the specified {@link DatumSerDe} and filter instances.
   *
   * @param consumptionEndPoint the consumption endpoint to add.
   * @param datumSerDe          the {@link DatumSerDe} instance to use to serialize data.
   * @param datumFilter         a filter to apply before delivering data.
   * @return a {@link DatumConsumerStreamsBuilder} instance configured with the specified consumption endpoint,
   * serialization method and filter.
   */
  public DatumConsumerStreamsBuilder<TDomainClass> consumeDataFrom(final ConsumptionEndPoint consumptionEndPoint,
                                                                   final DatumSerDe<TDomainClass> datumSerDe,
                                                                   final Predicate<TDomainClass> datumFilter) {

    consumptionEndPointInfo = new ConsumptionEndPointInfo<>(consumptionEndPoint, datumSerDe, datumFilter);
    return this;
  }

  /**
   * Builds a {@link DatumConsumerStream} that can be used to consume datums.
   *
   * @param datumConsumerStreamConfig the configuration information to use for building the {@link DatumConsumerStream}
   *                                  instance configured.
   * @return a fully configured {@link DatumConsumerStream} instance.
   */
  public List<DatumConsumerStream<TDomainClass>> build(final DatumConsumerStreamConfig datumConsumerStreamConfig) {
    return createDatumConsumerStream(new DatumProducerConfig(datumConsumerStreamConfig.getIncarnation(),
                                                             datumConsumerStreamConfig.getHostname()),
                                     consumptionEndPointInfo);
  }

  /**
   * Builds a {@link AletheiaBuilder} instance.
   *
   * @param domainClass    the type of the datum to be consumed.
   * @param <TDomainClass> the type of the datum to be consumed.
   * @return a fluent {@link AletheiaBuilder} to be used for building a {@link DatumConsumerStream} instances.
   */
  public static <TDomainClass> DatumConsumerStreamsBuilder<TDomainClass> forDomainClass(final Class<TDomainClass> domainClass) {
    return new DatumConsumerStreamsBuilder<>(domainClass);
  }

}