package com.outbrain.aletheia.datum.consumption;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
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
 * Created by slevin on 7/10/14.
 */
public class DatumConsumerBuilder<TDomainClass> extends AletheiaBuilder<TDomainClass, DatumConsumerBuilder<TDomainClass>> {

  private static final Logger logger = LoggerFactory.getLogger(DatumConsumerBuilder.class);

  private static final String DATUM_CONSUMER = "DatumConsumer";

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

  private final List<ConsumptionEndPointInfo<TDomainClass>> consumptionEndPointInfos = Lists.newArrayList();

  private final Map<Class, DatumEnvelopeFetcherFactory> endpoint2datumEnvelopeFetcherFactory =
          Maps.newHashMap();

  private DatumConsumerBuilder(final Class<TDomainClass> domainClass) {
    super(domainClass);
    registerKnownConsumptionEndPointTypes();
  }

  private void registerKnownConsumptionEndPointTypes() {
    registerConsumptionEndPointType(ManualFeedConsumptionEndPoint.class, new ManualFeedDatumEnvelopeFetcherFactory());
  }

  private AuditingDatumConsumer<TDomainClass> datumConsumer(final DatumProducerConfig datumProducerConfig,
                                                            final ConsumptionEndPointInfo<TDomainClass> consumptionEndPointInfo) {

    logger.info("Creating a datum consumer for end point: {} with config: {}",
                consumptionEndPointInfo.getConsumptionEndPoint(),
                datumProducerConfig);

    final BreadcrumbDispatcher<TDomainClass> datumAuditor;
    final MetricFactoryProvider metricFactoryProvider;

    if (isBreadcrumbProductionDefined()) {
      metricFactoryProvider = new DefaultMetricFactoryProvider(domainClass, DATUM_CONSUMER, metricFactory);
      datumAuditor = getDatumAuditor(datumProducerConfig,
                                     consumptionEndPointInfo.getConsumptionEndPoint(),
                                     metricFactoryProvider);
    } else {
      metricFactoryProvider = new InternalBreadcrumbProducerMetricFactoryProvider(domainClass,
                                                                                  DATUM_CONSUMER,
                                                                                  metricFactory);
      datumAuditor = BreadcrumbDispatcher.NULL;
    }

    final DatumEnvelopeOpener<TDomainClass> datumEnvelopeOpener =
            new DatumEnvelopeOpener<>(datumAuditor,
                                      consumptionEndPointInfo.getDatumSerDe(),
                                      metricFactoryProvider.forDatumEnvelopeMeta(
                                              consumptionEndPointInfo.getConsumptionEndPoint()));

    //TODO should be build according to productionEndPointWithSerDe.getProductionEndPoint()

    final DatumEnvelopeFetcherFactory datumEnvelopeFetcherFactory =
            endpoint2datumEnvelopeFetcherFactory.get(consumptionEndPointInfo.getConsumptionEndPoint().getClass());

    final DatumEnvelopeFetcher datumEnvelopeFetcher =
            datumEnvelopeFetcherFactory
                    .buildDatumEnvelopeFetcher(consumptionEndPointInfo.getConsumptionEndPoint(),
                                               metricFactoryProvider
                                                       .forDatumEnvelopeFetcher(
                                                               consumptionEndPointInfo.getConsumptionEndPoint()));

    return new AuditingDatumConsumer<>(datumEnvelopeFetcher,
                                       datumEnvelopeOpener,
                                       consumptionEndPointInfo.getFilter(),
                                       metricFactoryProvider
                                               .forAuditingDatumConsumer(
                                                       consumptionEndPointInfo.getConsumptionEndPoint()));
  }

  @Override
  protected DatumConsumerBuilder<TDomainClass> This() {
    return this;
  }

  public <TConsumptionEndPoint extends ConsumptionEndPoint, UConsumptionEndPoint extends TConsumptionEndPoint> DatumConsumerBuilder<TDomainClass> registerConsumptionEndPointType(
          final Class<TConsumptionEndPoint> endPointType,
          final DatumEnvelopeFetcherFactory<UConsumptionEndPoint> datumEnvelopeFetcherFactory) {

    endpoint2datumEnvelopeFetcherFactory.put(endPointType, datumEnvelopeFetcherFactory);

    return This();
  }

  public DatumConsumerBuilder<TDomainClass> addConsumptionEndPoint(final ConsumptionEndPoint consumptionEndPoint,
                                                                    final DatumSerDe<TDomainClass> datumSerDe) {
    return addConsumptionEndPoint(consumptionEndPoint, datumSerDe, Predicates.<TDomainClass>alwaysTrue());
  }

  public DatumConsumerBuilder<TDomainClass> addConsumptionEndPoint(final ConsumptionEndPoint consumptionEndPoint,
                                                                    final DatumSerDe<TDomainClass> datumSerDe,
                                                                    final Predicate<TDomainClass> filter) {

    consumptionEndPointInfos.add(new ConsumptionEndPointInfo<>(consumptionEndPoint, datumSerDe, filter));
    return this;
  }

  public Map<ConsumptionEndPoint, DatumConsumer<TDomainClass>> build(final DatumProducerConfig breadcrumbProducerConfig) {

    final Map<ConsumptionEndPoint, DatumConsumer<TDomainClass>> consumptionEndPoint2datumConsumer = Maps.newHashMap();

    for (final ConsumptionEndPointInfo<TDomainClass> consumptionEndPointInfo : consumptionEndPointInfos) {
      final ConsumptionEndPoint consumptionEndPoint = consumptionEndPointInfo.getConsumptionEndPoint();
      final AuditingDatumConsumer<TDomainClass> datumConsumer = datumConsumer(breadcrumbProducerConfig,
                                                                              consumptionEndPointInfo);
      consumptionEndPoint2datumConsumer.put(consumptionEndPoint, datumConsumer);
    }

    return consumptionEndPoint2datumConsumer;
  }

  public static <TDomainClass> DatumConsumerBuilder<TDomainClass> forDomainType(final Class<TDomainClass> domainClass) {
    return new DatumConsumerBuilder<>(domainClass);
  }

}