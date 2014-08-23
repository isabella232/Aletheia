package com.outbrain.aletheia.datum.production;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.envelope.DatumEnvelopeBuilder;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.DefaultMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by slevin on 7/10/14.
 */
public class DatumProducerBuilder<TDomainClass> extends AletheiaBuilder<TDomainClass, DatumProducerBuilder<TDomainClass>> {

  protected static final Logger logger = LoggerFactory.getLogger(DatumProducerBuilder.class);

  protected static final String DATUM_PRODUCER = "DatumProducer";

  private static class ProductionEndPointInfo<T> {

    private final ProductionEndPoint productionEndPoint;

    private final DatumSerDe<T> datumSerDe;

    private final Predicate<T> filter;

    private ProductionEndPointInfo(final ProductionEndPoint productionEndPoint,
                                   final DatumSerDe<T> datumSerDe,
                                   final Predicate<T> filter) {
      this.productionEndPoint = productionEndPoint;
      this.datumSerDe = datumSerDe;
      this.filter = filter;
    }

    public Predicate<T> getFilter() {
      return filter;
    }

    public ProductionEndPoint getProductionEndPoint() {
      return productionEndPoint;
    }

    public DatumSerDe<T> getDatumSerDe() {
      return datumSerDe;
    }

  }

  public static <TDomainClass> DatumProducerBuilder<TDomainClass> forDomainClass(final Class<TDomainClass> domainClass) {
    return new DatumProducerBuilder<>(domainClass);
  }

  private final List<ProductionEndPointInfo<TDomainClass>> productionEndPointInfos = Lists.newArrayList();

  private DatumProducerBuilder(final Class<TDomainClass> domainClass) {
    super(domainClass);
  }

  private DatumProducer<TDomainClass> createDatumProducer(final DatumProducerConfig datumProducerConfig,
                                                          final ProductionEndPointInfo<TDomainClass> productionEndPointInfo) {

    logger.info("Creating a datum producer for production end point: {} with config: {}",
                productionEndPointInfo.getProductionEndPoint(),
                datumProducerConfig);

    final BreadcrumbDispatcher<TDomainClass> datumAuditor;
    final MetricFactoryProvider metricFactoryProvider;

    if (!domainClass.equals(Breadcrumb.class)) {
      metricFactoryProvider = new DefaultMetricFactoryProvider(domainClass, DATUM_PRODUCER, metricFactory);

      if (isBreadcrumbProductionDefined()) {
        datumAuditor = getDatumAuditor(datumProducerConfig,
                                       productionEndPointInfo.getProductionEndPoint(),
                                       metricFactoryProvider);
      } else {
        datumAuditor = BreadcrumbDispatcher.NULL;
      }
    } else {
      metricFactoryProvider = new InternalBreadcrumbProducerMetricFactoryProvider(domainClass,
                                                                                  DATUM_PRODUCER,
                                                                                  metricFactory);
      datumAuditor = BreadcrumbDispatcher.NULL;
    }

    final Sender<DatumEnvelope> sender =
            getSender(productionEndPointInfo.getProductionEndPoint(),
                      metricFactoryProvider
                              .forDatumEnvelopeSender(productionEndPointInfo.getProductionEndPoint()));

    final DatumEnvelopeBuilder<TDomainClass> datumEnvelopeBuilder =
            new DatumEnvelopeBuilder<>(productionEndPointInfo.getDatumSerDe(),
                                       datumProducerConfig.getIncarnation(),
                                       datumProducerConfig.getHostname());


    return new AuditingDatumProducer<>(datumEnvelopeBuilder,
                                       sender,
                                       productionEndPointInfo.getFilter(),
                                       datumAuditor,
                                       metricFactoryProvider
                                               .forAuditingDatumProducer(
                                                       productionEndPointInfo.getProductionEndPoint()));
  }

  private NamedSender<DatumEnvelope> getSender(final ProductionEndPoint productionEndPoint,
                                               final MetricsFactory aMetricFactory) {

    final DatumEnvelopeSenderFactory datumEnvelopeSenderFactory = endpoint2datumEnvelopeSenderFactory.get(
            productionEndPoint.getClass());

    Preconditions.checkState(datumEnvelopeSenderFactory != null,
                             String.format("No transporter builder for production end point of type [%s] was provided.",
                                           productionEndPoint.getClass().getSimpleName()));

    return datumEnvelopeSenderFactory.buildDatumEnvelopeSender(productionEndPoint, aMetricFactory);
  }

  @Override
  protected DatumProducerBuilder<TDomainClass> This() {
    return this;
  }

  public DatumProducerBuilder<TDomainClass> deliverDataTo(final ProductionEndPoint dataProductionEndPoint,
                                                          final DatumSerDe<TDomainClass> datumSerDe) {
    return deliverDataTo(dataProductionEndPoint, datumSerDe, Predicates.<TDomainClass>alwaysTrue());
  }

  public <TProductionEndPoint extends ProductionEndPoint> DatumProducerBuilder<TDomainClass> deliverDataTo(final TProductionEndPoint dataProductionEndPoint,
                                                                                                           final DatumSerDe<TDomainClass> datumSerDe,
                                                                                                           final Predicate<TDomainClass> datumFilter) {

    productionEndPointInfos.add(new ProductionEndPointInfo<>(dataProductionEndPoint,
                                                             datumSerDe,
                                                             datumFilter));
    return this;
  }

  public DatumProducer<TDomainClass> build(final DatumProducerConfig datumProducerConfig) {

    final Function<ProductionEndPointInfo<TDomainClass>, DatumProducer<TDomainClass>> toDatumProducer =
            new Function<ProductionEndPointInfo<TDomainClass>, DatumProducer<TDomainClass>>() {
              @Override
              public DatumProducer<TDomainClass> apply(final ProductionEndPointInfo<TDomainClass> configuredProductionEndPoint) {
                return createDatumProducer(datumProducerConfig, configuredProductionEndPoint);
              }
            };

    final Collection<DatumProducer<TDomainClass>> datumProducers =
            Collections2.transform(productionEndPointInfos,
                                   toDatumProducer);
    return new CompositeDatumProducer<>(Lists.newArrayList(datumProducers));
  }

}