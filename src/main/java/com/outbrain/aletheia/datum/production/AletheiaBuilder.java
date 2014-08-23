package com.outbrain.aletheia.datum.production;

import com.google.common.collect.Maps;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDatumSerDe;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbsConfig;
import com.outbrain.aletheia.datum.auditing.BreadcrumbHandler;
import com.outbrain.aletheia.datum.auditing.DatumAuditor;
import com.outbrain.aletheia.datum.auditing.DatumBreadcrumbBaker;
import com.outbrain.aletheia.datum.auditing.DatumBucketKeySelector;
import com.outbrain.aletheia.EndPoint;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryPrefixer;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Created by slevin on 8/9/14.
 */
public abstract class AletheiaBuilder<TDomainClass, TBuilder extends AletheiaBuilder<TDomainClass, ?>> {

  /**
   * Created by slevin on 8/12/14.
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
              "No MetricFactory for datum envelope receiver instance should be asked for when already in internal breadcrumb producer mode.");
    }

    @Override
    public MetricsFactory forAuditingDatumConsumer(final EndPoint endPoint) {
      throw new NotImplementedException();
    }

    @Override
    public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
      throw new NotImplementedException();
    }
  }

  protected class BreadcrumbTransportingHandler implements BreadcrumbHandler {

    private final DatumProducer<Breadcrumb> breadcrumbDatumProducer;

    public BreadcrumbTransportingHandler(final DatumProducerConfig datumProducerConfig,
                                         final MetricsFactory metricsFactory) {

      final DatumProducerBuilder<Breadcrumb> breadcrumbProducerBuilder =
              configurableBreadcrumbProducerBuilder(metricsFactory);

      breadcrumbDatumProducer = registerEndPointTypes(breadcrumbProducerBuilder).build(datumProducerConfig);
    }

    private DatumProducerBuilder<Breadcrumb> configurableBreadcrumbProducerBuilder(final MetricsFactory metricsFactory) {
      return DatumProducerBuilder
              .forDomainClass(Breadcrumb.class)
              .reportMetricsTo(metricsFactory)
              .deliverDataTo(breadcrumbsProductionEndPoint, new BreadcrumbDatumSerDe());
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
            new DatumBucketKeySelector<TDomainClass>(this.breadcrumbsConfig.getBreadcrumbBucketDuration()),
            new DatumBreadcrumbBaker(breadcrumbsConfig.getSource(),
                                     endPoint.getName(),
                                     breadcrumbsConfig.getTier(),
                                     breadcrumbsConfig.getDatacenter(),
                                     breadcrumbsConfig.getApplication()),
            new BreadcrumbTransportingHandler(datumProducerConfig,
                                              metricFactoryProvider.forInternalBreadcrumbProducer(endPoint)),
            Executors.newSingleThreadScheduledExecutor(),
            this.breadcrumbsConfig.getBreadcrumbBucketFlushInterval());
  }

  protected abstract TBuilder This();

  protected boolean isBreadcrumbProductionDefined() {
    return breadcrumbsConfig != null && breadcrumbsProductionEndPoint != null;
  }

  protected void registerKnownProductionEndPointsTypes() {
    this.registerProductionEndPointType(InMemoryProductionEndPoint.class, new InMemoryDatumEnvelopeSenderFactory());
  }

  public TBuilder deliverBreadcrumbsTo(final ProductionEndPoint breadcrumbProductionEndPoint,
                                       final BreadcrumbsConfig breadcrumbsConfig) {

    this.breadcrumbsProductionEndPoint = breadcrumbProductionEndPoint;
    this.breadcrumbsConfig = breadcrumbsConfig;

    return This();
  }

  public <TProductionEndPoint extends ProductionEndPoint, UProductionEndPoint extends TProductionEndPoint> TBuilder registerProductionEndPointType(
          final Class<TProductionEndPoint> endPointType,
          final DatumEnvelopeSenderFactory<UProductionEndPoint> datumEnvelopeSenderFactory) {

    endpoint2datumEnvelopeSenderFactory.put(endPointType, datumEnvelopeSenderFactory);

    return This();
  }

  public TBuilder reportMetricsTo(final MetricsFactory metricFactory) {
    this.metricFactory = metricFactory;

    return This();
  }
}
