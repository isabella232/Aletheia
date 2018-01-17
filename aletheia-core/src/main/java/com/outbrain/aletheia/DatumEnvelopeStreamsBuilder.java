package com.outbrain.aletheia;

import com.google.common.base.Strings;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.StartTimeWithDurationBreadcrumbBaker;
import com.outbrain.aletheia.datum.DatumAuditor;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.consumption.AuditingDatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.openers.IdentityEnvelopeOpener;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.metrics.DefaultMetricFactoryProvider;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Provides a fluent API for building a {@link DatumConsumerStream}.
 */
public class DatumEnvelopeStreamsBuilder extends BaseAletheiaBuilder {

  private static final Logger logger = LoggerFactory.getLogger(DatumEnvelopeStreamsBuilder.class);
  private static final String ENVELOPE_CONSUMER_STREAM = "EnvelopeConsumerStream";
  private final AletheiaConfig config;
  private final DatumConsumerStreamConfig consumerConfig;
  protected MetricsFactory metricFactory = MetricsFactory.NULL;
  private DatumEnvelopeFetcherFactory datumEnvelopeFetcherFactory;
  private String endpointId;
  private Predicate<DatumEnvelope> envelopeFilter = x -> true;
  private String endpointIdToProduceBreadcrumbs = "";

  protected DatumEnvelopeStreamsBuilder(AletheiaConfig config, String endpointId, DatumEnvelopeFetcherFactory datumEnvelopeFetcherFactory) {

    this.config = config;
    this.endpointId = endpointId;
    this.datumEnvelopeFetcherFactory = datumEnvelopeFetcherFactory;

    consumerConfig = config.getDatumConsumerConfig();
  }

  public List<DatumConsumerStream<DatumEnvelope>> createStreams() {

    final ConsumptionEndPoint consumptionEndPoint = config.getConsumptionEndPoint(endpointId);
    logger.info("Creating an envelope stream for end point: {} with consumerConfig: {}",
            consumptionEndPoint,
            consumerConfig);

    final MetricFactoryProvider metricFactoryProvider = createMetricFactoryProvider();

    final BreadcrumbDispatcher<DatumEnvelope> datumAuditor =
            createAuditor(consumerConfig, consumptionEndPoint, metricFactoryProvider);

    final IdentityEnvelopeOpener datumEnvelopeOpener =
            createIdentityEnvelopeOpener(consumptionEndPoint, metricFactoryProvider, datumAuditor);

    final List<DatumEnvelopeFetcher> datumEnvelopeFetchers =
            getFetchers(consumptionEndPoint, metricFactoryProvider);

    return toEnvelopeStream(consumptionEndPoint, metricFactoryProvider, datumEnvelopeOpener, datumEnvelopeFetchers);
  }

  private List<DatumEnvelopeFetcher> getFetchers(ConsumptionEndPoint consumptionEndPoint, MetricFactoryProvider metricFactoryProvider) {
    return datumEnvelopeFetcherFactory.buildDatumEnvelopeFetcher(
            consumptionEndPoint,
            metricFactoryProvider.forDatumEnvelopeFetcher(consumptionEndPoint));
  }

  private List<DatumConsumerStream<DatumEnvelope>> toEnvelopeStream(ConsumptionEndPoint consumptionEndPoint, MetricFactoryProvider metricFactoryProvider, IdentityEnvelopeOpener datumEnvelopeOpener, List<DatumEnvelopeFetcher> datumEnvelopeFetchers) {
    return datumEnvelopeFetchers.stream().map((datumEnvelopeFetcher) -> new AuditingDatumConsumerStream<>(
            datumEnvelopeFetcher,
            datumEnvelopeOpener,
            envelopeFilter::test,
            metricFactoryProvider.forAuditingDatumStreamConsumer(consumptionEndPoint))).collect(Collectors.toList());
  }

  private DefaultMetricFactoryProvider createMetricFactoryProvider() {
    return new DefaultMetricFactoryProvider(DatumEnvelope.class.getSimpleName(),
            ENVELOPE_CONSUMER_STREAM,
            metricFactory);
  }

  private IdentityEnvelopeOpener createIdentityEnvelopeOpener(ConsumptionEndPoint consumptionEndPoint, MetricFactoryProvider metricFactoryProvider, BreadcrumbDispatcher<DatumEnvelope> datumAuditor) {
    return new IdentityEnvelopeOpener(datumAuditor, metricFactoryProvider.forDatumEnvelopeMeta(consumptionEndPoint));
  }

  private BreadcrumbDispatcher<DatumEnvelope> createAuditor(DatumConsumerStreamConfig consumerConfig, ConsumptionEndPoint consumptionEndPoint, MetricFactoryProvider metricFactoryProvider) {
    BreadcrumbDispatcher<DatumEnvelope> datumAuditor;
    if (Strings.isNullOrEmpty(endpointIdToProduceBreadcrumbs)) {
      datumAuditor = BreadcrumbDispatcher.NULL;
    } else {
      final ProductionEndPoint breadcrumbsProductionEndPoint =
              new AletheiaConfig(getBreadcrumbEnvironment(config.getProperties())).getProductionEndPoint(endpointIdToProduceBreadcrumbs);

      this.setBreadcrumbsEndpoint(breadcrumbsProductionEndPoint, new BreadcrumbsConfig(config.getProperties()));

      datumAuditor = getEnvelopeBreadcrumbsDispatcher(new DatumProducerConfig(consumerConfig.getIncarnation(),
                      consumerConfig.getHostname()),
              consumptionEndPoint,
              metricFactoryProvider);
    }
    return datumAuditor;
  }

  public static DatumEnvelopeStreamsBuilder createBuilder(AletheiaConfig config, String endpointId, DatumEnvelopeFetcherFactory fetcherFactory) {
    return new DatumEnvelopeStreamsBuilder(config, endpointId, fetcherFactory);
  }

  public DatumEnvelopeStreamsBuilder setBreadcrumbsEndpoint(String endpointIdToProduceBreadcrumbs) {
    this.endpointIdToProduceBreadcrumbs = endpointIdToProduceBreadcrumbs;
    return this;
  }

  public DatumEnvelopeStreamsBuilder setEnvelopeFilter(Predicate<DatumEnvelope> envelopeFilter) {
    this.envelopeFilter = envelopeFilter;
    return this;
  }

  protected BreadcrumbDispatcher<DatumEnvelope> getEnvelopeBreadcrumbsDispatcher(final DatumProducerConfig datumProducerConfig,
                                                                                 final EndPoint endPoint,
                                                                                 final MetricFactoryProvider metricFactoryProvider) {
    return new DatumAuditor<>(
            breadcrumbsConfig.getBreadcrumbBucketDuration(),
            new EnvelopeTimestampSelector(),
            new StartTimeWithDurationBreadcrumbBaker(breadcrumbsConfig.getSource(),
                    endPoint.getName(),
                    breadcrumbsConfig.getTier(),
                    breadcrumbsConfig.getDatacenter(),
                    breadcrumbsConfig.getApplication(),
                    ENVELOPE),
            new BreadcrumbProducingHandler(datumProducerConfig,
                    metricFactoryProvider.forInternalBreadcrumbProducer(endPoint)),
            breadcrumbsConfig.getBreadcrumbBucketFlushInterval());
  }

  public class EnvelopeTimestampSelector implements DatumType.TimestampSelector<DatumEnvelope> {

    @Override
    public DateTime extractDatumDateTime(DatumEnvelope envelope) {
      return new DateTime(envelope.getLogicalTimestamp());
    }
  }


}
