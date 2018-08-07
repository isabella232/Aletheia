package com.outbrain.aletheia;

import com.google.common.base.Strings;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbDispatcher;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbKey;
import com.outbrain.aletheia.breadcrumbs.KeyedBreadcrumbBaker;
import com.outbrain.aletheia.breadcrumbs.KeyedBreadcrumbDispatcher;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.PeriodicBreadcrumbDispatcher;
import com.outbrain.aletheia.datum.consumption.AuditingDatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.openers.IdentityEnvelopeOpener;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.DatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Provides a fluent API for building a {@link DatumConsumerStream} for consuming {@link DatumEnvelope}s.
 */
public class DatumEnvelopeStreamsBuilder extends BaseAletheiaBuilder {

  private static final Logger logger = LoggerFactory.getLogger(DatumEnvelopeStreamsBuilder.class);
  private final AletheiaConfig config;
  private final DatumConsumerStreamConfig consumerConfig;
  private String endpointId;
  private Predicate<DatumEnvelope> envelopeFilter = x -> true;
  private String endpointIdToProduceBreadcrumbs = "";

  private DatumEnvelopeStreamsBuilder(final AletheiaConfig config,
                                      final String endpointId) {

    this.config = config;
    this.endpointId = endpointId;

    consumerConfig = config.getDatumConsumerConfig();
  }

  /**
   * Instantiate the consumer streams for consuming {@link DatumEnvelope}s.
   *
   * @return A list of {@link DatumConsumerStream}s
   */
  public List<DatumConsumerStream<DatumEnvelope>> createStreams() {

    final ConsumptionEndPoint consumptionEndPoint = config.getConsumptionEndPoint(endpointId);
    logger.info("Creating an envelope stream for end point: {} with consumerConfig: {}",
            consumptionEndPoint,
            consumerConfig);

    final BreadcrumbDispatcher<DatumEnvelope> datumAuditor =
            createAuditor(consumerConfig, consumptionEndPoint, getMetricsFactoryProvider());

    final IdentityEnvelopeOpener datumEnvelopeOpener =
            createIdentityEnvelopeOpener(consumptionEndPoint, getMetricsFactoryProvider(), datumAuditor);

    final List<DatumEnvelopeFetcher> datumEnvelopeFetchers =
            getFetchers(consumptionEndPoint, getMetricsFactoryProvider());

    return toEnvelopeStream(consumptionEndPoint, getMetricsFactoryProvider(), datumEnvelopeOpener, datumEnvelopeFetchers);
  }

  /**
   * Create an instance of a {@link DatumEnvelopeStreamsBuilder}.
   * This is the main entry point for working with the {@link DatumEnvelopeStreamsBuilder}.
   *
   * @param config     An instance of {@link AletheiaConfig}.
   * @param endpointId The Id of a {@link ConsumptionEndPoint} to consume {@link DatumEnvelope}s from.
   * @return A {@link DatumEnvelopeStreamsBuilder} instance.
   */
  public static DatumEnvelopeStreamsBuilder createBuilder(final AletheiaConfig config,
                                                          final String endpointId) {
    return new DatumEnvelopeStreamsBuilder(config, endpointId);
  }

  /**
   * Registers a ProductionEndPoint type for producing {@link Breadcrumb}s
   *
   * @param endPointType               The type of the custom endpoint to register.
   * @param datumEnvelopeSenderFactory A {@link DatumEnvelopeSenderFactory} capable of building
   *                                   {@link NamedSender <com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope>}'s from the specified endpoint type.
   * @return A {@link DatumEnvelopeStreamsBuilder} instance with the custom production endpoint registered.
   */
  public <TProductionEndPoint extends ProductionEndPoint, UProductionEndPoint extends TProductionEndPoint> DatumEnvelopeStreamsBuilder registerBreadcrumbsEndpointType(
          final Class<TProductionEndPoint> endPointType,
          final DatumEnvelopeSenderFactory<? super UProductionEndPoint> datumEnvelopeSenderFactory) {

    this.<TProductionEndPoint, UProductionEndPoint>registerEnvelopeSenderType(endPointType, datumEnvelopeSenderFactory);

    return this;
  }

  /**
   * Registers a ConsumptionEndPoint type. After the registration, data can be consumed from an instance of this
   * endpoint type.
   *
   * @param consumptionEndPointType     The consumption endpoint to add.
   * @param datumEnvelopeFetcherFactory A {@link DatumEnvelopeFetcherFactory} capable of building
   *                                    {@link DatumEnvelopeFetcher}s from the specified endpoint type.
   * @return A {@link DatumEnvelopeStreamsBuilder} instance capable of consuming data from the specified consumption
   * endpoint type.
   */
  public <TConsumptionEndPoint extends ConsumptionEndPoint, UConsumptionEndPoint extends TConsumptionEndPoint> DatumEnvelopeStreamsBuilder registerConsumptionEndPointType(
          final Class<TConsumptionEndPoint> consumptionEndPointType,
          final DatumEnvelopeFetcherFactory<? super UConsumptionEndPoint> datumEnvelopeFetcherFactory) {

    this.<TConsumptionEndPoint, UConsumptionEndPoint>registerEnvelopeFetcherType(consumptionEndPointType, datumEnvelopeFetcherFactory);

    return this;
  }

  /**
   * Set the endpoint to which consumption breadcrumbs will be produced.
   *
   * @param endpointIdToProduceBreadcrumbs The Id of a {@link ProductionEndPoint}.
   * @return A {@link DatumEnvelopeStreamsBuilder} instance with a breadcrumbs endpoint configured.
   */
  public DatumEnvelopeStreamsBuilder setBreadcrumbsEndpoint(String endpointIdToProduceBreadcrumbs) {
    this.endpointIdToProduceBreadcrumbs = endpointIdToProduceBreadcrumbs;
    return this;
  }

  /**
   * Set a predicate for filtering consumed envelopes.
   *
   * @param envelopeFilter The filtering {@link Predicate}.
   * @return A {@link DatumEnvelopeStreamsBuilder} instance with an envelope filter defined.
   */
  public DatumEnvelopeStreamsBuilder setEnvelopeFilter(Predicate<DatumEnvelope> envelopeFilter) {
    this.envelopeFilter = envelopeFilter;
    return this;
  }

  /**
   * Configures metrics reporting.
   *
   * @param metricFactoryProvider A metricFactoryProvider instance to report metrics to.
   * @return A {@link DatumEnvelopeStreamsBuilder} instance with an envelope filter defined.
   */
  @SuppressWarnings({"unused"})
  public DatumEnvelopeStreamsBuilder reportMetricsTo(final MetricFactoryProvider metricFactoryProvider) {
    setMetricsFactoryProvider(metricFactoryProvider);

    return this;
  }

  /**
   * A {@link DatumType.TimestampSelector} which extracts a timestamp from a {@link DatumEnvelope}.
   */
  public static class EnvelopeTimestampSelector implements DatumType.TimestampSelector<DatumEnvelope> {

    @Override
    public DateTime extractDatumDateTime(DatumEnvelope envelope) {
      return new DateTime(envelope.getLogicalTimestamp());
    }
  }

  private List<DatumEnvelopeFetcher> getFetchers(ConsumptionEndPoint consumptionEndPoint, MetricFactoryProvider metricFactoryProvider) {
    final DatumEnvelopeFetcherFactory<ConsumptionEndPoint> datumEnvelopeFetcherFactory =
            getEnvelopeFetcherFactory(consumptionEndPoint.getClass());

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

  private BreadcrumbDispatcher<DatumEnvelope> getEnvelopeBreadcrumbsDispatcher(final DatumProducerConfig datumProducerConfig,
                                                                               final EndPoint endPoint,
                                                                               final MetricFactoryProvider metricFactoryProvider) {
    final Function<DatumEnvelope, BreadcrumbKey> breadcrumbKeyMapper = (envelope) -> new BreadcrumbKey(envelope.getDatumTypeId().toString(),
            envelope.getSourceHost().toString(),
            endPoint.getName(),
            breadcrumbsConfig.getApplication());

    final BreadcrumbDispatcher<DatumEnvelope> breadcrumbDispatcher = new KeyedBreadcrumbDispatcher<>(
            breadcrumbsConfig.getBreadcrumbBucketDuration(),
            new EnvelopeTimestampSelector(),
            new KeyedBreadcrumbBaker(breadcrumbsConfig.getTier(), breadcrumbsConfig.getDatacenter()),
            new BreadcrumbProducingHandler(datumProducerConfig, metricFactoryProvider),
            breadcrumbKeyMapper,
            Duration.standardDays(1)
    );

    return new PeriodicBreadcrumbDispatcher<>(breadcrumbDispatcher, breadcrumbsConfig.getBreadcrumbBucketFlushInterval());
  }
}
