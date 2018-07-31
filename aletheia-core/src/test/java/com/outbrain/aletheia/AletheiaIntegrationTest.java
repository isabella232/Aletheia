package com.outbrain.aletheia;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.configuration.PropertyUtils;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.InMemoryEndPoints;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.metrics.MetricFactoryProvider;
import com.outbrain.aletheia.metrics.RecordingMetricFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

public abstract class AletheiaIntegrationTest<TDomainClass> {

  protected static final Duration BREADCRUMB_BUCKET_DURATION = Duration.standardSeconds(30);
  protected static final Duration BREADCRUMB_BUCKET_FLUSH_INTERVAL = Duration.millis(10);
  protected static final BreadcrumbsConfig PRODUCER_BREADCRUMBS_CONFIG =
          new BreadcrumbsConfig(AletheiaIntegrationTest.BREADCRUMB_BUCKET_DURATION,
                  AletheiaIntegrationTest.BREADCRUMB_BUCKET_FLUSH_INTERVAL,
                  "app_producer_tx",
                  "src_tx",
                  "tier_tx",
                  "dc_tx");
  protected static final BreadcrumbsConfig CONSUMER_STREAM_BREADCRUMBS_CONFIG =
          new BreadcrumbsConfig(AletheiaIntegrationTest.BREADCRUMB_BUCKET_DURATION,
                  AletheiaIntegrationTest.BREADCRUMB_BUCKET_FLUSH_INTERVAL,
                  "app_consumer_rx",
                  "src_rx",
                  "tier_rx",
                  "dc_rx");
  protected static final boolean SHOULD_BE_SENT = true;
  protected static final Duration DATUM_CONSUMER_STREAM_CONSUME_TIMEOUT = Duration.standardSeconds(1);
  protected static final boolean SHOULD_NOT_BE_SENT = false;
  protected final Class<TDomainClass> domainClass;
  protected final Duration datumConsumerStreamConsumeTimeout;
  protected final Random random = new Random();

  protected final RecordingMetricFactory metricsFactory = new RecordingMetricFactory(MetricsFactory.NULL);
  protected final MetricFactoryProvider recordingMetricFactoryProvider = new MetricFactoryProvider() {
    @Override
    public MetricsFactory forAuditingDatumProducer(final EndPoint endPoint, final boolean isBreadcrambs) {
      return metricsFactory;
    }

    @Override
    public MetricsFactory forInternalBreadcrumbProducer(final EndPoint endPoint) {
      return metricsFactory;
    }

    @Override
    public MetricsFactory forDatumEnvelopeSender(final EndPoint endPoint, final boolean isBreadcrambs) {
      return metricsFactory;
    }

    @Override
    public MetricsFactory forDatumEnvelopeFetcher(final EndPoint endPoint) {
      return metricsFactory;
    }

    @Override
    public MetricsFactory forAuditingDatumStreamConsumer(final EndPoint endPoint) {
      return metricsFactory;
    }

    @Override
    public MetricsFactory forDatumEnvelopeMeta(final EndPoint endPoint) {
      return metricsFactory;
    }
  };

  protected AletheiaIntegrationTest(final Class<TDomainClass> domainClass) {
    this(domainClass, DATUM_CONSUMER_STREAM_CONSUME_TIMEOUT);
  }

  protected AletheiaIntegrationTest(final Class<TDomainClass> domainClass,
                                    final Duration datumConsumerStreamConsumeTimeout) {
    this.domainClass = domainClass;
    this.datumConsumerStreamConsumeTimeout = datumConsumerStreamConsumeTimeout;
  }

  protected Properties getConsumerBreadcrumbsProperties() {
    final Properties consumerBreadcrumbs = new Properties();
    consumerBreadcrumbs.setProperty("aletheia.breadcrumbs.bucketDurationSec",
            Long.toString(BREADCRUMB_BUCKET_DURATION.getStandardSeconds()));
    consumerBreadcrumbs.setProperty("aletheia.breadcrumbs.flushIntervalSec",
            Long.toString(BREADCRUMB_BUCKET_FLUSH_INTERVAL.getStandardSeconds()));
    consumerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.application",
            CONSUMER_STREAM_BREADCRUMBS_CONFIG.getApplication());
    consumerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.source",
            CONSUMER_STREAM_BREADCRUMBS_CONFIG.getSource());
    consumerBreadcrumbs.setProperty("aletheia.consumer.source",
            CONSUMER_STREAM_BREADCRUMBS_CONFIG.getSource());
    consumerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.tier",
            CONSUMER_STREAM_BREADCRUMBS_CONFIG.getTier());
    consumerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.datacenter",
            CONSUMER_STREAM_BREADCRUMBS_CONFIG.getDatacenter());
    return consumerBreadcrumbs;
  }

  protected Properties getProducerBreadcrumbsProperties() {
    final Properties producerBreadcrumbs = new Properties();
    producerBreadcrumbs.setProperty("aletheia.breadcrumbs.bucketDurationSec",
            Long.toString(BREADCRUMB_BUCKET_DURATION.getStandardSeconds()));
    producerBreadcrumbs.setProperty("aletheia.breadcrumbs.flushIntervalSec",
            Long.toString(BREADCRUMB_BUCKET_FLUSH_INTERVAL.getStandardSeconds()));
    producerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.application",
            PRODUCER_BREADCRUMBS_CONFIG.getApplication());
    producerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.source", PRODUCER_BREADCRUMBS_CONFIG.getSource());
    producerBreadcrumbs.setProperty("aletheia.producer.source", PRODUCER_BREADCRUMBS_CONFIG.getSource());
    producerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.tier", PRODUCER_BREADCRUMBS_CONFIG.getTier());
    producerBreadcrumbs.setProperty("aletheia.breadcrumbs.fields.datacenter",
            PRODUCER_BREADCRUMBS_CONFIG.getDatacenter());
    return producerBreadcrumbs;
  }

  protected Properties getConfigProperties() {
    final Properties properties = new Properties();
    properties.setProperty("datum.class", getDomainClass().getCanonicalName());
    properties.setProperty("aletheia.routing.config.path",
            "com/outbrain/aletheia/configuration/routing.json");
    properties.setProperty("aletheia.serdes.config.path",
            "com/outbrain/aletheia/configuration/serdes.json");
    properties.setProperty("aletheia.endpoints.config.path",
            "com/outbrain/aletheia/configuration/endpoints.json");
    properties.setProperty("aletheia.endpoints.config.path",
            "com/outbrain/aletheia/configuration/endpoints.json");
    properties.setProperty("aletheia.endpoint.groups.config.path",
            "com/outbrain/aletheia/configuration/endpoint.groups.json");
    return properties;
  }

  private InMemoryEndPoint getDataProductionEndPoint(final Route route) {
    return (InMemoryEndPoint) new AletheiaConfig(getConfigProperties()).getProductionEndPoint(route.getEndPointId());
  }

  private InMemoryEndPoint getBreadcrumbsEndPoint() {
    return (InMemoryEndPoint)
            new AletheiaConfig(getConfigProperties())
                    .getBreadcrumbsProductionEndPoint(DatumUtils.getDatumTypeId(getDomainClass()));
  }

  private ImmutableList<Breadcrumb> getBreadcrumbs(final int count) {
    final Properties props =
            PropertyUtils
                    .override(getConfigProperties())
                    .with(getConsumerBreadcrumbsProperties())
                    .all();

    props.setProperty("datum.class", "com.outbrain.aletheia.breadcrumbs.Breadcrumb");
    props.setProperty(AletheiaConfig.SERDES, "{\n" +
            "  \"breadcrumbSerDe\": {\n" +
            "    \"@class\": \"com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe\",\n" +
            "    \"datum.class\": \"com.outbrain.aletheia.breadcrumbs.Breadcrumb\"\n" +
            "  }\n" +
            "}");

    final AletheiaConfig config = new AletheiaConfig(props);
    final String breadcrumbEndPointId = config.getBreadcrumbEndPointId(DatumUtils.getDatumTypeId(domainClass));
    return consumeDataOrTimeout(DatumConsumerStreamsBuilder
                    .withConfig(Breadcrumb.class, config)
                    .consumeDataFrom(new Route(breadcrumbEndPointId, "breadcrumbSerDe"))
                    .build()
                    .get(0),
            count,
            datumConsumerStreamConsumeTimeout);
  }

  protected DatumConsumerStream<TDomainClass> getConsumer(final Route route) {
    return DatumConsumerStreamsBuilder
            .withConfig(domainClass,
                    new AletheiaConfig(
                            PropertyUtils
                                    .override(getConfigProperties())
                                    .with(getConsumerBreadcrumbsProperties())
                                    .all()))
            .consumeDataFrom(route)
            .reportMetricsTo(recordingMetricFactoryProvider)
            .build()
            .get(0);
  }

  protected DatumProducer<TDomainClass> getProducer() {
    return DatumProducerBuilder
            .withConfig(domainClass,
                    new AletheiaConfig(
                            PropertyUtils
                                    .override(getConfigProperties())
                                    .with(getProducerBreadcrumbsProperties())
                                    .all()))
            .reportMetricsTo(recordingMetricFactoryProvider)
            .build();
  }

  protected void assertBreadcrumbsByRoutes(final List<Route> routes) {

    final InMemoryEndPoint breadcrumbsEndPoint = getBreadcrumbsEndPoint();

    if (breadcrumbsEndPoint != null) {
      final int producerCount =
              new AletheiaConfig(getConfigProperties())
                      .getRouting(DatumUtils.getDatumTypeId(getDomainClass()))
                      .getRoutes()
                      .size();

      final int consumerCount = routes.size();

      final ImmutableList<Breadcrumb> breadcrumbs = getBreadcrumbs(producerCount + consumerCount);

      assertBreadcrumbsBySource(producerCount, PRODUCER_BREADCRUMBS_CONFIG, breadcrumbs);

      assertBreadcrumbsBySource(consumerCount, CONSUMER_STREAM_BREADCRUMBS_CONFIG, breadcrumbs);

      assertDatumKeys(breadcrumbsEndPoint, Sets.newHashSet(InMemoryEndPoint.DEFAULT_DATUM_KEY));
    }
  }

  private void assertBreadcrumbsBySource(final int expectedBreadcrumbsCount,
                                         final BreadcrumbsConfig producerBreadcrumbsConfig,
                                         final ImmutableList<Breadcrumb> breadcrumbs) {
    final ImmutableList<Breadcrumb> producerBreadcrumbs =
            FluentIterable
                    .from(breadcrumbs)
                    .filter(new Predicate<Breadcrumb>() {
                      @Override
                      public boolean apply(final Breadcrumb breadcrumb) {
                        return breadcrumb.getSource().equals(producerBreadcrumbsConfig.getSource());
                      }
                    })
                    .toList();

    assertThat(producerBreadcrumbs.size(), is(expectedBreadcrumbsCount));

    for (final Breadcrumb breadcrumb : producerBreadcrumbs) {
      assertSingleBreadcrumb(producerBreadcrumbsConfig, breadcrumb);
    }
  }

  private void assertDatumKeys(final InMemoryEndPoint endPoint, final HashSet<String> expectedDatumKeys) {
    assertThat("Unexpected datum keys were used for sending.",
            Sets.newHashSet(endPoint.getData().keySet()),
            is(expectedDatumKeys));
  }

  protected void assertDataByRoutes(final List<TDomainClass> domainObjects,
                                    final List<Route> routes) {
    for (final Route route : routes) {
      final ImmutableList<TDomainClass> consumedData =
              consumeDataOrTimeout(getConsumer(route), 1, datumConsumerStreamConsumeTimeout);

      assertData(domainObjects, consumedData);

      assertDatumKeys(getDataProductionEndPoint(route), expectedDatumKeys(domainObjects));
    }
  }

  private HashSet<String> expectedDatumKeys(final List<TDomainClass> domainObjects) {
    final DatumKeySelector<TDomainClass> datumKeySelector =
            new AletheiaConfig(getConfigProperties()).getRouting(DatumUtils.getDatumTypeId(domainClass))
                    .getDatumKeySelector();

    return Sets.newHashSet(
            FluentIterable
                    .from(domainObjects)
                    .transform(new Function<TDomainClass, String>() {
                      @Override
                      public String apply(final TDomainClass datum) {
                        final String datumKey = datumKeySelector.getDatumKey(datum);
                        return datumKey != null ? datumKey : InMemoryEndPoint.DEFAULT_DATUM_KEY;
                      }
                    })
                    .filter(Predicates.notNull())
                    .toList());
  }

  protected void testRoutes(final Route... routes) {
    testRoutes(Arrays.asList(routes));
  }

  protected void testRoutes(final List<Route> routes) {

    assertThat(routes.size(), is(not(0)));

    final List<TDomainClass> domainObjects =
            FluentIterable.from(Lists.newArrayList(domainClassRandomDatum(SHOULD_BE_SENT)))
                    .filter(Predicates.notNull())
                    .toList();

    produceData(getProducer(), domainObjects);

    assertDataByRoutes(domainObjects, routes);

    assertBreadcrumbsByRoutes(routes);

    // prints a pretty metric tree, but emits quite a few characters to the console...
    //metricsFactory.getMetricTree().prettyPrint();
  }

  protected <T> ImmutableList<T> consumeDataOrTimeout(final DatumConsumerStream<T> datumConsumerStream,
                                                      final int datumCountToConsume,
                                                      final Duration timeout) {

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final List<T> receivedDatums = Lists.newLinkedList();

    final Future<?> submit = executorService.submit(new Runnable() {
      @Override
      public void run() {
        for (int consumedCount = 0; consumedCount < datumCountToConsume; consumedCount++) {
          receivedDatums.add(Iterables.getFirst(datumConsumerStream.datums(), null));
        }
      }
    });

    try {
      submit.get(timeout.getMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    } catch (final TimeoutException e) {
      // done waiting
    }

    return ImmutableList.copyOf(receivedDatums);
  }

  protected void produceData(final DatumProducer<TDomainClass> datumProducer, final List<TDomainClass> domainObjects) {
    for (final TDomainClass aDatum : domainObjects) {
      datumProducer.deliver(aDatum);
    }
  }

  protected abstract TDomainClass domainClassRandomDatum(final boolean shouldBeSent);

  protected Class<TDomainClass> getDomainClass() {
    return domainClass;
  }

  protected void assertSingleBreadcrumb(final BreadcrumbsConfig breadcrumbsConfig, final Breadcrumb breadcrumb) {
    assertThat(breadcrumb.getSource(), is(breadcrumbsConfig.getSource()));
    assertThat(breadcrumb.getType(), is(DatumUtils.getDatumTypeId(domainClass)));
    assertThat(breadcrumb.getDatacenter(), is(breadcrumbsConfig.getDatacenter()));
    assertThat(breadcrumb.getTier(), is(breadcrumbsConfig.getTier()));
    assertThat(breadcrumb.getApplication(), is(breadcrumbsConfig.getApplication()));
  }

  protected void assertData(final List<TDomainClass> originalDomainObjects,
                            final ImmutableList<TDomainClass> consumedData) {
    assertThat("Received data was was different from the one that was sent.",
            consumedData,
            is(FluentIterable.from(originalDomainObjects).toList()));

  }

  @Before
  public void setUp() throws Exception {
    InMemoryEndPoints.clearAll();
  }

  @Test
  public void testDatumRoutes() {

    final List<Route> datumRoutes =
            new AletheiaConfig(getConfigProperties())
                    .getRouting(DatumUtils.getDatumTypeId(domainClass))
                    .getRoutes();

    testRoutes(datumRoutes);
  }
}

