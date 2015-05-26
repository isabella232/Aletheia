package com.outbrain.aletheia;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbsConfig;
import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStreamConfig;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStreamsBuilder;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.DatumProducerBuilder;
import com.outbrain.aletheia.datum.production.DatumProducerConfig;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.metrics.RecordingMetricFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.joda.time.Duration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

public abstract class AletheiaIntegrationTest<TDomainClass> {

  private static final String DATUM_KEY = "datumKey";

  private static final Duration BREADCRUMB_BUCKET_DURATION = Duration.standardSeconds(30);

  private static final Duration BREADCRUMB_BUCKET_FLUSH_INTERVAL = Duration.millis(10);

  private static final BreadcrumbsConfig PRODUCER_BREADCRUMBS_CONFIG =
          new BreadcrumbsConfig(BREADCRUMB_BUCKET_DURATION,
                                BREADCRUMB_BUCKET_FLUSH_INTERVAL,
                                "app_tx",
                                "src_tx",
                                "tier_tx",
                                "dc_tx");

  private static final BreadcrumbsConfig DATUM_CONSUMER_STREAM_BREADCRUMBS_CONFIG =
          new BreadcrumbsConfig(BREADCRUMB_BUCKET_DURATION,
                                BREADCRUMB_BUCKET_FLUSH_INTERVAL,
                                "app_rx",
                                "src_rx",
                                "tier_rx",
                                "dc_rx");

  private static final DatumProducerConfig DATUM_PRODUCER_CONFIG = new DatumProducerConfig(0, "originalHostname");

  private static final DatumConsumerStreamConfig DATUM_CONSUMER_STREAM_CONFIG =
          new DatumConsumerStreamConfig(0, "originalHostname");

  private static final boolean SHOULD_BE_SENT = true;
  private static final boolean SHOULD_NOT_BE_SENT = false;
  private static final Duration DATUM_CONSUMER_STREAM_CONSUME_TIMEOUT = Duration.standardSeconds(1);

  private final Class<TDomainClass> domainClass;
  private final Duration datumConsumerStreamConsumeTimeout;
  private final RecordingMetricFactory metricsFactory = new RecordingMetricFactory(MetricsFactory.NULL);
  private final DatumKeySelector<TDomainClass> datumKeySelector = new DatumKeySelector<TDomainClass>() {
    @Override
    public String getDatumKey(final TDomainClass domainObject) {
      return DATUM_KEY;
    }
  };

  protected final Random random = new Random();


  protected AletheiaIntegrationTest(final Class<TDomainClass> domainClass) {
    this(domainClass, DATUM_CONSUMER_STREAM_CONSUME_TIMEOUT);
  }

  protected AletheiaIntegrationTest(final Class<TDomainClass> domainClass,
                                    final Duration datumConsumerStreamConsumeTimeout) {
    this.domainClass = domainClass;
    this.datumConsumerStreamConsumeTimeout = datumConsumerStreamConsumeTimeout;
  }

  private DatumProducer<TDomainClass> createDataProducer(final ProductionEndPoint dataProductionEndPoint,
                                                         final ProductionEndPoint breadcrumbProductionEndPoint,
                                                         final DatumSerDe<TDomainClass> datumSerDe,
                                                         final Predicate<TDomainClass> datumFilter) {
    return DatumProducerBuilder
            .forDomainClass(domainClass)
            .reportMetricsTo(metricsFactory)
            .deliverBreadcrumbsTo(breadcrumbProductionEndPoint, PRODUCER_BREADCRUMBS_CONFIG)
            .deliverDataTo(dataProductionEndPoint, datumSerDe, datumFilter)
            .selectDatumKeyUsing(datumKeySelector)
            .build(DATUM_PRODUCER_CONFIG);
  }

  private DatumConsumerStream<TDomainClass> createDataConsumerStream(final ConsumptionEndPoint consumptionEndPoint,
                                                                     final ProductionEndPoint breadcrumbProductionEndPoint,
                                                                     final DatumSerDe<TDomainClass> datumSerDe) {

    final List<DatumConsumerStream<TDomainClass>> datumConsumerStreams =
            DatumConsumerStreamsBuilder
                    .forDomainClass(domainClass)
                    .reportMetricsTo(metricsFactory)
                    .consumeDataFrom(consumptionEndPoint, datumSerDe)
                    .deliverBreadcrumbsTo(breadcrumbProductionEndPoint, DATUM_CONSUMER_STREAM_BREADCRUMBS_CONFIG)
                    .build(DATUM_CONSUMER_STREAM_CONFIG);

    return Iterables.getFirst(datumConsumerStreams, null);
  }


  private DatumConsumerStream<Breadcrumb> createBreadcrumbConsumerStream(final ConsumptionEndPoint consumptionEndPoint) {

    final List<DatumConsumerStream<Breadcrumb>> datumConsumerStreams =
            DatumConsumerStreamsBuilder
                    .forDomainClass(Breadcrumb.class)
                    .consumeDataFrom(consumptionEndPoint, new JsonDatumSerDe<>(Breadcrumb.class))
                    .build(DATUM_CONSUMER_STREAM_CONFIG);

    return Iterables.getFirst(datumConsumerStreams, null);
  }

  private void assertBreadcrumb(final InMemoryEndPoint breadcrumbProductionEndPoint,
                                final BreadcrumbsConfig breadcrumbsConfig) {

    final int ONE = 1;

    final ImmutableList<Breadcrumb> breadcrumbs = consumeDataOrTimeout(
            createBreadcrumbConsumerStream(breadcrumbProductionEndPoint), ONE, datumConsumerStreamConsumeTimeout);

    assertThat(breadcrumbs.size(), is(ONE));

    final Breadcrumb breadcrumb = Iterables.getFirst(breadcrumbs, null);

    assertThat(breadcrumb.getSource(), is(breadcrumbsConfig.getSource()));
    assertThat(breadcrumb.getType(), is(DatumUtils.getDatumTypeId(domainClass)));
    assertThat(breadcrumb.getDatacenter(), is(breadcrumbsConfig.getDatacenter()));
    assertThat(breadcrumb.getTier(), is(breadcrumbsConfig.getTier()));
    assertThat(breadcrumb.getApplication(), is(breadcrumbsConfig.getApplication()));
    assertThat("Breadcrumbs were not sent with the default datum key",
               Sets.newHashSet(breadcrumbProductionEndPoint.getData().keySet()),
               is(Sets.newHashSet(InMemoryEndPoint.DEFAULT_DATUM_KEY)));
  }

  private <T> ImmutableList<T> consumeDataOrTimeout(final DatumConsumerStream<T> datumConsumerStream,
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
    } catch (TimeoutException e) {
      // done waiting
    }

    return ImmutableList.copyOf(receivedDatums);
  }

  private void produceData(final List<TDomainClass> domainObjects,
                           final DatumProducer<TDomainClass> datumProducer) {
    for (final TDomainClass aDatum : domainObjects) {
      datumProducer.deliver(aDatum);
    }
  }

  private void assertDatumFlow(final List<TDomainClass> originalDomainObjects,
                               final TDomainClass filteredDatum,
                               final ImmutableList<TDomainClass> consumedData,
                               final InMemoryEndPoint dataProductionEndPoint,
                               final InMemoryEndPoint producerBreadcrumbProductionEndPoint,
                               final InMemoryEndPoint consumerStreamBreadcrumbsProductionEndPoint) {

    final Predicate<TDomainClass> shouldHaveBeenSent = new Predicate<TDomainClass>() {
      @Override
      public boolean apply(final TDomainClass datum) {
        return !datum.equals(filteredDatum);
      }
    };

    assertThat(consumedData, not(hasItem(filteredDatum)));
    assertThat("Received data was was different from the one that was sent.",
               consumedData,
               is(FluentIterable.from(originalDomainObjects)
                                .filter(shouldHaveBeenSent)
                                .toList()));
    assertThat("Unexpected datum keys were used for sending.",
               Sets.newHashSet(dataProductionEndPoint.getData().keySet()),
               is(Sets.newHashSet(DATUM_KEY)));

    assertBreadcrumb(producerBreadcrumbProductionEndPoint, PRODUCER_BREADCRUMBS_CONFIG);
    assertBreadcrumb(consumerStreamBreadcrumbsProductionEndPoint, DATUM_CONSUMER_STREAM_BREADCRUMBS_CONFIG);
  }

  protected abstract TDomainClass domainClassRandomDatum(final boolean shouldBeSent);

  protected Class<TDomainClass> getDomainClass() {
    return domainClass;
  }

  protected void testEnd2End(final DatumSerDe<TDomainClass> datumSerDe, final Predicate<TDomainClass> filter) {

    final TDomainClass datum = domainClassRandomDatum(SHOULD_BE_SENT);
    final TDomainClass filteredDatum = domainClassRandomDatum(SHOULD_NOT_BE_SENT);
    final List<TDomainClass> domainObjects = FluentIterable.from(Lists.newArrayList(datum, filteredDatum))
                                                           .filter(Predicates.notNull())
                                                           .toList();

    final InMemoryEndPoint dataProductionEndPoint = new InMemoryEndPoint("test.producerData", 1);

    final InMemoryEndPoint producerBreadcrumbProductionEndPoint = new InMemoryEndPoint("test.producerBreadcrumbs", 10);

    final InMemoryEndPoint consumerBreadcrumbsProductionEndPoint = new InMemoryEndPoint("test.consumerBreadcrumbs", 10);

    final DatumProducer<TDomainClass> datumProducer =
            createDataProducer(dataProductionEndPoint, producerBreadcrumbProductionEndPoint, datumSerDe, filter);

    final DatumConsumerStream<TDomainClass> datumConsumerStream =
            createDataConsumerStream(dataProductionEndPoint, consumerBreadcrumbsProductionEndPoint, datumSerDe);

    produceData(domainObjects, datumProducer);

    final int ONE = 1;

    final ImmutableList<TDomainClass> consumedData = consumeDataOrTimeout(datumConsumerStream,
                                                                          ONE,
                                                                          datumConsumerStreamConsumeTimeout);

    assertThat(consumedData.size(), is(ONE));

    assertDatumFlow(domainObjects,
                    filteredDatum,
                    consumedData,
                    dataProductionEndPoint,
                    producerBreadcrumbProductionEndPoint,
                    consumerBreadcrumbsProductionEndPoint);

    // prints a pretty metric tree
    metricsFactory.getMetricTree().prettyPrint();
  }

}
