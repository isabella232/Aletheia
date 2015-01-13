package com.outbrain.aletheia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
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
import com.outbrain.aletheia.datum.production.*;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.metrics.RecordingMetricFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import junit.framework.Assert;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

public abstract class AletheiaIntegrationTest<TDomainClass> {

  private static final String DATUM_KEY = "datumKey";
  private static final int CONSUME_DATA_TIMEOUT_MILLI = 200;
  private static final int BREADCRUMB_WAIT_ATTEMPT_COOLDOWN_MILLIS = 100;

  private final RecordingMetricFactory metricsFactory = new RecordingMetricFactory(MetricsFactory.NULL);

  private final DatumKeySelector<TDomainClass> datumKeySelector = new DatumKeySelector<TDomainClass>() {
    @Override
    public String getDatumKey(final TDomainClass domainObject) {
      return DATUM_KEY;
    }
  };

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

  private static final DatumConsumerStreamConfig
          DATUM_CONSUMER_STREAM_CONFIG = new DatumConsumerStreamConfig(0, "originalHostname");

  private static final boolean SHOULD_BE_SENT = true;
  private static final boolean SHOULD_NOT_BE_SENT = false;

  private final Predicate<Iterable> nonEmpty = new Predicate<Iterable>() {
    @Override
    public boolean apply(final Iterable iterable) {
      return iterable.iterator().hasNext();
    }
  };
  protected final Random random = new Random();
  private final Class<TDomainClass> domainClass;

  protected AletheiaIntegrationTest(final Class<TDomainClass> domainClass) {
    this.domainClass = domainClass;
  }

  private DatumProducer<TDomainClass> createDatumProducer(final ProductionEndPoint dataProductionEndPoint,
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

  private DatumConsumerStream<TDomainClass> createDatumStream(final ConsumptionEndPoint consumptionEndPoint,
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

  private void assertBreadcrumb(final InMemoryEndPoint.WithStringStorage breadcrumbProductionEndPoint,
                                final BreadcrumbsConfig breadcrumbsConfig) {

    String breadcrumbJsonString = null;

    // wait for the breadcrumbs to arrive.
    for (int attempts = 1; attempts < 10; attempts++) {
      try {
        if (Iterables.any(breadcrumbProductionEndPoint.getData().values(), nonEmpty)) {
          final Iterable<String> allBreadcrumbs = Iterables.concat(breadcrumbProductionEndPoint.getData().values());
          breadcrumbJsonString = FluentIterable.from(allBreadcrumbs).first().get();
        } else {
          Thread.sleep(BREADCRUMB_WAIT_ATTEMPT_COOLDOWN_MILLIS);
        }
      } catch (final InterruptedException e) {
        Assert.fail();
      }
    }

    final Breadcrumb breadcrumb = deserializeBreadcrumb(breadcrumbJsonString);

    assertThat(breadcrumb.getSource(), is(breadcrumbsConfig.getSource()));
    assertThat(breadcrumb.getType(), is(DatumUtils.getDatumTypeId(domainClass)));
    assertThat(breadcrumb.getDatacenter(), is(breadcrumbsConfig.getDatacenter()));
    assertThat(breadcrumb.getTier(), is(breadcrumbsConfig.getTier()));
    assertThat(breadcrumb.getApplication(), is(breadcrumbsConfig.getApplication()));
    assertThat(Sets.newHashSet(breadcrumbProductionEndPoint.getData().keySet()),
               is(Sets.newHashSet(InMemoryAccumulatingNamedSender.DEFAULT_DATUM_KEY)));
  }

  private ImmutableList<TDomainClass> consumeDataOrTimeout(final DatumConsumerStream<TDomainClass> datumConsumerStream,
                                                           final int timeout) {

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final List<TDomainClass> receivedDatums = Lists.newLinkedList();

    final Future<?> submit = executorService.submit(new Runnable() {
      @Override
      public void run() {
        for (TDomainClass aDatum : datumConsumerStream.datums()) {
          receivedDatums.add(aDatum);
        }
      }
    });

    try {
      submit.get(timeout, TimeUnit.MILLISECONDS);
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
                               final ImmutableList<TDomainClass> consumedDatums,
                               final InMemoryEndPoint.WithBinaryStorage dataProductionEndPoint,
                               final InMemoryEndPoint.WithStringStorage producerBreadcrumbProductionEndPoint,
                               final InMemoryEndPoint.WithStringStorage consumerStreamBreadcrumbsProductionEndPoint) {

    final Predicate<TDomainClass> shouldHaveBeenSent = new Predicate<TDomainClass>() {
      @Override
      public boolean apply(final TDomainClass datum) {
        return !datum.equals(filteredDatum);
      }
    };

    assertThat(consumedDatums.size(), is(1));
    assertThat(consumedDatums, not(hasItem(filteredDatum)));
    assertThat("Received datums were not the same as the ones that were sent.",
               consumedDatums,
               is(FluentIterable.from(originalDomainObjects)
                                .filter(shouldHaveBeenSent)
                                .toList()));
    assertThat("Unexpected datum keys were used for sending.",
               Sets.newHashSet(dataProductionEndPoint.getData().keySet()),
               is(Sets.newHashSet(DATUM_KEY)));

    assertBreadcrumb(producerBreadcrumbProductionEndPoint, PRODUCER_BREADCRUMBS_CONFIG);
    assertBreadcrumb(consumerStreamBreadcrumbsProductionEndPoint, DATUM_CONSUMER_STREAM_BREADCRUMBS_CONFIG);
  }

  protected Breadcrumb deserializeBreadcrumb(final String breadcrumbJsonString) {

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JodaModule());

    try {
      return objectMapper.readValue(breadcrumbJsonString, Breadcrumb.class);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
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

    final InMemoryEndPoint.WithBinaryStorage dataProductionEndPoint =
            new InMemoryEndPoint.WithBinaryStorage(1);

    final InMemoryEndPoint.WithStringStorage producerBreadcrumbProductionEndPoint =
            new InMemoryEndPoint.WithStringStorage(10);

    final InMemoryEndPoint.WithStringStorage consumerStreamBreadcrumbsProductionEndPoint =
            new InMemoryEndPoint.WithStringStorage(10);

    final DatumProducer<TDomainClass> datumProducer =
            createDatumProducer(dataProductionEndPoint, producerBreadcrumbProductionEndPoint, datumSerDe, filter);

    final DatumConsumerStream<TDomainClass> datumConsumerStream =
            createDatumStream(dataProductionEndPoint, consumerStreamBreadcrumbsProductionEndPoint, datumSerDe);

    produceData(domainObjects, datumProducer);

    final ImmutableList<TDomainClass> consumedData =
            consumeDataOrTimeout(datumConsumerStream, CONSUME_DATA_TIMEOUT_MILLI);

    assertDatumFlow(domainObjects,
                    filteredDatum,
                    consumedData,
                    dataProductionEndPoint,
                    producerBreadcrumbProductionEndPoint,
                    consumerStreamBreadcrumbsProductionEndPoint
    );

    // prints a pretty metric tree
    metricsFactory.getMetricTree().prettyPrint();
  }

}
