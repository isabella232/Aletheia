package com.outbrain.aletheia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbsConfig;
import com.outbrain.aletheia.datum.consumption.*;
import com.outbrain.aletheia.datum.production.*;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.metrics.RecordingMetricFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import junit.framework.Assert;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

public abstract class AletheiaIntegrationTest<TDomainClass> {

  private final RecordingMetricFactory metricsFactory = new RecordingMetricFactory(MetricsFactory.NULL);

  private static final Duration BREADCRUMB_BUCKET_DURATION = Duration.standardSeconds(30);
  private static final Duration BREADCRUMB_BUCKET_FLUSH_INTERVAL = Duration.millis(10);

  private static final BreadcrumbsConfig PRODUCER_BREADCRUMBS_CONFIG =
          new BreadcrumbsConfig(BREADCRUMB_BUCKET_DURATION,
                                BREADCRUMB_BUCKET_FLUSH_INTERVAL,
                                "app_tx",
                                "src_tx",
                                "tier_tx",
                                "dc_tx");

  private static final BreadcrumbsConfig CONSUMER_BREADCRUMBS_CONFIG =
          new BreadcrumbsConfig(BREADCRUMB_BUCKET_DURATION,
                                BREADCRUMB_BUCKET_FLUSH_INTERVAL,
                                "app_rx",
                                "src_rx",
                                "tier_rx",
                                "dc_rx");

  private static final DatumProducerConfig DATUM_PRODUCER_CONFIG = new DatumProducerConfig(0, "originalHostname");
  private static final DatumConsumerConfig DATUM_CONSUMER_CONFIG = new DatumConsumerConfig(0, "originalHostname");

  private static final boolean SHOULD_BE_SENT = true;
  private static final boolean SHOULD_NOT_BE_SENT = false;

  protected final Random random = new Random();
  protected final Class<TDomainClass> domainClass;

  protected AletheiaIntegrationTest(final Class<TDomainClass> domainClass) {
    this.domainClass = domainClass;
  }

  private List<byte[]> deliverAll(final List<TDomainClass> originalDatums,
                                  final InMemoryProductionEndPoint dataProductionEndPoint,
                                  final ProductionEndPoint breadcrumbProductionEndPoint,
                                  final DatumSerDe<TDomainClass> datumSerDe,
                                  final Predicate<TDomainClass> filter) {

    final DatumProducer<TDomainClass> datumProducer = datumProducer(dataProductionEndPoint,
                                                                    breadcrumbProductionEndPoint,
                                                                    datumSerDe,
                                                                    filter);

    for (final TDomainClass datum : originalDatums) {
      datumProducer.deliver(datum);
    }

    return dataProductionEndPoint.getReceivedData();
  }

  private DatumProducer<TDomainClass> datumProducer(final ProductionEndPoint dataProductionEndPoint,
                                                    final ProductionEndPoint breadcrumbProductionEndPoint,
                                                    final DatumSerDe<TDomainClass> datumSerDe,
                                                    final Predicate<TDomainClass> datumFilter) {

    return DatumProducerBuilder
            .forDomainClass(domainClass)
            .reportMetricsTo(metricsFactory)
            .deliverBreadcrumbsTo(breadcrumbProductionEndPoint, PRODUCER_BREADCRUMBS_CONFIG)
            .deliverDataTo(dataProductionEndPoint, datumSerDe, datumFilter)
            .build(DATUM_PRODUCER_CONFIG);
  }

  private ImmutableList<TDomainClass> receiveAll(final List<byte[]> sentOnWire,
                                                 final DatumSerDe<TDomainClass> datumSerDe,
                                                 final ProductionEndPoint breadcrumbProductionEndPoint) throws InterruptedException {

    final ManualFeedConsumptionEndPoint consumptionEndPoint = new ManualFeedConsumptionEndPoint();

    final Map<ConsumptionEndPoint, List<? extends DatumConsumer<TDomainClass>>> consumptionEndPoint2datumConsumer =
            DatumConsumerBuilder
                    .forDomainClass(domainClass)
                    .reportMetricsTo(metricsFactory)
                    .consumeDataFrom(consumptionEndPoint, datumSerDe)
                    .deliverBreadcrumbsTo(breadcrumbProductionEndPoint, CONSUMER_BREADCRUMBS_CONFIG)
                    .build(DATUM_CONSUMER_CONFIG);

    final DatumConsumer<TDomainClass> datumConsumer =
            Iterables.getFirst(consumptionEndPoint2datumConsumer.get(consumptionEndPoint), null);

    final ExecutorService executorService = Executors.newFixedThreadPool(1);

    final Future<ImmutableList<TDomainClass>> submit =
            executorService.submit(new Callable<ImmutableList<TDomainClass>>() {
              @Override
              public ImmutableList<TDomainClass> call() {
                return FluentIterable
                        .from(datumConsumer.datums())
                        .limit(sentOnWire.size())
                        .toList();

              }
            });

    for (final byte[] sentBinaryDatum : sentOnWire) {
      consumptionEndPoint.deliver(sentBinaryDatum);
    }

    try {
      return submit.get(2000, TimeUnit.MILLISECONDS);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void assertBreadcrumb(final InMemoryProductionEndPoint breadcrumbProductionEndPoint,
                                final BreadcrumbsConfig breadcrumbsConfig) {

    String breadcrumbJsonString = null;

    // wait for the breadcrumbs to arrive.
    for (int attempts = 1; attempts < 10; attempts++) {
      try {
        if (breadcrumbProductionEndPoint.getReceivedData().size() != 0) {
          breadcrumbJsonString = ((List<String>) breadcrumbProductionEndPoint.getReceivedData()).get(0);
        } else {
          Thread.sleep(100);
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
    assertThat(breadcrumb.getCount(), is(1L));
  }

  protected Breadcrumb deserializeBreadcrumb(final String breadcrumbJsonString) {
    final Breadcrumb breadcrumb;
    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(new JodaModule());
      breadcrumb = objectMapper.readValue(breadcrumbJsonString, Breadcrumb.class);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return breadcrumb;
  }

  protected abstract TDomainClass randomDomainClassDatum(final boolean shouldBeSent);

  protected void testEnd2End(final DatumSerDe<TDomainClass> datumSerDe,
                             final Predicate<TDomainClass> filter) throws InterruptedException {

    final TDomainClass datum = randomDomainClassDatum(SHOULD_BE_SENT);
    final TDomainClass filteredDatum = randomDomainClassDatum(SHOULD_NOT_BE_SENT);

    final List<TDomainClass> originalDomainObjects =
            FluentIterable
                    .from(Lists.newArrayList(datum, filteredDatum))
                    .filter(Predicates.notNull())
                    .toList();


    final InMemoryProductionEndPoint dataProductionEndPoint =
            new InMemoryProductionEndPoint(InMemoryProductionEndPoint.EndPointType.RawDatumEnvelope);

    final InMemoryProductionEndPoint producerBreadcrumbProductionEndPoint =
            new InMemoryProductionEndPoint(InMemoryProductionEndPoint.EndPointType.String);

    final List<byte[]> sentOnWire = deliverAll(originalDomainObjects,
                                               dataProductionEndPoint,
                                               producerBreadcrumbProductionEndPoint,
                                               datumSerDe,
                                               filter);

    final InMemoryProductionEndPoint consumerBreadcrumbsProductionEndPoint =
            new InMemoryProductionEndPoint(InMemoryProductionEndPoint.EndPointType.String);

    final ImmutableList<TDomainClass> receivedDatums = receiveAll(sentOnWire,
                                                                  datumSerDe,
                                                                  consumerBreadcrumbsProductionEndPoint);

    final Predicate<TDomainClass> shouldHaveBeenSent = new Predicate<TDomainClass>() {
      @Override
      public boolean apply(final TDomainClass datum) {
        return !datum.equals(filteredDatum);
      }
    };

    assertThat(receivedDatums.size(), is(1));
    assertThat(receivedDatums, not(hasItem(filteredDatum)));
    assertThat("Received datums were not the same as the ones that were sent.",
               receivedDatums,
               is(FluentIterable.from(originalDomainObjects)
                                .filter(shouldHaveBeenSent)
                                .toList()));

    assertBreadcrumb(producerBreadcrumbProductionEndPoint, PRODUCER_BREADCRUMBS_CONFIG);
    assertBreadcrumb(consumerBreadcrumbsProductionEndPoint, CONSUMER_BREADCRUMBS_CONFIG);

    // prints a pretty metric tree
    metricsFactory.getMetricTree().prettyPrint();
  }

}
