package com.outbrain.aletheia.breadcrumbs.persistence;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumer;
import com.outbrain.aletheia.datum.consumption.DatumConsumerBuilder;
import com.outbrain.aletheia.datum.consumption.DatumConsumerConfig;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaDatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaTopicBreadcrumbSqlPersisterService {

  private static final Logger logger = LoggerFactory.getLogger(KafkaTopicBreadcrumbSqlPersisterService.class);

  private BreadcrumbPersister persister;
  private final MetricsFactory metricsFactory;
  private final List<? extends DatumConsumer<Breadcrumb>> datumConsumers;

  public KafkaTopicBreadcrumbSqlPersisterService(final KafkaTopicConsumptionEndPoint breadcrumbConsumptionEndPoint,
                                                 final BreadcrumbPersister persister,
                                                 final MetricsFactory metricsFactory) {
    this.persister = persister;
    this.metricsFactory = metricsFactory;
    datumConsumers = getDatumConsumers(breadcrumbConsumptionEndPoint, metricsFactory);
  }

  private List<? extends DatumConsumer<Breadcrumb>> getDatumConsumers(final KafkaTopicConsumptionEndPoint breadcrumbConsumptionEndPoint,
                                                                      final MetricsFactory metricsFactory) {
    final Map<ConsumptionEndPoint, List<? extends DatumConsumer<Breadcrumb>>> endPoint2datumConsumer =
            DatumConsumerBuilder
                    .forDomainClass(Breadcrumb.class)
                    .registerConsumptionEndPointType(KafkaTopicConsumptionEndPoint.class,
                                                     new KafkaDatumEnvelopeFetcherFactory())
                    .consumeDataFrom(breadcrumbConsumptionEndPoint, new JsonDatumSerDe<>(Breadcrumb.class))
                    .reportMetricsTo(metricsFactory)
                    .build(new DatumConsumerConfig(1, ""));

    final Map.Entry<? extends ConsumptionEndPoint, List<? extends DatumConsumer<Breadcrumb>>>
            firstEndPointAndDatumConsumerPair = Iterables.getFirst(endPoint2datumConsumer.entrySet(), null);

    return firstEndPointAndDatumConsumerPair.getValue();
  }

  private Function<DatumConsumer<Breadcrumb>, Runnable> toPersistingTaskFrom(final BreadcrumbPersister persister) {
    return new Function<DatumConsumer<Breadcrumb>, Runnable>() {

      final String GRAPHITE_COMPONENT_NAME = KafkaTopicBreadcrumbSqlPersisterService.class.getSimpleName();

      @Override
      public Runnable apply(final DatumConsumer<Breadcrumb> datumConsumer) {
        return new Runnable() {
          @Override
          public void run() {
            try {
              for (final Breadcrumb datum : datumConsumer.datums()) {
                metricsFactory.createCounter(GRAPHITE_COMPONENT_NAME + ".Persist", "Attempts.Success").inc();
                persister.persist(Collections.singletonList(datum));
              }
            } catch (final Exception e) {
              metricsFactory.createCounter(GRAPHITE_COMPONENT_NAME + ".Persist", "Attempts.Failure").inc();
              logger.warn("Breadcrumb consuming thread has exited due to:", e);
            }
          }
        };
      }
    };
  }

  public void beginPersisting() {

    final List<Runnable> runnables = Lists.transform(datumConsumers, toPersistingTaskFrom(persister));

    final ExecutorService executorService = Executors.newFixedThreadPool(datumConsumers.size());

    for (final Runnable runnable : runnables) {
      executorService.submit(runnable);
    }

  }
}
