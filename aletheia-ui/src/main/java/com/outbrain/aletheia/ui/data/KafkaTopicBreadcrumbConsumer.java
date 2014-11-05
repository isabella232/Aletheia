package com.outbrain.aletheia.ui.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbPersister;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaTopicBreadcrumbConsumer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaTopicBreadcrumbConsumer.class);

  public KafkaTopicBreadcrumbConsumer(final KafkaTopicConsumptionEndPoint breadcrumbConsumptionEndPoint,
                                      final BreadcrumbPersister persister,
                                      final DatumConsumerConfig datumConsumerConfig,
                                      final MetricsFactory metricsFactory) {

    final Map<ConsumptionEndPoint, List<? extends DatumConsumer<Breadcrumb>>> endPoint2datumConsumer =
            DatumConsumerBuilder
                    .forDomainClass(Breadcrumb.class)
                    .registerConsumptionEndPointType(KafkaTopicConsumptionEndPoint.class,
                                                     new KafkaDatumEnvelopeFetcherFactory())
                    .consumeDataFrom(breadcrumbConsumptionEndPoint, new JsonDatumSerDe<>(Breadcrumb.class))
                    .reportMetricsTo(metricsFactory)
                    .build(datumConsumerConfig);

    final Map.Entry<? extends ConsumptionEndPoint, List<? extends DatumConsumer<Breadcrumb>>>
            firstEndPointAndDatumConsumerPair = Iterables.getFirst(endPoint2datumConsumer.entrySet(), null);

    beginPersisting(persister, firstEndPointAndDatumConsumerPair.getValue());
  }

  private void beginPersisting(final BreadcrumbPersister persister,
                               final List<? extends DatumConsumer<Breadcrumb>> datumConsumers) {

    final List<Runnable> runnables = Lists.transform(datumConsumers, toPersistingTaskFrom(persister));

    final ExecutorService executorService = Executors.newFixedThreadPool(datumConsumers.size());

    for (final Runnable runnable : runnables) {
      executorService.submit(runnable);
    }

  }

  private Function<DatumConsumer<Breadcrumb>, Runnable> toPersistingTaskFrom(final BreadcrumbPersister persister) {
    return new Function<DatumConsumer<Breadcrumb>, Runnable>() {
      @Override
      public Runnable apply(final DatumConsumer<Breadcrumb> datumConsumer) {
        return new Runnable() {
          @Override
          public void run() {
            try {
              for (final Breadcrumb datum : datumConsumer.datums()) {
                persister.persist(datum);
              }
            } catch (final Exception e) {
              logger.warn("Breadcrumb consuming thread has exited due to:", e);
            }
          }
        };
      }
    };
  }
}
