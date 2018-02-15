package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.clients.consumer.KafkaConsumer;


/**
 * A {@link DatumEnvelopeFetcherFactory} for building {@link DatumEnvelopeFetcher}s capable of
 * consuming data from endpoints of type {@link KafkaTopicConsumptionEndPoint}.
 */
public class KafkaDatumEnvelopeFetcherFactory extends KafkaDatumEnvelopeFetcherFactoryBase {

  @Override
  DatumEnvelopeFetcher createFetcher(final KafkaConsumer<String, byte[]> consumer,
                                     final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                     final MetricsFactory metricFactory) {
    return new KafkaStreamDatumEnvelopeFetcher(consumer, consumptionEndPoint, metricFactory);
  }
}
