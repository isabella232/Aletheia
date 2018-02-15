package com.outbrain.aletheia.datum.consumption.kafka;

import com.google.common.collect.Lists;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * A {@link DatumEnvelopeFetcherFactory} for building {@link DatumEnvelopeFetcher}s capable of
 * consuming data from endpoints of type {@link KafkaTopicConsumptionEndPoint}.
 */
abstract class KafkaDatumEnvelopeFetcherFactoryBase implements DatumEnvelopeFetcherFactory<KafkaTopicConsumptionEndPoint> {
  final Logger logger = LoggerFactory.getLogger(getClass());

  private Properties createConsumerConfig(final String brokers,
                                          final String groupId,
                                          final Properties properties) {
    final Properties consumerConfig = (Properties) properties.clone();

    if (consumerConfig.getProperty("value.deserializer") != null
        || consumerConfig.getProperty("key.deserializer") != null) {
      logger.warn("serializer cannot be provided as consumer properties. "
          + "Overriding manually to be the correct serialization type");
    }
    consumerConfig.put("key.deserializer", StringDeserializer.class.getName());
    consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());

    if (consumerConfig.getProperty("enable.auto.commit") != null) {
      logger.warn("enable.auto.commit cannot be provided as consumer properties. "
          + "Please use offset.commit.mode to control offset management mode. see com.outbrain.aletheia.datum.consumption.OffsetCommitMode for supported modes.");
    }
    consumerConfig.put("enable.auto.commit", "false");

    consumerConfig.put("bootstrap.servers", brokers);
    consumerConfig.put("group.id", groupId);

    logger.warn("Using consumer config: {}", consumerConfig);

    return consumerConfig;
  }

  @Override
  public List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                                              final MetricsFactory metricFactory) {

    // Instantiate consumers according to configured concurrency level
    final int numConsumers = consumptionEndPoint.getConcurrencyLevel();
    final List<KafkaConsumer<String, byte[]>> consumerList = Lists.newArrayList();

    for (int i = 0; i < numConsumers; i++) {
      final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(
          createConsumerConfig(consumptionEndPoint.getBrokers(),
              consumptionEndPoint.getGroupId(),
              consumptionEndPoint.getProperties()));
      consumerList.add(consumer);
    }

    return consumerList.stream()
                       .map(consumer -> createFetcher(consumer, consumptionEndPoint, metricFactory))
                       .collect(Collectors.toList());
  }

  abstract DatumEnvelopeFetcher createFetcher(final KafkaConsumer<String, byte[]> consumer,
                                              final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                              final MetricsFactory metricFactory);
}
