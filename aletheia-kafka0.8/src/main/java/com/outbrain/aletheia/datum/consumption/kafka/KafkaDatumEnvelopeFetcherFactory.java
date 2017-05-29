package com.outbrain.aletheia.datum.consumption.kafka;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link DatumEnvelopeFetcherFactory} for building {@link DatumEnvelopeFetcher}s capable of
 * consuming data from endpoints of type {@link KafkaTopicConsumptionEndPoint}.
 */
@Deprecated
public class KafkaDatumEnvelopeFetcherFactory implements DatumEnvelopeFetcherFactory<KafkaTopicConsumptionEndPoint> {


  private ConsumerConfig createConsumerConfig(final String zkConnect,
                                              final String groupId,
                                              final Properties properties) {

    final Properties consumerConfig = (Properties) properties.clone();

    consumerConfig.put("zookeeper.connect", zkConnect);
    consumerConfig.put("group.id", groupId);

    return new ConsumerConfig(consumerConfig);
  }

  @Override
  @Deprecated
  public List<DatumEnvelopeFetcher> buildDatumEnvelopeFetcher(final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                                              final MetricsFactory metricFactory) {

    final ConsumerConnector javaConsumerConnector =
            Consumer.createJavaConsumerConnector(createConsumerConfig(consumptionEndPoint.getZkConnect(),
                                                                      consumptionEndPoint.getGroupId(),
                                                                      consumptionEndPoint.getProperties()));

    final Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams =
            javaConsumerConnector.createMessageStreams(ImmutableMap.of(consumptionEndPoint.getTopicName(),
                                                                       consumptionEndPoint.getConcurrencyLevel()));

    final Function<KafkaStream<byte[], byte[]>, DatumEnvelopeFetcher> toDatumEnvelopeFetcher =
            new Function<KafkaStream<byte[], byte[]>, DatumEnvelopeFetcher>() {
              @Override
              public DatumEnvelopeFetcher apply(final KafkaStream<byte[], byte[]> stream) {
                return new KafkaStreamDatumEnvelopeFetcher(stream);

              }
            };

    final List<KafkaStream<byte[], byte[]>> kafkaStreams =
            Iterables.getFirst(messageStreams.values(), Lists.<KafkaStream<byte[], byte[]>>newArrayList());

    return FluentIterable.from(kafkaStreams)
                         .transform(toDatumEnvelopeFetcher)
                         .toList();
  }
}
