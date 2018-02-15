package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;


/**
 * A {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher} implementation capable of fetching
 * {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s from a Kafka stream.
 * <p>
 * The DatumEnvelopeFetcher is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized.
 */
class KafkaStreamDatumEnvelopeFetcher extends BaseKafkaDatumEnvelopeFetcher {

  KafkaStreamDatumEnvelopeFetcher(final KafkaConsumer<String, byte[]> consumer,
                                  final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                  final MetricsFactory metricFactory) {
    super(consumer, consumptionEndPoint, metricFactory);
  }

  @Override
  void seekToBeginning(final TopicPartition topicPartition) {
    kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
  }

  @Override
  void seekToEnd(final TopicPartition topicPartition) {
    kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));
  }
}
