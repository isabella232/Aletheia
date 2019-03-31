package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.OffsetCommitMode;
import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.AletheiaMetricFactoryProvider;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


/**
 * A {@link DatumEnvelopeFetcher} implementation capable of fetching
 * {@link DatumEnvelope}s from a Kafka stream.
 * <p>
 * The DatumEnvelopeFetcher is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized.
 */
abstract class BaseKafkaDatumEnvelopeFetcher implements DatumEnvelopeFetcher, ConsumerRebalanceListener {
  final Logger logger = LoggerFactory.getLogger(getClass());

  final KafkaConsumer<String, byte[]> kafkaConsumer;
  private Iterator<ConsumerRecord<String, byte[]>> consumerRecordIterator = Collections.emptyIterator();
  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  private volatile boolean isWaitingForPoll = false;

  private ScheduledExecutorService autoOffsetCommitExecutor;
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> consumedOffsets = new ConcurrentHashMap<>();
  private final Counter autoCommitAttemptsCounter;
  private final Counter autoCommitResultsCounter;
  private final OffsetCommitMode offsetCommitMode;
  private final String offsetResetStrategy;
  private final boolean isAtMostOnceOffsetCommitMode;
  private boolean closed = false;
  private final Set<String> currentSubscription = new HashSet<>();
  private final long autoCommitInterval;
  private final KafkaTopicConsumptionEndPoint consumptionEndPoint;

  BaseKafkaDatumEnvelopeFetcher(final KafkaConsumer<String, byte[]> consumer,
                                final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                final MetricsFactory metricFactory) {
    this.kafkaConsumer = consumer;

    this.autoCommitAttemptsCounter = metricFactory.createCounter("autoCommitAttempts", "Number of auto commit attempts");
    this.autoCommitResultsCounter = metricFactory.createCounter("autoCommitResults", "Number of auto commit results", "result");
    this.offsetResetStrategy = consumptionEndPoint.getProperties().getProperty("auto.offset.reset");
    this.consumptionEndPoint = consumptionEndPoint;

    try {
      offsetCommitMode = OffsetCommitMode.valueOf(consumptionEndPoint.getOffsetCommitMode());
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Illegal offset commit mode value. See com.outbrain.aletheia.datum.consumption.OffsetCommitMode for supported modes.", e);
    }

    isAtMostOnceOffsetCommitMode = OffsetCommitMode.AT_MOST_ONCE.equals(offsetCommitMode);

    try {
      final Pattern topicsPattern = Pattern.compile(consumptionEndPoint.getTopicName());
      // Subscribe to topic with consumer rebalance listener
      kafkaConsumer.subscribe(topicsPattern, this);

    } catch (final PatternSyntaxException ex) {
      logger.error("topics pattern '{}' for endpoint id '{}' is not a valid regex", consumptionEndPoint.getTopicName(), consumptionEndPoint.getName());
      throw new IllegalArgumentException(String.format("topics pattern '%s' for endpoint id '%s' is not a valid regex", consumptionEndPoint.getTopicName(), consumptionEndPoint.getName()), ex);
    }

    autoCommitInterval =
        Long.parseLong(consumptionEndPoint.getProperties()
                                          .getProperty("auto.commit.interval.ms", "5000"));
    startAutoCommitExecutorIfNeeded(autoCommitInterval);

    // Handle consumer cleanup
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  @Override
  public void close() {
    shutdown();
  }

  @Override
  public Iterable<DatumEnvelope> datumEnvelopes() {
    return datumEnvelopeIterable;
  }

  @Override
  public void commitConsumedOffsets() {
    if (isAtMostOnceOffsetCommitMode) {
      logger.warn("Manual offset commit is illegal when offset commit mode is at most once. All Offsets were committed at poll.");
      return;
    }
    commitOffsetsInternal();
  }

  private void commitOffsetsInternal() {
    synchronized (kafkaConsumer) {
      kafkaConsumer.commitSync(consumedOffsets);
    }
  }

  private final Iterable<DatumEnvelope> datumEnvelopeIterable = () -> new Iterator<DatumEnvelope>() {
    @Override
    public boolean hasNext() {
      // Resume polling if no record is available
      while (!consumerRecordIterator.hasNext()) {
        getConsumerRecords();
      }
      return true;
    }

    @Override
    public DatumEnvelope next() {
      // Obtain next record
      final ConsumerRecord<String, byte[]> record = getConsumerRecords().next();
      updateConsumedRecord(record);

      // Return deserialized envelope
      return avroDatumEnvelopeSerDe.deserializeDatumEnvelope(ByteBuffer.wrap(record.value()));
    }

    @Override
    public void remove() {

    }
  };

  // Keep similar behavior to Kafka 0.8 API
  private Iterator<ConsumerRecord<String, byte[]>> getConsumerRecords() {
    // If we have no current records - poll kafka consumer
    //  Otherwise, just return the current iterator unchanged
    try {
      if (!consumerRecordIterator.hasNext()) {
        consumerRecordIterator = pollKafkaConsumer();
      }
    } catch (final WakeupException e) {
      shutdown();
    }
    return consumerRecordIterator;
  }

  // Poll for new records
  private Iterator<ConsumerRecord<String, byte[]>> pollKafkaConsumer() {
    final ConsumerRecords<String, byte[]> consumedRecords;
    try {
      synchronized (kafkaConsumer) {
        isWaitingForPoll = true;
        consumedRecords = kafkaConsumer.poll(autoCommitInterval);
        if (!consumedRecords.isEmpty() && isAtMostOnceOffsetCommitMode) {
          kafkaConsumer.commitSync();
        }
      }
      consumerRecordIterator = consumedRecords.iterator();
    } finally {
      isWaitingForPoll = false;
    }
    return consumerRecordIterator;
  }


  /**
   * Shutdown consumer.
   * <p>
   * This method may be called twice:
   * Once when trying to close a consumer which is waiting for poll()
   * And then when the consumer wakes-up.
   */
  private void shutdown() {
    if (autoOffsetCommitExecutor != null) {
      autoOffsetCommitExecutor.shutdown();
    }

    if (isWaitingForPoll) {
      logger.info("Waking up consumer for topics: {}", currentSubscription);
      kafkaConsumer.wakeup();
    } else {
      synchronized (kafkaConsumer) {
        if (!closed) {
          logger.info("Shutting down consumer for topics: {}", currentSubscription);
          kafkaConsumer.close();
          closed = true;
        }
      }
    }
  }

  private void updateConsumedRecord(final ConsumerRecord<String, byte[]> record) {
    if (!isAtMostOnceOffsetCommitMode) {
      // Consumed offsets is irrelevant when offset commit is immediately after poll
      consumedOffsets
          .put(new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1));
    }
  }

  private void startAutoCommitExecutorIfNeeded(final long autoCommitInterval) {
    if (OffsetCommitMode.AT_LEAST_ONCE.equals(offsetCommitMode)) {
      //  Executor for committing consumer offsets
      //  (Keep similar behavior to Kafka 0.8 High Level Consumer)
      autoOffsetCommitExecutor = Executors.newScheduledThreadPool(1);
      autoOffsetCommitExecutor.scheduleWithFixedDelay(() -> {
        autoCommitAttemptsCounter.inc();
        try {
          commitOffsetsInternal();
          autoCommitResultsCounter.inc(AletheiaMetricFactoryProvider.SUCCESS);
        } catch (final Exception e) {
          logger.error("commitSync for endpoint {} topic {} failed with exception: ", consumptionEndPoint.getName(), consumptionEndPoint.getTopicName(), e);
          autoCommitResultsCounter.inc(AletheiaMetricFactoryProvider.FAIL);
        }
      }, autoCommitInterval, autoCommitInterval, TimeUnit.MILLISECONDS);
    } else {
      autoOffsetCommitExecutor = null;
    }
  }

  @Override
  public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
    // Clear to make sure only offsets of partitions assigned to this consumer are managed
    consumedOffsets.clear();
    currentSubscription.clear();
  }

  @Override
  public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
    // Handle partitions with no explicit offset. Explicit offset should be committed for every new partition-consumer group, otherwise messages will be lost if offset reset strategy is "latest".
    if (!isAtMostOnceOffsetCommitMode) {
      synchronized (kafkaConsumer) {
        for (final TopicPartition topicPartition : partitions) {
          final OffsetAndMetadata currentOffsetMetadata = kafkaConsumer.committed(topicPartition);
          initializeOffsetIfNeeded(topicPartition, currentOffsetMetadata);
        }
      }
      if (!consumedOffsets.isEmpty()) {
        commitOffsetsInternal();
      }

      currentSubscription.clear();
      currentSubscription.addAll(kafkaConsumer.subscription());
    }
  }

  private void initializeOffsetIfNeeded(final TopicPartition topicPartition, final OffsetAndMetadata currentOffsetMetadata) {
    if (currentOffsetMetadata == null) {
      // If there's no explicit offset - commit an explicit offset according to the offset reset strategy.
      if (OffsetResetStrategy.EARLIEST.name().equalsIgnoreCase(offsetResetStrategy)) {
        seekToBeginning(topicPartition);
      } else {
        seekToEnd(topicPartition);
      }
      final long currentPosition = kafkaConsumer.position(topicPartition);
      consumedOffsets.put(topicPartition, new OffsetAndMetadata(currentPosition));
    }
  }

  abstract void seekToBeginning(final TopicPartition topicPartition);

  abstract void seekToEnd(final TopicPartition topicPartition);
}
