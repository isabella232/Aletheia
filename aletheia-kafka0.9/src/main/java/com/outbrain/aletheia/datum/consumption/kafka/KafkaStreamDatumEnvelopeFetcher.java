package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.consumption.OffsetCommitMode;
import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
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
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A {@link com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher} implementation capable of fetching
 * {@link com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope}s from a Kafka stream.
 * <p>
 * The DatumEnvelopeFetcher is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized.
 */
class KafkaStreamDatumEnvelopeFetcher implements DatumEnvelopeFetcher, ConsumerRebalanceListener {
  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamDatumEnvelopeFetcher.class);
  private static final long POLL_TIMEOUT_MS = Long.MAX_VALUE;
  public static final String AUTO_COMMIT = "AutoCommit";

  private final KafkaConsumer<String, byte[]> kafkaConsumer;
  private Iterator<ConsumerRecord<String, byte[]>> consumerRecordIterator = Collections.emptyIterator();
  private final KafkaTopicConsumptionEndPoint consumptionEndPoint;
  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  private volatile boolean isWaitingForPoll = false;

  private ScheduledExecutorService autoOffsetCommitExecutor;
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> consumedOffsets = new ConcurrentHashMap<>();
  private final Counter autoCommitAttempt;
  private final Counter autoCommitSuccess;
  private final Counter autoCommitFail;
  private final OffsetCommitMode offsetCommitMode;
  private final String offsetResetStrategy;
  private boolean isAtMostOnceOffsetCommitMode;

  public KafkaStreamDatumEnvelopeFetcher(final KafkaConsumer<String, byte[]> consumer,
                                         final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                         final MetricsFactory metricFactory) {
    this.kafkaConsumer = consumer;
    this.consumptionEndPoint = consumptionEndPoint;

    this.autoCommitAttempt = metricFactory.createCounter(AUTO_COMMIT, "Attempts");
    this.autoCommitSuccess = metricFactory.createCounter(AUTO_COMMIT, "Success");
    this.autoCommitFail = metricFactory.createCounter(AUTO_COMMIT, "Failure");
    this.offsetResetStrategy = consumptionEndPoint.getProperties().getProperty("auto.offset.reset");

    try {
      offsetCommitMode = OffsetCommitMode.valueOf(consumptionEndPoint.getProperties()
              .getProperty("offset.commit.mode", OffsetCommitMode.AT_LEAST_ONCE.name()));
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Illegal offset commit mode value. See com.outbrain.aletheia.datum.consumption.OffsetCommitMode for supported modes.");
    }

    isAtMostOnceOffsetCommitMode = OffsetCommitMode.AT_MOST_ONCE.equals(offsetCommitMode);

    // Subscribe to topic with consumer rebalance listener
    consumer.subscribe(Collections.singletonList(consumptionEndPoint.getTopicName()), this);

    final long autoCommitInterval =
            Long.parseLong(consumptionEndPoint.getProperties()
                    .getProperty("auto.commit.interval.ms", "5000"));
    startAutoCommitExecutorIfNeeded(autoCommitInterval);

    // Handle consumer cleanup
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        shutdown();
      }
    }));
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

  private final Iterable<DatumEnvelope> datumEnvelopeIterable = new Iterable<DatumEnvelope>() {

    @Override
    public Iterator<DatumEnvelope> iterator() {
      return new Iterator<DatumEnvelope>() {
        @Override
        public boolean hasNext() {
          return getConsumerRecords().hasNext();
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
    } catch (WakeupException e) {
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
        commitOffsetsInternal();
        consumedRecords = kafkaConsumer.poll(POLL_TIMEOUT_MS);
        if (isAtMostOnceOffsetCommitMode) {
          kafkaConsumer.commitSync();
        }
      }
      consumerRecordIterator = consumedRecords.iterator();
    } finally {
      isWaitingForPoll = false;
    }
    return consumerRecordIterator;
  }

  private void shutdown() {
    if (autoOffsetCommitExecutor != null) {
      autoOffsetCommitExecutor.shutdown();
    }

    // This method called twice when
    if (isWaitingForPoll) {
      logger.info("Waking up consumer for endpoint: " + consumptionEndPoint.getName());
      kafkaConsumer.wakeup();
    } else {
      logger.info("Shutting down consumer for endpoint: " + consumptionEndPoint.getName());
      synchronized (kafkaConsumer) {
        kafkaConsumer.close();
      }
    }
  }

  private void updateConsumedRecord(ConsumerRecord<String, byte[]> record) {
    if (!isAtMostOnceOffsetCommitMode) {
      // Consumed offsets is irrelevant when offset commit is immediately after poll
      consumedOffsets
              .put(new TopicPartition(record.topic(), record.partition()),
                      new OffsetAndMetadata(record.offset() + 1));
    }
  }

  private void startAutoCommitExecutorIfNeeded(long autoCommitInterval) {
    if (OffsetCommitMode.AT_LEAST_ONCE.equals(offsetCommitMode)) {
      //  Executor for committing consumer offsets
      //  (Keep similar behavior to Kafka 0.8 High Level Consumer)
      autoOffsetCommitExecutor = Executors.newScheduledThreadPool(1);
      autoOffsetCommitExecutor.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            commitOffsetsInternal();
            autoCommitSuccess.inc();
          } catch (Exception e) {
            logger.error("commitSync failed with exception: ", e);
            autoCommitFail.inc();
          }
          autoCommitAttempt.inc();
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
    }
  }

  private void initializeOffsetIfNeeded(final TopicPartition topicPartition, final OffsetAndMetadata currentOffsetMetadata) {
    if (currentOffsetMetadata == null) {
      // If there's no explicit offset - commit an explicit offset according to the offset reset strategy.
      if (OffsetResetStrategy.EARLIEST.name().equalsIgnoreCase(offsetResetStrategy)) {
        kafkaConsumer.seekToBeginning(topicPartition);
      } else {
        kafkaConsumer.seekToEnd(topicPartition);
      }
      final long currentPosition = kafkaConsumer.position(topicPartition);
      consumedOffsets.put(topicPartition, new OffsetAndMetadata(currentPosition));
    }
  }
}
