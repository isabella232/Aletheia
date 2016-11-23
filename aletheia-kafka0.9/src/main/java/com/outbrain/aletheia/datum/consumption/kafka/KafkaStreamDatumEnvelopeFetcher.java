package com.outbrain.aletheia.datum.consumption.kafka;

import com.outbrain.aletheia.datum.consumption.DatumEnvelopeFetcher;
import com.outbrain.aletheia.datum.envelope.AvroDatumEnvelopeSerDe;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
 *
 * The DatumEnvelopeFetcher is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized.
 */
class KafkaStreamDatumEnvelopeFetcher implements DatumEnvelopeFetcher {
  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamDatumEnvelopeFetcher.class);
  private static final long POLL_TIMEOUT_MS = Long.MAX_VALUE;
  public static final String AUTO_COMMIT = "AutoCommit";

  private final KafkaConsumer<String, byte[]> kafkaConsumer;
  private Iterator<ConsumerRecord<String, byte[]>> consumerRecordIterator = Collections.emptyIterator();
  private final KafkaTopicConsumptionEndPoint consumptionEndPoint;
  private final AvroDatumEnvelopeSerDe avroDatumEnvelopeSerDe = new AvroDatumEnvelopeSerDe();

  private volatile boolean isWaitingForPoll = false;

  private final ScheduledExecutorService autoOffsetCommitExecutor;
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> consumedOffsets = new ConcurrentHashMap<>();
  private final Counter autoCommitAttempt;
  private final Counter autoCommitSuccess;
  private final Counter autoCommitFail;

  public KafkaStreamDatumEnvelopeFetcher( final KafkaConsumer<String, byte[]> consumer,
                                          final KafkaTopicConsumptionEndPoint consumptionEndPoint,
                                          final MetricsFactory metricFactory) {
    this.kafkaConsumer = consumer;
    this.consumptionEndPoint = consumptionEndPoint;

    this.autoCommitAttempt = metricFactory.createCounter(AUTO_COMMIT, "Attempts");
    this.autoCommitSuccess = metricFactory.createCounter(AUTO_COMMIT, "Success");
    this.autoCommitFail = metricFactory.createCounter(AUTO_COMMIT, "Failure");

    final long autoCommitInterval =
            Long.parseLong(consumptionEndPoint.getProperties()
                                              .getProperty( "auto.commit.interval.ms",
                                                            "5000"));

    // Executor for committing consumer offsets
    //  (Keep similar behavior to Kafka 0.8 High Level Consumer)
    autoOffsetCommitExecutor = Executors.newScheduledThreadPool(1);
    autoOffsetCommitExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          synchronized (kafkaConsumer) {
            kafkaConsumer.commitSync(consumedOffsets);
          }
          autoCommitSuccess.inc();
        } catch (Exception e) {
          logger.error("commitSync failed with exception: ", e);
          autoCommitFail.inc();
        }
        autoCommitAttempt.inc();
      }
    }, autoCommitInterval, autoCommitInterval, TimeUnit.MILLISECONDS);

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

          // Update consumed offsets
          consumedOffsets
              .put(   new TopicPartition(record.topic(), record.partition()),
                      new OffsetAndMetadata(record.offset() + 1));

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
    try {
      synchronized (kafkaConsumer) {
        isWaitingForPoll = true;
        kafkaConsumer.commitSync(consumedOffsets);
        final ConsumerRecords<String, byte[]> consumerRecords = kafkaConsumer.poll(POLL_TIMEOUT_MS);
        consumerRecordIterator = consumerRecords.iterator();
      }
    } finally {
      isWaitingForPoll = false;
    }
    return consumerRecordIterator;
  }

  private void shutdown() {
    autoOffsetCommitExecutor.shutdown();

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
}
