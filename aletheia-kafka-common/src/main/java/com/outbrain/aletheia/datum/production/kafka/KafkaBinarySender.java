package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.metrics.kafka.KafkaMetrics;
import com.outbrain.aletheia.datum.production.DatumKeyAwareNamedSender;
import com.outbrain.aletheia.datum.production.DeliveryCallback;
import com.outbrain.aletheia.datum.production.EmptyCallback;
import com.outbrain.aletheia.datum.production.SilentSenderException;
import com.outbrain.aletheia.metrics.MoreExceptionUtils;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.Summary;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link DatumKeyAwareNamedSender} implementation that sends binary data to Kafka.
 */
public class KafkaBinarySender implements DatumKeyAwareNamedSender<byte[]> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaBinarySender.class);
  private static final int TEN_SECONDS = 10000;
  private static final long SEND_RESULT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(300);
  private static final String SYNC_TYPE = "sync";

  private final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint;
  private final Timer connectionTimer = new Timer(KafkaBinarySender.class.getSimpleName() + "-reconnectTimer");
  private final Properties customConfiguration;
  private final KafkaCallbackTransformer kafkaCallbackTransformer;

  private KafkaProducer<String, byte[]> producer;
  private boolean connected = false;
  private final boolean isSync;
  private ScheduledExecutorService metricsReporterScheduledExecutorService;

  private Counter sendCount;
  private Counter sendAttemptsFailures;
  private Summary sendDuration;
  private Summary messageSizeSummary;

  public KafkaBinarySender(final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint,
                           final KafkaCallbackTransformer kafkaCallbackTransformer,
                           final MetricsFactory metricFactory) {

    this.kafkaTopicDeliveryEndPoint = kafkaTopicDeliveryEndPoint;
    this.kafkaCallbackTransformer = kafkaCallbackTransformer;

    logger.info("Creating kafka sender for endpoint:" + kafkaTopicDeliveryEndPoint.toString());

    customConfiguration = getProducerConfig();

    // Keep similar behavior to Kafka 0.8 API
    final String producerType = customConfiguration.getProperty("producer.type", SYNC_TYPE);
    isSync = SYNC_TYPE.equals(producerType);

    initMetrics(metricFactory, customConfiguration);

    Runtime.getRuntime().addShutdownHook(new Thread(this::close));

    connect();
  }

  private void initMetrics(final MetricsFactory metricFactory, final Properties customConfiguration) {
    sendCount = metricFactory.createCounter("sendAttemptsSuccess", "Counts number of successful attempts");
    sendDuration = metricFactory.createSummary("sendAttemptsDuration", "measure duration of the attempts");
    messageSizeSummary = metricFactory.createSummary("messageSize", "size of the message");
    sendAttemptsFailures = metricFactory.createCounter("sendAttemptsFailures", "Counts number of failed send attempts", "error");

    metricsReporterScheduledExecutorService = KafkaMetrics.reportTo(metricFactory,
            customConfiguration.getProperty("client.id"),
            Duration.standardSeconds(
                    Long.parseLong(
                            kafkaTopicDeliveryEndPoint.getProperties()
                                    .getProperty(
                                            "kafka.client.metrics.periodic.sync.intervalInSec",
                                            "30"))));
  }

  private boolean singleConnect(final Properties config) {
    try {
      producer = new KafkaProducer<>(config);
      connected = true;
      logger.info("Connected to kafka. for destination {}", this.kafkaTopicDeliveryEndPoint.getName());
      return true;
    } catch (final Exception e) {
      logger.error("Failed to connect to kafka. (topic name : {} )", kafkaTopicDeliveryEndPoint.getTopicName(), e);
      return false;
    }
  }

  private void connect() {
    connected = singleConnect(customConfiguration);
    if (!connected) {
      logger.warn("Failed attempting connection to kafka ({}).", customConfiguration.toString());
      connectionTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          final Thread myThread = Thread.currentThread();
          final String prevName = myThread.getName();
          myThread.setName("Connecting to kafka " + System.currentTimeMillis());
          try {
            connect();
          } finally {
            myThread.setName(prevName);
          }
        }
      }, TEN_SECONDS);
    }
  }

  private Properties getProducerConfig() {

    final Properties producerProperties = (Properties) kafkaTopicDeliveryEndPoint.getProperties().clone();

    if (producerProperties.getProperty("value.serializer") != null
            || producerProperties.getProperty("key.serializer") != null) {
      logger.warn("serializer cannot be provided as producer properties. "
              + "Overriding manually to be the correct serialization type.");
    }

    producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
    producerProperties.setProperty("value.serializer", ByteArraySerializer.class.getName());

    producerProperties.setProperty("client.id",
            kafkaTopicDeliveryEndPoint.getProperties().getProperty("client.id", UUID.randomUUID().toString()));

    producerProperties.setProperty("bootstrap.servers", kafkaTopicDeliveryEndPoint.getBrokerList());
    producerProperties.setProperty("metric.reporters", "com.outbrain.aletheia.datum.metrics.kafka.KafkaMetrics");

    logger.warn("Using producer config: {}", producerProperties);

    return producerProperties;
  }

  @Override
  public void send(final byte[] data, final String key) throws SilentSenderException {
    send(data, key, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void send(final byte[] data, final String key, final DeliveryCallback deliveryCallback) throws SilentSenderException {
    if (!connected) {
      sendAttemptsFailures.inc("failureDueToUnconnected");
      return;
    }

    com.outbrain.swinfra.metrics.timing.Timer timer = sendDuration.startTimer();

    try {
      // Send datum
      final Future<RecordMetadata> sendResult =
              producer.send(
                      new ProducerRecord<>(kafkaTopicDeliveryEndPoint.getTopicName(), key, data),
                      isSync ? null : kafkaCallbackTransformer.transform(
                              deliveryCallback,
                              kafkaTopicDeliveryEndPoint));

      // Wait for result if configured as a sync producer type
      if (isSync) {
        sendResult.get(SEND_RESULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      }

      sendCount.inc();
      messageSizeSummary.observe(data.length);
    } catch (final BufferExhaustedException e) {
      throw new SilentSenderException(e);
    } catch (final Exception e) {
      sendAttemptsFailures.inc(MoreExceptionUtils.getType(e));
      logger.error("Error while sending message to kafka.", e);
    } finally {
      timer.stop();
    }
  }

  @Override
  public String getName() {
    return kafkaTopicDeliveryEndPoint.getName();
  }

  @Override
  public void close() {
    if (metricsReporterScheduledExecutorService != null) {
      metricsReporterScheduledExecutorService.shutdown();
    }
    if (connectionTimer != null) {
      connectionTimer.cancel();
    }
    if (connected) {
      if (producer != null) {
        try {
          logger.info("Closing producer for endpoint: {}", getName());
          producer.close();
        } catch (final Exception e) {
          logger.info("Could not close producer. Continuing", e);
        } finally {
          connected = false;
        }
      }
    }
  }
}


