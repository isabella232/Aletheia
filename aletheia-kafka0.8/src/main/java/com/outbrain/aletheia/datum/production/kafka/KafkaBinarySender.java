package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.metrics.kafka.KafkaMetrics;
import com.outbrain.aletheia.datum.production.DatumKeyAwareNamedSender;
import com.outbrain.aletheia.datum.production.SilentSenderException;
import com.outbrain.aletheia.metrics.MoreExceptionUtils;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.Histogram;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import kafka.common.QueueFullException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

/**
 * A {@link DatumKeyAwareNamedSender} implementation that sends binary data to Kafka.
 */
public class KafkaBinarySender implements DatumKeyAwareNamedSender<byte[]> {

  private static final Logger logger = LoggerFactory.getLogger(KafkaBinarySender.class);
  private static final int TEN_SECONDS = 10000;

  private final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint;
  private final MetricsFactory metricFactory;
  private final Timer connectionTimer = new Timer(KafkaBinarySender.class.getSimpleName() + "-reconnectTimer");
  private final ProducerConfig customConfiguration;

  private Producer<String, byte[]> producer;
  private boolean connected = false;

  private Counter sendCount;
  private Counter failureDueToUnconnected;
  private com.outbrain.aletheia.metrics.common.Timer sendDuration;
  private Histogram messageSizeHistogram;

  public KafkaBinarySender(final KafkaTopicProductionEndPoint kafkaTopicDeliveryEndPoint,
                           final MetricsFactory metricFactory) {

    this.kafkaTopicDeliveryEndPoint = kafkaTopicDeliveryEndPoint;
    this.metricFactory = metricFactory;

    logger.info("Creating kafka sender for endpoint:" + kafkaTopicDeliveryEndPoint.toString());
    if (this.kafkaTopicDeliveryEndPoint.getAddShutdownHook()) {
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          close();
        }
      }));
    }

    customConfiguration = getProducerConfig();

    initMetrics(metricFactory, customConfiguration);

    connect();
  }

  private void initMetrics(final MetricsFactory metricFactory, final ProducerConfig customConfiguration) {
    sendCount = metricFactory.createCounter("Send.Attempts", "Success");
    sendDuration = metricFactory.createTimer("Send.Attempts", "Duration");
    messageSizeHistogram = metricFactory.createHistogram("Message", "Size", false);
    failureDueToUnconnected = metricFactory.createCounter("Send.Attempts.Failures", "UnableToConnect");

    KafkaMetrics.reportTo(metricFactory,
                          customConfiguration.clientId(),
                          Duration.standardSeconds(
                                  Long.parseLong(
                                          kafkaTopicDeliveryEndPoint.getProperties()
                                                                    .getProperty(
                                                                            "kafka.client.metrics.periodic.sync.intervalInSec",
                                                                            "30"))));
  }

  private boolean singleConnect(final ProducerConfig config) {
    try {
      producer = new Producer<>(config);
      connected = true;
      logger.info("Connected to kafka. for destination " + this.kafkaTopicDeliveryEndPoint.getName());
      return true;
    } catch (final Exception e) {
      logger.error("Failed to connect to kafka. (topic name : " + kafkaTopicDeliveryEndPoint.getTopicName() + " )", e);
      return false;
    }
  }

  private void connect() {
    connected = singleConnect(customConfiguration);
    if (!connected) {
      logger.warn("Failed attempting connection to kafka (" + customConfiguration.toString() + ").");
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

  protected ProducerConfig getProducerConfig() {

    final Properties producerProperties = (Properties) kafkaTopicDeliveryEndPoint.getProperties().clone();

    if (producerProperties.getProperty("serializer.class") != null) {
      logger.warn("serializerClass cannot be provided as producer properties. " +
                  "Overriding manually to be the correct serialization type.");
    }

    producerProperties.setProperty("client.id",
                                   kafkaTopicDeliveryEndPoint.getProperties()
                                                             .getProperty("client.id", UUID.randomUUID().toString()));

    producerProperties.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");

    producerProperties.setProperty("metadata.broker.list", kafkaTopicDeliveryEndPoint.getBrokerList());

    producerProperties.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");

    return new ProducerConfig(producerProperties);
  }

  @Override
  public void send(final byte[] data, final String key) throws SilentSenderException {
    if (!connected) {
      failureDueToUnconnected.inc();
      return;
    }

    final com.outbrain.aletheia.metrics.common.Timer.Context timerContext = sendDuration.time();

    try {
      if (key != null) {
        producer.send(new KeyedMessage<>(kafkaTopicDeliveryEndPoint.getTopicName(), key, data));
      } else {
        producer.send(new KeyedMessage<String, byte[]>(kafkaTopicDeliveryEndPoint.getTopicName(), data));
      }
      sendCount.inc();
      messageSizeHistogram.update(data.length);
    } catch (final QueueFullException e) {
      throw new SilentSenderException(e);
    } catch (final Exception e) {
      metricFactory.createCounter("Send.Attempts.Failures", MoreExceptionUtils.getType(e)).inc();
      logger.error("Error while sending message to kafka.", e);
    } finally {
      timerContext.stop();
    }
  }

  @Override
  public String getName() {
    return kafkaTopicDeliveryEndPoint.getName();
  }

  public void close() {
    if (connected) {
      if (producer != null) {
        try {
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


