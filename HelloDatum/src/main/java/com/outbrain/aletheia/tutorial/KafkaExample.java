package com.outbrain.aletheia.tutorial;

import com.google.common.collect.Iterables;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.DatumConsumerStreamsBuilder;
import com.outbrain.aletheia.DatumEnvelopeStreamsBuilder;
import com.outbrain.aletheia.DatumProducerBuilder;
import com.outbrain.aletheia.configuration.PropertyUtils;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaDatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.kafka.KafkaDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.kafka.KafkaTopicProductionEndPoint;

import org.joda.time.Instant;

import java.util.List;
import java.util.Properties;

/**
 * This is an example of how to use aletheia features to produce and consume events
 * For this example you need to have a clean install of local kafka cluster running.
 * i.e. There is no cleanup.
 */


public class KafkaExample {

  private static final int BREADCRUMBS_FLUSH_INTERVAL_SEC = 1;

  public static void main(String[] args) throws Exception {

    System.out.println("Welcome to Aletheia 101 - Kafka production & consumption.");

    System.out.println("Building a DatumProducer...");

    final Properties properties = getProperties();

    AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);

    try (final DatumProducer<MyDatum> datumProducer =
                 DatumProducerBuilder
                         .withConfig(MyDatum.class,
                                 new AletheiaConfig(PropertyUtils.override(properties)
                                         .with(getBreadcrumbsProps("producer"))
                                         .all()))
                         .registerProductionEndPointType(KafkaTopicProductionEndPoint.class,
                                 new KafkaDatumEnvelopeSenderFactory())
                         .build()) {

      final String myInfo = "myInfo";

      System.out.println("Delivering a datum with info field = " + myInfo);

      datumProducer.deliver(new MyDatum(Instant.now(), myInfo));
    }

    final List<DatumConsumerStream<MyDatum>> datumConsumerStreams =
            DatumConsumerStreamsBuilder
                    .withConfig(MyDatum.class,
                            getConfig(properties))
                    .registerConsumptionEndPointType(KafkaTopicConsumptionEndPoint.class,
                            new KafkaDatumEnvelopeFetcherFactory())
                    .consumeDataFrom(new Route("kafka_endpoint", "json"))
                    .build();

    System.out.println("Iterating over received data...");

    for (final MyDatum myDatum : Iterables.getFirst(datumConsumerStreams, null).datums()) {
      System.out.println(String.format("Received a datum: '%s %s'", myDatum.getTimestamp(), myDatum.getInfo()));
      // we break forcibly here after receiving one datum since we only sent a single datum
      // and further iteration(s) will block
      break;
    }

    for (final DatumConsumerStream<MyDatum> datumConsumerStream : datumConsumerStreams) {
      datumConsumerStream.close();
    }

    System.out.println("**********************");
    System.out.println("Consume just envelopes");

    final List<DatumConsumerStream<DatumEnvelope>> envelopeStreams = DatumEnvelopeStreamsBuilder
            .createBuilder(
                    getConfig(properties),
                    "envelopes_endpoint",
                    new KafkaDatumEnvelopeFetcherFactory())
            .registerBreadcrumbsEndpointType(KafkaTopicProductionEndPoint.class, new KafkaDatumEnvelopeSenderFactory())
            .setBreadcrumbsEndpoint("kafka_breadcrumbs_endpoint")
            .setEnvelopeFilter(envelope -> true)
            .createStreams();

    System.out.println("Iterating over received envelopes...");

    // You can take a look at routing.withKafka.json and see that the my_datum_id has two endpoints (two different kafka topics)
    // For that reason we expect to have two events here, one from each topics.
    int numberOfMessagesConsumed = 0;
    for (final DatumEnvelope envelope : Iterables.getFirst(envelopeStreams, null).datums()) {
      System.out.println(String.format("Received an envelope: '%s %s'", envelope.getCreationTime(), envelope.getDatumTypeId()));
      numberOfMessagesConsumed++;
      if (numberOfMessagesConsumed == 2) {
        // We break forcibly here after receiving all datums we sent since further iteration(s) will block.
        break;
      }
    }

    System.out.println("Waiting for periodic Breadcrumbs flush");
    Thread.sleep(BREADCRUMBS_FLUSH_INTERVAL_SEC * 1000 * 2);

    for (final DatumConsumerStream<DatumEnvelope> envelopeConsumerStream : envelopeStreams) {
      envelopeConsumerStream.close();
    }

    System.out.println("Done.");
  }

  private static AletheiaConfig getConfig(Properties properties) {
    return new AletheiaConfig(PropertyUtils
            .override(properties)
            .with(getBreadcrumbsProps("consumer"))
            .all());
  }

  private static Properties getProperties() {
    final Properties properties = new Properties();
    properties.setProperty(AletheiaConfig.ROUTING_CONFIG_PATH,
            "com/outbrain/aletheia/configuration/routing.withKafka.json");
    properties.setProperty(AletheiaConfig.SERDES_CONFIG_PATH,
            "com/outbrain/aletheia/configuration/serdes.json");
    properties.setProperty(AletheiaConfig.ENDPOINTS_CONFIG_PATH,
            "com/outbrain/aletheia/configuration/endpoints.json");
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS_CONFIG_PATH,
            "com/outbrain/aletheia/configuration/endpoint.groups.json");
    properties.setProperty("aletheia.producer.incarnation", "1");
    properties.setProperty("aletheia.consumer.incarnation", "1");
    properties.setProperty("aletheia.consumer.source", "myHostName");
    properties.setProperty("aletheia.producer.source", "myHostName");
    return properties;
  }

  private static Properties getBreadcrumbsProps(final String source) {
    final Properties properties = new Properties();
    properties.setProperty("aletheia.breadcrumbs.bucketDurationSec", Long.toString(1));
    properties.setProperty("aletheia.breadcrumbs.flushIntervalSec", Long.toString(BREADCRUMBS_FLUSH_INTERVAL_SEC));
    properties.setProperty("aletheia.breadcrumbs.fields.application", "HelloDatum");
    properties.setProperty("aletheia.breadcrumbs.fields.source", source);
    properties.setProperty("aletheia.breadcrumbs.fields.tier", "Tutorials");
    properties.setProperty("aletheia.breadcrumbs.fields.datacenter", "Local");
    return properties;
  }
}
