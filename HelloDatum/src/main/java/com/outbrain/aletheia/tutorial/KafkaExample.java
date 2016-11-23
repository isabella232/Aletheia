package com.outbrain.aletheia.tutorial;

import com.google.common.collect.Iterables;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.DatumConsumerStreamsBuilder;
import com.outbrain.aletheia.DatumProducerBuilder;
import com.outbrain.aletheia.configuration.PropertyUtils;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaDatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.kafka.KafkaDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.kafka.KafkaTopicProductionEndPoint;

import org.joda.time.Instant;

import java.util.List;
import java.util.Properties;

public class KafkaExample {

  private static Properties getBreadcrumbsProps(final String source) {
    final Properties properties = new Properties();
    properties.setProperty("aletheia.breadcrumbs.bucketDurationSec", Long.toString(1));
    properties.setProperty("aletheia.breadcrumbs.flushIntervalSec", Long.toString(1));
    properties.setProperty("aletheia.breadcrumbs.fields.application", "HelloDatum");
    properties.setProperty("aletheia.breadcrumbs.fields.source", source);
    properties.setProperty("aletheia.breadcrumbs.fields.tier", "Tutorials");
    properties.setProperty("aletheia.breadcrumbs.fields.datacenter", "Local");
    return properties;
  }

  public static void main(String[] args) {

    System.out.println("Welcome to Aletheia 101 - Kafka production & consumption.");

    System.out.println("Building a DatumProducer...");

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

    AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);

    final DatumProducer<MyDatum> datumProducer =
            DatumProducerBuilder
                    .withConfig(MyDatum.class,
                                new AletheiaConfig(PropertyUtils.override(properties)
                                                                .with(getBreadcrumbsProps("producer"))
                                                                .all()))
                    .registerProductionEndPointType(KafkaTopicProductionEndPoint.class,
                                                    new KafkaDatumEnvelopeSenderFactory())
                    .build();

    final String myInfo = "myInfo";

    System.out.println("Delivering a datum with info field = " + myInfo);

    datumProducer.deliver(new MyDatum(Instant.now(), myInfo));

    final List<DatumConsumerStream<MyDatum>> datumConsumerStreams =
            DatumConsumerStreamsBuilder
                    .withConfig(MyDatum.class,
                                new AletheiaConfig(PropertyUtils.override(properties)
                                                                .with(getBreadcrumbsProps("consumer"))
                                                                .all()))
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

    System.out.println("Done.");
  }

}