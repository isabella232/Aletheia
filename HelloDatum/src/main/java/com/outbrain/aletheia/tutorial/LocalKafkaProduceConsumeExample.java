package com.outbrain.aletheia.tutorial;

import com.google.common.collect.Iterables;
import com.outbrain.aletheia.datum.consumption.ConsumptionEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumer;
import com.outbrain.aletheia.datum.consumption.DatumConsumerBuilder;
import com.outbrain.aletheia.datum.consumption.DatumConsumerConfig;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaDatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.DatumProducerBuilder;
import com.outbrain.aletheia.datum.production.DatumProducerConfig;
import com.outbrain.aletheia.datum.production.kafka.KafkaDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.kafka.KafkaTopicProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import org.joda.time.Instant;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LocalKafkaProduceConsumeExample {

  private static final String END_POINT_NAME = "myTestEndPoint";
  private static final String TOPIC_NAME = "test";
  private static final String CONSUMER_GROUP_ID = "test_consumers";

  public static void main(String[] args) {

    System.out.println("Welcome to Aletheia 101 - Kafka production & consumption");
    System.out.println("Local Kafka server is assumed to be running on localhost:9092.");
    System.out.println("Local Zookeeper server is assumed to be running on localhost:2181.");

    System.out.println("Building a DatumProducer...");

    final DatumProducer<MyDatum> datumProducer =
            DatumProducerBuilder
                    .forDomainClass(MyDatum.class)
                    .registerProductionEndPointType(KafkaTopicProductionEndPoint.class,
                                                    new KafkaDatumEnvelopeSenderFactory())
                    .deliverDataTo(
                            new KafkaTopicProductionEndPoint(
                                    "localhost:9092",
                                    TOPIC_NAME,
                                    KafkaTopicProductionEndPoint.EndPointType.RawDatumEnvelope,
                                    1,
                                    END_POINT_NAME,
                                    new Properties()),
                            new JsonDatumSerDe<>(MyDatum.class))
                    .build(new DatumProducerConfig(1, "localhost"));


    System.out.println("Done building a DatumProducer.");

    final MyDatum datum = new MyDatum(Instant.now(), "myInfo");

    System.out.println("Delivering a breadcrumb datum: " + datum.toString());

    datumProducer.deliver(datum);

    System.out.println("Building a DatumConsumer...");

    final Map<ConsumptionEndPoint, List<? extends DatumConsumer<MyDatum>>> endPointListMap =
            DatumConsumerBuilder
                    .forDomainClass(MyDatum.class)
                    .registerConsumptionEndPointType(KafkaTopicConsumptionEndPoint.class,
                                                     new KafkaDatumEnvelopeFetcherFactory())
                    .consumeDataFrom(
                            new KafkaTopicConsumptionEndPoint(
                                    "localhost:2181",
                                    TOPIC_NAME,
                                    CONSUMER_GROUP_ID,
                                    END_POINT_NAME,
                                    1,
                                    KafkaTopicConsumptionEndPoint.EndPointType.RawDatumEnvelope,
                                    new Properties()),
                            new JsonDatumSerDe<>(MyDatum.class))
                    .build(new DatumConsumerConfig(1, "localhost"));

    System.out.println("Done building a DatumConsumer.");

    System.out.println("Iterating over received data...");

    for (final MyDatum myDatum : Iterables.getFirst(endPointListMap.values(), null).get(0).datums()) {
      System.out.println("Received a MyDatum datum: " + myDatum.toString());
      // we break forcibly here after receiving one datum since we only sent a single datum
      // and further iteration(s) will block
      break;
    }

    System.out.println("Exiting...");
  }
}
