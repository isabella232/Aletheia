package com.outbrain.aletheia.tutorial;

import com.google.common.collect.Iterables;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStreamConfig;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStreamsBuilder;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.DatumProducerBuilder;
import com.outbrain.aletheia.datum.production.DatumProducerConfig;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import org.joda.time.Instant;

import java.util.List;

public class InMemoryProduceConsumeExample {

  public static void main(String[] args) {

    System.out.println("Welcome to Aletheia 101 - In memory production & consumption.");

    System.out.println("Building a DatumProducer...");

    final InMemoryEndPoint inMemoryProductionEndPoint = new InMemoryEndPoint("DemoEndpoint", 10);

    final DatumProducer<MyDatum> datumProducer =
            DatumProducerBuilder
                    .forDomainClass(MyDatum.class)
                    .deliverDataTo(inMemoryProductionEndPoint, new JsonDatumSerDe<>(MyDatum.class))
                    .build(new DatumProducerConfig(1, "myHostName"));

    final String myInfo = "myInfo";

    System.out.println("Delivering a datum with info field = " + myInfo);

    datumProducer.deliver(new MyDatum(Instant.now(), myInfo));

    final List<DatumConsumerStream<MyDatum>> datumConsumerStreams =
            DatumConsumerStreamsBuilder
                    .forDomainClass(MyDatum.class)
                    .consumeDataFrom(inMemoryProductionEndPoint, new JsonDatumSerDe<>(MyDatum.class))
                    .build(new DatumConsumerStreamConfig(1, "myHostName"));

    System.out.println("Iterating over received data...");

    for (final MyDatum myDatum : Iterables.getFirst(datumConsumerStreams, null).datums()) {
      System.out.println("Received a datum with info field = " + myDatum.getInfo());
      // we break forcibly here after receiving one datum since we only sent a single datum
      // and further iteration(s) will block
      break;
    }

    System.out.println("Exiting...");
  }
}
