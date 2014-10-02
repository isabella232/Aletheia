package com.outbrain.aletheia.tutorial;

import com.google.common.collect.Iterables;
import com.outbrain.aletheia.datum.consumption.*;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.DatumProducerBuilder;
import com.outbrain.aletheia.datum.production.DatumProducerConfig;
import com.outbrain.aletheia.datum.production.InMemoryProductionEndPoint;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import org.joda.time.Instant;

import java.util.List;
import java.util.Map;

public class InMemoryProduceConsume {

  public static void main(String[] args) {

    System.out.println("Welcome to Aletheia 101 - In memory production & consumption.");

    System.out.println("Building a DatumProducer...");

    final InMemoryProductionEndPoint inMemoryProductionEndPoint =
            new InMemoryProductionEndPoint(InMemoryProductionEndPoint.EndPointType.RawDatumEnvelope);

    final DatumSerDe<MyDatum> datumSerDe = new JsonDatumSerDe<>(MyDatum.class);

    final DatumProducerConfig datumProducerConfig = new DatumProducerConfig(1, "myHostName");

    final DatumProducer<MyDatum> datumProducer =
            DatumProducerBuilder
                    .forDomainClass(MyDatum.class)
                    .deliverDataTo(inMemoryProductionEndPoint, datumSerDe)
                    .build(datumProducerConfig);

    System.out.println("Done building a DatumProducer.");

    final String myInfo = "myInfo";

    System.out.println("Delivering a datum with info field = " + myInfo);

    datumProducer.deliver(new MyDatum(Instant.now(), myInfo));

    System.out.println("Building a DatumConsumer...");

    final ConsumptionEndPoint consumptionEndPoint =
            new ManualFeedConsumptionEndPoint(inMemoryProductionEndPoint.getReceivedData());

    final DatumConsumerConfig datumConsumerConfig = new DatumConsumerConfig(1, "myHostName");

    final Map<ConsumptionEndPoint, List<? extends DatumConsumer<MyDatum>>> datumConsumerMap =
            DatumConsumerBuilder
                    .forDomainClass(MyDatum.class)
                    .consumeDataFrom(consumptionEndPoint, datumSerDe)
                    .build(datumConsumerConfig);

    System.out.println("Done building a DatumConsumer.");

    System.out.println("Iterating over received data...");

    for (final MyDatum myDatum : Iterables.getFirst(datumConsumerMap.get(consumptionEndPoint), null).datums()) {
      System.out.println("Received a datum with info field = " + myDatum.getInfo());
      // we break forcibly here after receiving one datum since we only sent a single datum
      // and further iteration(s) will block
      break;
    }

    System.out.println("Exiting...");
  }
}
