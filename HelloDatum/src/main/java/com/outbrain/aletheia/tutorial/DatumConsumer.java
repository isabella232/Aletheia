package com.outbrain.aletheia.tutorial;


import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.DatumEnvelopeStreamsBuilder;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.datum.consumption.DatumConsumerStream;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaDatumEnvelopeFetcherFactory;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.production.kafka.KafkaDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.kafka.KafkaTopicProductionEndPoint;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static com.outbrain.aletheia.tutorial.KafkaExample.getConfig;
import static com.outbrain.aletheia.tutorial.KafkaExample.getProperties;

public class DatumConsumer {


    public static void main(String[] args) throws Exception {

        final Properties properties = getProperties();
        AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);


        System.out.println("**********************");
        System.out.println("Consume just envelopes");

        try {
            final List<DatumConsumerStream<DatumEnvelope>> envelopeStreams = DatumEnvelopeStreamsBuilder
                    .createBuilder(
                            getConfig(properties),
                            "envelopes_endpoint")
                    .registerConsumptionEndPointType(KafkaTopicConsumptionEndPoint.class, new KafkaDatumEnvelopeFetcherFactory())
                    .registerBreadcrumbsEndpointType(KafkaTopicProductionEndPoint.class, new KafkaDatumEnvelopeSenderFactory())
                    .setBreadcrumbsEndpoint("kafka_breadcrumbs_endpoint")
                    .setEnvelopeFilter(envelope -> true)
                    .createStreams();

            System.out.println("Iterating over received envelopes...");

            // You can take a look at routing.withKafka.json and see that the my_datum_id has two endpoints (two different kafka topics)
            // For that reason we expect to have two events here, one from each topics.
            int numberOfMessagesConsumed = 0;

            Iterator<DatumEnvelope> datumStream = envelopeStreams.stream().findFirst().get().datums().iterator();
            while (true) {


                if (datumStream.hasNext()) {

                    DatumEnvelope envelope = datumStream.next();
                    System.out.println(envelope.toString());

                }
                numberOfMessagesConsumed++;


                // Thread.sleep(50);


            }
        } catch (Exception e) {


            System.out.println(e);
            throw e;

        }
    }
}
