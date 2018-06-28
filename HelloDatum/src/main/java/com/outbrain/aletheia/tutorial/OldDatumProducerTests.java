package com.outbrain.aletheia.tutorial;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.DatumProducerBuilder;
import com.outbrain.aletheia.configuration.PropertyUtils;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.kafka.KafkaDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.kafka.KafkaTopicProductionEndPoint;
import org.joda.time.Instant;

import java.util.Properties;
import java.util.UUID;

import static com.outbrain.aletheia.tutorial.KafkaExample.getBreadcrumbsProps;
import static com.outbrain.aletheia.tutorial.KafkaExample.getProperties;

@SuppressWarnings("Duplicates")
public class OldDatumProducerTests {

    public static void main(String[] args) {

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


            while (true) {
                final String myInfo = "myInfo" + UUID.randomUUID();

                datumProducer.deliver(new MyDatum(Instant.now(), myInfo));
                Thread.sleep(4000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
