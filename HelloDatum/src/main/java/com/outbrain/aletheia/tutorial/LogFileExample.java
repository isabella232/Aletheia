package com.outbrain.aletheia.tutorial;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.DatumProducerBuilder;
import com.outbrain.aletheia.configuration.PropertyUtils;
import com.outbrain.aletheia.configuration.logFile.LogFileEndPointTemplate;
import com.outbrain.aletheia.datum.production.DatumProducer;
import com.outbrain.aletheia.datum.production.logFile.LogFileDatumEnvelopeSenderFactory;
import com.outbrain.aletheia.datum.production.logFile.LogFileProductionEndPoint;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.Properties;

public class LogFileExample {

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

  public static void main(String[] args) throws IOException {

    System.out.println("Welcome to Aletheia 101 - Log file production");

    System.out.println("Building a DatumProducer...");

    final Properties properties = new Properties();
    properties.setProperty(AletheiaConfig.ROUTING_CONFIG_PATH,
                           "com/outbrain/aletheia/configuration/routing.withLogFile.json");
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

    AletheiaConfig.registerEndPointTemplate(LogFileEndPointTemplate.TYPE, LogFileEndPointTemplate.class);

    try (final DatumProducer<MyDatum> datumProducer =
            DatumProducerBuilder
                    .withConfig(MyDatum.class,
                                new AletheiaConfig(PropertyUtils.override(properties)
                                                                .with(getBreadcrumbsProps("producer"))
                                                                .all()))
                    .registerProductionEndPointType(LogFileProductionEndPoint.class,
                                                    new LogFileDatumEnvelopeSenderFactory())
                    .build()) {

      final String myInfo = "myInfo";

      System.out.println("Delivering a datum with info field = " + myInfo);

      datumProducer.deliver(new MyDatum(Instant.now(), myInfo));
    }

    System.out.println("Done.");
  }

}
