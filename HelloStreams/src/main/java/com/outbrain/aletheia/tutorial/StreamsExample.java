package com.outbrain.aletheia.tutorial;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.kafka.streams.AletheiaStreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsExample {

  public static void main(String[] args) throws Exception {

    System.out.println("Welcome to Aletheia 201 - Kafka Streams consumption.");

    final Properties properties = new Properties();
    properties.setProperty(AletheiaConfig.ROUTING_CONFIG_PATH,
        "com/outbrain/aletheia/configuration/routing.withStreams.json");
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

    // Streams App Configuration
    Map<String, Object> appConfig = new HashMap<>();
    appConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "aletheia-streams-example");
    // For this example we use the wall-clock timestamp extractor (Processing-time semantics)
    appConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

    // Create the AletheiaStreams helper
    final AletheiaStreams aletheiaStreams = new AletheiaStreams(appConfig);

    // Configure Aletheia
    final AletheiaConfig aletheiaConfig = new AletheiaConfig(properties);

    // Build a source stream that consumes MyDatum
    final KStream<String, MyDatum> myDatumStream = aletheiaStreams.stream(
        aletheiaConfig,
        MyDatum.class,
        "kafka_endpoint",
        "json");

    // Define your processing topology
    myDatumStream.print();

    // Build the KafkaStreams instance
    final KafkaStreams streams = aletheiaStreams.constructStreamsInstance();

    // Start processing
    System.out.println("Starting KafkaStreams processing");
    streams.start();

    // For this example we stop processing after some time,
    // Usually a streams application would be running indefinitely
    Thread.sleep(10000L);

    streams.close();

    System.out.println("Done.");
  }
}
