package com.outbrain.aletheia.kafka.streams;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.datum.consumption.kafka.KafkaTopicConsumptionEndPoint;
import com.outbrain.aletheia.kafka.serialization.AletheiaSerdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An helper class for consuming Aletheia datums as Kafka Streams
 */
public class AletheiaStreams {

  private final Map<String, Object> originalConfig;
  private final Map<String, Object> effectiveConfig = new HashMap<>();
  private final Set<String> bootstrapServers = new HashSet<>();
  private final KStreamBuilder kstreamBuilder;

  public AletheiaStreams(final Map<String, Object> streamsAppConfig) {
    AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);
    this.kstreamBuilder = new KStreamBuilder();
    this.originalConfig = streamsAppConfig;
  }

  public <TDomainClass> KStream<String, TDomainClass> stream(final AletheiaConfig config,
                                                             final Class<TDomainClass> domainClass,
                                                             final String consumeFromEndPointId,
                                                             final String datumSerDeId) {
    final KafkaTopicConsumptionEndPoint consumptionEndPoint = (KafkaTopicConsumptionEndPoint) config.getConsumptionEndPoint(consumeFromEndPointId);
    bootstrapServers.add(consumptionEndPoint.getBrokers());

    final Serde<TDomainClass> valueSerDe = AletheiaSerdes.serdeFrom(domainClass, datumSerDeId, config);
    return kstreamBuilder.stream(Serdes.String(), valueSerDe, consumptionEndPoint.getTopicName());
  }

  public KafkaStreams constructStreamsInstance() {
    // Validate and set bootstrap servers config
    if (!originalConfig.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      effectiveConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    }

    // Override with original config
    effectiveConfig.putAll(originalConfig);

    return new KafkaStreams(kstreamBuilder, new StreamsConfig(effectiveConfig));
  }

  public KStreamBuilder getKStreamBuilder() {
    return kstreamBuilder;
  }

  public Map<String, Object> getEffectiveConfig() {
    return Collections.unmodifiableMap(effectiveConfig);
  }

  private String getBootstrapServers() {
    if (bootstrapServers.size() == 0) {
      // No streams created
      throw new IllegalStateException("At least one stream must be created before constructing a KafkaStreams instance");
    }

    if (bootstrapServers.size() > 1) {
      // Multiple clusters - unsupported
      throw new IllegalStateException("All streams consumption end-points must reside on the same Kafka cluster");
    }

    return bootstrapServers.iterator().next();
  }

}
