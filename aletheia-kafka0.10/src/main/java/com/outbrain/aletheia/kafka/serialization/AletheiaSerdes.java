package com.outbrain.aletheia.kafka.serialization;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.DatumProducerConfig;
import com.outbrain.aletheia.configuration.routing.RoutingInfo;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * A factory for Aletheia Kafka SerDes
 */
public class AletheiaSerdes {
  public static final String ALETHEIA_PRODUCER_INCARNATION = "aletheia.producer.incarnation";
  public static final String ALETHEIA_PRODUCER_SOURCE = "aletheia.producer.source";

  public static <TDomainClass> Serde<TDomainClass> serdeFrom(final Class<TDomainClass> domainClass, final String serDeId, final AletheiaConfig config) {
    return serdeFrom(domainClass, serDeId, config, null);
  }

  public static <TDomainClass> Serde<TDomainClass> serdeFrom(final Class<TDomainClass> domainClass,
                                                             final String serDeId,
                                                             final AletheiaConfig config,
                                                             final SerDeListener<TDomainClass> listener) {
    final DatumProducerConfig producerConfig = config.getDatumProducerConfig();
    final Map<String, Object> serDeConfig = new HashMap<>();
    serDeConfig.put(ALETHEIA_PRODUCER_INCARNATION, producerConfig.getIncarnation());
    serDeConfig.put(ALETHEIA_PRODUCER_SOURCE, producerConfig.getSource());

    final DatumSerDe<TDomainClass> datumSerDe = config.serDe(serDeId);
    final String datumTypeId = DatumUtils.getDatumTypeId(domainClass);
    final RoutingInfo routing = config.getRouting(datumTypeId);

    final Serializer<TDomainClass> serializer = new AletheiaKafkaSerializer<>(domainClass, datumSerDe, routing.getDatumKeySelector(), listener);
    final Deserializer<TDomainClass> deserializer = new AletheiaKafkaDeserializer<>(datumSerDe, listener);
    final Serde<TDomainClass> serDe = Serdes.serdeFrom(serializer, deserializer);
    serDe.configure(serDeConfig, false);

    return serDe;
  }

}
