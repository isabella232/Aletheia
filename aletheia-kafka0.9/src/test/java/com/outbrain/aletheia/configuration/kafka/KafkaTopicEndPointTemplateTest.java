package com.outbrain.aletheia.configuration.kafka;

import com.outbrain.aletheia.AletheiaConfig;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;

/**
 * Created by slevin on 6/25/15.
 */
public class KafkaTopicEndPointTemplateTest {

  private final Properties properties = new Properties();

  final String endPointsConfig = "{\n" +
                                 "  \"my_endpoint_1\": {\n" +
                                 "    \"type\": \"kafka\",\n" +
                                 "    \"topic.name\": \"test.datum\",\n" +
                                 "    \"produce\": {\n" +
                                 "      \"producer.dummy\": \"${producer.property}\",\n" +
                                 "      \"bootstrap.servers\": \"${my.broker.list}\"\n" +
                                 "      },\n" +
                                 "    \"consume\": {\n" +
                                 "      \"consumer.dummy\": \"${consumer.property}\",\n" +
                                 "      \"bootstrap.servers\": \"${my.broker.list}\",\n" +
                                 "      \"group.id\": \"${my.group.id.value}\",\n" +
                                 "      \"concurrency.level\": \"${my.concurrency.level.value}\"\n" +
                                 "    }\n" +
                                 "  },\n" +
                                 "  \"my_endpoint_2\": {\n" +
                                 "    \"type\": \"kafka\",\n" +
                                 "    \"topic.name\": \"test.datum\",\n" +
                                 "    \"produce\": {},\n" +
                                 "    \"consume\": {}\n" +
                                 "  }\n" +
                                 "}";

  private final AletheiaConfig config;

  public KafkaTopicEndPointTemplateTest() throws Exception {
    final String EMPTY_CONFIG = "{}";
    properties.setProperty("datum.class", "com.outbrain.aletheia.internal.configuration.ConfigTest$MyDatum");
    properties.setProperty(AletheiaConfig.ENDPOINTS, endPointsConfig);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.ROUTING, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.SERDES, EMPTY_CONFIG);
    AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);
    config = new AletheiaConfig(properties);
  }

  @Test
  public void testKafkaProductionEndpoint() throws Exception {
    properties.setProperty("producer.property", "some.value");
    properties.setProperty("my.broker.list", "my.broker.list.value");

    assertNotNull(config.getProductionEndPoint("my_endpoint_1"));
  }

  @Test
  public void testKafkaConsumptionEndpoint() throws Exception {
    properties.setProperty("consumer.property", "some.value.1");
    properties.setProperty("my.broker.list", "my.broker.list.value");
    properties.setProperty("my.group.id.value", "some.value.3");
    properties.setProperty("my.concurrency.level.value", "1");

    assertNotNull(config.getConsumptionEndPoint("my_endpoint_1"));
  }

}