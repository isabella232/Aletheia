package com.outbrain.aletheia;

import java.util.Properties;

public class MockAletheiaConfig {

  public static final String PRODUCER_SOURCE = "UnitTestProducer";

  public static Properties getTestProperties() {
    final Properties properties = new Properties();
    properties.setProperty("datum.class", MyDatum.class.getName());
    properties.setProperty(AletheiaConfig.ENDPOINTS, endPointsConfig);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, endPointGroupsConfig);
    properties.setProperty(AletheiaConfig.ROUTING, routingConfig);
    properties.setProperty(AletheiaConfig.SERDES, serDeConfig);
    properties.setProperty("aletheia.producer.incarnation", "1");
    properties.setProperty("aletheia.producer.source", PRODUCER_SOURCE);

    return properties;
  }

  public static final String routingConfig = "{\n" +
      "  \"my.datum\": {\n" +
      "    \"routes\": [\n" +
      "      {\n" +
      "        \"endpoint\": \"my_endpoint_1\",\n" +
      "        \"serDe\": \"json\"\n" +
      "      }\n" +
      "    ],\n" +
      "    \"routeGroups\": [\n" +
      "      \"my_routeGroup\"\n" +
      "    ],\n" +
      "    \"datum.key.selector.class\": \"com.outbrain.aletheia.MyDatum$MyDatumKeySelector\",\n" +
      "    \"breadcrumbs\": \"myBreadcrumbsEndpointId\"\n" +
      "  },\n" +
      "  \"another.datum\": {\n" +
      "    \"routes\": [\n" +
      "      {\n" +
      "        \"endpoint\": \"my_endpoint_2\",\n" +
      "        \"serDe\": \"json\"\n" +
      "      }\n" +
      "    ],\n" +
      "    \"datum.key.selector.class\": \"\",\n" +
      "    \"breadcrumbs\": \"myBreadcrumbsEndpointId\"\n" +
      "  }\n" +
      "}";

  public static final String endPointGroupsConfig = "{\n" +
      "  \"my_routeGroup\": [\n" +
      "    {\n" +
      "      \"endpoint\": \"my_endpoint_2\",\n" +
      "      \"serDe\": \"json\"\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  public static final String endPointsConfig = "{\n" +
      " \"my_endpoint_1\": {\n" +
      " \"type\": \"kafka\",\n" +
      " \"topic.name\": \"my.datum\",\n" +
      " \"produce\": {\n" +
      "   \"bootstrap.servers\": \"10.1.1.1:9092\"\n" +
      " },\n" +
      " \"consume\": {\n" +
      "   \"bootstrap.servers\": \"10.1.1.1:9092\",\n" +
      "   \"group.id\": \"aletheia-test-config\",\n" +
      "   \"concurrency.level\": \"1\"\n" +
      "  }\n" +
      " },\n" +
      " \"my_endpoint_2\": {\n" +
      " \"type\": \"kafka\",\n" +
      " \"topic.name\": \"also.my.datum\",\n" +
      " \"consume\": {\n" +
      "   \"bootstrap.servers\": \"10.1.1.2:9092\",\n" +
      "   \"group.id\": \"aletheia-test-config-2\",\n" +
      "   \"concurrency.level\": \"1\"\n" +
      "  }\n" +
      " }\n" +
      "}";

  public static final String serDeConfig = "{\n" +
      "  \"json\": {\n" +
      "    \"@class\": \"com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe\",\n" +
      "    \"datum.class\": \"${datum.class}\"\n" +
      "  }\n" +
      "}";

  public static final String EMPTY_CONFIG = "{}";
}
