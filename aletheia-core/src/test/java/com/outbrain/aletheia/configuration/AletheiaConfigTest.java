package com.outbrain.aletheia.configuration;

import com.google.common.collect.Lists;
import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.configuration.routing.Route;
import com.outbrain.aletheia.configuration.routing.RoutingInfo;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Created by slevin on 6/23/15.
 */
public class AletheiaConfigTest {

  private static class MyDatum {
  }

  final String routingConfig = "{\n" +
                               "  \"test.datum\": {\n" +
                               "    \"routes\": [\n" +
                               "      {\n" +
                               "        \"endpoint\": \"my_endpoint_1\",\n" +
                               "        \"serDe\": \"json\"\n" +
                               "      }\n" +
                               "    ],\n" +
                               "    \"routeGroupIds\": [\n" +
                               "      \"my_routeGroup\"\n" +
                               "    ],\n" +
                               "    \"datum.key.selector.class\": \"myDatumKeySelectorClass\",\n" +
                               "    \"breadcrumbs\": \"myBreadcrumbsEndpointId\"\n" +
                               "  }\n" +
                               "}";

  final String endPointGroupsConfig = "{\n" +
                                      "  \"my_routeGroup\": [\n" +
                                      "    {\n" +
                                      "      \"endpoint\": \"my_endpoint_2\",\n" +
                                      "      \"serDe\": \"json\"\n" +
                                      "    }\n" +
                                      "  ]\n" +
                                      "}";

  final String serDeConfig = "{\n" +
                             "  \"json\": {\n" +
                             "    \"@class\": \"com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe\",\n" +
                             "    \"datum.class\": \"${datum.class}\"\n" +
                             "  }\n" +
                             "}";

  private final AletheiaConfig config;

  public AletheiaConfigTest() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("datum.class", "com.outbrain.aletheia.configuration.AletheiaConfigTest$MyDatum");
    final String EMPTY_CONFIG = "{}";
    properties.setProperty(AletheiaConfig.ENDPOINTS, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, endPointGroupsConfig);
    properties.setProperty(AletheiaConfig.ROUTING, routingConfig);
    properties.setProperty(AletheiaConfig.SERDES, serDeConfig);
    config = new AletheiaConfig(properties);
  }

  @Test
  public void testRoutingConfigReading() throws Exception {
    assertThat(config.getRouting("test.datum"),
               is(new RoutingInfo(Lists.newArrayList(new Route("my_endpoint_1", "json"),
                                                     new Route("my_endpoint_2", "json")),
                                  "myDatumKeySelectorClass")));
  }

  @Test
  public void testSerDeConfigReading() throws Exception {
    assertNotNull(config.<MyDatum>serDe("json"));
  }
}