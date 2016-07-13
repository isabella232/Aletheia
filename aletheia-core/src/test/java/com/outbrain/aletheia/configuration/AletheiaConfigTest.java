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

  private static final String routingConfig = "{\n" +
                               "  \"test.datum\": {\n" +
                               "    \"routes\": [\n" +
                               "      {\n" +
                               "        \"endpoint\": \"my_endpoint_1\",\n" +
                               "        \"serDe\": \"json\"\n" +
                               "      }\n" +
                               "    ],\n" +
                               "    \"routeGroups\": [\n" +
                               "      \"my_routeGroup\"\n" +
                               "    ],\n" +
                               "    \"datum.key.selector.class\": \"myDatumKeySelectorClass\",\n" +
                               "    \"breadcrumbs\": \"myBreadcrumbsEndpointId\"\n" +
                               "  }\n" +
                               "}";

  private static final String endPointGroupsConfig = "{\n" +
                                      "  \"my_routeGroup\": [\n" +
                                      "    {\n" +
                                      "      \"endpoint\": \"my_endpoint_2\",\n" +
                                      "      \"serDe\": \"json\"\n" +
                                      "    }\n" +
                                      "  ]\n" +
                                      "}";

  private static final String serDeConfig = "{\n" +
                             "  \"json\": {\n" +
                             "    \"@class\": \"com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe\",\n" +
                             "    \"datum.class\": \"${datum.class}\"\n" +
                             "  }\n" +
                             "}";

  private static final String EMPTY_CONFIG = "{}";

  private final AletheiaConfig config;

  private static Properties getProperties() {
    final Properties properties = new Properties();
    properties.setProperty("datum.class", "com.outbrain.aletheia.configuration.AletheiaConfigTest$MyDatum");
    properties.setProperty(AletheiaConfig.ENDPOINTS, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, endPointGroupsConfig);
    properties.setProperty(AletheiaConfig.ROUTING, routingConfig);
    properties.setProperty(AletheiaConfig.SERDES, serDeConfig);
    return properties;
  }

  public AletheiaConfigTest() throws Exception {
    config = new AletheiaConfig(getProperties());
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

  private static Properties getMultipleConfigProperties() {
    final Properties properties = getProperties();
    properties.remove(AletheiaConfig.ROUTING);
    properties.remove(AletheiaConfig.ENDPOINT_GROUPS);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS_CONFIG_PATH, "com/outbrain/aletheia/configuration/endpoint.groups.json");
    properties.setProperty(AletheiaConfig.MULTIPLE_CONFIGURATIONS_PATH,
            "com/outbrain/aletheia/configuration");
    return properties;
  }

  @Test
  public void testMultipleConfig() throws Exception {
    final Properties properties = getMultipleConfigProperties();
    properties.setProperty(AletheiaConfig.ROUTING_EXTENSION, "routing.json");
    AletheiaConfig multiConfig = new AletheiaConfig(properties);

    assertThat(multiConfig.getRouting("test.datum"),
            is(new RoutingInfo(Lists.newArrayList(new Route("test_endpoint_1", "avro"),
                    new Route("test_endpoint_2", "json")),
                    "myDatumKeySelectorClass")));
    assertThat(multiConfig.getRouting("sample_domain_class"),
            is(new RoutingInfo(Lists.newArrayList(new Route("test_endpoint_1", "avro"),
                    new Route("test_endpoint_2", "json")),
                    "com.outbrain.aletheia.SampleDomainClass$SampleDatumKeySelector")));
  }

  @Test(expected = RuntimeException.class)
  public void testMultipleConfigConflict() throws Exception {
    final Properties properties = getMultipleConfigProperties();
    properties.setProperty(AletheiaConfig.ROUTING_EXTENSION, "conflict.json");
    properties.setProperty(AletheiaConfig.ROUTING_CONFIG_PATH, "com/outbrain/aletheia/configuration/routing.json");

    AletheiaConfig multiConfig = new AletheiaConfig(properties);

    multiConfig.getRouting("sample_domain_class");
  }
}