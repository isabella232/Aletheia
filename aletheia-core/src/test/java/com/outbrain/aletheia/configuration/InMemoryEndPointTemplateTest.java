package com.outbrain.aletheia.configuration;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.datum.InMemoryEndPoint;
import com.outbrain.aletheia.datum.InMemoryEndPoints;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Created by slevin on 6/30/15.
 */
public class InMemoryEndPointTemplateTest {

  final String endPointsConfig = "{\n" +
                                 "  \"my_endpoint_1\": {\n" +
                                 "    \"type\": \"inMemory\",\n" +
                                 "    \"size\": 100\n" +
                                 "  }\n" +
                                 "}";

  private final AletheiaConfig config;

  public InMemoryEndPointTemplateTest() throws Exception {
    final String EMPTY_CONFIG = "{}";
    final Properties properties = new Properties();
    properties.setProperty(AletheiaConfig.ENDPOINTS, endPointsConfig);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.ROUTING, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.SERDES, EMPTY_CONFIG);

    config = new AletheiaConfig(properties);
  }

  @Test
  public void testHiveProductionEndpoint() throws Exception {
    final InMemoryEndPoint my_endpoint_1 = ((InMemoryEndPoint) config.getProductionEndPoint("my_endpoint_1"));
    assertNotNull(my_endpoint_1);
    assertThat(my_endpoint_1, is(InMemoryEndPoints.get("my_endpoint_1")));
  }

  @Test
  public void testHiveConsumptionEndpoint() throws Exception {
    final InMemoryEndPoint my_endpoint_1 = ((InMemoryEndPoint) config.getConsumptionEndPoint("my_endpoint_1"));
    assertNotNull(my_endpoint_1);
    assertThat(my_endpoint_1, is(InMemoryEndPoints.get("my_endpoint_1")));
  }

}
