package com.outbrain.aletheia.configuration.logFile;

import com.outbrain.aletheia.AletheiaConfig;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;


/**
 * Created by slevin on 6/30/15.
 */
public class LogFileEndPointTemplateTest {

  final String endPointsConfig = "{\n" +
                                 "  \"my_endpoint_1\": {\n" +
                                 "    \"type\": \"logFile\",\n" +
                                 "    \"folder\": \",myFolder\",\n" +
                                 "    \"filename\": \"${myFile}\"\n" +
                                 "  }\n" +
                                 "}";

  private final AletheiaConfig config;

  public LogFileEndPointTemplateTest() throws Exception {
    final String EMPTY_CONFIG = "{}";
    final Properties properties = new Properties();

    properties.setProperty("myFile", "myFilename");

    properties.setProperty(AletheiaConfig.ENDPOINTS, endPointsConfig);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.ROUTING, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.SERDES, EMPTY_CONFIG);

    AletheiaConfig.registerEndPointTemplate(LogFileEndPointTemplate.TYPE,
                                            LogFileEndPointTemplate.class);

    config = new AletheiaConfig(properties);
  }

  @Test
  public void testLogFileProductionEndpoint() throws Exception {
    assertNotNull(config.getProductionEndPoint("my_endpoint_1"));
  }

  @Test(expected = NotImplementedException.class)
  public void testLogFileConsumptionEndpoint() throws Exception {
    config.getConsumptionEndPoint("my_endpoint_1");
  }

}
