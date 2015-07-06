package com.outbrain.aletheia.configuration.hive;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.configuration.logFile.LogFileEndPointTemplate;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;


/**
 * Created by slevin on 6/30/15.
 */
public class HiveTableEndPointTemplateTest {

  final String endPointsConfig = "{\n" +
                                 "  \"my_endpoint_1\": {\n" +
                                 "    \"type\": \"hive\",\n" +
                                 "    \"hdfs.namenode\": \"${namenode}\",\n" +
                                 "    \"hive.table.path\": \"myTablePath\",\n" +
                                 "    \"partitions\": [\n" +
                                 "      \"date\",\n" +
                                 "      \"hour\"\n" +
                                 "    ],\n" +
                                 "    \"produce\": {\n" +
                                 "      \"type\": \"logFile\",\n" +
                                 "      \"folder\": \"myFolder\",\n" +
                                 "      \"filename\": \"myFileName\"\n" +
                                 "    }\n" +
                                 "  }\n" +
                                 "}";

  private final AletheiaConfig config;

  public HiveTableEndPointTemplateTest() throws Exception {
    final String EMPTY_CONFIG = "{}";
    final Properties properties = new Properties();

    properties.setProperty("namenode", "myNamenodeValue");
    properties.setProperty(AletheiaConfig.ENDPOINTS, endPointsConfig);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.ROUTING, EMPTY_CONFIG);
    properties.setProperty(AletheiaConfig.SERDES, EMPTY_CONFIG);

    AletheiaConfig.registerEndPointTemplate(HiveTableEndPointTemplate.TYPE,
                                            HiveTableEndPointTemplate.class);
    AletheiaConfig.registerEndPointTemplate(LogFileEndPointTemplate.TYPE,
                                            LogFileEndPointTemplate.class);

    config = new AletheiaConfig(properties);
  }

  @Test
  public void testHiveProductionEndpoint() throws Exception {
    assertNotNull(config.getProductionEndPoint("my_endpoint_1"));
  }

  @Test(expected = NotImplementedException.class)
  public void testHiveConsumptionEndpoint() throws Exception {
    config.getConsumptionEndPoint("my_endpoint_1");
  }

}
