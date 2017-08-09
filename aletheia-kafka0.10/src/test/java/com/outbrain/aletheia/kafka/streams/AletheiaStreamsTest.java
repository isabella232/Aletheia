package com.outbrain.aletheia.kafka.streams;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.datum.DatumKeySelector;
import com.outbrain.aletheia.datum.DatumType;
import com.outbrain.aletheia.kafka.serialization.AletheiaSerdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Properties;

import static com.outbrain.aletheia.kafka.serialization.AletheiaSerdes.ALETHEIA_PRODUCER_INCARNATION;
import static com.outbrain.aletheia.kafka.serialization.AletheiaSerdes.ALETHEIA_PRODUCER_SOURCE;
import static org.junit.Assert.assertEquals;

public class AletheiaStreamsTest {

  @DatumType(datumTypeId = "my.datum", timestampExtractor = MyDatum.TimestampExtractor.class)
  private static class MyDatum {

    private long timeCreated;
    private String data;

    public MyDatum() {
    }

    public MyDatum(final String data) {
      this.timeCreated = DateTime.now().getMillis();
      this.data = data;
    }

    public long getTimeCreated() {
      return timeCreated;
    }

    public String getData() {
      return data;
    }

    public void setTimeCreated(long timeCreated) {
      this.timeCreated = timeCreated;
    }

    public void setData(String data) {
      this.data = data;
    }

    public static final class TimestampExtractor implements DatumType.TimestampSelector {
      @Override
      public DateTime extractDatumDateTime(Object domainObject) {
        return new DateTime(((MyDatum) domainObject).getTimeCreated());
      }
    }

    public static final class MyDatumKeySelector implements DatumKeySelector<MyDatum> {
      @Override
      public String getDatumKey(MyDatum domainObject) {
        return domainObject.getData();
      }
    }
  }

  private static final String routingConfig = "{\n" +
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
      "    \"datum.key.selector.class\": \"com.outbrain.aletheia.kafka.streams.AletheiaStreamsTest$MyDatum$MyDatumKeySelector\",\n" +
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

  private static final String endPointGroupsConfig = "{\n" +
      "  \"my_routeGroup\": [\n" +
      "    {\n" +
      "      \"endpoint\": \"my_endpoint_2\",\n" +
      "      \"serDe\": \"json\"\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  private static final String endPointsConfig = "{\n" +
      " \"my_endpoint_1\": {\n" +
      " \"type\": \"kafka\",\n" +
      " \"topic.name\": \"my.datum\",\n" +
      " \"consume\": {\n" +
      "   \"bootstrap.servers\": \"cluster1:9092\",\n" +
      "   \"group.id\": \"streams\",\n" +
      "   \"concurrency.level\": \"1\"\n" +
      "  }\n" +
      " },\n" +
      " \"my_endpoint_2\": {\n" +
      " \"type\": \"kafka\",\n" +
      " \"topic.name\": \"also.my.datum\",\n" +
      " \"consume\": {\n" +
      "   \"bootstrap.servers\": \"cluster2:9092\",\n" +
      "   \"group.id\": \"streams\",\n" +
      "   \"concurrency.level\": \"1\"\n" +
      "  }\n" +
      " }\n" +
      "}";

  private static final String serDeConfig = "{\n" +
      "  \"json\": {\n" +
      "    \"@class\": \"com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe\",\n" +
      "    \"datum.class\": \"${datum.class}\"\n" +
      "  }\n" +
      "}";

  private static final String EMPTY_CONFIG = "{}";

  private static final HashMap<String, Object> emptyAppConfig = new HashMap<>();

  private AletheiaConfig aletheiaConfig;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("datum.class", MyDatum.class.getName());
    properties.setProperty(AletheiaConfig.ENDPOINTS, endPointsConfig);
    properties.setProperty(AletheiaConfig.ENDPOINT_GROUPS, endPointGroupsConfig);
    properties.setProperty(AletheiaConfig.ROUTING, routingConfig);
    properties.setProperty(AletheiaConfig.SERDES, serDeConfig);
    properties.setProperty(ALETHEIA_PRODUCER_INCARNATION, "1");
    properties.setProperty(ALETHEIA_PRODUCER_SOURCE, "UnitTest");
    AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);
    aletheiaConfig = new AletheiaConfig(properties);
  }

  @Test
  public void testSerDe() throws Exception {
    final Serde<MyDatum> myDatumSerde = AletheiaSerdes.serdeFrom(
        MyDatum.class, "json",
        aletheiaConfig);

    final MyDatum expectedDatum = new MyDatum("mydatumtestdata");
    final byte[] serializedDatum = myDatumSerde.serializer().serialize("", expectedDatum);
    final MyDatum actualDatum = myDatumSerde.deserializer().deserialize("", serializedDatum);

    assertEquals(expectedDatum.getTimeCreated(), actualDatum.getTimeCreated());
    assertEquals(expectedDatum.getData(), actualDatum.getData());
  }

  @Test
  public void whenNoStreamCreatedShouldThrow() throws Exception {
    expectedException.expect(IllegalStateException.class);

    final AletheiaStreams aletheiaStreams = new AletheiaStreams(emptyAppConfig);
    aletheiaStreams.constructStreamsInstance();
  }

  @Test
  public void whenEndpointsOnDifferentClustersShouldThrow() throws Exception {
    expectedException.expect(IllegalStateException.class);

    final AletheiaStreams aletheiaStreams = new AletheiaStreams(emptyAppConfig);
    aletheiaStreams.stream(aletheiaConfig, MyDatum.class, "my_endpoint_1", "json");
    aletheiaStreams.stream(aletheiaConfig, MyDatum.class, "my_endpoint_2", "json");
    aletheiaStreams.constructStreamsInstance();
  }

  @Test
  public void whenBootstrapServersConfiguredShouldTakePrecedence() throws Exception {
    final String expectedBootstrapServers = "localhost:9092";
    final HashMap<String, Object> appConfig = new HashMap<>();
    appConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    appConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, expectedBootstrapServers);

    final AletheiaStreams aletheiaStreams = new AletheiaStreams(appConfig);
    aletheiaStreams.stream(aletheiaConfig, MyDatum.class, "my_endpoint_1", "json");
    aletheiaStreams.stream(aletheiaConfig, MyDatum.class, "my_endpoint_2", "json");
    aletheiaStreams.constructStreamsInstance();

    assertEquals(expectedBootstrapServers,
        aletheiaStreams.getEffectiveConfig().get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
  }
}
