package com.outbrain.aletheia.kafka.streams;

import com.outbrain.aletheia.AletheiaConfig;
import com.outbrain.aletheia.MyDatum;
import com.outbrain.aletheia.configuration.kafka.KafkaTopicEndPointTemplate;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.kafka.serialization.AletheiaSerdes;
import com.outbrain.aletheia.kafka.serialization.SerDeListener;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

import static com.outbrain.aletheia.MockAletheiaConfig.PRODUCER_SOURCE;
import static com.outbrain.aletheia.MockAletheiaConfig.getTestProperties;
import static org.junit.Assert.assertEquals;

public class AletheiaStreamsTest {

  private static final HashMap<String, Object> emptyAppConfig = new HashMap<>();
  private static final String TEST_TOPIC = "my.test.topic";

  private AletheiaConfig aletheiaConfig;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    AletheiaConfig.registerEndPointTemplate(KafkaTopicEndPointTemplate.TYPE, KafkaTopicEndPointTemplate.class);
    aletheiaConfig = new AletheiaConfig(getTestProperties());
  }

  @Test
  public void testSerDe() throws Exception {
    final String expectedTestData = "mydatumtestdata";

    final SerDeListener<MyDatum> listener = new SerDeListener<MyDatum>() {
      @Override
      public void onSerialize(String topic, MyDatum datum, DatumEnvelope datumEnvelope, long dataSizeBytes) {
        assertListenerArguments(topic, datum, datumEnvelope, dataSizeBytes);
      }

      @Override
      public void onDeserialize(String topic, MyDatum datum, DatumEnvelope datumEnvelope, long dataSizeBytes) {
        assertListenerArguments(topic, datum, datumEnvelope, dataSizeBytes);
      }

      private void assertListenerArguments(String topic, MyDatum datum, DatumEnvelope datumEnvelope, long dataSizeBytes) {
        assertEquals(TEST_TOPIC, topic);
        assertEquals(expectedTestData, datum.getData());
        assertEquals(PRODUCER_SOURCE, datumEnvelope.getSourceHost().toString());
        assertEquals(128, dataSizeBytes);
      }
    };

    final Serde<MyDatum> myDatumSerde = AletheiaSerdes.serdeFrom(
            MyDatum.class, "json",
            aletheiaConfig,
            listener);

    final MyDatum expectedDatum = new MyDatum(expectedTestData);
    final byte[] serializedDatum = myDatumSerde.serializer().serialize(TEST_TOPIC, expectedDatum);
    final MyDatum actualDatum = myDatumSerde.deserializer().deserialize(TEST_TOPIC, serializedDatum);

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
