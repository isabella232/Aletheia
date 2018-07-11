package com.outbrain.aletheia.configuration;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;

@RunWith(MockitoJUnitRunner.class)
public class RestConfigReaderTest {

    private RestConfigReader restConfigReader;

    @Mock
    private HttpClient httpClientMock;
    @Mock
    private HttpResponse httpResponseMock;
    @Mock
    private HttpEntity httpEntityMock;


    @Rule
    public ExpectedException thrown = ExpectedException.none();


    private static final int MAX_NUM_OF_RETRIES = 2;
    private static final int DELAY_IN_MS = 2;
    private static final RetryPolicy retryPolicy = new RetryPolicy(MAX_NUM_OF_RETRIES, DELAY_IN_MS);

    @Before
    public void setUp() throws IOException {
        restConfigReader = new RestConfigReader(retryPolicy, httpClientMock, 1, 1);
        Mockito.when(httpClientMock.execute(Mockito.any())).thenReturn(httpResponseMock);
        Mockito.when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);


    }

    @Test
    public void readSuccessfully() throws URISyntaxException, IOException {

        String configuration = "something to do";
        URI uri = new URI("http://localHost:8080");

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(configuration.getBytes())) {
            Mockito.when(httpEntityMock.getContent()).thenReturn(byteArrayInputStream);

            //ACT
            InputStream result = restConfigReader.read(uri);
            //ASSERT
            Assert.assertNotNull(result);
            Assert.assertEquals(configuration, IOUtils.toString(result));
        }
    }

    @Test
    public void ReadPassOnSecondTry() throws URISyntaxException, IOException {

        String configuration = "something to eat";
        URI uri = new URI("http://localHost:8080");
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(configuration.getBytes())) {

            Mockito.when(httpEntityMock.getContent())
                    .thenThrow(new ConnectException("Failed to connect"))
                    .thenReturn(byteArrayInputStream);


            //ACT
            RestConfigReader restConfigReaderSpy = Mockito.spy(restConfigReader);
            InputStream result = restConfigReaderSpy.read(uri);

            //ASSERT

            Mockito.verify(restConfigReaderSpy, Mockito.times(MAX_NUM_OF_RETRIES)).getConfigurationStream(uri);
            Assert.assertEquals(configuration, IOUtils.toString(result));
        }
    }

    @Test()
    public void ReadFailAfterAllRetries() throws URISyntaxException, IOException {
        URI uri = new URI("http://localHost:8080/some/impossible/address/to/throw/exception");
        RestConfigReader restConfigReaderSpy = Mockito.spy(new RestConfigReader(retryPolicy, HttpClients.createDefault(), 1, 1));


        thrown.expect(IOException.class);
        //ACT
        restConfigReaderSpy.read(uri);
    }
}
