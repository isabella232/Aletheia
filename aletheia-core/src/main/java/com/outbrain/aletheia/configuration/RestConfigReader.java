package com.outbrain.aletheia.configuration;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * REST configuration, loads the data from http endpoint. <br>
 * Reader applies retry policy in case of errors, see {@link RetryPolicy}
 * <p>
 * The reader activated when schema in URI is  <b>http</b>.
 */
public class RestConfigReader implements ConfigReader {

  private static final Logger logger = LoggerFactory.getLogger(RestConfigReader.class);

  private final int connectionTimeout;
  private final int socketTimeout;
  private final RetryPolicy retryPolicy;
  private final HttpClient httpClient;

  public RestConfigReader(final RetryPolicy retryPolicy, final HttpClient httpClient, final int connectionTimeout, final int socketTimeout) {
    this.retryPolicy = retryPolicy;
    this.httpClient = httpClient;
    this.connectionTimeout = connectionTimeout;
    this.socketTimeout = socketTimeout;
  }

  @Override
  public InputStream read(final URI configUri) throws IOException {

    int numOfTries = 0;

    InputStream result = null;
    while (numOfTries <= retryPolicy.getMaxRetries()) {
      try {
        numOfTries++;
        result = getConfigurationStream(configUri);
        break;
      } catch (final IOException e) {
        if (numOfTries == retryPolicy.getMaxRetries()) {
          logger.error("Failed to load configurations from {}, number of attempts exceeded the maximum ({}).", configUri, retryPolicy.getMaxRetries(), e);
          throw e;
        } else {
          logger.warn("Failed to load configuration from {}, this is attempt {} of {}. The error is - {}.", configUri, numOfTries, retryPolicy.getMaxRetries(), e);
          retryPolicy.delay();
        }
      }
    }
    return result;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  InputStream getConfigurationStream(final URI configUri) throws IOException {

    final HttpGet httpget = new HttpGet(configUri);
    final RequestConfig.Builder requestConfig = RequestConfig.custom();
    requestConfig.setConnectTimeout(getConnectionTimeout());
    requestConfig.setSocketTimeout(getSocketTimeout());

    final HttpResponse response = httpClient.execute(httpget);
    final HttpEntity entity = response.getEntity();

    if (entity == null) {
      throw new IOException(String.format("The result from %s is null", configUri));
    }

    return entity.getContent();
  }

}


