package com.outbrain.aletheia.datum.production.kafka;

import com.outbrain.aletheia.datum.EndPoint;
import com.outbrain.aletheia.datum.production.DeliveryCallback;
import com.outbrain.aletheia.datum.production.EndpointDeliveryMetadata;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A transformer object used to convert Aletheia callbacks to Kafka 0.9 callbacks.
 */
public class KafkaCallbackTransformer<TDomainClass> {

  private Counter kafkaDeliveryFailure;

  public KafkaCallbackTransformer(final MetricsFactory metricFactory) {
    kafkaDeliveryFailure = metricFactory.createCounter("Send.Callback", "Failure");
  }

  /**
   * Converts {@link com.outbrain.aletheia.datum.production.DeliveryCallback} to {@link  org.apache.kafka.clients.producer.Callback}
   *
   * @param deliveryCallback The callback provided by the user.
   * @param endpoint The endpoint for which the delivery callback will be invoked.
   *
   */
  public Callback transform(final DeliveryCallback deliveryCallback,
                            final EndPoint endpoint){
    return new Callback() {
      @Override
      public void onCompletion(final RecordMetadata metadata, final Exception exception) {
        // It's guaranteed in Kafka API that exactly one of the arguments will be null
        if (metadata != null){
          deliveryCallback.onSuccess(
                  new EndpointDeliveryMetadata(endpoint));
        } else {
          kafkaDeliveryFailure.inc();
          deliveryCallback.onError(
                  new EndpointDeliveryMetadata(endpoint),
                  exception);
        }
      }
    };
  }
}
