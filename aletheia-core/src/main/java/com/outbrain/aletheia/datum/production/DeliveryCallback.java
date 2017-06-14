package com.outbrain.aletheia.datum.production;

/**
 * The callback interface that can be used to invoke user code when deliver request of {@link com.outbrain.aletheia.datum.production.DatumProducer} is complete.
 */
public interface DeliveryCallback {
  /**
   * A callback method that will be invoked upon successful deliver request completion.
   *
   * @param endpointDeliveryMetadata The metadata of the endpoint the datum was successfully delivered to.
   *                                 This parameter can be used differ invocations of the callback for the same datum and different endpoints.
   */
  void onSuccess(final EndpointDeliveryMetadata endpointDeliveryMetadata);

  /**
   * A callback method that will be invoked upon failure to complete deliver request.
   *
   * @param endpointDeliveryMetadata The metadata of the endpoint where the datum delivery failed.
   *                                 This parameter can be used differ invocations of the callback for the same datum and different endpoints.
   * @param exception The exception thrown during datum deliver request.
   */
  void onError(final EndpointDeliveryMetadata endpointDeliveryMetadata, final Exception exception);
}
