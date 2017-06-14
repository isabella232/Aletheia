package com.outbrain.aletheia.datum.production;

/**
 * Empty implementation of {@link com.outbrain.aletheia.datum.production.DeliveryCallback}
 */
public class EmptyCallback implements DeliveryCallback {

  private static final EmptyCallback instance = new EmptyCallback();

  public static EmptyCallback getEmptyCallback(){
    return instance;
  }

  @Override
  public void onSuccess(final EndpointDeliveryMetadata endpointDeliveryMetadata) {
  }

  @Override
  public void onError(final EndpointDeliveryMetadata endpointDeliveryMetadata, final Exception exception) {
  }
}
