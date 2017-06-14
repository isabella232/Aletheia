package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.EndPoint;

/**
 * Metadata of an endpoint returned in {@link com.outbrain.aletheia.datum.production.DeliveryCallback} upon a completed datum deliver request.
 */
public class EndpointDeliveryMetadata {
  private final EndPoint endpoint;

  public EndpointDeliveryMetadata(final EndPoint endpoint) {
    this.endpoint = endpoint;
  }

  public EndPoint getEndpoint() {
    return endpoint;
  }
}
