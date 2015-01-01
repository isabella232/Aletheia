package com.outbrain.aletheia.datum;

import java.nio.ByteBuffer;

/**
 * This {@link com.outbrain.aletheia.datum.production.ProductionEndPoint} type works in collaboration with the {@link com.outbrain.aletheia.datum.production.InMemoryAccumulatingNamedSender},
 * jointly, they allow one to store incoming items in-memory, and query them later on.
 * This type of endpoint is useful for experiments and tests.
 */
public class InMemoryBinaryEndPoint extends InMemoryEndPoint<ByteBuffer,byte[]> {

  public InMemoryBinaryEndPoint() {

  }

  public InMemoryBinaryEndPoint(final int queueSize) {
    super(queueSize);
  }

  @Override
  protected byte[] convert(final ByteBuffer data) {
    return data.array();
  }
}
