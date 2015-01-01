package com.outbrain.aletheia.datum;

/**
 * This {@link com.outbrain.aletheia.datum.production.ProductionEndPoint} type works in collaboration with the {@link com.outbrain.aletheia.datum.production.InMemoryAccumulatingNamedSender},
 * jointly, they allow one to store incoming items in-memory, and query them later on.
 * This type of endpoint is useful for experiments and tests.
 */
public class InMemoryStringEndPoint extends InMemoryEndPoint<String, String>  {

  public InMemoryStringEndPoint() {
  }

  public InMemoryStringEndPoint(final int queueSize) {
    super(queueSize);
  }

  @Override
  protected String convert(final String data) {
    return data;
  }

}
