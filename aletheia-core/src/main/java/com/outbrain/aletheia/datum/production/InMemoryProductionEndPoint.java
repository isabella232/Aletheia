package com.outbrain.aletheia.datum.production;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This <code>ProductionEndPoint</code> type works in collaboration with the <code>InMemoryAccumulatingSender</code>,
 * jointly, they allow one to store incoming items in-memory, and query them later on.
 * This type of endpoint is useful for experiments and tests.
 */
public class InMemoryProductionEndPoint extends ProductionEndPoint {

  private static final String IN_MEMORY = "InMemory";

  public enum EndPointType {RawDatumEnvelope, String}

  private final EndPointType endPointType;
  private List receivedData = Collections.synchronizedList(new ArrayList());

  public InMemoryProductionEndPoint(final EndPointType endPointType) {
    this.endPointType = endPointType;
  }

  public List getReceivedData() {
    return receivedData;
  }

  public EndPointType getEndPointType() {
    return endPointType;
  }

  public void setReceivedData(final List receivedData) {
    this.receivedData = receivedData;
  }

  @Override
  public String getName() {
    return IN_MEMORY;
  }
}
