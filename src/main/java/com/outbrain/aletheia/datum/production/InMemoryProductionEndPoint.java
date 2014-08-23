package com.outbrain.aletheia.datum.production;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryProductionEndPoint extends ProductionEndPoint  {

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
    return "InMemory";
  }
}
