package com.outbrain.aletheia.datum.production;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This {@code ProductionEndPoint} type works in collaboration with the {@code InMemoryAccumulatingSender},
 * jointly, they allow one to store incoming items in-memory, and query them later on.
 * This type of endpoint is useful for experiments and tests.
 */
public class InMemoryProductionEndPoint extends ProductionEndPoint {

  private static final String IN_MEMORY = "InMemory";

  public enum EndPointType {RawDatumEnvelope, String}

  private final EndPointType endPointType;
  private Map<String, List<Object>> receivedData = Maps.newConcurrentMap();

  public InMemoryProductionEndPoint(final EndPointType endPointType) {
    this.endPointType = endPointType;
  }

  public ConcurrentMap<String, List<String>> getDataAsKey2Strings() {

    ConcurrentMap<String, List<String>> res = new ConcurrentHashMap<>();

    for (Map.Entry<String, List<Object>> e : getReceivedData().entrySet()) {
      res.put(e.getKey(), FluentIterable.from(e.getValue()).transform(new Function<Object, String>() {
        @Override
        public String apply(final Object value) {
          return (String) value;
        }
      }).toList());
    }

    return res;
  }


  public Map<String, List<byte[]>> getDataAsKey2ByteArrays() {

    Map<String, List<byte[]>> res = new ConcurrentHashMap<>();

    for (Map.Entry<String, List<Object>> e : getReceivedData().entrySet()) {
      res.put(e.getKey(), FluentIterable.from(e.getValue()).transform(new Function<Object, byte[]>() {
        @Override
        public byte[] apply(final Object value) {
          return (byte[]) value;
        }
      }).toList());
    }

    return res;
  }

  public Map<String, List<Object>> getReceivedData() {
    return receivedData;
  }

  public EndPointType getEndPointType() {
    return endPointType;
  }

  public void setReceivedData(final Map<String, List<Object>> receivedData) {
    this.receivedData = receivedData;
  }

  @Override
  public String getName() {
    return IN_MEMORY;
  }
}
