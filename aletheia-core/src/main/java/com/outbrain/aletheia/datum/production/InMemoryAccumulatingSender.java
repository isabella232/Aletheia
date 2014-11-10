package com.outbrain.aletheia.datum.production;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An {@code Sender} that stores incoming items in-memory, allowing one to query them later.
 */
public class InMemoryAccumulatingSender implements NamedKeyAwareSender, NamedSender, Serializable {

  public static String DEFAULT_DATUM_KEY = "random";
  private final Map<String, List<Object>> sentData = Maps.newHashMap();
  private final int maxSize;

  public InMemoryAccumulatingSender() {
    this(50 * 1000);
  }

  public InMemoryAccumulatingSender(final int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public void send(final Object item) throws SilentSenderException {
    send(item, DEFAULT_DATUM_KEY);
  }

  @Override
  public void send(final Object item, final String key) throws SilentSenderException {

    final String nonNullKey = key != null ? key : DEFAULT_DATUM_KEY;

    synchronized (sentData) {

      if (sentData.size() > maxSize) {
        sentData.clear();
      }

      if (!sentData.containsKey(nonNullKey)) {
        sentData.put(nonNullKey, Collections.synchronizedList(Lists.newArrayList()));
      }

      // type safety, it was nice knowing you, don't forget to write.
      if (item instanceof ByteBuffer) {
        sentData.get(nonNullKey).add(((ByteBuffer) item).array());
      } else if (item instanceof DatumEnvelope) {
        sentData.get(nonNullKey).add(((DatumEnvelope) item).getDatumBytes().array());
      } else {
        sentData.get(nonNullKey).add(item);
      }
    }
  }

  public Map<String, List<Object>> getSentData() {
    return sentData;
  }

  @Override
  public String getName() {
    return "InMemory";
  }
}
