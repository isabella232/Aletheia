package com.outbrain.aletheia.datum.production;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An {@link Sender} that stores incoming items in-memory, allowing one to query them later.
 */
public class InMemoryAccumulatingNamedSender<T> implements DatumKeyAwareNamedSender<T>, NamedSender<T>, Serializable {

  public static String DEFAULT_DATUM_KEY = "random";
  private final Map<String, List<T>> sentData = Maps.newHashMap();
  private final int maxSize;

  public InMemoryAccumulatingNamedSender() {
    this(50 * 1000);
  }

  public InMemoryAccumulatingNamedSender(final int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public void send(final T data) throws SilentSenderException {
    send(data, DEFAULT_DATUM_KEY);
  }

  @Override
  public void send(final T data, final String key) throws SilentSenderException {

    final String nonNullKey = key != null ? key : DEFAULT_DATUM_KEY;

    synchronized (sentData) {

      if (sentData.size() > maxSize) {
        sentData.clear();
      }

      if (!sentData.containsKey(nonNullKey)) {
        sentData.put(nonNullKey, Collections.synchronizedList(Lists.<T>newArrayList()));
      }

      sentData.get(nonNullKey).add(data);
    }
  }

  public Map<String, List<T>> getSentData() {
    return sentData;
  }

  @Override
  public String getName() {
    return "InMemory";
  }
}
