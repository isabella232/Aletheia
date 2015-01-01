package com.outbrain.aletheia.datum;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.outbrain.aletheia.datum.consumption.DatumKeyAwareFetchEndPoint;
import com.outbrain.aletheia.datum.consumption.FetchConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.DatumKeyAwareNamedSender;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.production.SilentSenderException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This {@link com.outbrain.aletheia.datum.production.ProductionEndPoint} type works in collaboration with the {@link com.outbrain.aletheia.datum.production.InMemoryAccumulatingNamedSender},
 * jointly, they allow one to store incoming items in-memory, and query them later on.
 * This type of endpoint is useful for experiments and tests.
 */
public abstract class InMemoryEndPoint<T, U>
        implements ProductionEndPoint, FetchConsumptionEndPoint<U>, DatumKeyAwareNamedSender<T>, NamedSender<T>, DatumKeyAwareFetchEndPoint<U> {

  public static class Binary extends InMemoryEndPoint<ByteBuffer,byte[]> {

    public Binary() {

    }

    public Binary(final int queueSize) {
      super(queueSize);
    }

    @Override
    protected byte[] convert(final ByteBuffer data) {
      return data.array();
    }
  }

  public static class RawString extends InMemoryEndPoint<String, String>  {

    public RawString() {
    }

    public RawString(final int queueSize) {
      super(queueSize);
    }

    @Override
    protected String convert(final String data) {
      return data;
    }

  }
  public static String DEFAULT_DATUM_KEY = "random";

  private static final int DEFAULT_QUEUE_SIZE = 100;
  private static final String IN_MEMORY = "InMemory";

  private final Predicate<ArrayBlockingQueue<U>> nonEmptyQueue = new Predicate<ArrayBlockingQueue<U>>() {
    @Override
    public boolean apply(final ArrayBlockingQueue<U> queue) {
      return queue.size() > 0;
    }
  };

  private Map<String, ArrayBlockingQueue<U>> sentData = Maps.newConcurrentMap();
  private int queueSize;

  protected InMemoryEndPoint() {
    this(DEFAULT_QUEUE_SIZE);
  }

  protected InMemoryEndPoint(int queueSize) {
    this.queueSize = queueSize;
  }

  protected abstract U convert(final T data);

  @Override
  public U fetch() {
    try {
      return FluentIterable.from(sentData.values())
                           .filter(nonEmptyQueue)
                           .first()
                           .get()
                           .take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public U fetch(String key) {
    try {
      return sentData.get(key).take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void send(final T data, final String key) throws SilentSenderException {

    final String validKey = key != null ? key : DEFAULT_DATUM_KEY;

    if (!sentData.containsKey(validKey)) {
      sentData.put(validKey, new ArrayBlockingQueue<U>(queueSize));
    }

    try {
      sentData.get(validKey).put(convert(data));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void send(final T data) throws SilentSenderException {
    send(data, DEFAULT_DATUM_KEY);
  }

  @Override
  public String getName() {
    return IN_MEMORY;
  }

  public Map<String, ? extends Iterable<U>> getSentData() {
    return sentData;
  }
}
