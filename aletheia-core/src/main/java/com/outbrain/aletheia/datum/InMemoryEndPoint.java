package com.outbrain.aletheia.datum;

import com.google.common.base.Optional;
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
import java.util.concurrent.ConcurrentMap;

/**
 * This {@link com.outbrain.aletheia.datum.production.ProductionEndPoint} type works in collaboration with the {@link com.outbrain.aletheia.datum.production.InMemoryAccumulatingNamedSender},
 * jointly, they allow one to store incoming items in-memory, and query them later on.
 * This type of endpoint is useful for experiments and tests.
 */
public abstract class InMemoryEndPoint<T, U>
        implements ProductionEndPoint,
        FetchConsumptionEndPoint<U>,
        DatumKeyAwareNamedSender<T>,
        NamedSender<T>,
        DatumKeyAwareFetchEndPoint<U> {

  public static class WithBinaryStorage extends InMemoryEndPoint<ByteBuffer, byte[]> {

    public WithBinaryStorage(final int queueSize) {
      super(queueSize);
    }

    @Override
    protected byte[] transform(final ByteBuffer data) {
      return data.array();
    }
  }

  public static class WithStringStorage extends InMemoryEndPoint<String, String> {

    public WithStringStorage(final int queueSize) {
      super(queueSize);
    }

    @Override
    protected String transform(final String data) {
      return data;
    }

  }

  public static String DEFAULT_DATUM_KEY = "random";
  private static final String IN_MEMORY = "InMemory";
  private static final long EMPTY_QUEUES_WAIT_MILLI = 10;

  private final Predicate<ArrayBlockingQueue<U>> nonEmptyQueue = new Predicate<ArrayBlockingQueue<U>>() {
    @Override
    public boolean apply(final ArrayBlockingQueue<U> queue) {
      return queue.size() > 0;
    }
  };

  private ConcurrentMap<String, ArrayBlockingQueue<U>> producedData = Maps.newConcurrentMap();
  private int queueSize;

  protected InMemoryEndPoint(int queueSize) {
    this.queueSize = queueSize;
  }

  protected abstract U transform(final T data);

  @Override
  public U fetch() {
    try {

      Optional<ArrayBlockingQueue<U>> firstNonEmptyQueue;

      do {
        firstNonEmptyQueue = FluentIterable.from(producedData.values())
                                           .filter(nonEmptyQueue)
                                           .first();
        Thread.sleep(EMPTY_QUEUES_WAIT_MILLI);
      } while (!firstNonEmptyQueue.isPresent());

      return firstNonEmptyQueue.get().take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public U fetch(String key) {
    try {
      return producedData.get(key).take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void send(final T data, final String key) throws SilentSenderException {

    final String validKey = key != null ? key : DEFAULT_DATUM_KEY;

    producedData.putIfAbsent(validKey, new ArrayBlockingQueue<U>(queueSize));

    try {
      producedData.get(validKey).put(transform(data));
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

  public Map<String, ? extends Iterable<U>> getData() {
    return producedData;
  }
}
