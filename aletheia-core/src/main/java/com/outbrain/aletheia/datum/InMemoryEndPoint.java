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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * A special kind of {@link com.outbrain.aletheia.datum.EndPoint} that allows clients to a pipeline where
 * the {@link com.outbrain.aletheia.datum.production.DatumProducer} produces data in-memory end point and the
 * {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream} consumes data off of it.
 * This type of endpoint is useful for in-memory experiments and tests, and can simulate a
 * quasi-synchronous, blocking produce/consume model if it's created with size = 1.
 */
public abstract class InMemoryEndPoint<T, U>
        implements ProductionEndPoint,
        FetchConsumptionEndPoint<U>,
        DatumKeyAwareNamedSender<T>,
        NamedSender<T>,
        DatumKeyAwareFetchEndPoint<U>,
        Serializable {

  public static class WithBinaryStorage extends InMemoryEndPoint<ByteBuffer, byte[]> {

    public WithBinaryStorage(final String endPointName, final int size) {
      super(endPointName, size);
    }

    public WithBinaryStorage(final String endPointName, final List<byte[]> data) throws SilentSenderException {
      this(endPointName, data.size());
      for (byte[] bytes : data) {
        this.send(ByteBuffer.wrap(bytes));
      }
    }

    @Override
    protected byte[] transform(final ByteBuffer data) {
      return data.array();
    }
  }

  public static class WithStringStorage extends InMemoryEndPoint<String, String> {

    public WithStringStorage(final String endPointName,int size) {
      super(endPointName, size );
    }

    @Override
    protected String transform(final String data) {
      return data;
    }

  }

  public static final String DEFAULT_DATUM_KEY = "random";
  private static final long EMPTY_QUEUES_WAIT_MILLI = 10;

  private final ConcurrentMap<String, ArrayBlockingQueue<U>> producedData = Maps.newConcurrentMap();
  private final String endPointName;
  private final int size;

  protected InMemoryEndPoint(final String endPointName, int size) {
    this.endPointName = endPointName;
    this.size = size;
  }

  protected abstract U transform(final T data);

  @Override
  public U fetch() {
    try {

      Optional<ArrayBlockingQueue<U>> firstNonEmptyQueue;

      final Predicate<ArrayBlockingQueue<U>> nonEmpty = new Predicate<ArrayBlockingQueue<U>>() {
        @Override
        public boolean apply(final ArrayBlockingQueue<U> queue) {
          return queue.size() > 0;
        }
      };

      do {
        firstNonEmptyQueue = FluentIterable.from(producedData.values())
                                           .filter(nonEmpty)
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

    producedData.putIfAbsent(validKey, new ArrayBlockingQueue<U>(size));

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
    return endPointName;
  }

  public Map<String, ? extends Iterable<U>> getData() {
    return producedData;
  }
}
