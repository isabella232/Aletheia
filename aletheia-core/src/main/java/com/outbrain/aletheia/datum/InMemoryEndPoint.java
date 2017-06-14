package com.outbrain.aletheia.datum;

import com.google.common.collect.Maps;
import com.outbrain.aletheia.datum.consumption.DatumKeyAwareFetchEndPoint;
import com.outbrain.aletheia.datum.consumption.FetchConsumptionEndPoint;
import com.outbrain.aletheia.datum.production.DeliveryCallback;
import com.outbrain.aletheia.datum.production.DatumKeyAwareNamedSender;
import com.outbrain.aletheia.datum.production.EmptyCallback;
import com.outbrain.aletheia.datum.production.NamedSender;
import com.outbrain.aletheia.datum.production.ProductionEndPoint;
import com.outbrain.aletheia.datum.production.SilentSenderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A special kind of {@link com.outbrain.aletheia.datum.EndPoint} that allows clients to construct a pipeline where
 * the {@link com.outbrain.aletheia.datum.production.DatumProducer} produces into an in-memory end point and the
 * {@link com.outbrain.aletheia.datum.consumption.DatumConsumerStream} consumes data off of it.
 * This type of endpoint is useful for in-memory experiments and tests, and can simulate a
 * quasi-synchronous, blocking produce/consume model if it's created with size = 1.
 *
 * Deliver with callback api is not suppoted in this implementation, callbacks will be ignored.
 */
public class InMemoryEndPoint
        implements ProductionEndPoint,
        FetchConsumptionEndPoint<byte[]>,
        DatumKeyAwareNamedSender<byte[]>,
        NamedSender<byte[]>,
        DatumKeyAwareFetchEndPoint<byte[]>,
        Serializable {

  private static final Logger logger = LoggerFactory.getLogger(InMemoryEndPoint.class);

  private final static class KeyedData implements Serializable {
    private byte[] data;
    private String key;

    public KeyedData(final String key, final byte[] data) {
      this.data = data;
      this.key = key;
    }

    public byte[] getData() {
      return data;
    }

    public String getKey() {
      return key;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final KeyedData keyedData = (KeyedData) o;

      return Arrays.equals(data, keyedData.data) && key.equals(keyedData.key);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(data);
      result = 31 * result + key.hashCode();
      return result;
    }
  }

  public static final String DEFAULT_DATUM_KEY = "r@|\\|d0|\\/|";
  //well, "r@|\|d0|\/|" is prettier, but there's no escape, pun intended.

  private final ConcurrentMap<String, BlockingQueue<byte[]>> partitionedProducedData = Maps.newConcurrentMap();
  private final BlockingQueue<KeyedData> producedData = new LinkedBlockingQueue<>();
  private final String endPointName;
  private final int size;

  public InMemoryEndPoint(final String endPointName, final int size) {
    this.endPointName = endPointName;
    this.size = size;

    logger.warn("*** Please note deliver with callback API is not supported for in memory endpoints ***");
  }

  public InMemoryEndPoint(final String endPointName, final List<byte[]> data) {
    this(endPointName, data.size());

    for (final byte[] bytes : data) {
      try {
        send(bytes);
      } catch (final SilentSenderException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public byte[] fetch() {
    try {
      final KeyedData keyedData = producedData.take();
      partitionedProducedData.get(keyedData.getKey()).remove(keyedData.getData());
      return keyedData.getData();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] fetch(final String key) {
    try {
      final byte[] bytes = partitionedProducedData.get(key).take();
      producedData.remove(new KeyedData(key, bytes));
      return bytes;
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void send(final byte[] data, final String key) throws SilentSenderException {
    send(data, key, EmptyCallback.getEmptyCallback());
  }

  @Override
  public void send(byte[] data, String key, DeliveryCallback deliveryCallback) throws SilentSenderException {

    final String validKey = key != null ? key : DEFAULT_DATUM_KEY;

    partitionedProducedData.putIfAbsent(validKey, new LinkedBlockingDeque<byte[]>(size));

    try {
      partitionedProducedData.get(validKey).put(data);
      producedData.add(new KeyedData(validKey, data));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void send(final byte[] data) throws SilentSenderException {
    send(data, DEFAULT_DATUM_KEY);
  }

  @Override
  public void send(byte[] data, DeliveryCallback deliveryCallback) throws SilentSenderException {
    send(data, DEFAULT_DATUM_KEY, deliveryCallback);
  }

  @Override
  public String getName() {
    return endPointName;
  }

  public Map<String, ? extends Iterable<byte[]>> getData() {
    return partitionedProducedData;
  }
}
