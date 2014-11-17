package com.outbrain.aletheia.datum.consumption;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A ConsumptionEndPoint which holds incoming data in-memory, and can be used a synchronized produced-consumer flow.
 * A ManualFeedConsumptionEndPoint will block the producing side once it's over the limits, and similarly will block
 * consumption when there is nothing to consume.
 */
public class ManualFeedConsumptionEndPoint extends ConsumptionEndPoint {

  private static final String MANUAL_FEED = "ManualFeed";
  private static final int DEFAULT_SIZE = 1;

  private final BlockingQueue<byte[]> queue;
  private final String endPointAlias;

  public ManualFeedConsumptionEndPoint() {
    this(MANUAL_FEED, DEFAULT_SIZE);
  }

  public ManualFeedConsumptionEndPoint(final String endPointAlias) {
    this(endPointAlias, 1);
  }

  public ManualFeedConsumptionEndPoint(final String endPointAlias, final int size) {
    this(endPointAlias, new ArrayList<byte[]>(size));
  }

  public ManualFeedConsumptionEndPoint(List<byte[]> data) {
    this(MANUAL_FEED, data);
  }

  public ManualFeedConsumptionEndPoint(final String endPointAlias, List<byte[]> data) {

    this.endPointAlias = endPointAlias;

    if(data.size() == 0) {
      queue = new ArrayBlockingQueue<>(DEFAULT_SIZE);
    }
    else {
      queue = new ArrayBlockingQueue<>(data.size());
      deliverAll(data);
    }

  }

  private void deliverAll(final List<byte[]> data) {
    for (final byte[] bytes : data) {
      try {
        deliver(bytes);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void deliver(final byte[] bytes) throws InterruptedException {
    queue.put(bytes);
  }

  public byte[] fetch() throws InterruptedException {
    return queue.take();
  }

  @Override
  public String getName() {
    return endPointAlias;
  }
}
