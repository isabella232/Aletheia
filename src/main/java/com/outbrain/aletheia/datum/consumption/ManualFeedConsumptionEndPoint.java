package com.outbrain.aletheia.datum.consumption;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by slevin on 8/15/14.
 */
public class ManualFeedConsumptionEndPoint extends ConsumptionEndPoint {

  private final BlockingQueue<byte[]> queue;

  public ManualFeedConsumptionEndPoint() {
    this(1);
  }

  public ManualFeedConsumptionEndPoint(final int size) {
    queue = new ArrayBlockingQueue<>(size);
  }

  public void deliver(final byte[] bytes) throws InterruptedException {
    queue.put(bytes);
  }

  public byte[] fetch() throws InterruptedException {
    return queue.take();
  }

  @Override
  public String getName() {
    return "ManualFeed";
  }
}
