package com.outbrain.aletheia.datum.production;

import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryAccumulatingSender<T> implements NamedSender<T>, Serializable {

  private final List sentData = Collections.synchronizedList(new ArrayList());
  private final int maxSize;

  public InMemoryAccumulatingSender() {
    this(50 * 1000);
  }

  public InMemoryAccumulatingSender(final int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public void send(final T item) {

    if (sentData.size() > maxSize) {
      sentData.clear();
    }

    // type safety, it was nice knowing you, don't forget to write.
    if (item instanceof ByteBuffer) {
      sentData.add(((ByteBuffer) item).array());
    } else if(item instanceof DatumEnvelope) {
      sentData.add(((DatumEnvelope) item).getDatumBytes().array());
    } else {
      sentData.add(item);
    }

  }

  public List getSentData() {
    return sentData;
  }

  @Override
  public String getName() {
    return "InMemory";
  }
}
