package com.outbrain.aletheia.metrics.codahale3;


public class Counter implements com.outbrain.aletheia.metrics.common.Counter {
  private final com.codahale.metrics.Counter counter;

  public Counter(final com.codahale.metrics.Counter counter) {
    this.counter = counter;
  }

  @Override
  public void inc() {
    counter.inc();

  }

  @Override
  public void inc(final long n) {
    counter.inc(n);
  }

  @Override
  public void dec() {
    counter.dec();
  }

  @Override
  public void dec(final long n) {
    counter.dec(n);
  }

  @Override
  public long getCount() {
    return counter.getCount();
  }
}