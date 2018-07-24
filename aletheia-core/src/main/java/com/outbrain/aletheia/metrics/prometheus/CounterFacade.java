package com.outbrain.aletheia.metrics.prometheus;

import com.outbrain.aletheia.metrics.common.Counter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;

public class CounterFacade implements Counter {
  final private com.outbrain.swinfra.metrics.Counter counter;
  final private String[] fixedLabelValues;

  public CounterFacade(final com.outbrain.swinfra.metrics.Counter counter, final String[] fixedLabelValues) {
    this.counter = counter;
    this.fixedLabelValues = fixedLabelValues;
  }


  @Override
  public void inc(final long n, final String... labelValues) {
    counter.inc(n, (String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));
    counter.inc(n, (String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));
  }

  @Override
  public void dec() {
    throw new NotImplementedException("delete");
  }

  @Override
  public void dec(final long n) {
    throw new NotImplementedException("delete");
  }

  @Override
  public long getCount() {
    throw new NotImplementedException("delete");
  }

  @Override
  public void inc(final String... labelValues) {

    counter.inc((String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));

  }
}
