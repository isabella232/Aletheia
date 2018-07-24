package com.outbrain.aletheia.metrics.prometheus;

import com.outbrain.aletheia.metrics.common.Histogram;
import org.apache.commons.lang3.ArrayUtils;

public class HistogramFacade implements Histogram {
  private com.outbrain.swinfra.metrics.Histogram histogram;
  private String[] fixedLabelValues;

  public HistogramFacade(final com.outbrain.swinfra.metrics.Histogram histogram, final String[] fixedLabelValues) {
    this.histogram = histogram;
    this.fixedLabelValues = fixedLabelValues;
  }

  @Override
  public void update(final int value, final String... labelValues) {
    histogram.observe(value, (String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));
  }

  @Override
  public void update(final long value, final String... labelValues) {
    histogram.observe(value, (String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));

  }
}
