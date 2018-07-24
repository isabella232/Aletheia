package com.outbrain.aletheia.metrics.prometheus;

import com.outbrain.aletheia.metrics.common.Gauge;
import com.outbrain.swinfra.metrics.SettableGauge;
import org.apache.commons.lang3.ArrayUtils;

public class SettableGaugeFacade implements Gauge<Double> {
  final private SettableGauge gauge;
  final private String[] fixedLabelValues;

  public SettableGaugeFacade(final SettableGauge gauge, final String[] fixedLabelValues) {
    this.gauge = gauge;
    this.fixedLabelValues = fixedLabelValues;
  }


  @Override
  public Double getValue(final String... labelValues) {
    return gauge.getValue((String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));
  }

  @Override
  public void set(final Double value, final String... labelValues) {
    gauge.set(value, (String[]) ArrayUtils.addAll(fixedLabelValues, labelValues));
  }
}
