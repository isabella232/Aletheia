package com.outbrain.aletheia.metrics.prometheus;

import com.outbrain.aletheia.metrics.common.Summary;
import com.outbrain.swinfra.metrics.timing.Timer;

public class SummaryFacade implements Summary {

  private final com.outbrain.swinfra.metrics.Summary summary;

  public SummaryFacade(final com.outbrain.swinfra.metrics.Summary summary) {
    this.summary = summary;
  }

  @Override
  public Timer startTimer(final String... labelValues) {
    return this.summary.startTimer(labelValues);
  }
}
