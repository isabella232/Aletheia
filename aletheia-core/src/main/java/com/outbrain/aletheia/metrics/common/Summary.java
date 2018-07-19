package com.outbrain.aletheia.metrics.common;

import com.outbrain.swinfra.metrics.timing.Timer;

public interface Summary {

  Timer startTimer(final String... labelValues);

}
