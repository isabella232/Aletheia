package com.outbrain.aletheia.metrics;

import com.google.common.base.Joiner;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.PrefixedMetricFactory;

import java.util.Arrays;

/**
 * Created by slevin on 8/12/14.
 */
public class MetricFactoryPrefixer {

  public static class PrefixedMetricFactoryBuilder {

    private final MetricsFactory metricsFactory;

    public PrefixedMetricFactoryBuilder(final MetricsFactory metricsFactory) {
      this.metricsFactory = metricsFactory;
    }

    public MetricsFactory with(final String... withPrefixes) {
      final String prefix = Joiner.on(".").join(Arrays.asList(withPrefixes));

      return new PrefixedMetricFactory(prefix, metricsFactory);
    }
  }

  public static PrefixedMetricFactoryBuilder prefix(final MetricsFactory metricsFactory) {
    return new PrefixedMetricFactoryBuilder(metricsFactory);
  }
}
