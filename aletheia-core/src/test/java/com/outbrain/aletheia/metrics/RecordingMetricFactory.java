package com.outbrain.aletheia.metrics;

import com.google.common.base.Strings;
import com.outbrain.aletheia.metrics.common.Counter;
import com.outbrain.aletheia.metrics.common.Gauge;
import com.outbrain.aletheia.metrics.common.Histogram;
import com.outbrain.aletheia.metrics.common.Meter;
import com.outbrain.aletheia.metrics.common.MetricsFactory;
import com.outbrain.aletheia.metrics.common.Summary;
import com.outbrain.aletheia.metrics.common.Timer;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by slevin on 8/13/14.
 */
public class RecordingMetricFactory implements MetricsFactory {

  @Override
  public Summary createSummary(String name, String help, String... labelNames) {
    return null;
  }

  public static class MetricsTree {

    private final String nodeName;
    private final HashSet<MetricsTree> children = new HashSet<>();

    private MetricsTree(final String nodeName) {
      this.nodeName = nodeName;
    }

    protected void prettyPrint(final int numberOfTabsForPrefix) {
      System.out.println(Strings.repeat("\t", numberOfTabsForPrefix) + this.nodeName);
      for (final MetricsTree child : children) {
        child.prettyPrint(numberOfTabsForPrefix + 1);
      }
    }

    protected void addMetric(final String[] splitMetric, final int index, String... labelNames) {
      if (splitMetric.length == index) {
        return;
      }
      boolean found = false;
      for (final MetricsTree child : children) {
        if (child.nodeName.equals(splitMetric[index])) {
          found = true;
          child.addMetric(splitMetric, index + 1);
        }
      }
      if (!found) {
        final MetricsTree subTree = new MetricsTree(splitMetric[index]);
        subTree.addMetric(splitMetric, index + 1);
        children.add(subTree);
      }
    }

    public void addMetric(final String metric) {
      final String[] split = StringUtils.split(metric, ".");
      addMetric(split, 0);
    }

    public void prettyPrint() {
      prettyPrint(0);
    }
  }

  private final MetricsFactory decoratedMetricFactory;

  private final Collection<String> createdMetrics = new ConcurrentLinkedQueue<>();

  public Collection<String> getCreatedMetrics() {
    return createdMetrics;
  }

  public MetricsTree getMetricTree() {

    final Collection<String> createdMetrics = getCreatedMetrics();

    final MetricsTree metricsTree = new MetricsTree("root");

    for (final String createdMetric : createdMetrics) {
      System.out.println(createdMetric);
      metricsTree.addMetric(createdMetric);
    }

    return metricsTree;
  }

  public RecordingMetricFactory(final MetricsFactory decoratedMetricFactory) {
    this.decoratedMetricFactory = decoratedMetricFactory;
  }

  @Override
  public Timer createTimer(final String component, final String methodName, String... labelNames) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createTimer(component, methodName);
  }

  @Override
  public Counter createCounter(final String component, final String methodName, String... labelNames) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createCounter(component, methodName);
  }

  @Override
  public <T> Gauge<T> createGauge(final String component, final String methodName, final Gauge<T> metric, String... labelNames) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createGauge(component, methodName, metric);
  }

  @Override
  public Meter createMeter(final String component, final String methodName, final String eventType, String... labelNames) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createMeter(component, methodName, eventType);
  }

  @Override
  public Histogram createHistogram(final String component, final String methodName, final boolean biased, String... labelNames) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createHistogram(component, methodName, biased);
  }

}
