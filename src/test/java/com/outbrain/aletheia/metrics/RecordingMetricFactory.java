package com.outbrain.aletheia.metrics;

import com.google.common.base.Strings;
import com.outbrain.aletheia.metrics.common.*;
import org.apache.velocity.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by slevin on 8/13/14.
 */
public class RecordingMetricFactory implements MetricsFactory {

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

    protected void addMetric(final String[] splitMetric, final int index) {
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

  private final List<String> createdMetrics = new ArrayList<>();

  public List<String> getCreatedMetrics() {
    return createdMetrics;
  }

  public MetricsTree getMetricTree() {

    final List<String> createdMetrics = getCreatedMetrics();

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
  public Timer createTimer(final String component, final String methodName) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createTimer(component, methodName);
  }

  @Override
  public Counter createCounter(final String component, final String methodName) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createCounter(component, methodName);
  }

  @Override
  public <T> Gauge<T> createGauge(final String component, final String methodName, final Gauge<T> metric) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createGauge(component, methodName, metric);
  }

  @Override
  public Meter createMeter(final String component, final String methodName, final String eventType) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createMeter(component, methodName, eventType);
  }

  @Override
  public Histogram createHistogram(final String component, final String methodName, final boolean biased) {
    createdMetrics.add(component + "." + methodName);
    return decoratedMetricFactory.createHistogram(component, methodName, biased);
  }

  @Override
  public void createStatefulGauge(final String component,
                                  final String methodName,
                                  final GaugeStateHolder monitoredObject) {
    createdMetrics.add(component + "." + methodName);
    decoratedMetricFactory.createStatefulGauge(component, methodName, monitoredObject);
  }

}
