package com.outbrain.aletheia.breadcrumbs;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.Duration;

public class BreadcrumbsConfig {

  private final Duration breadcrumbBucketDuration;
  private final Duration breadcrumbBucketFlushInterval;
  private final String application;
  private final String source;
  private final String tier;
  private final String datacenter;

  public BreadcrumbsConfig(final Duration breadcrumbBucketDuration,
                           final Duration breadcrumbBucketFlushInterval,
                           final String application,
                           final String source,
                           final String tier,
                           final String datacenter) {
    this.breadcrumbBucketDuration = breadcrumbBucketDuration;
    this.breadcrumbBucketFlushInterval = breadcrumbBucketFlushInterval;
    this.application = application;
    this.source = source;
    this.tier = tier;
    this.datacenter = datacenter;
  }

  public Duration getBreadcrumbBucketDuration() {
    return breadcrumbBucketDuration;
  }

  public Duration getBreadcrumbBucketFlushInterval() {
    return breadcrumbBucketFlushInterval;
  }

  public String getApplication() {
    return application;
  }

  public String getSource() {
    return source;
  }

  public String getTier() {
    return tier;
  }

  public String getDatacenter() {
    return datacenter;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}
