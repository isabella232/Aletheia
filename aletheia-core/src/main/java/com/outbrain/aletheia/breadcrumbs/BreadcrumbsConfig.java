package com.outbrain.aletheia.breadcrumbs;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.Duration;

/**
 * Holds the configuration details for breadcrumbs emitted by the <code>DatumProducer</code>.
 * THe configuration consists of details concerning the length of the bucket each breadcrumb describes,
 * the flush period, which determines how often breadcrumbs will be emitted, plus the metadata that will accompany
 * each outgoing breadcrumb.
 */
public class BreadcrumbsConfig {

  private final Duration breadcrumbBucketDuration;
  private final Duration breadcrumbBucketFlushInterval;
  private final String application;
  private final String source;
  private final String tier;
  private final String datacenter;

  /**
   * @param breadcrumbBucketDuration      The time frame to be used as a bucket, aggregating all incoming hits whose
   *                                      timestamp falls within the bucket start and end time.
   * @param breadcrumbBucketFlushInterval The time interval to wait between two consecutive flush operations, where
   *                                      a flush operation is defined as dispatching all buckets currently in memory.
   * @param application                   The application string to be set to the dispatched breadcrumbs.
   * @param source                        The source string to be set to the dispatched breadcrumbs.
   * @param tier                          The tier string to be set to the dispatched breadcrumbs.
   * @param datacenter                    The datacenter string to be set to the dispatched breadcrumbs.
   */
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
