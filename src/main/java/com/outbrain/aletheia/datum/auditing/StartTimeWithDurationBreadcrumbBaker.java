package com.outbrain.aletheia.datum.auditing;

import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbBaker;
import com.outbrain.aletheia.breadcrumbs.BucketStartWithDuration;
import org.joda.time.Instant;

/**
 * Created by slevin on 7/14/14.
 */
public class StartTimeWithDurationBreadcrumbBaker implements BreadcrumbBaker<BucketStartWithDuration> {

  private final String application;
  private final String source;
  private final String destination;
  private final String tier;
  private final String datacenter;
  private final String breadcrumbTypeId;

  public StartTimeWithDurationBreadcrumbBaker(final String source,
                                              final String destination,
                                              final String tier,
                                              final String datacenter,
                                              final String application,
                                              final String breadcrumbTypeId) {

    this.source = source;
    this.destination = destination;
    this.tier = tier;
    this.datacenter = datacenter;
    this.application = application;
    this.breadcrumbTypeId = breadcrumbTypeId;
  }

  @Override
  public Breadcrumb bakeBreadcrumb(final BucketStartWithDuration bucketKey,
                                   final Instant processingTimestamp,
                                   final long bucketHitCount) {

    return new Breadcrumb(breadcrumbTypeId,
                          source,
                          destination,
                          bucketKey.getBucketStart(),
                          bucketKey.getBucketStart().plus(bucketKey.getBucketDuration()),
                          processingTimestamp,
                          bucketHitCount,
                          datacenter,
                          application,
                          tier);

  }
}
