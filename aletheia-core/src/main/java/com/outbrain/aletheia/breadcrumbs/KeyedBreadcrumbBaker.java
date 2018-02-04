package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Instant;

/**
 * An implementation of {@link BreadcrumbBaker}, where the bucket key consists of a bucket's start time,
 * a bucket duration and a breadcrumb key
 */
public class KeyedBreadcrumbBaker implements BreadcrumbBaker<KeyedBucket> {

  private final String tier;
  private final String datacenter;

  public KeyedBreadcrumbBaker(final String tier,
                              final String datacenter) {
    this.tier = tier;
    this.datacenter = datacenter;
  }

  @Override
  public Breadcrumb bakeBreadcrumb(final KeyedBucket bucketKey,
                                   final Instant processingTimestamp,
                                   final long bucketHitCount) {

    final BreadcrumbKey breadcrumbKey = bucketKey.getBreadcrumbKey();

    return new Breadcrumb(breadcrumbKey.getType(),
        breadcrumbKey.getSource(),
        breadcrumbKey.getDestination(),
        bucketKey.getBucketStart(),
        bucketKey.getBucketStart().plus(bucketKey.getBucketDuration()),
        processingTimestamp,
        bucketHitCount,
        datacenter,
        breadcrumbKey.getApplication(),
        tier);
  }
}
