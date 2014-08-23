package com.outbrain.aletheia.datum.auditing;

import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.BreadcrumbBaker;
import org.joda.time.Instant;

/**
 * Created by slevin on 7/14/14.
 */
public class DatumBreadcrumbBaker implements BreadcrumbBaker<DatumBucketKey> {

  private final String application;
  private final String source;
  private final String destination;
  private final String tier;
  private final String datacenter;

  public DatumBreadcrumbBaker(final String source,
                              final String destination,
                              final String tier,
                              final String datacenter,
                              final String application) {

    this.source = source;
    this.destination = destination;
    this.tier = tier;
    this.datacenter = datacenter;
    this.application = application;
  }

  @Override
  public Breadcrumb bakeBreadcrumb(final DatumBucketKey bucketKey,
                                   final Instant processingTimestamp,
                                   final long bucketHitCount) {
    return new Breadcrumb(bucketKey.getDatumTypeId(),
                          source,
                          destination,
                          bucketKey.getTimestamp(),
                          bucketKey.getTimestamp().plus(bucketKey.getBucketDuration()),
                          processingTimestamp,
                          bucketHitCount,
                          datacenter,
                          application,
                          tier);

  }
}
