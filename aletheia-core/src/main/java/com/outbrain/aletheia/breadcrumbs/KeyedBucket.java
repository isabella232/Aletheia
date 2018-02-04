package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Duration;
import org.joda.time.Instant;

public class KeyedBucket extends BucketStartWithDuration {

  private final BreadcrumbKey breadcrumbKey;

  KeyedBucket(final Duration bucketDuration, final Instant bucketStart, final BreadcrumbKey breadcrumbKey) {
    super(bucketDuration, bucketStart);
    this.breadcrumbKey = breadcrumbKey;
  }

  public BreadcrumbKey getBreadcrumbKey() {
    return breadcrumbKey;
  }
}
