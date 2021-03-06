package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Duration;
import org.joda.time.Instant;

public class BucketStartWithDuration {

  private final Duration bucketDuration;
  private final Instant bucketStart;

  BucketStartWithDuration(final Duration bucketDuration, final Instant bucketStart) {
    this.bucketDuration = bucketDuration;
    this.bucketStart = bucketStart;
  }

  public Duration getBucketDuration() {
    return bucketDuration;
  }

  public Instant getBucketStart() {
    return bucketStart;
  }
}
