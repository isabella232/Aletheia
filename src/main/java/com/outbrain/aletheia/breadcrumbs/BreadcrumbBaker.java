package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Instant;

public interface BreadcrumbBaker<TBucketKey> {

  Breadcrumb bakeBreadcrumb(TBucketKey bucketKey, Instant processingTimestamp, long bucketHitCount);

}
