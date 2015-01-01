package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Instant;

/**
 * A base interface for generating {@link Breadcrumb} instances according to a bucketKey, processingTimestamp and
 * a bucketHitCount provided by the {@link BucketBasedBreadcrumbDispatcher}.
 *
 * @param <TBucketKey> The type of the bucket key used by the {@link BucketBasedBreadcrumbDispatcher} that
 *                     requested to bake a breadcrumb.
 */
public interface BreadcrumbBaker<TBucketKey> {
  Breadcrumb bakeBreadcrumb(TBucketKey bucketKey, Instant processingTimestamp, long bucketHitCount);
}
