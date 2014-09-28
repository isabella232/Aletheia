package com.outbrain.aletheia.breadcrumbs;

import org.joda.time.Instant;

/**
 * A base interface for generating <code>Breadcrumb</code> instances according to a bucketKey, processingTimestamp and
 * a bucketHitCount provided by the <code>BucketBasedBreadcrumbDispatcher</code>.
 *
 * @param <TBucketKey> The type of the bucket key used by the <code>BucketBasedBreadcrumbDispatcher</code> that
 *                     requested to bake a breadcrumb.
 */
public interface BreadcrumbBaker<TBucketKey> {
  Breadcrumb bakeBreadcrumb(TBucketKey bucketKey, Instant processingTimestamp, long bucketHitCount);
}
