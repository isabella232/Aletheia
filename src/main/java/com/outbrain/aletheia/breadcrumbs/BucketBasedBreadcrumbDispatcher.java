package com.outbrain.aletheia.breadcrumbs;

import com.github.staslev.concurrent.NonBlockingOperations;
import com.google.common.collect.Lists;
import com.outbrain.aletheia.datum.auditing.BreadcrumbHandler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A hit counter that uses buckets in order to aggregate incoming hits.
 * Each bucket represents a logical timeslot to which incoming hits are assigned according to their (logical) timestamp.
 */
public class BucketBasedBreadcrumbDispatcher<TElement, TBucketKey> implements BreadcrumbDispatcher<TElement> {

  private static final Logger log = LoggerFactory.getLogger(BucketBasedBreadcrumbDispatcher.class);

  private final ConcurrentMap<TBucketKey, Long> bucketId2CountMap = new ConcurrentHashMap<>();
  private final BreadcrumbBaker<TBucketKey> breadcrumbBaker;
  private final BucketKeySelector<TElement, TBucketKey> bucketKeySelector;
  private final BreadcrumbHandler BreadcrumbHandler;
  private final HitLogger hitLogger;

  public BucketBasedBreadcrumbDispatcher(final BucketKeySelector<TElement, TBucketKey> bucketKeySelector,
                                         final BreadcrumbBaker<TBucketKey> breadcrumbBaker,
                                         final BreadcrumbHandler breadcrumbHandler,
                                         final HitLogger hitLogger) {

    this.bucketKeySelector = bucketKeySelector;
    this.breadcrumbBaker = breadcrumbBaker;
    this.BreadcrumbHandler = breadcrumbHandler;
    this.hitLogger = hitLogger;
  }

  private void threadSafelyIncreaseHitCount(final TBucketKey bucketId) {
    NonBlockingOperations.forMap.withLongValues().increase(bucketId2CountMap, bucketId);
  }

  @Override
  public void report(final TElement item) {

    final TBucketKey bucketId = bucketKeySelector.selectKey(item);

    hitLogger.logHit(bucketId);

    threadSafelyIncreaseHitCount(bucketId);
  }

  @Override
  public void dispatchBreadcrumbs() {

    final Instant processingTimestamp = Instant.now();
    final List<TBucketKey> bucketIds = Lists.newLinkedList(bucketId2CountMap.keySet());

    for (final TBucketKey bucketId : bucketIds) {

      final Long bucketHitCount = bucketId2CountMap.remove(bucketId);

      if (bucketHitCount == null) {
        // current bucketId has already been removed from the map by someone else.
        // oh, well, the show must go on.
        continue;
      }

      try {
        final Breadcrumb breadcrumb = breadcrumbBaker.bakeBreadcrumb(bucketId, processingTimestamp, bucketHitCount);
        BreadcrumbHandler.handle(breadcrumb);
      } catch (final Exception e) {

        log.error(String.format(
                          "Failed to flush hits for processingTimestamp: [%d], bucketId: [%s], aggregated hit count: [%d]",
                          processingTimestamp.getMillis(),
                          bucketId,
                          bucketHitCount),
                  e);
      }
    }
  }
}
